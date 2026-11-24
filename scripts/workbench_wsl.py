"""Host the replication demo in WSL over private standard-input/output pipes."""

import argparse
from collections import deque
from contextlib import contextmanager
import json
from pathlib import Path
from queue import Empty, Queue
import signal
import subprocess
import sys
from threading import Lock, Thread

from workbench_operations import Problem, action_from_path, embedded, operation


MAX_LINE = 2 * 1024 * 1024
STARTUP_TIMEOUT = 45
OPERATION_TIMEOUT = 20
SHUTDOWN_TIMEOUT = 40


def _send(stream, payload):
    stream.write(json.dumps(payload).encode("ascii") + b"\n")
    stream.flush()


def _receive(stream):
    line = stream.readline(MAX_LINE + 1)
    if not line:
        return None
    if len(line) > MAX_LINE or not line.endswith(b"\n"):
        raise ValueError("WSL 消息超过上限或不完整")
    payload = json.loads(line)
    if not isinstance(payload, dict):
        raise ValueError("WSL 消息必须是 JSON 对象")
    return payload


def _problem(error):
    return {"status": getattr(error, "status", 503), "error": str(error)[:1500]}


def _serve(manager, stdin, stdout):
    while True:
        try:
            request = _receive(stdin)
        except ValueError as error:
            _send(stdout, _problem(error))
            return
        if request is None:
            return
        try:
            if request == {"op": "status"}:
                result = manager.status()
            elif request.keys() == {"op", "path", "body"} and request["op"] == "change":
                path, body = request["path"], request["body"]
                if not isinstance(path, str) or not isinstance(body, dict):
                    raise Problem(400, "复制操作参数无效")
                action, data = action_from_path(path, body)
                path, body, _ = operation(action, data)
                result = manager.change(path, body)
            else:
                raise Problem(400, "未知 WSL 操作")
        except (Problem, OSError, RuntimeError, ValueError, KeyError) as error:
            response = _problem(error)
        else:
            response = {"result": result}
        _send(stdout, response)


def _worker(bin_dir):
    def shutdown(_signal, _frame):
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGHUP, shutdown)
    try:
        with embedded(bin_dir) as manager:
            _send(sys.stdout.buffer, {"result": {"label": manager.label}})
            _serve(manager, sys.stdin.buffer, sys.stdout.buffer)
    except BrokenPipeError:
        return 0
    except (Problem, OSError, RuntimeError, ValueError) as error:
        try:
            _send(sys.stdout.buffer, _problem(error))
        except BrokenPipeError:
            pass
        return 1
    return 0


def _diagnostic(raw):
    if not raw:
        return ""
    if b"\x00" in raw and raw.count(b"\x00") * 4 > len(raw):
        return raw.decode("utf-16-le", errors="replace").strip("\ufeff \r\n\x00")[:1500]
    return raw.decode("utf-8", errors="replace").strip()[:1500]


def _wsl_command(distro, user):
    command = ["wsl.exe"]
    if distro:
        command += ["--distribution", distro]
    if user:
        command += ["--user", user]
    return command + ["--exec"]


class WslManager:
    def __init__(self, process):
        self.process = process
        self.label = ""
        self.requests = Lock()
        self.responses = Queue()
        self.stderr = deque(maxlen=4)
        self.reader = Thread(target=self._read_responses, daemon=True)
        self.errors = Thread(target=self._read_errors, daemon=True)
        self.reader.start()
        self.errors.start()

    def _read_responses(self):
        try:
            while (payload := _receive(self.process.stdout)) is not None:
                self.responses.put(payload)
        except (OSError, ValueError):
            self.responses.put({"status": 502, "error": "WSL 复制进程返回了无效消息"})
        finally:
            self.responses.put(None)

    def _read_errors(self):
        try:
            while (chunk := self.process.stderr.read1(4096)):
                self.stderr.append(chunk)
        except OSError:
            pass

    def _failure(self, label):
        detail = _diagnostic(b"".join(self.stderr.copy()))
        return Problem(503, f"{label}。{detail}" if detail else label)

    def _end_input(self):
        try:
            self.process.stdin.close()
        except OSError:
            pass

    def _wait(self, timeout, timeout_message):
        try:
            response = self.responses.get(timeout=timeout)
        except Empty:
            self._end_input()
            raise self._failure(timeout_message) from None
        if response is None:
            self.errors.join(timeout=0.2)
            raise self._failure("WSL 复制进程已退出")
        if "error" in response:
            raise Problem(response.get("status", 503), response["error"])
        result = response.get("result")
        if not isinstance(result, dict):
            self._end_input()
            raise Problem(502, "WSL 复制进程返回了无效结果")
        return result

    def start(self, distro):
        data = self._wait(STARTUP_TIMEOUT, "WSL 复制进程启动超时")
        self.label = f"WSL {distro + ':' if distro else ''}{data['label']}"

    def _call(self, request):
        with self.requests:
            if self.process.stdin.closed or self.process.poll() is not None:
                raise self._failure("WSL 复制进程已退出")
            try:
                _send(self.process.stdin, request)
                return self._wait(OPERATION_TIMEOUT, "WSL 复制操作超时，正在结束托管进程")
            except OSError:
                self._end_input()
                raise self._failure("无法与 WSL 复制进程通信") from None
            except KeyboardInterrupt:
                self._end_input()
                raise

    def status(self):
        return self._call({"op": "status"})

    def change(self, path, body):
        if not isinstance(path, str) or not isinstance(body, dict):
            raise Problem(400, "复制操作参数无效")
        action, data = action_from_path(path, body)
        path, body, _ = operation(action, data)
        return self._call({"op": "change", "path": path, "body": body})

    def close(self):
        with self.requests:
            self._end_input()
            try:
                self.process.wait(timeout=SHUTDOWN_TIMEOUT)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
        self.reader.join(timeout=1)
        self.errors.join(timeout=1)
        if not self.reader.is_alive():
            self.process.stdout.close()
        if not self.errors.is_alive():
            self.process.stderr.close()


@contextmanager
def wsl_managed(bin_dir=None, distro=None, user=None):
    """Start a WSL worker and stop its services when this context ends."""
    command = _wsl_command(distro, user)
    flags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    try:
        conversion = subprocess.run(
            [*command, "wslpath", "-a", "-u", str(Path(__file__).resolve())],
            capture_output=True, timeout=20, creationflags=flags,
        )
        if conversion.returncode:
            raise Problem(503, "无法定位 WSL 内的工作台脚本。" + _diagnostic(conversion.stderr))
        worker = conversion.stdout.decode("utf-8-sig").strip()
        if not worker.startswith("/"):
            raise Problem(503, "无法定位 WSL 内的工作台脚本。")
        args = [*command, "python3", "-B", worker]
        if bin_dir is not None:
            args.append(f"--bin-dir={bin_dir}")
        process = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, creationflags=flags)
    except (OSError, ValueError, subprocess.TimeoutExpired) as error:
        raise Problem(503, f"无法启动 WSL 复制进程，请检查 WSL 发行版、用户和 Python 3。{error}") from None
    manager = WslManager(process)
    try:
        manager.start(distro)
        yield manager
    finally:
        manager.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kerniva workbench WSL worker")
    parser.add_argument("--bin-dir", type=Path, default=Path(__file__).resolve().parents[1] / "bin")
    raise SystemExit(_worker(parser.parse_args().bin_dir))
