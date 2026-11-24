"""End-to-end verification of the local Workbench API and the interactive shell."""

import argparse
from contextlib import contextmanager
import json
import os
from pathlib import Path
import queue
import re
import socket
import subprocess
import sys
import tempfile
import threading
import time
from urllib.error import HTTPError
from urllib.request import ProxyHandler, Request, build_opener
from uuid import uuid4


WORKBENCH = Path(__file__).with_name("kerniva-workbench.py").resolve()
OPENING_LINE = re.compile(r"Open (http://127\.0\.0\.1:\d+)/")
READINESS_TIMEOUT = 75
STOP_TIMEOUT = 35

# EOF gives the parent a portable way to trigger KeyboardInterrupt on the
# service's main thread and unwind both the native and WSL controller contexts.
EOF_LAUNCHER = """
import _thread
import runpy
import sys
from pathlib import Path
from threading import Thread

def wait_for_eof():
    sys.stdin.buffer.read()
    _thread.interrupt_main()

Thread(target=wait_for_eof, daemon=True).start()
del sys.argv[0]
sys.path.insert(0, str(Path(sys.argv[0]).parent))
runpy.run_path(sys.argv[0], run_name="__main__")
"""


def require(condition, detail):
    if not condition:
        raise RuntimeError(detail)


def wait_for(probe, description, seconds=15):
    until = time.monotonic() + seconds
    while time.monotonic() < until:
        result = probe()
        if result:
            return result
        time.sleep(0.1)
    raise RuntimeError(f"Timed out waiting for {description}")


@contextmanager
def workbench(args, root):
    command = [sys.executable, "-u", "-B", "-c", EOF_LAUNCHER, str(WORKBENCH),
               "--with-demo", "--config", str(root / "workbench.json"), "--port", "0"]
    for option in ("bin_dir", "wsl_distro", "wsl_user"):
        if value := getattr(args, option):
            command.append(f"--{option.replace('_', '-')}={value}")
    env = {**os.environ, "PYTHONUTF8": "1", "TMPDIR": str(root)}
    lines = queue.Queue()

    def read_opening_line(pipe):
        try:
            for line in pipe:
                if match := OPENING_LINE.fullmatch(line.strip()):
                    lines.put(match.group(1))
                    return
        finally:
            lines.put(None)

    with (root / "service.stderr").open("w+", encoding="utf-8") as stderr:
        with subprocess.Popen(command, env=env, stdin=subprocess.PIPE,
                              stdout=subprocess.PIPE, stderr=stderr,
                              text=True, encoding="utf-8") as process:
            reader = threading.Thread(target=read_opening_line, args=(process.stdout,), daemon=True)
            reader.start()
            try:
                try:
                    origin = lines.get(timeout=READINESS_TIMEOUT)
                except queue.Empty:
                    raise RuntimeError("Workbench startup timed out") from None
                require(origin, f"Workbench failed to start (exit status {process.poll()})")
                yield origin
            finally:
                process.stdin.close()
                try:
                    process.wait(timeout=STOP_TIMEOUT)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                    raise RuntimeError("Workbench did not stop gracefully") from None
                finally:
                    reader.join(timeout=2)
                if process.returncode:
                    stderr.seek(0)
                    # The summary stays free of session and marker data.
                    hint = " A startup diagnostic was emitted." if stderr.read(1) else ""
                    raise RuntimeError(f"Workbench exited with status {process.returncode}.{hint}")


class API:
    def __init__(self, origin):
        self.origin = origin
        self.opener = build_opener(ProxyHandler({}))

    def __call__(self, path, body=None, *, status=200):
        data = None if body is None else json.dumps(body).encode("utf-8")
        request = Request(f"{self.origin}/api/{path}", data=data,
                          headers={"Content-Type": "application/json", "Origin": self.origin})
        try:
            with self.opener.open(request, timeout=30) as response:
                code, payload = response.status, json.load(response)
        except HTTPError as error:
            with error:
                code, payload = error.code, json.load(error)
        require(code == status, f"Unexpected HTTP {code} for {path} (expected {status})")
        return payload

    def topology(self):
        return self("replication/status")


def shell(origin, *commands):
    result = subprocess.run(
        [sys.executable, "-B", str(WORKBENCH), "--shell", "--url", origin],
        input="\n".join((*commands, "/exit", "")),
        capture_output=True, text=True, encoding="utf-8", timeout=60,
        env={**os.environ, "PYTHONUTF8": "1"},
    )
    require(result.returncode == 0, f"Interactive shell exited with status {result.returncode}")
    require(not any(line.startswith("错误 ") for line in result.stdout.splitlines()),
            "Interactive shell reported a command error")
    return result.stdout


def reading(topology, node, db):
    return (topology[node] if node in ("primary", "source") else topology["replicas"][node])["dbs"][db]


def node_ports(topology):
    return [topology["primary"]["port"], topology["source"]["port"],
            *(node["port"] for node in topology["replicas"].values())]


def ports_closed(ports, args=None):
    if os.name == "nt" and args is not None:
        command = ["wsl.exe"]
        for option, value in (("--distribution", args.wsl_distro), ("--user", args.wsl_user)):
            if value:
                command += [option, value]
        probe = (
            "import socket, sys\n"
            "for port in map(int, sys.argv[1:]):\n"
            "    with socket.socket() as sock:\n"
            "        sock.settimeout(1)\n"
            "        if sock.connect_ex(('127.0.0.1', port)) == 0:\n"
            "            raise SystemExit(1)\n"
        )
        result = subprocess.run([*command, "--exec", "python3", "-c", probe, *map(str, ports)],
                                capture_output=True, timeout=25)
        return result.returncode == 0
    for port in ports:
        with socket.socket() as sock:
            sock.settimeout(1)
            if sock.connect_ex(("127.0.0.1", port)) == 0:
                return False
    return True


def exercise(origin):
    api = API(origin)
    state = api("state")
    require(state["apiVersion"] == 2 and state["replicationAvailable"], "Managed API unavailable")
    def initially_caught_up():
        current = api.topology()
        return current if all(
            reading(current, name, db).get("caught_up")
            for name in ("a", "b") for db in ("0", "1")
        ) else None

    initial = wait_for(initially_caught_up, "initial replica synchronization")
    source = reading(initial, "source", "1")["source"]
    require(source and reading(initial, "source", "0")["source"] == source, "Expected a single log source")
    default_db0 = reading(initial, "primary", "0")
    profile_name = "Workbench integration DB1"
    profile = api("profile/save", {"profile": {
        "name": profile_name, "host": "127.0.0.1", "port": initial["primary"]["port"],
        "db": 1, "mode": "standalone", "timeout": 5,
    }})["profile"]
    token = api("connect", {"id": profile["id"], "password": ""})["session"]
    status = api("status", {"session": token})
    require(status["db"] == 1 and status["nodes"][0]["storageBackend"] == "go-log",
            "Connected status did not report the Go-log DB1 primary")

    first = f"integration-api-{uuid4().hex}"
    second = f"integration-cli-{uuid4().hex}"

    def caught_up(value):
        current = api.topology()
        require(reading(current, "source", "1")["source"] == source, "Log source changed")
        return current if all(
            reading(current, replica, "1")["caught_up"]
            and reading(current, replica, "1")["value"] == value
            and reading(current, replica, "1")["source"] == source
            for replica in ("a", "b")
        ) else None

    require(api("replication/marker", {"db": 1, "value": first})["ok"], "API marker rejected")
    wait_for(lambda: caught_up(first), "API marker on both replicas")
    output = shell(origin, "/profiles", f"/connect {profile['id']}", "/status",
                   "/get demo:marker", "/replica status", "/replica activity", "/disconnect")
    require(profile_name in output and first in output.splitlines() and source in output
            and "go-log" in output, "Shell did not read the API profile, marker and topology")

    shell(origin, "/replica pause b", f"/replica mark 1 {second}", "/settings pageSize 7")

    def paused_and_advanced():
        current = api.topology()
        return (current["replicas"]["b"]["paused"] and not current["replicas"]["b"]["running"]
                and reading(current, "a", "1")["value"] == second)

    wait_for(paused_and_advanced, "CLI marker on A while B is paused")
    key = api("key", {"session": token, "key_b64": "ZGVtbzptYXJrZXI="})
    require(key["value"]["text"] == second, "API did not read the CLI marker")
    require(api("replication/replicas/b/resume", {})["ok"], "API did not resume B")
    wait_for(lambda: caught_up(second), "B catch-up after API resume")
    require(api("replication/replicas/a/pause", {})["ok"], "API did not pause A")
    output = shell(origin, "/replica resume a", "/replica activity", "/replica activity 2")
    require(first in output and second in output and source in output, "Shell history missing API or CLI writes")
    final = wait_for(lambda: caught_up(second), "A catch-up after CLI resume")
    require(reading(final, "primary", "0") == default_db0, "DB0 changed during DB1 operations")
    require(api("state")["settings"]["pageSize"] == 7, "API did not read CLI setting")
    require(api("settings", {"settings": {"previewBytes": 2048}})["settings"]["previewBytes"] == 2048,
            "API did not save setting")
    require('"previewBytes": 2048' in shell(origin, "/settings"), "Shell did not read API setting")
    require(api("disconnect", {"session": token})["ok"], "API did not disconnect")
    api("status", {"session": token}, status=401)
    api("replication/marker", {"db": 2, "value": second}, status=400)
    first_page, second_page = (api(f"replication/activity?page={page}") for page in (1, 2))
    history = first_page["items"] + second_page["items"]
    require(first_page["total"] == 6 and first_page["pages"] == 2
            and len(first_page["items"]) == 5 and len(second_page["items"]) == 1
            and len({item["id"] for item in history}) == 6 and all(item["source"] == source for item in history)
            and first in second_page["items"][0]["message"]
            and any(second in item["message"] for item in first_page["items"]),
            "Shared operation history was not paginated or source-scoped")
    records = api("replication/experiments")["records"]
    require(len(records) == 16 and len({record["series"] for record in records}) == 2
            and all(re.fullmatch(r"[0-9a-f]{64}", record["source_sha256"]) for record in records),
            "Incorrect experiment archive")
    return node_ports(final), {"databases": 2, "replicas": 2, "activityRecords": len(history),
                               "activityPages": first_page["pages"], "experimentRecords": len(records)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bin-dir", help="Build directory (Linux path in WSL on Windows)")
    parser.add_argument("--wsl-distro", help="Optional WSL distribution (Windows)")
    parser.add_argument("--wsl-user", help="Optional WSL user (Windows)")
    args = parser.parse_args()
    if os.name != "nt" and (args.wsl_distro or args.wsl_user):
        parser.error("WSL options apply on Windows")
    try:
        with tempfile.TemporaryDirectory(prefix="kerniva-workbench-test-") as tmp:
            with workbench(args, Path(tmp)) as origin:
                ports, result = exercise(origin)
            web_port = int(origin.rsplit(":", 1)[1])
            require(ports_closed([web_port]), "Workbench HTTP listener remained open")
            wait_for(lambda: ports_closed(ports, args), "all replication listeners to close", seconds=12)
    except (OSError, RuntimeError, ValueError, subprocess.SubprocessError, KeyError) as error:
        reason = str(error) if isinstance(error, RuntimeError) else type(error).__name__
        print(json.dumps({"ok": False, "error": reason}, ensure_ascii=True))
        return 1
    print(json.dumps({"ok": True, **result, "cleanShutdown": True}, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
