#!/usr/bin/env python3
"""Manage a small, real Go-log replication demo on Linux loopback."""

import argparse
from contextlib import ExitStack, closing
from http.client import HTTPConnection, HTTPException
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import select
import signal
import socket
import subprocess
import sys
import tempfile
from threading import Lock
import time
from urllib.parse import urlencode, urlsplit


HERE = Path(__file__).resolve().parent
LOOPBACK = "127.0.0.1"
DATABASES = (0, 1)
MARKER_KEY = "demo:marker"
PROBE_TIMEOUT = 0.6
MAX_MARKER_BYTES = 256
PROBE_ERRORS = (OSError, HTTPException, ValueError, RuntimeError, KeyError)


def resp_command(conn, reader, *args):
    words = [arg.encode("utf-8") for arg in args]
    conn.sendall(f"*{len(words)}\r\n".encode() + b"".join(
        b"$%d\r\n" % len(word) + word + b"\r\n" for word in words
    ))
    header = reader.readline(4096)
    if not header.endswith(b"\r\n"):
        raise RuntimeError("Incomplete RESP reply")
    kind, value = header[:1], header[1:-2]
    if kind == b"+":
        return value.decode()
    if kind == b"-":
        raise RuntimeError(value.decode(errors="replace"))
    if kind == b"$":
        length = int(value)
        if length == -1:
            return None
        if 0 <= length <= 65536:
            payload = reader.read(length + 2)
            if len(payload) == length + 2 and payload.endswith(b"\r\n"):
                return payload[:-2].decode()
    raise RuntimeError("Unexpected RESP reply")


def http_get(port, path, limit, **query):
    with closing(HTTPConnection(LOOPBACK, port, timeout=PROBE_TIMEOUT)) as client:
        client.request("GET", f"{path}?{urlencode(query)}")
        with client.getresponse() as response:
            return response.status, response.read(limit)


def inspect_source(port, db, after):
    with closing(HTTPConnection(LOOPBACK, port, timeout=PROBE_TIMEOUT)) as client:
        client.request("GET", f"/pull?db={db}&after={after}")
        with client.getresponse() as response:
            if response.status != 200:
                raise RuntimeError(f"Source DB{db} returned HTTP {response.status}")
            header = response.read(4)
            length = int.from_bytes(header, "big")
            if len(header) != 4 or not 0 < length <= 1 << 20:
                raise RuntimeError("Invalid source manifest length")
            manifest = json.loads(response.read(length))
            return {"source": manifest["source"], "sequence": manifest["last"]}


def listener_inodes(port):
    address = f"0100007F:{port:04X}"
    with Path("/proc/net/tcp").open() as table:
        return {f"socket:[{parts[9]}]" for line in table if (parts := line.split())
                and parts[1] == address and parts[3] == "0A"}


def owns_socket(proc, inodes):
    if not inodes:
        return False
    try:
        with os.scandir(proc / "fd") as descriptors:
            for fd in descriptors:
                try:
                    if os.readlink(fd.path) in inodes:
                        return True
                except FileNotFoundError:
                    continue
    except (FileNotFoundError, PermissionError):
        pass
    return False


class Service:
    def __init__(self, root, name, command, reservation, environment):
        self.root = root
        self.command = command
        self.reservation = reservation
        self.port = reservation.getsockname()[1]
        self.environment = environment
        self.log = root / f"{name}.console.log"
        self.process = None
        self.paused = False

    def start(self):
        if self.running():
            return
        self.reservation.close()
        with self.log.open("ab") as log:
            self.process = subprocess.Popen(
                self.command, cwd=self.root, env=self.environment, stdin=subprocess.DEVNULL,
                stdout=log, stderr=subprocess.STDOUT, start_new_session=True,
            )

    def running(self):
        return self.process is not None and self.process.poll() is None

    def owns_listener(self):
        if not self.running():
            return False
        # Closing the reservation and binding in the child are separate steps.
        # Check that the loopback listener belongs to this child before using it.
        return owns_socket(Path(f"/proc/{self.process.pid}"), listener_inodes(self.port))

    def status(self):
        running = self.running()
        exit_code = self.process.returncode if self.process is not None else None
        error = None
        if not running and not self.paused and exit_code is not None:
            error = f"Process exited with status {exit_code}"
        return {"running": running, "ready": False, "port": self.port, "paused": self.paused, "error": error}

    def require_listener(self):
        if not self.owns_listener():
            raise RuntimeError(self.status()["error"] or f"Demo service on port {self.port} is not ready")

    def wait_for_listener(self):
        deadline = time.monotonic() + 8
        while self.running() and time.monotonic() < deadline:
            if self.owns_listener():
                return
            time.sleep(0.05)
        raise RuntimeError(f"Service failed to listen on port {self.port}; see {self.log}")

    def stop(self):
        if not self.running():
            return
        self.process.terminate()
        try:
            self.process.wait(timeout=6)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait()


class Demo:
    def __init__(self, root, args, reservations):
        self.lock = Lock()
        self.closed = False
        self.workbench_url = args.workbench_url
        env = {name: value for name, value in os.environ.items() if not name.startswith("KERNIVA_")}
        env["KERNIVA_ENGINE_LIBRARY"] = str(args.library)
        primary_socket, source_socket, a_socket, b_socket = reservations
        config = root / "primary.json"
        data = root / "primary-data"
        config.write_text(json.dumps({
            "host": LOOPBACK,
            "port": primary_socket.getsockname()[1],
            "network_type": "std",
            "db_count": len(DATABASES),
            "storage_backend": "go-log",
            "persistence_enabled": True,
            "persistence_write_mode": "strong",
            "persistence_durability": "sync",
            "cold_start_strategy": "load_all",
            "data_dir": str(data),
            "go_log_segment_age_seconds": 1,
            "max_memory": 64 << 20,
            "max_memory_policy": "noeviction",
            "log_level": "info",
            "log_path": str(root / "primary.log"),
            "pprof_enabled": False,
        }), encoding="utf-8")

        self.primary = Service(root, "primary", [args.server_bin, "-config", config], primary_socket, env)
        self.source = Service(root, "source", [
            args.replica_bin, "serve", "--root", data / "go-log-v1",
            "--listen", f"{LOOPBACK}:{source_socket.getsockname()[1]}",
        ], source_socket, env)
        self.source_cursors = dict.fromkeys(DATABASES, 0)
        self.replicas = {}
        for name, reservation in (("a", a_socket), ("b", b_socket)):
            self.replicas[name] = Service(root, f"replica-{name}", [
                args.replica_bin, "follow", "--root", root / f"replica-{name}",
                "--listen", f"{LOOPBACK}:{reservation.getsockname()[1]}",
                "--source", f"http://{LOOPBACK}:{self.source.port}", "--dbs", "0,1", "--interval", "150ms",
            ], reservation, env)

    def start(self):
        self.primary.start()
        self.primary.wait_for_listener()
        for db in DATABASES:
            self.write_marker(db, f"DB{db} initial")
        self.source.start()
        self.source.wait_for_listener()
        for replica in self.replicas.values():
            replica.start()
            replica.wait_for_listener()

    def close(self):
        with self.lock:
            self.closed = True
            for service in (*self.replicas.values(), self.source, self.primary):
                service.stop()

    def primary_commands(self, db, *commands):
        self.primary.require_listener()
        with socket.create_connection((LOOPBACK, self.primary.port), timeout=PROBE_TIMEOUT) as conn:
            with conn.makefile("rb") as reader:
                if resp_command(conn, reader, "SELECT", str(db)) != "OK":
                    raise RuntimeError(f"Could not select primary DB{db}")
                return [resp_command(conn, reader, *command) for command in commands]

    def write_marker(self, db, value):
        result, = self.primary_commands(db, ("SET", MARKER_KEY, value))
        if result != "OK":
            raise RuntimeError(f"Primary DB{db} did not acknowledge the write")

    def read_primary(self, db):
        value, info = self.primary_commands(db, ("GET", MARKER_KEY), ("INFO",))
        fields = dict(line.split(":", 1) for line in info.splitlines() if ":" in line and not line.startswith("#"))
        if fields.get("storage_backend") != "go-log" or fields.get("persistence_durability") != "sync":
            raise RuntimeError("Expected the Go-log backend with synchronous durability")
        return {"value": value, "sequence": int(fields["last_sequence"])}

    def set_paused(self, name, paused):
        replica = self.replicas[name]
        if paused:
            replica.stop()
            replica.paused = True
        else:
            replica.start()
            try:
                replica.wait_for_listener()
            except RuntimeError:
                replica.stop()
                raise
            replica.paused = False

    def read_source(self, db):
        state = inspect_source(self.source.port, db, self.source_cursors[db])
        self.source_cursors[db] = state["sequence"]
        return state

    def read_replica(self, replica, db, primary, source):
        def view_status():
            code, body = http_get(replica.port, "/status", 65536, db=db)
            if code != 200:
                raise RuntimeError(f"Replica DB{db} status returned HTTP {code}")
            return json.loads(body)

        before = view_status()
        code, body = http_get(replica.port, "/get", MAX_MARKER_BYTES + 1, db=db, key=MARKER_KEY)
        after = view_status()
        stable = (before["ready"], before["source"], before["sequence"]) == (
            after["ready"], after["source"], after["sequence"],
        )
        after["value"] = body.decode() if code == 200 and stable else None
        after["caught_up"] = (
            stable and after["ready"] and not after["last_error"] and code == 200
            and after["source"] == source.get("source")
            and after["sequence"] == primary.get("sequence")
            and after["value"] == primary.get("value")
        )
        return after

    def read_node(self, service, empty, probe):
        node = service.status()
        node["dbs"] = {str(db): dict(empty) for db in DATABASES}
        if not node["running"]:
            return node
        try:
            service.require_listener()
        except PROBE_ERRORS as error:
            node["error"] = str(error)
            return node
        all_read = True
        for db in DATABASES:
            try:
                node["dbs"][str(db)] = probe(db)
            except PROBE_ERRORS as error:
                node["error"] = str(error)
                all_read = False
        node["ready"] = all_read
        return node

    def status(self):
        primary = self.read_node(self.primary, {"value": None, "sequence": None}, self.read_primary)
        source = self.read_node(self.source, {"source": None, "sequence": None},
                                self.read_source)
        replicas = {}
        for name, replica in self.replicas.items():
            replicas[name] = self.read_node(
                replica, {"ready": False, "value": None, "sequence": None, "caught_up": False},
                lambda db: self.read_replica(replica, db, primary["dbs"][str(db)], source["dbs"][str(db)]),
            )
        return {"primary": primary, "source": source, "replicas": replicas, "marker_key": MARKER_KEY,
                "workbench_url": self.workbench_url}


STATIC = {
    "/": ("index.html", "text/html; charset=utf-8"),
    "/index.html": ("index.html", "text/html; charset=utf-8"),
    "/app.js": ("app.js", "text/javascript; charset=utf-8"),
    "/styles.css": ("styles.css", "text/css; charset=utf-8"),
}
REPLICA_ACTIONS = {f"/api/replicas/{name}/{action}": (name, action == "pause")
                   for name in ("a", "b") for action in ("pause", "resume")}


class PanelHandler(BaseHTTPRequestHandler):
    def setup(self):
        super().setup()
        self.connection.settimeout(4)

    def reply(self, status, content_type, data):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Content-Security-Policy", "default-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'")
        self.end_headers()
        self.wfile.write(data)

    def reply_json(self, status, data):
        self.reply(status, "application/json", json.dumps(data, ensure_ascii=True).encode())

    def valid_host(self):
        port = self.server.server_port
        host = self.headers.get("Host")
        return self.headers.get_all("Host") == [host] and host in (f"{LOOPBACK}:{port}", f"localhost:{port}")

    def do_GET(self):
        if not self.valid_host():
            self.reply_json(403, {"error": "Use the dashboard's loopback URL"})
            return
        if self.path == "/api/status":
            with self.server.demo.lock:
                state = self.server.demo.status()
            self.reply_json(200, state)
            return
        asset = STATIC.get(self.path)
        if asset is not None:
            name, content_type = asset
            try:
                data = (HERE / "replica-demo-ui" / name).read_bytes()
            except FileNotFoundError:
                self.reply_json(404, {"error": "Dashboard asset is missing"})
                return
            self.reply(200, content_type, data)
            return
        self.reply_json(404, {"error": "Unknown route"})

    def do_POST(self):
        origin = f"http://{self.headers.get('Host')}"
        if (not self.valid_host() or self.headers.get("Origin", origin) != origin
                or self.headers.get("Sec-Fetch-Site", "same-origin") != "same-origin"):
            self.reply_json(403, {"error": "Cross-origin requests are denied"})
            return
        action = REPLICA_ACTIONS.get(self.path)
        if self.path != "/api/marker" and action is None:
            self.reply_json(404, {"error": "Unknown action"})
            return
        if self.headers.get_content_type() != "application/json":
            self.reply_json(415, {"error": "Expected application/json"})
            return

        try:
            length = int(self.headers.get("Content-Length", "0"))
            if not 1 <= length <= 4096 or self.headers.get("Transfer-Encoding"):
                raise ValueError("Invalid request size")
            request = json.loads(self.rfile.read(length))
            if not isinstance(request, dict):
                raise ValueError("Expected a JSON object")
            if action is None:
                db, value = request.get("db"), request.get("value")
                if (request.keys() != {"db", "value"} or type(db) is not int or db not in DATABASES
                        or not isinstance(value, str) or not 1 <= len(value.encode()) <= MAX_MARKER_BYTES):
                    raise ValueError(f"Expected DB0 or DB1 and a marker of 1-{MAX_MARKER_BYTES} UTF-8 bytes")
            elif request:
                raise ValueError("Expected an empty JSON object")
        except (OSError, ValueError) as error:
            self.reply_json(400, {"error": str(error)})
            return

        demo = self.server.demo
        result = {"ok": True}
        try:
            with demo.lock:
                if demo.closed:
                    raise RuntimeError("Demo is shutting down")
                if action is None:
                    demo.write_marker(db, value)
                    result.update({"db": db, "value": value})
                else:
                    demo.set_paused(*action)
        except PROBE_ERRORS as error:
            self.reply_json(503, {"error": str(error)})
            return
        self.reply_json(200, result)

    def log_message(self, *_args):
        pass


def is_demo_process(proc):
    if proc.stat().st_uid != os.geteuid() or not (proc / "exe").samefile(sys.executable):
        return False
    argv = (proc / "cmdline").read_bytes().rstrip(b"\0").split(b"\0")
    script_index = 1
    while script_index < len(argv) and argv[script_index] in (b"-u", b"-B"):
        script_index += 1
    if script_index < len(argv) and argv[script_index] == b"--":
        script_index += 1
    if script_index >= len(argv):
        return False
    script = Path(os.fsdecode(argv[script_index]))
    if not script.is_absolute():
        script = (proc / "cwd") / script
    return script.resolve() == Path(__file__).resolve()


def stop_panel(port):
    if not hasattr(os, "pidfd_open") or not hasattr(signal, "pidfd_send_signal"):
        raise RuntimeError("Stopping by port requires Linux pidfd support and Python 3.9 or newer")
    inodes = listener_inodes(port)
    matches = []
    with ExitStack() as targets:
        if inodes:
            for proc in Path("/proc").iterdir():
                if not proc.name.isdecimal():
                    continue
                with ExitStack() as candidate:
                    try:
                        if proc.stat().st_uid != os.geteuid():
                            continue
                        pidfd = os.pidfd_open(int(proc.name))
                        candidate.callback(os.close, pidfd)
                        if not owns_socket(proc, inodes) or not is_demo_process(proc):
                            continue
                    except (FileNotFoundError, PermissionError, ProcessLookupError):
                        continue
                    targets.enter_context(candidate.pop_all())
                    matches.append(pidfd)

        if len(matches) != 1:
            raise RuntimeError(f"No unique demo panel from this checkout is listening on {LOOPBACK}:{port}")
        pidfd, = matches
        signal.pidfd_send_signal(pidfd, signal.SIGTERM)
        exited = select.poll()
        exited.register(pidfd, select.POLLIN)
        if not exited.poll(40000):
            raise RuntimeError(f"Demo panel on {LOOPBACK}:{port} did not exit after SIGTERM")
    print(f"Stopped demo panel on {LOOPBACK}:{port}")


def stop_on_term(_signal, _frame):
    raise KeyboardInterrupt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    binaries = HERE.parent / "bin"
    parser.add_argument("--server-bin", type=Path, default=binaries / "kerniva-server")
    parser.add_argument("--replica-bin", type=Path, default=binaries / "kerniva-replica")
    parser.add_argument("--library", type=Path, default=binaries / "libkerniva_engine.so")
    parser.add_argument("--port", type=int, default=0, help="loopback dashboard port, 0 selects a free port")
    parser.add_argument("--workbench-url", default="http://127.0.0.1:18782/",
                        help="URL of the separate database client offered in the sidebar")
    parser.add_argument("--stop", action="store_true", help="stop the demo panel from this checkout on --port")
    args = parser.parse_args()
    client_url = urlsplit(args.workbench_url)
    if client_url.scheme not in ("http", "https") or not client_url.hostname or client_url.username or client_url.password:
        parser.error("--workbench-url requires an HTTP(S) address without credentials")
    if not sys.platform.startswith("linux"):
        parser.error("Run the demo inside Linux or WSL")
    if not 0 <= args.port <= 65535:
        parser.error("--port must be between 0 and 65535")
    if args.stop:
        if args.port == 0:
            parser.error("--stop requires the running panel's nonzero --port")
        stop_panel(args.port)
        return
    for name in ("server_bin", "replica_bin", "library"):
        path = getattr(args, name).resolve()
        if not path.is_file() or (name != "library" and not os.access(path, os.X_OK)):
            parser.error(f"Missing Linux build artifact: {path}")
        setattr(args, name, path)

    signal.signal(signal.SIGTERM, stop_on_term)
    with ThreadingHTTPServer((LOOPBACK, args.port), PanelHandler) as http, ExitStack() as held:
        reservations = [held.enter_context(socket.socket()) for _ in range(4)]
        for reservation in reservations:
            reservation.bind((LOOPBACK, 0))
        root = Path(tempfile.mkdtemp(prefix="kerniva-replica-demo-"))
        print(f"Demo data and logs: {root}", flush=True)
        demo = Demo(root, args, reservations)
        http.demo = demo
        try:
            demo.start()
            print(f"Open http://{LOOPBACK}:{http.server_port}/", flush=True)
            http.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            demo.close()


if __name__ == "__main__":
    try:
        main()
    except PROBE_ERRORS as error:
        raise SystemExit(f"Kerniva demo: {error}")
