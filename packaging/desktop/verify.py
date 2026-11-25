#!/usr/bin/env python3
"""Exercise an unpacked workbench and a disposable native Kerniva database."""

import argparse
import base64
from contextlib import ExitStack
import hashlib
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
from urllib.request import ProxyHandler, Request, build_opener


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def database(stack, root, server):
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]
    config = root / "db.json"
    config.write_text(json.dumps({"host": "127.0.0.1", "port": port, "network_type": "std",
                      "db_count": 2, "persistence_enabled": False, "data_dir": str(root / "db"),
                      "log_path": str(root / "database.log")}), encoding="utf-8")
    stop_file = root / "stop"
    log = stack.enter_context((root / "database-console.log").open("wb"))
    env = {key: value for key, value in os.environ.items() if not key.startswith("KERNIVA_")}
    if sys.platform.startswith("linux"):
        env["LD_LIBRARY_PATH"] = os.pathsep.join(filter(None, [str(server.parent), env.get("LD_LIBRARY_PATH")]))
    proc = subprocess.Popen([str(server), "-config", str(config), "-stop-file", str(stop_file)],
                            env=env, cwd=root, stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT)

    def stop():
        stop_file.touch()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
            raise RuntimeError("test database did not stop on its stop file")
        require(proc.returncode == 0, f"database exit {proc.returncode}")
    stack.callback(stop)
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline and proc.poll() is None:
        try:
            return socket.create_connection(("127.0.0.1", port), timeout=1), port
        except OSError:
            time.sleep(.1)
    raise RuntimeError("test database did not listen: " +
                       (root / "database-console.log").read_text(encoding="utf-8", errors="replace")[-600:])


def seed(connection, key, value):
    with connection, connection.makefile("rb") as reader:
        commands = [((b"SELECT", b"1"), b"+OK"), ((b"SET", key, value), b"+OK"),
                    ((b"HSET", b"packaging:hash", b"field", value), b":1")]
        for words, expected in commands:
            wire = b"*%d\r\n" % len(words) + b"".join(b"$%d\r\n" % len(w) + w + b"\r\n" for w in words)
            connection.sendall(wire)
            require(reader.readline() == expected + b"\r\n", "unexpected database reply to fixture write")


def service(stack, root, helper, config, env, arguments=()):
    log = stack.enter_context((root / "backend-stderr.log").open("ab"))
    proc = subprocess.Popen([str(helper), *arguments, "--port", "0", "--config", str(config), "--parent-stdin"],
                            cwd=root, env=env, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=log)
    ready = queue.Queue()
    def read_stdout():
        with proc.stdout:
            for raw in proc.stdout:
                if match := re.fullmatch(rb"Open (http://127\.0\.0\.1:[0-9]+)/", raw.strip()):
                    ready.put(match[1].decode("ascii"))
            ready.put(None)
    reader = threading.Thread(target=read_stdout, daemon=True)
    reader.start()
    def stop():
        proc.stdin.close()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
            raise RuntimeError("packaged service did not stop on parent EOF")
        reader.join(timeout=2)
        require(proc.returncode == 0, f"packaged service exit {proc.returncode}")
    stack.callback(stop)
    origin = ready.get(timeout=40)
    require(origin, "packaged service did not open: " +
            (root / "backend-stderr.log").read_text(encoding="utf-8", errors="replace")[-600:])
    return origin


def verify(bundle, server):
    mac = bundle.suffix == ".app"
    assets = bundle / "Contents/Resources" if mac else bundle
    programs = bundle / "Contents/MacOS" if mac else bundle
    windows = os.name == "nt"
    helper = assets / "backend" / ("kerniva-workbench-service.exe" if windows else "kerniva-workbench-service")
    gui = programs / ("Kerniva Workbench.exe" if windows else "kerniva-workbench")
    tui = programs / ("Kerniva-TUI.cmd" if windows else "kerniva-tui")
    for path in [helper, gui, tui]:
        require(path.is_file(), f"missing package file {path}")
    opener = build_opener(ProxyHandler({}))
    with tempfile.TemporaryDirectory(prefix="kerniva package 核验 ") as name, ExitStack() as stack:
        root = Path(name)
        connection, port = database(stack, root, server)
        key, value = "packaging:键".encode(), b"\x00Kerniva\xff"
        seed(connection, key, value)
        env = {k: v for k, v in os.environ.items() if k not in {"PYTHONPATH", "PYTHONHOME", "VIRTUAL_ENV"}}
        empty = root / "empty-path"
        empty.mkdir()
        env["PATH"] = str(Path(os.environ["SystemRoot"]) / "System32") if windows else str(empty)
        env["APPDATA"] = env["XDG_CONFIG_HOME"] = str(root)
        config = root / "settings.json"
        with ExitStack() as running:
            origin = service(running, root, helper, config, env)
            def fetch(path, body=None):
                request = Request(origin + path, headers={"Content-Type": "application/json", "Origin": origin},
                                  data=json.dumps(body).encode() if body is not None else None)
                with opener.open(request, timeout=8) as response:
                    return response.read()
            def api(path, body=None):
                return json.loads(fetch("/api/" + path, body))
            require(api("state")["apiVersion"] == 2, "wrong workbench API")
            ui = Path(__file__).resolve().parents[2] / "scripts/workbench-ui"
            assets_sha = {}
            for path, filename in [("/", "index.html"), ("/app.js", "app.js"), ("/ops.js", "ops.js"),
                                   ("/instances.js", "instances.js"), ("/instances.css", "instances.css"),
                                   ("/styles.css", "styles.css"), ("/api/replication/experiments", "replication-experiments.json")]:
                data = fetch(path)
                source = (ui / filename).read_bytes()
                matches = json.loads(data) == json.loads(source) if filename.endswith(".json") else data == source
                require(matches, f"packaged asset differs from checked-out source: {filename}")
                assets_sha[path] = hashlib.sha256(data).hexdigest()
            profile = api("profile/save", {"profile": {"name": "解包验收", "host": "127.0.0.1", "port": port,
                                                       "mode": "standalone", "db": 1}})["profile"]
            session = api("connect", {"id": profile["id"]})["session"]
            status = api("status", {"session": session})
            require(status["db"] == 1 and status["mode"] == "standalone", "wrong database session")
            scanned = api("scan", {"session": session, "pattern": "packaging:*", "count": 20})
            encoded = base64.b64encode(key).decode()
            require(any(item["key_b64"] == encoded for item in scanned["items"]), "SCAN missed fixture key")
            detail = api("key", {"session": session, "key_b64": encoded})
            require(detail["type"] == "string" and base64.b64decode(detail["value"]["b64"]) == value,
                    "inspector returned incorrect raw bytes")
            h = api("key", {"session": session, "key_b64": base64.b64encode(b"packaging:hash").decode()})
            require(h["type"] == "hash" and len(h["items"]) == 1 and h["items"][0]["field"]["text"] == "field"
                    and base64.b64decode(h["items"][0]["value"]["b64"]) == value, "incorrect hash field or value")
            api("select-db", {"session": session, "db": 0})
            require(api("key", {"session": session, "key_b64": encoded})["type"] == "none", "DB 0 leaked DB 1 key")
            api("disconnect", {"session": session})
        with ExitStack() as restarted:
            origin = service(restarted, root, helper, config, env)
            with opener.open(origin + "/api/state", timeout=5) as response:
                state = json.loads(response.read())
            require(profile in state["profiles"], "saved profile not recovered on restart")
        return {"passed": True, "bundle": bundle.name, "host": sys.platform, "database": server.name,
                "database_port": port, "selected_db": 1, "read_key_b64": encoded, "read_value_b64": detail["value"]["b64"],
                "hash_field": "field", "db0_key_absent": True,
                "static_sha256": assets_sha, "restarted_profile": True, "service_eof_exits": [0, 0],
                "service_path_environment": env["PATH"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle-dir", type=Path, required=True, help="unpacked application directory or .app")
    parser.add_argument("--server", type=Path, required=True, help="native kerniva-server used only for this test")
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    result = json.dumps(verify(args.bundle_dir.resolve(strict=True), args.server.resolve(strict=True)), indent=2)
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(result + "\n", encoding="utf-8")
    print(result)


if __name__ == "__main__":
    main()
