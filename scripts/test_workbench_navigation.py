"""Run the database-navigation browser journey against a temporary Kerniva and workbench."""

import argparse
import base64
from contextlib import contextmanager
import json
import os
from pathlib import Path
import socket
import queue
import threading
import subprocess
import sys
from tempfile import TemporaryDirectory

from test_workbench_integration import API, OPENING_LINE, WORKBENCH, ports_closed, require, wait_for
from workbench_redis import _Client


@contextmanager
def workbench(args, root):
    shell = [args.service.resolve()] if args.service else [sys.executable, "-u", "-B", WORKBENCH]
    with (root / "workbench.log").open("wb") as log:
        web = subprocess.Popen([*shell, "--port", "0", "--parent-stdin", "--config", root / "profiles.json"],
                               cwd=root, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=log,
                               env={**os.environ, "PYTHONUTF8": "1"}, text=True, encoding="utf-8")
        try:
            lines = queue.Queue()
            threading.Thread(target=lambda: lines.put(web.stdout.readline()), daemon=True).start()
            try:
                opening = OPENING_LINE.fullmatch(lines.get(timeout=15).strip())
            except queue.Empty:
                raise RuntimeError("Workbench startup timed out") from None
            require(opening, "Missing workbench startup URL")
            yield opening.group(1)
        finally:
            web.stdin.close()
            try:
                web.wait(timeout=12)
            except subprocess.TimeoutExpired:
                web.kill()
                web.wait()
                raise RuntimeError("Workbench did not stop on stdin EOF")


def journey(args, fixture):
    script = WORKBENCH.with_name("test_workbench_browser.mjs")
    child = subprocess.run(["node", script, args.browser.resolve(), "-"], input=json.dumps(fixture),
                           capture_output=True, text=True, encoding="utf-8", timeout=120)
    require(child.returncode == 0, child.stderr.strip() or "Browser navigation failed")
    return json.loads(child.stdout)


def exercise(args, root, evidence):
    with socket.socket() as allocation:
        allocation.bind(("127.0.0.1", 0))
        db_port = allocation.getsockname()[1]
    config = root / "server.yaml"
    config.write_text(f'host: "127.0.0.1"\nport: {db_port}\nnetwork_type: "std"\ndb_count: 2\n'
                      'persistence_enabled: false\nlog_path: "./server.log"\n', encoding="utf-8")
    stop = root / "stop"
    with (root / "server-output.log").open("wb") as db_log:
        db = subprocess.Popen([args.server.resolve(), "-config", config, "-stop-file", stop],
                              cwd=root, stdout=db_log, stderr=db_log)
        try:
            wait_for(lambda: not ports_closed([db_port]), "isolated Kerniva")
            for index, value in enumerate(("zero-only-value", "one-only-value")):
                with _Client({"host": "127.0.0.1", "port": db_port, "db": index}) as client:
                    require(client.at(client.seed, "SET", "shared", value) == b"OK", "Seed shared key")
                    for key in range(8):
                        client.at(client.seed, "SET", f"db{index}:item{key}", value)
            with workbench(args, root) as origin:
                api = API(origin)
                profiles = []
                for db_index in (0, 1):
                    profiles.append(api("profile/save", {"profile": {
                        "name": f"DB {db_index}", "mode": "standalone", "db": db_index,
                        "host": "127.0.0.1", "port": db_port,
                    }})["profile"])
                api("settings", {"settings": {"pageSize": 1}})
                report = journey(args, {"origin": origin, "profileId": profiles[0]["id"],
                                        "evidence": str(evidence), "navigation": True})
        finally:
            stop.touch()
            try:
                db.wait(timeout=10)
            except subprocess.TimeoutExpired:
                db.kill()
                db.wait()
                raise RuntimeError("Kerniva did not stop on its stop file")
    require(ports_closed([db_port, int(origin.rsplit(":", 1)[1])]), "Fixture listener survived")
    return {**report, "cleanShutdown": True}


def cluster_journey(args, root, evidence):
    wsl = ["wsl.exe", "-d", args.distro, "-u", args.user, "--exec"]
    fixture = subprocess.check_output([*wsl, "wslpath", "-a", str(WORKBENCH.with_name("test_workbench_cluster_fixture.py"))],
                                      stderr=subprocess.DEVNULL, text=True, encoding="utf-8", timeout=6).strip()
    with (evidence / "fixture.stderr").open("wb") as log:
        child = subprocess.Popen([*wsl, "python3", "-u", "-B", fixture, "--lock", args.lock],
                                 stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=log, text=True, encoding="utf-8")
        try:
            record = child.stdout.readline()
            require(record, "Fixture did not acquire the lock or start the cluster")
            cluster = json.loads(record)
            require(len(cluster.get("nodes", {})) == 3, "Incomplete cluster fixture")
            with workbench(args, root) as origin:
                api = API(origin)
                profile = api("profile/save", {"profile": {"name": "Cluster ACL", "host": "127.0.0.1",
                              "port": cluster["seed"], "db": 0, "mode": "cluster",
                              "username": "exact" if args.pin_cluster else "viewer"}})["profile"]
                api("settings", {"settings": {"pageSize": 1}})
                if args.pin_cluster:
                    token = api("connect", {"id": profile["id"], "password": cluster["password"]})["session"]
                    for node in [*cluster["nodes"].values(), {"key": "forbidden:demo"}]:
                        key = base64.b64encode(node["key"].encode()).decode("ascii")
                        api("pins/add", {"session": token, "profileId": profile["id"], "db": 0, "key_b64": key})
                    api("disconnect", {"session": token})
                    cluster["exactOnly"] = True
                report = journey(args, {"origin": origin, "profileId": profile["id"],
                                        "cluster": cluster, "evidence": str(evidence)})
                require(cluster["password"] not in (root / "profiles.json").read_text(encoding="utf-8"),
                        "GUI retained the ephemeral ACL credential")
        finally:
            child.stdin.close()
            child.wait(timeout=18)
        exits = json.loads(child.stdout.readline())["exits"]
        require(child.returncode == 0 and exits == [0, 0, 0], "Redis Cluster fixture did not exit cleanly")
        require(ports_closed([int(origin.rsplit(":", 1)[1])]), "Workbench listener survived")
        return {**report, "passwordPersisted": False, "redisExits": exits, "cleanShutdown": True}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--server", type=Path, help="Kerniva server binary for standalone DB navigation")
    mode.add_argument("--cluster", action="store_true", help="Start three temporary WSL Redis primaries with ACL")
    parser.add_argument("--distro", help="WSL distribution for the Redis Cluster fixture")
    parser.add_argument("--user", help="WSL user for the Redis Cluster fixture")
    parser.add_argument("--lock", help="Absolute WSL path for the shared nonblocking CPU lock")
    parser.add_argument("--pin-cluster", action="store_true", help="Exercise saved keys using a restricted exact-read ACL")
    parser.add_argument("--browser", type=Path, required=True, help="Chromium or Edge (Node 22+)")
    parser.add_argument("--service", type=Path, help="Optional packaged workbench service")
    parser.add_argument("--evidence", type=Path, required=True, help="New screenshot and result directory")
    args = parser.parse_args()
    if args.cluster and not (args.distro and args.user and args.lock):
        parser.error("--cluster requires --distro, --user and --lock")
    if args.pin_cluster and not args.cluster:
        parser.error("--pin-cluster requires --cluster")
    evidence = args.evidence.resolve()
    evidence.mkdir(parents=True, exist_ok=False)
    try:
        with TemporaryDirectory(prefix="kerniva-navigation-") as root:
            scenario = cluster_journey if args.cluster else exercise
            report = scenario(args, Path(root), evidence)
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        report = {"passed": False, "error": str(error)}
    result = json.dumps(report, ensure_ascii=False, indent=2)
    (evidence / "result.json").write_text(result + "\n", encoding="utf-8")
    print(result)
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
