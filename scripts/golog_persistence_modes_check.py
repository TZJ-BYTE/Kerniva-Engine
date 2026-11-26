"""Exercise Go persistence modes over TCP in a new, isolated directory."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import time
import traceback


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def digest(path):
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def wait_for(check, description, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = check()
        if value:
            return value
        time.sleep(0.025)
    raise TimeoutError(description)


def command(port, *args):
    payload = [arg if isinstance(arg, bytes) else str(arg).encode() for arg in args]
    request = b"*%d\r\n" % len(payload)
    request += b"".join(b"$%d\r\n%s\r\n" % (len(arg), arg) for arg in payload)
    with socket.create_connection(("127.0.0.1", port), timeout=3) as connection:
        connection.sendall(request)
        with connection.makefile("rb") as reader:
            line = reader.readline()
            require(line.endswith(b"\r\n"), f"truncated reply for {args[0]}")
            prefix, value = line[:1], line[1:-2]
            if prefix == b"-":
                raise RuntimeError(f"{args[0]} failed: {value.decode(errors='replace')}")
            if prefix == b"$":
                size = int(value)
                if size == -1:
                    return None
                data = reader.read(size + 2)
                require(len(data) == size + 2 and data.endswith(b"\r\n"), "truncated bulk reply")
                return data[:-2].decode()
            require(prefix in (b"+", b":"), f"unexpected reply {line!r}")
            return value.decode() if prefix == b"+" else int(value)


class Case:
    def __init__(self, folder, options, evidence):
        self.folder = folder
        self.folder.mkdir()
        self.data = folder / "data"
        self.options = options
        self.evidence = evidence
        self.environment = {key: value for key, value in os.environ.items() if not key.startswith("KERNIVA_")}
        self.process = None
        self.port = None

    def start(self, label, wal, snapshot, interval=3600, expect_failure=False):
        require(self.process is None, "previous server active")
        stage = self.folder / label
        stage.mkdir()
        logs = stage / "logs"
        logs.mkdir()
        self.stage = stage
        self.stop_file = stage / "stop.request"
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            self.port = reservation.getsockname()[1]
        config = {
            "host": "127.0.0.1", "port": self.port, "network_type": self.options.network,
            "db_count": 2, "storage_backend": "go-log", "max_memory_policy": "noeviction",
            "persistence_enabled": True, "cold_start_strategy": "load_all",
            "persistence_write_mode": "strong", "persistence_durability": "sync",
            "data_dir": str(self.data), "engine_segment_size": 4096,
            "go_log_enabled": wal, "snapshot_enabled": snapshot,
            "snapshot_interval_seconds": interval, "log_path": str(logs / "server.log"),
        }
        config_path = stage / "config.json"
        config_path.write_text(json.dumps(config, indent=2), encoding="utf-8")
        argv = [str(self.options.server), "-config", str(config_path), "-stop-file", str(self.stop_file)]
        environment = dict(self.environment)
        library = self.options.library if wal else stage / "missing-kerniva-engine"
        if library is not None:
            environment["KERNIVA_ENGINE_LIBRARY"] = str(library)
        with (logs / "console.log").open("wb") as output:
            self.process = subprocess.Popen(argv, cwd=stage, env=environment, stdout=output,
                                            stderr=subprocess.STDOUT)
        details = {"pid": self.process.pid, "argv": argv, "port": self.port, "config": config, "library": str(library)}
        self.evidence[label] = details
        if expect_failure:
            details["exit"] = self.process.wait(timeout=10)
            require(details["exit"] != 0, f"{label} unexpectedly started successfully")
            details["console"] = (logs / "console.log").read_text(encoding="utf-8", errors="replace")
            self.process = None
            return details["console"]

        def ready():
            code = self.process.poll()
            require(code is None, f"{label} exited early with {code}; see {logs}")
            try:
                return command(self.port, "PING") == "PONG"
            except (OSError, RuntimeError):
                return False

        wait_for(ready, f"{label} failed to listen")
        raw = command(self.port, "INFO")
        fields = dict(line.split(":", 1) for line in raw.splitlines() if ":" in line and not line.startswith("#"))
        details["info"] = raw
        flags = {"persistence_enabled": wal or snapshot, "go_log_enabled": wal, "snapshot_enabled": snapshot}
        for key, value in flags.items():
            expected = str(value).lower()
            require(fields.get(key) == expected, f"{label} INFO {key}={fields.get(key)!r}, expected {expected!r}")
        if wal or snapshot:
            expected_dir = self.data / ("go-log-v1" if wal else "go-snapshot-v1")
            require(Path(fields["data_dir"]) == expected_dir, f"{label}: INFO data_dir does not match configuration")

    def request(self, *args, expected):
        response = command(self.port, *args)
        display = [f"<{len(arg)} bytes>" if isinstance(arg, bytes) else arg for arg in args]
        detail = self.evidence[self.stage.name]
        detail.setdefault("checks", []).append({"args": display, "response": response, "expected": expected})
        require(response == expected, f"{display}: got {response!r}, expected {expected!r}")

    def stop(self, crash=False, cleanup=False):
        if self.process is None:
            return
        try:
            if self.process.poll() is None:
                if crash or cleanup:
                    self.process.kill()
                else:
                    self.stop_file.touch()
            code = self.process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=5)
            raise
        finally:
            self.process = None
        details = self.evidence[self.stage.name]
        details["stop"] = {"method": "cleanup" if cleanup else ("kill" if crash else "stop-file"), "exit": code}
        if not cleanup:
            require((code != 0) == crash, f"{self.stage.name} unexpected exit code {code}")

    def disk_files(self):
        return sorted(path.relative_to(self.data).as_posix() for path in self.data.rglob("*") if path.is_file())

    def snapshot_names(self):
        return sorted(path.name for path in (self.data / "go-snapshot-v1").glob("snapshot-*.bin"))

    def rust_snapshot_files(self):
        log_dir = self.data / "go-log-v1"
        return {path.relative_to(log_dir).as_posix(): digest(path) for path in sorted(log_dir.rglob("*"))
                if path.is_file() and (path.name == "CURRENT" or path.match("snapshot-*.bin"))}

    def runtime_state(self):
        state = self.data / ".go-persistence-mode-v1"
        return state.read_text(encoding="utf-8").strip() if state.exists() else None

    def run_maintainer(self, label):
        if self.options.maintainer is None:
            return False
        argv = [str(self.options.maintainer), "--data-dir", str(self.data / "go-log-v1"),
                "--once", "--min-sealed-bytes", "1", "--max-age", "0"]
        before = self.rust_snapshot_files()
        policy = (self.data / "go-log-v1" / ".snapshot-maintenance").read_text(encoding="ascii")
        proc = subprocess.run(argv, cwd=self.folder, env=self.environment, capture_output=True, timeout=20)
        details = {
            "argv": argv, "exit": proc.returncode, "stdout": proc.stdout.decode(errors="replace"),
            "stderr": proc.stderr.decode(errors="replace"), "policy": policy, "before": before,
            "after": self.rust_snapshot_files(),
        }
        self.evidence[label] = details
        require(proc.returncode == 0, f"{label} failed: {details['stderr']}")
        return True


def memory_only(case):
    case.start("write", False, False)
    case.request("SET", "recovery", "memory", expected="OK")
    case.stop(crash=True)
    case.start("restart", False, False)
    case.request("GET", "recovery", expected=None)
    case.stop()
    case.evidence["disk"] = case.disk_files()
    require(not case.data.exists(), "disabled persistence created data directory")


def seal_wal(case):
    for index in range(3):
        case.request("SET", f"padding-{index}", b"p" * 3000, expected="OK")
    files = case.disk_files()
    case.evidence["sealed_wal"] = [name for name in files if name.endswith(".seg")]
    require(case.evidence["sealed_wal"], "no sealed WAL data for maintenance proof")


def wal_only(case):
    case.start("write", True, False)
    case.request("SET", "recovery", "durable-wal", expected="OK")
    seal_wal(case)
    case.stop(crash=True)
    if case.run_maintainer("forced_maintainer"):
        proof = case.evidence["forced_maintainer"]
        require(proof["policy"] == "0" and proof["before"] == proof["after"] == {},
                "WAL-only Rust maintenance was enabled or published a snapshot")
    case.start("restart", True, False)
    case.request("GET", "recovery", expected="durable-wal")
    case.stop()
    rejection = case.start("unsafe_switch", False, True, expect_failure=True)
    require("WAL is authoritative" in rejection, "direct WAL-to-snapshot switch lacked expected rejection")
    case.evidence["disk"] = case.disk_files()
    require(not case.snapshot_names(), "WAL-only created standalone snapshots")


def snapshot_only(case):
    case.start("write", False, True, interval=1)
    case.request("SET", "recovery", "published", expected="OK")
    initial = case.snapshot_names()

    def next_generation():
        names = case.snapshot_names()
        return names if names and names != initial else None

    published = wait_for(next_generation, "snapshot-only did not publish a new generation")
    case.evidence["before_late_ack"] = published
    case.request("SET", "recovery", "late-ack", expected="OK")
    case.stop(crash=True)
    case.evidence["after_kill"] = case.snapshot_names()
    require(case.evidence["after_kill"] == published, "another snapshot raced the late-write crash")
    case.start("restart", False, True)
    case.request("GET", "recovery", expected="published")
    case.stop()
    require(not (case.data / "go-log-v1").exists(), "snapshot-only created a WAL")

    case.start("import_wal", True, False)
    case.request("GET", "recovery", expected="published")
    case.request("SET", "recovery", "imported-wal", expected="OK")
    case.stop(crash=True)
    case.start("import_wal_restart", True, False)
    case.request("GET", "recovery", expected="imported-wal")
    case.stop()
    case.evidence["disk"] = case.disk_files()


def combined(case):
    case.start("write", True, True, interval=1)
    case.request("SET", "recovery", "published", expected="OK")
    seal_wal(case)
    published = wait_for(case.snapshot_names, "combined mode did not publish a snapshot")
    case.evidence["before_late_ack"] = published
    case.request("SET", "recovery", "latest-wal", expected="OK")
    case.stop(crash=True)
    require(case.snapshot_names() == published, "another snapshot raced the late-write crash")
    if case.run_maintainer("positive_maintenance_control"):
        proof = case.evidence["positive_maintenance_control"]
        require(proof["policy"] in ("", "1") and not proof["before"]
                and any(name.endswith("/CURRENT") for name in proof["after"]),
                "maintenance did not publish a Rust snapshot in combined mode")
    case.start("restart", True, True)
    case.request("GET", "recovery", expected="latest-wal")
    case.request("SET", "recovery", "handoff", expected="OK")
    case.stop()
    case.evidence["handoff_state"] = case.runtime_state()
    require(case.evidence["handoff_state"].startswith("handoff "), "combined shutdown did not mark handoff")
    wal_dir = case.data / "go-log-v1"
    before = {file.relative_to(wal_dir).as_posix(): digest(file) for file in wal_dir.rglob("*")
              if file.is_file() and file.name != ".snapshot-maintenance"}
    case.start("handoff", False, True)
    case.request("GET", "recovery", expected="handoff")
    case.request("SET", "recovery", "snapshot-authority", expected="OK")
    case.stop()
    after = {file.relative_to(wal_dir).as_posix(): digest(file) for file in wal_dir.rglob("*")
             if file.is_file() and file.name != ".snapshot-maintenance"}
    case.evidence["wal_before_handoff"] = before
    case.evidence["wal_after_handoff"] = after
    require(before == after, "snapshot-only changed retained WAL data")
    if case.run_maintainer("handoff_disabled_maintainer"):
        proof = case.evidence["handoff_disabled_maintainer"]
        require(proof["policy"] == "0" and proof["before"] == proof["after"],
                "snapshot-only Rust maintenance changed retained snapshots")
    case.start("handoff_restart", False, True)
    case.request("GET", "recovery", expected="snapshot-authority")
    case.stop()
    rejection = case.start("stale_wal_switch", True, False, expect_failure=True)
    require("snapshots are authoritative" in rejection, "stale WAL switch lacked expected rejection")
    preserved = case.folder / "preserved-go-log-v1"
    wal_dir.rename(preserved)
    case.start("handoff_import_wal", True, False)
    case.request("GET", "recovery", expected="snapshot-authority")
    case.request("SET", "recovery", "migrated-wal", expected="OK")
    case.stop(crash=True)
    case.start("handoff_import_restart", True, False)
    case.request("GET", "recovery", expected="migrated-wal")
    case.stop()
    case.evidence["disk"] = case.disk_files()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", required=True, type=Path)
    parser.add_argument("--maintainer", type=Path)
    parser.add_argument("--library", type=Path)
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--network", required=True, choices=("std", "gnet"))
    options = parser.parse_args()
    options.server = options.server.resolve(strict=True)
    options.maintainer = options.maintainer.resolve(strict=True) if options.maintainer else None
    options.library = options.library.resolve(strict=True) if options.library else None
    root = options.root.resolve()
    root.mkdir(parents=True, exist_ok=False)
    result = {"network": options.network, "root": str(root), "cases": {},
              "script_sha256": digest(Path(__file__)),
              "library": {"path": str(options.library), "sha256": digest(options.library)}
              if options.library else None,
              "server": {"path": str(options.server), "sha256": digest(options.server)},
              "maintainer": {"path": str(options.maintainer), "sha256": digest(options.maintainer)}
              if options.maintainer else None}
    for scenario in (memory_only, wal_only, snapshot_only, combined):
        evidence = result["cases"][scenario.__name__] = {}
        case = Case(root / scenario.__name__, options, evidence)
        try:
            scenario(case)
            evidence["pass"] = True
        except Exception:
            evidence["pass"] = False
            evidence["error"] = traceback.format_exc()
        finally:
            case.stop(cleanup=True)
    result["pass"] = all(case["pass"] for case in result["cases"].values())
    result["rust_maintenance_tested"] = options.maintainer is not None
    rendered = json.dumps(result, ensure_ascii=False, indent=2)
    (root / "result.json").write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    return 0 if result["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
