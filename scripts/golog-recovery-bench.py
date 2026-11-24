#!/usr/bin/env python3
"""Measure and verify Go-log startup before and after one Rust snapshot."""

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import importlib.util
import math
import os
from pathlib import Path
import socket
import statistics
import subprocess
import sys
import tempfile
import time


SCRIPTS = Path(__file__).resolve().parent
REPO = SCRIPTS.parent
runner_spec = importlib.util.spec_from_file_location(
    "thesis_bench_linux", SCRIPTS / "thesis-bench-linux.py",
)
runner = importlib.util.module_from_spec(runner_spec)
runner_spec.loader.exec_module(runner)

PING_INTERVAL = 0.002
DELETE_EVERY = 10
VALUE_BYTES = 64
MAX_MEMORY_BYTES = 512 << 20


def key(index):
    return f"recovery:{index:08d}"


def value(index, version):
    return f"version={version:04d};key={index:08d}".ljust(VALUE_BYTES, "x")


def batches(indices, size):
    for offset in range(0, len(indices), size):
        yield indices[offset:offset + size]


class RESP:
    def __init__(self, connection):
        self.connection = connection
        self.reader = connection.makefile("rb")

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        self.reader.close()

    def command(self, *arguments):
        words = [argument.encode("ascii") for argument in arguments]
        request = f"*{len(words)}\r\n".encode("ascii")
        request += b"".join(f"${len(word)}\r\n".encode("ascii") + word + b"\r\n" for word in words)
        self.connection.sendall(request)
        return runner.read_resp(self.reader)


def configuration(data_dir, log_path, port, args):
    return {
        "host": "127.0.0.1",
        "port": port,
        "network_type": args.network,
        "db_count": 1,
        "storage_backend": "go-log",
        "max_memory": MAX_MEMORY_BYTES,
        "max_memory_policy": "noeviction",
        "persistence_enabled": True,
        "data_dir": str(data_dir),
        "cold_start_strategy": "load_all",
        "persistence_write_mode": "strong",
        "persistence_durability": "sync",
        "engine_segment_size": args.segment_bytes,
        "go_log_segment_age_seconds": 30,
        "log_level": "error",
        "log_path": str(log_path),
        "pprof_enabled": False,
    }


@contextmanager
def running_server(root, data_dir, label, args):
    attempt = root / label
    attempt.mkdir()
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    config = attempt / "config.json"
    runner.write_json(config, configuration(data_dir, attempt / "server.log", port, args))
    environment = {key: value for key, value in os.environ.items() if not key.startswith("KERNIVA_")}
    environment["KERNIVA_ENGINE_LIBRARY"] = str(args.library)
    console = attempt / "console.log"
    with console.open("w", encoding="utf-8") as log:
        start = time.perf_counter_ns()
        process = subprocess.Popen(
            [args.server, "-config", config], cwd=attempt, env=environment,
            stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT,
        )
        try:
            deadline = time.monotonic() + args.timeout
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    raise RuntimeError(f"Server exited with {process.returncode}; see {console}")
                try:
                    if runner.command(port, "PING", timeout=0.25) != "PONG":
                        raise RuntimeError(f"Unexpected PING response; see {console}")
                except (ConnectionRefusedError, TimeoutError):
                    time.sleep(PING_INTERVAL)
                    continue
                startup_ns = time.perf_counter_ns() - start
                break
            else:
                raise TimeoutError(f"Server did not answer PING; see {console}")

            info = runner.server_info(port)
            if info.get("storage_backend") != "go-log" or info.get("persistence_durability") != "sync":
                raise RuntimeError(f"Unexpected persistent server configuration; see {console}")
            with socket.create_connection(("127.0.0.1", port), timeout=args.timeout) as connection:
                with RESP(connection) as client:
                    yield client, startup_ns
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(args.timeout)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                    raise TimeoutError(f"Graceful shutdown did not finish; see {console}")
        if process.returncode != 0:
            raise RuntimeError(f"Server shutdown returned {process.returncode}; see {console}")


def seed(client, args):
    commits = 0
    for version in range(args.updates + 1):
        for batch in batches(range(args.keys), args.batch):
            entries = [word for index in batch for word in (key(index), value(index, version))]
            if client.command("MSET", *entries) != "OK":
                raise RuntimeError("MSET did not acknowledge the complete batch")
            commits += 1
    deleted = range(0, args.keys, DELETE_EVERY)
    for batch in batches(deleted, args.batch):
        if client.command("DEL", *(key(index) for index in batch)) != len(batch):
            raise RuntimeError("DEL did not clear the expected keys")
        commits += 1
    return commits


def verify(client, args):
    deleted = len(range(0, args.keys, DELETE_EVERY))
    size = client.command("DBSIZE")
    if size != args.keys - deleted:
        raise RuntimeError(f"DBSIZE reported {size}; expected {args.keys - deleted}")
    digest = hashlib.sha256()
    for batch in batches(range(args.keys), args.batch):
        names = [key(index) for index in batch]
        actual = client.command("MGET", *names)
        if not isinstance(actual, list) or len(actual) != len(names):
            raise RuntimeError("MGET did not return all requested positions")
        for index, name, entry in zip(batch, names, actual):
            expected = None if index % DELETE_EVERY == 0 else value(index, args.updates)
            if entry != expected:
                raise RuntimeError(f"{name} returned {entry!r}; expected {expected!r}")
            if entry is not None:
                digest.update(f"{name}\0{entry}\n".encode("ascii"))
    return {
        "live_keys": size, "deleted_keys_verified": deleted,
        "total_keys_checked": args.keys, "live_key_value_sha256": digest.hexdigest(),
    }


def inventory(data_dir):
    files = {}
    for path in sorted(data_dir.rglob("*")):
        if path.is_file():
            info = path.stat()
            files[str(path.relative_to(data_dir))] = {
                "bytes": info.st_size, "allocated_bytes": info.st_blocks * 512,
            }
    return {
        "files": files,
        "total_bytes": sum(file["bytes"] for file in files.values()),
        "total_allocated_bytes": sum(file["allocated_bytes"] for file in files.values()),
    }


def measure_restarts(root, data_dir, label, expected, args):
    attempts = []
    for trial in range(1, args.restarts + 1):
        with running_server(root, data_dir, f"{label}-{trial:02d}", args) as (client, startup_ns):
            verified = verify(client, args)
            if verified != expected:
                raise RuntimeError(f"Recovered state differs from seeded state in {label} trial {trial}")
        attempts.append({"trial": trial, "launch_to_ping_ns": startup_ns, "verified": verified})
    return {"trials": attempts,
            "median_launch_to_ping_ns": statistics.median(attempt["launch_to_ping_ns"] for attempt in attempts)}


def run_snapshot(root, storage_root, sequence, args):
    command = [args.maintainer, "--data-dir", storage_root, "--once"]
    with (root / "maintainer.log").open("w", encoding="utf-8") as log:
        start = time.perf_counter_ns()
        subprocess.run(command, cwd=root, stdout=log, stderr=subprocess.STDOUT,
                       stdin=subprocess.DEVNULL, timeout=args.timeout, check=True)
        elapsed = time.perf_counter_ns() - start
    database = storage_root / "db_0"
    cutoff = int((database / "CURRENT").read_text(encoding="ascii").strip())
    if cutoff != sequence:
        raise RuntimeError(f"Snapshot covered sequence {cutoff}; expected {sequence}")
    if any(database.glob("log-*.seg")):
        raise RuntimeError("Snapshot left covered sealed segments on disk")
    snapshot = database / f"snapshot-{cutoff:020d}.bin"
    return {
        "command": [str(argument) for argument in command], "cutoff": cutoff,
        "elapsed_ns": elapsed, "snapshot_bytes": snapshot.stat().st_size,
        "snapshot_sha256": runner.file_hash(snapshot),
    }


def parse_arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=Path.home() / "kerniva-recovery-results",
                        help="parent directory on WSL ext4 for a unique evidence directory")
    parser.add_argument("--server", type=Path, default=REPO / "bin" / "kerniva-server")
    parser.add_argument("--library", type=Path, default=os.environ.get(
        "KERNIVA_ENGINE_LIBRARY", REPO / "bin" / "libkerniva_engine.so"))
    parser.add_argument("--maintainer", type=Path, default=REPO / "bin" / "kerniva-maintainer")
    parser.add_argument("--keys", type=int, default=100_000)
    parser.add_argument("--updates", type=int, default=2, help="complete overwrite passes after initial seeding")
    parser.add_argument("--batch", type=int, default=128)
    parser.add_argument("--segment-bytes", type=int, default=4 << 20)
    parser.add_argument("--network", choices=("std", "gnet"), default="gnet")
    parser.add_argument("--restarts", type=int, default=3, help="verified restarts per measurement phase")
    parser.add_argument("--timeout", type=float, default=90, help="timeout per startup, shutdown or snapshot, in seconds")
    args = parser.parse_args()
    if min(args.keys, args.updates, args.batch, args.segment_bytes, args.restarts) < 1:
        parser.error("keys, updates, batch, segment bytes and restarts must be positive")
    if args.keys >= 100_000_000 or args.updates >= 10_000:
        parser.error("keys and updates must fit their fixed-width field")
    if not math.isfinite(args.timeout) or args.timeout <= 0:
        parser.error("timeout must be positive and finite")
    if sys.platform != "linux" or "microsoft" not in os.uname().release.lower():
        parser.error("run inside WSL 2 using native ext4 storage")
    for option in ("server", "library", "maintainer"):
        path = getattr(args, option).expanduser().resolve()
        if not path.is_file():
            parser.error(f"{option} does not exist: {path}")
        setattr(args, option, path)
    args.output_dir = args.output_dir.expanduser().resolve()
    if runner.filesystem_type(args.output_dir) != "ext4":
        parser.error("output directory must use WSL ext4")
    return args


def main():
    args = parse_arguments()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    root = Path(tempfile.mkdtemp(prefix=f"golog-recovery-{timestamp}-", dir=args.output_dir))
    data_dir = root / "data"
    data_dir.mkdir()
    storage_root = data_dir / "go-log-v1"
    print(f"Evidence directory {root}", flush=True)
    result = {
        "schema_version": 1, "status": "running", "started_utc": runner.utc_now(),
        "kernel": os.uname().release, "filesystem": runner.filesystem_type(data_dir),
        "run_directory": str(root), "clock": "perf_counter_ns",
        "method": ("Same data directory, sequential restarts before and after one Rust snapshot. "
                   "No OS cache flush. Startup ends at the first TCP PONG and excludes data verification."),
        "ping_retry_interval_seconds": PING_INTERVAL,
        "restarts_per_phase": args.restarts, "process_timeout_seconds": args.timeout,
        "workload": {
            "keys": args.keys, "overwrite_passes": args.updates, "batch_size": args.batch,
            "delete_every_nth_key": DELETE_EVERY, "value_bytes": VALUE_BYTES,
        },
        "network": args.network, "durability": "sync", "segment_bytes": args.segment_bytes,
        "artifacts": {name: {"path": str(getattr(args, name)), "sha256": runner.file_hash(getattr(args, name))}
                      for name in ("server", "library", "maintainer")},
    }
    evidence = root / "result.json"
    runner.write_json(evidence, result)
    try:
        with running_server(root, data_dir, "seed", args) as (client, _startup_ns):
            sequence = seed(client, args)
            expected = verify(client, args)
        result["seeded"] = {**expected, "last_sequence": sequence}
        database = storage_root / "db_0"
        if not any(database.glob("log-*.seg")) or (database / "CURRENT").exists():
            raise RuntimeError("Expected sealed logs and no snapshot after seeding")
        result["before_snapshot"] = measure_restarts(root, data_dir, "before", expected, args)
        result["before_snapshot"]["storage"] = inventory(data_dir)
        result["snapshot"] = run_snapshot(root, storage_root, sequence, args)
        result["after_snapshot"] = measure_restarts(root, data_dir, "after", expected, args)
        result["after_snapshot"]["storage"] = inventory(data_dir)
        result["status"] = "passed"
    except (Exception, KeyboardInterrupt) as error:
        result.update(status="failed", error=f"{type(error).__name__}: {error}")
        raise
    finally:
        result["finished_utc"] = runner.utc_now()
        runner.write_json(evidence, result)
    print(f"Recovery measurements {evidence}")


if __name__ == "__main__":
    main()
