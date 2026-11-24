"""Measure sequential full recovery with the source disconnected on Linux."""
import argparse
from contextlib import ExitStack
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import time

from golog_replica_snapshot_capacity import (
    DBS, LIGHT, MARKER_KEY, check, current, db_dir, digest, get, snapshot, value,
)


def verify(local, fixture, saved, port):
    statuses = {}
    for db, cursor in saved.items():
        code, body = check.http(port, "/status", db=db)
        if code != 200:
            raise RuntimeError(f"DB {db} status returned {code}")
        status = json.loads(body)
        if (not status["ready"] or status["installs"] != 0 or current(local, db) != cursor
                or any(status[key] != cursor[key] for key in ("source", "sequence", "snapshot"))
                or status["source"] != fixture["source"]):
            raise RuntimeError(f"DB {db} offline cursor differs")
        if db == LIGHT:
            reads = [(MARKER_KEY, b"fixed-link-snapshot-capacity-marker-4")]
        else:
            row = fixture["dbs"][db]
            file = db_dir(local, db) / cursor["generation"] / snapshot(cursor["snapshot"])
            if cursor["sequence"] != row["tail"] or digest(file) != row["snapshot_sha256"]:
                raise RuntimeError(f"DB {db} snapshot or sequence differs")
            reads = [("anchor", b"new"), ("tail", f"sealed tail {db:02d}".encode())]
            for index in (0, row["rows"] // 2, row["rows"] - 1):
                reads.append((f"bulk:{index:04d}", value(db, index).encode()))
            if get(port, db, "removed")[0] != 404:
                raise RuntimeError(f"DB {db} deletion missing")
        for key, expected in reads:
            if get(port, db, key) != (200, expected):
                raise RuntimeError(f"DB {db} value differs: {key}")
        statuses[db] = {key: status[key] for key in ("snapshot", "sequence", "source", "installs")}
    return statuses


def summarize_stages(trace):
    events = [json.loads(line) for line in trace.read_text().splitlines()]
    measured = {}
    for db in range(LIGHT):
        phases = {e["stage"]: e for e in events if e["kind"] == "startup" and e["db"] == db}
        measured[db] = {}
        for label, start, end in (("open", "rust_recovery_start", "rust_handle_ready"),
                                  ("iterate", "rust_handle_ready", "rust_rows_restored")):
            first, last = phases[start], phases[end]
            measured[db][label] = {
                "elapsed_ms": (last["monotonic_ns"] - first["monotonic_ns"]) / 1e6,
                "alloc_bytes": last["total_alloc_bytes"] - first["total_alloc_bytes"],
                "rss_delta_bytes": last["rss_bytes"] - first["rss_bytes"],
            }
        if phases["go_view_ready"]["monotonic_ns"] < phases["rust_rows_restored"]["monotonic_ns"]:
            raise RuntimeError(f"DB {db} startup stages out of order")
    return measured


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("replica", "lib", "fixture", "state", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    args = parser.parse_args()
    root = args.output.resolve()
    root.mkdir(exist_ok=False)
    fixture = json.loads((args.fixture / "fixture.json").read_text())
    local, trace = root / "local", root / "trace.jsonl"
    shutil.copytree(args.state, local)
    saved = {db: current(local, db) for db in range(LIGHT + 1)}
    (root / "cursors.json").write_text(json.dumps(saved))
    with ExitStack() as cleanup:
        # Binding without listening keeps the upstream unreachable until verification ends.
        upstream = cleanup.enter_context(socket.socket())
        upstream.bind(("127.0.0.1", 0))
        port = check.ports(1)[0]
        env = {**os.environ, "KERNIVA_ENGINE_LIBRARY": str(args.lib.resolve()),
               "KERNIVA_GOLOG_MEMORY_TRACE": str(trace)}
        log = cleanup.enter_context((root / "follower.log").open("w"))
        begin = time.monotonic_ns()
        process = subprocess.Popen([
            str(args.replica.resolve()), "follow", "--root", str(local), "--dbs", DBS,
            "--source", f"http://127.0.0.1:{upstream.getsockname()[1]}",
            "--listen", f"127.0.0.1:{port}", "--interval", "10s",
        ], env=env, stdout=log, stderr=log)
        cleanup.callback(check.stop, process)

        def ready():
            if process.poll() is not None:
                raise RuntimeError(f"follower exited early with status {process.returncode}")
            code, body = check.http(port, "/status", db=LIGHT)
            return code == 200 and json.loads(body)["ready"]

        check.wait_for("offline follower ready", ready, 60)
        elapsed_ms = (time.monotonic_ns() - begin) / 1e6
        counters = {}
        for line in Path(f"/proc/{process.pid}/status").read_text().splitlines():
            if line.startswith(("VmHWM:", "VmRSS:")):
                name, val = line.split(":")
                counters[name + "_kib"] = int(val.split()[0])
        statuses = verify(local, fixture, saved, port)
    if process.returncode != 0:
        raise RuntimeError(f"follower exited with status {process.returncode}")
    measured = summarize_stages(trace)
    result = dict(binary_sha256=digest(args.replica), source=fixture["source"], dbs=statuses,
                  since_launch_ms=elapsed_ms, proc_at_ready=counters, per_db=measured)
    for phase in ("open", "iterate"):
        result[phase + "_alloc_bytes"] = sum(row[phase]["alloc_bytes"] for row in measured.values())
    (root / "summary.json").write_text(json.dumps(result, indent=2) + "\n")
    print(root, {k: v for k, v in result.items() if k not in ("dbs", "per_db")}, flush=True)


if __name__ == "__main__":
    main()
