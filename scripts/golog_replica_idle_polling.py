#!/usr/bin/env python3
"""Measure steady no-update polling using direct follower-to-source requests."""
from contextlib import ExitStack
import json
import os
from pathlib import Path
import shutil
import signal
import struct
import argparse
import time

from golog_replica_snapshot_capacity import (
    DBS, LIGHT, MARKER_KEY, TIMEOUT, Experiment, check, config, current, digest, get,
    memory_process, require, rollup, save_result, value,
)



def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True, help="existing ext4 output directory")
    parser.add_argument("--origin", type=Path, required=True, help="previously converged snapshot-capacity window")
    parser.add_argument("--bin", type=Path, required=True, help="directory containing the source binary and Rust library")
    parser.add_argument("--follower", type=Path, required=True, help="follower executable to measure")
    parser.add_argument("--source-binary", type=Path, help="source executable, defaults to kerniva-replica in --bin")
    args = parser.parse_args()
    root, old, baseline, candidate = (p.resolve() for p in (args.root, args.origin, args.bin, args.follower))
    source_binary = args.source_binary.resolve() if args.source_binary else baseline / "kerniva-replica"
    require(config(["findmnt", "-n", "-o", "FSTYPE", "-T", root]), "ext4", "output filesystem")
    prior = json.loads((old / "result.json").read_text())
    require(prior["passed"], True, "origin trial")
    start = {db: current(old / "b", db) for db in range(LIGHT + 1)}
    expected = {db: cursor["sequence"] for db, cursor in start.items()}
    lineage = prior["fixture"]["source"]
    hashes = {str(path): digest(path) for path in (source_binary, candidate,
                                                  baseline / "libkerniva_engine.so", Path(__file__))}
    for index, interval in enumerate(("100ms", "1s", "1s", "100ms"), 1):
        work = root / f"{index}-{interval}"
        work.mkdir()
        for folder in ("b", "primary-data"):
            shutil.copytree(old / folder, work / folder)
        trace = work / "b-memory.jsonl"
        data = {"interval": interval, "origin": str(old), "artifacts_sha256": hashes,
                "source_request_count_available": False, "requested_idle_seconds": 15}
        with save_result(work, data), ExitStack() as context:
            run = Experiment(work, baseline, source_binary, work / "primary-data/go-log-v1", context)
            source = run.start("source", run.replica, "serve", "--root", run.storage,
                               "--listen", f"127.0.0.1:{run.source}")
            b = run.start("b", candidate, "follow", "--root", work / "b",
                          "--source", f"http://127.0.0.1:{run.source}", "--listen", f"127.0.0.1:{run.b}",
                          "--dbs", DBS, "--interval", interval, "--pull-idle-timeout", TIMEOUT,
                          "--snapshot-concurrency", "0",
                          env={**run.env, "KERNIVA_GOLOG_MEMORY_TRACE": str(trace)})
            data["pid"] = b.pid
            data["ports"] = run.ports

            def verify():
                statuses = run.wait("all DBs ready without errors", lambda: run.ready(run.b, expected))
                checks = {}
                cursors = {}
                for db, state in statuses.items():
                    require(state["source"], lineage, f"B DB {db} source")
                    cursor = cursors[db] = current(work / "b", db)
                    require(cursor, start[db], f"DB {db} CURRENT matches seed")
                    code, body = check.http(run.source, "/pull", db=db, after=expected[db], source=lineage)
                    require(code, 200, f"DB {db} direct source pull")
                    size, = struct.unpack(">I", body[:4])
                    manifest = json.loads(body[4:4 + size])
                    payload = body[4 + size:]
                    require((manifest["source"], manifest["db"], manifest["after"], manifest["last"]),
                            (lineage, db, expected[db], expected[db]), f"DB {db} unchanged source lineage")
                    require((manifest.get("files", []), payload), ([], b""), f"DB {db} direct source no data files")
                    checks[db] = {"status": code, "body_bytes": len(body), "payload_bytes": len(payload),
                                  "manifest": manifest}
                    if db == LIGHT:
                        require(get(run.b, db, MARKER_KEY), (200, prior["markers"][-1]["value"].encode()),
                                "light marker retained")
                    else:
                        require(get(run.b, db, "anchor"), (200, b"new"), "anchor retained")
                        require(get(run.b, db, "tail"), (200, f"sealed tail {db:02d}".encode()), "tail retained")
                        require(get(run.b, db, "bulk:0000"), (200, value(db, 0).encode()), "bulk row retained")
                        require(get(run.b, db, "removed")[0], 404, "deleted row absent")
                return {"statuses": statuses, "cursors": cursors, "source_checks": checks}

            def sample(index):
                before_ns = time.monotonic_ns()
                counters = memory_process(b.pid)
                b.send_signal(signal.SIGUSR1)

                def signal_record():
                    if not trace.exists():
                        return None
                    samples = [e for line in trace.read_text().split("\n")[:-1]
                               if (e := json.loads(line))["stage"] == "sample"]
                    if len(samples) != index:
                        return None
                    return samples[-1]

                event = run.wait(f"heap sample {index}", signal_record, 5)
                require(event["monotonic_ns"] >= before_ns, True, "heap signal precedes sample")
                return {"clock_before_ns": before_ns, "heap": event, "proc": counters, "rollup_kib": rollup(b.pid)}

            data["before"] = verify()
            time.sleep(1)
            first = sample(1)
            deadline = first["heap"]["monotonic_ns"] + 15_000_000_000
            time.sleep(max(0, (deadline - time.monotonic_ns()) / 1e9))
            last = sample(2)
            data["samples"] = [first, last]
            data["after"] = verify()
            elapsed = (last["heap"]["monotonic_ns"] - first["heap"]["monotonic_ns"]) / 1e9
            allocated = last["heap"]["total_alloc_bytes"] - first["heap"]["total_alloc_bytes"]
            data.update(actual_seconds=elapsed, total_alloc_delta_bytes=allocated,
                        total_alloc_mib_per_second=allocated / (1 << 20) / elapsed,
                        cpu_ticks=last["proc"]["cpu_ticks"] - first["proc"]["cpu_ticks"],
                        cpu_ticks_per_second=os.sysconf("SC_CLK_TCK"))
            check.stop(b)
            data["b_exit_status"] = b.returncode
            require(b.returncode, 0, "B graceful exit")
            check.stop(source)
            data["source_exit_status"] = source.returncode
            require(source.returncode, 0, "source graceful exit")
            data["passed"] = True
        print(interval, elapsed, data["total_alloc_mib_per_second"], data["cpu_ticks"], flush=True)


if __name__ == "__main__":
    main()
