#!/usr/bin/env python3
"""Measure effective sealed-delta install rate on an unpaced HTTP loopback source."""

import argparse
from contextlib import ExitStack
from hashlib import sha256
import json
from pathlib import Path
import shutil
import time

from golog_replica_poll_ordinary import filesystem, pull
from golog_replica_poll_phase import Trace
from golog_replica_snapshot_capacity import (
    Experiment, check, current, db_dir, digest, get, require, save_result,
)

DB = 15
KEYS = 256
VALUE_BYTES = 256 << 10
ORDER = ("baseline", "candidate", "candidate", "baseline",
         "candidate", "baseline", "baseline", "candidate")


def record(index):
    key = f"reader:delta:{index:04d}"
    block = sha256(key.encode()).hexdigest()
    return key, block * (VALUE_BYTES // len(block))


def prepare(run, seed, before, already_sealed):
    storage = seed / "primary-data/go-log-v1"
    run.storage.mkdir(parents=True)
    shutil.copy2(storage / ".replica-source-id", run.storage)
    shutil.copytree(storage / f"db_{DB}", run.storage / f"db_{DB}")
    run.start_primary()
    run.start_source()
    run.wait("source listener ready", lambda: "listening on" in (run.root / "source.log").read_text())
    if not already_sealed:
        run.write(DB, ((("SET", *record(i)), "OK") for i in range(KEYS)))
    end = before["sequence"] + KEYS
    files = run.storage / f"db_{DB}"
    run.wait("last delta record sealed", lambda: list(files.glob(f"log-*-{end:020d}.seg")))
    manifest, body = pull(run, DB, before["sequence"], before["source"])
    require((manifest["last"], manifest.get("snapshot", 0), manifest.get("rebase", False)),
            (end, 0, False), "delta manifest")
    expected = {}
    offset = 0
    for item in manifest["files"]:
        path = files / item["name"]
        size = item["size"]
        checksum = digest(path)
        require(sha256(memoryview(body)[offset:offset + size]).hexdigest(), checksum, "source payload")
        expected[path.name] = {"bytes": size, "sha256": checksum}
        offset += size
    require((offset == len(body), offset >= KEYS * VALUE_BYTES), (True, True), "source payload size")
    return manifest, expected


def window(root, run, candidate, origin, manifest, files, expected_reads):
    root.mkdir()
    source = origin / "b"
    base = root / "b"
    db_dir(base, DB).parent.mkdir(parents=True)
    shutil.copy2(source / "follower-v1/UPSTREAM", base / "follower-v1")
    shutil.copytree(db_dir(source, DB), db_dir(base, DB))
    trace = Trace(root / "b-phase.jsonl")
    port, = check.ports(1)
    data = {"binary": str(candidate), "label": root.name}
    with save_result(root, data):
        proc = run.start(root.name, candidate, "follow", "--root", base, "--dbs", str(DB),
                         "--source", f"http://127.0.0.1:{run.source}", "--listen", f"127.0.0.1:{port}",
                         env={**run.env, "KERNIVA_GOLOG_POLL_PHASE_TRACE": str(trace.path),
                              "KERNIVA_GOLOG_POLL_PHASE_DBS": str(DB)})
        trace.wait(lambda event: event["stage"] == "view_published" and event["last"] == manifest["last"],
                   time.monotonic_ns() + 60_000_000_000)
        rows = trace.events[:]
        stages = ["pull_request", "manifest_validated", "current_durable", "view_published"]
        require([r["stage"] for r in rows[:4]], stages, "first post-start install stages")
        request, checked, durable, view = rows[:4]
        stamps = [row["monotonic_ns"] for row in rows[:4]]
        require((stamps == sorted(stamps), all(row["db"] == DB for row in rows[:4])),
                (True, True), "one ordered DB install")
        require((request["after"], checked["last"], checked["files"], checked["snapshot"], durable["last"]),
                (manifest["after"], manifest["last"], len(files), 0, manifest["last"]), "complete delta")
        cursor = current(base, DB)
        require((cursor["source"], cursor["sequence"]), (manifest["source"], manifest["last"]), "durable cursor")
        for name, meta in files.items():
            require(digest(db_dir(base, DB) / cursor["generation"] / name), meta["sha256"], name)
        for key, expected in expected_reads.items():
            status, value = get(port, DB, key)
            require((status, sha256(value).hexdigest()), (200, expected), key)
        check.stop(proc)
        require(proc.returncode, 0, "graceful close")
        trace.collect()
        require((trace.events[-1]["stage"], trace.events[-1]["dropped"]), ("trace_closed", 0), "trace complete")
        require(any(event["stage"] == "poll_error" for event in trace.events), False, "poll error")
        seconds = (view["monotonic_ns"] - request["monotonic_ns"]) / 1e9
        size = sum(meta["bytes"] for meta in files.values())
        data.update(
            passed=True, events=rows[:4], payload_bytes=size, verified_files=len(files),
            verified_keys=len(expected_reads), current=cursor, trace_sha256=digest(trace.path),
            request_to_durable_ms=(durable["monotonic_ns"] - request["monotonic_ns"]) / 1e6,
            request_to_view_ms=seconds * 1000, effective_mib_per_second=size / (1 << 20) / seconds,
        )
    return data


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True, help="empty ext4 output directory")
    parser.add_argument("--origin", type=Path, required=True, help="passed, stopped 16-DB ordinary fixture")
    parser.add_argument("--bin", type=Path, required=True, help="frozen production server, source and library")
    parser.add_argument("--baseline", type=Path, required=True, help="phase-instrumented baseline CLI")
    parser.add_argument("--candidate", type=Path, required=True, help="phase-instrumented candidate CLI")
    parser.add_argument("--prepared", type=Path, help="reuse the exact sealed files of a passed earlier block")
    args = parser.parse_args()
    root, origin, binary = args.root.resolve(), args.origin.resolve(), args.bin.resolve()
    require(list(root.iterdir()), [], "empty output directory")
    require(filesystem(root), filesystem(origin), "same ext4 device")
    index = origin / "result.json"
    require(json.loads(index.read_bytes())["passed"], True, "converged fixture")
    arms = {"baseline": args.baseline.resolve(), "candidate": args.candidate.resolve()}
    artifacts = [*arms.values(), binary / "kerniva-server", binary / "kerniva-replica",
                 binary / "libkerniva_engine.so", index, Path(__file__).resolve()]
    data = {"order": ORDER, "db": DB, "keys": KEYS, "value_bytes": VALUE_BYTES,
            "sha256": {str(path): digest(path) for path in artifacts}, "windows": []}
    with save_result(root, data), ExitStack() as stack:
        run = Experiment(root, binary, binary / "kerniva-replica", root / "primary-data/go-log-v1", stack)
        before = current(origin / "b", DB)
        seed = args.prepared.resolve() if args.prepared else origin
        manifest, files = prepare(run, seed, before, args.prepared is not None)
        if args.prepared:
            previous = json.loads((seed / "result.json").read_bytes())
            require((previous["passed"], previous["before"], previous["manifest"], previous["files"]),
                    (True, before, manifest, files), "identical presealed replay")
            data["replayed_block_sha256"] = digest(seed / "result.json")
        data.update(before=before, manifest=manifest, files=files)
        expected = {}
        for i in range(KEYS):
            key, value = record(i)
            expected[key] = sha256(value.encode()).hexdigest()
        for i, version in enumerate(ORDER, 1):
            directory = root / f"{i:02d}-{version}"
            row = window(directory, run, arms[version], origin, manifest, files, expected)
            print(directory.name, round(row["effective_mib_per_second"], 2), "MiB/s", flush=True)
            data["windows"].append({"file": str(directory.relative_to(root) / "result.json"),
                                    "sha256": digest(directory / "result.json")})
        data["passed"] = True


if __name__ == "__main__":
    main()
