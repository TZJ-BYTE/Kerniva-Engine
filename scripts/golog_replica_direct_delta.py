#!/usr/bin/env python3
"""Measure effective sealed-delta install rate on an unpaced HTTP loopback source."""

import argparse
from contextlib import ExitStack, nullcontext
from hashlib import sha256
from inspect import unwrap
import json
import math
import os
from pathlib import Path
import shutil
import time

from diagnostics.golog_install_latency.observe import external_observation
from diagnostics.golog_install_latency.phases import install_events, phase_summary
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
MODES = ("ordinary", "coarse", "fine", "narrow", "off")


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
    start = time.monotonic_ns()
    manifest, body = pull(run, DB, before["sequence"], before["source"])
    end_ns = time.monotonic_ns()
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
    return manifest, expected, {"start_ns": start, "end_ns": end_ns, "source": manifest["source"],
                                "after": manifest["after"], "last": manifest["last"],
                                "verified_files": len(expected), "payload_bytes": offset}


def window(root, run, follower, mode, origin, before, manifest, files, expected_reads, source, args):
    root.mkdir()
    data = {"binary": str(follower), "label": root.name, "mode": mode}
    with save_result(root, data):
        seed = origin / "b"
        base = root / "b"
        db_dir(base, DB).parent.mkdir(parents=True)
        shutil.copy2(seed / "follower-v1/UPSTREAM", base / "follower-v1")
        shutil.copytree(db_dir(seed, DB), db_dir(base, DB))
        require(current(base, DB), before, "copied seed CURRENT")
        data["before"] = before
        trace = Trace(root / "b-phase.jsonl") if mode not in ("ordinary", "off") else None
        env = {**run.env}
        if trace is not None:
            env.update(KERNIVA_GOLOG_POLL_PHASE_TRACE=str(trace.path), KERNIVA_GOLOG_POLL_PHASE_DBS=str(DB),
                       KERNIVA_GOLOG_POLL_PHASE_DETAIL=mode)
        port, = check.ports(1)
        observer = (external_observation(root, data, source, args.sample_interval_ms, args.host_disk)
                    if args.observe else nullcontext())
        try:
            with observer as external:
                start = time.monotonic_ns()
                proc = run.start(root.name, follower, "follow", "--root", base, "--dbs", str(DB),
                                 "--source", f"http://127.0.0.1:{run.source}", "--listen", f"127.0.0.1:{port}",
                                 env=env)
                data["process"] = {"pid": proc.pid, "port": port, "start_ns": start,
                                   "end_ns": time.monotonic_ns(), "log": f"../{root.name}.log"}
                deadline = start + 60_000_000_000
                if external is not None:
                    external.data["follower_start"] = data["process"].copy()
                    external.emit("follower_start", **external.data["follower_start"])
                    witness_key = next(iter(expected_reads))
                    external.wait(proc, base, DB, before, manifest["last"], port, witness_key,
                                  expected_reads[witness_key], deadline)
                else:
                    trace.wait(lambda event: event["db"] == DB and event["stage"] == "view_published"
                               and event["last"] == manifest["last"], deadline)
        finally:
            if trace is not None and trace.path.exists():
                data["trace_file"] = trace.path.name
        cursor = current(base, DB)
        require((cursor["source"], cursor["sequence"], cursor["snapshot"]),
                (manifest["source"], manifest["last"], before["snapshot"]), "installed cursor")
        if args.observe:
            require(cursor, data["external"]["current"]["first_positive"]["observed"], "observed CURRENT retained")
        installed = data["installed_files"] = {}
        for name, meta in files.items():
            path = db_dir(base, DB) / cursor["generation"] / name
            installed[name] = {"bytes": path.stat().st_size, "sha256": digest(path)}
            require(installed[name], meta, name)
        reads = data["read_sha256"] = {}
        for key, expected in expected_reads.items():
            status, value = get(port, DB, key)
            reads[key] = sha256(value).hexdigest()
            require((status, reads[key]), (200, expected), key)
        status = run.status(port, DB)
        require((status["ready"], status["source"], status["sequence"], status["last_error"], status["installs"]),
                (True, manifest["source"], manifest["last"], "", 1), "one complete visible install")
        check.stop(proc)
        data["process"].update(returncode=proc.returncode, log_sha256=digest(run.root / f"{root.name}.log"))
        require(proc.returncode, 0, "graceful close")
        size = sum(meta["bytes"] for meta in files.values())
        if trace is not None:
            trace.collect()
            data["trace_sha256"] = digest(trace.path)
            events, group = install_events(trace.events, DB, manifest, mode)
            request, checked, durable, view = (event["monotonic_ns"] for event in events)
            data.update(events=events, phase_summary=phase_summary(events, group, files, mode),
                        manifest_to_durable_ms=(durable - checked) / 1e6, request_to_durable_ms=(durable - request) / 1e6,
                        request_to_view_ms=(view - request) / 1e6,
                        durable_mib_per_second=size / (1 << 20) / ((durable - request) / 1e9),
                        effective_mib_per_second=size / (1 << 20) / ((view - request) / 1e9))
        data.update(passed=True, payload_bytes=size, verified_files=len(files),
                    verified_keys=len(expected_reads), current=cursor, status=status)
    return data


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True, help="empty ext4 output directory")
    parser.add_argument("--origin", type=Path, required=True, help="passed, stopped 16-DB ordinary fixture")
    parser.add_argument("--bin", type=Path, required=True, help="frozen production server, source and library")
    parser.add_argument("--baseline", type=Path, required=True, help="baseline follower CLI")
    parser.add_argument("--candidate", type=Path, required=True, help="candidate follower CLI")
    parser.add_argument("--baseline-mode", choices=MODES, default="coarse")
    parser.add_argument("--candidate-mode", choices=MODES, default="coarse")
    parser.add_argument("--prepared", type=Path, help="reuse the exact sealed files of a passed earlier block")
    parser.add_argument("--order", choices=("ABBA-BAAB", "BAAB-ABBA"), default="ABBA-BAAB")
    parser.add_argument("--observe", action="store_true", help="use identical external witnesses in both arms")
    parser.add_argument("--sample-interval-ms", type=float, default=50, help="target /proc sample period with --observe")
    parser.add_argument("--host-disk", type=Path, help="Windows mount containing the WSL virtual disk")
    args = parser.parse_args()
    if not math.isfinite(args.sample_interval_ms) or args.sample_interval_ms < 1:
        parser.error("--sample-interval-ms must be finite and at least 1")
    args.observe |= bool({args.baseline_mode, args.candidate_mode} & {"ordinary", "off"})
    if args.host_disk is not None:
        args.host_disk = args.host_disk.resolve()
    root, origin, binary = args.root.resolve(), args.origin.resolve(), args.bin.resolve()
    require(list(root.iterdir()), [], "empty output directory")
    require(filesystem(root), filesystem(origin), "same ext4 device")
    index = origin / "result.json"
    require(json.loads(index.read_bytes())["passed"], True, "converged fixture")
    arms = {"baseline": args.baseline.resolve(), "candidate": args.candidate.resolve()}
    modes = {"baseline": args.baseline_mode, "candidate": args.candidate_mode}
    order = ORDER if args.order == "ABBA-BAAB" else ORDER[4:] + ORDER[:4]
    checkpoints = os.environ.get("KERNIVA_LATENCY_WINDOW_CHECKPOINT_DIR")
    artifacts = [*arms.values(), binary / "kerniva-server", binary / "kerniva-replica",
                 binary / "libkerniva_engine.so", index, Path(__file__).resolve(),
                 Path(unwrap(external_observation).__code__.co_filename).resolve(), Path(install_events.__code__.co_filename).resolve()]
    if checkpoints:
        from golog_replica_latency_run import write_json

        artifacts.append(Path(write_json.__code__.co_filename).resolve())
    data = {"order": order, "modes": modes, "db": DB, "keys": KEYS, "value_bytes": VALUE_BYTES,
            "sha256": {str(path): digest(path) for path in dict.fromkeys(artifacts)}, "windows": [],
            "external_observer": args.observe, "host_load_start": os.getloadavg()}
    with save_result(root, data), ExitStack() as stack:
        run = Experiment(root, binary, binary / "kerniva-replica", root / "primary-data/go-log-v1", stack)
        before = current(origin / "b", DB)
        seed = args.prepared.resolve() if args.prepared else origin
        manifest, files, source = prepare(run, seed, before, args.prepared is not None)
        if args.prepared:
            previous = json.loads((seed / "result.json").read_bytes())
            require((previous["passed"], previous["before"], previous["manifest"], previous["files"]),
                    (True, before, manifest, files), "identical presealed replay")
            data["replayed_block_sha256"] = digest(seed / "result.json")
        data.update(before=before, manifest=manifest, files=files, source_verified_available=source)
        expected = {}
        for i in range(KEYS):
            key, value = record(i)
            expected[key] = sha256(value.encode()).hexdigest()
        for i, version in enumerate(order, 1):
            directory = root / f"{i:02d}-{version}"
            row = window(directory, run, arms[version], modes[version], origin, before,
                         manifest, files, expected, source, args)
            if "effective_mib_per_second" in row:
                print(directory.name, round(row["effective_mib_per_second"], 2), "MiB/s", flush=True)
            else:
                print(directory.name, row["external"]["get_visible_after_start_ms"], "ms to correct GET", flush=True)
            data["windows"].append({"file": str(directory.relative_to(root) / "result.json"),
                                    "sha256": digest(directory / "result.json")})
            if checkpoints:
                write_json(Path(checkpoints) / f"{i:02d}.json", data)
        for name, meta in files.items():
            path = run.storage / f"db_{DB}" / name
            require({"bytes": path.stat().st_size, "sha256": digest(path)}, meta, f"source retained {name}")
        data.update(source_verified_unchanged=True, host_load_end=os.getloadavg())
        data["passed"] = True


if __name__ == "__main__":
    main()
