#!/usr/bin/env python3
"""Compare direct 16-DB polling and sealed-write visibility on one ext4 device."""

import argparse
from contextlib import ExitStack
from hashlib import sha256
from io import BytesIO
import json
import os
from pathlib import Path
import shutil
import signal
from statistics import median
import time

from golog_replica_snapshot_capacity import (
    DBS, LIGHT, MARKER_KEY, TIMEOUT, Experiment, check, config, current, digest, get,
    memory_process, require, save_result, unpack_manifest, value,
)

DATABASES = range(LIGHT + 1)
MARKS = ((15, 0.07), (0, 0.29), (7, 0.53), (14, 0.83))
MARK_SPACING = 3.0
WITNESS_SLEEP = 0.010
IDLE_SETTLE = 1.2
NANOSECONDS = 1_000_000_000


def sleep_until(deadline_ns):
    time.sleep(max(0, (deadline_ns - time.monotonic_ns()) / NANOSECONDS))


def filesystem(path):
    device, kind = config(["findmnt", "-n", "-o", "SOURCE,FSTYPE", "-T", path]).split()
    require(kind, "ext4", f"filesystem at {path}")
    return device


def pull(run, db, after, lineage):
    status, body = check.http(run.source, "/pull", db=db, after=after, source=lineage)
    require(status, 200, f"source pull for DB {db}")
    stream = BytesIO(body)
    _, manifest = unpack_manifest(stream)
    require((manifest["source"], manifest["db"], manifest["after"]), (lineage, db, after),
            f"source pull lineage for DB {db}")
    return manifest, stream.read()


def verify(run, lineage, expected, markers, prior_value):
    statuses = {}
    cursors = {}
    sources = {}
    for db in DATABASES:
        seq = expected[db]
        state = run.wait(f"B DB {db} HTTP status", lambda: run.status(run.b, db), 15)
        require((state["ready"], state["source"], state["sequence"], state["last_error"]),
                (True, lineage, seq, ""), f"DB {db} status")
        statuses[db] = state
        cursor = current(run.root / "b", db)
        require((cursor["source"], cursor["sequence"]), (lineage, seq), f"DB {db} CURRENT")
        cursors[db] = cursor
        manifest, payload = pull(run, db, seq, lineage)
        require((manifest["last"], manifest.get("files"), payload), (seq, None, b""),
                f"DB {db} caught-up source")
        sources[db] = manifest
        if db == LIGHT:
            require(get(run.b, db, MARKER_KEY), (200, prior_value.encode()), "fixture marker retained")
        else:
            require(get(run.b, db, "anchor"), (200, b"new"), f"DB {db} anchor")
            require(get(run.b, db, "tail"), (200, f"sealed tail {db:02d}".encode()), f"DB {db} tail")
            require(get(run.b, db, "bulk:0000"), (200, value(db, 0).encode()), f"DB {db} bulk")
            require(get(run.b, db, "removed")[0], 404, f"DB {db} delete")
        if db in markers:
            key, value_written = markers[db]
            require(get(run.b, db, key), (200, value_written.encode()), f"DB {db} new marker")
    return {"status": statuses, "current": cursors, "caught_up_source": sources}


def observe(probes, name, probe):
    entry = probes.setdefault(name, {})
    if "first_positive" in entry:
        return False
    start = time.monotonic_ns()
    positive, observed = probe()
    end = time.monotonic_ns()
    entry["first_positive" if positive else "last_negative"] = {
        "start_ns": start, "end_ns": end, "observed": observed,
    }
    return positive


def lag_ms(probe, file_probe, source_probe):
    # The source cannot serve a file before its sealed name exists. A verified
    # HTTP response proves availability by the end of that request.
    source_lower = file_probe["last_negative"]["start_ns"]
    source_upper = source_probe["first_positive"]["end_ns"]
    lower = probe["last_negative"]["start_ns"] - source_upper
    upper = probe["first_positive"]["end_ns"] - source_lower
    require(upper >= 0, True, "B observed after source lower bound")
    return [max(0, lower) / 1e6, upper / 1e6]


def write_marker(run, lineage, db, before, ordinal, phase, start_ns):
    after = before + 1
    key = "ordinary-poll:marker"
    marker = f"ordinary-poll-db-{db:02d}-sequence-{after:06d}"
    name = f"log-{after:020d}-{after:020d}.seg"
    sealed = run.storage / f"db_{db}" / name
    probes = {}

    def file_exists():
        present = sealed.is_file()
        return present, present

    def b_current():
        cursor = current(run.root / "b", db)
        require(cursor["source"], lineage, f"DB {db} CURRENT lineage")
        require(cursor["sequence"] in (before, after), True, f"DB {db} CURRENT sequence")
        return cursor["sequence"] == after, cursor

    def b_get():
        status, body = get(run.b, db, key)
        require(status in (200, 404), True, f"DB {db} GET status")
        if status == 200:
            require(body, marker.encode(), f"DB {db} marker bytes")
        return status == 200, {"status": status, "value": body.decode()}

    def source_pull():
        manifest, payload = pull(run, db, before, lineage)
        require(manifest["last"], after, f"DB {db} source marker sequence")
        require((manifest.get("snapshot", 0), manifest.get("rebase", False)), (0, False),
                f"DB {db} marker is a regular delta")
        require(manifest["files"], [{"name": name, "size": len(payload)}], f"DB {db} source delta file")
        require(marker.encode() in payload, True, f"DB {db} marker in source payload")
        require(payload, sealed.read_bytes(), f"DB {db} source payload agrees with sealed file")
        return True, {"manifest": manifest, "payload_bytes": len(payload), "sha256": sha256(payload).hexdigest()}

    sleep_until(start_ns + int(phase * NANOSECONDS))
    for tag, probe in (("sealed_file", file_exists), ("b_current", b_current), ("b_get", b_get)):
        require(observe(probes, tag, probe), False, f"DB {db} marker absent before SET in {tag}")
    send_ns = time.monotonic_ns()
    run.write(db, [(("SET", key, marker), "OK")])
    ack_ns = time.monotonic_ns()
    deadline_ns = ack_ns + 8 * NANOSECONDS
    while time.monotonic_ns() < deadline_ns:
        new_file = observe(probes, "sealed_file", file_exists)
        observe(probes, "b_current", b_current)
        observe(probes, "b_get", b_get)
        if new_file:
            observe(probes, "source_pull", source_pull)
        if all("first_positive" in probes.get(tag, {})
               for tag in ("sealed_file", "source_pull", "b_current", "b_get")):
            break
        time.sleep(WITNESS_SLEEP)
    else:
        raise TimeoutError(f"DB {db} marker witnesses timed out: {probes}")

    status = run.status(run.b, db)
    require((status["source"], status["sequence"], status["last_error"]),
            (lineage, after, ""), f"DB {db} status after marker")
    return {
        "ordinal": ordinal, "db": db, "key": key, "value": marker, "before": before, "after": after,
        "offset_seconds": phase, "offset_reference_ns": start_ns, "send_ns": send_ns, "ack_ns": ack_ns,
        "status": status, "probes": probes,
        "ack_to_sealed_observed_ms": (probes["sealed_file"]["first_positive"]["end_ns"] - ack_ns) / 1e6,
        "source_publication_bound_width_ms": (
            probes["source_pull"]["first_positive"]["end_ns"]
            - probes["sealed_file"]["last_negative"]["start_ns"]
        ) / 1e6,
        "source_to_b_current_bounds_ms": lag_ms(probes["b_current"], probes["sealed_file"], probes["source_pull"]),
        "source_to_b_get_bounds_ms": lag_ms(probes["b_get"], probes["sealed_file"], probes["source_pull"]),
    }


def connection_resources(pid, source_port):
    proc = Path(f"/proc/{pid}")
    try:
        descriptors = list((proc / "fd").iterdir())
        sockets = set()
        for descriptor in descriptors:
            try:
                target = os.readlink(descriptor)
            except FileNotFoundError:
                continue
            if target.startswith("socket:["):
                sockets.add(target[8:-1])
        peers = {f"{address}:{source_port:04X}" for address in (
            "0100007F", "0000000000000000FFFF00000100007F",
        )}
        established = 0
        for table in ("tcp", "tcp6"):
            path = proc / "net" / table
            if table == "tcp6" and not path.exists():
                continue
            for line in path.read_text().splitlines()[1:]:
                fields = line.split()
                established += fields[2] in peers and fields[3] == "01" and fields[9] in sockets
        return {"file_descriptors": len(descriptors), "source_established": established}
    except OSError as error:
        return {"error": repr(error)}


def sample(run, trace, b, index):
    start = time.monotonic_ns()
    counters = memory_process(b.pid)
    connections = connection_resources(b.pid, run.source)
    b.send_signal(signal.SIGUSR1)

    def find():
        if not trace.exists():
            return None
        rows = [row for line in trace.read_text().split("\n")[:-1]
                if (row := json.loads(line))["stage"] == "sample"]
        if len(rows) == index:
            return rows[-1]
        return None

    event = run.wait(f"B natural memory sample {index}", find, 5)
    require(event["monotonic_ns"] >= start, True, "signal sample monotonic time")
    return {"before_signal_ns": start, "memory": event, "proc": counters, "connections": connections}


def idle_window(run, b, trace, duration):
    time.sleep(IDLE_SETTLE)
    first = sample(run, trace, b, 1)
    sleep_until(first["memory"]["monotonic_ns"] + int(duration * NANOSECONDS))
    last = sample(run, trace, b, 2)
    begin, end = first["memory"], last["memory"]
    elapsed = (end["monotonic_ns"] - begin["monotonic_ns"]) / NANOSECONDS
    allocated = end["total_alloc_bytes"] - begin["total_alloc_bytes"]
    cpu_ticks = last["proc"]["cpu_ticks"] - first["proc"]["cpu_ticks"]
    ticks_per_second = os.sysconf("SC_CLK_TCK")
    return {"first": first, "last": last, "seconds": elapsed, "total_alloc_bytes": allocated,
            "total_alloc_mib_per_second": allocated / (1 << 20) / elapsed,
            "natural_gc": end["num_gc"] - begin["num_gc"], "cpu_ticks": cpu_ticks,
            "ticks_per_second": ticks_per_second, "cpu_ms_per_second": cpu_ticks / ticks_per_second * 1000 / elapsed}


def start_follower(run, follower, interval, label, trace=None):
    flags = ("--interval", interval) if interval != "default" else ()
    env = run.env if trace is None else {**run.env, "KERNIVA_GOLOG_MEMORY_TRACE": str(trace)}
    return run.start(label, follower, "follow", "--root", run.root / "b",
                     "--source", f"http://127.0.0.1:{run.source}", "--listen", f"127.0.0.1:{run.b}",
                     "--dbs", DBS, "--pull-idle-timeout", TIMEOUT, *flags, env=env)


def trial(work, args, fixture, interval, label, follower, device):
    work.mkdir()
    require(filesystem(work), device, "fixture/trial ext4 device")
    for folder in ("b", "primary-data"):
        shutil.copytree(args.origin / folder, work / folder)
    lineage = fixture["fixture"]["source"]
    prior_value = fixture["markers"][-1]["value"]
    copied = {db: current(work / "b", db) for db in DATABASES}
    for db, cursor in copied.items():
        require(cursor, current(args.origin / "b", db), f"DB {db} copied seed cursor")
    expected = {db: cursor["sequence"] for db, cursor in copied.items()}
    data = {"binary": label, "interval": interval, "interval_ms": 100 if interval == "100ms" else 1000,
            "initial_current": copied, "markers": [], "host_load_start": os.getloadavg()}
    with save_result(work, data), ExitStack() as context:
        run = Experiment(work, args.bin, args.source_binary, work / "primary-data/go-log-v1", context)
        data["ports"] = {key: run.ports[key] for key in ("primary", "source", "b")}
        run.start_primary()
        run.start_source()
        trace = work / "b-memory.jsonl"
        idle = start_follower(run, follower, interval, "b-idle", trace)
        data["idle_before"] = verify(run, lineage, expected, {}, prior_value)
        data["idle"] = idle_window(run, idle, trace, args.idle_seconds)
        data["idle_after"] = verify(run, lineage, expected, {}, prior_value)
        require(data["idle_after"]["current"], copied, "idle B did not replace CURRENT")
        check.stop(idle)
        require(idle.returncode, 0, "idle B graceful exit")

        b = start_follower(run, follower, interval, "b-writes")
        data["write_before"] = verify(run, lineage, expected, {}, prior_value)
        require(data["write_before"]["current"], copied, "write B starts at fixture CURRENT")
        time.sleep(IDLE_SETTLE)
        successful = {}
        reference_ns = time.monotonic_ns()
        for ordinal, (db, phase) in enumerate(MARKS, 1):
            offset = (ordinal - 1) * MARK_SPACING + phase
            row = write_marker(run, lineage, db, expected[db], ordinal, offset, reference_ns)
            data["markers"].append(row)
            expected[db] = row["after"]
            successful[db] = (row["key"], row["value"])
        data["write_after"] = verify(run, lineage, expected, successful, prior_value)
        for db in DATABASES:
            require(data["write_after"]["status"][db]["installs"], int(db in successful),
                    f"DB {db} expected only marker installs")
        check.stop(b)
        require(b.returncode, 0, "write B graceful exit")
        for name in ("source", "b-idle", "b-writes"):
            lines = (work / f"{name}.log").read_text().splitlines()
            require(len(lines), 1, f"{name} log has only listener startup")
            require("listening on" in lines[0], True, f"{name} listener startup")
        data["host_load_end"] = os.getloadavg()
        data["passed"] = True
    return data


def distribution(values):
    return {"min": min(values), "median": median(values), "max": max(values)}


def summarize(windows):
    groups = {}
    for window in windows:
        groups.setdefault((window["interval"], window["binary"]), []).append(window)
    return [
        {"interval": interval, "binary": label, "windows": len(runs),
         "marker_count": sum(len(run["markers"]) for run in runs),
         "idle_total_alloc_mib_per_second": [run["idle"]["total_alloc_mib_per_second"] for run in runs],
         "idle_cpu_ms_per_second": [run["idle"]["cpu_ms_per_second"] for run in runs],
         "idle_natural_gc": [run["idle"]["natural_gc"] for run in runs],
         "idle_connection_samples": [
             [run["idle"][point]["connections"] for point in ("first", "last")] for run in runs
         ],
         "idle_rss_bytes": [
             [run["idle"][point]["memory"].get("rss_bytes") for point in ("first", "last")] for run in runs
         ],
         "source_publication_bound_width_ms": distribution([
             row["source_publication_bound_width_ms"] for run in runs for row in run["markers"]
         ]),
         **{field: {
             "lower": distribution([row[field][0] for run in runs for row in run["markers"]]),
             "upper": distribution([row[field][1] for run in runs for row in run["markers"]]),
         } for field in ("source_to_b_current_bounds_ms", "source_to_b_get_bounds_ms")}}
        for (interval, label), runs in groups.items()
    ]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, type=Path, help="existing empty ext4 output directory")
    parser.add_argument("--origin", required=True, type=Path, help="passed and stopped 16-DB fixture")
    parser.add_argument("--bin", required=True, type=Path, help="frozen server/replica/library directory")
    parser.add_argument("--baseline", required=True, type=Path, help="baseline follower executable")
    parser.add_argument("--candidate", type=Path, help="optional candidate follower executable")
    parser.add_argument("--source-binary", type=Path, help="source executable, defaults to kerniva-replica in --bin")
    parser.add_argument("--idle-seconds", type=float, default=15)
    args = parser.parse_args()
    if args.idle_seconds <= 0:
        parser.error("--idle-seconds must be greater than zero")
    for name, item in vars(args).items():
        if isinstance(item, Path):
            setattr(args, name, item.resolve())
    if args.source_binary is None:
        args.source_binary = args.bin / "kerniva-replica"
    require(list(args.root.iterdir()), [], "new output directory")
    device = filesystem(args.root)
    require(filesystem(args.origin), device, "origin/output ext4 device")
    fixture = json.loads((args.origin / "result.json").read_bytes())
    require(fixture["passed"], True, "converged fixture result")
    artifacts = (args.baseline, args.candidate, args.source_binary, args.bin / "kerniva-server",
                 args.bin / "libkerniva_engine.so", Path(__file__).resolve())
    baseline = ("baseline", args.baseline)
    if args.candidate is None:
        schedule = [(interval, baseline) for interval in ("100ms", "default", "default", "100ms")]
    else:
        candidate = ("candidate", args.candidate)
        schedule = [(interval, binary) for interval in ("100ms", "default")
                    for binary in (baseline, candidate, candidate, baseline)]
    metadata = {
        "fixture": str(args.origin), "fixture_result_sha256": digest(args.origin / "result.json"),
        "device": device, "os": list(os.uname()), "cpu_count": os.cpu_count(),
        "artifacts_sha256": {str(path): digest(path) for path in artifacts if path is not None},
        "schedule": [[interval, label] for interval, (label, _) in schedule],
        "idle_seconds": args.idle_seconds, "writer_witness_target_sleep_ms": WITNESS_SLEEP * 1000,
        "measurements": {
            "idle": "Direct loopback. SIGUSR1 samples ReadMemStats without forcing GC. No application probes between samples. Pull counts are measured in the separate diagnostic experiment.",
            "write": "Fresh untraced B. A single sync RESP SET is scheduled for each marked DB at a separate offset from a shared monotonic anchor. After its exact sealed file appears, one source pull verifies its manifest and small marker delta.",
            "bounds": "Subtract end of verified source pull from start of last negative B witness for lower bound (clamp to zero). Subtract start of last missing sealed-file witness from end of first positive B witness for upper bound.",
            "interval": "The default condition omits the CLI --interval flag.",
            "current": "Positive CURRENT witness observes the published file by pathname; B GET observes read-view publication.",
        },
        "windows": [],
    }
    windows = []
    with save_result(args.root, metadata):
        for index, (interval, (label, follower)) in enumerate(schedule, 1):
            work = args.root / f"{index:02d}-{interval}-{label}"
            print(f"starting {work.name}", flush=True)
            result = trial(work, args, fixture, interval, label, follower, device)
            windows.append(result)
            metadata["windows"].append(str(work / "result.json"))
            print(f"passed {work.name} with {len(result['markers'])} marks", flush=True)
        metadata.update(passed=True, groups=summarize(windows))


if __name__ == "__main__":
    main()
