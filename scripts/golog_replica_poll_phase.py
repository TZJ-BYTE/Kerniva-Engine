#!/usr/bin/env python3
"""Compare default-cadence follower visibility at measured, matched seal phases."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from hashlib import sha256
import json
from pathlib import Path
import shutil
import socket
from statistics import median
import time
from types import SimpleNamespace

from golog_replica_poll_ordinary import (
    DATABASES, filesystem, observe, pull, sleep_until, verify,
)
from golog_replica_snapshot_capacity import (
    DBS, TIMEOUT, Experiment, check, current, digest, get, require, save_result,
)

PHASES = ((15, 25), (0, 450), (7, 850))
SEGMENT_BYTES = 4096
TRIGGER_VALUE = "x" * (2 * SEGMENT_BYTES)
SEGMENT_AGE_SECONDS = 60
MATCH_TOLERANCE_MS = 20
MAX_SOURCE_BOUND_MS = 15
NS_PER_MS = 1_000_000
STAT_SLEEP = 0.001
GET_SLEEP = 0.005


class PhaseMismatch(RuntimeError):
    """The source opened too early, too late, or across a wide observation bound."""


class Trace:
    def __init__(self, path):
        self.path = path
        self.events = []
        self.offset = 0

    def collect(self):
        if not self.path.exists():
            return
        with self.path.open("rb") as stream:
            stream.seek(self.offset)
            for line in stream:
                if not line.endswith(b"\n"):
                    break
                event = json.loads(line)
                event["observed_ns"] = time.monotonic_ns()
                self.events.append(event)
                self.offset += len(line)

    def wait(self, predicate, deadline_ns):
        while time.monotonic_ns() < deadline_ns:
            self.collect()
            for event in self.events:
                if predicate(event):
                    return event
            time.sleep(STAT_SLEEP)
        raise TimeoutError(f"poll phase event missing in {self.path}")

    def after(self, db, time_ns):
        self.collect()
        return [event for event in self.events if event["db"] == db and event["monotonic_ns"] > time_ns]


def ms_between(later, earlier):
    return (later - earlier) / NS_PER_MS


def start_primary(run):
    settings = check.RECOVERY.configuration(
        run.storage.parent, run.root / "primary.log", run.primary,
        SimpleNamespace(network="gnet", segment_bytes=SEGMENT_BYTES),
    )
    settings.update(db_count=len(DATABASES), go_log_segment_age_seconds=SEGMENT_AGE_SECONDS,
                    go_log_group_max=8, max_memory=768 << 20)
    config = run.root / "config.json"
    check.RUNNER.write_json(config, settings)
    run.start("primary", run.binary / "kerniva-server", "-config", config)
    run.wait("primary ready", lambda: check.RUNNER.command(run.primary, "PING", timeout=1) == "PONG")
    info = check.RUNNER.server_info(run.primary)
    require((info.get("storage_backend"), info.get("persistence_durability")), ("go-log", "sync"),
            "phase primary storage and durability")


def start_follower(run, follower, trace, phases):
    return run.start(
        "b", follower, "follow", "--root", run.root / "b",
        "--source", f"http://127.0.0.1:{run.source}", "--listen", f"127.0.0.1:{run.b}",
        "--dbs", DBS, "--pull-idle-timeout", TIMEOUT,
        env={**run.env, "KERNIVA_GOLOG_POLL_PHASE_TRACE": str(trace.path),
             "KERNIVA_GOLOG_POLL_PHASE_DBS": ",".join(str(db) for db, _ in phases)},
    )


def send_marker(client, due_ns, key, marker):
    sleep_until(due_ns)
    send = time.monotonic_ns()
    require(client.command("SET", key, marker), "OK", "synchronous marker SET")
    marker_ack = time.monotonic_ns()
    require(client.command("SET", "phase:rotate", TRIGGER_VALUE), "OK", "synchronous rotation SET")
    return {"send_ns": send, "marker_ack_ns": marker_ack, "trigger_ack_ns": time.monotonic_ns()}


def witness(run, trace, lineage, db, before, phase, row, executor):
    sequence = before + 1
    key = "phase:marker"
    marker = f"default-phase-db-{db:02d}-seq-{sequence:06d}-offset-{phase:03d}"
    filename = f"log-{sequence:020d}-{sequence:020d}.seg"
    directory = run.storage / f"db_{db}"
    sealed = directory / filename
    require((directory / "active.log").stat().st_size, 8, f"DB {db} initially empty active segment")
    probes = row["probes"] = {}
    row.update(db=db, before=before, sequence=sequence, target_phase_ms=phase, key=key, marker=marker)

    def sealed_exists():
        exists = sealed.is_file()
        return exists, exists

    def b_get():
        status, body = get(run.b, db, key)
        require(status in (404, 200), True, f"DB {db} GET response")
        if status == 200:
            require(body, marker.encode(), f"DB {db} marker GET bytes")
        return status == 200, {"status": status, "value": body.decode()}

    def source_pull():
        manifest, payload = pull(run, db, before, lineage)
        require((manifest["last"], manifest.get("snapshot", 0), manifest.get("rebase", False)),
                (sequence, 0, False), f"DB {db} regular delta and exact source sequence")
        require(manifest["files"], [{"name": filename, "size": len(payload)}], f"DB {db} single sealed delta")
        require(marker.encode() in payload, True, f"DB {db} marker in HTTP source payload")
        require(payload, sealed.read_bytes(), f"DB {db} HTTP source payload equals sealed file")
        return True, {"manifest": manifest, "payload_bytes": len(payload), "sha256": sha256(payload).hexdigest()}

    with socket.create_connection(("127.0.0.1", run.primary), timeout=5) as connection:
        connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        with check.RECOVERY.RESP(connection) as client:
            require(client.command("SELECT", str(db)), "OK", "select phase marker DB")
            require(observe(probes, "sealed_file", sealed_exists), False, "marker sealed file initially absent")
            require(observe(probes, "b_get", b_get), False, "marker view initially absent")
            ready_ns = time.monotonic_ns()
            t0 = trace.wait(
                lambda event: event["db"] == db and event["stage"] == "poll_no_change"
                and (event["after"], event["last"]) == (before, before) and event["monotonic_ns"] >= ready_ns,
                ready_ns + 3_000 * NS_PER_MS,
            )
            row["t0"] = t0
            t0_ns = t0["monotonic_ns"]
            due_ns = t0_ns + phase * NS_PER_MS
            row["scheduled_write_ns"] = due_ns
            lead_ms = min(5, max(1, phase / 5))
            if time.monotonic_ns() > due_ns - lead_ms * NS_PER_MS:
                raise PhaseMismatch(f"DB {db} received T0 with insufficient lead time")
            writer = executor.submit(send_marker, client, due_ns, key, marker)
            sleep_until(due_ns - 5 * NS_PER_MS)
            while not observe(probes, "sealed_file", sealed_exists):
                if time.monotonic_ns() > due_ns + 200 * NS_PER_MS:
                    writer.result(timeout=5)
                    raise TimeoutError(f"DB {db} rotation did not publish {filename}")
                time.sleep(STAT_SLEEP)
            observe(probes, "source_pull", source_pull)
            row["resp"] = writer.result(timeout=5)

    s_lower = probes["sealed_file"]["last_negative"]["start_ns"]
    s_upper = probes["source_pull"]["first_positive"]["end_ns"]
    row["source_bounds_ns"] = [s_lower, s_upper]
    row["source_phase_bounds_ms"] = [ms_between(s_lower, t0_ns), ms_between(s_upper, t0_ns)]
    row["source_phase_midpoint_ms"] = ms_between((s_lower + s_upper) / 2, t0_ns)
    row["source_bound_width_ms"] = ms_between(s_upper, s_lower)
    require((directory / "active.log").stat().st_size > SEGMENT_BYTES, True, f"DB {db} rotation frame stays active")

    sleep_until(max(s_upper, t0_ns + 940 * NS_PER_MS))
    while not observe(probes, "b_get", b_get):
        if time.monotonic_ns() > t0_ns + 4_000 * NS_PER_MS:
            raise TimeoutError(f"DB {db} marker never became readable")
        time.sleep(GET_SLEEP)

    trace.wait(lambda event: event["db"] == db and event["stage"] == "view_published"
               and event["last"] == sequence and event["monotonic_ns"] > t0_ns,
               t0_ns + 4_500 * NS_PER_MS)
    events = trace.after(db, t0_ns)
    row["events"] = events
    if any(event["stage"] == "poll_error" for event in events):
        raise RuntimeError(f"DB {db} polling error: {events}")
    requests = [event for event in events if event["stage"] == "pull_request"]
    t1 = requests[0]
    t1_ns = t1["monotonic_ns"]
    row["t1"] = t1
    if not s_upper < t1_ns:
        raise PhaseMismatch(f"DB {db} source availability window overlaps first request")
    require(t1["after"], before, f"DB {db} first post-T0 request starts at old sequence")
    manifest = next(event for event in events if event["stage"] == "manifest_validated")
    require((manifest["after"], manifest["last"], manifest["files"], manifest["snapshot"], manifest["rebase"]),
            (before, sequence, 1, 0, False), f"DB {db} first post-T0 manifest includes marker")
    durable = next(event for event in events if event["stage"] == "current_durable" and event["last"] == sequence)
    view = next(event for event in events if event["stage"] == "view_published" and event["last"] == sequence)
    durable_ns, view_ns = durable["monotonic_ns"], view["monotonic_ns"]
    require(t1_ns <= manifest["monotonic_ns"] <= durable_ns <= view_ns, True, f"DB {db} publication order")
    require([event for event in requests[1:] if event["monotonic_ns"] <= view_ns], [],
            f"DB {db} published during its first post-T0 pull")
    get_lower = probes["b_get"]["last_negative"]["start_ns"]
    get_upper = probes["b_get"]["first_positive"]["end_ns"]
    require(get_lower <= view_ns <= get_upper, True, f"DB {db} GET encloses read-view publication")
    cursor = current(run.root / "b", db)
    require((cursor["source"], cursor["sequence"]), (lineage, sequence), f"DB {db} durable CURRENT lineage")
    row.update(
        durable=durable, view=view, end_current=cursor,
        t0_observation_delay_ms=ms_between(t0["observed_ns"], t0_ns),
        schedule_ms=ms_between(t1_ns, t0_ns),
        source_to_request_bounds_ms=[ms_between(t1_ns, s_upper), ms_between(t1_ns, s_lower)],
        request_to_durable_ms=ms_between(durable_ns, t1_ns),
        request_to_view_ms=ms_between(view_ns, t1_ns),
        request_to_get_bounds_ms=[max(0, ms_between(get_lower, t1_ns)), ms_between(get_upper, t1_ns)],
        source_to_get_bounds_ms=[max(0, ms_between(get_lower, s_upper)), ms_between(get_upper, s_lower)],
    )
    if row["source_bound_width_ms"] > MAX_SOURCE_BOUND_MS:
        raise PhaseMismatch(f"DB {db} source bound wider than {MAX_SOURCE_BOUND_MS} ms")
    if abs(row["source_phase_midpoint_ms"] - phase) > MATCH_TOLERANCE_MS:
        raise PhaseMismatch(f"DB {db} observed source phase differs from planned by over {MATCH_TOLERANCE_MS} ms")
    return key, marker


def trial(work, args, fixture, phases, label, follower, matched):
    work.mkdir()
    data = {"binary": label, "markers": [], "interval": "CLI default 1s"}
    with save_result(work, data), ExitStack() as stack:
        require(filesystem(work), args.device, "same fixture and trial device")
        for folder in ("b", "primary-data"):
            shutil.copytree(args.origin / folder, work / folder)
        run = Experiment(work, args.bin, args.source_binary, work / "primary-data/go-log-v1", stack)
        data["ports"] = {key: run.ports[key] for key in ("primary", "source", "b")}
        data["origin"] = str(args.origin)
        data["trace_path"] = str(work / "b-phase.jsonl")
        start_primary(run)
        run.start_source()
        trace = Trace(Path(data["trace_path"]))
        follower_process = start_follower(run, follower, trace, phases)
        initial = {db: current(work / "b", db) for db in DATABASES}
        expected = {db: cursor["sequence"] for db, cursor in initial.items()}
        lineage = fixture["fixture"]["source"]
        prior_marker = fixture["markers"][-1]["value"]
        data["before"] = verify(run, lineage, expected, {}, prior_marker)
        observed = {}
        executor = stack.enter_context(ThreadPoolExecutor(max_workers=1))
        for db, phase in phases:
            row = {}
            data["markers"].append(row)
            observed[db] = witness(run, trace, lineage, db, expected[db], phase, row, executor)
            expected[db] = row["sequence"]
            for midpoint in matched.get(db, []):
                if abs(midpoint - row["source_phase_midpoint_ms"]) > MATCH_TOLERANCE_MS:
                    raise PhaseMismatch(f"DB {db} source phases differ by over {MATCH_TOLERANCE_MS} ms")
        data["after"] = verify(run, lineage, expected, observed, prior_marker)
        for db in DATABASES:
            require(data["after"]["status"][db]["installs"], int(db in observed),
                    f"DB {db} marker-only install count")
        check.stop(follower_process)
        require(follower_process.returncode, 0, "phase B graceful exit")
        trace.collect()
        closed = trace.events[-1]
        require((closed["stage"], closed["db"], closed["dropped"]), ("trace_closed", -1, 0),
                "phase trace closed without dropped events")
        data["trace_close"] = closed
        require([event for event in trace.events if event["stage"] == "poll_error"], [],
                "all traced B polls completed without error")
        data["passed"] = True
    return data


def summarize(windows, phases):
    groups = []
    for db, phase in phases:
        group = {"db": db, "target_phase_ms": phase}
        for label in dict.fromkeys(window["binary"] for window in windows):
            rows = [row for window in windows if window["binary"] == label
                    for row in window["markers"] if row["db"] == db]
            group[label] = {field: [row[field] for row in rows] for field in (
                "source_phase_bounds_ms", "source_phase_midpoint_ms", "schedule_ms",
                "source_to_request_bounds_ms", "request_to_durable_ms", "request_to_view_ms",
                "request_to_get_bounds_ms", "source_to_get_bounds_ms",
            )}
        rows = [row for window in windows for row in window["markers"] if row["db"] == db]
        midpoints = [row["source_phase_midpoint_ms"] for row in rows]
        group["source_phase_midpoint_spread_ms"] = max(midpoints) - min(midpoints)
        if "candidate" in group:
            group["candidate_minus_baseline_median_view_install_ms"] = (
                median(group["candidate"]["request_to_view_ms"]) - median(group["baseline"]["request_to_view_ms"])
            )
        groups.append(group)
    return groups


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, type=Path, help="existing empty ext4 directory")
    parser.add_argument("--origin", required=True, type=Path, help="passed, stopped 16-DB fixture")
    parser.add_argument("--bin", required=True, type=Path, help="frozen primary, source and native library directory")
    parser.add_argument("--baseline", required=True, type=Path, help="phase-instrumented baseline B")
    parser.add_argument("--candidate", required=True, type=Path, help="phase-instrumented candidate B")
    run_mode = parser.add_mutually_exclusive_group()
    run_mode.add_argument("--preflight", action="store_true", help="single baseline DB at the latest phase")
    run_mode.add_argument("--phase", nargs=2, type=int, metavar=("DB", "MS"),
                          help="run the full ABBA schedule for a single DB and offset")
    args = parser.parse_args()
    if args.phase is not None:
        db, phase = args.phase
        if db not in DATABASES or not 0 < phase < 1000:
            parser.error("--phase requires DB 0–15 and a positive offset below 1000 ms")
    for name, path in vars(args).items():
        if isinstance(path, Path):
            setattr(args, name, path.resolve())
    require(list(args.root.iterdir()), [], "new phase output directory")
    args.device = filesystem(args.root)
    require(filesystem(args.origin), args.device, "origin/output ext4 device")
    args.source_binary = args.bin / "kerniva-replica"
    fixture_path = args.origin / "result.json"
    fixture = json.loads(fixture_path.read_bytes())
    require(fixture["passed"], True, "converged fixture")
    schedule = (("baseline", args.baseline),)
    phases = PHASES[-1:]
    if not args.preflight:
        schedule = (*schedule, ("candidate", args.candidate), ("candidate", args.candidate), *schedule)
        phases = (tuple(args.phase),) if args.phase is not None else PHASES
    artifacts = (args.bin / "kerniva-server", args.bin / "libkerniva_engine.so", args.source_binary,
                 args.baseline, args.candidate, Path(__file__).resolve(), fixture_path)
    data = {
        "devices": [args.device], "plan": [label for label, _ in schedule], "phases": phases,
        "segment_bytes": SEGMENT_BYTES, "segment_age_seconds": SEGMENT_AGE_SECONDS,
        "source_phase_matching_tolerance_ms": MATCH_TOLERANCE_MS, "max_source_bound_ms": MAX_SOURCE_BOUND_MS,
        "artifacts_sha256": {str(path): digest(path) for path in artifacts},
        "windows": [], "exclusions": [],
        "bounds": "S lower is the start of the last local stat with the exact sealed name absent. S upper is the completion of a single verified HTTP source delta probe. GET is bounded by the last negative and first positive HTTP observations. CURRENT durable is measured after directory sync.",
    }
    windows = []
    matched = {}
    with save_result(args.root, data):
        for index, (label, follower) in enumerate(schedule, 1):
            for attempt in range(1, 4):
                work = args.root / f"{index:02d}-{label}-attempt{attempt}"
                print(f"starting {work.name}", flush=True)
                try:
                    result = trial(work, args, fixture, phases, label, follower, matched)
                except PhaseMismatch as error:
                    data["exclusions"].append({"result": str(work / "result.json"), "reason": str(error)})
                    print(f"phase mismatch {work.name}: {error}", flush=True)
                    if attempt == 3 or args.preflight:
                        raise
                    continue
                for row in result["markers"]:
                    matched.setdefault(row["db"], []).append(row["source_phase_midpoint_ms"])
                windows.append(result)
                data["windows"].append(str(work / "result.json"))
                print(f"passed {work.name}", flush=True)
                break
        data.update(passed=True, groups=summarize(windows, phases))


if __name__ == "__main__":
    main()
