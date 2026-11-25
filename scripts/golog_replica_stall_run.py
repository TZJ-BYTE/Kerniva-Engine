#!/usr/bin/env python3
"""Run a signed same-arm OFF/ON stall campaign using the frozen fixed-rate topology."""

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import fcntl
from hashlib import file_digest
import json
import math
import os
from pathlib import Path
import subprocess
import sys
from threading import Event
import time
from types import SimpleNamespace
from unittest.mock import patch

import golog_replica_live_load as live
import golog_replica_set_tail as settled
from golog_replica_live_observer import need


RATE = 6000
READ_PERCENT = 95
DURATION = 30
WARMUP = 5
STRIDE = 16
PHASES = {"calibration": "technical_calibration", "formal-1": "formal_period_1", "formal-2": "formal_period_2"}
DISK_FIELDS = (
    "reads_completed", "reads_merged", "sectors_read", "read_ms",
    "writes_completed", "writes_merged", "sectors_written", "write_ms",
    "io_in_progress", "io_ms", "weighted_io_ms",
    "discards_completed", "discards_merged", "sectors_discarded", "discard_ms",
    "flushes_completed", "flush_ms",
)
INFO_FIELDS = ("sync_count", "last_sequence", "log_group_batches", "log_group_requests")


def digest(path):
    with path.open("rb") as source:
        return file_digest(source, "sha256").hexdigest()


def clock():
    start = time.monotonic_ns()
    wall = time.time_ns()
    return {"monotonic_before_ns": start, "utc_unix_ns": wall, "monotonic_after_ns": time.monotonic_ns()}


def observation(probe, *args):
    start = clock()
    try:
        return {"start": start, "value": probe(*args), "end": clock()}
    except Exception as error:
        return {"start": start, "error": f"{type(error).__name__}: {error}", "end": clock()}


def cpu():
    with Path("/proc/stat").open() as source:
        fields = source.readline().split()
    need(fields[0] == "cpu" and len(fields) >= 9, "missing aggregate guest CPU")
    names = ("user", "nice", "system", "idle", "iowait", "irq", "softirq", "steal", "guest", "guest_nice")
    return dict(zip(names, map(int, fields[1:])))


def pressure(resource):
    values = {}
    for line in Path(f"/proc/pressure/{resource}").read_text().splitlines():
        category, *fields = line.split()
        values[category] = {}
        for token in fields:
            key, value = token.split("=", 1)
            values[category][key] = int(value) if key == "total" else float(value)
    need("some" in values, f"missing {resource} pressure")
    return values


def disk_identity(path):
    device = path.stat().st_dev
    major, minor = os.major(device), os.minor(device)
    record = Path(f"/sys/dev/block/{major}:{minor}").resolve(strict=True)
    return {"major": major, "minor": minor, "sysfs": str(record), "kernel_name": record.name}


def diskstats(identity):
    key = str(identity["major"]), str(identity["minor"])
    for row in Path("/proc/diskstats").read_text().splitlines():
        major, minor, name, *counters = row.split()
        if (major, minor) == key:
            need(name == identity["kernel_name"] and len(counters) >= 11, "guest block identity changed")
            return {"major": int(major), "minor": int(minor), "name": name,
                    "fields": dict(zip(DISK_FIELDS, map(int, counters))), "raw_counters": list(map(int, counters))}
    raise ValueError(f"mapped guest block device {key} absent from /proc/diskstats")


def info(port):
    with live.primary_client(port, 0) as client:
        fields = dict(line.split(":", 1) for line in client.command("INFO").splitlines() if ":" in line)
    return {key: int(fields[key]) for key in INFO_FIELDS}


def write_jsonl(path, rows):
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("x", encoding="utf-8") as stream:
        for row in rows:
            stream.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)


class GuestSampler:
    """Bracket successful and failed inputs separately on the same guest monotonic clock."""

    def __init__(self, case, storage, primary, duration):
        self.case, self.storage, self.primary, self.duration = case, storage, primary, duration
        self.stop = Event()
        self.samples, self.started, self.pool = [], None, None
        self.identity = disk_identity(storage)

    def start(self):
        self.started = clock()
        self.pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="guest-counters")
        self.future = self.pool.submit(self.sample)

    def sample(self):
        epoch = self.started["monotonic_after_ns"]
        sequence = 0
        while not self.stop.is_set():
            target = epoch + sequence * 1_000_000_000
            if self.stop.wait(max(0, (target - time.monotonic_ns()) / 1e9)):
                break
            age = time.monotonic_ns() - epoch
            if age >= (sequence + 1) * 1_000_000_000:
                sequence = age // 1_000_000_000
                target = epoch + sequence * 1_000_000_000
            row = {"record": "sample", "index": sequence, "target_monotonic_ns": target, "start": clock()}
            row["source"] = {"cpu": observation(cpu), "psi_cpu": observation(pressure, "cpu"),
                             "psi_io": observation(pressure, "io"), "psi_memory": observation(pressure, "memory"),
                             "disk": observation(diskstats, self.identity), "primary_db0_info": observation(info, self.primary)}
            row["end"] = clock()
            self.samples.append(row)
            sequence += 1

    def finish(self):
        if self.pool is None:
            return None
        requested_stop = clock()
        self.stop.set()
        self.pool.shutdown(wait=True)
        self.future.result()
        self.pool = None
        counts = Counter(key for row in self.samples for key, item in row["source"].items() if "value" in item)
        errors = Counter(key for row in self.samples for key, item in row["source"].items() if "error" in item)
        header = {"record": "meta", "schema": 1, "device": self.identity, "interval_ns": 1_000_000_000,
                  "start": self.started, "cpu_ticks_per_second": os.sysconf("SC_CLK_TCK"),
                  "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
                  "planned_offer_seconds": self.duration,
                  "boundaries": {"disk": "guest virtual block device counters for the ext4 st_dev; sectors use 512-byte units",
                                 "psi": "guest pressure cumulative total in microseconds",
                                 "info": "two synchronous loopback RESP commands per attempt, SELECT DB0 and INFO; cumulative values"}}
        total_slots = max(1, math.ceil((requested_stop["monotonic_after_ns"] -
                                        self.started["monotonic_after_ns"]) / 1_000_000_000))
        footer = {"record": "summary", "sample_rows": len(self.samples), "attempted_period_slots": total_slots,
                  "missing_period_slots": max(0, total_slots - len(self.samples)),
                  "sources_succeeded": dict(counts), "sources_failed": dict(errors),
                  "stop_requested": requested_stop, "file_sealed": clock()}
        path = self.case / "guest-samples.jsonl"
        write_jsonl(path, (header, *self.samples, footer))
        return {"basename": path.name, "sha256": digest(path), **footer}


class HostSignal:
    def __init__(self, signals, key, run, duration):
        self.signals, self.key, self.run, self.duration = signals, key, run, duration
        self.activated = False
        self.ready = None
        self.closed = None

    def path(self, suffix):
        return self.signals / f"{self.key}.{suffix}.json"

    def publish(self, suffix, **fields):
        path = self.path(suffix)
        temporary = path.with_suffix(".tmp")
        sampled = clock()
        body = {"run_id": self.run, "id": self.key, "duration_seconds": self.duration,
                "guest_utc_ns": sampled["utc_unix_ns"], "guest_clock": sampled, **fields}
        with temporary.open("x", encoding="utf-8") as stream:
            json.dump(body, stream, sort_keys=True)
            stream.write("\n")
        temporary.replace(path)
        return body

    def arm(self):
        self.activated = True
        self.publish("open")
        until = time.monotonic() + 15
        while time.monotonic() < until:
            path = self.path("ready")
            if path.is_file():
                response = live.read_json(path)
                need(response["run_id"] == self.run and response["id"] == self.key, "host ready identity mismatch")
                self.ready = response
                break
            time.sleep(0.10)

    def finish(self, result):
        if self.activated and self.closed is None:
            self.closed = self.publish("close", measurement_client_exit=result)
        return {"ready": self.ready, "close": self.closed}


class Observers:
    """Keep the old observation path and append only the enabled guest sampler."""

    def __init__(self, root, storage, port, *arguments, detail, diagnostic, duration, **options):
        self.common = settled.CommonObservation(root, storage, port, *arguments, detail=detail, **options)
        self.guest = GuestSampler(root, storage, port, duration) if diagnostic else None

    def __enter__(self):
        self.common.__enter__()
        return self

    def start(self):
        epoch = self.common.start()
        if self.guest:
            self.guest.start()
        return epoch

    def finish(self):
        return self.common.finish()

    def __exit__(self, *error):
        self.common.__exit__(*error)


def launch_case(attempt, args, item, used_ports):
    arm, mode, position = item["arm"], item["mode"], item["position"]
    key = f"{args.stage}--{item['label']}"
    folder = attempt / item["label"]
    trace = folder / "client-trace.jsonl"
    host = HostSignal(args.host_signals, key, args.run_id, args.duration) if mode == "ON" else None
    common = None
    guest_result = None
    host_result = None
    resolution = None
    error = None

    def safe_end():
        need(live.now() + timedelta(seconds=DURATION) < args.stop_opening, "offer would outlast approved slot")
        need(not any(path.exists() for path in args.pause), "pause guard set")

    def release():
        safe_end()
        if host:
            host.arm()
            safe_end()

    def observer(*positional, **optional):
        nonlocal common
        common = Observers(*positional, **optional, detail=settled.ARMS[arm]["detailed_observer"],
                           diagnostic=mode == "ON", duration=args.duration)
        return common

    def stop_diagnostics(result):
        nonlocal guest_result, host_result
        if host:
            host_result = host.finish(result)
        if common is not None and common.guest is not None and guest_result is None:
            guest_result = common.guest.finish()

    def measured_client(*positional, **optional):
        nonlocal resolution
        try:
            resolution = live_client(*positional, **optional)
            return resolution
        finally:
            stop_diagnostics(resolution)

    def bench_process(argv, **options):
        argv.extend(("-request-trace-out", str(trace), "-request-trace-stride", str(STRIDE)))
        return subprocess.Popen(argv, **options)

    live_client = live.observe_client
    client_subprocess = SimpleNamespace(Popen=bench_process, DEVNULL=subprocess.DEVNULL)
    try:
        if mode == "ON":
            with patch.object(live, "subprocess", client_subprocess), patch.object(live, "observe_client", measured_client):
                state = live.case(attempt, args, (READ_PERCENT, RATE), arm, position, used_ports,
                                  components=settled.ARMS[arm], observer_factory=observer, before_release=release)
        else:
            state = live.case(attempt, args, (READ_PERCENT, RATE), arm, position, used_ports,
                              components=settled.ARMS[arm], observer_factory=observer, before_release=release)
    finally:
        if mode == "ON" and folder.is_dir():
            if not host.closed:
                stop_diagnostics(resolution)
            error = ("missing host ready receipt" if not host.ready else None)
            if not trace.is_file() or guest_result is None:
                error = "missing ON trace or guest sampler evidence"
        if folder.is_dir():
            settled.write_json(folder / "stall-diagnostics.json",
                               {"mode": mode, "id": key, "host": host_result, "guest": guest_result, "error": error,
                                "trace": {"basename": trace.name, "sha256": digest(trace), "stride": STRIDE}
                                if trace.is_file() else None})
    return state


def approval_plan(approved, registration, run_id, stage):
    need(approved["run_id"] == run_id and approved["approved_stage"] == stage, "approval scope mismatch")
    plan = approved["plan"]
    need((plan["target_rate"], plan["get_percent"], plan["duration_s"], plan["warmup_s"]) ==
         (RATE, READ_PERCENT, DURATION, WARMUP), "workload differs from validated configuration")
    windows = []
    for i, entry in enumerate(plan["windows"], 1):
        arm, mode = entry["arm"], entry["mode"]
        need(arm in ("A", "E") and mode in ("OFF", "ON"), "invalid arm or diagnostic mode")
        windows.append({"position": i, "arm": arm, "mode": mode, "label": f"{i:02d}-r95-q6000-{arm}"})
    need(len(windows) == 8 and set(Counter((x["arm"], x["mode"]) for x in windows).values()) == {2},
         "expected eight balanced signed windows")
    order = [f"{item['arm']}-{item['mode'].lower()}" for item in windows]
    need(order == registration["matrix"][PHASES[stage]]["order"], "signed phase differs from registered order")
    return windows


def actual_formal2_open(root):
    first = root / "formal-1"
    ledger = live.read_json(first / "run.json")
    need(ledger.get("completed_at"), "first period is not complete")

    def witness(window, event):
        attempt = window["attempts"][-1]
        need(attempt["state"] == "completed", "first period contains an uncompleted window")
        case = live.read_json(first / attempt["directory"] / "case.json")
        value = next(row["utc"] for row in case["events"] if row["name"] == event)
        return datetime.fromisoformat(value)

    first_offer = witness(ledger["windows"][0], "client_start_release")
    last_exit = witness(ledger["windows"][-1], "client_exited")
    return max(first_offer + timedelta(minutes=45), last_exit + timedelta(minutes=15))


def measurement_coverage(actual, case):
    alignment = case["measurement_alignment"]
    start = alignment["projected_start_monotonic_ns"][1]
    end = alignment["projected_offer_end_monotonic_ns"][0]
    counts = Counter()
    for line in (actual / "guest-samples.jsonl").read_bytes().splitlines():
        row = json.loads(line)
        if row["record"] != "sample":
            continue
        for name, channel in row["source"].items():
            counts[name] += int(start <= channel["start"]["monotonic_before_ns"]
                                <= channel["end"]["monotonic_after_ns"] <= end and "value" in channel)
    return {"projected_offer_valid": alignment["projection_consistent"], "inner_offer_monotonic_ns": [start, end],
            "samples_by_source": dict(counts), "minimum_required_per_source": 27,
            "coverage_met": alignment["projection_consistent"] and len(counts) == 6 and min(counts.values()) >= 27}


def run_stage(args, approved, registration):
    stage = args.root / args.stage
    plan = approval_plan(approved, registration, args.run_id, args.stage)
    args.opens = datetime.fromisoformat(approved["not_before"])
    args.stop_opening = datetime.fromisoformat(approved["closes"])
    hard_open_stop = datetime.fromisoformat(args.setup["deadline"]) - timedelta(minutes=45)
    need(args.opens <= live.now() < args.stop_opening <= hard_open_stop,
         "outside signed slot or hard deadline")
    if args.stage == "formal-2":
        need(live.now() >= actual_formal2_open(args.root), "measured first-period holdoff active")
    need(not any(path.exists() for path in args.pause), "pause guard set")
    stage.mkdir()
    attempt = stage / "attempt-001"
    attempt.mkdir()
    contract = {"stage": args.stage, "run_id": args.run_id, "approval_sha256": digest(args.approval),
                "script_sha256": digest(Path(__file__)), "input_manifest_sha256": digest(args.inputs),
                "start": args.opens.isoformat(), "close": args.stop_opening.isoformat(),
                "stock": args.setup["stock_source_revision"],
                "binaries": {name: digest(args.bin/name) for name in live.BINARIES}}
    ledger = {"contract": contract, "driver": live.check.RUNNER.process_identity(os.getpid()),
              "started_at": live.now().isoformat(),
              "windows": [{**entry, "attempts": []} for entry in plan]}
    path = stage / "run.json"
    settled.write_json(path, ledger)

    def save():
        settled.write_json(path, ledger)

    used_ports = set()
    for entry in ledger["windows"]:
        need(not any(p.exists() for p in args.pause) and live.now() + timedelta(seconds=DURATION) < args.stop_opening,
             "closing or paused before opening window")
        relative = f"{attempt.name}/{entry['label']}"
        window = {"directory": relative, "started_at": live.now().isoformat(), "state": "running"}
        entry["attempts"].append(window)
        save()
        state = launch_case(attempt, args, entry, used_ports)
        actual = stage / relative
        case = live.read_json(actual / "case.json")
        diagnostics = live.read_json(actual / "stall-diagnostics.json")
        if state == "completed" and diagnostics["guest"] is not None:
            window["guest_measurement_coverage"] = measurement_coverage(actual, case)
        window.update(state=state, finished_at=live.now().isoformat(), files=settled.case_files(actual),
                      scope=settled.scope(case, actual), diagnostics_error=diagnostics["error"])
        save()
        if state != "completed":
            return 2
    ledger["completed_at"] = live.now().isoformat()
    save()
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--bin", type=Path, required=True)
    parser.add_argument("--stage", choices=tuple(PHASES), required=True)
    parser.add_argument("--startup", type=Path, required=True)
    parser.add_argument("--approval", type=Path, required=True)
    parser.add_argument("--approval-sha256", required=True)
    parser.add_argument("--inputs", type=Path, required=True)
    parser.add_argument("--input-sha256", required=True)
    parser.add_argument("--preregistration", type=Path, required=True)
    parser.add_argument("--preregistration-sha256", required=True)
    parser.add_argument("--host-signals", type=Path, required=True)
    parser.add_argument("--pause", type=Path, required=True, action="append")
    args = parser.parse_args()
    args.setup = live.read_json(args.startup)
    args.run_id = args.setup["run_id"]
    need(Path(args.setup["root"]) == args.root and live.check.RUNNER.filesystem_type(args.root) == "ext4",
         "run directory must match the initialized ext4 path")
    need(digest(args.approval) == args.approval_sha256, "Astra approval bytes changed")
    need(digest(args.inputs) == args.input_sha256, "frozen input manifest changed")
    need(digest(args.preregistration) == args.preregistration_sha256, "preregistration bytes changed")
    approval = live.read_json(args.approval)
    need(approval["input_manifest_sha256"] == args.input_sha256 and
         approval["preregistration_sha256"] == args.preregistration_sha256, "approval does not pin these inputs")
    args.duration, args.warmup = DURATION, WARMUP
    args.child_process_options = settled.child_process_options()
    args.host_signals.mkdir(exist_ok=True)
    with Path(args.setup["performance_lock"]).open("r+b") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return run_stage(args, approval, live.read_json(args.preregistration))


if __name__ == "__main__":
    sys.exit(main())
