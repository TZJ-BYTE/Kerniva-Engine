#!/usr/bin/env python3
"""Run the preregistered five-arm synchronized-write topology experiment."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from ctypes import CDLL
from datetime import datetime, timedelta
import fcntl
from pathlib import Path
import json
import os
import re
import signal
import sys
from threading import Event
import time

from golog_replica_live_load import BINARIES, PROFILES, case, check, digest, now, read_json, sequence
from golog_replica_live_load import primary_client
from golog_replica_live_observer import LiveObserver, follower_get, need, primary, status, witness

ARMS = {
    "A": {"maintenance": False, "replication": False, "detailed_observer": False},
    "B": {"maintenance": True, "replication": False, "detailed_observer": False},
    "C": {"maintenance": False, "replication": True, "detailed_observer": False},
    "D": {"maintenance": True, "replication": True, "detailed_observer": False},
    "E": {"maintenance": True, "replication": True, "detailed_observer": True},
}
SCRIPT_NAMES = ("golog_replica_set_tail.py", "golog_replica_live_load.py", "golog_replica_live_observer.py",
                "golog-replica-check.py", "golog_replica_snapshot_capacity.py", "golog_replica_capacity.py",
                "golog_replica_shared_link.py", "golog-recovery-bench.py", "golog_replica_stress.py",
                "thesis-bench-linux.py")
PORT_COLLISION = re.compile(r"address already in use|cannot bind to address", re.IGNORECASE)
DATADIR_NAMES = {"primary-data", "a", "b"}
OBSERVE_INTERVAL = 1
PR_SET_PDEATHSIG = 1


def child_process_options():
    parent = os.getpid()
    libc = CDLL(None, use_errno=True)

    def stop_if_driver_exits():
        if libc.prctl(PR_SET_PDEATHSIG, signal.SIGKILL, 0, 0, 0) != 0 or os.getppid() != parent:
            os._exit(125)

    return {"preexec_fn": stop_if_driver_exits}

def write_json(path, value):
    check.RUNNER.write_json(path, value)


def current_sequence(port, db):
    with primary_client(port, db) as client:
        return sequence(client)


def resp_request_bytes(*parts):
    encoded = [str(part).encode("utf-8") for part in parts]
    return len(f"*{len(encoded)}\r\n") + sum(len(f"${len(part)}\r\n") + len(part) + 2 for part in encoded)


class CommonObservation:
    """Record equal primary-side controls in all arms; E retains the previous full observer."""

    def __init__(self, root, storage, primary_port, followers, databases, *, source_port, offsets,
                 snapshot_intervals, detail=False):
        self.root, self.storage, self.primary = root, storage, primary_port
        self.followers, self.databases, self.offsets = followers, databases, offsets
        self.stack = ExitStack()
        self.full = LiveObserver(root, storage, primary_port, followers, databases, source_port=source_port,
                                 offsets=offsets, snapshot_intervals=snapshot_intervals) if detail else None
        self.lineage = None
        self.cancel = Event()
        self.markers, self.samples = [], []
        self.pool = self.sample_task = self.finished = None

    def __enter__(self):
        if self.full is not None:
            self.stack.enter_context(self.full)
        return self

    def __exit__(self, *_):
        self.cancel.set()
        if self.pool is not None:
            self.pool.shutdown(wait=True, cancel_futures=True)
        if self.finished is None:
            write_json(self.root / "common-observer-incomplete.json",
                       {"primary_and_status_samples": self.samples, "markers": self.markers})
        self.stack.__exit__(*_)
        return False

    def start(self):
        if self.followers:
            self.lineage = (self.storage / ".replica-source-id").read_text().strip()
        self.pool = ThreadPoolExecutor(max_workers=1 + len(self.databases) * len(self.offsets))
        self.anchor = self.full.start() if self.full else time.monotonic_ns()
        self.sample_task = self.pool.submit(self.sample_until_done)
        if not self.full:
            self.jobs = [self.pool.submit(self.mark, index, db, delay)
                         for index, delay in enumerate(self.offsets) for db in self.databases]
        return self.anchor

    def sample_until_done(self):
        try:
            while not self.cancel.is_set():
                for db in self.databases:
                    sample = {"db": db, "primary": witness(current_sequence, self.primary, db)}
                    sample["followers"] = {peer: witness(status, port, db, self.lineage)
                                           for peer, port in self.followers.items()}
                    self.samples.append(sample)
                self.cancel.wait(OBSERVE_INTERVAL)
        except BaseException:
            self.cancel.set()
            raise

    def mark(self, ordinal, db, delay):
        wake_at = self.anchor + round(delay * 1e9)
        if self.cancel.wait(max(0, (wake_at - time.monotonic_ns()) / 1e9)):
            raise RuntimeError("marker observation cancelled")
        key = f"live-observer:{ordinal}:db:{db}"
        value = "value:" + key
        absent = witness(primary, self.primary, db, "GET", key)
        need(absent["value"] is None, f"unexpected previous marker value {key}")
        ack = witness(primary, self.primary, db, "SET", key, value)
        need(ack["value"] == "OK", f"marker SET failed for {key}")
        record = {"db": db, "ordinal": ordinal, "key": key, "value": value, "offset": delay,
                  "before_primary": absent, "set": ack}
        self.markers.append(record)
        return record

    def check_marker(self, marker):
        db, key, value = (marker[field] for field in ("db", "key", "value"))
        mark = witness(primary, self.primary, db, "GET", key)
        need(mark["value"] == value, f"primary missing or wrong marker {key}")
        marker["final_primary"] = mark
        marker["final_replicas"] = {}
        attempts = []
        for peer, port in self.followers.items():
            def observed():
                sample = witness(follower_get, port, db, key, value)
                attempts.append({"peer": peer, **sample})
                return sample if sample["value"]["status"] == 200 else None

            marker["final_replicas"][peer] = check.wait_for(f"{peer} {key} readback", observed, 10)[0]
        return attempts

    def finish(self):
        if self.full:
            full = self.full.finish()
            path = self.root / full["evidence"]
            markers = read_json(path)["markers"]
            peer_queries = sum(len(marker["replicas"][peer]["samples"])
                               for marker in markers for peer in self.followers)
            final_get_requests = len(markers) * len(self.followers)
        else:
            markers = [job.result() for job in self.jobs]
            peer_queries = 0
            checks = [sample for marker in markers for sample in self.check_marker(marker)]
            final_get_requests = len(checks)
        self.cancel.set()
        self.sample_task.result()
        marker_request_bytes = sum(3 * resp_request_bytes("SELECT", m["db"])
                                   + 2 * resp_request_bytes("GET", m["key"])
                                   + resp_request_bytes("SET", m["key"], m["value"]) for m in markers)
        summary = {"file": "common-observer.json", "passed": True, "anchor_ns": self.anchor,
                   "primary_sample_interval_seconds": OBSERVE_INTERVAL, "common_samples": len(self.samples),
                   "marker_rows": len(markers), "marker_primary_get_commands": 2 * len(markers),
                   "marker_primary_set_commands": len(markers),
                   "marker_primary_select_commands": 3 * len(markers),
                   "marker_logical_key_and_value_bytes": sum(len(m["key"]) + len(m["value"]) for m in markers),
                   "marker_primary_resp_request_bytes": marker_request_bytes,
                   "sample_primary_info_commands": len(self.samples), "sample_primary_select_commands": len(self.samples),
                   "sample_primary_resp_request_bytes": sum(resp_request_bytes("SELECT", s["db"])
                                                            + resp_request_bytes("INFO") for s in self.samples),
                   "sample_follower_status_requests": sum(len(s["followers"]) for s in self.samples),
                   "marker_follower_live_get_requests": peer_queries,
                   "marker_follower_terminal_get_requests": final_get_requests}
        raw = {**summary, "primary_and_status_samples": self.samples, "markers": markers}
        if self.full:
            summary.update(detailed_file="live-observer.json", detailed_sha256=digest(self.root / "live-observer.json"),
                           detailed_pulls=full["source_pull_requests"],
                           detailed_response_bytes=full["source_pull_response_body_bytes_read"])
            raw.update(summary)
        else:
            raw["terminal_follower_gets"] = checks
        path = self.root / summary["file"]
        write_json(path, raw)
        self.finished = {**summary, "sha256": digest(path)}
        return self.finished


def phase_plan(stage, preregistration, confirmation=None):
    ratings = {rate: (read, rate) for read, rate in PROFILES}
    if stage in ("pilot", "pilot-noise"):
        pilot = preregistration["pilot"]
        duration, warmup = pilot["duration_s"], pilot["warmup_s"]
        order = pilot["arms_each_profile"] if stage == "pilot" else pilot["probe_repeat_arms_per_profile"]
        groups = [(rate, (order,)) for rate in pilot["profile_order"]]
    elif stage == "recheck":
        config = confirmation["recheck"]
        read, rate = ratings[config["rate"]]
        order = config["arms"]
        need(len(order) == 2 and order[0] != order[1] and set(order) <= ARMS.keys(), "invalid recheck arm pair")
        duration, warmup = preregistration["formal"]["duration_s"], preregistration["formal"]["warmup_s"]
        groups = [(rate, (order, order[::-1]))]
    else:
        formal = preregistration["formal"]
        duration, warmup = formal["duration_s"], formal["warmup_s"]
        selected = next(period for period in formal["periods"] if period["id"] == stage)
        groups = [(rate, selected["round_order_in_each_profile"]) for rate in selected["profile_order"]]
    windows = []
    for rate, orders in groups:
        read, _ = ratings[rate]
        for round_number, order in enumerate(orders, 1):
            for arm in order:
                position = len(windows) + 1
                windows.append({"position": position, "round": round_number, "arm": arm,
                                "read_percent": read, "rate": rate, "label": f"{position:02d}-r{read}-q{rate}-{arm}"})
    return duration, warmup, windows


def case_files(folder):
    retained = {}
    for path in folder.iterdir():
        if path.name in DATADIR_NAMES:
            continue
        files = [path] if path.is_file() else path.rglob("*")
        for file in files:
            if file.is_file():
                retained[file.relative_to(folder).as_posix()] = digest(file)
    return retained


def retryable_startup(folder, record):
    if (record.get("failed_phase") not in ("startup", "topology_started")
            or record.get("client_release_interval") or (folder / "client.start").exists()):
        return False
    for name in ("primary.log", "source.log", "follower-a.log", "follower-b.log", "client.stderr.log"):
        path = folder / name
        if path.is_file() and PORT_COLLISION.search(path.read_text(errors="replace")[-65536:]):
            return True
    return False


def scope(record, folder):
    if record["status"] != "completed":
        return {}
    flags = record["components"]
    projection = record["measurement_alignment"]
    start = projection["projected_start_monotonic_ns"][1]
    end = projection["projected_offer_end_monotonic_ns"][0]
    samples = read_json(folder / record["observations_file"])["primary_and_status_samples"]
    peers = {"a": [], "b": []} if flags["replication"] else {}
    for sample in samples:
        if sample["db"] != 0:
            continue
        for peer in peers:
            entry = sample["followers"][peer]
            if start <= entry["start_ns"] <= entry["end_ns"] <= end:
                peers[peer].append(entry["value"]["sequence"])
    return {"clock_projection_consistent": projection["projection_consistent"],
            "observed_cutoffs_in_offer": len(projection["snapshot_brackets_inside_projected_offer"]),
            "follower_db0_inside_offer": {peer: {"samples": len(values),
                                                 "first": values[0] if values else None,
                                                 "last": values[-1] if values else None,
                                                 "advanced": len(values) >= 2 and values[-1] > values[0]}
                                           for peer, values in peers.items()}}


def save_phase(path, phase):
    write_json(path, phase)


def run_phase(args, contract):
    phase_path = args.root / "run.json"
    if phase_path.exists():
        phase = read_json(phase_path)
        need(phase["contract"] == contract, "phase inputs changed on recovery")
        for window in phase["windows"]:
            for attempt in window["attempts"]:
                if attempt["state"] == "running":
                    folder = args.root / attempt["directory"]
                    case_path = folder / "case.json"
                    record = read_json(case_path) if case_path.is_file() else {}
                    released = bool(record.get("client_release_interval")) or (folder / "client.start").is_file()
                    files = case_files(folder) if folder.is_dir() else {}
                    if record.get("status") == "completed" and record.get("process_exit_codes"):
                        attempt.update(state="completed", files=files, scope=scope(record, folder))
                    elif record and retryable_startup(folder, record) and not released:
                        attempt.update(state="retryable_startup", files=files, release_observed=False)
                    else:
                        attempt.update(state="interrupted", files=files, release_observed=released)
                if attempt.get("files"):
                    folder = args.root / attempt["directory"]
                    need(case_files(folder) == attempt["files"], f"evidence changed for {folder}")
    else:
        phase = {"created_at": now().isoformat(), "contract": contract,
                 "windows": [{**entry, "attempts": []} for entry in contract["plan"]], "sessions": []}
    session_number = len(phase["sessions"]) + 1
    session_dir = f"attempt-{session_number:03}"
    phase["sessions"].append({"number": session_number, "pid": os.getpid(),
                              "identity": check.RUNNER.process_identity(os.getpid()), "started_at": now().isoformat()})
    save_phase(phase_path, phase)
    attempt_root = args.root / session_dir
    attempt_root.mkdir()

    def allow():
        need(args.opens <= now() and now() + timedelta(seconds=args.duration) < args.closes,
             "not enough time for a full offer window inside the registered slot")
        need(not any(path.exists() for path in args.pause), "run is paused")

    used_ports = set()
    for window in phase["windows"]:
        for attempt in window["attempts"]:
            case_path = args.root / attempt["directory"] / "case.json"
            if case_path.is_file():
                used_ports.update(read_json(case_path).get("ports", {}).values())
    for window in phase["windows"]:
        attempts = window["attempts"]
        if attempts and attempts[-1]["state"] == "completed":
            continue
        if attempts and (attempts[-1]["state"] != "retryable_startup" or len(attempts) >= 2):
            return 2
        while len(attempts) < 2:
            allow()
            label = window["label"]
            target = attempt_root if not (attempt_root / label).exists() else attempt_root / "startup-retry"
            target.mkdir(exist_ok=True)
            folder = target / label
            attempt = {"number": len(attempts) + 1, "state": "running",
                       "directory": folder.relative_to(args.root).as_posix(), "started_at": now().isoformat()}
            attempts.append(attempt)
            save_phase(phase_path, phase)
            arm = window["arm"]

            def observer(*positional, **optional):
                return CommonObservation(*positional, **optional, detail=ARMS[arm]["detailed_observer"])

            outcome = case(target, args, (window["read_percent"], window["rate"]), arm, window["position"],
                           used_ports, components=ARMS[arm], observer_factory=observer, before_release=allow)
            record = read_json(folder / "case.json")
            if outcome != "completed" and retryable_startup(folder, record):
                outcome = "retryable_startup"
            attempt.update(state=outcome, finished_at=now().isoformat(),
                           files=case_files(folder), scope=scope(record, folder))
            save_phase(phase_path, phase)
            if outcome == "completed":
                break
            if outcome != "retryable_startup":
                return 2
        if attempts[-1]["state"] != "completed":
            return 2
    phase.update(completed_at=now().isoformat())
    save_phase(phase_path, phase)
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stage", choices=("pilot", "pilot-noise", "formal-1", "formal-2", "recheck"), required=True)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--bin", type=Path, required=True)
    parser.add_argument("--lock", type=Path, required=True)
    parser.add_argument("--pause", type=Path, action="append", required=True)
    parser.add_argument("--startup-ledger", type=Path, required=True)
    parser.add_argument("--preregistration", type=Path, required=True)
    parser.add_argument("--preregistration-sha256", required=True)
    parser.add_argument("--source-archive-sha256", required=True)
    parser.add_argument("--approval", type=Path)
    parser.add_argument("--approval-sha256")
    args = parser.parse_args()
    preregistration = read_json(args.preregistration)
    need(digest(args.preregistration) == args.preregistration_sha256, "registration hash changed")
    startup = read_json(args.startup_ledger)
    need(startup["run_id"] == preregistration["run_id"], "registration run identity changed")
    args.opens = datetime.fromisoformat(startup["stages"][args.stage]["opens"])
    args.closes = datetime.fromisoformat(startup["stages"][args.stage]["closes"])
    args.stop_opening = args.closes
    confirmation = None
    if args.stage.startswith("formal") or args.stage == "recheck":
        need(args.approval and args.approval_sha256 and digest(args.approval) == args.approval_sha256,
             "verified Astra phase release required")
        confirmation = read_json(args.approval)
    args.duration, args.warmup, plan = phase_plan(args.stage, preregistration, confirmation)
    args.bin = args.bin.resolve(strict=True)
    args.root = args.root.resolve()
    args.root.mkdir(exist_ok=True)
    need(check.RUNNER.filesystem_type(args.root) == "ext4", "phase output must use ext4")
    contract = {"stage": args.stage, "duration": args.duration, "warmup": args.warmup, "plan": plan,
                "source_revision": startup["source_revision"], "source_archive_sha256": args.source_archive_sha256,
                "startup_sha256": digest(args.startup_ledger), "preregistration_sha256": args.preregistration_sha256,
                "approval_sha256": args.approval_sha256, "lock": str(args.lock),
                "binaries": {name: digest(args.bin / name) for name in BINARIES},
                "scripts": {name: digest(Path(__file__).with_name(name)) for name in SCRIPT_NAMES}}
    args.child_process_options = child_process_options()
    with args.lock.open("r+b") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return run_phase(args, contract)


if __name__ == "__main__":
    sys.exit(main())
