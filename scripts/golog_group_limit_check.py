#!/usr/bin/env python3
"""Run the frozen four-window Go-log group-limit configuration check on WSL ext4."""
import argparse
from datetime import timedelta
import fcntl
import os
from pathlib import Path
import sys
import tempfile
import time
from types import SimpleNamespace
from unittest.mock import patch

import golog_gc_capture as gc


PLAN = ((50, 3000, 8), (95, 6000, 16), (50, 3000, 16), (95, 6000, 8))
REVISION = "bf6da5fb92748ebe2843a2654ce05a0761977983"


class PassiveWindow:
    def __init__(self, *_args, **_options):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *_error):
        return None

    def start(self):
        return time.monotonic_ns()

    def finish(self):
        return {}


def wait_for_client(client, _run, _record, _children, _intervals, _initial, _initial_start, duration):
    return client.wait(timeout=duration + 15)


def run_window(root, args, position, profile, group, ports):
    class Primary(gc.TOPOLOGY):
        def start(self, label, *cmd, env=None):
            config = Path(cmd[-1])
            settings = gc.live.read_json(config)
            settings["go_log_group_max"] = group
            gc.settled.write_json(config, settings)
            return super().start(label, *cmd, env=env)

    with patch.object(gc.live, "Topology", Primary), patch.object(gc.live, "observe_client", wait_for_client):
        status = gc.live.case(root, args, profile, "A", position, ports, observer_factory=PassiveWindow)
    read_percent, rate = profile
    folder = root / f"{position:02d}-r{read_percent}-q{rate}-A"
    record = gc.live.read_json(folder / "case.json")
    clean = gc.verify_clean(record, record["ports"]["primary"])
    result = {"folder": folder.name, "read_percent": read_percent, "rate": rate, "group_max": group,
              "case_status": status, "clean": clean, "pass": False}
    if status != "completed" or any(clean.values()):
        return result
    client = gc.live.read_json(folder / "client.json")
    windows = client["windows"]
    counts = ("offered", "sent", "succeeded")
    stop_failures = ("failures", "queue_drops", "queue_timeouts", "request_timeouts",
                     "network_errors", "reply_errors", "reconnect_attempts")
    settings = gc.live.read_json(folder / "config.json")
    result.update({key: client[key] for key in (*counts, "failures", "window_success_rate",
                                                "actual_success_rate", "drain_seconds")})
    result["latency_us"] = {op: client[op]["success_latency"] for op in ("get", "set")}
    result["pass"] = (
        settings["go_log_group_max"] == group and settings["network_type"] == "gnet"
        and settings["persistence_write_mode"] == "strong" and settings["persistence_durability"] == "sync"
        and not settings["pprof_enabled"] and client["keys"] == gc.live.KEYS and client["clients"] == gc.live.CLIENTS
        and all(client[key] == rate * gc.DURATION for key in counts) and client["failures"] == 0
        and all(client[op][key] == 0 for key in stop_failures for op in ("get", "set"))
        and [row["planned_second"] for row in windows] == list(range(gc.DURATION))
        and all(sum(row[op][key] for op in ("get", "set")) == rate for row in windows for key in counts)
        and all(row[op][key] == 0 for row in windows for key in stop_failures for op in ("get", "set"))
    )
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--setup", type=Path, required=True)
    parser.add_argument("--c-build-receipt", type=Path, required=True)
    parser.add_argument("--mirror", type=Path, required=True)
    cli = parser.parse_args()
    previous = gc.live.read_json(cli.setup)
    variant = gc.live.read_json(cli.c_build_receipt)["variants"]["C"]
    if variant["revision"] != REVISION:
        raise RuntimeError("unexpected C revision")
    bins = Path(variant["bin"]).resolve()
    binaries = {name: gc.stall.digest(bins / name) for name in variant["binaries"]}
    if binaries != variant["binaries"]:
        raise RuntimeError("C binary digest mismatch")
    home = Path.home()
    if gc.live.check.RUNNER.filesystem_type(home) != "ext4":
        raise RuntimeError("capture requires WSL ext4")
    inherited = {"GODEBUG", "GOGC", "GOMEMLIMIT", "GOMAXPROCS", "GOTRACEBACK", "LD_PRELOAD"} & os.environ.keys()
    if inherited:
        raise RuntimeError(f"inherited runtime settings: {sorted(inherited)}")
    receipt = {"plan": PLAN, "revision": REVISION, "binaries": binaries, "argv": sys.argv, "windows": [],
               "frozen_scripts": gc.verified_group(Path(previous["root"]) / "source" / "scripts",
                   cli.setup.resolve().parent / "stock-runner.sha256"),
               "preregistered": {"pass": "30 planned seconds at exact offered rate, all requests sent and fully replied, zero failures or reconnects, verified configuration and clean exit",
                   "latency": "observed paired p99 is descriptive; no default or capacity decision from one pair per load",
                   "throughput": "window_success_rate reports completions before cutoff; planned-bucket success includes drain"},
               "start": gc.stall.clock()}
    status = 2
    with Path(previous["performance_lock"]).open("rb") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        mirror = cli.mirror.resolve()
        mirror.mkdir(parents=True, exist_ok=False)
        root = Path(tempfile.mkdtemp(prefix="kerniva-golog-group-check.", dir=home))
        print(f"ext4 {root}", flush=True)
        args = SimpleNamespace(bin=bins, stage="golog-group-check", duration=gc.DURATION, warmup=gc.WARMUP,
                               stop_opening=gc.live.now() + timedelta(seconds=len(PLAN) * 65 + 20),
                               child_process_options=gc.settled.child_process_options())
        ports = set()
        gc.settled.write_json(root / "preregistered.json", receipt)
        try:
            for position, (read_percent, rate, group) in enumerate(PLAN, 1):
                result = run_window(root, args, position, (read_percent, rate), group, ports)
                receipt["windows"].append(result)
                gc.settled.write_json(root / "run.json", receipt)
            status = 0 if all(window["pass"] for window in receipt["windows"]) else 2
        finally:
            receipt.update(end=gc.stall.clock(), driver_exit_code=status)
            gc.settled.write_json(root / "run.json", receipt)
            gc.mirror_evidence(root, mirror, status)
    return status


if __name__ == "__main__":
    sys.exit(main())
