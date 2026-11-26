#!/usr/bin/env python3
"""Capture a bounded final-gate run with passive WSL block and pressure counters."""
import argparse
from collections import Counter, deque
from datetime import timedelta
import fcntl
import json
import os
from pathlib import Path
import sys
import tempfile
import time
from types import SimpleNamespace
from unittest.mock import patch

from golog_batch_capture import gc, run_window


PERIOD_NS = 40_000_000


def cpu_stat():
    values = {}
    for line in Path("/proc/stat").read_text().splitlines():
        name, *fields = line.split()
        if name in {"cpu", "ctxt", "procs_running", "procs_blocked"}:
            values[name] = list(map(int, fields))
    return values


class PassiveGuestSampler(gc.stall.GuestSampler):
    def sample(self):
        epoch = self.started["monotonic_after_ns"]
        slot = 0
        sources = {"disk": (gc.stall.diskstats, self.identity), "cpu": (cpu_stat,),
                   "psi_io": (gc.stall.pressure, "io"), "psi_cpu": (gc.stall.pressure, "cpu")}
        while not self.stop.is_set():
            now = time.monotonic_ns()
            slot = max(slot, (now - epoch) // PERIOD_NS)
            target = epoch + slot * PERIOD_NS
            if self.stop.wait(max(0, (target - now) / 1e9)):
                break
            start = gc.stall.clock()
            self.samples.append({"record": "sample", "index": slot, "target_monotonic_ns": target,
                                 "start": start, "source": {key: gc.stall.observation(*source)
                                                            for key, source in sources.items()},
                                 "end": gc.stall.clock()})
            slot += 1

    def finish(self):
        if self.pool is None:
            return None
        requested_stop = gc.stall.clock()
        self.stop.set()
        self.pool.shutdown(wait=True)
        self.future.result()
        self.pool = None
        footer = {"record": "summary", "sample_rows": len(self.samples), "stop_requested": requested_stop,
                  "sources_failed": dict(Counter(key for row in self.samples for key, source in row["source"].items()
                                                 if "error" in source)),
                  "missed_internal_slots": self.samples[-1]["index"] + 1 - len(self.samples) if self.samples else None}
        meta = {"record": "meta", "schema": 1, "device": self.identity, "interval_ns": PERIOD_NS,
                "start": self.started, "cpu_ticks_per_second": os.sysconf("SC_CLK_TCK"),
                "proc_schedstat_present": Path("/proc/schedstat").is_file(),
                "clock": "CLOCK_MONOTONIC and bracketed CLOCK_REALTIME observations"}
        output = self.case / "guest-passive.jsonl"
        gc.stall.write_jsonl(output, (meta, *self.samples, footer))
        return {"basename": output.name, "sha256": gc.stall.digest(output), **footer}


def slow_count(file):
    with file.open() as stream:
        summary = json.loads(deque(stream, maxlen=1)[0])
    if summary["type"] != "summary" or summary["drops"]:
        raise RuntimeError(f"incomplete client trace in {file}")
    return sum(op["slow"] for worker in summary["by_worker_second_get_set"] for second in worker for op in second)

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--setup", required=True, type=Path)
    parser.add_argument("--bin", required=True, type=Path)
    parser.add_argument("--binary-manifest", required=True, type=Path)
    parser.add_argument("--mirror", required=True, type=Path)
    parser.add_argument("--windows", type=int, choices=range(1, 5), default=4)
    args = parser.parse_args()
    setup = gc.live.read_json(args.setup)
    home = Path.home()
    if gc.live.check.RUNNER.filesystem_type(home) != "ext4":
        raise RuntimeError("capture requires ext4")
    inherited = {"GODEBUG", "GOGC", "GOMEMLIMIT", "GOMAXPROCS", "GOTRACEBACK", "KERNIVA_GOLOG_BATCH_TRACE"} & os.environ.keys()
    if inherited:
        raise RuntimeError(f"inherited override: {sorted(inherited)}")
    bins = args.bin.resolve()
    binaries = gc.verified_group(bins, args.binary_manifest)
    reference = args.setup.resolve().parent
    frozen = Path(setup["root"])
    old = gc.verified_group(frozen / "bin", reference / "diagnostic-binaries.sha256")
    if old.keys() != binaries.keys() or any(binaries[key] != old[key] for key in old.keys() - {"kerniva-server", "kerniva-bench"}):
        raise RuntimeError("unexpected auxiliary binary")
    receipt = {"argv": sys.argv, "setup": setup, "variant": "C", "binaries": binaries, "windows": [],
               "frozen_scripts": gc.verified_group(frozen / "source" / "scripts", reference / "stock-runner.sha256"),
               "start": gc.stall.clock(), "sampler_period_ns": PERIOD_NS,
               "offered_seconds": gc.DURATION, "read_percent": 95, "rate": gc.RATE,
               "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip()}
    status = 2
    with Path(setup["performance_lock"]).open("rb") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        mirror = args.mirror.resolve()
        mirror.mkdir(parents=True, exist_ok=False)
        root = Path(tempfile.mkdtemp(prefix="kerniva-golog-sync.", dir=home))
        print(f"ext4 {root}", flush=True)
        options = SimpleNamespace(bin=bins, stage="golog-sync-C", duration=gc.DURATION, warmup=gc.WARMUP,
                                  guest_counters=True, stop_opening=gc.live.now() + timedelta(seconds=args.windows * 65 + 20),
                                  child_process_options=gc.settled.child_process_options())
        used_ports = set()
        try:
            with patch.object(gc.stall, "GuestSampler", PassiveGuestSampler):
                for position in range(1, args.windows + 1):
                    window = run_window(root, options, position, "C", used_ports)
                    receipt["windows"].append(window)
                    if not window["pass"]:
                        raise RuntimeError(f"window failed: {window['folder']}")
                    window["slow_count"] = slow_count(root / window["folder"] / "client-trace.jsonl")
                    gc.settled.write_json(root / "run.json", receipt)
                    if window["slow_count"]:
                        break
            status = 0
        finally:
            receipt.update(end=gc.stall.clock(), driver_exit_code=status)
            gc.settled.write_json(root / "run.json", receipt)
            gc.mirror_evidence(root, mirror, status)
    return status


if __name__ == "__main__":
    sys.exit(main())
