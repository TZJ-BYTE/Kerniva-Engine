#!/usr/bin/env python3
"""Capture frozen C with synchronized Windows and WSL evidence."""
import argparse
from contextlib import nullcontext
from datetime import timedelta
import fcntl
import os
from pathlib import Path
from shutil import which
import sys
import tempfile
import time
from types import SimpleNamespace
from unittest.mock import patch

from golog_batch_capture import gc, run_window, slow_sync_fixture
from golog_batch_report import records, require
from golog_sync_diagnosis import PERIOD_NS, PassiveGuestSampler


ORDER = ("off", "on", "on", "on", "on", "on", "on", "off")
CONTROL_DELAYS_MS = (0, 220)


def capture(root, options, position, mode, ports, args):
    from golog_host_clock import HostWindow

    label = f"{position:02d}-r95-q6000-A"
    host_dir = args.host_root / label
    context = (HostWindow(args.windows_python, host_dir, args.sampler_script, drive="F", interval_ms=40)
               if mode == "on" else nullcontext())
    with context:
        window = run_window(root, options, position, "C", ports)

    require(window["pass"], f"{label}: workload capture failed")
    folder = root / label
    rows = list(records(folder / "client-trace.jsonl"))
    summary = rows[-1]
    expected = gc.RATE * options.duration
    require(rows[0]["stride"] == gc.STRIDE and summary["type"] == "summary" and summary["drops"] == 0
            and summary["offered"] == expected, f"{label}: incomplete request trace")
    measurements = gc.live.read_json(folder / "client.json")
    slow = {op: sum(second[i]["slow"] for worker in summary["by_worker_second_get_set"] for second in worker)
            for i, op in enumerate(("get", "set"))}
    window.update(host_mode=mode, windows_host_directory=str(host_dir) if mode == "on" else None,
                  request_rows=len(rows) - 2, client_failures=measurements["failures"], slow_count=slow)
    gc.settled.write_json(folder / "capture.json", window)
    print(f"{label}: host {mode}, slow {slow}, failures {measurements['failures']}", flush=True)
    return window


def capture_control(root, options, delay, args):
    from golog_host_clock import HostWindow

    label = f"control-fsync-return-{delay:03d}ms"
    folder = root / label
    host_dir = args.host_root / label
    sampler = guest = None

    class SampledPrimary(gc.TOPOLOGY):
        def start_primary(self):
            nonlocal sampler
            primary = super().start_primary()
            sampler = PassiveGuestSampler(folder, self.storage, self.primary, 1)
            sampler.start()
            time.sleep(0.25)
            return primary

    with HostWindow(args.windows_python, host_dir, args.sampler_script, drive="F", interval_ms=40):
        try:
            with patch.object(gc, "TOPOLOGY", SampledPrimary):
                control = slow_sync_fixture(root, options, label, delay, commands=(None,))
        finally:
            if sampler is not None:
                time.sleep(0.25)
                guest = sampler.finish()

    require(control["pass"], f"{label}: syscall control failed")
    control.update(folder=label, guest_sampler=guest, windows_host_directory=str(host_dir))
    gc.settled.write_json(folder / "fixture.json", control)
    print(f"{label}: matched return-delay control", flush=True)
    return control


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--setup", type=Path, required=True)
    parser.add_argument("--bin", type=Path, required=True)
    parser.add_argument("--binary-manifest", type=Path, required=True)
    parser.add_argument("--host-root", type=Path, required=True)
    parser.add_argument("--sampler-script", type=Path, default=Path(__file__).resolve().with_name(
        "golog_windows_disk_sampler.py"))
    parser.add_argument("--windows-python", type=Path, default=which("python.exe"),
                        help="Windows python.exe visible from WSL (defaults to PATH)")
    parser.add_argument("--mirror", type=Path, required=True)
    parser.add_argument("--controls", action="store_true", help="run paired isolated fsync return controls")
    args = parser.parse_args()
    if args.windows_python is None:
        parser.error("Windows Python unavailable on PATH; pass --windows-python with its WSL path")

    setup = gc.live.read_json(args.setup)
    home = Path.home()
    require(gc.live.check.RUNNER.filesystem_type(home) == "ext4", "capture requires ext4")
    inherited = {"GODEBUG", "GOGC", "GOMEMLIMIT", "GOMAXPROCS", "GOTRACEBACK", "KERNIVA_GOLOG_BATCH_TRACE"}
    require(not inherited.intersection(os.environ), "inherited Go or trace overrides")
    bins = args.bin.resolve()
    frozen = Path(setup["root"])
    reference = args.setup.resolve().parent
    current = gc.verified_group(bins, args.binary_manifest)
    old = gc.verified_group(frozen / "bin", reference / "diagnostic-binaries.sha256")
    require(old.keys() == current.keys() and all(current[key] == old[key] for key in old.keys() -
            {"kerniva-server", "kerniva-bench"}), "unexpected frozen binary dependency")
    receipt = {"argv": sys.argv, "setup": setup, "variant": "C", "binaries": current,
               "frozen_scripts": gc.verified_group(frozen / "source" / "scripts", reference / "stock-runner.sha256"),
               "runner_sha256": gc.stall.digest(Path(__file__)), "start": gc.stall.clock(), "windows": [],
               "group_max": 8, "sampler_period_ns": PERIOD_NS, "host_drive": "F",
               "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip()}
    if args.controls:
        receipt.update(kind="fsync-return-control", delays_ms=CONTROL_DELAYS_MS, measured_sets_per_arm=1)
    else:
        receipt.update(kind="natural", order=ORDER, connections=gc.live.CLIENTS, keys=gc.live.KEYS,
                       seed=gc.live.SEED, stride=gc.STRIDE, duration_s=gc.DURATION, warmup_s=gc.WARMUP,
                       rate=gc.RATE, read_percent=95)

    status = 2
    with Path(setup["performance_lock"]).open("rb") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        mirror = args.mirror.resolve()
        mirror.mkdir(parents=True, exist_ok=False)
        args.host_root = args.host_root.resolve()
        args.host_root.mkdir(parents=True, exist_ok=False)
        root = Path(tempfile.mkdtemp(prefix="kerniva-golog-host.", dir=home))
        print(f"ext4 {root}", flush=True)
        options = SimpleNamespace(bin=bins, stage="golog-host-C", duration=gc.DURATION, warmup=gc.WARMUP,
                                  guest_counters=True, fixture_trace=True, stop_opening=gc.live.now() + timedelta(minutes=12),
                                  child_process_options=gc.settled.child_process_options())
        ports = set()
        try:
            gc.settled.write_json(mirror / "plan.json", receipt)
            if args.controls:
                for delay in CONTROL_DELAYS_MS:
                    receipt["windows"].append(capture_control(root, options, delay, args))
                    gc.settled.write_json(root / "run.json", receipt)
            else:
                with patch.object(gc.stall, "GuestSampler", PassiveGuestSampler):
                    for position, mode in enumerate(ORDER, 1):
                        receipt["windows"].append(capture(root, options, position, mode, ports, args))
                        gc.settled.write_json(root / "run.json", receipt)
            status = 0
        finally:
            receipt.update(end=gc.stall.clock(), driver_exit_code=status)
            gc.settled.write_json(root / "run.json", receipt)
            gc.mirror_evidence(root, mirror, status)
    return status


if __name__ == "__main__":
    sys.exit(main())
