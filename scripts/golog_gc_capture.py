#!/usr/bin/env python3
"""Run an isolated matched A-arm crossover with a server runtime trace."""
import argparse
from contextlib import ExitStack
from datetime import timedelta
import fcntl
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
from unittest.mock import patch
from urllib.request import urlopen

import golog_replica_live_load as live
import golog_replica_set_tail as settled
import golog_replica_stall_run as stall

TOPOLOGY = live.Topology
OBSERVE_CLIENT = live.observe_client
DURATION = 30
WARMUP = 5
RATE = 6000
STRIDE = 16
TRACE_SECONDS = 35


def verified_group(directory, manifest):
    hashes = {}
    for row in manifest.read_text().splitlines():
        expected, name = row.split()
        actual = stall.digest(directory / name)
        if expected != actual:
            raise ValueError(f"{name}: expected {expected}, found {actual}")
        hashes[name] = actual
    return hashes


def verify_clean(case, pprof_port):
    survivors = {}
    for label, expected in case["processes"].items():
        observed = live.check.RUNNER.process_identity(expected["pid"])
        if observed and observed["start_ticks"] == expected["start_ticks"]:
            survivors[label] = observed
    listeners = subprocess.check_output(["ss", "-Hltnp"], text=True).splitlines()
    ports = {str(port) for port in (*case["ports"].values(), pprof_port)}
    return {"survivors": survivors,
            "listeners": [line for line in listeners if line.split()[3].rsplit(":", 1)[1] in ports]}


def primary_gc_info(port):
    with live.primary_client(port, 0) as client:
        raw = client.command("INFO")
    return {line.split(":", 1)[0]: line.split(":", 1)[1]
            for line in raw.splitlines() if ":" in line}


def run_window(root, args, position, mode, used_ports):
    label = f"{position:02d}-r95-q6000-A"
    folder = root / label
    receipt = {"mode": mode, "folder": label, "start": stall.clock()}
    sampler = None
    trace_client = None
    pprof_port, = live.check.ports(1)
    while pprof_port in used_ports:
        pprof_port, = live.check.ports(1)
    used_ports.add(pprof_port)
    endpoint = f"http://127.0.0.1:{pprof_port}"
    receipt["pprof_endpoint"] = endpoint
    trace_path = folder / "server-runtime.trace"
    partial_trace = trace_path.with_suffix(".partial")

    class PrimaryWithProfiler(TOPOLOGY):
        def start(self, label, *cmd, env=None):
            if label == "primary":
                cfg = Path(cmd[-1])
                settings = live.read_json(cfg)
                settings.update(pprof_enabled=True, pprof_addr=f"127.0.0.1:{pprof_port}")
                settled.write_json(cfg, settings)
                receipt["primary_port"] = self.primary
                receipt["server_argv"] = list(map(str, cmd))
                receipt["server_environment"] = {"KERNIVA_ENGINE_LIBRARY": self.env["KERNIVA_ENGINE_LIBRARY"]}
            return super().start(label, *cmd, env=env)

    class Observer(stall.Observers):
        def __init__(self, *positional, **optional):
            nonlocal sampler
            super().__init__(*positional, **optional, detail=False, diagnostic=args.guest_counters, duration=DURATION)
            sampler = self.guest

    def child(argv, **options):
        argv.extend(("-request-trace-out", str(folder / "client-trace.jsonl"),
                     "-request-trace-stride", str(STRIDE)))
        return subprocess.Popen(argv, **options)

    def finish_trace():
        if trace_client is None or "trace_exit" in receipt:
            return
        try:
            receipt["trace_exit"] = trace_client.wait(timeout=12)
        except subprocess.TimeoutExpired:
            trace_client.kill()
            receipt["trace_exit"] = trace_client.wait()
        receipt["trace_done"] = stall.clock()
        if receipt["trace_exit"] == 0 and partial_trace.stat().st_size > 0:
            partial_trace.replace(trace_path)

    def observe(*positional, **optional):
        try:
            status = OBSERVE_CLIENT(*positional, **optional)
            receipt["gc_info_after"] = stall.observation(primary_gc_info, receipt["primary_port"])
            return status
        finally:
            try:
                if sampler is not None:
                    receipt["guest_sampler"] = sampler.finish()
            finally:
                finish_trace()

    def release():
        nonlocal trace_client
        with urlopen(endpoint + "/debug/pprof/", timeout=2) as response:
            receipt["pprof_index_http_status"] = response.status
        if mode == "on":
            receipt["trace_argv"] = ["curl", "--noproxy", "*", "--silent", "--show-error", "--fail",
                                     "--max-time", "40", "--output", str(partial_trace),
                                     endpoint + f"/debug/pprof/trace?seconds={TRACE_SECONDS}"]
            with (folder / "trace.stderr.log").open("wb") as stderr:
                receipt["trace_invocation"] = stall.clock()
                trace_client = subprocess.Popen(receipt["trace_argv"], stdin=subprocess.DEVNULL,
                                                stderr=stderr, **args.child_process_options)
                receipt["trace_pid"] = trace_client.pid
        time.sleep(0.55)
        if trace_client is not None and trace_client.poll() is not None:
            raise RuntimeError("trace request exited before release")
        receipt["gc_info_before"] = stall.observation(primary_gc_info, receipt["primary_port"])
        receipt["release_callback_done"] = stall.clock()

    child_module = SimpleNamespace(Popen=child, DEVNULL=subprocess.DEVNULL)
    try:
        with patch.object(live, "Topology", PrimaryWithProfiler), patch.object(live, "subprocess", child_module),                 patch.object(live, "observe_client", observe):
            receipt["case_status"] = live.case(root, args, (95, RATE), "A", position, used_ports,
                                               components=settled.ARMS["A"], observer_factory=Observer,
                                               before_release=release)
    finally:
        if sampler is not None and sampler.pool is not None:
            receipt["guest_sampler"] = sampler.finish()
        finish_trace()
        receipt["end"] = stall.clock()
        if folder.is_dir() and (folder / "case.json").is_file():
            case = live.read_json(folder / "case.json")
            receipt["clean"] = verify_clean(case, pprof_port)
            receipt["pass"] = (receipt.get("case_status") == "completed"
                               and (folder / "client-trace.jsonl").is_file()
                               and ("on" != mode or trace_path.is_file())
                               and not any(receipt["clean"].values()))
            settled.write_json(folder / "capture.json", receipt)
    return receipt


def run_calibration(root, args, used_ports):
    folder = root / "calibration"
    folder.mkdir()
    port, = live.check.ports(1)
    used_ports.add(port)
    endpoint = f"http://127.0.0.1:{port}"
    receipt = {"kind": "isolated forced GC during runtime trace, outside load matrix", "start": stall.clock()}
    with ExitStack() as children:
        class CalibrationTopology(TOPOLOGY):
            def start(self, label, *cmd, env=None):
                receipt["server_argv"] = list(map(str, cmd))
                return super().start(label, *cmd, env={**self.env, "KERNIVA_PPROF_ENABLED": "true",
                                                       "KERNIVA_PPROF_ADDR": f"127.0.0.1:{port}"})

        run = CalibrationTopology(folder, args.bin, args.bin / "kerniva-replica",
                                  folder / "primary-data" / "go-log-v1", children)
        run.process_options = args.child_process_options
        while used_ports.intersection(run.ports.values()):
            run.primary, run.source, run.a, run.b = live.check.ports(4)
            run.ports = dict(zip(("primary", "source", "a", "b"), (run.primary, run.source, run.a, run.b)))
        used_ports.update(run.ports.values())
        server = run.start_primary()
        receipt["server_identity"] = live.check.RUNNER.process_identity(server.pid)
        with urlopen(endpoint + "/debug/pprof/", timeout=2) as response:
            receipt["pprof_index_http_status"] = response.status
        receipt["gc_info_before"] = stall.observation(primary_gc_info, run.primary)
        receipt["trace_argv"] = ["curl", "--noproxy", "*", "--silent", "--show-error", "--fail",
                                 "--max-time", "10", "--output", str(folder / "server-runtime.trace"),
                                 endpoint + "/debug/pprof/trace?seconds=4"]
        with (folder / "trace.stderr.log").open("wb") as stderr:
            receipt["trace_invocation"] = stall.clock()
            trace = subprocess.Popen(receipt["trace_argv"], stdin=subprocess.DEVNULL,
                                     stderr=stderr, **args.child_process_options)
        children.callback(live.check.stop, trace)
        time.sleep(0.6)
        receipt["force_command"] = endpoint + "/debug/pprof/heap?gc=1"
        receipt["force_start"] = stall.clock()
        with urlopen(receipt["force_command"], timeout=5) as response:
            receipt["force_http_status"] = response.status
            (folder / "forced-heap.pprof").write_bytes(response.read())
        receipt["force_end"] = stall.clock()
        receipt["gc_info_after"] = stall.observation(primary_gc_info, run.primary)
        receipt["trace_exit"] = trace.wait(timeout=10)
        receipt["trace_done"] = stall.clock()
    receipt["server_exit"] = server.returncode
    case = {"processes": {"primary": receipt["server_identity"]}, "ports": {"primary": run.primary}}
    receipt["clean"] = verify_clean(case, port)
    receipt["pass"] = receipt["trace_exit"] == 0 and not any(receipt["clean"].values())
    receipt["end"] = stall.clock()
    settled.write_json(folder / "capture.json", receipt)
    return receipt


def mirror_evidence(root, mirror, status):
    verified = {}
    for base in sorted(root.iterdir()):
        files = base.iterdir() if base.is_dir() else [base]
        for source in sorted(files):
            if not source.is_file():
                continue
            relative = source.relative_to(root)
            target = mirror / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            partial = target.with_suffix(target.suffix + ".copying")
            expected = stall.digest(source)
            shutil.copyfile(source, partial)
            if stall.digest(partial) != expected:
                raise RuntimeError(f"readback mismatch {relative}")
            partial.replace(target)
            verified[relative.as_posix()] = expected
    settled.write_json(mirror / "ext4-readback.json",
                       {"ext4": str(root), "raw_sha256": verified, "driver_exit_code": status})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mirror", required=True, type=Path)
    parser.add_argument("--setup", required=True, type=Path)
    parser.add_argument("--order", default="off,on,on,off")
    parser.add_argument("--guest-counters", action="store_true")
    cli = parser.parse_args()
    plan = cli.order.split(",")
    if not 1 <= len(plan) <= 6 or not set(plan) <= {"on", "off"}:
        parser.error("order requires 1 to 6 off/on entries")
    previous = live.read_json(cli.setup)
    frozen = Path(previous["root"])
    lock_path = Path(previous["performance_lock"])
    mirror = cli.mirror.resolve()
    bins = frozen / "bin"
    source = frozen / "source" / "scripts"
    previous_output = cli.setup.resolve().parent
    home = Path.home()
    if live.check.RUNNER.filesystem_type(home) != "ext4":
        raise RuntimeError("capture requires ext4")
    profiles = {key: value for key, value in os.environ.items() if key in
                {"GODEBUG", "GOGC", "GOMEMLIMIT", "GOMAXPROCS", "GOTRACEBACK"}}
    if profiles:
        raise RuntimeError(f"inherited Go runtime settings: {list(profiles)}")
    result = {
        "setup": previous, "binaries": verified_group(bins, previous_output / "diagnostic-binaries.sha256"),
        "frozen_scripts": verified_group(source, previous_output / "stock-runner.sha256"),
        "stall_runner_sha256": stall.digest(Path(stall.__file__)), "runner_sha256": stall.digest(Path(__file__)),
        "argv": sys.argv, "order": plan, "guest_counters": cli.guest_counters,
        "duration_s": DURATION, "warmup_s": WARMUP, "rate": RATE, "stride": STRIDE,
        "read_percent": 95, "connections": live.CLIENTS, "keys": live.KEYS, "seed": live.SEED,
        "server_trace_seconds": TRACE_SECONDS, "windows": [], "start": stall.clock(),
        "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
        "initial_listeners": subprocess.check_output(["ss", "-Hltnp"], text=True).splitlines(),
    }
    status = 2
    with lock_path.open("rb") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        # A repeated invocation must preserve the raw evidence of the first run.
        mirror.mkdir(parents=True, exist_ok=False)
        root = Path(tempfile.mkdtemp(prefix="kerniva-golog-gc-20261126.", dir=home))
        print(f"ext4 {root}", flush=True)
        args = SimpleNamespace(bin=bins, stage="golog-gc", duration=DURATION, warmup=WARMUP,
                               guest_counters=cli.guest_counters,
                               stop_opening=live.now() + timedelta(seconds=len(plan) * 55 + 20),
                               child_process_options=settled.child_process_options())
        used_ports = set()
        settled.write_json(root / "run.json", result)
        try:
            result["calibration"] = run_calibration(root, args, used_ports)
            if not result["calibration"]["pass"]:
                raise RuntimeError("calibration capture failed")
            for position, mode in enumerate(plan, 1):
                window = run_window(root, args, position, mode, used_ports)
                result["windows"].append(window)
                settled.write_json(root / "run.json", result)
                if not window["pass"]:
                    break
            if len(result["windows"]) == len(plan) and all(window["pass"] for window in result["windows"]):
                status = 0
        finally:
            result["end"] = stall.clock()
            result["driver_exit_code"] = status
            settled.write_json(root / "run.json", result)
            mirror_evidence(root, mirror, status)
    return status


if __name__ == "__main__":
    sys.exit(main())
