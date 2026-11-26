#!/usr/bin/env python3
"""Capture kernel writer traces in isolated frozen-C gnet workloads."""
import argparse
from contextlib import ExitStack
from datetime import timedelta
import fcntl
import os
import re
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
from unittest.mock import patch

from golog_batch_capture import gc, run_window
from golog_batch_report import require


ORDER = ("off", "on", "on", "off")


class KernelTrace:
    def __init__(self, folder, primary, storage, args, duration):
        self.folder = folder
        self.output = folder / "kernel"
        script = Path(__file__).with_name("golog_kernel_writer_trace.py").resolve()
        self.root_command = ["wsl.exe", "--distribution", args.distribution, "--user", "root", "--exec"]
        self.argv = [*self.root_command, "timeout", "--signal=INT", "--kill-after=10s", "125s",
                     "python3", "-B", str(script), "--pid", str(primary.pid), "--active-log",
                     str(storage / "db_0" / "active.log"), "--output", str(self.output),
                     "--duration-sec", str(duration + 20)]
        if args.journal_stacks:
            self.argv.append("--journal-stacks")
        self.child = None

    def start(self):
        with (self.folder / "kernel-driver.log").open("wb") as log:
            self.child = subprocess.Popen(self.argv, stdin=subprocess.DEVNULL, stdout=log, stderr=log)
        deadline = time.monotonic() + 15
        while not (self.output / "ready.json").is_file():
            if self.child.poll() is not None or time.monotonic() >= deadline:
                raise RuntimeError(f"kernel trace did not become ready, see {self.folder / 'kernel-driver.log'}")
            time.sleep(0.02)

    def remove_failed_instance(self):
        ready = self.output / "ready.json"
        if not ready.is_file():
            return
        instance = gc.live.read_json(ready).get("instance", "")
        if not re.fullmatch(r"/sys/kernel/tracing/instances/kerniva-writer-[a-f0-9]{32}", instance):
            return
        code = (
            "from pathlib import Path\n"
            "import sys\n"
            "instance = Path(sys.argv[1])\n"
            "if instance.exists():\n"
            "    (instance / 'tracing_on').write_text('0')\n"
            "    instance.rmdir()\n"
        )
        completed = subprocess.run([*self.root_command, "python3", "-B", "-c", code, instance],
                                   capture_output=True, text=True, timeout=10)
        gc.settled.write_json(self.folder / "kernel-emergency-cleanup.json",
                              {"instance": instance, "returncode": completed.returncode,
                               "stderr": completed.stderr})

    def stop(self):
        child, self.child = self.child, None
        if child is None:
            return
        deadline = time.monotonic() + 90
        while child.poll() is None and not self.output.is_dir() and time.monotonic() < deadline:
            time.sleep(0.02)
        if child.poll() is None and self.output.is_dir():
            (self.output / "stop").touch()
        try:
            child.wait(timeout=max(1, deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            # The root-side timeout sends INT to unwind the helper's tracefs instance.
            child.wait(timeout=55)
        if self.output.is_dir():
            for source in self.output.iterdir():
                if not source.is_file() or source.name == "stop":
                    continue
                if child.returncode == 0 or source.name in ("receipt.json", "ready.json"):
                    shutil.copyfile(source, self.folder / f"kernel-{source.name}")
        if child.returncode != 0:
            self.remove_failed_instance()
            raise RuntimeError(f"kernel trace returned {child.returncode}")
        require((self.folder / "kernel-receipt.json").is_file() and
                (self.folder / "kernel-trace.txt").is_file(), "kernel trace output missing")


def capture(root, options, position, mode, ports, args):
    folder = root / f"{position:02d}-r95-q6000-A"
    primary = storage = trace = None
    if mode == "off":
        result = run_window(root, options, position, "C", ports)
    else:
        original_case = gc.live.case
        original_observe = gc.OBSERVE_CLIENT

        class MeasuredPrimary(gc.TOPOLOGY):
            def start_primary(self):
                nonlocal primary, storage
                primary = super().start_primary()
                storage = self.storage
                return primary

        def case(*case_args, before_release, **kwargs):
            def release():
                nonlocal trace
                before_release()
                require(primary is not None and primary.poll() is None, "primary missing before release")
                trace = KernelTrace(folder, primary, storage, args, options.duration)
                trace.start()
            return original_case(*case_args, before_release=release, **kwargs)

        def observe(*observe_args, **kwargs):
            try:
                return original_observe(*observe_args, **kwargs)
            finally:
                if trace is not None:
                    trace.stop()

        try:
            with patch.object(gc, "TOPOLOGY", MeasuredPrimary), patch.object(gc.live, "case", case), \
                    patch.object(gc, "OBSERVE_CLIENT", observe):
                result = run_window(root, options, position, "C", ports)
        finally:
            if trace is not None:
                trace.stop()

    require(result["pass"], f"{folder.name}: workload capture failed")
    client = gc.live.read_json(folder / "client.json")
    require(client["offered"] == options.duration * gc.RATE, "incomplete offer count")
    result.update(kernel_mode=mode, kernel_argv=trace.argv if trace else None,
                  offered=client["offered"], failures=client["failures"])
    gc.settled.write_json(folder / "capture.json", result)
    print(f"{folder.name}: kernel {mode}, sent {client['offered']}, failures {client['failures']}", flush=True)
    return result


def single_write(root, options, args):
    delay = 220 if args.stage == "single-delay" else 0
    folder = root / ("single-write-delay-220ms" if delay else "single-write")
    folder.mkdir()
    with ExitStack() as processes:
        class SinglePrimary(gc.TOPOLOGY):
            def start(self, name, *cmd, env=None):
                env = {**(self.env if env is None else env),
                       "KERNIVA_GOLOG_BATCH_TRACE": str(folder / "server-batches.jsonl")}
                config = Path(cmd[-1])
                settings = gc.live.read_json(config)
                settings["go_log_segment_age_seconds"] = 300
                gc.settled.write_json(config, settings)
                if delay:
                    cmd = ("strace", "-D", "-f", "-qq", "-yy", "-ttt", "-T", "-e", "trace=fsync",
                           "-e", f"inject=fsync:delay_exit={delay}ms", "-o", str(folder / "fsync.strace"), *cmd)
                return super().start(name, *cmd, env=env)

        run = SinglePrimary(folder, options.bin, options.bin / "kerniva-replica",
                            folder / "primary-data" / "go-log-v1", processes)
        run.process_options = options.child_process_options
        primary = run.start_primary()
        identity = gc.live.check.RUNNER.process_identity(primary.pid)
        trace = KernelTrace(folder, primary, run.storage, args, 5)
        try:
            trace.start()
            with gc.live.primary_client(run.primary, 0) as client:
                local_port = client.connection.getsockname()[1]
                started = gc.stall.clock()
                reply = client.command("SET", "kernel-trace:single-write", "fixture")
                finished = gc.stall.clock()
                require(reply == "OK", "single SET failed")
        finally:
            trace.stop()

    clean = gc.verify_clean({"processes": {"primary": identity}, "ports": {"primary": run.primary}}, run.primary)
    require(primary.returncode == 0 and not any(clean.values()), "single-write primary cleanup failed")
    if delay:
        syscall_entries = [(line, line.split()[1]) for line in (folder / "fsync.strace").read_text().splitlines()
                           if "/db_0/active.log>" in line and "fsync(" in line]
        matched = [line for line, utc in syscall_entries if
                   started["utc_unix_ns"] <= int(utc.replace(".", "")) * 1000 <= finished["utc_unix_ns"]]
        require(len(matched) == 1 and "(DELAYED)" in matched[0] and " = 0" in matched[0],
                "expected one successfully delayed active-log fsync")
    receipt = {"folder": folder.name, "start": started, "end": finished, "reply": reply,
               "kernel_argv": trace.argv, "local_port": local_port, "clean": clean,
               "strace_delay_exit_ms": delay}
    gc.settled.write_json(folder / "capture.json", receipt)
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--setup", type=Path, required=True)
    parser.add_argument("--bin", type=Path, required=True)
    parser.add_argument("--binary-manifest", type=Path, required=True)
    parser.add_argument("--mirror", type=Path, required=True)
    parser.add_argument("--distribution", default=os.environ.get("WSL_DISTRO_NAME"))
    parser.add_argument("--stage", choices=("single", "single-delay", "smoke", "natural"), required=True)
    parser.add_argument("--journal-stacks", action="store_true", help="record DB0 writer D-state stacks and journal phases")
    args = parser.parse_args()
    if args.journal_stacks and args.stage != "single":
        parser.error("--journal-stacks requires --stage single")
    require(args.distribution and gc.live.check.RUNNER.filesystem_type(Path.home()) == "ext4",
            "WSL ext4 and distribution identity required")
    inherited = {"GODEBUG", "GOGC", "GOMEMLIMIT", "GOMAXPROCS", "GOTRACEBACK", "KERNIVA_GOLOG_BATCH_TRACE"}
    require(not inherited.intersection(os.environ), "inherited Go or trace overrides")
    setup = gc.live.read_json(args.setup)
    bins = args.bin.resolve()
    old = gc.verified_group(Path(setup["root"]) / "bin", args.setup.resolve().parent / "diagnostic-binaries.sha256")
    new = gc.verified_group(bins, args.binary_manifest)
    require(old.keys() == new.keys() and all(old[key] == new[key] for key in old.keys() -
            {"kerniva-server", "kerniva-bench"}), "unexpected frozen binary dependency")
    order = ORDER if args.stage == "natural" else ("on",)
    duration = gc.DURATION if args.stage == "natural" else 5
    receipt = {"argv": sys.argv, "stage": args.stage, "order": order, "duration_s": duration,
               "warmup_s": gc.WARMUP, "rate": gc.RATE, "keys": gc.live.KEYS, "read_percent": 95,
               "connections": gc.live.CLIENTS, "group_max": 8, "stride": gc.STRIDE,
               "binaries": new, "runner_sha256": gc.stall.digest(Path(__file__)),
               "kernel_helper_sha256": gc.stall.digest(Path(__file__).with_name("golog_kernel_writer_trace.py")),
               "setup": setup,
               "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
               "start": gc.stall.clock(), "windows": []}
    status = 2
    with Path(setup["performance_lock"]).open("rb") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        mirror = args.mirror.resolve()
        mirror.mkdir(parents=True, exist_ok=False)
        root = Path(tempfile.mkdtemp(prefix="kerniva-kernel-writer.", dir=Path.home()))
        print(f"ext4 {root}", flush=True)
        options = SimpleNamespace(bin=bins, stage="golog-kernel-writer-C", duration=duration, warmup=gc.WARMUP,
                                  guest_counters=False, stop_opening=gc.live.now() + timedelta(minutes=8),
                                  child_process_options=gc.settled.child_process_options())
        try:
            gc.settled.write_json(mirror / "plan.json", receipt)
            if args.stage in ("single", "single-delay"):
                receipt["windows"].append(single_write(root, options, args))
            else:
                ports = set()
                for position, mode in enumerate(order, 1):
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
