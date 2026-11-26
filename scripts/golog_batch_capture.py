#!/usr/bin/env python3
"""Compare bounded Go-log batch observations on fresh WSL ext4 data."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from datetime import timedelta
import fcntl
import os
from pathlib import Path
import re
import socket
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
from unittest.mock import patch

import golog_gc_capture as gc


def recovery(root, options):
    """Reopen the gnet primary after SIGKILL and verify acknowledged writes."""
    live = gc.live
    check = live.check
    folder = root / "primary-crash-recovery"
    folder.mkdir()
    receipt = {"start": gc.stall.clock(), "keys": {}}
    with ExitStack() as children:
        run = live.Topology(folder, options.bin, options.bin / "kerniva-replica",
                            folder / "primary-data" / "go-log-v1", children)
        run.process_options = options.child_process_options
        primary = run.start_primary()
        receipt["primary"] = check.RUNNER.process_identity(primary.pid)
        with live.primary_client(run.primary, 0) as client:
            writes = {"batch-recovery:existing": "before", "batch-recovery:new": "value"}
            for key, value in writes.items():
                check.require(client.command("SET", key, value), "OK", f"acknowledge {key}")
            writes["batch-recovery:existing"] = "after"
            check.require(client.command("SET", "batch-recovery:existing", "after"),
                          "OK", "acknowledge overwrite")
            receipt["expected_sequence"] = live.sequence(client)
        check.stop(primary, crash=True)
        receipt["crash_exit"] = primary.returncode
        check.require(primary.returncode, -9, "primary exits by SIGKILL")
        restarted = run.start("primary-reopened", options.bin / "kerniva-server",
                              "-config", folder / "config.json")
        receipt["reopened"] = check.RUNNER.process_identity(restarted.pid)
        run.wait("reopened primary listening",
                 lambda: check.RUNNER.command(run.primary, "PING", timeout=1) == "PONG")
        with live.primary_client(run.primary, 0) as client:
            for key, value in writes.items():
                recovered = client.command("GET", key)
                receipt["keys"][key] = {"expected": value, "recovered": recovered}
                check.require(recovered, value, f"recover {key}")
            receipt["recovered_sequence"] = live.sequence(client)
            check.require(receipt["recovered_sequence"], receipt["expected_sequence"], "recover sequence")
            check.require(client.command("SET", "batch-recovery:reopened", "ready"), "OK", "resume writes")
            receipt["resumed_sequence"] = live.sequence(client)
            check.require(receipt["resumed_sequence"], receipt["expected_sequence"] + 1, "resume sequence")
    receipt["clean"] = gc.verify_clean(
        {"processes": {"primary": receipt["primary"], "reopened": receipt["reopened"]},
         "ports": {"primary": run.primary}}, run.primary)
    receipt["end"] = gc.stall.clock()
    receipt["pass"] = not any(receipt["clean"].values())
    gc.settled.write_json(folder / "recovery.json", receipt)
    return receipt


def slow_sync_fixture(root, args, label, delay, commands=(None, ("SELECT", "1"), ("INFO",))):
    """Exercise the real gnet path with an externally delayed fsync return."""
    live = gc.live
    check = live.check
    folder = root / label
    folder.mkdir()
    receipt = {"delay_exit_ms": delay, "start": gc.stall.clock()}

    class TracedPrimary(gc.TOPOLOGY):
        def start(self, name, *cmd, env=None):
            config = Path(cmd[-1])
            settings = live.read_json(config)
            settings["go_log_segment_age_seconds"] = 300
            gc.settled.write_json(config, settings)
            tracer = ["strace", "-D", "-f", "-qq", "-yy", "-ttt", "-T", "-e", "trace=fsync",
                      "-o", str(folder / "fsync.strace")]
            if delay:
                tracer.extend(("-e", f"inject=fsync:delay_exit={delay}ms"))
            receipt["argv"] = [*tracer, *map(str, cmd)]
            environment = self.env.copy()
            if args.fixture_trace:
                environment["KERNIVA_GOLOG_BATCH_TRACE"] = str(folder / "server-batches.jsonl")
            return super().start(name, *receipt["argv"], env=environment)

    with ExitStack() as processes, ThreadPoolExecutor(max_workers=3) as pool:
        run = TracedPrimary(folder, args.bin, args.bin / "kerniva-replica",
                            folder / "primary-data" / "go-log-v1", processes)
        run.process_options = args.child_process_options
        primary = run.start_primary()
        receipt["primary"] = check.RUNNER.process_identity(primary.pid)
        with live.primary_client(run.primary, 0) as primary_client:
            check.require(primary_client.command("SET", "sync-fixture:write", "before"), "OK", "seed write")
        with live.primary_client(run.primary, 1) as secondary_client:
            check.require(secondary_client.command("SET", "sync-fixture:read", "ready"), "OK", "seed read")
        with ExitStack() as connections:
            def connect():
                connection = connections.enter_context(socket.create_connection(("127.0.0.1", run.primary), timeout=5))
                client = connections.enter_context(check.RECOVERY.RESP(connection))
                return connection.getsockname()[1], client

            def call(endpoint, command):
                port, client = endpoint
                start = gc.stall.clock()
                reply = client.command(*command)
                end = gc.stall.clock()
                return {"local_port": port, "command": command, "start": start, "end": end, "reply": reply}

            writer, reader = connect(), connect()
            check.require(reader[1].command("SELECT", "1"), "OK", "reader must already use DB1")
            receipt["requests"] = []
            for command in commands:
                observer = None if command is None else connect()
                write = pool.submit(call, writer, ("SET", "sync-fixture:write", str(len(receipt["requests"]))))
                time.sleep(0.075)
                observer_call = None if observer is None else pool.submit(call, observer, command)
                time.sleep(0.075)
                read = pool.submit(call, reader, ("GET", "sync-fixture:read"))
                row = {"write": write.result(), "observer": observer_call.result() if observer_call else None,
                       "read": read.result()}
                check.require(row["write"]["reply"], "OK", "DB0 overwrite")
                check.require(row["read"]["reply"], "ready", "DB1 read")
                if command == ("INFO",):
                    check.require("storage_backend:go-log" in row["observer"]["reply"], True, "INFO reply")
                elif command == ("PING",):
                    check.require(row["observer"]["reply"], "PONG", "PING reply")
                elif command is not None:
                    check.require(row["observer"]["reply"], "OK", "SELECT reply")
                receipt["requests"].append(row)
    receipt["primary_exit"] = primary.returncode
    receipt["clean"] = gc.verify_clean({"processes": {"primary": receipt["primary"]},
                                         "ports": {"primary": run.primary}}, run.primary)
    syscalls = (folder / "fsync.strace").read_text().splitlines()
    receipt["delayed_syscalls"] = sum("(DELAYED)" in row for row in syscalls)
    active = re.compile(r"^\d+ (\d+)\.(\d{6}) fsync\(\d+<[^>]+/db_0/active\.log>")
    entries = []
    for line in syscalls:
        if match := active.match(line):
            entries.append(int(match[1]) * 10**9 + int(match[2]) * 1000)
    for sample in receipt["requests"]:
        start = sample["write"]["start"]["utc_unix_ns"]
        end = sample["write"]["end"]["utc_unix_ns"]
        sample["db0_active_fsync_entries_utc_ns"] = [entry for entry in entries if start <= entry <= end]
        if delay:
            check.require(len(sample["db0_active_fsync_entries_utc_ns"]), 1, "one active-log fsync per measured SET")
            entry = sample["db0_active_fsync_entries_utc_ns"][0]
            for call_name in ("observer", "read"):
                call = sample[call_name]
                if call:
                    check.require(entry < call["start"]["utc_unix_ns"] < entry + delay * 10**6, True,
                                  f"{call_name} issued while fsync return is held")
    receipt["end"] = gc.stall.clock()
    receipt["pass"] = (not any(receipt["clean"].values())
                       and bool(receipt["delayed_syscalls"]) == bool(delay))
    gc.settled.write_json(folder / "fixture.json", receipt)
    return receipt


def run_window(root, args, position, mode, ports):
    trace = not mode.endswith("off")

    class Primary(gc.TOPOLOGY):
        def start(self, label, *cmd, env=None):
            if label == "primary":
                if trace:
                    env = {**(self.env if env is None else env),
                           "KERNIVA_GOLOG_BATCH_TRACE": str(self.root / "server-batches.jsonl")}
            return super().start(label, *cmd, env=env)

    options = SimpleNamespace(**vars(args))
    options.bin = args.baseline if mode.startswith("A") else args.bin
    with patch.object(gc, "TOPOLOGY", Primary):
        receipt = gc.run_window(root, options, position, "off", ports)
    rows = root / receipt["folder"] / "server-batches.jsonl"
    receipt.update(batch_trace=trace, group_max=8, gate_arm=mode[0])
    receipt["pass"] = receipt["pass"] and (rows.is_file() and rows.stat().st_size > 0 if trace else not rows.exists())
    gc.settled.write_json(root / receipt["folder"] / "capture.json", receipt)
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--setup", required=True, type=Path)
    parser.add_argument("--bin", required=True, type=Path)
    parser.add_argument("--baseline-bin", type=Path)
    parser.add_argument("--mirror", required=True, type=Path)
    parser.add_argument("--order", default="A,B,B,A")
    parser.add_argument("--recovery", action="store_true")
    parser.add_argument("--fixture", action="store_true")
    parser.add_argument("--fixture-no-trace", action="store_true", help="perform the syscall fixture without server batch stamps")
    cli = parser.parse_args()
    choices = {"A", "B", "Aoff", "Boff"}
    plan = cli.order.split(",") if cli.order else []
    if len(plan) > 6 or not set(plan) <= choices or not (plan or cli.recovery or cli.fixture):
        parser.error("use up to six comma-separated A/B/Aoff/Boff entries, --recovery, or --fixture")
    if any(mode.startswith("A") for mode in plan) and not cli.baseline_bin:
        parser.error("A windows require --baseline-bin")
    previous = gc.live.read_json(cli.setup)
    frozen = Path(previous["root"])
    reference = cli.setup.resolve().parent
    bins = cli.bin.resolve()
    old_binaries = gc.verified_group(frozen / "bin", reference / "diagnostic-binaries.sha256")
    new_binaries = {name: gc.stall.digest(bins / name) for name in old_binaries}
    baseline = cli.baseline_bin.resolve() if cli.baseline_bin else None
    baseline_binaries = {name: gc.stall.digest(baseline / name) for name in old_binaries} if baseline else None
    if baseline:
        for name in set(old_binaries) - {"kerniva-server"}:
            if baseline_binaries[name] != new_binaries[name]:
                raise RuntimeError(f"A/B binary mismatch for {name}")
    for name in set(old_binaries) - {"kerniva-server", "kerniva-bench"}:
        if old_binaries[name] != new_binaries[name]:
            raise RuntimeError(f"unchanged dependency {name} differs from sealed reference")
    home = Path.home()
    if gc.live.check.RUNNER.filesystem_type(home) != "ext4":
        raise RuntimeError("capture requires WSL ext4")
    profiles = {"GODEBUG", "GOGC", "GOMEMLIMIT", "GOMAXPROCS", "GOTRACEBACK"} & os.environ.keys()
    if profiles:
        raise RuntimeError(f"unexpected Go runtime profiles: {sorted(profiles)}")
    result = {
        "setup": previous, "argv": sys.argv, "order": plan, "binaries": new_binaries,
        "reference_binaries": old_binaries, "baseline_binaries": baseline_binaries,
        "frozen_scripts": gc.verified_group(frozen / "source" / "scripts", reference / "stock-runner.sha256"),
        "runner_sha256": gc.stall.digest(Path(__file__)), "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
        "duration_s": gc.DURATION, "warmup_s": gc.WARMUP, "read_percent": 95,
        "rate": gc.RATE, "stride": gc.STRIDE, "keys": gc.live.KEYS,
        "connections": gc.live.CLIENTS, "seed": gc.live.SEED, "windows": [], "start": gc.stall.clock(),
    }
    status = 2
    with Path(previous["performance_lock"]).open("rb") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        mirror = cli.mirror.resolve()
        mirror.mkdir(parents=True, exist_ok=False)
        root = Path(tempfile.mkdtemp(prefix="kerniva-golog-batches.", dir=home))
        print(f"ext4 {root}", flush=True)
        args = SimpleNamespace(bin=bins, baseline=baseline, stage="golog-batches", duration=gc.DURATION, warmup=gc.WARMUP,
                               guest_counters=False, fixture_trace=not cli.fixture_no_trace,
                               stop_opening=gc.live.now() + timedelta(seconds=len(plan) * 65 + 20),
                               child_process_options=gc.settled.child_process_options())
        ports = set()
        gc.settled.write_json(root / "run.json", result)
        try:
            for position, mode in enumerate(plan, 1):
                window = run_window(root, args, position, mode, ports)
                result["windows"].append(window)
                gc.settled.write_json(root / "run.json", result)
                if not window["pass"]:
                    raise RuntimeError(f"window failed: {window['folder']}")
            if cli.recovery:
                result["recovery"] = recovery(root, args)
                if not result["recovery"]["pass"]:
                    raise RuntimeError("crash recovery failed")
            if cli.fixture:
                result["controlled_fsync_fixture"] = [
                    slow_sync_fixture(root, args, "controlled-B-fsync-baseline", 0),
                    slow_sync_fixture(root, args, "controlled-B-fsync-delay", 350),
                ]
                if baseline:
                    baseline_args = SimpleNamespace(**vars(args))
                    baseline_args.bin = baseline
                    result["controlled_fsync_fixture"].append(
                        slow_sync_fixture(root, baseline_args, "controlled-A-fsync-delay", 350))
                if not all(item["pass"] for item in result["controlled_fsync_fixture"]):
                    raise RuntimeError("controlled fsync fixture failed")
            status = 0
        finally:
            result["end"] = gc.stall.clock()
            result["driver_exit_code"] = status
            gc.settled.write_json(root / "run.json", result)
            gc.mirror_evidence(root, mirror, status)
    return status


if __name__ == "__main__":
    sys.exit(main())
