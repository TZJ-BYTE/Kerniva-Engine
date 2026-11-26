#!/usr/bin/env python3
"""Compare existing Go-log group caps in short, controlled gnet windows."""

import argparse
from collections import Counter
from contextlib import ExitStack
import fcntl
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
from types import SimpleNamespace

import golog_gc_capture as gc
from golog_batch_report import describe, records, require


KEYS = 4096
DURATION = 5
DELAY_MS = 32
ORDER = ((8, 0), (8, DELAY_MS), (16, DELAY_MS), (16, DELAY_MS), (8, DELAY_MS), (16, 0))
STRACE = ["strace", "-D", "-f", "-qq", "-yy", "-ttt", "-T", "-e", "trace=fsync", "-e", "signal=none"]
ACTIVE_SYNC = re.compile(r"^\d+ (\d+)\.(\d{6}) fsync\(\d+<[^>]+/db_0/active\.log>")


def command_completed_from_seed(run, delay, group, trace, use_seccomp):
    check = gc.live.check
    settings = check.RECOVERY.configuration(run.storage.parent, run.root / "server.log", run.primary,
                                             SimpleNamespace(network="gnet", segment_bytes=4 << 20))
    settings.update(go_log_segment_age_seconds=300, go_log_group_max=group)
    config = run.root / "config.json"
    gc.settled.write_json(config, settings)
    argv = [str(run.binary / "kerniva-server"), "-config", str(config)]
    env = run.env.copy()
    if trace:
        prefix = STRACE + (["--seccomp-bpf"] if use_seccomp else []) + ["-o", str(run.root / "fsync.strace")]
        if delay:
            prefix += ["-e", f"inject=fsync:delay_exit={delay}ms"]
        argv = prefix + argv
        env["KERNIVA_GOLOG_BATCH_TRACE"] = str(run.root / "server-batches.jsonl")
    primary = run.start("primary", *argv, env=env)
    run.wait("gnet primary ready", lambda: check.RUNNER.command(run.primary, "PING", timeout=1) == "PONG", timeout=12)
    return primary, argv


def bench(run, duration, measure):
    argv = [str(run.binary / "kerniva-bench"), "-mode", "mixed", "-addr", f"127.0.0.1:{run.primary}",
            "-network", "gnet", "-durability", "sync", "-clients", str(64 if measure else 1), "-seed", "42",
            "-keys", str(KEYS), "-rate", str(6000 if measure else 1), "-read-percent", "95" if measure else "100",
            "-duration", f"{duration}s", "-warmup", "0s", "-timeout", "2s"]
    if measure:
        argv += ["-prefill=false", "-request-trace-stride", "16", "-request-trace-out", str(run.root / "client-trace.jsonl"),
                 "-histogram-out", str(run.root / "client-hist.json")]
    with (run.root / "client.json").open("wb") as stdout, (run.root / "client.stderr.log").open("wb") as stderr:
        subprocess.run(argv, cwd=run.root, env=run.env, stdout=stdout, stderr=stderr, stdin=subprocess.DEVNULL,
                       timeout=duration + 12, check=True, **run.process_options)
    return argv


def stats(run):
    raw = gc.live.check.RUNNER.server_info(run.primary)
    config = gc.live.read_json(run.root / "config.json")
    require((raw.get("storage_backend"), raw.get("persistence_durability"), config.get("persistence_write_mode"))
            == ("go-log", "sync", "strong"), "unexpected storage settings")
    return {field: int(raw[field]) for field in ("last_sequence", "sync_count", "log_group_max",
                                                "log_group_batches", "log_group_requests")}


def audit(folder, before, after, delay, group):
    client = gc.live.read_json(folder / "client.json")
    sampled = list(records(folder / "client-trace.jsonl"))
    coverage = sampled[-1]
    require(coverage.get("type") == "summary" and coverage["offered"] == DURATION * 6000 == client["offered"], "client trace total")
    slow = {op: sum(second[i]["slow"] for worker in coverage["by_worker_second_get_set"] for second in worker)
            for i, op in enumerate(("get", "set"))}
    server = list(records(folder / "server-batches.jsonl"))
    writes = [row for row in server[1:-1] if row.get("command") == "SET"]
    require(server[0].get("type") == "meta" and server[-1].get("type") == "summary" and server[-1]["drops"] == 0,
            "server trace completeness")
    durable = [row for row in writes if row["batch"].get("sequence") and not row["batch"].get("failed")]
    sequence = {row["batch"]["sequence"] for row in durable}
    require(len(sequence) == len(durable) and sequence == set(range(before["last_sequence"] + 1, after["last_sequence"] + 1)),
            "trace sequence range differs from live Go-log counters")
    batches = {}
    for row in durable:
        batch = row["batch"]
        shared = {key: val for key, val in batch.items() if key != "sequence"}
        require(batches.setdefault(batch["id"], shared) == shared and 0 < batch["size"] <= group, "batch consistency or cap")
    sizes = Counter(batch["size"] for batch in batches.values())
    require(sum(n * count for n, count in sizes.items()) == len(durable)
            and len(batches) == after["sync_count"] - before["sync_count"], "batch sum differs from commits or sync calls")
    if client["failures"] == 0:
        require(len(durable) == client["set"]["succeeded"], "acknowledgments differ from committed sequence")
    origin = server[0]["origin_unix_ns"]
    first = min(batch["sync_start_ns"] for batch in batches.values()) + origin
    last = max(batch["sync_end_ns"] for batch in batches.values()) + origin
    fsync = []
    for line in (folder / "fsync.strace").read_text().splitlines():
        if match := ACTIVE_SYNC.match(line):
            entry = int(match[1]) * 10**9 + int(match[2]) * 1000
            if first - 2_000_000 <= entry <= last:
                fsync.append(line)
    require(len(fsync) == len(batches) and all(("(DELAYED)" in line) == bool(delay) and " = 0" in line for line in fsync),
            "did not match an active-log syscall per measured batch")
    pending = peak = 0
    for _, delta in sorted([(row["queue_ns"], 1) for row in durable]
                           + [(row["batch"]["dequeue_ns"], -1) for row in durable]):
        pending += delta
        peak = max(peak, pending)
    return {"cap": group, "delay_ms": delay, "failures": client["failures"], "offered": client["offered"],
            "succeeded": client["succeeded"], "succeeded_set": client["set"]["succeeded"],
            "success_rate": client["actual_success_rate"], "window_success_rate": client["window_success_rate"],
            "slow_ge_100ms": slow, "client_trace_drops": coverage["drops"],
            "client_queue_us": {op: client[op]["client_queue_delay"] for op in ("get", "set")},
            "server_queue_ns": describe([row["batch"]["dequeue_ns"] - row["queue_ns"] for row in durable]),
            "sync_ns": describe([b["sync_end_ns"] - b["sync_start_ns"] for b in batches.values()]),
            "max_callers_waiting_for_drain": peak, "sizes": dict(sorted(sizes.items())),
            "first_last_sequence": [min(sequence), max(sequence)], "confirmed_sequences": len(sequence), "matched_fsyncs": len(fsync)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--setup", type=Path, required=True)
    parser.add_argument("--bin", type=Path, required=True)
    parser.add_argument("--bin-manifest", type=Path, required=True)
    parser.add_argument("--mirror", type=Path, required=True)
    args = parser.parse_args()
    setup, home = gc.live.read_json(args.setup), Path.home()
    check = gc.live.check
    require(check.RUNNER.filesystem_type(home) == "ext4", "need WSL ext4")
    require(not ({"GODEBUG", "GOGC", "GOMEMLIMIT", "GOMAXPROCS", "GOTRACEBACK"} & os.environ.keys()), "inherited Go options")
    hashes = gc.verified_group(args.bin.resolve(), args.bin_manifest)
    driver = {"argv": sys.argv, "runner_sha256": gc.stall.digest(Path(__file__)), "bin_hashes": hashes,
              "start": gc.stall.clock(), "keys": KEYS, "duration_s": DURATION, "plan": ORDER, "windows": []}
    status = 2
    with Path(setup["performance_lock"]).open("rb") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        mirror = args.mirror.resolve()
        mirror.mkdir(parents=True, exist_ok=False)
        root = Path(tempfile.mkdtemp(prefix="kerniva-group-cap.", dir=home))
        print(f"ext4 {root}", flush=True)
        options = gc.settled.child_process_options()

        def start(path, context):
            run = gc.TOPOLOGY(path, args.bin.resolve(), args.bin / "kerniva-replica", path / "data" / "go-log-v1", context)
            run.process_options = options
            return run

        try:
            probe = subprocess.run(STRACE + ["--seccomp-bpf", "-e", "inject=fsync:delay_exit=1ms", "-o", str(root / "strace-preflight.trace"),
                                             "python3", "-c", "import os; f=os.open('/dev/null',os.O_RDONLY);\ntry: os.fsync(f)\nexcept OSError: pass"],
                                   capture_output=True, text=True, timeout=5)
            use_seccomp = probe.returncode == 0 and not probe.stderr.strip()
            driver.update(seccomp_bpf_requested=use_seccomp, seccomp_probe=probe.stderr, seccomp_probe_exit=probe.returncode)
            seed = root / "seed"
            seed.mkdir()
            with ExitStack() as processes:
                run = start(seed, processes)
                command_completed_from_seed(run, 0, 8, False, use_seccomp)
                driver["seed_argv"] = bench(run, 1, False)
                driver["seed_stats"] = stats(run)
            for index, (group, delay) in enumerate(ORDER, 1):
                folder = root / f"{index:02d}-group{group}-delay{delay}ms"
                folder.mkdir()
                shutil.copytree(seed / "data", folder / "data")
                with ExitStack() as processes:
                    run = start(folder, processes)
                    primary, argv = command_completed_from_seed(run, delay, group, True, use_seccomp)
                    identity = check.RUNNER.process_identity(primary.pid)
                    before = stats(run)
                    require(before["last_sequence"] == driver["seed_stats"]["last_sequence"] and before["log_group_max"] == group, "seed or group mismatch")
                    client_argv = bench(run, DURATION, True)
                    after = stats(run)
                require(primary.returncode == 0, f"primary could not stop cleanly ({primary.returncode})")
                clean = gc.verify_clean({"processes": {"primary": identity}, "ports": {"primary": run.primary}}, run.primary)
                require(not any(clean.values()), "surviving primary or listener")
                result = audit(folder, before, after, delay, group)
                result.update(folder=folder.name, primary_argv=argv, client_argv=client_argv, info_before=before, info_after=after, clean=clean)
                gc.settled.write_json(folder / "window.json", result)
                driver["windows"].append(result)
                print(f"{folder.name}: {result['slow_ge_100ms']} >=100ms", flush=True)
            status = 0
        finally:
            driver.update(end=gc.stall.clock(), driver_exit_code=status)
            gc.settled.write_json(root / "run.json", driver)
            gc.mirror_evidence(root, mirror, status)
    return status


if __name__ == "__main__":
    sys.exit(main())
