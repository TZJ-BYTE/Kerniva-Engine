#!/usr/bin/env python3
"""Verify one WSL Go-log primary and two isolated asynchronous replicas."""

import argparse
from contextlib import ExitStack
import importlib.util
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import ProxyHandler, build_opener


SCRIPTS = Path(__file__).resolve().parent
REPO = SCRIPTS.parent
SPEC = importlib.util.spec_from_file_location("golog_recovery_bench", SCRIPTS / "golog-recovery-bench.py")
RECOVERY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RECOVERY)
RUNNER = RECOVERY.runner
HTTP = build_opener(ProxyHandler({}))


def require(actual, expected, operation):
    if actual != expected:
        raise RuntimeError(f"{operation} returned {actual!r}, expected {expected!r}")


def ports(count):
    with ExitStack() as holding:
        sockets = [holding.enter_context(socket.socket()) for _ in range(count)]
        for listener in sockets:
            listener.bind(("127.0.0.1", 0))
        return [listener.getsockname()[1] for listener in sockets]


def http(port, path, **query):
    url = f"http://127.0.0.1:{port}{path}?{urlencode(query)}"
    try:
        with HTTP.open(url, timeout=1) as response:
            return response.status, response.read()
    except HTTPError as response:
        with response:
            return response.code, response.read()


def wait_for(label, check, timeout):
    start = time.monotonic()
    last = None
    while time.monotonic() - start < timeout:
        try:
            last = check()
            if last:
                return last, round(1000 * (time.monotonic() - start), 2)
        except (FileNotFoundError, ConnectionError, TimeoutError, URLError) as error:
            last = str(error)
        time.sleep(0.05)
    raise TimeoutError(f"{label} did not complete within {timeout}s, last observation {last!r}")


def stop(process, crash=False):
    if process.poll() is None:
        if crash:
            process.kill()
        else:
            process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
            raise TimeoutError(f"process {process.pid} did not stop")


def run(root, args):
    environment = {key: value for key, value in os.environ.items() if not key.startswith("KERNIVA_")}
    environment["KERNIVA_ENGINE_LIBRARY"] = str(args.library)
    primary_port, source_port, a_port, b_port = ports(4)
    storage = root / "primary-data" / "go-log-v1"
    config_path = root / "config.json"
    config = RECOVERY.configuration(storage.parent, root / "primary.log", primary_port, args)
    config.update({"db_count": 2, "go_log_segment_age_seconds": 1, "go_log_group_max": 8})
    RUNNER.write_json(config_path, config)
    evidence = {"filesystem": RUNNER.filesystem_type(root), "ports": {
        "primary": primary_port, "source": source_port, "a": a_port, "b": b_port,
    }, "artifacts": {
        path.name: RUNNER.file_hash(path) for path in (args.server, args.library, args.maintainer, args.replica)
    }, "network": args.network, "events": []}

    def record(name, value):
        event = {"name": name, **value}
        if args.stress or args.source_restore or args.slow_link:
            event["utc"] = RUNNER.utc_now()
        evidence["events"].append(event)
        RUNNER.write_json(root / "result.json", evidence)

    def start(label, command):
        with (root / f"{label}.log").open("w", encoding="utf-8") as log:
            process = subprocess.Popen(command, cwd=root, env=environment,
                                       stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT)
        processes.callback(stop, process)
        return process

    def start_primary(label, settings=config_path):
        process = start(label, [args.server, "-config", settings])
        wait_for("primary PING", lambda: RUNNER.command(primary_port, "PING", timeout=1) == "PONG", args.timeout)
        info = RUNNER.server_info(primary_port)
        require(info.get("storage_backend"), "go-log", "primary backend")
        require(info.get("persistence_durability"), "sync", "primary durability")
        return process

    def start_follower(name, port, log, upstream_port=source_port, *, pull_idle_timeout=None):
        command = [args.replica, "follow", "--root", root / name,
                   "--source", f"http://127.0.0.1:{upstream_port}",
                   "--listen", f"127.0.0.1:{port}", "--dbs", "0,1", "--interval", "100ms"]
        if pull_idle_timeout is not None:
            command.extend(["--pull-idle-timeout", pull_idle_timeout])
        return start(log, command)

    def write(commands):
        with socket.create_connection(("127.0.0.1", primary_port), timeout=args.timeout) as sock:
            with RECOVERY.RESP(sock) as client:
                for operation, expected in commands:
                    require(client.command(*operation), expected, " ".join(operation[:2]))

    def statuses(port, sequences):
        result = {}
        for db, sequence in enumerate(sequences):
            status, response = http(port, "/status", db=db)
            if status != 200:
                return None
            state = json.loads(response)
            if not state["ready"] or state["last_error"] or state["sequence"] < sequence:
                return None
            result[str(db)] = state
        return result

    def await_replica(port, label, sequences, expected):
        def check():
            state = statuses(port, sequences)
            if state:
                for (db, key), value in expected.items():
                    status, body = http(port, "/get", db=db, key=key)
                    if value is None:
                        require(status, 404, f"{label} db{db} GET {key}")
                    else:
                        require((status, body), (200, value.encode("ascii")), f"{label} db{db} GET {key}")
            return state

        state, elapsed = wait_for(label, check, args.timeout)
        return {"wait_ms": elapsed, "states": state, "values_checked": len(expected)}

    def run_source_restore(primary, source):
        primary_id = storage / ".replica-source-id"
        follower_a = start_follower("a", a_port, "follower-a")
        write([(("SET", "anchor", "backup"), "OK")])
        seed = await_replica(a_port, "backup point", (1, 0), {(0, "anchor"): "backup"})
        original_id = seed["states"]["0"]["source"]
        require(seed["states"]["0"]["sequence"], 1, "backup cursor")
        require(primary_id.read_text(), original_id, "persisted source ID")

        def rotate(directory, label):
            result = subprocess.run([args.replica, "rotate-source-id", "--root", directory],
                                    cwd=root, env=environment, stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT, text=True, timeout=args.timeout)
            (root / f"{label}.log").write_text(result.stdout, encoding="utf-8")
            return result

        busy = rotate(storage, "rotate-live-source")
        if busy.returncode == 0 or "replication source lease is busy" not in busy.stdout:
            raise RuntimeError(f"expected live source lease rejection, got {busy.returncode}: {busy.stdout}")
        require(primary_id.read_text(), original_id, "live source identity preserved")

        # Copy a quiescent primary at LSN 1, including the old source ID.
        stop(primary)
        restored = root / "restored-data" / "go-log-v1"
        shutil.copytree(storage, restored)
        restored_id = restored / primary_id.name
        require(restored_id.read_text(), original_id, "backup copied source identity")
        record("copy_at_lsn_one", {"original_source": original_id, "snapshot": seed,
                                   "live_rotation_exit_code": busy.returncode})

        old_primary = start_primary("primary-original")
        write([(("SET", "branch", "old"), "OK")])
        old = await_replica(a_port, "original side of fork", (2, 0),
                            {(0, "anchor"): "backup", (0, "branch"): "old", (0, "tail"): None})
        require(old["states"]["0"]["sequence"], 2, "old branch cursor")
        pointer = root / "a" / "follower-v1" / "db_0" / "CURRENT"
        old_pointer = pointer.read_bytes()
        stop(follower_a, crash=True)
        stop(source)
        stop(old_primary)

        restored_config = root / "restored-config.json"
        RUNNER.write_json(restored_config, {**config, "data_dir": str(restored.parent)})
        start_primary("primary-restored", restored_config)
        # Seal LSN 2 separately so a same-ID pull at 2 could accept the unrelated LSN 3.
        for sequence, operation in ((2, ("SET", "branch", "new")), (3, ("SET", "tail", "restored"))):
            write([(operation, "OK")])
            segment = restored / "db_0" / f"log-{sequence:020d}-{sequence:020d}.seg"
            wait_for(f"restored seal at {sequence}", segment.is_file, args.timeout)
        changed = rotate(restored, "rotate-restored-source")
        require(changed.returncode, 0, "rotate restored source identity")
        new_id = restored_id.read_text()
        require(changed.stdout.strip(), new_id, "rotated identity output")
        if len(new_id) != 32 or new_id == original_id:
            raise RuntimeError(f"rotation did not produce a new source ID {new_id!r}")
        require(primary_id.read_text(), original_id, "old primary identity preserved")
        start("source-restored", [args.replica, "serve", "--root", restored,
                                  "--listen", f"127.0.0.1:{source_port}"])

        start_follower("a", a_port, "follower-a-reopened")

        def old_source_rejected():
            status, body = http(a_port, "/status", db=0)
            state = json.loads(body) if status == 200 else {}
            return state if "identity changed" in state.get("last_error", "") else None

        denied, elapsed = wait_for("old replica rejects restored lineage", old_source_rejected, args.timeout)
        require((denied["ready"], denied["source"], denied["sequence"]), (True, original_id, 2),
                "old replica keeps its published view")
        require(pointer.read_bytes(), old_pointer, "old replica CURRENT preserved")
        require(http(a_port, "/get", db=0, key="branch"), (200, b"old"), "old replica branch")
        require(http(a_port, "/get", db=0, key="tail")[0], 404, "old replica rejected forked tail")
        record("old_replica_rejects_restored_lineage", {"state": denied, "wait_ms": elapsed})

        start_follower("b", b_port, "follower-b-new-root")
        new = await_replica(b_port, "new replica from restored lineage", (3, 0),
                            {(0, "anchor"): "backup", (0, "branch"): "new", (0, "tail"): "restored"})
        require(new["states"]["0"]["sequence"], 3, "new branch cursor")
        for state in new["states"].values():
            require(state["source"], new_id, "new replica source")
        record("fresh_replica_installs_restored_lineage", new)

    with ExitStack() as processes:
        primary = start_primary("primary")
        source = start("source", [args.replica, "serve", "--root", storage,
                                  "--listen", f"127.0.0.1:{source_port}"])
        if args.source_restore:
            evidence["scenario"] = "source-restore"
            try:
                run_source_restore(primary, source)
            except Exception as error:
                record("source_restore_failure", {"error": repr(error)})
                raise
            evidence["passed"] = True
            RUNNER.write_json(root / "result.json", evidence)
            return
        if args.slow_link:
            from golog_replica_slow_link import run as run_slow_link

            evidence["scenario"] = f"slow-link-{args.slow_link}"
            try:
                run_slow_link(root, args, start=start, start_follower=start_follower, stop=stop,
                              write=write, await_replica=await_replica, record=record, require=require,
                              wait_for=wait_for, http=http, storage=storage, source_port=source_port,
                              a_port=a_port, b_port=b_port)
            except Exception as error:
                record("slow_link_failure", {"error": repr(error)})
                raise
            evidence["passed"] = True
            RUNNER.write_json(root / "result.json", evidence)
            return
        if args.stress:
            from golog_replica_stress import run as run_stress

            evidence["scenario"] = "concurrent-stress"
            try:
                run_stress(root, args, primary=primary, start=start, start_primary=start_primary,
                           start_follower=start_follower, stop=stop, record=record, require=require,
                           wait_for=wait_for, http=http, resp=RECOVERY.RESP, storage=storage,
                           primary_port=primary_port, source_port=source_port, a_port=a_port, b_port=b_port)
            except Exception as error:
                record("stress_failure", {"error": repr(error)})
                raise
            evidence["passed"] = True
            RUNNER.write_json(root / "result.json", evidence)
            return
        start_follower("a", a_port, "follower-a")
        follower_b = start_follower("b", b_port, "follower-b")
        write([
            (("SET", "same", "db0-old"), "OK"), (("SET", "deleted", "old"), "OK"),
            (("SET", "expiring", "temporary", "PX", "4000"), "OK"),
            (("SELECT", "1"), "OK"), (("SET", "same", "db1-old"), "OK"),
        ])
        expected = {(0, "same"): "db0-old", (0, "deleted"): "old",
                    (0, "expiring"): "temporary", (1, "same"): "db1-old", (1, "deleted"): None}
        initial_a = await_replica(a_port, "initial a", (3, 1), expected)
        initial_b = await_replica(b_port, "initial b", (3, 1), expected)
        require(initial_a["states"]["0"]["source"], initial_b["states"]["0"]["source"], "common source ID")
        record("initial", {"a": initial_a, "b": initial_b})
        stop(follower_b, crash=True)

        write([
            (("SET", "same", "db0-new"), "OK"), (("DEL", "deleted"), 1),
            (("SELECT", "1"), "OK"), (("SET", "same", "db1-new"), "OK"),
        ])
        expected.update({(0, "same"): "db0-new", (0, "deleted"): None, (1, "same"): "db1-new"})
        verify_incremental = dict(expected)
        verify_incremental.pop((0, "expiring"))
        incremental = await_replica(a_port, "incremental a", (5, 2), verify_incremental)
        for db in ("0", "1"):
            require(incremental["states"][db]["snapshot"], 0, f"db{db} incremental snapshot")
        record("a_incremental_while_b_offline", incremental)

        with (root / "maintainer.log").open("w", encoding="utf-8") as log:
            subprocess.run([args.maintainer, "--data-dir", storage, "--once"], cwd=root, env=environment,
                           stdout=log, stderr=subprocess.STDOUT, check=True, timeout=args.timeout)
        cutoffs = [int((storage / f"db_{db}" / "CURRENT").read_text()) for db in range(2)]
        for db, cutoff in enumerate(cutoffs):
            if cutoff <= initial_b["states"][str(db)]["sequence"]:
                raise RuntimeError(f"db{db} snapshot did not advance beyond the stopped replica")
            if any((storage / f"db_{db}").glob("log-*.seg")):
                raise RuntimeError(f"db{db} retained covered segments")
        record("snapshot_and_reclamation", {"cutoffs": cutoffs})

        write([
            (("SET", "tail", "post-snapshot-0"), "OK"),
            (("SELECT", "1"), "OK"), (("SET", "tail", "post-snapshot-1"), "OK"),
        ])
        wait_for("expiry", lambda: http(a_port, "/get", db=0, key="expiring")[0] == 404, args.timeout)
        expected.update({(0, "expiring"): None, (0, "tail"): "post-snapshot-0", (1, "tail"): "post-snapshot-1"})
        minimums = [cutoff + 1 for cutoff in cutoffs]
        after_a = await_replica(a_port, "a after snapshot", minimums, expected)
        follower_b = start_follower("b", b_port, "follower-b-restarted")
        after_b = await_replica(b_port, "b snapshot plus tail", minimums, expected)
        for db, cutoff in enumerate(cutoffs):
            state = after_b["states"][str(db)]
            require(state["snapshot"], cutoff, f"db{db} restored cutoff")
            if state["bootstraps"] < 1:
                raise RuntimeError(f"db{db} did not install a snapshot")
        record("b_snapshot_plus_tail", {"a": after_a, "b": after_b})

        stop(source)
        stop(primary, crash=True)
        start_primary("primary-restarted")
        start("source-restarted", [args.replica, "serve", "--root", storage,
                                   "--listen", f"127.0.0.1:{source_port}"])
        write([(("SET", "restart", "ready"), "OK")])
        expected[(0, "restart")] = "ready"
        minimums[0] = after_b["states"]["0"]["sequence"] + 1
        final_a = await_replica(a_port, "a after primary restart", minimums, expected)
        final_b = await_replica(b_port, "b after primary restart", minimums, expected)
        for final in (final_a, final_b):
            require(final["states"]["0"]["source"], initial_a["states"]["0"]["source"], "restarted source ID")
        record("primary_restart", {"a": final_a, "b": final_b})

        if args.rebase:
            stop(follower_b)
            history = [f"history:{key:04d}" for key in range(1024)]
            versions = 320

            def history_value(version):
                return f"{version:03d}" + "x" * 253

            def history_writes():
                for version in range(versions):
                    value = history_value(version)
                    yield ("MSET", *(word for key in history for word in (key, value))), "OK"

            write(history_writes())
            minimums[0] = final_a["states"]["0"]["sequence"] + versions
            for key in (history[0], history[len(history) // 2], history[-1]):
                expected[(0, key)] = history_value(versions - 1)
            caught_up = await_replica(a_port, "a with full local history", minimums, expected)
            a_database = root / "a" / "follower-v1" / "db_0"

            def local_generation():
                current = json.loads((a_database / "CURRENT").read_text())
                generation = a_database / current["generation"]
                bytes_in_segments = sum(path.stat().st_size for path in generation.glob("log-*.seg"))
                return generation, bytes_in_segments

            old_generation, old_bytes = local_generation()
            if old_bytes < 64 << 20:
                raise RuntimeError(f"the replica only received {old_bytes} bytes of local history")
            with (root / "maintainer-rebase.log").open("w", encoding="utf-8") as log:
                subprocess.run([args.maintainer, "--data-dir", storage, "--once"], cwd=root, env=environment,
                               stdout=log, stderr=subprocess.STDOUT, check=True, timeout=args.timeout)
            cutoff = int((storage / "db_0" / "CURRENT").read_text())
            require(cutoff, minimums[0], "online rebase cutoff")

            def observe_rebase():
                state = statuses(a_port, minimums)
                return state if state and state["0"]["rebases"] > 0 else None

            state, elapsed = wait_for("online follower rebase", observe_rebase, args.timeout)
            require(state["0"]["sequence"], minimums[0], "sequence during online rebase")
            require(state["0"]["snapshot"], cutoff, "published snapshot after online rebase")
            old_bootstraps = caught_up["states"]["0"]["bootstraps"]
            require(state["0"]["bootstraps"], old_bootstraps, "online rebase kept the existing cursor")
            _, reclaim_wait = wait_for("old replica generation reclaimed",
                                       lambda: not old_generation.exists(), args.timeout)
            new_generation, new_bytes = local_generation()
            require(new_bytes, 0, "covered replica segments reclaimed")
            snapshot_bytes = (new_generation / f"snapshot-{cutoff:020d}.bin").stat().st_size
            await_replica(a_port, "values after online rebase", minimums, expected)
            record("online_rebase", {"before_segment_bytes": old_bytes, "after_segment_bytes": new_bytes,
                                     "snapshot_bytes": snapshot_bytes, "cutoff": cutoff, "wait_ms": elapsed,
                                     "reclaim_wait_ms_after_status": reclaim_wait})

            write([(("SET", "after-rebase", "incremental"), "OK")])
            expected[(0, "after-rebase")] = "incremental"
            minimums[0] += 1
            record("incremental_after_online_rebase",
                   await_replica(a_port, "a continues after online rebase", minimums, expected))
    evidence["passed"] = True
    RUNNER.write_json(root / "result.json", evidence)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=REPO / "logs")
    parser.add_argument("--server", type=Path, default=REPO / "bin" / "kerniva-server")
    parser.add_argument("--library", type=Path, default=REPO / "bin" / "libkerniva_engine.so")
    parser.add_argument("--maintainer", type=Path, default=REPO / "bin" / "kerniva-maintainer")
    parser.add_argument("--replica", type=Path, default=REPO / "bin" / "kerniva-replica")
    parser.add_argument("--network", choices=("std", "gnet"), default="gnet")
    parser.add_argument("--timeout", type=float, default=15)
    scenarios = parser.add_mutually_exclusive_group()
    scenarios.add_argument("--rebase", action="store_true", help="also compact an online replica with over 64 MiB of history")
    scenarios.add_argument("--stress", action="store_true",
                           help="exercise concurrent writes, autonomous snapshots, replica interruption and recovery")
    scenarios.add_argument("--source-restore", action="store_true",
                           help="verify a restored primary has a distinct source identity")
    scenarios.add_argument("--slow-link", choices=("baseline", "candidate"),
                           help="verify snapshot catch-up across a paced link and, for the candidate, a stall")
    parser.add_argument("--stress-seconds", type=int, default=0, metavar="N",
                        help="minimum concurrent workload duration with --stress (0–120 seconds)")
    parser.add_argument("--stress-retain-segments", type=int, default=0, metavar="N",
                        help="covered segments to retain per database during --stress (0–16)")
    args = parser.parse_args()
    if not 0 <= args.stress_seconds <= 120:
        parser.error("--stress-seconds must be between 0 and 120")
    if not 0 <= args.stress_retain_segments <= 16:
        parser.error("--stress-retain-segments must be between 0 and 16")
    if (args.stress_seconds or args.stress_retain_segments) and not args.stress:
        parser.error("stress options require --stress")
    for name in ("server", "library", "maintainer", "replica"):
        path = getattr(args, name).resolve()
        if not path.is_file():
            parser.error(f"missing {name} at {path}")
        setattr(args, name, path)
    args.segment_bytes = 64 << 10 if args.stress else (4 << 20 if args.rebase or args.slow_link else 4096)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    if RUNNER.filesystem_type(args.output_dir.resolve()) != "ext4":
        parser.error("the test output directory must be on ext4")
    root = Path(tempfile.mkdtemp(prefix="golog-replica-", dir=args.output_dir.resolve()))
    print(f"Evidence {root}", flush=True)
    run(root, args)
    print("Replication and recovery checks passed.", flush=True)


if __name__ == "__main__":
    main()
