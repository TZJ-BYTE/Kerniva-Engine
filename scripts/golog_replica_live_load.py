#!/usr/bin/env python3
"""Compare fixed-rate TCP load with and without Go-log maintenance and two replicas."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, contextmanager
from datetime import datetime, timezone
import argparse
import fcntl
from hashlib import sha256
import json
import os
from pathlib import Path
import socket
import struct
import subprocess
import sys
import time
from types import SimpleNamespace

from golog_replica_capacity import check
from golog_replica_live_observer import LiveObserver
from golog_replica_snapshot_capacity import Experiment, digest
from golog_replica_stress import sequence


DATABASES = (0, 1)
PROFILES = ((50, 3000), (95, 6000))
BINARIES = ("kerniva-server", "libkerniva_engine.so", "kerniva-replica",
            "kerniva-maintainer", "kerniva-bench")
SEED = 42
CLIENTS = 64
KEYS = 100_000
SEED_KEY = "live-load:seed"


def now():
    return datetime.now(timezone.utc)


def clock_bridge():
    start = time.monotonic_ns()
    realtime = time.time_ns()
    return {"start_ns": start, "realtime_ns": realtime, "end_ns": time.monotonic_ns()}


def measurement_alignment(measurement, record):
    start_utc = datetime.fromisoformat(measurement["started_utc"])
    elapsed = start_utc - datetime(1970, 1, 1, tzinfo=timezone.utc)
    wall_start = (elapsed.days * 86400 + elapsed.seconds) * 10**9 + elapsed.microseconds * 1000
    bridges = record["measurement_clock_bridges"]
    offsets_lower = [bridge["start_ns"] - bridge["realtime_ns"] for bridge in bridges]
    offsets_upper = [bridge["end_ns"] - bridge["realtime_ns"] for bridge in bridges]
    # Go records wall time just before setting its monotonic scheduling epoch.
    # Preserve both projections, with a conservative allowance for that gap.
    epoch_lower = max(record["client_release_interval"]["start_ns"], wall_start + min(offsets_lower) - 5_000_000)
    epoch_upper = wall_start + max(offsets_upper) + 5_000_000
    duration_ns = round(measurement["duration_seconds"] * 1e9)
    end_lower, end_upper = epoch_lower + duration_ns, epoch_upper + duration_ns
    completion = record["events"][-1]["monotonic_ns"]
    inside = [span for span in record["rust"]["snapshot_intervals"]
              if span["start_ns"] >= epoch_upper and span["end_ns"] <= end_lower]
    return {"client_started_utc": measurement["started_utc"],
            "client_elapsed_seconds": measurement["elapsed_seconds"],
            "client_duration_seconds": measurement["duration_seconds"],
            "projected_start_monotonic_ns": [epoch_lower, epoch_upper],
            "projected_offer_end_monotonic_ns": [end_lower, end_upper],
            "projection_method": "UTC-to-monotonic paired samples, with 5 ms allowance; use only for observational overlap",
            "clock_bridge_width_ns": max(offsets_upper) - min(offsets_lower),
            "projection_consistent": epoch_lower <= epoch_upper and end_lower <= completion,
            "snapshot_brackets_inside_projected_offer": inside}


def read_json(path):
    return json.loads(path.read_bytes())


@contextmanager
def primary_client(port, db):
    with socket.create_connection(("127.0.0.1", port), timeout=5) as connection:
        with check.RECOVERY.RESP(connection) as client:
            check.require(client.command("SELECT", str(db)), "OK", f"select DB{db}")
            yield client


def benchmark_value(port, key):
    request = f"*2\r\n$3\r\nGET\r\n${len(key)}\r\n{key}\r\n".encode("ascii")
    with socket.create_connection(("127.0.0.1", port), timeout=5) as connection:
        connection.sendall(request)
        with connection.makefile("rb") as reply:
            check.require(reply.readline(), b"$32\r\n", f"RESP GET length for {key}")
            value = reply.read(32)
            check.require(reply.read(2), b"\r\n", f"RESP GET tail for {key}")
            return value


def primary_state(port):
    state = {}
    for db in DATABASES:
        with primary_client(port, db) as client:
            state[db] = {"sequence": sequence(client), "dbsize": client.command("DBSIZE"),
                         "seed": client.command("GET", SEED_KEY)}
    return state


class Topology(Experiment):
    def start_primary(self):
        settings = check.RECOVERY.configuration(
            self.storage.parent, self.root / "server.log", self.primary,
            SimpleNamespace(network="gnet", segment_bytes=4 << 20),
        )
        settings.update(db_count=len(DATABASES), go_log_segment_age_seconds=1, go_log_group_max=8)
        config = self.root / "config.json"
        check.RUNNER.write_json(config, settings)
        child = self.start("primary", self.binary / "kerniva-server", "-config", config)
        self.wait("primary listening", lambda: check.RUNNER.command(self.primary, "PING", timeout=1) == "PONG")
        info = check.RUNNER.server_info(self.primary)
        check.require(info.get("storage_backend"), "go-log", "primary backend")
        check.require(info.get("persistence_durability"), "sync", "primary durability")
        for db in DATABASES:
            with primary_client(self.primary, db) as client:
                check.require(client.command("SET", SEED_KEY, f"live-load-db{db}"), "OK", "seed SET")
        return child

    def start_source(self):
        return self.start("source", self.replica, "serve", "--root", self.storage,
                          "--listen", f"127.0.0.1:{self.source}")

    def start_follower(self, name, port):
        return self.start(f"follower-{name}", self.replica, "follow", "--root", self.root / name,
                          "--source", f"http://127.0.0.1:{self.source}",
                          "--listen", f"127.0.0.1:{port}", "--dbs", "0,1", "--interval", "100ms")


def record_process(record, label, child):
    identity = check.RUNNER.process_identity(child.pid)
    check.require(bool(identity), True, f"{label} process identity")
    record["processes"][label] = identity
    return child


def follower_state(port):
    states = {}
    for db in DATABASES:
        code, response = check.http(port, "/status", db=db)
        check.require(code, 200, f"follower status DB{db}")
        states[db] = json.loads(response)
    return states


def await_followers(followers, target, label, timeout):
    def converge():
        results = {}
        for follower, port in followers.items():
            states = follower_state(port)
            if any(not states[db]["ready"] or states[db]["last_error"]
                   or states[db]["sequence"] != target[db]["sequence"] for db in DATABASES):
                return None
            results[follower] = states
        return results
    return check.wait_for(label, converge, timeout)[0]


def checkpoints(storage):
    result = {}
    for db in DATABASES:
        try:
            result[db] = int((storage / f"db_{db}" / "CURRENT").read_text())
        except FileNotFoundError:
            result[db] = 0
    return result


def observe_client(client, run, record, child_processes, snapshot_intervals, initial, initial_start, duration):
    deadline = time.monotonic() + duration + 15
    last_check = initial_start
    last = initial
    first_sample = last_sample = None
    peak_rss = 0
    while client.poll() is None:
        if time.monotonic() >= deadline:
            raise TimeoutError("fixed-rate client exceeded its measurement deadline")
        for name, child in child_processes.items():
            if child.poll() is not None:
                raise RuntimeError(f"{name} exited during fixed-rate window with {child.returncode}")
        probe_start = time.monotonic_ns()
        current = checkpoints(run.storage)
        sampled = time.monotonic_ns()
        for db, cutoff in current.items():
            if cutoff != last[db]:
                path = run.storage / f"db_{db}" / f"snapshot-{cutoff:020d}.bin"
                try:
                    size = path.stat().st_size
                except FileNotFoundError:
                    size = None
                snapshot_intervals.append({"db": db, "start_ns": last_check, "end_ns": sampled,
                                           "previous_cutoff": last[db], "cutoff": cutoff, "bytes": size})
        last_check, last = probe_start, current
        if maintainer := child_processes.get("maintainer"):
            sample = check.RUNNER.read_process_sample(maintainer.pid)
            if sample:
                first_sample = first_sample or sample
                last_sample = sample
                peak_rss = max(peak_rss, sample["working_set_bytes"])
        time.sleep(0.05)
    record["rust"].update(last_sampled_cutoffs=last, first_process_sample=first_sample,
                          last_process_sample=last_sample, sampled_peak_rss_bytes=peak_rss or None)
    return client.returncode


def terminal(run, final, followers):
    port_map = {"primary": run.primary, **followers}
    keys = [f"kerniva:thesis:{index * (KEYS - 1) // 31}" for index in range(32)]
    samples = {}

    def collect(name):
        port = port_map[name]
        values = {}
        for key in keys:
            if name == "primary":
                value = benchmark_value(port, key)
                check.require(struct.unpack("<Q", value[:8])[0], int(key.rsplit(":", 1)[1]),
                              f"{key} primary value identity")
            else:
                code, value = check.http(port, "/get", db=0, key=key)
                check.require(code, 200, f"{name} GET {key}")
            values[key] = value.hex()
        return values

    with ThreadPoolExecutor(max_workers=len(port_map)) as pool:
        for name, values in zip(port_map, pool.map(collect, port_map)):
            samples[name] = values
    for name in followers:
        check.require(samples[name], samples["primary"], f"{name} sampled benchmark values")
    result = {"primary": final, "bench_get_samples_hex": samples}
    if not followers:
        return result
    states = await_followers(followers, final, "terminal follower convergence", 20)
    disk = {}
    for name, port in followers.items():
        disk[name] = {}
        for db in DATABASES:
            code, value = check.http(port, "/get", db=db, key=SEED_KEY)
            check.require((code, value), (200, final[db]["seed"].encode("ascii")), f"{name} DB{db} seed")
            position = read_json(run.root / name / "follower-v1" / f"db_{db}" / "CURRENT")
            status = states[name][db]
            check.require((position["source"], position["sequence"], position["snapshot"]),
                          (status["source"], final[db]["sequence"], status["snapshot"]),
                          f"{name} DB{db} durable cursor")
            disk[name][db] = position
    source_end = {}
    for db in DATABASES:
        status = states["a"][db]
        check.require(states["b"][db]["source"], status["source"], f"DB{db} follower source identity")
        code, response = check.http(run.source, "/pull", db=db, after=final[db]["sequence"],
                             source=status["source"], defer_snapshot=1)
        check.require(code, 200, f"source DB{db} final cursor request")
        size = struct.unpack(">I", response[:4])[0]
        manifest = json.loads(response[4:4 + size])
        check.require((manifest["db"], manifest["source"], manifest["after"], manifest["last"]),
                      (db, status["source"], final[db]["sequence"], final[db]["sequence"]),
                      f"source DB{db} terminal manifest")
        check.require(response[4 + size:], b"", f"source DB{db} terminal payload")
        source_end[db] = {"status": code, "manifest": manifest}
    return {**result, "follower_status": states, "follower_current": disk, "source": source_end}


def case(root, args, profile, arm, position, used_ports):
    read_percent, rate = profile
    label = f"{position:02d}-r{read_percent}-q{rate}-{arm}"
    path = root / label
    path.mkdir()
    record = {"arm": arm, "rate": rate, "read_percent": read_percent, "seed": SEED,
              "position": position, "profile": args.stage, "status": "started", "events": [],
              "processes": {}, "client_exit_code": None, "rust": {"snapshot_intervals": []}}

    def save():
        check.RUNNER.write_json(path / "case.json", record)

    children = {}

    def event(name):
        entry = {"name": name, "utc": now().isoformat(), "monotonic_ns": time.monotonic_ns()}
        record["events"].append(entry)
        save()
        return entry

    try:
        with ExitStack() as processes:
            run = Topology(path, args.bin, args.bin / "kerniva-replica",
                           path / "primary-data" / "go-log-v1", processes)
            while used_ports.intersection(run.ports.values()):
                run.primary, run.source, run.a, run.b = check.ports(4)
                run.ports = dict(zip(("primary", "source", "a", "b"), (run.primary, run.source, run.a, run.b)))
            used_ports.update(run.ports.values())
            record["ports"] = run.ports if arm == "B" else {"primary": run.primary}
            primary = record_process(record, "primary", run.start_primary())
            children["primary"] = primary
            followers = {}
            if arm == "B":
                children["source"] = record_process(record, "source", run.start_source())
                followers = {"a": run.a, "b": run.b}
                for name, port in followers.items():
                    children[name] = record_process(record, name, run.start_follower(name, port))
            event("topology_started")

            start_marker, ready_marker = path / "client.start", path / "client.ready"
            bench = [str(args.bin / "kerniva-bench"), "-mode", "mixed", "-addr", f"127.0.0.1:{run.primary}",
                     "-network", "gnet", "-durability", "sync", "-clients", str(CLIENTS), "-seed", str(SEED),
                     "-keys", str(KEYS), "-rate", str(rate), "-read-percent", str(read_percent),
                     "-duration", f"{args.duration}s", "-warmup", f"{args.warmup}s", "-timeout", "2s",
                     "-histogram-out", str(path / "client-hist.json"),
                     "-ready-file", str(ready_marker), "-start-file", str(start_marker)]
            record["client_argv"] = bench
            with (path / "client.json").open("wb") as stdout, (path / "client.stderr.log").open("wb") as stderr:
                client = subprocess.Popen(bench, cwd=path, env=run.env, stdout=stdout, stderr=stderr,
                                          stdin=subprocess.DEVNULL)
            processes.callback(check.stop, client)
            record_process(record, "client", client)

            def prepared():
                if client.poll() is not None:
                    raise RuntimeError(f"client exited before ready with {client.returncode}")
                return ready_marker.is_file()

            check.wait_for("client prefill and warmup", prepared, args.warmup + 45)
            before = primary_state(run.primary)
            record["measurement_start_primary"] = before
            if followers:
                record["measurement_start_followers"] = await_followers(followers, before, "warmup follower catch-up", 20)
            initial_start = time.monotonic_ns()
            initial = checkpoints(run.storage)
            record["rust"]["before_cutoffs"] = initial
            if followers:
                maintenance = [args.bin / "kerniva-maintainer", "--data-dir", run.storage, "--interval", "1",
                               "--max-age", "2", "--min-sealed-bytes", "32768", "--retain-covered-segments", "8"]
                record["rust"]["argv"] = list(map(str, maintenance))
                children["maintainer"] = record_process(record, "maintainer", run.start("maintainer", *maintenance))
            with LiveObserver(path, run.storage, run.primary, followers, databases=DATABASES,
                              source_port=run.source if followers else None,
                              offsets=(0.35, args.duration / 2, args.duration - 1.5),
                              snapshot_intervals=record["rust"]["snapshot_intervals"]) as observer:
                if now() >= args.stop_opening:
                    raise TimeoutError("window opening cutoff passed before client release")
                record["observation_anchor_ns"] = observer.start()
                record["measurement_clock_bridges"] = [clock_bridge()]
                record["client_release_interval"] = {"start_ns": time.monotonic_ns()}
                start_marker.touch(exist_ok=False)
                record["client_release_interval"]["end_ns"] = time.monotonic_ns()
                event("client_start_release")
                record["client_exit_code"] = observe_client(client, run, record, children,
                                                             record["rust"]["snapshot_intervals"], initial,
                                                             initial_start, args.duration)
                record["measurement_clock_bridges"].append(clock_bridge())
                event("client_exited")
                check.require(record["client_exit_code"], 0, "fixed-rate client exit")
                record["measurement_end_primary"] = primary_state(run.primary)
                observation = observer.finish()
                record["observations_file"] = "live-observer.json"
                record["observer_returned"] = isinstance(observation, dict)

            final = primary_state(run.primary)
            if followers:
                await_followers(followers, final, "followers catch the last sealed writes", 20)
            record["terminal"] = terminal(run, final, followers)
            record["rust"]["after_cutoffs"] = checkpoints(run.storage)
            measurement = read_json(path / "client.json")
            record["measurement_alignment"] = measurement_alignment(measurement, record)
            check.require((measurement["target_rate"], measurement["read_percent"], measurement["seed"]),
                          (rate, read_percent, SEED), "client recorded profile")
            if not (path / "client-hist.json").is_file():
                raise RuntimeError("missing raw histogram")
            record["status"] = "completed"
            event("verified_terminal")
    except Exception as error:
        record.update(status="failed", error=f"{type(error).__name__}: {error}")
        event("failed")
        print(f"{label}: {record['error']}", file=sys.stderr, flush=True)
    finally:
        record["process_exit_codes"] = {name: child.poll() for name, child in children.items()}
        save()
    print(f"{label}: {record['status']}", flush=True)
    return record["status"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True, help="new ext4 result directory")
    parser.add_argument("--bin", type=Path, required=True)
    parser.add_argument("--lock", type=Path, required=True, help="existing shared performance lock")
    parser.add_argument("--stage", choices=("pilot", "formal"), required=True)
    parser.add_argument("--duration", type=int, required=True)
    parser.add_argument("--warmup", type=int, default=5)
    parser.add_argument("--stop-opening", type=datetime.fromisoformat, required=True)
    parser.add_argument("--source-revision", required=True)
    parser.add_argument("--source-archive-sha256", required=True)
    parser.add_argument("--pilot-plan", type=Path, required=True)
    parser.add_argument("--pilot-plan-sha256", required=True)
    parser.add_argument("--approval", type=Path)
    parser.add_argument("--approval-sha256")
    args = parser.parse_args()
    if not args.stop_opening.tzinfo or args.duration < 6 or args.warmup < 0:
        parser.error("positive duration (minimum 6 seconds) and zoned stop-opening required")
    args.root = args.root.resolve()
    args.bin = args.bin.resolve(strict=True)
    args.root.mkdir()
    if check.RUNNER.filesystem_type(args.root) != "ext4":
        parser.error("result directory must be ext4")
    if digest(args.pilot_plan) != args.pilot_plan_sha256:
        parser.error("pilot plan hash mismatch")
    if args.stage == "formal":
        if not args.approval or not args.approval_sha256 or digest(args.approval) != args.approval_sha256:
            parser.error("formal stage requires verified approval")
    arm_order = "AB" if args.stage == "pilot" else "ABBA"
    plan = [(read, rate, arm) for read, rate in PROFILES for arm in arm_order]
    manifest = {"created_at": now().isoformat(), "driver_pid": os.getpid(), "stage": args.stage,
                "source_revision": args.source_revision, "source_archive_sha256": args.source_archive_sha256,
                "duration": args.duration, "warmup": args.warmup, "network": "gnet",
                "stop_opening": args.stop_opening.isoformat(), "plan": plan,
                "binaries": {name: digest(args.bin / name) for name in BINARIES},
                "scripts": {name: digest(Path(__file__).parent / name) for name in
                            ("golog_replica_live_load.py", "golog_replica_live_observer.py", "golog_replica_snapshot_capacity.py",
                             "golog_replica_capacity.py", "golog_replica_stress.py", "golog_replica_shared_link.py",
                             "golog-replica-check.py", "golog-recovery-bench.py", "thesis-bench-linux.py")},
                "lock": str(args.lock), "pilot_plan_sha256": args.pilot_plan_sha256,
                "approval_sha256": args.approval_sha256, "results": []}
    with args.lock.open("r+b") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        manifest["lock_acquired_at"] = now().isoformat()
        check.RUNNER.write_json(args.root / "run.json", manifest)
        used_ports = set()
        for position, (read, rate, arm) in enumerate(plan, 1):
            if now() >= args.stop_opening:
                break
            status = case(args.root, args, (read, rate), arm, position, used_ports)
            manifest["results"].append({"position": position, "read_percent": read, "rate": rate,
                                        "arm": arm, "status": status})
            check.RUNNER.write_json(args.root / "run.json", manifest)
    return int(len(manifest["results"]) != len(plan)
               or any(item["status"] != "completed" for item in manifest["results"]))


if __name__ == "__main__":
    sys.exit(main())
