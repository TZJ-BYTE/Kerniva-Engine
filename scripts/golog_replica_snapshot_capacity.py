#!/usr/bin/env python3
"""Compare follower snapshot limits on one fixed-rate shared downstream link."""

import argparse
from contextlib import ExitStack, closing, contextmanager
from functools import partial
from hashlib import sha256
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
from random import Random
import shutil
import signal
import socket
import struct
import subprocess
import time
from threading import Event, Lock, Thread
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

from golog_replica_capacity import (
    CHUNK_BYTES, Evidence, INTERVAL, MARKER_KEY, check, config, get, proc, require, value,
)
from golog_replica_shared_link import SharedLink
from golog_replica_stress import covered_logs

LIGHT = 15
RATE = 8 << 20
SIZES = [256, 512, 1024] * 5
SEED = 102437
DBS = ",".join(map(str, range(LIGHT + 1)))
TIMEOUT = "40s"


def digest(path):
    with path.open("rb") as stream:
        return sha256(stream.read()).hexdigest()


def db_dir(root, db):
    return root / "follower-v1" / f"db_{db}"


def current(root, db):
    return json.loads((db_dir(root, db) / "CURRENT").read_bytes())


def snapshot(cut):
    return f"snapshot-{cut:020d}.bin"


def unpack_manifest(stream):
    header = stream.read(4)
    length, = struct.unpack(">I", header)
    if not 0 < length <= 1 << 20:
        raise RuntimeError(f"invalid source manifest length {length}")
    raw = stream.read(length)
    return header + raw, json.loads(raw)


class Relay:
    def __init__(self, evidence, link, source, fault):
        self.ev, self.link, self.source, self.fault = evidence, link, source, fault
        self.lock = Lock()
        self.attempts = []
        self.requests = []
        self.errors = []
        self.shutdown_errors = []
        self.stopping = Event()
        self.fault_ready = Event()
        self.fault_drop = Event()
        self.fault_attempt = None

    def start(self, context):
        relay = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                try:
                    relay.forward(self)
                except Exception as error:
                    target = relay.shutdown_errors if relay.stopping.is_set() else relay.errors
                    target.append(repr(error))

            def log_message(self, *_args):
                pass

        class Server(ThreadingHTTPServer):
            request_queue_size = 128
            daemon_threads = False

        server = context.enter_context(Server(("127.0.0.1", 0), Handler))
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()

        def stop():
            self.stopping.set()
            self.fault_drop.set()
            server.shutdown()
            thread.join()

        context.callback(stop)
        return server.server_port

    def forward(self, handler):
        query = parse_qs(urlsplit(handler.path).query)
        db = int(query["db"][0])
        row = {"db": db, "query": query, "request_ns": self.ev.now()}
        with self.lock:
            self.requests.append(row)
        with closing(HTTPConnection("127.0.0.1", self.source, timeout=10)) as conn:
            conn.request("GET", handler.path)
            with conn.getresponse() as response:
                row["status"] = response.status
                row["snapshot_required"] = response.getheader("Kerniva-Snapshot-Required")
                row["source_headers_ns"] = self.ev.now()
                headers = [(key, response.getheader(key)) for key in
                           ("Content-Type", "Kerniva-Snapshot-Required")]
                if response.status == 200:
                    prefix, manifest = unpack_manifest(response)
                    payload = response.read()
                    require(len(payload), sum(file["size"] for file in (manifest.get("files") or [])),
                            "source payload length")
                    row["manifest"] = manifest
                else:
                    prefix, payload = b"", response.read()
                row["payload_bytes"] = len(payload)
        attempt = None
        drop = False
        if row.get("manifest", {}).get("snapshot"):
            with self.lock:
                attempt = {"id": len(self.attempts) + 1, "db": db, "request": row,
                           "forwarded_bytes": 0, "outcome": "in_flight"}
                self.attempts.append(attempt)
                if self.fault and self.fault_attempt is None:
                    self.fault_attempt, drop = attempt, True
        handler.send_response(row["status"])
        for key, entry in headers:
            if entry is not None:
                handler.send_header(key, entry)
        handler.send_header("Content-Length", str(len(prefix) + len(payload)))
        handler.end_headers()
        row["downstream_headers_ns"] = self.ev.now()
        handler.wfile.write(prefix)
        for offset in range(0, len(payload), CHUNK_BYTES):
            chunk = payload[offset:offset + CHUNK_BYTES]
            self.link.write(db, attempt is not None, handler.wfile, chunk)
            if attempt is not None:
                if offset == 0:
                    attempt["first_ns"] = self.ev.now()
                attempt["forwarded_bytes"] += len(chunk)
                if drop and attempt["forwarded_bytes"] >= 512 << 10:
                    self.fault_ready.set()
                    if not self.fault_drop.wait(15):
                        raise TimeoutError("fault observer did not release deliberate interruption")
                    attempt.update(outcome="interrupted", end_ns=self.ev.now())
                    handler.connection.shutdown(socket.SHUT_RDWR)
                    return
        row["downstream_end_ns"] = self.ev.now()
        if attempt is not None:
            attempt.update(outcome="complete", end_ns=row["downstream_end_ns"])


class Experiment:
    def __init__(self, root, binary, replica, storage, context):
        self.root, self.binary, self.replica, self.storage, self.context = root, binary, replica, storage, context
        self.env = {key: val for key, val in os.environ.items() if not key.startswith("KERNIVA_")}
        self.env["KERNIVA_ENGINE_LIBRARY"] = str(binary / "libkerniva_engine.so")
        self.primary, self.source, self.a, self.b = check.ports(4)
        self.ports = {"primary": self.primary, "source": self.source, "a": self.a, "b": self.b}

    def start(self, label, *cmd, env=None):
        with (self.root / f"{label}.log").open("w") as log:
            child = subprocess.Popen(cmd, cwd=self.root, env=self.env if env is None else env,
                                     stdin=subprocess.DEVNULL,
                                     stdout=log, stderr=subprocess.STDOUT)
        self.context.callback(check.stop, child)
        return child

    def start_primary(self):
        settings = check.RECOVERY.configuration(
            self.storage.parent, self.root / "primary.log", self.primary,
            SimpleNamespace(network="gnet", segment_bytes=4 << 20),
        )
        settings.update(db_count=LIGHT + 1, go_log_segment_age_seconds=1, go_log_group_max=8,
                        max_memory=768 << 20)
        config_path = self.root / "config.json"
        check.RUNNER.write_json(config_path, settings)
        self.start("primary", self.binary / "kerniva-server", "-config", config_path)
        self.wait("primary ready", lambda: check.RUNNER.command(self.primary, "PING", timeout=1) == "PONG")
        info = check.RUNNER.server_info(self.primary)
        require(info.get("storage_backend"), "go-log", "storage backend")
        require(info.get("persistence_durability"), "sync", "storage durability")

    def start_source(self):
        self.start("source", self.replica, "serve", "--root", self.storage,
                   "--listen", f"127.0.0.1:{self.source}")

    def follow(self, label, folder, source, port, flags=(), env=None, executable=None):
        return self.start(label, self.replica if executable is None else executable,
                          "follow", "--root", self.root / folder,
                          "--source", f"http://127.0.0.1:{source}", "--listen", f"127.0.0.1:{port}",
                          "--dbs", DBS, "--interval", "100ms", "--pull-idle-timeout", TIMEOUT, *flags,
                          env=env)

    def write(self, db, operations):
        with socket.create_connection(("127.0.0.1", self.primary), timeout=30) as sock:
            with check.RECOVERY.RESP(sock) as client:
                require(client.command("SELECT", str(db)), "OK", f"SELECT DB {db}")
                for cmd, expected in operations:
                    require(client.command(*cmd), expected, f"DB {db} {cmd[0]}")

    @staticmethod
    def wait(label, fn, timeout=75):
        return check.wait_for(label, fn, timeout)[0]

    @staticmethod
    def status(port, db):
        status, body = check.http(port, "/status", db=db)
        return json.loads(body) if status == 200 else None

    def ready(self, port, expected, allow_poll_error=False):
        states = {}
        for db, sequence in expected.items():
            state = self.status(port, db)
            if (not state or not state["ready"] or state["sequence"] != sequence
                    or (state["last_error"] and not allow_poll_error)):
                return None
            states[db] = state
        return states


def prepare(root, binary):
    root.mkdir()
    storage = root / "primary-data/go-log-v1"
    rows = SIZES.copy()
    Random(SEED).shuffle(rows)
    seed = {**{db: 2 for db in range(LIGHT)}, LIGHT: 1}
    replica = binary / "kerniva-replica"
    metadata = {"seed": SEED, "dbs": [], "seed_sequences": seed, "fixture_replica_sha256": digest(replica)}
    with ExitStack() as context:
        run = Experiment(root, binary, replica, storage, context)
        run.start_primary()
        run.start_source()
        for db in range(LIGHT):
            run.write(db, [(("SET", "anchor", "old"), "OK"), (("SET", "removed", "present"), "OK")])
        run.write(LIGHT, [(("SET", MARKER_KEY, "seed"), "OK")])
        b = run.follow("b-prepare", "b", run.source, run.b)
        run.wait("B stale cursors ready", lambda: run.ready(run.b, seed))
        check.stop(b)
        for db, count in enumerate(rows):
            ops = ((("SET", f"bulk:{i:04d}", value(db, i)), "OK") for i in range(count))
            run.write(db, ops)
            run.write(db, [(("SET", "anchor", "new"), "OK"), (("DEL", "removed"), 1)])
        run.wait("all DB fixture cuts sealed", lambda: all(
            any((storage / f"db_{db}").glob(f"log-*-{count + 4:020d}.seg"))
            for db, count in enumerate(rows)
        ))
        p = run.start("maintainer", binary / "kerniva-maintainer", "--data-dir", storage, "--once")
        require(p.wait(timeout=180), 0, "fixture maintainer exit")
        for db, count in enumerate(rows):
            cut = count + 4
            directory = storage / f"db_{db}"
            retained = covered_logs(directory)
            require((retained["cutoff"], retained["segments"]), (cut, 0), f"DB {db} compacted logs")
            file = directory / snapshot(cut)
            metadata["dbs"].append({"db": db, "rows": count, "cut": cut, "tail": cut + 1,
                                    "snapshot_bytes": file.stat().st_size, "snapshot_sha256": digest(file)})
            run.write(db, [(("SET", "tail", f"sealed tail {db:02d}"), "OK")])
        metadata["source"] = (storage / ".replica-source-id").read_text()
        run.follow("a-prepare", "a", run.source, run.a)
        expected = {**{row["db"]: row["tail"] for row in metadata["dbs"]}, LIGHT: 1}
        run.wait("A ready at compacted tails", lambda: run.ready(run.a, expected), 120)
    check.RUNNER.write_json(root / "fixture.json", metadata)


def clone_fixture(fixture, root, metadata):
    root.mkdir()
    for folder in ("a", "b", "primary-data"):
        shutil.copytree(fixture / folder, root / folder)
    for row in metadata["dbs"]:
        path = root / "primary-data/go-log-v1" / f"db_{row['db']}" / snapshot(row["cut"])
        require(digest(path), row["snapshot_sha256"], "pre-trial source snapshot identity")


def staged(root, rows):
    result = {}
    for row in rows:
        db = row["db"]
        for file in db_dir(root / "b", db).glob(f"generation-*/{snapshot(row['cut'])}"):
            try:
                size = file.stat().st_size
            except FileNotFoundError:
                continue
            if 0 < size < row["snapshot_bytes"]:
                result[db] = {"bytes": size, "generation": file.parent.name}
    return result


def observe_marker(run, ev, source, marker, sequence):
    probes = {}

    def source_pull():
        status, body = check.http(run.source, "/pull", db=LIGHT, after=sequence - 1, source=source)
        require(status, 200, "source marker pull")
        length, = struct.unpack(">I", body[:4])
        manifest = json.loads(body[4:4 + length])
        require((manifest["source"], manifest["db"], manifest["after"]), (source, LIGHT, sequence - 1),
                "source marker lineage")
        seen = marker.encode() in body[4 + length:]
        if seen:
            require(manifest["last"], sequence, "source marker sequence")
        return seen, manifest

    def marker_get(port):
        outcome = get(port, LIGHT, MARKER_KEY)
        require(outcome[0], 200, "light follower GET")
        return outcome[1] == marker.encode(), outcome[1].decode()

    def durable():
        cursor = current(run.root / "b", LIGHT)
        require(cursor["source"], source, "B light CURRENT source")
        return cursor["sequence"] == sequence, cursor

    actions = {"source": source_pull, "a_get": partial(marker_get, run.a),
               "b_get": partial(marker_get, run.b), "b_current": durable}

    def watch():
        for name, action in actions.items():
            probe = probes.setdefault(name, {})
            if "first_positive" in probe:
                continue
            start = ev.now()
            seen, observed = action()
            probe["first_positive" if seen else "last_negative"] = {
                "start_ns": start, "end_ns": ev.now(), "value": observed,
            }
        return all("first_positive" in probes[name] for name in actions)

    run.wait("light marker source, A GET, B GET and B CURRENT", watch, 10)
    return probes


def verify_final(run, metadata, states):
    source = metadata["source"]
    final = {}
    bulk = sha256()
    for row in metadata["dbs"]:
        db, cut, tail = row["db"], row["cut"], row["tail"]
        cursors = {label: current(run.root / label, db) for label in ("a", "b")}
        for state in (states["a"][db], states["b"][db], *cursors.values()):
            require((state["source"], state["sequence"]), (source, tail), "final source and sequence")
        b = cursors["b"]
        require(b["snapshot"], cut, "B durable snapshot sequence")
        file = db_dir(run.root / "b", db) / b["generation"] / snapshot(cut)
        require(digest(file), row["snapshot_sha256"], "B verified snapshot digest")
        for port in (run.a, run.b):
            require(get(port, db, "anchor"), (200, b"new"), "updated anchor")
            require(get(port, db, "removed")[0], 404, "deleted key")
            require(get(port, db, "tail"), (200, f"sealed tail {db:02d}".encode()), "sealed tail")
        # The digest verifies the complete snapshot. Sample first, middle and last
        # bulk values through each follower's normal query path.
        indexes = (0, row["rows"] // 2, row["rows"] - 1)
        for i in indexes:
            expected = value(db, i).encode()
            for port in (run.a, run.b):
                require(get(port, db, f"bulk:{i:04d}") == (200, expected), True, f"DB {db} bulk key {i}")
            bulk.update(expected)
        final[db] = {**row, "cursors": cursors, "sampled_bulk_keys": list(indexes),
                     "a_status": states["a"][db], "b_status": states["b"][db]}
    return final, bulk.hexdigest()


@contextmanager
def save_result(root, data):
    try:
        yield
    except BaseException as error:
        data.update(passed=False, error=repr(error))
        raise
    finally:
        check.RUNNER.write_json(root / "result.json", data)


def rollup(pid):
    values = {}
    for line in Path(f"/proc/{pid}/smaps_rollup").read_text().splitlines()[1:]:
        name, value = line.split(":", 1)
        values[name] = int(value.split()[0])
    return values


def memory_process(pid):
    counters = proc(pid)
    fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
    return {**counters, "cpu_ticks": int(fields[11]) + int(fields[12])}


def trial(root, fixture, binary, replica, limit, fault, memory=False, trace=False, follower=None):
    metadata = json.loads((fixture / "fixture.json").read_bytes())
    clone_fixture(fixture, root, metadata)
    mount, fs = config(["findmnt", "-n", "-o", "SOURCE,FSTYPE", "-T", root]).split()
    require(fs, "ext4", "isolated trial filesystem")
    ev = Evidence()
    rows = metadata["dbs"]
    expected = {**{row["db"]: row["tail"] for row in rows}, LIGHT: 4}
    follower = replica if follower is None else follower
    artifacts = (replica, follower, binary / "kerniva-server", binary / "kerniva-maintainer",
                 binary / "libkerniva_engine.so", Path(__file__).resolve(),
                 Path(__file__).with_name("golog_replica_shared_link.py").resolve())
    data = {"fixture": metadata, "limit": limit, "fault_enabled": fault, "root": str(root),
            "filesystem": [mount, fs], "interval_ms": 100, "sample_target_ms": INTERVAL * 1000,
            "pull_idle_timeout": TIMEOUT, "os": list(os.uname()), "cpu_count": os.cpu_count(),
            "artifacts_sha256": {str(path): digest(path) for path in dict.fromkeys(artifacts)},
            "replica_artifacts": {"source_and_a": str(replica), "b": str(follower)}}
    link = SharedLink(ev, root, None, RATE, None)
    if memory:
        data["memory"] = {"monotonic_origin_ns": ev.origin, "trace_enabled": trace,
                          "cpu_ticks_per_second": os.sysconf("SC_CLK_TCK"), "idle_samples": []}
    with save_result(root, data), ExitStack() as context:
        run = Experiment(root, binary, replica, root / "primary-data/go-log-v1", context)
        relay = Relay(ev, link, run.source, fault)
        data.update(requests=relay.requests, attempts=relay.attempts, relay_errors=relay.errors,
                    relay_shutdown_errors=relay.shutdown_errors)
        run.start_primary()
        run.start_source()
        run.follow("follower-a", "a", run.source, run.a)
        run.wait("A ready before measurement", lambda: run.ready(run.a, {**expected, LIGHT: 1}))
        link.start(context)
        relay_port = relay.start(context)
        data["ports"] = {**run.ports, "relay": relay_port}
        initial = {db: current(root / "b", db) for db in range(LIGHT + 1)}
        for db, cursor in initial.items():
            require((cursor["source"], cursor["sequence"]), (metadata["source"], 1 if db == LIGHT else 2),
                    "B initial durable cursor")
        data["initial_b_current"] = initial
        start = ev.now()
        trace_file = root / "b-memory.jsonl"
        b_env = {**run.env, "KERNIVA_GOLOG_MEMORY_TRACE": str(trace_file)} if trace else None
        b = run.follow("follower-b", "b", relay_port, run.b, ("--snapshot-concurrency", str(limit)),
                       env=b_env, executable=follower)
        context.callback(relay.stopping.set)
        done = Event()
        samples = []
        sample_errors = []

        def read_sample(with_rollup=False):
            stamp = ev.now()
            counters = memory_process(b.pid) if memory else proc(b.pid)
            sample = {"ns": stamp, **counters, "partial": staged(root, rows)}
            if with_rollup:
                sample["smaps_start_ns"] = ev.now()
                sample["smaps_rollup_kib"] = rollup(b.pid)
                sample["smaps_end_ns"] = ev.now()
            return sample

        def measure():
            next_rollup = 0
            while not done.is_set():
                try:
                    sample = read_sample(memory and ev.now() >= next_rollup)
                    if "smaps_rollup_kib" in sample:
                        next_rollup = sample["ns"] + 1_000_000_000
                    samples.append(sample)
                except (FileNotFoundError, ProcessLookupError, KeyError):
                    pass
                except Exception as error:
                    sample_errors.append(error)
                    return
                done.wait(INTERVAL)

        thread = Thread(target=measure, daemon=True)
        thread.start()

        def stop_samples():
            done.set()
            thread.join()

        context.callback(stop_samples)
        data["samples"] = samples

        def wait(label, fn, timeout=75):
            def guarded():
                if relay.errors:
                    raise RuntimeError(relay.errors[0])
                if sample_errors:
                    raise sample_errors[0]
                return fn()
            return run.wait(label, guarded, timeout)

        if fault:
            require(relay.fault_ready.wait(15), True, "started deliberate partial body")
            failed = relay.fault_attempt
            db = failed["db"]
            partial_on_disk = wait("faulted snapshot partially written", lambda: staged(root, rows).get(db))
            previous = current(root / "b", db)
            require(previous, initial[db], "fault precondition old durable cursor")
            require(get(run.b, db, "anchor"), (200, b"old"), "fault precondition old readable state")
            data["fault"] = {"attempt": failed["id"], "db": db, "partial": partial_on_disk,
                             "old_current": previous, "old_get": "old", "trigger_ns": ev.now()}
            relay.fault_drop.set()

            def observed_failure():
                status = run.status(run.b, db)
                if status and status["last_error"]:
                    require(current(root / "b", db), previous, "failed transfer left CURRENT intact")
                    require(get(run.b, db, "anchor"), (200, b"old"), "failed transfer left GET intact")
                    return status
                return None

            data["fault"]["observed_failure"] = wait("B detected interrupted body", observed_failure, 15)
            data["fault"]["error_ns"] = ev.now()

        total_snapshot_bytes = sum(row["snapshot_bytes"] for row in rows)
        data["markers"] = []
        for sequence, progress in ((2, 0.10), (3, 0.45), (4, 0.75)):
            def marker_precondition():
                partial = staged(root, rows)
                return partial if partial and link.snapshot_bytes >= total_snapshot_bytes * progress else None

            partial_snapshot = wait(f"B staged partial snapshot files at {progress:.0%} aggregate progress",
                                    marker_precondition)
            marker = f"fixed-link-snapshot-capacity-marker-{sequence}"
            marker_write = ev.now()
            marker_transfer_bytes = link.snapshot_bytes
            run.write(LIGHT, [(("SET", MARKER_KEY, marker), "OK")])
            marker_commit = ev.now()
            data["markers"].append({
                "value": marker, "sequence": sequence, "write_start_ns": marker_write,
                "write_end_ns": marker_commit, "partial_at_write": partial_snapshot,
                "progress_target": progress, "paced_snapshot_bytes_at_write": marker_transfer_bytes,
                "witness": observe_marker(run, ev, metadata["source"], marker, sequence),
            })

        states = {"a": wait("A converged", lambda: run.ready(run.a, expected)),
                  "b": wait("B converged", lambda: run.ready(run.b, expected))}
        finish = ev.now()
        stop_samples()
        if sample_errors:
            raise sample_errors[0]
        last_proc = memory_process(b.pid) if memory else proc(b.pid)
        data["measurement"] = {"start_ns": start, "finish_ns": finish,
                               "convergence_ms": (finish - start) / 1e6,
                               "rss_hwm_kib": max(sample["hwm_kib"] for sample in [*samples, last_proc]),
                               "final_proc": last_proc,
                               "peak_partial_on_disk": max(len(sample["partial"]) for sample in samples)}
        if memory:
            for delay in (0, 5, 15, 30, 60):
                time.sleep(max(0, (finish + delay * 1_000_000_000 - ev.now()) / 1e9))
                sample = read_sample(with_rollup=True)
                sample["target_seconds_after_convergence"] = delay
                if trace:
                    sample["heap_signal_ns"] = ev.now()
                    b.send_signal(signal.SIGUSR1)
                data["memory"]["idle_samples"].append(sample)
            if trace:
                def idle_heap_samples():
                    try:
                        lines = trace_file.read_text().split("\n")[:-1]
                    except FileNotFoundError:
                        return None
                    events = [json.loads(line) for line in lines]
                    sampled = [event for event in events if event["stage"] == "sample"]
                    return sampled if len(sampled) == len(data["memory"]["idle_samples"]) else None

                sampled = wait("idle Go heap readings", idle_heap_samples, 5)
                for sample, event in zip(data["memory"]["idle_samples"], sampled):
                    require(event["monotonic_ns"] >= ev.origin + sample["heap_signal_ns"], True,
                            "idle heap sample follows signal")
                    sample["go_heap"] = event
        data["final"], data["sampled_bulk_sha256"] = verify_final(run, metadata, states)
        for label, port in (("a", run.a), ("b", run.b)):
            cursor = current(root / label, LIGHT)
            require((cursor["source"], cursor["sequence"]), (metadata["source"], expected[LIGHT]), "final light CURRENT")
            require(get(port, LIGHT, MARKER_KEY), (200, marker.encode()), "final light GET")
            data["final"][LIGHT] = {**data["final"].get(LIGHT, {}), label: cursor}

        completed = [attempt for attempt in relay.attempts if attempt["outcome"] == "complete"]
        failed = [attempt for attempt in relay.attempts if attempt["outcome"] == "interrupted"]
        require(sorted(attempt["db"] for attempt in completed), list(range(LIGHT)), "each slow DB completed")
        require(len(failed), int(fault), "interrupted transfer count")
        snapshots = {row["db"]: row for row in rows}
        intervals = []
        for attempt in relay.attempts:
            row = snapshots[attempt["db"]]
            manifest = attempt["request"]["manifest"]
            require((manifest["source"], manifest["db"], manifest["after"], manifest["snapshot"],
                     manifest["last"]), (metadata["source"], row["db"], 2, row["cut"], row["tail"]),
                    "snapshot manifest lineage")
            require(manifest["files"][0], {"name": snapshot(row["cut"]), "size": row["snapshot_bytes"]},
                    "snapshot manifest file")
            if attempt["outcome"] == "complete":
                require(attempt["forwarded_bytes"], attempt["request"]["payload_bytes"], "complete payload")
            intervals += [(attempt["first_ns"], 1), (attempt["end_ns"], -1)]
        probe_dbs = {row["db"] for row in relay.requests
                     if row.get("status") == 204 and row.get("snapshot_required") == "1"}
        require(probe_dbs, set(range(LIGHT)) if limit else set(), "snapshot negotiation DBs")
        for row in relay.requests:
            if row.get("snapshot_required") == "1":
                require((row["query"].get("defer_snapshot"), row["payload_bytes"]), (["1"], 0),
                        "unpaced and empty snapshot discovery")
        active = peak = 0
        for _, delta in sorted(intervals):
            active += delta
            peak = max(peak, active)
        require(peak, limit or LIGHT, "observed peak slow payload streams")
        require(data["measurement"]["peak_partial_on_disk"], peak, "B staged slow-file overlap")
        if fault:
            lost = failed[0]
            retry = next(attempt for attempt in completed if attempt["db"] == lost["db"])
            require(lost["end_ns"] < retry["request"]["request_ns"], True, "retry after interruption")
            require(0 < lost["forwarded_bytes"] < retry["forwarded_bytes"], True, "partial body retried in full")
            aborted = db_dir(root / "b", lost["db"]) / data["fault"]["partial"]["generation"]
            require(aborted.exists(), False, "interrupted staging generation removed")
            data["fault"]["retry_attempt"] = retry["id"]
            data["fault"]["discarded_partial_bytes"] = lost["forwarded_bytes"]
            data["fault"]["full_retry_payload_bytes"] = retry["forwarded_bytes"]
        data["measurement"]["peak_payload_streams"] = peak
        data["measurement"]["sampling_max_gap_ms"] = max((b["ns"] - a["ns"]) / 1e6
                                                          for a, b in zip(samples, samples[1:]))
        data["shared"] = link.evidence()
        for marker_record in data["markers"]:
            first = marker_record["write_start_ns"]
            last = max(probe["first_positive"]["end_ns"] for probe in marker_record["witness"].values())
            windows = sorted((attempt["first_ns"], attempt["end_ns"], attempt["id"])
                             for attempt in relay.attempts
                             if attempt["first_ns"] <= last and first <= attempt["end_ns"])
            writing = [attempt for start, end, attempt in windows if start <= first <= end]
            witnessing = [attempt for start, end, attempt in windows if start <= last <= end]
            require(bool(writing and witnessing), True, "slow snapshot bodies at marker write and witness")
            end = first
            gaps = []
            for start, stop, _ in windows:
                if end < start:
                    gaps.append([end, start])
                end = max(end, stop)
            marker_record.update(attempts_at_write=writing, attempts_at_witness=witnessing,
                                 intervening_gaps_ns=gaps)
            sequence = marker_record["sequence"]
            response = next(row for row in relay.requests
                            if row["db"] == LIGHT and row.get("manifest", {}).get("last") == sequence
                            and row.get("payload_bytes"))
            require((response["manifest"]["source"], response["manifest"]["after"]),
                    (metadata["source"], sequence - 1), "light upstream response lineage")
            chunks = [chunk for chunk in data["shared"]["chunks"]
                      if chunk["db"] == LIGHT and response["downstream_headers_ns"] <= chunk["ns"] <= last]
            require(sum(chunk["size"] for chunk in chunks), response["payload_bytes"], "paced marker body")
            marker_record["relay_response"] = response
            marker_record["paced_chunks"] = chunks
        require(data["shared"]["max_observed_early_bytes"] <= 1, True, "strict shared schedule")
        require(data["shared"]["achieved_payload_bytes_per_second"] > RATE * 0.80, True, "sustained link rate")
        require(relay.errors, [], "relay errors")
        if fault:
            relay.stopping.set()
            check.stop(b)
            require(b.returncode, 0, "B graceful exit before offline restart")
            data["fault"]["b_exit_status"] = b.returncode
            unavailable_source = check.ports(1)[0]
            offline = run.follow("follower-b-offline", "b", unavailable_source, run.b,
                                 ("--snapshot-concurrency", str(limit)), executable=follower)
            reloaded = wait("offline B reloaded final cursors",
                            lambda: run.ready(run.b, expected, allow_poll_error=True))
            for db, cursor in initial.items():
                expected_cursor = data["final"][db]["b" if db == LIGHT else "cursors"]
                if db != LIGHT:
                    expected_cursor = expected_cursor["b"]
                require(current(root / "b", db), expected_cursor, "reloaded cursor unchanged")
                require((reloaded[db]["source"], reloaded[db]["sequence"]),
                        (cursor["source"], expected[db]), "reloaded source and sequence")
            _, offline_digest = verify_final(run, metadata, {"a": states["a"], "b": reloaded})
            require(offline_digest, data["sampled_bulk_sha256"], "reloaded sampled bulk digest")
            require(get(run.b, LIGHT, MARKER_KEY), (200, marker.encode()), "reloaded light marker")
            data["fault"]["offline_restart"] = {"pid": offline.pid, "source_port": unavailable_source,
                                                "b_status": reloaded, "sampled_bulk_sha256": offline_digest,
                                                "verified_ns": ev.now()}
            check.stop(offline)
            require(offline.returncode, 0, "offline B graceful exit")
            data["fault"]["offline_restart"]["b_exit_status"] = offline.returncode
        if memory:
            relay.stopping.set()
            check.stop(b)
            data["memory"]["b_exit_status"] = b.returncode
            require(b.returncode, 0, "B graceful exit")
        data["passed"] = True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--bin", type=Path, required=True)
    parser.add_argument("--replica", type=Path, help="override --bin/kerniva-replica for measurements")
    parser.add_argument("--follower", type=Path, help="use an alternate binary for follower B")
    parser.add_argument("--compare-follower", type=Path,
                        help="compare the alternate B against --replica at cap 0 in traced AB/BA order")
    parser.add_argument("--prepare", action="store_true")
    parser.add_argument("--fixture", type=Path, help="reuse a prepared fixture directory")
    parser.add_argument("--name", default="paired")
    parser.add_argument("--limit", type=int, choices=(0, 4))
    parser.add_argument("--fault", action="store_true")
    parser.add_argument("--memory", action="store_true", help="run two reversed traced and control pairs")
    args = parser.parse_args()
    root, binary = args.root.resolve(), args.bin.resolve()
    require(config(["findmnt", "-n", "-o", "FSTYPE", "-T", root]), "ext4", "experiment root")
    fixture = args.fixture.resolve() if args.fixture else root / "fixture"
    if args.prepare:
        prepare(fixture, binary)
        return
    if args.fault and args.limit != 4:
        parser.error("--fault requires --limit 4")
    if args.memory and (args.limit is not None or args.fault):
        parser.error("--memory runs the full paired order without fault injection")
    if args.compare_follower and (args.follower or args.memory or args.limit is not None or args.fault):
        parser.error("--compare-follower selects the binaries, trace mode and cap 0")
    replica = args.replica.resolve() if args.replica else binary / "kerniva-replica"
    follower = args.follower.resolve() if args.follower else replica
    memory = args.memory or args.compare_follower is not None
    order = [(limit, False, follower, "") for limit in
             ([args.limit] if args.limit is not None else [0, 4, 4, 0])]
    if args.memory:
        order = [(limit, trace, follower, "-trace" if trace else "-control")
                 for limit, trace in ((0, False), (4, False), (0, True), (4, True),
                                      (4, True), (0, True), (4, False), (0, False))]
    if args.compare_follower:
        candidate = args.compare_follower.resolve()
        order = [(0, True, path, suffix) for path, suffix in
                 ((replica, "-baseline"), (candidate, "-candidate"),
                  (candidate, "-candidate"), (replica, "-baseline"))]
    for index, (limit, trace, follower, suffix) in enumerate(order, 1):
        destination = root / f"{args.name}-{index}-cap{limit}{suffix}"
        print(f"Running snapshot cap {limit} into {destination}", flush=True)
        trial(destination, fixture, binary, replica, limit, args.fault, memory, trace, follower)
        print(f"Passed {destination}", flush=True)


if __name__ == "__main__":
    main()
