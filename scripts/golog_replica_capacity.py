#!/usr/bin/env python3
"""Measure independent replica polls under concurrent paced snapshot traffic."""
import argparse
from contextlib import ExitStack, closing
from hashlib import sha256
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import importlib.util
import json
import os
from pathlib import Path
import struct
import subprocess
import time
from threading import Event, Lock, Thread
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit
from uuid import uuid4

from golog_replica_slow_link import CHUNK_BYTES, PAYLOAD_KEYS, VALUE_BYTES
from golog_replica_stress import covered_logs
from golog_replica_shared_link import SharedLink, demo_delta, demo_snapshot

REPO = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("replica_check", REPO / "scripts/golog-replica-check.py")
check = importlib.util.module_from_spec(spec)
spec.loader.exec_module(check)
require = check.require
RATE = 512 << 10
INTERVAL = 0.025
CUT = 2 + PAYLOAD_KEYS + 2
TAIL = CUT + 1
SNAP = f"snapshot-{CUT:020d}.bin"
MARKER_KEY = "capacity:marker"


class Evidence:
    def __init__(self):
        self.origin = time.monotonic_ns()
        self.events = []
        self.lock = Lock()

    def now(self):
        return time.monotonic_ns() - self.origin

    def record(self, name, **data):
        with self.lock:
            stamp = self.now()
            self.events.append({"name": name, "ns": stamp, **data})
            return stamp


class Relay:
    def __init__(self, source, light, evidence, shared=None):
        self.source = source
        self.light = light
        self.ev = evidence
        self.shared = shared
        self.lock = Lock()
        self.stop = Event()
        self.armed = False
        self.transfers = {}
        self.errors = []
        self.shutdown_errors = []
        self.teardown = Event()
        self.bytes = {"upstream_body": 0, "downstream_body": 0}
        self.markers = []

    def follow(self, resources):
        relay = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                try:
                    relay.forward(self)
                except Exception as error:
                    target = relay.shutdown_errors if relay.teardown.is_set() else relay.errors
                    target.append(repr(error))

            def log_message(self, *_args):
                pass

        class Server(ThreadingHTTPServer):
            request_queue_size = 128
            daemon_threads = False

        server = resources.enter_context(Server(("127.0.0.1", 0), Handler))
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()

        def shutdown():
            self.teardown.set()
            self.stop.set()
            server.shutdown()
            thread.join()

        resources.callback(shutdown)
        return server.server_port

    def forward(self, handler):
        query = parse_qs(urlsplit(handler.path).query)
        db = int(query["db"][0])
        request_ns = self.ev.record("b_request", db=db, query=query)
        if self.shared and self.armed and db != self.light and query.get("after") == ["2"]:
            with self.shared.admit(db) as slot:
                self.deliver(handler, db, request_ns, slot)
        else:
            self.deliver(handler, db, request_ns)

    def deliver(self, handler, db, request_ns, slot=None):
        with closing(HTTPConnection("127.0.0.1", self.source, timeout=10)) as upstream:
            upstream.request("GET", handler.path)
            with upstream.getresponse() as response:
                data = response.read()
                content_type = response.getheader("Content-Type")
                status = response.status
        with self.lock:
            self.bytes["upstream_body"] += len(data)
        handler.send_response(status)
        if content_type:
            handler.send_header("Content-Type", content_type)
        handler.send_header("Content-Length", str(len(data)))
        if slot is not None:
            slot["headers_ns"] = self.ev.record("shared_response_header_start", db=db)
        handler.end_headers()
        if status != 200:
            handler.wfile.write(data)
            with self.lock:
                self.bytes["downstream_body"] += len(data)
            return
        length, = struct.unpack(">I", data[:4])
        manifest = json.loads(data[4:4 + length])
        header, payload = data[:4 + length], data[4 + length:]
        require(len(payload), sum(item["size"] for item in (manifest.get("files") or [])), "relay payload length")
        if db == self.light:
            matched = [marker for marker in self.markers if marker.encode() in payload]
            self.ev.record("light_response", request_ns=request_ns, markers=matched, manifest=manifest)
        if not (self.armed and db != self.light and manifest.get("snapshot")):
            if self.shared and self.armed and payload:
                handler.wfile.write(header)
                for offset in range(0, len(payload), CHUNK_BYTES):
                    self.shared.write(db, False, handler.wfile, payload[offset:offset + CHUNK_BYTES])
            else:
                handler.wfile.write(data)
            with self.lock:
                self.bytes["downstream_body"] += len(data)
            return
        if self.shared:
            require(slot is not None, True, f"DB {db} had shared slot before snapshot")
        with self.lock:
            require(db not in self.transfers, True, f"DB {db} served exactly one paced snapshot")
            entry = {"manifest": manifest, "payload_bytes": len(payload), "forwarded_payload_bytes": 0}
            self.transfers[db] = entry
        handler.wfile.write(header)
        with self.lock:
            self.bytes["downstream_body"] += len(header)
        start = time.monotonic()
        snap_bytes = manifest["files"][0]["size"]
        for off in range(0, len(payload), CHUNK_BYTES):
            chunk = payload[off:off + CHUNK_BYTES]
            if self.shared:
                self.shared.write(db, True, handler.wfile, chunk)
            else:
                due = start + (off + len(chunk)) / RATE
                if self.stop.wait(max(0, due - time.monotonic())):
                    return
                handler.wfile.write(chunk)
            with self.lock:
                self.bytes["downstream_body"] += len(chunk)
                entry["forwarded_payload_bytes"] += len(chunk)
                if off == 0:
                    entry["first_payload_ns"] = self.ev.record("slow_first", db=db)
                if entry["forwarded_payload_bytes"] >= snap_bytes and "snapshot_last_ns" not in entry:
                    entry["snapshot_last_ns"] = self.ev.record("slow_snapshot_last", db=db)
                if entry["forwarded_payload_bytes"] == len(payload):
                    entry["last_payload_ns"] = self.ev.record("slow_last", db=db)


def proc(pid):
    base = Path(f"/proc/{pid}")
    status = {key: value.strip() for key, value in (line.split(":", 1) for line in (base / "status").read_text().splitlines())}
    io = {key: int(value) for key, value in (line.split(":", 1) for line in (base / "io").read_text().splitlines())}
    return {"rss_kib": int(status["VmRSS"].split()[0]), "hwm_kib": int(status["VmHWM"].split()[0]), **io}


def disk(device):
    for line in Path("/proc/diskstats").read_text().splitlines():
        fields = line.split()
        if fields[2] == device:
            return {"reads_completed": int(fields[3]), "read_sectors": int(fields[5]),
                    "writes_completed": int(fields[7]), "written_sectors": int(fields[9]),
                    "io_ms": int(fields[12])}
    raise RuntimeError(f"{device} absent from /proc/diskstats")


def get(port, db, key):
    return check.http(port, "/get", db=db, key=key)


def value(db, index):
    return f"db={db:02d}/row={index:03d};".ljust(VALUE_BYTES, "x")


def config(cmd):
    return subprocess.check_output(cmd, text=True).strip()


def trial(args):
    root = args.root.resolve()
    root.mkdir(parents=True)
    ev = Evidence()
    fixtures = REPO / "logs/golog-followup-bSuIKDwd/db-poll-fairness-20261124/bin"
    serial = fixtures.parent.parent / "slow-link-paired/bin/candidate-replica"
    parallel = fixtures / "parallel-replica"
    server, library, maintainer = (fixtures / name for name in ("kerniva-server", "libkerniva_engine.so", "kerniva-maintainer"))
    for file, expected in (
        (serial, "208304bf6f641f53798942f3690e204a3688ac3f5c604a235815df466645338a"),
        (parallel, "7dc71d56d827b74e8998a2f3f4e3d483147c21d41d4544e6d145326f4ce5763b"),
        (server, "0a86dc154895a6ca364fd8000bcf7117042ca629dff66caee1a893a561509773"),
        (library, "f92f95d1db788377ca50ae390faa18c4b25f85509d7e4efc65cf090a78d09e65"),
        (maintainer, "a3454cc627c93be6a008f6be588635d506ac03a397056a5ac47b9351d456cd5a"),
    ):
        require(sha256(file.read_bytes()).hexdigest(), expected, str(file))
    mount_source, fstype = config(["findmnt", "-n", "-o", "SOURCE,FSTYPE", "-T", root]).split()
    require(fstype, "ext4", "isolated trial mount")
    device = Path(mount_source).name
    primary_port, source_port, a_port, b_port = check.ports(4)
    light = args.dbs - 1
    storage = root / "primary-data/go-log-v1"
    settings = check.RECOVERY.configuration(storage.parent, root / "primary.log", primary_port,
                                            SimpleNamespace(network="gnet", segment_bytes=4 << 20))
    settings.update(db_count=args.dbs, go_log_segment_age_seconds=1, go_log_group_max=8)
    settings["max_memory"] = 512 << 20
    config_path = root / "config.json"
    check.RUNNER.write_json(config_path, settings)
    data = {
        "db_count": args.dbs, "light_db": light, "trial_root": str(root),
        "rate_bytes_per_second_per_slow_db": None if args.shared_cap else RATE, "chunk_bytes": CHUNK_BYTES,
        "shared_cap": args.shared_cap, "candidate_pull_idle_timeout": "40s" if args.shared_cap else "default",
        "follower_interval_ms": 100, "sample_sleep_ms": INTERVAL * 1000,
        "time_origin_monotonic_ns": ev.origin, "hostname": os.uname().nodename,
        "uname": config(["uname", "-a"]), "cpu_count": os.cpu_count(),
        "cpu_model": next(line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines()
                          if line.startswith("model name")),
        "mem_total": next(line for line in Path("/proc/meminfo").read_text().splitlines() if line.startswith("MemTotal:")),
        "fs": fstype, "disk_device": device, "ports": [primary_port, source_port, a_port, b_port],
        "artifacts_sha256": {str(file): sha256(file.read_bytes()).hexdigest()
                             for file in (server, library, maintainer, serial, parallel, Path(__file__),
                                          REPO / "scripts/golog-replica-check.py", REPO / "scripts/golog_replica_slow_link.py",
                                          REPO / "scripts/golog_replica_stress.py", REPO / "scripts/golog_replica_shared_link.py")},
        "marker_observations": [], "events": ev.events,
    }
    shared = SharedLink(ev, root, args.shared_cap, 8 << 20, TAIL) if args.shared_cap else None
    relay = Relay(source_port, light, ev, shared)
    env = {key: val for key, val in os.environ.items() if not key.startswith("KERNIVA_")}
    env["KERNIVA_ENGINE_LIBRARY"] = str(library)
    list_dbs = ",".join(map(str, range(args.dbs)))

    def ready(port, expected):
        states = {}
        for db, sequence in expected.items():
            status, body = check.http(port, "/status", db=db)
            if status != 200:
                return None
            state = json.loads(body)
            if not state["ready"] or state["last_error"] or state["sequence"] != sequence:
                return None
            states[db] = state
        return states

    def write(db, operations):
        import socket
        with socket.create_connection(("127.0.0.1", primary_port), timeout=30) as sock:
            with check.RECOVERY.RESP(sock) as client:
                require(client.command("SELECT", str(db)), "OK", f"SELECT DB {db}")
                for op, expected in operations:
                    require(client.command(*op), expected, f"DB {db} {op[0]}")

    def current(db):
        return json.loads((root / f"b/follower-v1/db_{db}/CURRENT").read_bytes())

    def wait(label, fn, timeout=30):
        def guarded():
            if relay.errors:
                raise RuntimeError(f"relay: {relay.errors[0]}")
            return fn()
        return check.wait_for(label, guarded, timeout)[0]

    try:
        with ExitStack() as context:
            def start(label, cmd):
                with (root / f"{label}.log").open("w") as log:
                    p = subprocess.Popen(cmd, cwd=root, env=env, stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT)
                context.callback(check.stop, p)
                return p

            def follow(label, folder, bin_path, upstream, port):
                flags = ["--pull-idle-timeout", "40s"] if (shared and label == "follower-b") else []
                return start(label, [bin_path, "follow", "--root", root / folder,
                            "--source", f"http://127.0.0.1:{upstream}", "--listen", f"127.0.0.1:{port}",
                            "--dbs", list_dbs, "--interval", "100ms", *flags])

            start("primary", [server, "-config", config_path])
            wait("primary PING", lambda: check.RUNNER.command(primary_port, "PING", timeout=1) == "PONG")
            info = check.RUNNER.server_info(primary_port)
            require(info.get("storage_backend"), "go-log", "primary backend")
            require(info.get("persistence_durability"), "sync", "primary durability")
            start("source", [serial, "serve", "--root", storage, "--listen", f"127.0.0.1:{source_port}"])
            if shared:
                shared.start(context)
            relay_port = relay.follow(context)
            data["ports"].append(relay_port)

            for db in range(light):
                write(db, [(("SET", "anchor", "old"), "OK"), (("SET", "removed", "present"), "OK")])
            write(light, [(("SET", MARKER_KEY, "seed"), "OK")])
            seed = {**{db: 2 for db in range(light)}, light: 1}
            follow("follower-a", "a", serial, source_port, a_port)
            b_seed = follow("follower-b-seed", "b", parallel, relay_port, b_port)
            wait("A's and B's seed state", lambda: ready(a_port, seed) and ready(b_port, seed))
            source_id = (storage / ".replica-source-id").read_text()
            old = [current(db) for db in range(args.dbs)]
            for db, curs in enumerate(old):
                require((curs["source"], curs["sequence"]), (source_id, seed[db]), "seed/current")
            check.stop(b_seed)

            for db in range(light):
                writes = [(("SET", f"bulk:{i:03d}", value(db, i)), "OK") for i in range(PAYLOAD_KEYS)]
                write(db, writes + [(("SET", "anchor", "new"), "OK"), (("DEL", "removed"), 1)])
            wait("A before compaction", lambda: ready(a_port, {**{db: CUT for db in range(light)}, light: 1}))
            p = start("maintainer", [maintainer, "--data-dir", storage, "--once"])
            require(p.wait(timeout=120), 0, "maintainer exit")
            sizes = {}
            for db in range(light):
                source_db = storage / f"db_{db}"
                retained = covered_logs(source_db)
                require((retained["cutoff"], retained["segments"]), (CUT, 0), f"db {db} snapshot cutoff")
                sizes[db] = (source_db / SNAP).stat().st_size
                write(db, [(("SET", "tail", f"sealed tail {db:02d}"), "OK")])
            require(len(set(sizes.values())), 1, "fixed single-db snapshot size")
            data["snapshot_bytes_per_db"] = next(iter(sizes.values()))
            wait("A's DB tails", lambda: ready(a_port, {**{db: TAIL for db in range(light)}, light: 1}))

            relay.bytes = {"upstream_body": 0, "downstream_body": 0}
            relay.armed = True
            if shared:
                demo_idle_start = demo_snapshot(18780)
                time.sleep(1)
                demo_idle_end = demo_snapshot(18780)
                data["demo_idle"] = demo_delta(demo_idle_start, demo_idle_end)
            disk_before = disk(device)
            start_ns = ev.record("candidate_launch")
            b = follow("follower-b", "b", parallel, relay_port, b_port)
            context.callback(relay.teardown.set)
            samples = []
            reading = Event()

            def memory():
                while not reading.is_set():
                    try:
                        samples.append({"ns": ev.now(), **proc(b.pid)})
                    except (FileNotFoundError, ProcessLookupError):
                        return
                    except KeyError:
                        pass
                    reading.wait(INTERVAL)

            thread = Thread(target=memory, daemon=True)
            thread.start()

            def stop_samples():
                reading.set()
                thread.join()

            context.callback(stop_samples)

            def partial():
                staged = {}
                for db in range(light):
                    db_dir = root / f"b/follower-v1/db_{db}"
                    for path in db_dir.glob(f"generation-*/{SNAP}"):
                        seen = path.stat().st_size
                        if 0 < seen < sizes[db]:
                            staged[db] = seen
                return staged if len(staged) == (shared.cap if shared else light) else None

            staged = wait("initial group partially staged on disk", partial)
            partial_label = "initial_admitted_partial" if shared else "all_slow_partial"
            data[partial_label] = {"ns": ev.record(partial_label), "bytes": staged}
            expected_slow_bytes = sum(sizes.values())
            for sequence, progress in zip((2, 3, 4), (0.10, 0.45, 0.75)):
                def reached_progress():
                    return (shared.snapshot_bytes >= expected_slow_bytes * progress
                            and any("first_payload_ns" in item and "snapshot_last_ns" not in item
                                    for item in list(relay.transfers.values())))
                if shared:
                    wait(f"slow transfer progress {progress:.0%}", reached_progress, 30)
                marker = f"mark-{uuid4().hex}"
                relay.markers.append(marker)
                witness = {key: {} for key in ("source", "a_get", "b_get", "b_current")}
                row = {"value": marker, "sequence": sequence, "witness": witness}
                data["marker_observations"].append(row)

                def available():
                    status, body = check.http(source_port, "/pull", db=light, after=sequence - 1, source=source_id)
                    require(status, 200, "source marker pull")
                    length, = struct.unpack(">I", body[:4])
                    manifest = json.loads(body[4:4 + length])
                    payload = body[4 + length:]
                    require((manifest["source"], manifest["db"], manifest["after"]),
                            (source_id, light, sequence - 1), "source marker identity")
                    present = marker.encode() in payload
                    if present:
                        require(manifest["last"], sequence, "source marker sequence")
                    return present, {"last": manifest["last"], "payload_bytes": len(payload)}

                def seen(port):
                    status, body = get(port, light, MARKER_KEY)
                    require(status, 200, "light GET")
                    return body == marker.encode(), body.decode()

                def durable():
                    cursor = current(light)
                    require(cursor["source"], source_id, "light CURRENT source")
                    return cursor["sequence"] == sequence, cursor

                probes = {"source": available, "a_get": lambda: seen(a_port),
                          "b_get": lambda: seen(b_port), "b_current": durable}

                def sample_all():
                    for key, probe in probes.items():
                        target = witness[key]
                        if "first_positive" in target:
                            continue
                        before = ev.now()
                        found, detail = probe()
                        after = ev.now()
                        label = "first_positive" if found else "last_negative"
                        target[label] = {"start_ns": before, "end_ns": after, "detail": detail}
                        target["samples"] = target.get("samples", 0) + 1

                sample_all()
                require(all("last_negative" in x for x in witness.values()), True, "initial light negative sample")
                if shared:
                    wait("active slow stream at marker start", reached_progress, 30)
                    row["slow_payload_bytes_at_write"] = shared.snapshot_bytes
                    row["target_snapshot_progress"] = progress
                row["write_start_ns"] = ev.record("light_write_start", sequence=sequence)
                write(light, [(("SET", MARKER_KEY, marker), "OK")])
                row["write_ack_ns"] = ev.record("light_write_ack", sequence=sequence)
                deadline = time.monotonic() + 15
                while not all("first_positive" in item for item in witness.values()):
                    if relay.errors:
                        raise RuntimeError(f"relay: {relay.errors[0]}")
                    if time.monotonic() > deadline:
                        raise TimeoutError(f"light marker {sequence} did not reach all endpoints")
                    sample_all()
                    time.sleep(INTERVAL)
                source_last = witness["source"]["last_negative"]["start_ns"]
                source_first = witness["source"]["first_positive"]["end_ns"]
                for key in ("a_get", "b_get", "b_current"):
                    first = witness[key]["first_positive"]["end_ns"]
                    last = witness[key]["last_negative"]["start_ns"]
                    row[f"{key}_source_latency_ms"] = [(max(0, last - source_first)) / 1e6, (first - source_last) / 1e6]
                responses = [e for e in ev.events if e["name"] == "light_response" and marker in e["markers"]]
                require(bool(responses), True, "marker observed in B's source response")
                response = min(responses, key=lambda item: item["request_ns"])
                require((response["manifest"]["source"], response["manifest"]["db"], response["manifest"]["after"],
                         response["manifest"]["last"]), (source_id, light, sequence - 1, sequence), "B response markers and sequence")
                row["b_marker_response"] = response

            expected = {**{db: TAIL for db in range(light)}, light: 4}
            a_ready = wait("A reached all final sequences", lambda: ready(a_port, expected))
            b_ready = wait("B reached all final sequences", lambda: ready(b_port, expected))
            finish_ns = ev.record("all_final_sequences_observed")
            if shared:
                data["demo_during"] = demo_delta(demo_idle_end, demo_snapshot(18780))
                wait("all paced CURRENT checks released", lambda: len(shared.in_flight) == light and shared.busy == 0)
            stop_samples()
            last_proc = proc(b.pid)
            disk_after = disk(device)
            data["resource_window"] = {
                "start_ns": start_ns, "end_ns": finish_ns, "elapsed_ms": (finish_ns - start_ns) / 1e6,
                "b_peak_rss_kib": max(item["hwm_kib"] for item in [*samples, last_proc]),
                "proc_b_final": last_proc, "b_sample_count": len(samples),
                "proc_semantics": "VmHWM process lifetime peak, rchar/wchar syscall bytes including sockets, read_bytes/write_bytes process storage I/O accounting",
                "disk_before": disk_before, "disk_after": disk_after,
                "disk_delta": {key: disk_after[key] - disk_before[key] for key in disk_before},
                "disk_semantics": "WSL block device whole-system delta for the candidate launch-to-convergence window; 512-byte sectors; includes other WSL activity and buffered flush timing",
            }
            data["b_resource_samples"] = samples
            final = {}
            bulk_digest = sha256()
            for db, seq in expected.items():
                a_state, b_state, curs = a_ready[db], b_ready[db], current(db)
                for state in (a_state, b_state, curs):
                    require((state["source"], state["sequence"]), (source_id, seq), "final source/sequence")
                generation = root / f"b/follower-v1/db_{db}" / curs["generation"]
                require(generation.is_dir(), True, "B CURRENT generation exists")
                final[db] = {"a_status": a_state, "b_status": b_state, "b_current": curs}
                if db == light:
                    for endpoint in (a_port, b_port):
                        require(get(endpoint, db, MARKER_KEY), (200, marker.encode()), "final light value")
                    continue
                require(curs["snapshot"], CUT, "B durable snapshot cutoff")
                digest = sha256((generation / SNAP).read_bytes()).hexdigest()
                require(digest, sha256((storage / f"db_{db}" / SNAP).read_bytes()).hexdigest(), "snapshot byte identity")
                final[db]["snapshot_sha256"] = digest
                for endpoint in (a_port, b_port):
                    require(get(endpoint, db, "anchor"), (200, b"new"), "updated anchor")
                    require(get(endpoint, db, "removed")[0], 404, "deleted key")
                    require(get(endpoint, db, "tail"), (200, f"sealed tail {db:02d}".encode()), "sealed tail")
                for i in range(PAYLOAD_KEYS):
                    wanted = value(db, i).encode()
                    for endpoint in (a_port, b_port):
                        require(get(endpoint, db, f"bulk:{i:03d}") == (200, wanted), True, f"DB {db} bulk key {i}")
                    bulk_digest.update(wanted)
                final[db]["bulk_keys_verified_per_follower"] = PAYLOAD_KEYS
            data["final"] = final
            data["verified_bulk_sha256"] = bulk_digest.hexdigest()
            require(len(relay.transfers), light, "all slow DBs transferred")
            intervals = []
            for db, entry in relay.transfers.items():
                m = entry["manifest"]
                require((m["source"], m["db"], m["after"], m["snapshot"], m["last"]),
                        (source_id, db, 2, CUT, TAIL), "paced manifest lineage")
                require(m["files"][0], {"name": SNAP, "size": sizes[db]}, "paced snapshot file manifest")
                require(entry["forwarded_payload_bytes"], entry["payload_bytes"], "paced transfer complete")
                intervals.append((entry["first_payload_ns"], entry["snapshot_last_ns"]))
            overlap_start = max(start for start, _ in intervals)
            overlap_end = min(end for _, end in intervals)
            points = sorted([(start, 1) for start, _ in intervals] + [(end, -1) for _, end in intervals])
            active = peak = 0
            for _, change in points:
                active += change
                peak = max(peak, active)
            if shared:
                require(peak <= shared.cap, True, "concurrent payload cap")
                require(len(shared.in_flight), light, "all snapshot installations admitted")
                slot_points = []
                for db, trace in shared.in_flight.items():
                    entry = relay.transfers[db]
                    require(trace["admitted_ns"] <= trace["headers_ns"] <= entry["first_payload_ns"] <
                            entry["last_payload_ns"] <= trace["published_ns"] <= trace["released_ns"],
                            True, "slot held through durable CURRENT")
                    slot_points += [(trace["admitted_ns"], 1), (trace["released_ns"], -1)]
                active = slot_peak = 0
                for _, change in sorted(slot_points):
                    active += change
                    slot_peak = max(slot_peak, active)
                require(slot_peak, shared.cap, "admitted/installing peak")
                data["shared"] = shared.evidence()
                data["shared"]["peak_slots"] = slot_peak
                require(data["shared"]["max_observed_early_bytes"] <= 1, True, "strict shared schedule")
                require(data["shared"]["achieved_payload_bytes_per_second"] > shared.rate * 0.80, True,
                        "aggregate sustained at least 80% of the configured limit")
                for item in data["marker_observations"]:
                    row = item["b_marker_response"]
                    observed = item["witness"]["b_get"]["first_positive"]["end_ns"]
                    pull_chunks = [chunk for chunk in data["shared"]["chunks"] if chunk["db"] == light
                                   and row["ns"] <= chunk["ns"] <= observed]
                    require(bool(pull_chunks), True, "light payload consumed shared queue capacity")
                    require(any(first <= item["write_start_ns"] <= last for first, last in intervals),
                            True, "light marker initiated during active slow snapshot traffic")
                    item["slow_streams_at_first_get_observation"] = sum(first <= observed <= last for first, last in intervals)
                    item["shared_marker_chunks"] = pull_chunks
                data["overlap"] = {"peak_active_slow_streams": peak,
                                   "all_slow_overlap_ms": max(0, overlap_end - overlap_start) / 1e6}
            else:
                require(peak, light, "peak snapshot stream overlap")
                for item in data["marker_observations"]:
                    stages = [item["witness"][key]["first_positive"]["end_ns"] for key in ("source", "a_get", "b_get", "b_current")]
                    require(overlap_start <= item["write_start_ns"] <= max(stages) < overlap_end, True,
                            "light marker written and witnessed while all slow snapshot streams overlap")
                data["overlap"] = {"peak_active_slow_streams": peak, "all_slow_start_ns": overlap_start,
                                   "all_slow_end_ns": overlap_end, "all_slow_overlap_ms": (overlap_end - overlap_start) / 1e6}
            require(relay.errors, [], "relay errors")
            data["passed"] = True
    except Exception as error:
        data["error"] = repr(error)
        raise
    finally:
        data["transfers"] = relay.transfers
        data["relay_http_body_bytes"] = relay.bytes
        data["relay_errors"] = relay.errors
        data["relay_shutdown_errors"] = relay.shutdown_errors
        post_checkpoint_error = data.get("passed") and relay.errors
        if post_checkpoint_error:
            data["passed"] = False
            data["error"] = f"relay runtime errors after checkpoint: {relay.errors!r}"
        check.RUNNER.write_json(root / "result.json", data)
        if post_checkpoint_error:
            raise RuntimeError(data["error"])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--dbs", required=True, choices=(2, 4, 8, 16), type=int)
    parser.add_argument("--shared-cap", choices=(2, 4, 8, 15), type=int)
    args = parser.parse_args()
    if args.shared_cap and args.dbs != 16:
        parser.error("shared link requires 16 followed databases")
    trial(args)


if __name__ == "__main__":
    main()
