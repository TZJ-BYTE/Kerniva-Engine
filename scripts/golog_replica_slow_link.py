#!/usr/bin/env python3
"""Pace real replication bytes over loopback without privileged network tooling."""

from contextlib import closing, contextmanager
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import struct
from threading import Event, Lock, Thread
import time
from urllib.parse import parse_qs, urlsplit

from golog_replica_stress import covered_logs, replica_state


LINK_BYTES_PER_SECOND = 112 << 10
CHUNK_BYTES = 16 << 10
STALL_BYTES = 1024
PULL_IDLE_SECONDS = 3
PAYLOAD_KEYS = 160
VALUE_BYTES = 32 << 10


class Transfer:
    def __init__(self, mode):
        self.mode = mode
        self.manifest = None
        self.began = 0.0
        self.last_progress = 0.0
        self.sent = 0
        self.total = 0
        self.samples = []
        self.paused = Event()
        self.done = Event()
        self.lock = Lock()

    def progress(self, count):
        with self.lock:
            self.sent += count
            self.last_progress = time.monotonic()
            elapsed = self.last_progress - self.began
            if not self.samples or elapsed >= self.samples[-1]["elapsed_seconds"] + 1 or self.sent == self.total:
                self.samples.append({"elapsed_seconds": round(elapsed, 3), "bytes": self.sent})

    def evidence(self):
        with self.lock:
            return {
                "mode": self.mode,
                "manifest": self.manifest,
                "payload_bytes": self.total,
                "forwarded_bytes": self.sent,
                "last_progress_seconds": round(self.last_progress - self.began, 3) if self.sent else 0,
                "samples": list(self.samples),
            }


class PullRelay:
    """Change one DB 0 response and hold retries until its outcome is observed."""

    def __init__(self, source_port):
        self.source_port = source_port
        self.lock = Lock()
        self.armed = None
        self.resume = Event()
        self.resume.set()
        self.shutdown = Event()
        self.error = None

    def arm(self, mode):
        transfer = Transfer(mode)
        with self.lock:
            self.armed = transfer
            self.resume.clear()
        return transfer

    @contextmanager
    def listen(self):
        relay = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                try:
                    relay.forward(self)
                except (BrokenPipeError, ConnectionResetError):
                    # The follower can close the connection before a timed-out response finishes.
                    pass
                except Exception as error:
                    relay.error = repr(error)

            def log_message(self, *_args):
                pass

        with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
            thread = Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                yield server.server_port
            finally:
                self.shutdown.set()
                self.resume.set()
                server.shutdown()
                thread.join()

    def forward(self, handler):
        query = parse_qs(urlsplit(handler.path).query)
        transfer = None
        if query.get("db") == ["0"]:
            with self.lock:
                transfer, self.armed = self.armed, None
            if transfer is None and (not self.resume.wait(80) or self.shutdown.is_set()):
                return
        try:
            with closing(HTTPConnection("127.0.0.1", self.source_port, timeout=5)) as upstream:
                upstream.request("GET", handler.path)
                with upstream.getresponse() as response:
                    self.respond(handler, response, transfer)
        except ConnectionRefusedError:
            if transfer is not None:
                raise
            handler.send_error(503, "replication source not available yet")
        finally:
            if transfer is not None:
                transfer.done.set()

    def respond(self, handler, response, transfer):
        if response.status != 200:
            if transfer is not None:
                raise RuntimeError(f"source rejected the controlled pull with HTTP {response.status}")
            handler.send_error(response.status, f"source HTTP {response.status}")
            return

        header = response.read(4)
        length, = struct.unpack(">I", header)
        if not 0 < length <= 1 << 20:
            raise RuntimeError(f"invalid manifest length {length}")
        raw = response.read(length)
        manifest = json.loads(raw)
        files = manifest.get("files") or []
        total = sum(entry["size"] for entry in files)
        if transfer is not None:
            if not STALL_BYTES < total <= 8 << 20:
                raise RuntimeError(f"controlled pull has unexpected payload size {total}")
            # Finish the source HTTP request at full speed. Only the follower sees the shaped
            # stream, so a baseline failure cannot be attributed to the source write timeout.
            body = response.read(total)
            if len(body) != total or response.read(1):
                raise RuntimeError("source response differs from its manifest")
            with transfer.lock:
                transfer.manifest = manifest
                transfer.total = total
                transfer.began = time.monotonic()
        handler.send_response(200)
        handler.send_header("Content-Type", response.getheader("Content-Type"))
        handler.send_header("Content-Length", str(len(header) + len(raw) + total))
        handler.end_headers()
        handler.wfile.write(header + raw)
        if transfer is None:
            remaining = total
            while remaining:
                chunk = response.read(min(remaining, CHUNK_BYTES))
                if not chunk:
                    raise RuntimeError(f"source response missing {remaining} payload bytes")
                handler.wfile.write(chunk)
                remaining -= len(chunk)
            return

        if transfer.mode == "stall":
            handler.wfile.write(body[:STALL_BYTES])
            handler.wfile.flush()
            transfer.progress(STALL_BYTES)
            transfer.paused.set()
            self.resume.wait(20)
            return

        for offset in range(0, total, CHUNK_BYTES):
            chunk = body[offset:offset + CHUNK_BYTES]
            delay = transfer.began + (offset + len(chunk)) / LINK_BYTES_PER_SECOND - time.monotonic()
            if self.shutdown.wait(max(delay, 0)):
                return
            handler.wfile.write(chunk)
            transfer.progress(len(chunk))


def run(root, args, *, start, start_follower, stop, write, await_replica, record,
        require, wait_for, http, storage, source_port, a_port, b_port):
    database = root / "b" / "follower-v1" / "db_0"
    current = database / "CURRENT"
    relay = PullRelay(source_port)

    def wait(label, check, timeout=None):
        def probe():
            if relay.error is not None:
                raise RuntimeError(f"controlled replica relay failed: {relay.error}")
            return check()

        return wait_for(label, probe, args.timeout if timeout is None else timeout)[0]

    def get(port, key):
        return http(port, "/get", db=0, key=key)

    def partial_file(transfer, previous, *, timeout=None):
        generation = json.loads(previous)["generation"]

        def probe():
            manifest = transfer.evidence()["manifest"]
            if manifest is None:
                return None
            entry = manifest["files"][0]
            for path in database.glob(f"generation-*/{entry['name']}"):
                if path.parent.name != generation:
                    size = path.stat().st_size
                    if 0 < size < entry["size"]:
                        return path, size
            return None

        return wait("incomplete file staged by B", probe, timeout)

    def old_view(previous, expected):
        require(current.read_bytes(), previous, "B durable CURRENT")
        state = replica_state(http, b_port, 0)
        cursor = json.loads(previous)
        require((state["ready"], state["sequence"], state["source"]),
                (True, cursor["sequence"], cursor["source"]), "B reads its previous cursor")
        for key, value in expected.items():
            if value is None:
                require(get(b_port, key)[0], 404, f"B old view GET {key}")
            else:
                require(get(b_port, key), (200, value.encode("ascii")), f"B old view GET {key}")
        return state

    def failed(transfer, partial, previous, expected, *, idle):
        def observe():
            state = replica_state(http, b_port, 0)
            return state if state["last_error"] else None

        state = wait("B reported the interrupted pull", observe, 38 if not idle else PULL_IDLE_SECONDS + 6)
        elapsed = time.monotonic() - (transfer.last_progress if idle else transfer.began)
        error = state["last_error"].lower()
        if idle:
            require("idle" in error and "timeout" in error, True, f"B idle timeout error {error!r}")
            if not PULL_IDLE_SECONDS - 0.75 <= elapsed <= PULL_IDLE_SECONDS + 4:
                raise RuntimeError(f"B idle deadline was observed after {elapsed:.3f}s")
            require(transfer.done.is_set(), False, "relay kept the stalled response open at timeout")
        else:
            require("deadline exceeded" in error or "timeout" in error, True, f"B baseline timeout error {error!r}")
            if not 27 <= elapsed <= 36:
                raise RuntimeError(f"B total deadline was observed after {elapsed:.3f}s")
        require(partial.parent.exists(), False, "B cleaned the interrupted staging generation")
        require(sorted(path.name for path in database.glob("generation-*")),
                [json.loads(previous)["generation"]], "B kept only its old generation after timeout")
        return {"state": old_view(previous, expected), "elapsed_seconds": round(elapsed, 3),
                "staged_generation_removed": str(partial.parent)}

    def check_installed(result, sequence, cutoff):
        state = result["states"]["0"]
        cursor = json.loads(current.read_bytes())
        require((state["sequence"], state["snapshot"]), (sequence, cutoff), "B published exact position")
        require((cursor["sequence"], cursor["snapshot"]), (sequence, cutoff), "B persisted exact position")
        require((database / cursor["generation"]).is_dir(), True, "B CURRENT refers to an installed generation")

    def bulk_key(index):
        return f"slow:bulk:{index:03d}"

    def bulk_value(index):
        return f"row={index:03d};".ljust(VALUE_BYTES, "x")

    record("slow_link_scope", {"shaped_hop": "loopback relay to follower B",
                               "source_response": "fully read at local speed before shaping",
                               "covers_source_slow_write_deadline": False})
    seed = {(0, "slow:anchor"): "old", (0, "slow:removed"): "present", (1, "slow:db1"): "seed"}
    write([(("SET", "slow:anchor", "old"), "OK"), (("SET", "slow:removed", "present"), "OK"),
           (("SELECT", "1"), "OK"), (("SET", "slow:db1", "seed"), "OK")])
    start_follower("a", a_port, "follower-a")
    with relay.listen() as relay_port:
        follower_b = start_follower("b", b_port, "follower-b-seed", relay_port)
        a = await_replica(a_port, "A seeded", (2, 1), seed)
        b = await_replica(b_port, "B seeded", (2, 1), seed)
        previous = current.read_bytes()
        stop(follower_b)
        record("slow_link_seed", {"a": a, "b": b})

        write((("SET", bulk_key(index), bulk_value(index)), "OK") for index in range(PAYLOAD_KEYS))
        write([(("SET", "slow:anchor", "new"), "OK"), (("DEL", "slow:removed"), 1)])
        expected = {**seed, (0, "slow:anchor"): "new", (0, "slow:removed"): None}
        for index in (0, PAYLOAD_KEYS // 2, PAYLOAD_KEYS - 1):
            expected[(0, bulk_key(index))] = bulk_value(index)
        cutoff = 2 + PAYLOAD_KEYS + 2
        await_replica(a_port, "A received all sealed snapshot input", (cutoff, 1), expected)
        maintainer = start("maintainer-slow-link", [args.maintainer, "--data-dir", storage, "--once"])
        require(maintainer.wait(timeout=args.timeout), 0, "Rust maintainer exit")
        primary_db = storage / "db_0"
        retained = covered_logs(primary_db)
        require((retained["cutoff"], retained["segments"]), (cutoff, 0), "B must miss the retained window")
        snapshot = primary_db / f"snapshot-{cutoff:020d}.bin"
        snapshot_bytes = snapshot.stat().st_size
        if not (4 << 20) <= snapshot_bytes <= (6 << 20):
            raise RuntimeError(f"snapshot must be between 4 and 6 MiB, got {snapshot_bytes} bytes")
        write([(("SET", "slow:tail", "sealed after snapshot"), "OK")])
        expected[(0, "slow:tail")] = "sealed after snapshot"
        latest = cutoff + 1
        await_replica(a_port, "A received sealed suffix before shaping", (latest, 1), expected)

        slow = relay.arm("paced")
        try:
            follower_b = start_follower("b", b_port, "follower-b-paced", relay_port)
            partial, size = partial_file(slow, previous)
            manifest = slow.evidence()["manifest"]
            require((manifest["after"], manifest["snapshot"], manifest["last"]), (2, cutoff, latest),
                    "controlled response carries the missing snapshot and sealed suffix")
            require(manifest["files"][0], {"name": snapshot.name, "size": snapshot_bytes},
                    "controlled response snapshot matches disk")
            old_expected = {"slow:anchor": "old", "slow:removed": "present", "slow:tail": None, bulk_key(0): None}
            record("paced_transfer_staged", {"state": old_view(previous, old_expected), "path": str(partial),
                                             "staged_bytes": size, "rate_bytes_per_second": LINK_BYTES_PER_SECOND,
                                             "minimum_transfer_seconds": round(slow.total / LINK_BYTES_PER_SECOND, 3)})
            write([(("SELECT", "1"), "OK"), (("SET", "slow:db1", "advanced during paced transfer"), "OK")])
            expected[(1, "slow:db1")] = "advanced during paced transfer"
            a = await_replica(a_port, "A advances while B is paced", (latest, 2), expected)
            require(slow.done.is_set(), False, "A advanced before B paced transfer ended")
            record("independent_replica_advanced", a)

            def later_progress():
                if slow.evidence()["last_progress_seconds"] < 20:
                    return None
                disk_bytes = partial.stat().st_size
                return disk_bytes if disk_bytes >= 2 << 20 else None

            disk_bytes = wait("B continues staging the healthy slow stream near its deadline", later_progress, 26)
            record("paced_transfer_still_progressing", {"state": old_view(previous, old_expected),
                                                        "staged_bytes": disk_bytes,
                                                        "elapsed_seconds": round(time.monotonic() - slow.began, 3)})

            if args.slow_link == "baseline":
                record("baseline_total_deadline", failed(slow, partial, previous, old_expected, idle=False))
                wait("relay noticed the baseline connection close", slow.done.is_set)
                pace = slow.evidence()
                if len(pace["samples"]) < 20 or pace["last_progress_seconds"] < 27 or slow.sent >= slow.total:
                    raise RuntimeError(f"baseline did not receive a sustained incomplete stream: {pace}")
                relay.resume.set()
            else:
                wait("candidate completed the paced response", slow.done.is_set,
                     slow.total / LINK_BYTES_PER_SECOND + args.timeout)
                pace = slow.evidence()
                require(slow.sent, slow.total, "candidate received the full paced payload")
                if pace["last_progress_seconds"] <= 30 or len(pace["samples"]) < 30:
                    raise RuntimeError(f"candidate stream did not stay active beyond 30s: {pace}")
                relay.resume.set()
            restored = await_replica(b_port, "B installed snapshot and suffix", (latest, 2), expected)
            check_installed(restored, latest, cutoff)
            record("b_installed_snapshot_and_suffix", restored)
        finally:
            record("paced_relay_progress", slow.evidence())

        if args.slow_link == "baseline":
            return

        previous = current.read_bytes()
        stop(follower_b)
        write([(("MSET", "slow:anchor", "after stall", "slow:after-idle", bulk_value(0)), "OK")])
        expected.update({(0, "slow:anchor"): "after stall", (0, "slow:after-idle"): bulk_value(0)})
        await_replica(a_port, "A received the segment used for the stall", (latest + 1, 2), expected)
        stall = relay.arm("stall")
        try:
            start_follower("b", b_port, "follower-b-idle", relay_port, pull_idle_timeout=f"{PULL_IDLE_SECONDS}s")
            wait("relay stopped sending inside the sealed segment", stall.paused.is_set)
            partial, size = partial_file(stall, previous)
            manifest = stall.evidence()["manifest"]
            require((manifest["after"], manifest.get("snapshot", 0), manifest["last"]), (latest, 0, latest + 1),
                    "the idle witness paused an incremental sealed segment")
            require(size, STALL_BYTES, "B staged only the forwarded prefix")
            old_expected = {"slow:anchor": "new", "slow:tail": "sealed after snapshot", "slow:after-idle": None}
            record("stall_transfer_staged", {"state": old_view(previous, old_expected), "path": str(partial),
                                             "staged_bytes": size, "idle_timeout_seconds": PULL_IDLE_SECONDS})
            record("candidate_idle_deadline", failed(stall, partial, previous, old_expected, idle=True))
            relay.resume.set()
            restored = await_replica(b_port, "B retried the stalled segment at full speed", (latest + 1, 2), expected)
            check_installed(restored, latest + 1, cutoff)
            record("b_recovered_after_idle_timeout", restored)
        finally:
            record("stalled_relay_progress", stall.evidence())
