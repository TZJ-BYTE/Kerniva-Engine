#!/usr/bin/env python3
"""Concurrent and interrupted-transfer checks for the real replica processes."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, contextmanager
import fcntl
import hashlib
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import signal
import socket
import struct
from threading import Event, Thread
import time


WRITERS_PER_DB = 6
KEYS_PER_WRITER = 96
BATCH_KEYS = 8
PAIR = ("stress:pair:left", "stress:pair:right")
LOG_HEADER_BYTES = len(b"KGLLOG01")


class PullGate:
    """Forward the owned source, stopping the first new DB 0 segment partway."""

    def __init__(self, source_port):
        self.source_port = source_port
        self.cut_after = None
        self.hold_db1_at = None
        self.interrupted = None
        self.freeze = Event()
        self.release = Event()
        self.recover_after = None
        self.recovery = None
        self.error = None

    @contextmanager
    def listen(self):
        gate = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                try:
                    gate.serve(self)
                except (BrokenPipeError, ConnectionResetError):
                    # Expected when the test kills the receiver.
                    pass
                except Exception as error:
                    gate.error = repr(error)
                    self.close_connection = True

            def log_message(self, *_args):
                pass

        with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
            thread = Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                yield server.server_port
            finally:
                self.release.set()
                server.shutdown()
                thread.join()

    def serve(self, handler):
        if self.interrupted is not None and self.recover_after is None:
            handler.send_error(503, "stress test paused the source")
            return
        with closing(HTTPConnection("127.0.0.1", self.source_port, timeout=3)) as upstream:
            upstream.request("GET", handler.path)
            with upstream.getresponse() as response:
                if response.status != 200:
                    handler.send_error(response.status, f"replica source replied {response.status}")
                    return
                header = response.read(4)
                length, = struct.unpack(">I", header)
                if not 0 < length <= 1 << 20:
                    raise RuntimeError(f"invalid source manifest length {length}")
                raw = response.read(length)
                manifest = json.loads(raw)
                files = manifest.get("files") or []
                snapshot = manifest.get("snapshot", 0)
                if manifest["db"] == 1 and self.hold_db1_at is not None and manifest["last"] > self.hold_db1_at:
                    handler.send_error(503, "stress test holds database 1 at its seeded cursor")
                    return
                recovering = manifest["db"] == 0 and self.recover_after is not None and self.recovery is None
                if recovering and not (snapshot > self.recover_after and manifest["last"] > snapshot):
                    handler.send_error(503, "waiting for a snapshot with a sealed suffix")
                    return

                cutting = (manifest["db"] == 0 and self.cut_after is not None and
                           manifest["after"] == self.cut_after and not snapshot and bool(files))
                handler.send_response(200)
                handler.send_header("Content-Type", response.getheader("Content-Type"))
                handler.send_header("Content-Length", str(4 + length + sum(file["size"] for file in files)))
                handler.end_headers()
                handler.wfile.write(header + raw)
                if cutting:
                    file = files[0]
                    prefix = min(1024, file["size"] - 1)
                    if prefix <= 0 or not file["name"].endswith(".seg"):
                        raise RuntimeError(f"expected a sealed log segment, got {file}")
                    payload = response.read(prefix)
                    if len(payload) != prefix:
                        raise RuntimeError(f"source closed before delivering {prefix} bytes")
                    handler.wfile.write(payload)
                    handler.wfile.flush()
                    self.interrupted = {"manifest": manifest, "received_bytes": prefix}
                    self.cut_after = None
                    self.freeze.set()
                    self.release.wait(25)
                    return

                remaining = sum(file["size"] for file in files)
                while remaining:
                    data = response.read(min(65536, remaining))
                    if not data:
                        raise RuntimeError(f"source response was truncated by {remaining} bytes")
                    handler.wfile.write(data)
                    remaining -= len(data)
                if recovering:
                    self.recovery = manifest


def writer_keys(lane):
    return [f"stress:{lane:02d}:{key:03d}" for key in range(KEYS_PER_WRITER)]


def payload(db, key, version):
    return f"db={db};key={key};version={version:09d}".ljust(128, "x")


def pairs(values):
    return [word for item in values.items() for word in item]


def sequence(client):
    return next(int(line.split(":", 1)[1]) for line in client.command("INFO").splitlines()
                if line.startswith("last_sequence:"))


def replica_state(http, port, db):
    status, body = http(port, "/status", db=db)
    if status != 200:
        raise RuntimeError(f"replica status for db {db} returned HTTP {status}")
    return json.loads(body)


def covered_logs(directory):
    with (directory / ".recovery.lock").open("rb") as lock:
        fcntl.flock(lock, fcntl.LOCK_SH)
        cutoff = int((directory / "CURRENT").read_text())
        ranges = []
        for path in directory.glob("log-*.seg"):
            _, first, last = path.stem.split("-")
            if int(last) <= cutoff:
                ranges.append((int(first), int(last), path.stat().st_size))
        cursor = cutoff
        for first, last, _ in sorted(ranges, reverse=True):
            if last != cursor or first > last:
                raise RuntimeError(f"covered logs in {directory} do not reach {cutoff} continuously: {ranges}")
            cursor = first - 1
        return {"cutoff": cutoff, "after": cursor, "segments": len(ranges),
                "bytes": sum(size for _, _, size in ranges)}


def verify_primary(client, expected, require, db):
    names = sorted(expected)
    require(client.command("DBSIZE"), sum(value is not None for value in expected.values()),
            f"primary db{db} cardinality")
    for first in range(0, len(names), 128):
        batch = names[first:first + 128]
        replies = client.command("MGET", *batch)
        require(len(replies), len(batch), f"primary db{db} array size")
        for name, reply in zip(batch, replies):
            require(reply, expected[name], f"primary db{db} GET {name}")
    return digest(expected)


def verify_replica(http, port, db, expected, require, deadline):
    for name, value in sorted(expected.items()):
        if time.monotonic() >= deadline:
            raise TimeoutError(f"key verification timed out at replica {port} db{db} key {name}")
        actual = http(port, "/get", db=db, key=name)
        if value is None:
            require(actual[0], 404, f"replica {port} db{db} GET {name}")
        else:
            require(actual, (200, value.encode("ascii")), f"replica {port} db{db} GET {name}")
    return digest(expected)


def digest(values):
    encoded = json.dumps(values, sort_keys=True, separators=(",", ":")).encode("ascii")
    keys = sum(value is not None for value in values.values())
    return {"checked": len(values), "present": keys, "absent": len(values) - keys,
            "sha256": hashlib.sha256(encoded).hexdigest()}


def write_lane(connected, require, stop, db, lane, keys):
    current = {key: payload(db, key, 0) for key in keys}
    step = acknowledged = 0
    blocks = len(keys) // BATCH_KEYS
    with connected(db) as client:
        while not stop.is_set():
            block = step % blocks
            first = block * BATCH_KEYS
            group = keys[first:first + BATCH_KEYS]
            step += 1
            if step // blocks % 3 == 1 and block % 3 == 0:
                present = sum(current[key] is not None for key in group)
                require(client.command("DEL", *group), present, f"writer db{db}/{lane} DEL {step}")
                current.update(dict.fromkeys(group))
                acknowledged += 1
            elif lane % 2:
                next_values = {key: payload(db, key, step) for key in group}
                require(client.command("MSET", *pairs(next_values)), "OK", f"writer db{db}/{lane} MSET {step}")
                current.update(next_values)
                acknowledged += 1
            else:
                for key in group:
                    value = payload(db, key, step)
                    require(client.command("SET", key, value), "OK", f"writer db{db}/{lane} SET {key} {step}")
                    current[key] = value
                    acknowledged += 1
            stop.wait(0.006)
    return db, acknowledged, current


def write_pair(connected, require, stop):
    version = 0
    with connected(0) as client:
        while not stop.is_set():
            version += 1
            value = f"{version:012d}"
            require(client.command("MSET", PAIR[0], value, PAIR[1], value), "OK", "atomic witness write")
            stop.wait(0.008)
    return 0, version, dict.fromkeys(PAIR, f"{version:012d}")


def read_pair(connected, require, stop):
    checked = 0
    previous = "000000000000"
    with connected(0) as client:
        while not stop.is_set():
            left, right = client.command("MGET", *PAIR)
            require(left, right, "primary atomic pair")
            if not isinstance(left, str) or len(left) != 12 or not left.isdigit() or left < previous:
                raise RuntimeError(f"primary witness moved from {previous} to {left!r}")
            previous = left
            checked += 1
            stop.wait(0.006)
    return {"verified": checked, "last_value": previous}


def read_replica_pair(http, port, require, stop):
    checked = changed = 0
    previous = b"000000000000"
    previous_sequence = 0
    while not stop.is_set():
        before = replica_state(http, port, 0)
        left_code, left = http(port, "/get", db=0, key=PAIR[0])
        right_code, right = http(port, "/get", db=0, key=PAIR[1])
        after = replica_state(http, port, 0)
        for status in (before, after):
            if not status["ready"] or status["last_error"] or status["sequence"] < previous_sequence:
                raise RuntimeError(f"online replica lost its view or cursor: {status}")
            previous_sequence = status["sequence"]
        require((left_code, right_code), (200, 200), "online replica witness HTTP")
        if before["sequence"] == after["sequence"]:
            require(left, right, "replica atomic pair at stable cursor")
            if len(left) != 12 or not left.isdigit() or left < previous:
                raise RuntimeError(f"replica witness moved from {previous!r} to {left!r}")
            previous = left
            checked += 1
        else:
            changed += 1
        stop.wait(0.018)
    return {"verified": checked, "cursor_changed": changed, "last_value": previous.decode("ascii"),
            "last_sequence": previous_sequence}


def run(root, args, *, primary, start, start_primary, start_follower, stop, record, require, wait_for,
        http, resp, storage, primary_port, source_port, a_port, b_port):
    @contextmanager
    def connected(db):
        with socket.create_connection(("127.0.0.1", primary_port), timeout=args.timeout) as sock:
            with resp(sock) as client:
                require(client.command("SELECT", str(db)), "OK", f"select primary db{db}")
                yield client

    def primary_sequences():
        result = []
        for db in range(2):
            with connected(db) as client:
                result.append(sequence(client))
        return result

    def followers_at(port, positions, *, offline=False):
        states = [replica_state(http, port, db) for db in range(2)]
        for state, position in zip(states, positions):
            if not state["ready"] or state["sequence"] < position or (state["last_error"] and not offline):
                return None
        return states

    life_deadline = time.monotonic() + 210

    def bounded(label, check, *, timeout=None):
        remaining = life_deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(f"stress deadline reached before {label}")
        limit = args.timeout if timeout is None else timeout
        return wait_for(label, check, min(limit, remaining))[0]

    seeds = [{}, {}]
    primary_ttl = []
    for db in range(2):
        with connected(db) as client:
            for lane in range(WRITERS_PER_DB):
                values = {key: payload(db, key, 0) for key in writer_keys(lane)}
                require(client.command("MSET", *pairs(values)), "OK", f"seed db{db} writer {lane}")
                seeds[db].update(values)
            values = dict.fromkeys(PAIR, "000000000000" if db == 0 else "other database")
            require(client.command("MSET", *pairs(values)), "OK", f"seed db{db} pair")
            seeds[db].update(values)
            expires_at = str(time.time_ns() // 1_000_000 + 600_000)
            require(client.command("SET", "stress:ttl", f"live in db{db}", "PXAT", expires_at), "OK", "seed TTL")
            require(client.command("SET", "stress:expired", "gone"), "OK", "seed expiry witness")
            require(client.command("PEXPIREAT", "stress:expired", "1"), 1, "expire seed witness immediately")
            seeds[db].update({"stress:ttl": f"live in db{db}", "stress:expired": None, "stress:missing": None})
            primary_ttl.append(int(expires_at))

    initial = primary_sequences()
    start_follower("a", a_port, "follower-a")
    gate = PullGate(source_port)
    b_database = root / "b" / "follower-v1" / "db_0"
    b_current = b_database / "CURRENT"
    with gate.listen() as relay_port:
        follower_b = start_follower("b", b_port, "follower-b", relay_port)
        initial_a = bounded("replica A seeds", lambda: followers_at(a_port, initial))
        initial_b = bounded("replica B seeds", lambda: followers_at(b_port, initial))
        require([state["sequence"] for state in initial_b], initial, "replica B seed positions")
        original_current = b_current.read_bytes()
        gate.hold_db1_at = initial[1]
        gate.cut_after = initial[0]
        record("seeded", {"a": initial_a, "b": initial_b, "keys": [digest(seed) for seed in seeds]})

        finish = Event()
        workers = []
        readers = []
        with ThreadPoolExecutor(max_workers=WRITERS_PER_DB * 2 + 3) as pool:
            try:
                workload_started = time.monotonic()
                for db in range(2):
                    for lane in range(WRITERS_PER_DB):
                        workers.append(pool.submit(write_lane, connected, require, finish, db, lane, writer_keys(lane)))
                workers.append(pool.submit(write_pair, connected, require, finish))
                readers.append(pool.submit(read_pair, connected, require, finish))
                readers.append(pool.submit(read_replica_pair, http, a_port, require, finish))

                def progress(check):
                    for job in workers + readers:
                        if job.done():
                            try:
                                job.result()
                            except Exception as error:
                                raise RuntimeError(f"concurrent workload failed: {error}") from error
                            raise RuntimeError("concurrent workload exited early")
                    if gate.error:
                        raise RuntimeError(f"replication relay failed: {gate.error}")
                    return check()

                bounded("first blocked replica transfer", lambda: progress(gate.freeze.is_set))
                manifest = gate.interrupted["manifest"]
                entry = manifest["files"][0]
                generation = json.loads(original_current)["generation"]

                def staged_file():
                    for candidate in b_database.glob(f"generation-*/{entry['name']}"):
                        if candidate.parent.name != generation and 0 < candidate.stat().st_size < entry["size"]:
                            return candidate
                    return None

                partial = bounded("received incomplete unpublished segment", lambda: progress(staged_file))
                require(b_current.read_bytes(), original_current, "replica B CURRENT during transfer")
                record("partial_transfer", {**gate.interrupted, "path": str(partial),
                                            "disk_bytes": partial.stat().st_size})

                maintenance_command = [
                    args.maintainer, "--data-dir", storage, "--min-sealed-bytes", "32768",
                    "--max-age", "2", "--interval", "1",
                ]
                if args.stress_retain_segments:
                    maintenance_command.extend(["--retain-covered-segments", str(args.stress_retain_segments)])
                maintainer = start("maintainer-autonomous", maintenance_command)
                primary_db = storage / "db_0"

                def new_cutoff(after):
                    require(maintainer.poll(), None, "autonomous maintainer process")
                    cutoff = int((primary_db / "CURRENT").read_text())
                    return cutoff if cutoff > after and not (primary_db / entry["name"]).exists() else None

                cutoff = bounded("autonomous snapshot covering held segment",
                                 lambda: progress(lambda: new_cutoff(manifest["last"] - 1)))
                require(follower_b.poll(), None, "replica B running before interruption")
                require(partial.stat().st_size, gate.interrupted["received_bytes"], "replica B is still receiving")
                require(b_current.read_bytes(), original_current, "replica B CURRENT immediately before interruption")
                stop(follower_b, crash=True)
                require(follower_b.returncode, -signal.SIGKILL, "replica B interrupted process")
                gate.release.set()
                require(b_current.read_bytes(), original_current, "replica B CURRENT after interruption")
                follower_b = start_follower("b", b_port, "follower-b-restarted-offline", relay_port)
                reopened_b = bounded("offline reload of replica B",
                                     lambda: progress(lambda: followers_at(b_port, initial, offline=True)))
                require([state["sequence"] for state in reopened_b], initial, "reopened replica B seed cursor")
                require(b_current.read_bytes(), original_current, "replica B reopened its previous generation")
                require(partial.parent.exists(), False, "replica B cleaned the interrupted generation")
                checked = [verify_replica(http, b_port, db, seed, require, life_deadline) for db, seed in enumerate(seeds)]
                record("replica_crash_and_offline_reopen", {"cutoff": cutoff, "states": reopened_b, "keys": checked,
                                                           "interrupted_exit": -signal.SIGKILL})

                second_cutoff = bounded("second autonomous snapshot during writes",
                                        lambda: progress(lambda: new_cutoff(cutoff)))
                gate.hold_db1_at = None
                gate.recover_after = initial[0]
                recovery = bounded("source transferred snapshot and sealed suffix",
                                   lambda: progress(lambda: gate.recovery))
                caught = bounded("replica B publishes snapshot and suffix",
                                 lambda: progress(lambda: followers_at(b_port, [recovery["last"], initial[1]])))
                if caught[0]["snapshot"] < recovery["snapshot"] or caught[0]["bootstraps"] < 1:
                    raise RuntimeError(f"replica B did not publish recovered snapshot: {caught[0]}")
                record("autonomous_snapshot_rejoin", {"first_cutoff": cutoff, "second_cutoff": second_cutoff,
                                                     "transfer": recovery, "states": caught})
                last_cutoff = second_cutoff
                if args.stress_seconds:
                    minimum_end = workload_started + args.stress_seconds

                    def duration_reached():
                        require(maintainer.poll(), None, "autonomous maintainer during sustained writes")
                        return time.monotonic() >= minimum_end

                    remaining_window = max(0, minimum_end - time.monotonic())
                    bounded("sustained concurrent workload duration", lambda: progress(duration_reached),
                            timeout=remaining_window + args.timeout)
                    prior_cutoff = int((primary_db / "CURRENT").read_text())

                    def next_snapshot():
                        latest = new_cutoff(prior_cutoff)
                        if latest and (primary_db / f"snapshot-{latest:020d}.bin").stat().st_size > 0:
                            return latest
                        return None

                    last_cutoff = bounded("snapshot advance after sustained workload",
                                          lambda: progress(next_snapshot))
                    positions = primary_sequences()
                    positions[0] = max(positions[0], last_cutoff)
                    sustained_followers = []
                    observed_start = time.monotonic()
                    reached_ms = []
                    for name, port in (("A", a_port), ("B", b_port)):
                        states = bounded(f"replica {name} advances during sustained workload",
                                         lambda port=port: progress(lambda: followers_at(port, positions)))
                        reached_ms.append(round((time.monotonic() - observed_start) * 1000, 2))
                        for db, state in enumerate(states):
                            require(state["source"], caught[db]["source"], f"replica {name} db{db} source")
                            verify_replica(http, port, db, {"stress:ttl": seeds[db]["stress:ttl"]},
                                           require, life_deadline)
                        sustained_followers.append(states)
                    record("sustained_checkpoint", {"before": prior_cutoff, "after": last_cutoff,
                                                    "advanced": last_cutoff - prior_cutoff,
                                                    "positions": positions, "replicas": sustained_followers,
                                                    "follower_positions_observed_ms": reached_ms})
            finally:
                finish.set()
        workload_seconds = time.monotonic() - workload_started

        expected = [dict(seed) for seed in seeds]
        acknowledgements = [0, 0]
        for job in workers:
            db, count, values = job.result()
            expected[db].update(values)
            acknowledgements[db] += count
        observations = [job.result() for job in readers]
        if min(report["verified"] for report in observations) < 10:
            raise RuntimeError(f"insufficient concurrent read observations: {observations}")
        positions = primary_sequences()
        if positions[0] <= last_cutoff or min(acknowledgements) < 100:
            raise RuntimeError(f"insufficient writes across autonomous snapshots: {positions}, {acknowledgements}")
        record("concurrent_workload", {"acknowledgements": acknowledgements, "readers": observations,
                                      "positions": positions, "requested_seconds": args.stress_seconds,
                                      "elapsed_seconds": round(workload_seconds, 3)})

        snapshots = [covered_logs(storage / f"db_{db}") for db in range(2)]
        if any(state["segments"] > args.stress_retain_segments or state["bytes"] > (64 << 20) for state in snapshots):
            raise RuntimeError(f"covered logs exceed retention budget: {snapshots}")
        if args.stress_retain_segments and any(state["segments"] == 0 for state in snapshots):
            raise RuntimeError(f"expected retained segments in both databases: {snapshots}")
        record("covered_log_retention", {"limit": args.stress_retain_segments, "databases": snapshots})

        active = [storage / f"db_{db}" / "active.log" for db in range(2)]
        bounded("earlier writes seal before final crash witness",
                lambda: all(path.stat().st_size == LOG_HEADER_BYTES for path in active))
        final = []
        for db in range(2):
            with connected(db) as client:
                require(client.command("SET", "stress:after-checkpoint", f"acknowledged in db{db}"), "OK",
                        f"db{db} final unsealed write")
                expected[db]["stress:after-checkpoint"] = f"acknowledged in db{db}"
                final.append(sequence(client))
        active_bytes = [path.stat().st_size for path in active]
        if min(active_bytes) <= LOG_HEADER_BYTES:
            raise RuntimeError(f"final crash witness was not in both active logs: {active_bytes}")
        require(primary.poll(), None, "primary running before crash")
        stop(primary, crash=True)
        require(primary.returncode, -signal.SIGKILL, "primary crash result")
        record("primary_crash_with_acknowledged_active_tail", {"positions": final, "active_bytes": active_bytes,
                                                             "exit": primary.returncode})
        start_primary("primary-restarted")
        require(primary_sequences(), final, "primary recovered positions")
        primary_results = []
        for db in range(2):
            with connected(db) as client:
                primary_results.append(verify_primary(client, expected[db], require, db))
                require(client.command("PEXPIRETIME", "stress:ttl"), primary_ttl[db], f"db{db} retained TTL")
        states = [bounded(f"replica {name} catches up after crash", lambda port=port: followers_at(port, final))
                  for name, port in (("A", a_port), ("B", b_port))]
        results = [[verify_replica(http, port, db, expected[db], require, life_deadline) for db in range(2)]
                   for port in (a_port, b_port)]
        require(results, [primary_results, primary_results], "recovered keyed digests")
        for result in states:
            require([state["sequence"] for state in result], final, "replica final positions")
            require([state["source"] for state in result], [state["source"] for state in initial_a],
                    "source identity after primary crash")
        record("complete_primary_and_replica_recovery", {"primary": primary_results, "replicas": results,
                                                         "states": states})
