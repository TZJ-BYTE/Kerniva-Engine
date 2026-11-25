"""External witnesses for an already-available sealed delta on Linux."""

from contextlib import contextmanager
from hashlib import sha256
from http.client import HTTPConnection, HTTPException
import json
import os
from pathlib import Path
import shutil
import time
from urllib.parse import urlencode

from golog_replica_snapshot_capacity import current, digest, require


NS_PER_MS = 1_000_000
WITNESS_SLEEP_MS = 5


def disk_sample(root, host_disk):
    start = time.monotonic_ns()
    paths = (root,) if host_disk is None else (root, host_disk)
    free = {}
    for path in paths:
        try:
            free[str(path)] = dict(zip(("total", "used", "free"), shutil.disk_usage(path)))
        except OSError as error:
            free[str(path)] = {"error": repr(error)}
    return {"start_ns": start, "end_ns": time.monotonic_ns(), "free_bytes": free}


def process_sample(pid):
    proc = Path(f"/proc/{pid}")
    try:
        fields = (proc / "stat").read_text().rsplit(")", 1)[1].split()
        counters = {key: int(value) for key, value in (line.split(":", 1)
                    for line in (proc / "io").read_text().splitlines())}
        return {"pid": pid, "cpu_user_ticks": int(fields[11]), "cpu_system_ticks": int(fields[12]),
                "process_start_ticks": int(fields[19]), "io": counters}
    except OSError as error:
        return {"pid": pid, "error": repr(error)}


class Observer:
    def __init__(self, stream, data, interval_ms, host_disk):
        self.stream, self.data, self.host_disk = stream, data, host_disk
        self.interval_ns = int(interval_ms * NS_PER_MS)
        self.process_times = []
        self.valid_process_times = []
        self.process_errors = 0
        self.witness_times = {"current": [], "get": []}

    def emit(self, event, **row):
        self.stream.write(json.dumps({"event": event, **row}, separators=(",", ":")) + "\n")
        self.stream.flush()

    def sample_process(self, proc):
        start = time.monotonic_ns()
        sample = process_sample(proc.pid)
        self.emit("process", start_ns=start, end_ns=time.monotonic_ns(), **sample)
        self.process_times.append(start)
        if "error" in sample:
            self.process_errors += 1
        else:
            self.valid_process_times.append(start)

    def witness(self, name, probe):
        state = self.data.setdefault(name, {})
        if "first_positive" in state:
            return
        start = time.monotonic_ns()
        positive, observed = probe()
        row = {"start_ns": start, "end_ns": time.monotonic_ns(), "observed": observed}
        self.emit(name, positive=positive, **row)
        self.witness_times[name].append(start)
        if positive is not None:
            state["first_positive" if positive else "last_negative"] = row

    def wait(self, proc, root, db, before, target, port, key, value_hash, deadline_ns):
        def read_current():
            cursor = current(root, db)
            require(cursor["source"], before["source"], "observed CURRENT source")
            require(cursor["sequence"] in (before["sequence"], target), True, "observed CURRENT sequence")
            if cursor["sequence"] == before["sequence"]:
                require(cursor, before, "observed seed CURRENT")
            return cursor["sequence"] == target, cursor

        connection = HTTPConnection("127.0.0.1", port, timeout=0.25)
        path = "/get?" + urlencode({"db": db, "key": key})

        def read_key():
            try:
                connection.request("GET", path)
                with connection.getresponse() as response:
                    status, body = response.status, response.read()
            except ConnectionRefusedError as error:
                connection.close()
                return False, {"error": repr(error), "listener_unavailable": True}
            except (OSError, HTTPException) as error:
                connection.close()
                return None, {"error": repr(error)}
            require(status in (200, 404), True, "observed GET status")
            checksum = sha256(body).hexdigest()
            if status == 200:
                require(checksum, value_hash, "first GET value")
            return status == 200, {"status": status, "bytes": len(body), "sha256": checksum}

        try:
            next_sample = 0
            while time.monotonic_ns() < deadline_ns:
                if proc.poll() is not None:
                    raise RuntimeError(f"follower exited before external witnesses: {proc.returncode}")
                if time.monotonic_ns() >= next_sample:
                    self.sample_process(proc)
                    next_sample = self.process_times[-1] + self.interval_ns
                self.witness("current", read_current)
                self.witness("get", read_key)
                if all("first_positive" in self.data[name] for name in ("current", "get")):
                    self.sample_process(proc)
                    break
                time.sleep(WITNESS_SLEEP_MS / 1000)
            else:
                raise TimeoutError(f"external witnesses timed out for DB {db}")
        finally:
            connection.close()

        spawn = self.data["follower_start"]
        for name in ("current", "get"):
            evidence = self.data[name]
            # A seed CURRENT was read before launch. The follower GET endpoint
            # did not exist before launch. Either gives a conservative zero
            # lower bound when startup runs faster than the first observation.
            lower = evidence.get("last_negative", {"start_ns": spawn["start_ns"]})["start_ns"]
            upper = evidence["first_positive"]["end_ns"]
            self.data[f"{name}_visible_after_start_ms"] = [
                max(0, lower - spawn["end_ns"]) / NS_PER_MS, (upper - spawn["start_ns"]) / NS_PER_MS,
            ]
        self.data["durable_upper_after_start_ms"] = self.data["get_visible_after_start_ms"][1]
        require(self.data["get"]["first_positive"]["end_ns"] >=
                self.data["current"].get("last_negative", spawn)["start_ns"], True,
                "no negative CURRENT observation after correct GET")


def gaps(stamps):
    return {"samples": len(stamps),
            "max_gap_ms": max((b - a for a, b in zip(stamps, stamps[1:])), default=0) / NS_PER_MS}


@contextmanager
def external_observation(root, data, source, interval_ms, host_disk):
    path = root / "b-external.jsonl"
    evidence = data["external"] = {
        "clock": "CLOCK_MONOTONIC", "source": source,
        "witness_target_sleep_ms": WITNESS_SLEEP_MS,
        "process_target_period_ms": interval_ms, "cpu_ticks_per_second": os.sysconf("SC_CLK_TCK"),
    }
    try:
        with path.open("w", encoding="utf-8") as stream:
            observer = Observer(stream, evidence, interval_ms, host_disk)
            observer.emit("source_verified_available", **source)
            observer.emit("disk_start", **disk_sample(root, host_disk))
            try:
                yield observer
            finally:
                observer.emit("disk_end", **disk_sample(root, host_disk))
                evidence["sampling"] = {
                    "process": gaps(observer.process_times), "process_valid": gaps(observer.valid_process_times),
                    "process_errors": observer.process_errors,
                    **{name: gaps(stamps) for name, stamps in observer.witness_times.items()},
                }
    finally:
        if path.exists():
            evidence.update(raw_file=path.name, raw_sha256=digest(path))
