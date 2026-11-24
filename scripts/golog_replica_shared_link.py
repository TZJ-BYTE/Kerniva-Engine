"""A shared B-facing payload clock and bounded snapshot generation admission."""
from collections import deque
from contextlib import contextmanager
import json
from pathlib import Path
from queue import Queue
import time
from threading import Condition, Event, Thread

from golog_replica_slow_link import CHUNK_BYTES


class SharedLink:
    def __init__(self, evidence, root, cap, rate, tail):
        self.ev, self.root, self.cap, self.rate, self.tail = evidence, root, cap, rate, tail
        self.busy = 0
        self.ticket = deque()
        self.admission = Condition()
        self.in_flight = {}
        self.paced = Queue()
        self.snapshot_bytes = 0
        self.payload_bytes = 0
        self.chunks = []
        self.clock_ns = None

    def start(self, resources):
        thread = Thread(target=self._run, daemon=True)
        thread.start()

        def stop():
            self.paced.put(None)
            thread.join()

        resources.callback(stop)

    def _run(self):
        # At most one queued chunk per response. Returning a chunk lets that
        # response join the back without blocking any other response's turn.
        due = None
        while (job := self.paced.get()) is not None:
            db, snapshot, writer, payload, done, outcome = job
            now = time.monotonic_ns()
            if self.clock_ns is None:
                self.clock_ns = self.ev.now()
            due = max(due or now, now) + (len(payload) * 1_000_000_000 + self.rate - 1) // self.rate
            sleep = (due - now) / 1e9
            if sleep > 0:
                time.sleep(sleep)
            try:
                writer.write(payload)
                ns = self.ev.now()
                self.payload_bytes += len(payload)
                if snapshot:
                    self.snapshot_bytes += len(payload)
                self.chunks.append({"ns": ns, "db": db, "size": len(payload), "snapshot": snapshot,
                                    "total_bytes": self.payload_bytes})
            except Exception as error:
                outcome.append(error)
            finally:
                done.set()

    def write(self, db, snapshot, writer, payload):
        done = Event()
        outcome = []
        self.paced.put((db, snapshot, writer, payload, done, outcome))
        done.wait()
        if outcome:
            raise outcome[0]

    @contextmanager
    def admit(self, db):
        token = object()
        with self.admission:
            self.ticket.append(token)
            requested = self.ev.record("shared_slot_requested", db=db)
            if not self.admission.wait_for(lambda: self.ticket[0] is token and self.busy < self.cap, timeout=60):
                self.ticket.remove(token)
                self.admission.notify_all()
                raise TimeoutError(f"DB {db} could not enter the shared snapshot gate")
            self.ticket.popleft()
            self.busy += 1
            self.admission.notify_all()
            acquired = self.ev.record("shared_slot_admitted", db=db, busy=self.busy)
        trace = {"requested_ns": requested, "admitted_ns": acquired}
        self.in_flight[db] = trace
        try:
            yield trace
            deadline = time.monotonic() + 20
            path = self.root / f"b/follower-v1/db_{db}/CURRENT"
            while time.monotonic() < deadline:
                cursor = json.loads(path.read_bytes())
                if cursor["sequence"] == self.tail:
                    trace["published_ns"] = self.ev.record("shared_current_verified", db=db,
                                                          generation=cursor["generation"])
                    trace["current"] = cursor
                    break
                time.sleep(0.005)
            else:
                raise TimeoutError(f"DB {db} did not publish CURRENT while holding admission")
        finally:
            with self.admission:
                self.busy -= 1
                trace["released_ns"] = self.ev.record("shared_slot_released", db=db, busy=self.busy)
                self.admission.notify_all()

    def evidence(self):
        elapsed = self.chunks[-1]["ns"] - self.clock_ns
        peak_deficit = max(chunk["total_bytes"] - (chunk["ns"] - self.clock_ns) * self.rate / 1e9
                           for chunk in self.chunks)
        return {"cap": self.cap, "scheduled_rate_bytes_per_second": self.rate, "payload_bytes": self.payload_bytes,
                "snapshot_payload_bytes": self.snapshot_bytes, "scheduled_elapsed_ns": elapsed,
                "achieved_payload_bytes_per_second": self.payload_bytes * 1e9 / elapsed,
                "max_observed_early_bytes": peak_deficit, "slots": self.in_flight, "chunks": self.chunks}


def demo_snapshot(port):
    """Read passive WSL process counters for the running demo; issue no requests."""
    pids = []
    for candidate in Path("/proc").iterdir():
        if not candidate.name.isdecimal():
            continue
        try:
            parts = (candidate / "cmdline").read_bytes().split(b"\0")
            if (any(part.endswith(b"/kerniva-replica-demo.py") for part in parts)
                    and b"--port" in parts and str(port).encode() in parts):
                pids.append(int(candidate.name))
        except (FileNotFoundError, ProcessLookupError, PermissionError):
            continue
    result = {"ns": time.monotonic_ns(), "controller": None, "processes": {}}
    if len(pids) != 1:
        return result
    parent = pids[0]
    result["controller"] = parent
    children = Path(f"/proc/{parent}/task/{parent}/children").read_text().split()
    for pid in [str(parent), *children]:
        base = Path("/proc") / pid
        try:
            fields = (base / "stat").read_text().rsplit(")", 1)[1].split()
            counters = {key: int(value) for key, value in
                        (line.split(":", 1) for line in (base / "io").read_text().splitlines())}
            result["processes"][pid] = {"cpu_ticks": int(fields[11]) + int(fields[12]), **counters}
        except (FileNotFoundError, ProcessLookupError, PermissionError):
            continue
    return result


def demo_delta(start, end):
    stable = start["controller"] == end["controller"] and start["processes"].keys() == end["processes"].keys()
    delta = {}
    for pid in start["processes"].keys() & end["processes"].keys():
        delta[pid] = {key: end["processes"][pid][key] - old for key, old in start["processes"][pid].items()}
    controller = delta.get(str(start["controller"]), {})
    return {"seconds": (end["ns"] - start["ns"]) / 1e9, "stable_processes": stable,
            "controller_pid": start["controller"], "process_counters": delta,
            "controller_activity": bool(controller.get("rchar") or controller.get("wchar"))}
