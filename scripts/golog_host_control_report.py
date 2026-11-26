"""Compare the paired 0/220 ms Go-log fsync return-delay controls."""
import argparse
from hashlib import file_digest
import json
from pathlib import Path
import re

from golog_batch_report import phases, records, require
from golog_host_disk_report import guest_buckets, host_buckets, match_buckets


DELAYS = (0, 220)
PREFIX_NS = 120_000_000
STRACE_FSYNC = re.compile(
    r"^(\d+)\s+(\d+)\.(\d{6}) fsync\(\d+<[^>]+/db_0/active\.log>(.*)$"
)
STRACE_RESUMED = re.compile(r"^(\d+)\s+\d+\.\d{6} <\.\.\. fsync resumed>(.*)$")
STRACE_SUCCESS = re.compile(r"^\)\s+= 0 (\(DELAYED\) )?<(\d+)\.(\d{6})>$")
GUEST_DELTAS = ("flushes", "flush_ms", "writes", "write_ms")
HOST_DELTAS = ("reads", "writes", "write_bytes", "write_time_100ns")


def read(path):
    return json.loads(path.read_bytes())


def verify_readback(raw):
    manifest = read(raw / "ext4-readback.json")
    require(manifest["driver_exit_code"] == 0, "incomplete control readback")
    required = {"run.json"}
    for delay in DELAYS:
        folder = f"control-fsync-return-{delay:03d}ms"
        for filename in ("fixture.json", "fsync.strace", "server-batches.jsonl", "guest-passive.jsonl"):
            required.add(f"{folder}/{filename}")
    for relative in sorted(required):
        with (raw / relative).open("rb") as stream:
            require(file_digest(stream, "sha256").hexdigest() == manifest["raw_sha256"].get(relative),
                    f"control readback hash mismatch: {relative}")
    return len(required)


def clock_range(clock):
    return clock["monotonic_before_ns"], clock["monotonic_after_ns"]


def latency_ms(call):
    # The wall time read is inside each bracket. The midpoint difference
    # summarizes the monotonic duration, and its full range is also retained.
    start, end = (clock_range(call[key]) for key in ("start", "end"))
    lower, upper = (end[0] - start[1]) / 1e6, (end[1] - start[0]) / 1e6
    require(0 <= lower <= upper, "invalid request clock")
    return {"ms": (lower + upper) / 2, "bounds_ms": [lower, upper]}


def measured_syscall(path, request, delay):
    writer = request["write"]
    start, end = (writer[key]["utc_unix_ns"] for key in ("start", "end"))
    entries = []
    unfinished = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if match := STRACE_FSYNC.match(line):
            tid, sec, micros, result = match.groups()
            entry = int(sec) * 10**9 + int(micros) * 1000
            if not start <= entry <= end:
                continue
            if result == " <unfinished ...>":
                require(tid not in unfinished, f"{path}: overlapping fsync entries for thread {tid}")
                unfinished[tid] = entry
                continue
        elif match := STRACE_RESUMED.match(line):
            tid, result = match.groups()
            if tid not in unfinished:
                continue
            entry = unfinished.pop(tid)
        else:
            continue
        success = STRACE_SUCCESS.match(result)
        require(success is not None, f"{path}: measured fsync did not return success")
        delayed, dur_sec, dur_micros = success.groups()
        entries.append((entry, bool(delayed), int(dur_sec) * 10**6 + int(dur_micros)))
    require(len(entries) == 1 and not unfinished,
            f"{path}: expected one completed DB0 active-log fsync within measured SET")
    entry, delayed, usec = entries[0]
    require(request["db0_active_fsync_entries_utc_ns"] == [entry] and delayed == bool(delay),
            f"{path}: measured fsync disagrees with fixture receipt")
    # Translate guest realtime locally using the two request clock pairs.
    # Expand for the microsecond resolution of strace's printed entry.
    clocks = (writer["start"], writer["end"])
    wall_minus_mono = [
        clock["utc_unix_ns"] - edge for clock in clocks for edge in clock_range(clock)
    ]
    return {
        "entry_utc_ns": entry, "strace_duration_ms": usec / 1000, "delayed": delayed,
        "entry_guest_ns": [entry - max(wall_minus_mono), entry + 999 - min(wall_minus_mono)],
    }


def server_sync(path, writer, entry_guest):
    meta, *events, summary = records(path)
    require(meta["type"] == "meta" and summary["type"] == "summary" and summary["drops"] == 0
            and summary["seen"] == summary["kept"] == len(events), f"{path}: incomplete trace")
    matched = [row for row in events if row["local_port"] == writer["local_port"]]
    require(len(matched) == 1 and matched[0]["command"] == "SET"
            and matched[0]["conn_ordinal"] == 1, f"{path}: ambiguous measured SET")
    measured = matched[0]
    batch = measured["batch"]
    measured_phases = phases(measured)
    require(batch["size"] == 1, f"{path}: measured SET used a shared batch")

    start, end = (clock_range(writer[key]) for key in ("start", "end"))
    # Server stamps use a monotonic origin. Bound its location using SET send
    # before parsing, reply submission before client completion, and the traced
    # fsync entry between the server's sync start and return.
    lower = max(start[0] - measured["parsed_ns"], entry_guest[0] - batch["sync_end_ns"])
    upper = min(end[1] - measured["response_start_ns"], entry_guest[1] - batch["sync_start_ns"])
    require(lower <= upper, f"{path}: client, server and syscall clocks do not reconcile")
    return {
        "duration_ms": measured_phases["sync"] / 1e6,
        "start_guest_ns": [batch["sync_start_ns"] + edge for edge in (lower, upper)],
        "end_guest_ns": [batch["sync_end_ns"] + edge for edge in (lower, upper)],
        "batch_id": batch["id"],
        "writer_local_port": writer["local_port"],
    }


def summarize_buckets(buckets, start, end, fields):
    definite, possible = match_buckets(buckets, start, end)
    contained = [bucket for bucket in possible
                 if start[1] <= bucket["start"][0] and bucket["end"][1] <= end[0]]
    covers = bool(possible and possible[0]["start"][1] <= start[0]
                  and end[1] <= possible[-1]["end"][0]
                  and all(left["end"] == right["start"] for left, right in zip(possible, possible[1:])))
    bounds = {}
    for field in fields:
        values = [bucket.get(field) for bucket in possible]
        bounds[field] = (
            [sum(bucket[field] for bucket in contained), sum(values) if covers else None]
            if values and all(value is not None and value >= 0 for value in values)
            else [None, None]
        )
    return {
        "covers_span": covers,
        "contained_bucket_count": len(contained),
        "possible_overlap_bucket_count": len(possible),
        "contained_bucket_indices": [bucket["ix"] for bucket in contained],
        "definite_overlap_bucket_indices": [bucket["ix"] for bucket in definite],
        "counter_delta_bounds": bounds,
        "possible_overlap_buckets": possible,
    }


def arm(raw, host, delay):
    name = f"control-fsync-return-{delay:03d}ms"
    folder = raw / name
    fixture = read(folder / "fixture.json")
    require(fixture["pass"] and fixture["delay_exit_ms"] == delay and len(fixture["requests"]) == 1,
            f"{folder}: invalid control receipt")
    request = fixture["requests"][0]
    writer, reader = request["write"], request["read"]
    require(writer["command"][0] == "SET" and writer["reply"] == "OK"
            and reader["command"][0] == "GET" and reader["reply"] == "ready",
            f"{folder}: invalid measured replies")
    syscall = measured_syscall(folder / "fsync.strace", request, delay)
    entry = syscall["entry_guest_ns"]
    sync = server_sync(folder / "server-batches.jsonl", writer, entry)
    fixed_end = [edge + PREFIX_NS for edge in entry]
    require(fixed_end[1] <= clock_range(reader["start"])[0], f"{folder}: fixed span overlapped fixture GET")
    spans = {
        "server_sync": (sync["start_guest_ns"], sync["end_guest_ns"]),
        "first_120ms_from_entry": (entry, fixed_end),
    }
    win_rows, win_quality = host_buckets(host / name)
    guest_rows, guest_quality = guest_buckets(folder / "guest-passive.jsonl")
    return {
        "arm": name, "delay_exit_ms": delay,
        "request_set": latency_ms(writer), "request_db1_get": latency_ms(reader),
        "syscall": syscall, "server_sync": sync,
        "quality": {"windows": win_quality, "wsl": guest_quality},
        "spans": {
            name: {"start_guest_ns": start, "end_guest_ns": end,
                   "windows": summarize_buckets(win_rows, start, end, HOST_DELTAS),
                   "wsl": summarize_buckets(guest_rows, start, end, GUEST_DELTAS)}
            for name, (start, end) in spans.items()
        },
    }


def range_difference(baseline, delayed):
    low, high = baseline
    delayed_low, delayed_high = delayed
    return [
        delayed_low - high if delayed_low is not None and high is not None else None,
        delayed_high - low if delayed_high is not None and low is not None else None,
    ]


def compare(baseline, delayed):
    fixed = "first_120ms_from_entry"
    return {
        "request_set_ms_bounds": range_difference(baseline["request_set"]["bounds_ms"],
                                                    delayed["request_set"]["bounds_ms"]),
        "server_sync_ms": delayed["server_sync"]["duration_ms"] - baseline["server_sync"]["duration_ms"],
        "strace_duration_ms": delayed["syscall"]["strace_duration_ms"] - baseline["syscall"]["strace_duration_ms"],
        "first_120ms_counter_delta_bounds": {
            source: {
                metric: range_difference(base_bounds, delayed["spans"][fixed][source]["counter_delta_bounds"][metric])
                for metric, base_bounds in baseline["spans"][fixed][source]["counter_delta_bounds"].items()
            } for source in ("wsl", "windows")
        },
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw", required=True, type=Path)
    parser.add_argument("--host", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()
    verified_files = verify_readback(args.raw)
    run = read(args.raw / "run.json")
    require(run["driver_exit_code"] == 0 and run["kind"] == "fsync-return-control"
            and tuple(run["delays_ms"]) == DELAYS and run["measured_sets_per_arm"] == 1,
            "unexpected control run configuration")
    baseline, delayed = (arm(args.raw, args.host, delay) for delay in DELAYS)
    result = {
        "schema": 1, "verified_guest_files": verified_files,
        "arms": [baseline, delayed], "delayed_minus_baseline": compare(baseline, delayed),
        "timing": "strace -T measures traced syscall execution. Server sync spans include the injected return delay. Entry alignment assumes the guest realtime-to-monotonic offset stays within the envelope of the enclosing client clock pairs.",
        "counters": "Counter bounds sum fully contained buckets as the lower bound and all overlapping buckets as the upper bound when samples cover both edges. Null bounds indicate unavailable counters or coverage. These are aggregate device observations. Windows has no flush counter in this capture. A bucket can include unrelated I/O and cannot identify an individual fsync or confirm physical-media persistence.",
    }
    args.out.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
