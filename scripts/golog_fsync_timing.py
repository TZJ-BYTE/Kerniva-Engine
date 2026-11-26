#!/usr/bin/env python3
"""Pair Go writer fsync syscall transitions and compare them with client request timing."""
from bisect import bisect_left
from collections import Counter
import json
from pathlib import Path
import re
import subprocess
import sys

TRANSITION = re.compile(
    r'^M=(-?\d+) P=(-?\d+) G=(-?\d+) StateTransition Time=(\d+) GoID=(\d+) (\w+)->(\w+)'
)


def load(path):
    return json.loads(path.read_bytes())


def transitions(lines):
    start = None
    stack = []
    for line in lines:
        if line.startswith("M="):
            if start is not None:
                yield start, stack
            start = TRANSITION.match(line)
            stack = []
        elif start is not None and start.group(7) == "Syscall" and line.startswith("\t"):
            func = line.strip().split(" @ ", 1)[0]
            if " @ " in line and func not in stack:
                stack.append(func)
    if start is not None:
        yield start, stack


def extract(binary_trace):
    opened = {}
    values = []
    counts = Counter()
    with subprocess.Popen(["go", "tool", "trace", "-d=parsed", str(binary_trace)], text=True,
                          stdout=subprocess.PIPE) as process:
        for match, stack in transitions(process.stdout):
            thread, _, _, stamp, goroutine, old, new = match.groups()
            if new == "Syscall" and "syscall.Fsync" in stack:
                assert goroutine not in opened
                assert "os.(*File).Sync" in stack
                opened[goroutine] = (int(stamp), thread, stack)
                counts["fsync_entries"] += 1
            elif old == "Syscall" and goroutine in opened:
                start, entry_thread, stack = opened.pop(goroutine)
                end = int(stamp)
                assert end >= start
                writer = any(func.endswith("(*Writer).writeFramesLocked") for func in stack)
                values.append({"t_start_ns": start, "t_end_ns": end, "duration_ns": end-start,
                               "go_id": int(goroutine), "start_thread": int(entry_thread),
                               "end_thread": int(thread), "exit_state": new, "write_frames": writer,
                               "stack": stack})
        assert process.wait() == 0
    counts["complete_entries"] = len(values)
    counts["only_entry"] = len(opened)
    return sorted(values, key=lambda row: row["t_start_ns"]), counts


def extent(spike, span, epoch, pad):
    lo, hi = epoch
    start, end = spike
    a, b = span
    # All possible positions share one unknown epoch. The overlap as a function
    # of epoch is trapezoidal, so its minimum is reached at one endpoint.
    overlap_at = lambda t: max(0, min(b-pad, end+t) - max(a+pad, start+t))
    lower = min(overlap_at(lo), overlap_at(hi))
    upper = max(0, min(end-start, b-a+2*pad, end+hi-a+pad, b+pad-start-lo))
    return lower, upper


def measure(rows, event, epoch, pad):
    stats = Counter()
    longest = {}
    for row in rows:
        operation = row["op"]
        for label, left in (("exchange", "send_start_ns"), ("planned", "planned_ns")):
            length = row["completed_ns"] - row[left]
            lower, upper = extent((row[left], row["completed_ns"]), event, epoch, pad)
            key = f"{operation}_{label}"
            stats[f"{key}_any_possible"] += int(upper > 0)
            stats[f"{key}_any_guaranteed"] += int(lower > 0)
            stats[f"{key}_at_least_90pct_guaranteed"] += int(lower * 10 >= length * 9)
            longest[key] = max(longest.get(key, 0), lower)
    return {"counts": dict(stats), "max_guaranteed_overlap_ns": longest}


def main():
    case, binary_trace, out = map(Path, sys.argv[1:4])
    prefix = sys.argv[4]
    rows, counts = extract(binary_trace)
    case_meta = load(case / "case.json")
    capture = load(case / "capture.json")
    client = load(case / "client.json")
    with (case / "server-stw.jsonl").open() as trace:
        meta = json.loads(next(trace))
    anchors = meta["anchors"]
    points = [row["trace_ns"] for row in anchors]
    offsets = [row["monotonic_ns"]-row["trace_ns"] for row in anchors]
    pad = max(offsets)-min(offsets)+max(1000, meta.get("trace_tick_ns", 0))

    def project(ts, field):
        i = bisect_left(points, ts)
        index = min(range(max(0, i-1), min(len(points), i+1)), key=lambda j: abs(points[j]-ts))
        return ts-points[index]+anchors[index][field]

    for row in rows:
        for edge in ("start", "end"):
            ts = row[f"t_{edge}_ns"]
            for field in ("monotonic_ns", "unix_ns"):
                row[f"{edge}_{field}"] = project(ts, field)
    with (out / f"{prefix}-fsync-transitions.jsonl").open("w") as file:
        for row in rows:
            file.write(json.dumps(row, separators=(",", ":")) + "\n")

    upper = capture["gc_info_after"]["start"]["monotonic_before_ns"]-round(client["elapsed_seconds"]*1e9)+1
    physical = case_meta["client_release_interval"]["start_ns"], upper
    observed = case_meta["measurement_alignment"]["projected_start_monotonic_ns"]
    sensitivity = max(physical[0], observed[0]), min(physical[1], observed[1])
    assert physical[0] <= sensitivity[0] <= sensitivity[1] <= physical[1]
    with (case / "client-trace.jsonl").open() as file:
        rows_client = [json.loads(line) for line in file]
    threshold = rows_client[0]["slow_threshold_ns"]
    slow = [row for row in rows_client[1:-1]
            if row["outcome"] == "success" and row["completed_ns"]-row["planned_ns"] >= threshold]
    counts["successful_slow"] = len(slow)
    counts["writer_frames"] = sum(row["write_frames"] for row in rows)
    longs = []
    for row in sorted(rows, key=lambda row: -row["duration_ns"]):
        if row["duration_ns"] < 10_000_000:
            break
        event = row["start_monotonic_ns"], row["end_monotonic_ns"]
        longs.append({**row,
                      "start_relative_range_ns": [event[0]-physical[1]-pad, event[0]-physical[0]+pad],
                      "end_relative_range_ns": [event[1]-physical[1]-pad, event[1]-physical[0]+pad],
                      "causal": measure(slow, event, physical, pad),
                      "wall_sensitivity": measure(slow, event, sensitivity, pad)})
    print(json.dumps({"counts": counts, "physical_epoch_ns": physical, "wall_sensitivity_epoch_ns": sensitivity,
                      "trace_alignment_pad_ns": pad,
                      "fsync_total_ns": sum(row["duration_ns"] for row in rows),
                      "writer_frames_total_ns": sum(row["duration_ns"] for row in rows if row["write_frames"]),
                      "fsync_at_least_10ms": longs,
                      "slow_by_planned_second": dict(Counter(
                          f'{row["op"]}:{row["planned_ns"]//1_000_000_000}' for row in slow))}, indent=2))


if __name__ == "__main__":
    main()
