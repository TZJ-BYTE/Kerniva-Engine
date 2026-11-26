#!/usr/bin/env python3
"""Summarize complete write-frame Fsync intervals aligned with client offer time."""
import json
from pathlib import Path
import sys

NS = 1_000_000_000
PARTITIONS = ((0, 1_200_000_000), (1_200_000_000, 30 * NS), (0, 2 * NS), (2 * NS, 30 * NS))


def cohort(rows, lower, upper, pad, after, before):
    durations = sorted(row["duration_ns"] for row in rows
                       if row["start_monotonic_ns"] >= upper + pad + after
                       and row["end_monotonic_ns"] <= lower - pad + before)
    if not durations:
        raise ValueError("empty timing bin")
    count = len(durations)
    return {"interval_ns": [after, before], "count": count,
            "duration_sum_ns": sum(durations), "max_ns": durations[-1],
            "p50_ns": durations[(count-1)//2], "p99_ns": durations[(count*99+99)//100-1],
            "at_least_10ms": sum(value >= 10_000_000 for value in durations),
            "at_least_50ms": sum(value >= 50_000_000 for value in durations)}


def summarize(root, prefix):
    summary = json.loads((root / f"{prefix}-fsync-timing.json").read_bytes())
    lower, upper = summary["physical_epoch_ns"]
    pad = summary["trace_alignment_pad_ns"]
    with (root / f"{prefix}-fsync-transitions.jsonl").open() as source:
        rows = [row for row in map(json.loads, source) if row["write_frames"]]
    return {"slow_requests": summary["counts"]["successful_slow"],
            "writer_frames_fsync_count_full_trace": len(rows),
            "writer_frames_fsync_max_ns_full_trace": max(row["duration_ns"] for row in rows),
            "writer_frames_fsync_at_least_50ms_full_trace": sum(
                row["duration_ns"] >= 50_000_000 for row in rows),
            "certain_offer_intervals": [cohort(rows, lower, upper, pad, after, before)
                                       for after, before in PARTITIONS]}


def main():
    root = Path(sys.argv[1])
    report = {prefix: summarize(root, prefix) for prefix in ("on2", "on3")}
    (root / "fsync-window-comparison.json").write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
