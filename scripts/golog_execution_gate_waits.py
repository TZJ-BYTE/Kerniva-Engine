#!/usr/bin/env python3
"""Summarize Go runtime waits intersecting an episode on the trace's monotonic clock.

Usage: go tool trace -d=parsed server.trace | python golog_execution_gate_waits.py
       --start-ns START --end-ns END --out summary.json
"""

import argparse
import json
from pathlib import Path
import re
import sys


TRANSITION = re.compile(
    r"^M=\S+ P=\S+ G=\S+ StateTransition Time=(\d+) GoID=(\d+) (\w+)->(\w+)"
)
FRAMES = {
    "execution_shared": "/database.LockSharedExecution",
    "execution_exclusive": "/database.LockExecution",
    "writer_stats": "/golog.(*Writer).Stats",
    "writer_queue": "/golog.(*Writer).enqueueWrite",
}


def transitions(lines):
    match = None
    frames = []
    in_transition_stack = False
    for line in lines:
        if line.startswith("M="):
            if match:
                yield match.groups(), frames
            match = TRANSITION.match(line)
            frames = []
            in_transition_stack = False
        elif match and line.startswith("TransitionStack="):
            in_transition_stack = True
        elif match and line.startswith("Stack="):
            in_transition_stack = False
        elif in_transition_stack and line.startswith("\t") and " @ " in line:
            frames.append(line.split(" @ ", 1)[0].strip())
    if match:
        yield match.groups(), frames


def classify(frames):
    for label, suffix in FRAMES.items():
        if any(frame.endswith(suffix) for frame in frames):
            return label
    if any("/server.dispatchCommand" in frame for frame in frames):
        return "other_dispatch"
    return None


def empty_group():
    return dict(count=0, ge10ms=0, ge100ms=0, max_ms=0, total_ns=0, top=[])


def summarize(lines, start_ns, end_ns):
    active = {}
    groups = {name: empty_group() for name in (*FRAMES, "other_dispatch")}
    first = last = None
    for (timestamp, goid, old, new), frames in transitions(lines):
        timestamp = int(timestamp)
        if first is None:
            first = timestamp
        last = timestamp
        if old == "Waiting" and goid in active:
            begin, kind, stack = active.pop(goid)
            if begin < end_ns and timestamp > start_ns:
                elapsed = timestamp - begin
                duration_ms = elapsed / 1_000_000
                group = groups[kind]
                group["count"] += 1
                group["ge10ms"] += elapsed >= 10_000_000
                group["ge100ms"] += elapsed >= 100_000_000
                group["max_ms"] = max(group["max_ms"], duration_ms)
                group["total_ns"] += elapsed
                longest = group["top"]
                if len(longest) < 2 or duration_ms > longest[-1]["ms"]:
                    longest.append(dict(goid=int(goid), start=begin, end=timestamp,
                                        ms=duration_ms, stack=stack))
                    longest.sort(key=lambda wait: -wait["ms"])
                    del longest[2:]
        if new == "Waiting" and old != "Undetermined" and timestamp < end_ns:
            kind = classify(frames)
            if kind:
                active[goid] = (timestamp, kind, frames)
    if first is None or start_ns < first or end_ns > last:
        raise ValueError("episode must be covered by parsed Go goroutine transitions")
    for group in groups.values():
        group["total_ms"] = group["total_ns"] / 1_000_000
    return {
        "episode_ns": [start_ns, end_ns],
        "trace_transition_extent_ns": [first, last],
        "scope": "Completed waiting intervals intersecting the half-open episode. Durations include the full interval, exclude runnable time, and can overlap across goroutines.",
        "unclosed_matching_waits_started_before_episode_end": len(active),
        "groups": groups,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-ns", required=True, type=int, help="absolute trace monotonic start")
    parser.add_argument("--end-ns", required=True, type=int, help="exclusive absolute trace monotonic end")
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()
    if args.start_ns >= args.end_ns:
        parser.error("--end-ns must exceed --start-ns")
    try:
        report = summarize(sys.stdin, args.start_ns, args.end_ns)
    except ValueError as error:
        parser.error(str(error))
    args.out.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
