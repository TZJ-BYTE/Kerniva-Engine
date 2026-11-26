"""Extract stop-the-world intervals from a Go 1.25 or 1.26 server execution trace.

Usage: python scripts/bench-gc-stw.py TRACE --go /path/to/go > STW.jsonl
"""

import argparse
import bisect
import io
import json
import re
import subprocess
import sys
from pathlib import Path

NS = 1_000_000_000
GC_PHASES = {"GC concurrent mark phase": "gc_concurrent_mark", "GC mark assist": "gc_mark_assist"}
GC_REASONS = {"GC sweep termination", "GC mark termination"}
GO125_HEADER = b"go 1.25 trace\0\0\0"
GO126_HEADER = b"go 1.26 trace\0\0\0"
# Go internal/trace/tracev2/events.go for versions 1.25 and 1.26.
# Go 1.25's debug text omits fractional wall seconds. Read raw clock
# snapshots and let the Go reader resolve the time ranges.
EVENT_BATCH, FREQUENCY, EXPERIMENTAL_BATCH, SYNC, CLOCK_SNAPSHOT = 1, 8, 49, 50, 51
END_OF_GENERATION = 52
SYNC_LINE = re.compile(r" Sync Time=(\d+) N=(\d+)(?: Trace=(\d+) Mono=(\d+) Wall=\S+)?$")
RANGE_LINE = re.compile(
    r' Range(Begin|Active|End) Time=(\d+) Name="(stop-the-world \([^"]+\)|GC concurrent mark phase|GC mark assist)" Scope=(\S+)'
)


def read_uint(stream):
    value = 0
    for shift in range(0, 70, 7):
        raw = stream.read(1)
        if not raw or (shift == 63 and raw[0] > 1):
            raise ValueError("invalid trace varint")
        value |= (raw[0] & 127) << shift
        if raw[0] < 128:
            return value
    raise ValueError("trace varint overflow")


def read_syncs(path):
    anchors = {}
    with path.open("rb") as file:
        header = file.read(len(GO125_HEADER))
        if header not in (GO125_HEADER, GO126_HEADER):
            raise ValueError("expected Go 1.25 or 1.26 trace format")
        end = path.stat().st_size
        final_generation_complete = False
        while kind := file.read(1):
            if header == GO126_HEADER and kind[0] == END_OF_GENERATION:
                final_generation_complete = True
                continue
            final_generation_complete = False
            experimental = kind[0] == EXPERIMENTAL_BATCH
            if experimental:
                file.read(1)
            elif kind[0] != EVENT_BATCH:
                raise ValueError(f"unexpected trace batch kind {kind[0]}")
            generation, _, base, size = (read_uint(file) for _ in range(4))
            following = file.tell() + size
            if following > end:
                raise ValueError("truncated trace batch")
            if not experimental and size and file.read(1) == bytes([SYNC]):
                batch = io.BytesIO(file.read(size - 1))
                if batch.read(1) != bytes([FREQUENCY]):
                    raise ValueError("expected trace frequency")
                freq = read_uint(batch)
                if batch.read(1) != bytes([CLOCK_SNAPSHOT]):
                    raise ValueError("expected trace clock snapshot")
                tick, mono, sec, nsec = (read_uint(batch) for _ in range(4))
                if batch.read(1) or generation in anchors or not freq or nsec >= NS:
                    raise ValueError("invalid trace clock snapshot")
                anchors[generation] = (base + tick, freq, mono, sec * NS + nsec)
            file.seek(following)
        if header == GO126_HEADER and not final_generation_complete:
            raise ValueError("trace lacks its final end-of-generation marker")
    return [anchors[gen] for gen in sorted(anchors)]


def read_ranges(path, go):
    syncs, intervals, phases = [], [], []
    open_ranges, open_phases = {}, {}
    final = None
    with subprocess.Popen(
        [go, "tool", "trace", "-d=parsed", str(path)],
        stdout=subprocess.PIPE,
        encoding="utf-8",
        errors="replace",
    ) as process:
        for line in process.stdout:
            if " Sync Time=" in line:
                match = SYNC_LINE.search(line.rstrip())
                if match is None:
                    raise ValueError(f"unrecognized Go trace sync line: {line.strip()}")
                at, index, trace, mono = match.groups()
                if final is not None or int(index) != len(syncs) + 1:
                    raise ValueError("out-of-order Go trace sync")
                if trace is None:
                    final = int(at)
                else:
                    syncs.append((int(trace), int(mono)))
            elif " Range" in line and (match := RANGE_LINE.search(line)) is not None:
                action, timestamp, name, scope = match.groups()
                at = int(timestamp)
                if name in GC_PHASES:
                    key = (name, scope)
                    if action == "Begin":
                        if key in open_phases:
                            raise ValueError(f"overlapping GC range begin: {key}")
                        open_phases[key] = at
                    elif action == "Active":
                        open_phases.setdefault(key, None)
                    else:
                        phases.append((open_phases.pop(key, None), at, name))
                    continue
                reason = name.removeprefix("stop-the-world (").removesuffix(")")
                key = (reason, scope)
                if action == "Begin":
                    if key in open_ranges:
                        raise ValueError(f"overlapping STW begin: {key}")
                    open_ranges[key] = at
                elif action == "Active":
                    if key not in open_ranges:
                        raise ValueError(f"STW interval active without a recorded beginning: {key}")
                elif key in open_ranges:
                    start = open_ranges.pop(key)
                    intervals.append((start, at, reason))
                else:
                    raise ValueError(f"STW interval end without a recorded beginning: {key}")
        if process.wait():
            raise ValueError("go tool trace failed")
    if open_ranges or final is None:
        raise ValueError(f"trace ended without a complete STW stream: {list(open_ranges)}")
    phases.extend((start, None, name) for (name, _), start in open_phases.items())
    return syncs, sorted(intervals), phases, final


def extract(path, go):
    raw = read_syncs(path)
    syncs, intervals, phases, trace_end = read_ranges(path, go)
    if not raw or len(raw) != len(syncs):
        raise ValueError("Go reader and raw trace clock snapshot counts differ")
    anchors = []
    for (tick, freq, raw_mono, wall), (trace, mono) in zip(raw, syncs):
        if raw_mono != mono or int(tick * (1.0 / (freq / 1e9))) != trace:
            raise ValueError("Go reader and raw trace clock snapshots disagree")
        anchors.append({"trace_ns": trace, "unix_ns": wall, "monotonic_ns": mono})
    trace_times = [sample["trace_ns"] for sample in anchors]
    if any(a >= b for a, b in zip(trace_times, trace_times[1:])):
        raise ValueError("Go trace clock snapshots are not increasing")

    def anchor_at(at):
        # Convert against the closest snapshot. The observed offset span is
        # reported separately so consumers can assess alignment sensitivity.
        index = bisect.bisect_left(trace_times, at)
        choices = anchors[max(index - 1, 0):index + 1]
        return min(choices, key=lambda sample: abs(sample["trace_ns"] - at))

    def clock_values(at):
        anchor = anchor_at(at)
        delta = at - anchor["trace_ns"]
        return anchor["unix_ns"] + delta, anchor["monotonic_ns"] + delta

    startup = [end for _, end, reason in intervals if reason == "start trace"]
    if len(startup) != 1 or trace_end <= startup[0]:
        raise ValueError("cannot establish complete trace coverage")
    offsets = [sample["unix_ns"] - sample["trace_ns"] for sample in anchors]
    coverage_start, coverage_start_mono = clock_values(startup[0])
    coverage_end, coverage_end_mono = clock_values(trace_end)
    phase_stats = {}
    for name, label in GC_PHASES.items():
        ranges = [(start, end) for start, end, phase in phases if phase == name]
        complete = [(start, end) for start, end in ranges if start is not None and end is not None]
        phase_stats[label] = {
            "complete_count": len(complete),
            "incomplete_count": len(ranges) - len(complete),
            "total_complete_ns": sum(end - start for start, end in complete),
            "max_complete_ns": max((end - start for start, end in complete), default=0),
        }
    yield {
        "type": "meta",
        "schema_version": 1,
        "clock_alignment": "nearest Go trace clock snapshot",
        "anchors": anchors,
        "observed_offset_span_ns": max(offsets) - min(offsets),
        "coverage_start_unix_ns": coverage_start,
        "coverage_end_unix_ns": coverage_end,
        "coverage_start_monotonic_ns": coverage_start_mono,
        "coverage_end_monotonic_ns": coverage_end_mono,
        "gc_phase_stats": phase_stats,
        "coverage_basis": "end of start trace STW through final decoded event",
        "gc_stw_count": sum(reason in GC_REASONS for _, _, reason in intervals),
        "stw_count": len(intervals),
    }
    for start, end, reason in intervals:
        unix, mono = clock_values(start)
        yield {
            "type": "stw",
            "reason": reason,
            "is_gc": reason in GC_REASONS,
            "start_unix_ns": unix,
            "end_unix_ns": unix + end - start,
            "start_monotonic_ns": mono,
            "end_monotonic_ns": mono + end - start,
            "pause_ns": end - start,
        }


    for start, end, name in phases:
        start_unix, start_mono = clock_values(start) if start is not None else (None, None)
        end_unix, end_mono = clock_values(end) if end is not None else (None, None)
        yield {
            "type": "gc_phase",
            "phase": GC_PHASES[name],
            "start_unix_ns": start_unix,
            "end_unix_ns": end_unix,
            "start_monotonic_ns": start_mono,
            "end_monotonic_ns": end_mono,
            "observed_start_monotonic_ns": (
                max(start_mono, coverage_start_mono) if start_mono is not None else coverage_start_mono
            ),
            "observed_end_monotonic_ns": (
                min(end_mono, coverage_end_mono) if end_mono is not None else coverage_end_mono
            ),
            "duration_ns": end - start if start is not None and end is not None else None,
            "left_truncated": start is None,
            "right_truncated": end is None,
        }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("trace", type=Path)
    parser.add_argument("--go", default="go", help="Go executable matching the trace recorder")
    args = parser.parse_args()
    try:
        for row in extract(args.trace, args.go):
            print(json.dumps(row))
    except (OSError, ValueError) as error:
        sys.exit(f"Cannot extract STW intervals: {error}")


if __name__ == "__main__":
    main()
