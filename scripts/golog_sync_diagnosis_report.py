#!/usr/bin/env python3
"""Read back saved WSL counters and compare conservative in-window intervals."""
import argparse
from hashlib import file_digest
import json
from pathlib import Path


def read(path):
    return json.loads(path.read_bytes())


def rows(path):
    with path.open() as stream:
        yield from map(json.loads, stream)


def clock_edges(clock):
    return clock["monotonic_before_ns"], clock["monotonic_after_ns"]


def metrics(sample):
    source = {name: entry["value"] for name, entry in sample["source"].items()}
    disk = source["disk"]["fields"]
    cpu = source["cpu"]
    return {**{key: disk[key] for key in ("writes_completed", "write_ms", "io_ms", "weighted_io_ms",
                                          "flushes_completed", "flush_ms") if key in disk},
            "cpu_total_ticks": sum(cpu["cpu"][:8]), "cpu_iowait_ticks": cpu["cpu"][4],
            "cpu_busy_ticks": sum(cpu["cpu"][:3] + cpu["cpu"][5:8]), "context_switches": cpu["ctxt"][0],
            "psi_io_some_us": source["psi_io"]["some"]["total"],
            "psi_io_full_us": source["psi_io"]["full"]["total"],
            "psi_cpu_some_us": source["psi_cpu"]["some"]["total"]}


def interval(samples, start_ns, end_ns):
    selected = [sample for sample in samples if clock_edges(sample["start"])[0] >= start_ns
                and clock_edges(sample["end"])[1] <= end_ns]
    if len(selected) < 2:
        return {"samples": len(selected)}
    first, last = map(metrics, (selected[0], selected[-1]))
    elapsed_ns = clock_edges(selected[-1]["start"])[0] - clock_edges(selected[0]["start"])[0]
    return {"samples": len(selected), "elapsed_ms": round(elapsed_ns / 1e6, 3),
            "counter_delta": {key: last[key] - value for key, value in first.items()},
            "max_io_in_progress": max(sample["source"]["disk"]["value"]["fields"]["io_in_progress"]
                                      for sample in selected),
            "max_procs_blocked": max(sample["source"]["cpu"]["value"]["procs_blocked"][0] for sample in selected),
            "max_procs_running": max(sample["source"]["cpu"]["value"]["procs_running"][0] for sample in selected),
            "max_sampler_lateness_ms": round(max(clock_edges(sample["start"])[0] - sample["target_monotonic_ns"]
                                                   for sample in selected) / 1e6, 3)}


def analyzed_window(path, capture):
    client = read(path / "client.json")
    if (client["offered"], client["succeeded"], client["failures"]) != (180000, 180000, 0):
        raise ValueError(f"incomplete 180,000-request window {path}")
    meta, *samples, footer = rows(path / "guest-passive.jsonl")
    if footer["sources_failed"] or footer["sample_rows"] != len(samples) or not capture["pass"]:
        raise ValueError(f"incomplete passive capture in {path}")
    alignment = read(path / "case.json")["measurement_alignment"]
    epoch_early, epoch_late = alignment["projected_start_monotonic_ns"]
    spans = {"first_1.2s": (0, 1_200_000_000), "second_1.2s": (1_200_000_000, 2_400_000_000),
             "6s_to_10s": (6_000_000_000, 10_000_000_000), "last_20s": (10_000_000_000, 30_000_000_000)}
    slow = [row for row in rows(path / "client-trace.jsonl") if "planned_ns" in row
            and row["completed_ns"] - row["planned_ns"] >= 100_000_000]
    if len(slow) != capture["slow_count"]:
        raise ValueError(f"inconsistent slow count in {path}")
    if slow:
        first = min(row["planned_ns"] for row in slow)
        last = max(row["completed_ns"] for row in slow)
        spans["slow_request_envelope"] = (first, last)
    return {"folder": path.name, "capture": footer, "device": meta["device"], "slow_requests": len(slow),
            "client_epoch_uncertainty_ms": round((epoch_late - epoch_early) / 1e6, 3),
            "counter_intervals": {name: {"relative_client_ms": [start / 1e6, end / 1e6],
                                         **interval(samples, epoch_late + start, epoch_early + end)}
                                  for name, (start, end) in spans.items()},
            "client_p99_ms": {op: client[op]["success_latency"]["p99_us"] / 1000 for op in ("get", "set")}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("raw", type=Path)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()
    manifest = read(args.raw / "ext4-readback.json")
    for name, expected in manifest["raw_sha256"].items():
        with (args.raw / name).open("rb") as source:
            if file_digest(source, "sha256").hexdigest() != expected:
                raise ValueError(f"readback hash mismatch for {name}")
    result = {"verified_raw_files": len(manifest["raw_sha256"]), "windows": [
        analyzed_window(args.raw / window["folder"], window) for window in read(args.raw / "run.json")["windows"]]}
    args.out.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
