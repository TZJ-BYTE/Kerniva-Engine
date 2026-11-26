#!/usr/bin/env python3
"""Detail guest polling gaps and counter changes near recorded Go-log stalls."""
import argparse
from collections import Counter
import json
from pathlib import Path
from statistics import median

from golog_batch_report import records

CPU_NAMES = ("user", "nice", "system", "idle", "iowait", "irq", "softirq", "steal", "guest", "guest_nice")
FOCUS = {"05": (), "07": (7025, 7067, 7197)}
DISK_COUNTERS = ("io_ms", "weighted_io_ms", "flushes_completed", "flush_ms", "writes_completed", "write_ms")


def start(clock):
    return clock["monotonic_before_ns"]


def end(clock):
    return clock["monotonic_after_ns"]


def overlaps(a, b):
    return a[0] <= b[1] and b[0] <= a[1]


def interval(source):
    return [start(source["start"]), end(source["end"])]


def values(row):
    return {key: source["value"] for key, source in row["source"].items() if "value" in source}


def summarize(row, previous, period):
    source = row["source"]
    opened, closed = interval(row)
    observed = values(row)
    cpu = observed["cpu"]
    disk = observed["disk"]["fields"]
    out = {"slot": row["index"], "target_ns": row["target_monotonic_ns"], "observation_ns": [opened, closed],
           "wake_lateness_ns": opened - row["target_monotonic_ns"],
           "sample_elapsed_ns": closed - opened,
           "source_intervals_ns": {key: interval(item) for key, item in source.items()},
           "source_elapsed_ns": {key: end(item["end"]) - start(item["start"]) for key, item in source.items()},
           "io_in_progress": disk["io_in_progress"], "procs_running": cpu["procs_running"][0],
           "procs_blocked": cpu["procs_blocked"][0]}
    if previous is None:
        return out
    prior = values(previous)
    out.update(missing_slots=row["index"] - previous["index"] - 1,
               previous_wake_lateness_ns=start(previous["start"]) - previous["target_monotonic_ns"],
               first_unserviced_deadline_ns=previous["target_monotonic_ns"] + period,
               inter_sample_start_ns=opened - start(previous["start"]),
               unobserved_between_samples_ns=opened - end(previous["end"]),
               previous_slot=previous["index"], previous_observation_end_ns=end(previous["end"]),
               previous_source_elapsed_ns={key: end(item["end"]) - start(item["start"])
                                           for key, item in previous["source"].items()},
               disk_delta={key: disk[key] - prior["disk"]["fields"][key] for key in DISK_COUNTERS},
               cpu_delta_ticks={key: new - old for key, new, old in
                                zip(CPU_NAMES, cpu["cpu"], prior["cpu"]["cpu"])},
               context_switch_delta=cpu["ctxt"][0] - prior["cpu"]["ctxt"][0],
               psi_delta_us={key: {mode: result["total"] - prior[key][mode]["total"]
                                   for mode, result in observed[key].items()}
                             for key in ("psi_cpu", "psi_io")})
    out["delta_source_spans_ns"] = {key: [start(previous["source"][key]["start"]), end(source[key]["end"])]
                                    for key in source}
    return out


def distribution(rows):
    def stats(items):
        return {"median": median(items), "max": max(items)}

    return {"samples": len(rows), "wake_lateness_ns": stats([row["wake_lateness_ns"] for row in rows]),
            "inter_sample_start_ns": stats([row["inter_sample_start_ns"] for row in rows[1:]]),
            "source_elapsed_ns": {key: stats([row["source_elapsed_ns"][key] for row in rows])
                                  for key in rows[0]["source_elapsed_ns"]},
            "disk_io_in_progress": dict(Counter(row["io_in_progress"] for row in rows)),
            "procs_running": dict(Counter(row["procs_running"] for row in rows)),
            "procs_blocked": dict(Counter(row["procs_blocked"] for row in rows)),
            "cpu_delta_ticks": {key: stats([row["cpu_delta_ticks"][key] for row in rows[1:]])
                                for key in ("iowait", "steal")},
            "psi_delta_us": {key: {mode: stats([row["psi_delta_us"][key][mode] for row in rows[1:]])
                                   for mode in ("some", "full")} for key in ("psi_cpu", "psi_io")}}


def analyze(folder):
    guest = list(records(folder / "guest-passive.jsonl"))
    meta, samples = guest[0], guest[1:-1]
    period = meta["interval_ns"]
    rows = [summarize(row, samples[i-1] if i else None, period) for i, row in enumerate(samples)]
    clocks = [sample[mark] for sample in samples for mark in ("start", "end")]
    # Conversion envelope from observed WSL realtime-minus-monotonic clock pairs.
    wall_minus_mono = [min(c["utc_unix_ns"] - end(c) for c in clocks),
                       max(c["utc_unix_ns"] - start(c) for c in clocks)]
    result = {"window": folder.name, "device": meta["device"], "interval_ns": period,
              "cpu_ticks_per_second": meta["cpu_ticks_per_second"], "guest_realtime_minus_mono_ns": wall_minus_mono,
              "full_window": distribution(rows), "gaps": [r for r in rows if r.get("missing_slots")],
              "long_rows": [r for r in rows if r["sample_elapsed_ns"] >= period]}
    key = folder.name[:2]
    if key not in FOCUS:
        return result
    regular = [row for row in rows[1:] if 3_000_000_000 <= row["target_ns"] - rows[0]["target_ns"] <= 18_000_000_000]
    result["same_window_3_to_18_seconds"] = distribution(regular)
    server = records(folder / "server-batches.jsonl")
    origin = next(server)["origin_unix_ns"]
    batches = {row["batch"]["id"]: row["batch"] for row in server
               if row.get("batch", {}).get("sync_end_ns")}
    top = sorted(batches, key=lambda i: batches[i]["sync_end_ns"] - batches[i]["sync_start_ns"], reverse=True)[:3]
    result["batches"] = []
    for identifier in dict.fromkeys((*FOCUS[key], *top)):
        batch = batches[identifier]
        utc_begin = origin + batch["sync_start_ns"]
        utc_end = origin + batch["sync_end_ns"]
        mono = [utc_begin - wall_minus_mono[1], utc_end - wall_minus_mono[0]]
        result["batches"].append({"id": identifier, "frames": batch["size"], "sync_ns": utc_end - utc_begin,
                                  "guest_mono_span_ns": mono,
                                  "sample_slots_inside": [row["slot"] for row in rows
                                      if mono[0] <= row["observation_ns"][0] <= row["observation_ns"][1] <= mono[1]],
                                  "adjacent_samples": [row for row in rows[1:] if
                                      overlaps(mono, [row["previous_observation_end_ns"] - period,
                                                      row["observation_ns"][1] + period])]})
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("raw", type=Path, help="natural-window raw evidence directory")
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    result = {"clock_mapping": "Server origin UTC plus elapsed monotonic stamp, projected using the full window's "
                               "observed guest UTC-minus-monotonic envelope; unsampled clock changes are unobserved.",
              "intervals": "Counter deltas span adjacent source readings. Source elapsed time includes scheduler loss "
                           "while the read is in progress. Time between samples includes waits and wakeup delays.",
              "disk_io_in_progress": "Instantaneous guest block-device observation, not a cumulative counter.",
              "windows": [analyze(folder) for folder in sorted(args.raw.glob("??-r95-q6000-A"))]}
    args.out.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
