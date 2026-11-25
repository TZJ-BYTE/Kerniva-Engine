"""Publish compact stage results from the per-window request and telemetry replays."""
import argparse
import hashlib
import json
from pathlib import Path

STAGES = ("calibration", "formal-1", "formal-2")


def read(path):
    return json.loads(path.read_bytes())


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def summarize(root, stages):
    result = {"schema_version": 1, "stages": {}, "formal_queue_majority_windows": {"A": [], "E": []}}
    for stage in stages:
        replay = root / "trace-replay" / (stage + ".json")
        phase = read(replay)
        ledger = read(root / stage / "run.json")
        rows = []
        for w, entry in zip(phase["windows"], ledger["windows"], strict=True):
            if entry["label"] != w["case"]:
                raise ValueError("replay and ledger window mismatch")
            folder = root / stage / entry["attempts"][-1]["directory"]
            client = read(folder / "client.json")
            row = {k: w[k] for k in ("case","arm","mode","started_utc","offered","success","failures",
                  "in_offer_success","actual_success_rate","set_p99_us","set_p999_us","get_p99_us",
                  "trigger_seconds","control_seconds")}
            row["client_gc"] = {"collections":client["client_num_gc_delta"],
                                "pause_ns":client["client_gc_pause_ns_delta"]}
            row["client_sha256"] = digest(folder / "client.json")
            if "external_control_seconds" in w:
                row["external_control_seconds"] = w["external_control_seconds"]
            if w["trace"]:
                t, telemetry = w["trace"], w["telemetry"]
                row["request_capture"] = {k: t[k] for k in ("uniform","exact_slow","retained","drops","sampled_workers")}
                row["host_minus_guest_utc_bracket_ns"] = telemetry["host_minus_guest_utc_bracket_ns"]
                row["resource_intervals"] = {k:len(v) for k,v in telemetry["intervals"].items()}
                row["trigger_slow"] = {}
                for op, group in t["trigger_slow"].items():
                    if not group["count"]:
                        row["trigger_slow"][op] = {"count":0}
                        continue
                    row["trigger_slow"][op] = {k:group[k] for k in
                        ("count","queue_strict_majority","queue_equal_half","queue_below_half","sum_ns","durations",
                         "traced_immediate_predecessors","predecessor_exchange_spans_current_plan",
                         "max_total_example","max_total_traced_predecessor")}
                slow = row["trigger_slow"]["set"]
                controls = {i for ids in w["control_seconds"].values() for i in ids if i is not None}
                has_control = bool(controls or any(w.get("external_control_seconds",{}).values()))
                if (stage.startswith("formal") and has_control and slow["count"] and
                        slow["queue_strict_majority"]*2 > slow["count"]):
                    result["formal_queue_majority_windows"][w["arm"]].append(stage+"/"+w["case"])
            rows.append(row)
        result["stages"][stage] = {"replay_sha256":digest(replay),"pairs":phase["sample_pairs"],"windows":rows,
                                    "count":len(rows), "trigger_windows":sum(bool(w["trigger_seconds"]) for w in rows),
                                    "trigger_seconds":sum(len(w["trigger_seconds"]) for w in rows)}
    windows = [w for phase in result["stages"].values() for w in phase["windows"]]
    totals = {k: sum(w[k] for w in windows) for k in ("offered","success","failures","in_offer_success")}
    totals.update(windows=len(windows), planned_seconds=30*len(windows),
                  trigger_windows=sum(bool(w["trigger_seconds"]) for w in windows),
                  trigger_seconds=sum(len(w["trigger_seconds"]) for w in windows),
                  client_gc_collections=sum(w["client_gc"]["collections"] for w in windows),
                  client_gc_pause_ns=sum(w["client_gc"]["pause_ns"] for w in windows))
    for key in ("uniform","retained","drops"):
        totals[key] = sum(w["request_capture"][key] for w in windows if w["mode"] == "ON")
    totals["exact_slow_on"] = {op: sum(w["request_capture"]["exact_slow"][op] for w in windows if w["mode"]=="ON")
                               for op in ("set","get")}
    result["totals"] = totals
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--stages", nargs="+", choices=STAGES, default=STAGES)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    result = summarize(args.root, args.stages)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_bytes((json.dumps(result, sort_keys=True, indent=2)+"\n").encode())


if __name__ == "__main__":
    main()
