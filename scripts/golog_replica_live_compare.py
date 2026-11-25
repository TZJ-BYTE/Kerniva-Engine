"""Recompute primary fixed-rate A/B results from per-second counts and sparse histograms."""
import argparse
import hashlib
import json
import math
import runpy
from pathlib import Path

analysis = runpy.run_path(str(Path(__file__).with_name("thesis-analyze.py")))
RATES = ("actual_send_rate", "actual_success_rate", "window_send_rate", "window_success_rate")
COUNTS = ("offered", "sent", "succeeded", "failures", "sent_in_window", "succeeded_in_window")
CONFIG = ("network", "durability", "clients", "keys", "value_bytes", "read_percent", "hot_percent",
          "hot_keys", "seed", "duration_seconds", "target_rate", "timeout_seconds", "queue_per_client")
TAILS = {"p99_us": 0.99, "p99_9_us": 0.999}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def equal_numbers(actual, expected, name):
    require(math.isclose(actual, expected, rel_tol=1e-9, abs_tol=1e-9),
            f"{name}: {actual} disagrees with {expected}")


def histogram_tails(raw, summary, count, label):
    bins = raw["bins"] or []
    require(sum(item["count"] for item in bins) == raw["count"] == count == summary["count"],
            f"{label}: histogram count differs from client accounting")
    indices = [item["index"] for item in bins]
    require(indices == sorted(set(indices)) and all(item["count"] > 0 for item in bins),
            f"{label}: invalid histogram bins")
    tails = {}
    for field, fraction in TAILS.items():
        rank = math.ceil(count * fraction)
        seen = 0
        value = 0
        for item in bins:
            seen += item["count"]
            if seen >= rank:
                value = min(analysis["histogram_upper"](item["index"]), raw["max_ns"]) / 1000
                break
        equal_numbers(value, summary[field], f"{label}.{field}")
        tails[field] = value if count else None
    return tails


def read_window(folder):
    files = {key: (folder / name).read_bytes() for key, name in
             (("case", "case.json"), ("client", "client.json"), ("histogram", "client-hist.json"))}
    case, client, histogram = (json.loads(files[key]) for key in ("case", "client", "histogram"))
    require(client["mode"] == "mixed" and client["durability"] == "sync", "unexpected benchmark mode")
    require(client["window_basis"] == "planned_send_second" and
            client["latency_origin"] == "planned_send_to_full_reply_or_failure_including_client_queue",
            "the measurement uses different window or latency boundaries")
    for field, key in (("rate", "target_rate"), ("read_percent", "read_percent"), ("seed", "seed")):
        require(case[field] == client[key], f"{key}: mismatch between case and client")
    seconds = [window["planned_second"] for window in client["windows"]]
    require(seconds == list(range(math.ceil(client["duration_seconds"]))), "missing or reordered per-second counts")
    counts = {field: 0 for field in COUNTS}
    tails = {}
    for op in ("get", "set"):
        result = client[op]
        raw = histogram[op]
        require(result["succeeded"] + result["failures"] == result["offered"], f"{op}: denominator mismatch")
        for field in COUNTS:
            subtotal = sum(second[op][field] for second in client["windows"])
            require(subtotal == result[field], f"{op}.{field}: per-second count differs")
            if field != "failures":
                require(raw["counts"][field] == subtotal, f"{op}.{field}: histogram count differs")
            counts[field] += subtotal
        for data, name, denominator in (("success", "success_latency", "succeeded"),
                                        ("observed", "observed_latency", "offered"),
                                        ("client_queue", "client_queue_delay", "offered")):
            tails[f"{op}_{data}"] = histogram_tails(raw[data], result[name], result[denominator], f"{op}.{name}")
    for field in COUNTS[:4]:
        require(client[field] == counts[field], f"{field}: overall count differs")
    duration = client["duration_seconds"]
    equal_numbers(counts["offered"], duration * client["target_rate"], "offered")
    elapsed = client["elapsed_seconds"]
    rates = dict(zip(RATES, (counts["sent"] / elapsed, counts["succeeded"] / elapsed,
                             counts["sent_in_window"] / duration, counts["succeeded_in_window"] / duration)))
    for key in RATES:
        equal_numbers(rates[key], client[key], key)
    first, last, slope, rising = analysis["queue_trend"](client["windows"])
    return {
        "case": folder.name, "arm": case["arm"], "driver_status": case["status"],
        "client_exit_code": case["client_exit_code"],
        "sha256": {key: hashlib.sha256(data).hexdigest() for key, data in files.items()},
        "config": {key: client[key] for key in CONFIG}, "counts": counts, "rates": rates,
        "get_set_tails": tails, "queue_first_third_mean_us": first, "queue_last_third_mean_us": last,
        "queue_slope_us_per_second": slope, "sustained_queue_growth": rising,
        "primary_load_met": counts["failures"] == 0 and min(rates.values()) >= 0.98 * client["target_rate"] and not rising,
    }


def replication_observations(folder):
    case = json.loads((folder / "case.json").read_bytes())
    receipt = json.loads((folder / "live-observer.json").read_bytes())
    raw = (folder / "live-observer-evidence" / "witnesses.json").read_bytes()
    require(receipt["passed"] and hashlib.sha256(raw).hexdigest() == receipt["witness_sha256"],
            "observer receipt does not match witnesses")
    alignment = case["measurement_alignment"]
    require(alignment["projection_consistent"], "time projection disagrees")
    start = alignment["projected_start_monotonic_ns"][1]
    end = alignment["projected_offer_end_monotonic_ns"][0]
    samples = [sample for sample in receipt["sequence_lag_samples"]
               if start <= sample["start_ns"] <= sample["end_ns"] <= end]
    lag = {}
    for sample in samples:
        peers = lag.setdefault(str(sample["db"]), {})
        for peer, bounds in sample["sequence_lag_bounds"].items():
            counts = peers.setdefault(peer, {"samples": 0, "max_lower": 0, "max_upper": 0})
            counts["samples"] += 1
            counts["max_lower"] = max(counts["max_lower"], bounds[0])
            counts["max_upper"] = max(counts["max_upper"], bounds[1])
    fields = ("db", "ordinal", "key", "sequence", "scope", "scope_reason", "source_available_ns",
              "source_to_get_ms", "source_to_disk_current_ms")
    marks = [{key: marker[key] for key in fields if key in marker} for marker in receipt["markers"]]
    terminal = case["terminal"]
    snapshots = alignment["snapshot_brackets_inside_projected_offer"]
    return {
        "witness_sha256": receipt["witness_sha256"], "measurement_interval_ns": [start, end],
        "markers": marks, "scope_limited_markers": receipt["scope_limited_markers"],
        "sampled_sequence_lag_against_sealed_source": lag, "snapshot_publications_inside_offered_window": snapshots,
        "end_primary": terminal["primary"], "end_follower_current": terminal.get("follower_current", {}),
        "terminal_db0_workload_keys_per_node": {peer: len(values) for peer, values in
                                                 terminal["bench_get_samples_hex"].items()},
        "process_exit_codes": case["process_exit_codes"],
    }


def compare(root, phase, with_replication=False):
    layout = "ABBA" if phase == "formal" else "AB"
    windows, pairs, errors = [], [], []
    expected = []
    for read, rate in ((50, 3000), (95, 6000)):
        group = []
        for arm in layout:
            name = f"{len(expected) + 1:02d}-r{read}-q{rate}-{arm}"
            expected.append(name)
            try:
                result = read_window(root / name)
                cfg = result["config"]
                require((result["arm"], cfg["read_percent"], cfg["target_rate"]) == (arm, read, rate),
                        f"{name}: matrix identity differs")
                require((cfg["network"], cfg["keys"], cfg["clients"], cfg["value_bytes"], cfg["hot_percent"])
                        == ("gnet", 100000, 64, 32, 0), f"{name}: workload differs")
                if with_replication:
                    result["replication"] = replication_observations(root / name)
                windows.append(result)
                require(result["client_exit_code"] == 0, f"{name}: nonzero client exit")
                group.append(result)
            except (OSError, ValueError, KeyError, TypeError, ZeroDivisionError) as exc:
                errors.append({"case": name, "reason": str(exc)})
                group.append(None)
        for left, right in zip(group[::2], group[1::2]):
            if not left or not right:
                continue
            a, b = (left, right) if left["arm"] == "A" else (right, left)
            if a["config"] != b["config"]:
                errors.append({"pair": [left["case"], right["case"]], "reason": "client configurations differ"})
                continue
            delta = {}
            for key in TAILS:
                before = a["get_set_tails"]["get_success"][key]
                after = b["get_set_tails"]["get_success"][key]
                delta[key] = (after - before) if before is not None and after is not None else None
            pairs.append({"order": left["arm"] + right["arm"], "A": a["case"], "B": b["case"],
                          "both_primary_load_met": a["primary_load_met"] and b["primary_load_met"],
                          "get_success_B_minus_A": delta})
    extra_cases = sorted(p.name for p in root.iterdir() if (p / "case.json").is_file() and p.name not in expected)
    if extra_cases:
        errors.append({"unregistered_cases": extra_cases})
    return {"phase": phase, "expected_windows": expected, "windows": windows, "pairs": pairs,
            "errors": errors, "client_evidence_complete": not errors,
            "all_driver_cases_completed": not errors and all(w["driver_status"] == "completed" for w in windows)}


if __name__ == "__main__":
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("root", type=Path, help="directory directly containing the phase's window directories")
    cli.add_argument("--phase", choices=("pilot", "formal"), required=True)
    cli.add_argument("--with-replication", action="store_true",
                     help="include bounded marker observations and lag from the second pilot or formal matrix")
    cli.add_argument("--output", type=Path, required=True)
    args = cli.parse_args()
    report = compare(args.root, args.phase, args.with_replication)
    args.output.write_bytes((json.dumps(report, indent=2, allow_nan=False) + "\n").encode("utf-8"))
    raise SystemExit(0 if report["client_evidence_complete"] else 1)
