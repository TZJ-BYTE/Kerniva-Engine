"""Replay the five-arm SET experiment from the mirrored attempt ledgers and raw samples."""

import argparse
from datetime import datetime, timedelta
import hashlib
import json
import math
from pathlib import Path
from statistics import median

from golog_replica_live_compare import read_window, replication_observations, require, TAILS
from golog_replica_live_observer import verify_window


ORDERS = {
    "pilot": ((3000, 6000), ("ABCDE",)),
    "pilot-noise": ((3000, 6000), ("AE",)),
    "formal-1": ((3000, 6000), ("ABCDE", "EDCBA")),
    "formal-2": ((6000, 3000), ("CDEAB", "BAEDC")),
}
CONTRASTS = ("E-A", "B-A", "C-A", "D-C", "D-B", "E-D", "D-A", "(D-C)-(B-A)")
ENDPOINTS = [(op, field) for op in ("set", "get") for field in TAILS]


def read_json(path):
    return json.loads(path.read_bytes())


def digest(path):
    with path.open("rb") as stream:
        sha = hashlib.sha256()
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            sha.update(chunk)
    return sha.hexdigest()


def offer_interval(case, client):
    align = case["measurement_alignment"]
    when = datetime.fromisoformat(client["started_utc"].replace("Z", "+00:00"))
    require(when == datetime.fromisoformat(align["client_started_utc"].replace("Z", "+00:00")), "client start differs")
    epoch = when - datetime.fromtimestamp(0, when.tzinfo)
    wall_ns = (epoch.days * 86400 + epoch.seconds) * 10**9 + epoch.microseconds * 1000
    a, b = case["measurement_clock_bridges"]
    require(all(w["end_ns"] >= w["start_ns"] for w in (a, b)) and a["end_ns"] <= b["start_ns"],
            "invalid clock samples")
    lower = min(w["start_ns"] - w["realtime_ns"] for w in (a, b))
    upper = max(w["end_ns"] - w["realtime_ns"] for w in (a, b))
    start_min = max(case["client_release_interval"]["start_ns"], wall_ns + lower - 5_000_000)
    start_max = wall_ns + upper + 5_000_000
    duration_ns = round(client["duration_seconds"] * 1e9)
    end_min, end_max = start_min + duration_ns, start_max + duration_ns
    require(align["projected_start_monotonic_ns"] == [start_min, start_max] and
            align["projected_offer_end_monotonic_ns"] == [end_min, end_max], "clock projection differs")
    completion = next(e["monotonic_ns"] for e in case["events"] if e["name"] == "client_exited")
    consistent = start_min <= start_max and end_min <= completion
    require(align["projection_consistent"] == consistent, "clock consistency flag differs")
    return when, start_max, end_min


def samples_and_markers(folder, case, start, end, duration):
    arm = case["arm"]
    peers = {"a", "b"} if arm in "CDE" else set()
    recorded = case["window_observer"]
    path = folder / "common-observer.json"
    require(recorded["sha256"] == digest(path), "common observer digest changed")
    common = read_json(path)
    markers = common["markers"]
    samples = common["primary_and_status_samples"]
    require(all(common[k] == v for k, v in recorded.items() if k != "sha256"), "raw observer counters differ")
    require(len(markers) == 6 and {(m["ordinal"], m["db"]) for m in markers} ==
            {(i, db) for i in range(3) for db in (0, 1)}, "missing or duplicate markers")
    in_window = 0
    for m in markers:
        expected = f"live-observer:{m['ordinal']}:db:{m['db']}"
        value = "value:" + expected
        require(m["key"] == expected and m["value"] == value and
                m["offset"] == (0.35, duration / 2, duration - 1.5)[m["ordinal"]], "marker input differs")
        require(m["before_primary"]["value"] is None and m["set"]["value"] == "OK" and
                m["final_primary"]["value"] == value, "unexpected primary marker")
        require(set(m["final_replicas"]) == peers and all(
            x["value"] == {"status": 200, "body": value} for x in m["final_replicas"].values()
        ), "unexpected replica marker")
        in_window += start <= m["set"]["start_ns"] <= m["set"]["end_ns"] <= end
    require(common["marker_primary_set_commands"] == 6 and common["marker_primary_get_commands"] == 12
            and common["marker_primary_select_commands"] == 18 and common["primary_sample_interval_seconds"] == 1,
            "unmatched primary observation protocol")
    require(common["common_samples"] == common["sample_primary_info_commands"] ==
            common["sample_primary_select_commands"] == len(samples) and
            common["sample_follower_status_requests"] == sum(len(s["followers"]) for s in samples),
            "observer request count differs")
    require(all(set(s["followers"]) == peers for s in samples), "sampled follower topology differs")
    def frame_bytes(*fields):
        args = [str(s).encode("ascii") for s in fields]
        return len(f"*{len(args)}\r\n") + sum(len(f"${len(v)}\r\n") + len(v) + 2 for v in args)
    marker_bytes = sum(3 * frame_bytes("SELECT", m["db"]) + 2 * frame_bytes("GET", m["key"]) +
                       frame_bytes("SET", m["key"], m["value"]) for m in markers)
    require(common["marker_primary_resp_request_bytes"] == marker_bytes and
            common["sample_primary_resp_request_bytes"] == sum(frame_bytes("SELECT", s["db"]) + frame_bytes("INFO")
                                                               for s in samples), "RESP request byte count differs")
    progress = {}
    for peer in sorted(peers):
        seq = []
        for sample in samples:
            w = sample["followers"][peer]
            if sample["db"] == 0 and start <= w["start_ns"] <= w["end_ns"] <= end:
                status = w["value"]
                require(status["ready"] and not status["last_error"] and
                        status["source"] == case["terminal"]["follower_current"][peer]["0"]["source"],
                        "follower source or state differs")
                seq.append(status["sequence"])
        require(seq == sorted(seq), "nonmonotone follower sequence")
        progress[peer] = {"samples": len(seq), "first": seq[0] if seq else None,
                          "last": seq[-1] if seq else None, "advanced": len(seq) >= 2 and seq[-1] > seq[0]}
    detail = None
    if arm == "E":
        require(recorded["detailed_sha256"] == digest(folder / "live-observer.json"), "full observer digest differs")
        verify_window(folder)
        detail = replication_observations(folder)
    else:
        require(not (folder / "live-observer.json").exists(), "unexpected full observation")
    counts = {k: v for k, v in recorded.items() if k not in ("file", "detailed_file", "passed", "anchor_ns")}
    return {"counts": counts, "markers_inside_offer": in_window, "db0_follower_progress": progress,
            "detailed_replication": detail}


def check_terminal(folder, case):
    end = case["terminal"]
    peers = {"a", "b"} if case["arm"] in "CDE" else set()
    values = end["bench_get_samples_hex"]
    require(set(values) == {"primary", *peers}, "terminal sample nodes differ")
    expected = {f"kerniva:thesis:{i * 99999 // 31}": i * 99999 // 31 for i in range(32)}
    require(set(values["primary"]) == set(expected), "incorrect terminal sample keys")
    for key, number in expected.items():
        raw = bytes.fromhex(values["primary"][key])
        require(len(raw) == 32 and int.from_bytes(raw[:8], "little") == number, "wrong primary sample")
    for peer in peers:
        require(values[peer] == values["primary"], "wrong follower values")
    sources = set()
    for db in ("0", "1"):
        final = end["primary"][db]
        require(final["seed"] == f"live-load-db{db}", "wrong terminal database seed")
        for peer in peers:
            cursor, status = end["follower_current"][peer][db], end["follower_status"][peer][db]
            require(read_json(folder / "terminal-evidence" / peer / f"db_{db}" / "CURRENT") == cursor,
                    "archived CURRENT differs from driver readback")
            source = end["source"][db]["manifest"]
            require(cursor["source"] == status["source"] == source["source"] and status["ready"]
                    and not status["last_error"] and status["snapshot"] == cursor["snapshot"]
                    and source["last"] == source["after"] == final["sequence"] == cursor["sequence"] == status["sequence"]
                    and source["db"] == int(db) and 0 <= cursor["snapshot"] <= cursor["sequence"],
                    "terminal source, follower or durable cursor differs")
            sources.add(cursor["source"])
    require(len(sources) == (1 if peers else 0), "database source identities disagree")
    return {"dbs": end["primary"], "source": next(iter(sources), None),
            "follower_current": end.get("follower_current", {}), "keys_per_node": {k: len(v) for k, v in values.items()}}


def replay_case(folder, duration, warmup, rate, arm, bounds):
    window = read_window(folder)
    case = read_json(folder / "case.json")
    cfg = window["config"]
    expected = {"network": "gnet", "durability": "sync", "seed": 42, "keys": 100000, "clients": 64, "value_bytes": 32,
                "target_rate": rate, "read_percent": 50 if rate == 3000 else 95, "duration_seconds": duration,
                "hot_percent": 0, "timeout_seconds": 2}
    require(all(cfg[k] == v for k, v in expected.items()) and case["arm"] == arm and
            case["client_argv"][case["client_argv"].index("-warmup") + 1] == f"{warmup}s", "workload differs")
    require(case["status"] == "completed" and case["client_exit_code"] == 0, "driver or client failed")
    server = read_json(folder / "config.json")
    controls = {"network_type": "gnet", "db_count": 2, "storage_backend": "go-log", "persistence_enabled": True,
                "persistence_write_mode": "strong", "persistence_durability": "sync", "go_log_group_max": 8,
                "go_log_segment_age_seconds": 1, "engine_segment_size": 4194304, "port": case["ports"]["primary"]}
    require(all(server[k] == v for k, v in controls.items()), "server configuration changed")
    require(case["components"] == {"maintenance": arm in "BDE", "replication": arm in "CDE",
                                   "detailed_observer": arm == "E"}, "wrong arm flags")
    children = {"primary"} | ({"maintainer"} if arm in "BDE" else set()) | ({"a", "b", "source"} if arm in "CDE" else set())
    codes = case["process_exit_codes"]
    require(set(codes) == children and all(v == 0 or (k == "maintainer" and v == -15) for k, v in codes.items()),
            "wrong process topology or exit codes")
    align = case["measurement_alignment"]
    utc, start, end = offer_interval(case, read_json(folder / "client.json"))
    require(bounds[0] <= utc and utc + timedelta(seconds=duration) <= bounds[1], "offer outside registered period")
    cuts = [s for s in case["rust"]["snapshot_intervals"] if start <= s["start_ns"] <= s["end_ns"] <= end
            and s["cutoff"] > s["previous_cutoff"]]
    require(cuts == align["snapshot_brackets_inside_projected_offer"], "cutoff witness mismatch")
    require(arm in "BDE" or not cuts, "unexpected maintenance")
    obs = samples_and_markers(folder, case, start, end, duration)
    window.update(terminal=check_terminal(folder, case), offer_start=utc.isoformat(), observed_cutoffs=cuts, observer=obs)
    window["scope_met"] = bool(align["projection_consistent"] and (arm not in "BDE" or cuts)
                               and all(v["advanced"] for v in obs["db0_follower_progress"].values())
                               and obs["markers_inside_offer"] == 6)
    hist = read_json(folder / "client-hist.json")
    window["success_tail_ranks_from_top"] = {
        op: {field: n - math.ceil(n * fraction) + 1 for field, fraction in TAILS.items()}
        for op, n in ((op, hist[op]["success"]["count"]) for op in ("set", "get"))}
    return window


def replay_phase(root, name, startup, order, registration_hash):
    folder = root / name
    ledger = read_json(folder / "run.json")
    ledger_sha = digest(folder / "run.json")
    mirror = read_json(folder / "ext4-readback.json")
    require(mirror["matched"] and mirror["ledger_sha256"] == ledger_sha, "ext4 and Windows ledgers differ")
    contract = ledger["contract"]
    require(contract["stage"] == name and contract["source_revision"] == startup["source_revision"] and
            contract["startup_sha256"] == digest(root / "run.json") and
            contract["preregistration_sha256"] == registration_hash, "unexpected phase identity")
    binaries = {Path(relative).name: sha for sha, relative in
                (line.split(maxsplit=1) for line in (root / "binaries.sha256").read_text().splitlines())}
    require(contract["binaries"] == binaries and contract["source_archive_sha256"] == digest(root / "source.tar"),
            "build snapshot differs from phase")
    scripts = {relative: sha for sha, relative in (line.split(maxsplit=1)
               for line in (root / "frozen" / "scripts.sha256").read_text().splitlines())}
    require(contract["scripts"] == scripts and all(digest(root / "frozen" / k) == v for k, v in scripts.items()),
            "driver snapshot differs from phase")
    if name.startswith("formal") or name == "recheck":
        approvals = [p for p in (root / "reviews").glob("*.json") if digest(p) == contract["approval_sha256"]]
        require(len(approvals) == 1, "phase approval missing or ambiguous")
        if name == "recheck":
            signed = read_json(approvals[0])["recheck"]
            require(order == ([signed["rate"]], [signed["arms"], signed["arms"][::-1]]),
                    "supplied recheck options differ from approval")
    profiles, rounds = order
    expected = [(rate, r, arm) for rate in profiles for r, arms in enumerate(rounds, 1) for arm in arms]
    slots = ledger["windows"]
    require(len(slots) == len(expected) and len(contract["plan"]) == len(slots), "phase layout differs")
    duration, warmup = (8, 3) if name.startswith("pilot") else (30, 5)
    require((contract["duration"], contract["warmup"]) == (duration, warmup), "registered timing differs")
    bounds = [datetime.fromisoformat(startup["stages"][name][key]) for key in ("opens", "closes")]
    windows, errors, attempts_seen, partial = [], [], [], []
    previous = None
    for position, ((rate, r, arm), slot, planned) in enumerate(zip(expected, slots, contract["plan"]), 1):
        label = f"{position:02d}-r{50 if rate == 3000 else 95}-q{rate}-{arm}"
        require(all(slot[k] == planned[k] for k in planned) and
                (slot["label"], slot["round"], slot["position"], slot["rate"], slot["arm"]) ==
                (label, r, position, rate, arm), f"{name}/{label}: wrong period, round or load order")
        attempts = slot["attempts"]
        attempts_seen.append({"case": label, "attempts": [{k: a.get(k) for k in
                              ("directory", "number", "state", "started_at", "finished_at")} for a in attempts]})
        try:
            require(attempts and attempts[-1]["state"] == "completed", "no completed attempt")
            require(all(a["state"] == "retryable_startup" for a in attempts[:-1]), "unreviewed duplicate attempt")
            attempt = attempts[-1]
            path = folder / attempt["directory"]
            require(path.resolve().is_relative_to(folder.resolve()) and path.name == label, "unexpected case path")
            for filename, expected_hash in attempt["files"].items():
                file = path / filename
                require(file.resolve().is_relative_to(path.resolve()) and digest(file) == expected_hash,
                        f"file hash differs: {filename}")
            window = replay_case(path, duration, warmup, rate, arm, bounds)
            began = datetime.fromisoformat(window["offer_start"])
            require(not previous or began >= previous, "reordered or overlapping offer windows")
            previous = began + timedelta(seconds=duration)
            window.update(phase=name, round=r, evidence=path.relative_to(root).as_posix())
            windows.append(window)
        except (OSError, ValueError, KeyError, IndexError, TypeError, StopIteration, ZeroDivisionError) as error:
            errors.append({"case": label, "reason": str(error)})
            for attempt in attempts:
                path = folder / attempt["directory"]
                if not (path / "client.json").is_file():
                    continue
                try:
                    # Include available client evidence even if the topology or terminal readback failed.
                    client = read_json(path / "client.json")
                    raw = {k: client.get(k) for k in ("offered", "sent", "succeeded", "failures",
                           "actual_send_rate", "actual_success_rate", "window_send_rate", "window_success_rate")}
                    row = {"evidence": path.relative_to(root).as_posix(), "client_reported": raw}
                    partial.append(row)
                    row["recomputed"] = read_window(path)
                except (OSError, ValueError, KeyError, TypeError, ZeroDivisionError):
                    continue
    return {"name": name, "expected": len(slots), "ledger_sha256": ledger_sha,
            "frozen": {k: contract[k] for k in ("binaries", "scripts", "source_archive_sha256")},
            "attempts": attempts_seen, "windows": windows, "errors": errors, "partial_client_windows": partial}


def difference(values, label):
    if label == "(D-C)-(B-A)":
        return (values["D"] - values["C"]) - (values["B"] - values["A"])
    a, b = label.split("-")
    return values[a] - values[b]


def threshold(arm_noise, label):
    if label == "(D-C)-(B-A)":
        return threshold(arm_noise, "D-C") + threshold(arm_noise, "B-A")
    a, b = label.split("-")
    return max(arm_noise[a], arm_noise[b])


def contrasts(windows):
    groups = {(w["phase"], w["config"]["target_rate"], w["round"], w["arm"]): w for w in windows}
    output = {}
    for rate in (3000, 6000):
        results = {}
        for op, field in ENDPOINTS:
            values = []
            for p in ("formal-1", "formal-2"):
                for r in (1, 2):
                    cells = {}
                    for a in "ABCDE":
                        w = groups.get((p, rate, r, a))
                        if w and w["scope_met"] and w["primary_load_met"]:
                            # Inputs are histogram upper bounds in integer nanoseconds.
                            cells[a] = round(1000 * w["get_set_tails"][f"{op}_success"][field])
                    values.append(cells)
            variations = {}
            for a in "ABCDE":
                if all(a in row for row in values):
                    variations[a] = [abs(values[i][a] - values[i + 1][a]) for i in (0, 2)]
            noise = {a: median(v) for a, v in variations.items()}
            effects = {}
            for label in CONTRASTS:
                operands = {c for c in label if c in "ABCDE"}
                deltas = [difference(row, label) if operands <= row.keys() else None for row in values]
                valid = [x for x in deltas if x is not None]
                n = threshold(noise, label) if operands <= noise.keys() else None
                center = median(valid) if valid else None
                same = len(valid) == 4 and (min(valid) > 0 or max(valid) < 0)
                effects[label] = {"per_round_us": [v / 1000 if v is not None else None for v in deltas],
                                  "valid_pairs": len(valid), "median_us": center / 1000 if valid else None,
                                  "min_us": min(valid) / 1000 if valid else None,
                                  "max_us": max(valid) / 1000 if valid else None,
                                  "threshold_us": n / 1000 if n is not None else None,
                                  "repeats_above_noise": bool(same and n is not None and abs(center) > n)}
            results[f"{op}_{field}"] = {
                "absolute_within_period_differences_us": {a: [n / 1000 for n in v] for a, v in variations.items()},
                "median_noise_by_arm_us": {a: n / 1000 for a, n in noise.items()}, "contrasts": effects}
        output[str(rate)] = results
    return output


def compare(root, stages, recheck_order=None):
    require(stages and len(stages) == len(set(stages)), "duplicate or empty stage selection")
    startup = read_json(root / "run.json")
    registration_hash = digest(root / "reviews" / "preregistration.json")
    phases = [replay_phase(root, name, startup, recheck_order if name == "recheck" else ORDERS[name],
                           registration_hash) for name in stages]
    frozen = phases[0]["frozen"]
    require(all(p["frozen"]["binaries"] == frozen["binaries"] and
                p["frozen"]["source_archive_sha256"] == frozen["source_archive_sha256"] for p in phases),
            "production binaries or source archive changed between stages")
    formal = [p for p in phases if p["name"].startswith("formal")]
    require(len({json.dumps(p["frozen"]["scripts"], sort_keys=True) for p in formal}) <= 1,
            "measurement code differs across formal periods")
    windows = [w for p in phases for w in p["windows"]]
    partial = [w["client_reported"] for p in phases for w in p["partial_client_windows"]]
    for rate in (3000, 6000):
        configs = {json.dumps(w["config"], sort_keys=True) for p in formal for w in p["windows"]
                   if w["config"]["target_rate"] == rate}
        require(len(configs) <= 1, "formal client settings differ")
    result = {"run_id": startup["run_id"], "phase_reports": phases, "client_windows_replayed": len(windows),
              "total_counts": {k: (sum(w["counts"][k] for w in windows) +
                                   sum((w[k] or 0) for w in partial)) for k in ("offered", "succeeded", "failures")},
              "counts_coverage": {"full_window_replays": len(windows), "additional_client_receipts": len(partial),
                                  "counts_with_missing_fields": {k: sum(w[k] is None for w in partial)
                                  for k in ("offered", "succeeded", "failures")}},
              "load_met": sum(w["primary_load_met"] for w in windows), "scope_met": sum(w["scope_met"] for w in windows)}
    if formal:
        result["formal_contrasts"] = contrasts(windows)
    if "recheck" in stages:
        phase = next(p for p in phases if p["name"] == "recheck")
        approval_sha = read_json(root / "recheck" / "run.json")["contract"]["approval_sha256"]
        approved = next(read_json(p)["recheck"] for p in (root / "reviews").glob("*.json") if digest(p) == approval_sha)
        label, rate, endpoint = approved["contrast"], approved["rate"], approved["endpoint"]
        op, field = endpoint.split("_", 1)
        require((op, field) in ENDPOINTS and len(label) == 3 and label[1] == "-" and
                set(label[::2]) == set(approved["arms"]), "invalid signed recheck target")
        noise, sign = approved["noise_us"], approved["direction"]
        require(noise >= 0 and sign in (-1, 1), "invalid signed recheck threshold")
        if len(formal) == 2:
            original = result["formal_contrasts"][str(rate)][endpoint]["contrasts"][label]
            require(original["threshold_us"] == noise, "approved threshold differs from original noise")
        deltas = []
        for r in (1, 2):
            values = {w["arm"]: round(1000 * w["get_set_tails"][f"{op}_success"][field])
                      for w in phase["windows"] if w["round"] == r and w["scope_met"] and w["primary_load_met"]}
            deltas.append(difference(values, label) if set(approved["arms"]) <= values.keys() else None)
        passed = all(v is not None and v * sign > 0 for v in deltas) and abs(median(deltas)) > round(2000 * noise) / 2
        result["recheck_effect"] = {"target": approved, "per_round_us": [v / 1000 if v is not None else None
                                                                        for v in deltas], "verified": passed}
    if set(stages) >= {"pilot", "pilot-noise"}:
        pilot = {(w["phase"], w["config"]["target_rate"], w["arm"]): w for w in windows}
        result["short_repeat_signed_us"] = {}
        for rate in (3000, 6000):
            for arm in "AE":
                if all((p, rate, arm) in pilot for p in ("pilot", "pilot-noise")):
                    a, b = [pilot[p, rate, arm]["get_set_tails"] for p in ("pilot", "pilot-noise")]
                    result["short_repeat_signed_us"][f"{rate}-{arm}"] = {
                        f"{op}_{field}": b[f"{op}_success"][field] - a[f"{op}_success"][field] for op, field in ENDPOINTS}
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("--stages", choices=(*ORDERS, "recheck"), nargs="+", default=["formal-1", "formal-2"])
    parser.add_argument("--recheck-rate", type=int, choices=(3000, 6000))
    parser.add_argument("--recheck-arms", help="two signed-off physical arm letters in execution order")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    repeat = None
    if "recheck" in args.stages:
        arms = args.recheck_arms or ""
        if args.recheck_rate is None or len(arms) != 2 or len(set(arms) & set("ABCDE")) != 2:
            parser.error("recheck needs the signed-off rate and two different physical arms")
        repeat = ([args.recheck_rate], [arms, arms[::-1]])
    report = compare(args.root, args.stages, repeat)
    args.output.write_bytes((json.dumps(report, indent=2, allow_nan=False) + "\n").encode("utf-8"))
    return int(any(p["errors"] for p in report["phase_reports"]))


if __name__ == "__main__":
    raise SystemExit(main())
