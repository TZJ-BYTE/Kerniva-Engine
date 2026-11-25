#!/usr/bin/env python3
"""Audit direct-delta blocks against the frozen fixture and recompute paired timings."""

import argparse
import hashlib
import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import median

FIXTURE_SHA = "68aa8aa837030b5782e87bf845a17109d7d7e8f5b8461f6d33d861cb382a7cd0"
ORDER = ("baseline", "candidate", "candidate", "baseline", "candidate", "baseline", "baseline", "candidate")
INSTALL_PHASES = ("generation_prepare", "segment_transfer", "response_end", "view_prepare", "generation_dir_sync",
                  "database_dir_sync", "pin_source", "current_temp_prepare", "current_rename", "current_final_dir_sync")
BOUNDARIES = ("pull_request", "manifest_validated", "current_durable", "view_published")
SLOW_NS = 2_000_000_000
MS = 1_000_000


def read(path, checksum=None):
    raw = path.read_bytes()
    if checksum and hashlib.sha256(raw).hexdigest() != checksum:
        raise ValueError(f"SHA-256 mismatch: {path}")
    return raw


def require(value, path, message):
    if not value:
        raise ValueError(f"{path}: {message}")


def trace_timings(path, result, manifest):
    events = [json.loads(line) for line in read(path, result["trace_sha256"]).splitlines()]
    require(events[-1]["stage"] == "trace_closed" and events[-1]["dropped"] == 0, path, "trace loss")
    require(not any(e["stage"] == "poll_error" for e in events), path, "poll failure")
    rows = [e for e in events if e["db"] == 15]
    start, = [i for i, e in enumerate(rows) if e["stage"] == "pull_request" and e["after"] == manifest["after"]]
    stop = next(i for i in range(start, len(rows)) if rows[i]["stage"] == "view_published")
    install = rows[start:stop + 1]
    boundary = [e for e in install if e["stage"] in BOUNDARIES]
    require(tuple(e["stage"] for e in boundary) == BOUNDARIES, path, "install boundary order")
    require(all(e.get("request_id") == boundary[0].get("request_id") for e in install), path, "mixed requests")
    require(all(e["last"] == manifest["last"] for e in boundary[1:]), path, "wrong target cursor")
    request, checked, durable, view = [e["monotonic_ns"] for e in boundary]
    require(request <= checked < durable <= view, path, "monotonic install timeline")
    spans = defaultdict(int)
    for e in install:
        end = e["monotonic_ns"]
        if not checked <= end <= durable:
            continue
        if "start_ns" in e:
            require(checked <= e["start_ns"] <= end, path, "span crosses install boundary")
            spans[e["stage"]] += end - e["start_ns"]
        elif "duration_ns" in e:
            require(0 <= e["duration_ns"] <= durable - checked, path, "invalid aggregate")
            spans[e["stage"]] += e["duration_ns"]
    top = [e for e in install if e["stage"] in INSTALL_PHASES]
    finish = checked
    for e in sorted(top, key=lambda row: row["start_ns"]):
        require(finish <= e["start_ns"] <= e["monotonic_ns"] <= durable, path, "overlapping top-level spans")
        finish = e["monotonic_ns"]
    measured = sum(spans[stage] for stage in {e["stage"] for e in top})
    return {"install_ns": durable - checked, "request_durable_ns": durable - request,
            "request_view_ns": view - request, "stages_ns": dict(spans),
            "unmeasured_install_ns": durable - checked - measured}


def external_timings(directory, external):
    path = directory / "b-external.jsonl"
    require(external["raw_file"] == path.name, path, "unexpected external evidence path")
    rows = [json.loads(line) for line in read(path, external["raw_sha256"]).splitlines()]
    source, = [r for r in rows if r["event"] == "source_verified_available"]
    spawn, = [r for r in rows if r["event"] == "follower_start"]
    require(all(spawn[k] == v for k, v in external["follower_start"].items()), path, "launch witness changed")
    require(source["end_ns"] <= spawn["start_ns"] <= spawn["end_ns"], path, "source/start order")
    result = {}
    for name in ("current", "get"):
        witnesses = [r for r in rows if r["event"] == name]
        positive, = [r for r in witnesses if r["positive"] is True]
        negatives = [r for r in witnesses if r["positive"] is False]
        negative_start = negatives[-1]["start_ns"] if negatives else spawn["start_ns"]
        require(witnesses[-1] == positive and negative_start <= positive["end_ns"], path, "witness order")
        bounds = [max(0, negative_start - spawn["end_ns"]) / MS,
                  (positive["end_ns"] - spawn["start_ns"]) / MS]
        key = f"{name}_visible_after_start_ms"
        require(bounds == external[key], path, f"{key} differs from external observations")
        result[key] = bounds
    processes = [r for r in rows if r["event"] == "process" and "error" not in r]
    host = {"valid_process_samples": len(processes),
            "process_sample_errors": sum(r["event"] == "process" and "error" in r for r in rows)}
    if processes:
        first, last = processes[0], processes[-1]
        require(all((r["pid"], r["process_start_ticks"]) == (first["pid"], first["process_start_ticks"])
                    for r in processes), path, "sampled process changed")
        gaps = [(b["start_ns"] - a["start_ns"]) / MS for a, b in zip(processes, processes[1:])]
        ticks = {key: last[key] - first[key] for key in ("cpu_user_ticks", "cpu_system_ticks")}
        host.update(max_valid_sample_gap_ms=max(gaps, default=None),
                    observed_ms=(last["end_ns"] - first["start_ns"]) / MS,
                    cpu_ms=sum(ticks.values()) * 1000 / external["cpu_ticks_per_second"],
                    io_bytes={key: last["io"][key] - first["io"][key] for key in ("read_bytes", "write_bytes")})
    result["host_samples"] = host
    return result


def expected_reads():
    values = {}
    for i in range(256):
        key = f"reader:delta:{i:04d}"
        part = hashlib.sha256(key.encode()).hexdigest().encode()
        values[key] = hashlib.sha256(part * (262144 // len(part))).hexdigest()
    return values


def load_window(directory, block, index, arm, meta, fixture, expected):
    label = f"{index:02d}-{arm}"
    require(meta["file"] == f"{label}/result.json", directory, "wrong window order or duplicate")
    local = directory / meta["file"]
    result = json.loads(read(local, meta["sha256"]))
    require(result["passed"] and result["label"] == label and result["verified_files"] == 18
            and result["verified_keys"] == 256 and result["payload_bytes"] == 67_126_416
            and result["current"]["source"] == fixture["manifest"]["source"]
            and result["current"]["sequence"] == 260, local, "invalid correctness evidence")
    row = {"window": label, "arm": arm, "binary_sha256": block["sha256"][result["binary"]],
           "mode": result.get("mode", "coarse"), "result_sha256": meta["sha256"]}
    if "installed_files" in result:
        require(result["installed_files"] == fixture["files"] and result["read_sha256"] == expected,
                local, "installed files or read values differ from fixture")
    if log_sha := result.get("process", {}).get("log_sha256"):
        read(directory / f"{label}.log", log_sha)
    if result.get("trace_sha256"):
        row.update(trace_timings(local.with_name("b-phase.jsonl"), result, fixture["manifest"]))
    if "external" in result:
        row.update(external_timings(local.parent, result["external"]))
    require("install_ns" in row or "get_visible_after_start_ms" in row, local, "no elapsed evidence")
    return row


def load_block(period, name, purpose, directory, fixture, expected=None):
    path = directory / "result.json"
    raw = read(path)
    block = json.loads(raw)
    order = tuple(block["order"])
    require(block["passed"] and order in (ORDER, ORDER[4:] + ORDER[:4]) and len(block["windows"]) == 8,
            path, "expected one passing ABBA BAAB block")
    require(all(block[k] == fixture[k] for k in ("db", "keys", "value_bytes", "before", "manifest", "files")),
            path, "frozen fixture differs")
    output = [load_window(directory, block, index, arm, meta, fixture, expected)
              for index, (arm, meta) in enumerate(zip(order, block["windows"]), start=1)]
    return {"block": name, "period": period, "purpose": purpose, "sha256": hashlib.sha256(raw).hexdigest(),
            "windows": output, "valid_windows": len(output),
            "timed_install_windows": sum("install_ns" in w for w in output),
            "slow_installs": sum(w.get("install_ns", 0) >= SLOW_NS for w in output)}


def install_distribution(rows):
    durations = [w["install_ns"] / MS for w in rows if "install_ns" in w]
    if not durations:
        return None
    return {"min_ms": min(durations), "median_ms": median(durations), "max_ms": max(durations),
            "slow": sum(t >= SLOW_NS / MS for t in durations), "count": len(durations)}


def attempts(ledger):
    output = {}
    for block_id, block in ledger["blocks"].items():
        output[block_id] = [
            {key: a[key] for key in ("number", "status", "reason", "exit_code", "started_at",
                                      "finished_at", "result_sha256", "time_gaps") if key in a}
            for a in block["attempts"]]
    return output


def check_attempt_results(directory, attempt, spec, require_follower_log):
    path = directory / "result.json"
    hashes = attempt["result_hashes"]
    require(hashes.get("result.json") == attempt["result_sha256"], path, "ledger block hash mismatch")
    for name, checksum in hashes.items():
        artifact = (directory / name).resolve()
        require(artifact.is_relative_to(directory.resolve()), path, "result path escapes attempt")
        read(artifact, checksum)
    block = json.loads(read(path, attempt["result_sha256"]))
    for meta in block["windows"]:
        name = meta["file"]
        label = Path(name).parent.as_posix()
        require(hashes.get(name) == meta["sha256"], path, "window not covered by ledger")
        window = json.loads(read(directory / name, meta["sha256"]))
        for evidence, definition in spec.get("evidence", {}).items():
            checksum = window
            for key in definition["field"].split("."):
                checksum = checksum.get(key) if isinstance(checksum, dict) else None
            artifact = f"{label}/{evidence}"
            require(checksum or not definition.get("required"), path, f"missing declared evidence: {artifact}")
            if checksum:
                require(hashes.get(artifact) == checksum, path, f"{artifact} not covered by ledger")
        log = f"{label}.log"
        log_checksum = window.get("process", {}).get("log_sha256")
        if require_follower_log or log in hashes:
            require(log_checksum and hashes.get(log) == log_checksum, path, f"{log} not covered by ledger")


def partial_attempts(root, ledger, statuses, fixture, expected):
    observations = []
    for block, period in statuses.items():
        for attempt in ledger["blocks"].get(block, {}).get("attempts", []):
            if attempt["status"] == "passed":
                continue
            number = attempt["number"]
            attempt_dir = root / "blocks" / block / f"attempt-{number:03d}"
            directory = attempt_dir / "result"
            entry = {"block": block, "period": period["slot"], "attempt": number,
                     "status": attempt["status"], "reason": attempt.get("reason"),
                     "exit_code": attempt.get("exit_code"),
                     "verified_windows": [], "unanchored_windows": [], "errors": []}
            snapshots = [(attempt_dir / "windows" / file, checksum)
                         for file, checksum in sorted(attempt.get("window_checkpoints", {}).items())]
            if checksum := attempt.get("result_sha256"):
                snapshots.append((directory / "result.json", checksum))
            seen = {}
            for path, checksum in snapshots:
                try:
                    data = json.loads(read(path, checksum))
                    order = tuple(data["order"])
                    require(order in (ORDER, ORDER[4:] + ORDER[:4]) and
                            len(data["windows"]) <= len(order), path, "invalid partial order")
                    require(all(data[k] == fixture[k] for k in
                                ("db", "keys", "value_bytes", "before", "manifest", "files")),
                            path, "frozen fixture differs")
                    for index, meta in enumerate(data["windows"], start=1):
                        name = meta["file"]
                        if name in seen:
                            require(seen[name] == meta["sha256"], path, "window hash changed between checkpoints")
                            continue
                        row = load_window(directory, data, index, order[index - 1], meta, fixture, expected)
                        seen[name] = meta["sha256"]
                        entry["verified_windows"].append(row)
                except (OSError, ValueError, KeyError, IndexError, StopIteration) as error:
                    entry["errors"].append(str(error))
            for path in sorted(directory.glob("*/result.json")):
                if path.relative_to(directory).as_posix() in seen:
                    continue
                try:
                    result = json.loads(read(path))
                    if result.get("passed") is True:
                        entry["unanchored_windows"].append(path.parent.name)
                except (OSError, ValueError) as error:
                    entry["errors"].append(str(error))
            entry["unanchored_window_count"] = len(entry["unanchored_windows"])
            entry["verified_timed_windows"] = sum("install_ns" in row for row in entry["verified_windows"])
            entry["slow_verified_windows"] = sum(row.get("install_ns", 0) >= SLOW_NS
                                                  for row in entry["verified_windows"])
            observations.append(entry)
    return observations


def pairs(block):
    rows = block["windows"]
    result = {}
    for key in ("install_ns", "request_durable_ns", "request_view_ns"):
        if not all(key in w for w in rows):
            continue
        ratios = []
        for i in range(0, len(rows), 2):
            a, b = sorted(rows[i:i + 2], key=lambda w: w["arm"])
            ratios.append(b[key] / a[key] if key == "install_ns" else a[key] / b[key])
        middle = median(ratios)
        summary = {"ratios": ratios, "median_ratio": middle, "range": max(ratios) - min(ratios)}
        if key == "install_ns":
            summary["definition"] = "candidate / baseline elapsed"
            summary["median_change_percent"] = 100 * (middle - 1)
            if block["purpose"] == "calibration":
                summary["within_five_percent"] = abs(middle - 1) <= 0.05
        else:
            summary["definition"] = "candidate / baseline throughput, equal payload"
            if block["purpose"] == "candidate":
                summary["passes_regression_screen"] = middle >= 0.95 and sum(r < 0.90 for r in ratios) < 2
                summary["repeat_required"] = not summary["passes_regression_screen"] or summary["range"] > 0.20
        result[key] = summary
    for key in ("get_visible_after_start_ms", "current_visible_after_start_ms"):
        if not all(key in w for w in rows):
            continue
        differences = []
        for i in range(0, len(rows), 2):
            a, b = sorted(rows[i:i + 2], key=lambda w: w["arm"])
            differences.append([b[key][0] - a[key][1], b[key][1] - a[key][0]])
        result[key] = {"candidate_minus_baseline_ms": differences,
                       "median_bounds_ms": [median(pair[j] for pair in differences) for j in (0, 1)]}
    return result


def attribution(blocks):
    normal = defaultdict(list)
    for b in blocks:
        if b["purpose"] == "formal":
            for w in b["windows"]:
                if w.get("install_ns", SLOW_NS) < SLOW_NS:
                    normal[b["period"], w["binary_sha256"], w["mode"]].append(w)
    slow = []
    votes = defaultdict(set)
    for b in blocks:
        if b["purpose"] != "formal":
            continue
        for w in b["windows"]:
            if w.get("install_ns", 0) < SLOW_NS:
                continue
            peers = normal[b["period"], w["binary_sha256"], w["mode"]]
            result = {"block": b["block"], "window": w["window"], "normal_count": len(peers),
                      "install_ms": w["install_ns"] / MS, "stages": {}}
            slow.append(result)
            if not peers:
                continue
            reference = median(p["install_ns"] for p in peers)
            extra = w["install_ns"] - reference
            result.update(normal_median_ms=reference / MS, extra_ms=extra / MS)
            for stage, elapsed in w["stages_ns"].items():
                if not all(stage in p["stages_ns"] for p in peers):
                    continue
                delta = elapsed - median(p["stages_ns"][stage] for p in peers)
                result["stages"][stage] = {"extra_ms": delta / MS, "share": delta / extra}
                if delta >= extra / 2:
                    votes[w["binary_sha256"], w["mode"], stage].add(b["block"])
    return {"slow_windows": slow, "stages_above_half_in_two_blocks": [
        {"binary_sha256": binary, "mode": mode, "stage": stage, "blocks": sorted(names)}
        for (binary, mode, stage), names in sorted(votes.items()) if len(names) >= 2]}


def scheduled_blocks(root):
    control = root / "control"
    path = control / "attribution-queue.json"
    queue = json.loads(read(path))
    state = json.loads(read(control / "attribution-queue-state.json"))
    read(path, state["queue_sha256"])
    ledger = json.loads(read(root / "ledger.json"))
    run = json.loads(read(root / "run.json", ledger["run_sha256"]))
    start = datetime.fromisoformat(run["created_at"])
    specs, statuses = [], {}
    slots = queue["slots"]
    require(len(slots) == 3, path, "three preregistered periods required")
    for index, slot in enumerate(slots):
        first, last = (datetime.fromisoformat(slot[k]) for k in ("not_before", "not_after"))
        require(first == start + timedelta(hours=5 + index * 2) and first < last and
                last <= start + timedelta(hours=7 + index * 2) and len(slot["blocks"]) == 2,
                path, "period no longer matches the registered design")
        for item in slot["blocks"]:
            filename = item["file"]
            require(Path(filename).name == filename, path, "unexpected spec path")
            spec = json.loads(read(control / filename, item["sha256"]))
            name = spec["id"]
            require(filename == name + ".json" and name not in statuses and spec["expected_windows"] == 8,
                    path, "unexpected or repeated formal block")
            recorded = ledger["blocks"].get(name)
            entry = state["slots"].get(slot["id"], {}).get("blocks", {}).get(name, {})
            status = statuses[name] = {"slot": slot["id"], "queue_status": entry.get("status", "pending"),
                                       "attempts": [], "review": []}
            if not recorded:
                continue
            require(recorded["spec_sha256"] == item["sha256"], path, "ledger/spec mismatch")
            status["attempts"] = [a["status"] for a in recorded["attempts"]]
            if status["attempts"] != ["passed"]:
                status["review"].append("incomplete or repeated attempts")
            completed = [a for a in recorded["attempts"] if a["status"] == "passed"]
            if not completed:
                continue
            a = completed[-1]
            require(a["exit_code"] == 0, path, "passing attempt has a nonzero exit")
            begin, end = (datetime.fromisoformat(a[k]) for k in ("started_at", "finished_at"))
            if not first <= begin <= end <= last or a.get("time_gaps"):
                status["review"].append("time discontinuity or outside allocated period")
            folder = root / "blocks" / name / f"attempt-{a['number']:03d}" / "result"
            specs.append((slot["id"], name, "formal", folder))
    return specs, statuses, ledger, start


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--fixture", type=Path, required=True, help="archived direct-1/result.json")
    p.add_argument("--block", action="append", nargs=4, default=[],
                   metavar=("PERIOD", "NAME", "PURPOSE", "DIR"),
                   help="purpose: reference, repeatability, calibration, formal, candidate, or external")
    p.add_argument("--schedule", type=Path, help="run root containing preregistered six-block queue and ledger")
    p.add_argument("--ledger", type=Path, help="include failed and interrupted attempts with timestamps")
    p.add_argument("--output", type=Path)
    args = p.parse_args()
    fixture = json.loads(read(args.fixture, FIXTURE_SHA))
    if not args.block and not args.schedule:
        p.error("specify at least one --block or a --schedule run root")
    if args.schedule and args.ledger:
        p.error("--schedule reads its own ledger")
    blocks = []
    expected = expected_reads()
    specs = args.block.copy()
    ledger = json.loads(read(args.ledger)) if args.ledger else None
    if args.schedule:
        formal, statuses, ledger, started = scheduled_blocks(args.schedule)
        specs.extend(formal)
    for period, name, purpose, directory in specs:
        if purpose not in {"reference", "repeatability", "calibration", "formal", "candidate", "external"}:
            p.error(f"unknown block purpose {purpose}")
        b = load_block(period, name, purpose, Path(directory), fixture, expected)
        require(not any(other["block"] == name or other["sha256"] == b["sha256"] for other in blocks),
                directory, "block counted twice")
        b["install_distribution"] = install_distribution(b["windows"])
        b["pairs"] = pairs(b)
        if ledger is not None and b["purpose"] != "reference":
            record = ledger["blocks"].get(name, {})
            for attempt in record.get("attempts", []):
                if attempt["status"] == "passed" and attempt.get("result_sha256") == b["sha256"]:
                    control = (args.schedule or args.ledger.parent) / "control"
                    spec_path = control / f"{name}.json"
                    if not spec_path.is_file() and record.get("spec_file"):
                        spec_path = Path(record["spec_file"])
                    spec = json.loads(read(spec_path, record["spec_sha256"]))
                    check_attempt_results(Path(directory), attempt, spec, purpose == "formal")
                    break
        blocks.append(b)
    report = {"fixture_sha256": FIXTURE_SHA, "slow_threshold_ms": 2000,
              "blocks": blocks, "attribution": attribution(blocks)}
    if args.schedule:
        formal = [b for b in blocks if b["block"] in statuses]
        windows = [w for b in formal for w in b["windows"]]
        same_binary = len({w["binary_sha256"] for w in windows}) == 1
        audit_ok = (len(formal) == 6 and len(windows) == 48 and same_binary and
                    all(w["mode"] == "fine" and w.get("stages_ns") and "host_samples" in w for w in windows) and
                    all(not status["review"] and status["queue_status"] in {"passed", "skipped"} for status in statuses.values()))
        gate = started + timedelta(hours=11)
        slow_count = sum(w.get("install_ns", 0) >= SLOW_NS for w in windows)
        report["partial_formal_attempts"] = partial_attempts(args.schedule, ledger, statuses, fixture, expected)
        report["schedule"] = {"blocks": statuses, "all_formal_evidence_valid": audit_ok,
                              "valid_complete_windows": len(windows), "slow_in_complete_windows": slow_count,
                              "earliest_supplement_decision": gate.isoformat(),
                              "eligible_for_no_slow_supplement": audit_ok and not slow_count
                              and datetime.now(timezone.utc) >= gate}
    if ledger is not None:
        report["attempts"] = attempts(ledger)
        for b in blocks:
            if b["purpose"] != "reference":
                recorded = report["attempts"].get(b["block"], [])
                require(any(a["status"] == "passed" and a.get("result_sha256") == b["sha256"]
                            for a in recorded), args.ledger or args.schedule, f"missing matching passed attempt for {b['block']}")
    text = json.dumps(report, indent=2, allow_nan=False)
    if args.output:
        args.output.write_text(text + "\n", encoding="utf-8")
    else:
        print(text)


if __name__ == "__main__":
    main()
