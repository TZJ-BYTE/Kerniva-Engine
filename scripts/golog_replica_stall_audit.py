"""Replay all formal and recheck planned seconds directly from the sealed SET experiment."""

import argparse
from collections import Counter, defaultdict
from hashlib import sha256
from io import BytesIO
import json
from pathlib import Path
from statistics import median
import sys
from tempfile import TemporaryDirectory
from zipfile import Path as ZipPath, ZipFile


ARCHIVE_SHA256 = "09b4fea4a37acc1241a0a9a3cdc713863587aec833228bfb1aed6d26e588d3f8"
STAGES = {"formal-1": 20, "formal-2": 20, "recheck": 4}
OPS = ("get", "set")
HISTOGRAMS = {"success": "success_latency", "observed": "observed_latency", "client_queue": "client_queue_delay"}
SECOND = 10**9
THRESHOLD_US = 100_000
PINNED_ANALYZER = "analyzer/72116ac5ff6b7baf577635016a1e83863163b94e994d9e323c9566b42c49a650/"


def require(ok, message):
    if not ok:
        raise ValueError(message)


def digest(data):
    return sha256(data).hexdigest()


def read_json(path):
    return json.loads(path.read_bytes())


def save_json(path, value):
    path.write_bytes((json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + "\n").encode())


def save_rows(path, rows):
    with path.open("w", encoding="utf-8", newline="\n") as file:
        for row in rows:
            file.write(json.dumps(row, ensure_ascii=False, separators=(",", ":"), allow_nan=False) + "\n")


def verify_manifest(archive):
    names = archive.namelist()
    sums = {}
    for line in archive.read("SHA256SUMS").decode().splitlines():
        expected, name = line.split("  ", 1)
        require(name not in sums and digest(archive.read(name)) == expected, f"manifest mismatch: {name}")
        sums[name] = expected
    require(len(names) == len(set(names)) and set(names) == set(sums) | {"SHA256SUMS"}, "archive coverage")
    return sums


def marker_clock_overlap(start, event, second):
    """Half-open planned bin with an uncertain start, closed event bracket."""
    lo, hi = start
    left, right = event
    require(lo <= hi and left <= right, "inverted clock bracket")
    offset = second * SECOND
    if left >= hi + offset and right < lo + offset + SECOND:
        return "inside"
    if left < hi + offset + SECOND and right >= lo + offset:
        return "boundary"
    return None


def select_controls(trigger, rows_by_window):
    """Match in temporal order only. A missing side in a partially normal window stays missing."""
    second = trigger["second"]
    same = [r for r in rows_by_window[trigger["window"]] if not r["triggered"]]
    earlier = [r for r in same if r["second"] < second]
    later = [r for r in same if r["second"] > second]
    before = max(earlier, key=lambda r: r["second"], default=None)
    after = min(later, key=lambda r: r["second"], default=None)
    fallback = None
    if not same:
        candidates = [r for window, group in rows_by_window.items() if window != trigger["window"]
                      for r in group if not r["triggered"] and
                      all(r[k] == trigger[k] for k in ("stage", "rate", "arm", "read_percent"))]
        def distance(r):
            return (abs(r["window_start_lo_ns"] - trigger["window_start_lo_ns"]), r["window_start_lo_ns"],
                    abs(r["second"] - second), r["second"], r["window"])
        fallback = min(candidates, key=distance, default=None)
    return {k: v["id"] if v else None for k, v in (("before", before), ("after", after), ("fallback", fallback))}


def operation_values(raw):
    n = raw["offered"]
    require(n > 0 and n == raw["sent"] == raw["succeeded"] and raw["failures"] == 0, "operation denominator")
    sums = {}
    for kind, name in HISTOGRAMS.items():
        h = raw[name]
        require(h["count"] == n and 0 <= h["mean_us"] <= h["max_us"] and
                0 <= h["p50_us"] <= h["p95_us"] <= h["p99_us"] <= h["p99_9_us"], "operation histogram")
        sums[kind] = round(h["mean_us"] * n * 1000)
    require(sums["success"] == sums["observed"] >= sums["client_queue"], "incompatible operation time")
    return {**raw, "sum_ns": sums}


def summarize(rows):
    result = {"seconds": len(rows), "windows": len({r["window"] for r in rows})}
    for op in OPS:
        records = [r[op] for r in rows]
        success = sum(r["sum_ns"]["success"] for r in records)
        queue = sum(r["sum_ns"]["client_queue"] for r in records)
        result[op] = {
            "offered": sum(r["offered"] for r in records), "succeeded": sum(r["succeeded"] for r in records),
            "failures": sum(r["failures"] for r in records), "total_success_elapsed_ns": success,
            "total_client_queue_ns": queue, "total_elapsed_after_queue_ns": success - queue,
            "fraction_of_elapsed_in_client_queue": queue / success if success else None,
            "seconds_queue_p99_ge_100ms": sum(r["client_queue_delay"]["p99_us"] >= THRESHOLD_US for r in records),
        }
        for field, prefix in (("success_latency", "success"), ("client_queue_delay", "queue")):
            for tail in ("p99_us", "p99_9_us"):
                values = [r[field][tail] for r in records]
                result[op][f"{prefix}_{tail}_across_seconds_min_median_max"] = (
                    [min(values), median(values), max(values)] if values else None)
    return result


def seconds_for(window, slot, start, client, histogram):
    rows = []
    for measured in client["windows"]:
        n = measured["planned_second"]
        row = {"id": f"{window}@{n:02}", "window": window, "stage": window.split("/")[0],
               "arm": slot["arm"], "rate": slot["rate"], "read_percent": slot["read_percent"],
               "window_start_lo_ns": start[0], "second": n, "start_bounds_ns": [t + n*SECOND for t in start],
               "end_bounds_ns": [t + (n+1)*SECOND for t in start],
               "triggered": measured["set"]["success_latency"]["p99_us"] >= THRESHOLD_US,
               "initial_server_wait_observed": None,
               "event_counts": {"inside": {}, "boundary": {}}}
        row.update({op: operation_values(measured[op]) for op in OPS})
        require(sum(row[op]["offered"] for op in OPS) == slot["rate"], "per-second offered population")
        rows.append(row)
    require([r["second"] for r in rows] == list(range(30)), f"{window}: incomplete seconds")
    for op in OPS:
        for kind in HISTOGRAMS:
            raw = histogram[op][kind]
            count = sum(r[op][HISTOGRAMS[kind]]["count"] for r in rows)
            total = sum(r[op]["sum_ns"][kind] for r in rows)
            require((count, total) == (raw["count"], raw["sum_ns"]), f"{window} {op}/{kind}: raw sum/count mismatch")
    return rows


def observations(case, common, full):
    arm = case["arm"]
    peers = {"a", "b"} if arm in "CDE" else set()
    events = []
    def add(kind, db, bounds, **values):
        lo, hi = bounds
        require(lo is not None and lo <= hi and db in (0, 1), f"{kind}: bad bounds/db")
        events.append({"kind": kind, "db": db, "time_ns": [lo, hi], **values})

    def span(record):
        return record["start_ns"], record["end_ns"]

    for snap in case["rust"]["snapshot_intervals"]:
        require(arm in "BDE" and snap["previous_cutoff"] < snap["cutoff"], "unexpected snapshot progress")
        add("snapshot_cutoff_published", snap["db"], span(snap), previous=snap["previous_cutoff"],
            current=snap["cutoff"], snapshot_bytes=snap["bytes"])
    prev = {}
    for sample in common["primary_and_status_samples"]:
        db, primary = sample["db"], sample["primary"]
        add("primary_position_sample", db, span(primary), sequence=primary["value"])
        require(set(sample["followers"]) == peers, "unexpected peer sample topology")
        for peer, observation in sample["followers"].items():
            s = observation["value"]
            lineage = case["terminal"]["follower_current"][peer][str(db)]["source"]
            require(s["source"] == lineage and s["ready"] and not s["last_error"], "follower lineage/status")
            add("follower_status_sample", db, span(observation), peer=peer, source=lineage, sequence=s["sequence"],
                snapshot=s["snapshot"], installs=s["installs"], rebases=s["rebases"])
            last = prev.get((db, peer))
            if last:
                prior = last["value"]["sequence"]
                require(prior <= s["sequence"] and last["end_ns"] <= observation["start_ns"], "cursor regression")
                if prior < s["sequence"]:
                    add("follower_sequence_advanced", db, [last["start_ns"], observation["end_ns"]],
                        peer=peer, previous=prior, current=s["sequence"], source=lineage)
            prev[db, peer] = observation

    details = {(m["ordinal"], m["db"]): m for m in full["markers"]} if full else {}
    markers = common["markers"]
    require(len(markers) == 6 and {(m["ordinal"], m["db"]) for m in markers} ==
            {(i, db) for i in range(3) for db in range(2)}, "marker coverage")
    for m in markers:
        db, ordinal, key = m["db"], m["ordinal"], m["key"]
        value = "value:" + key
        require(key == f"live-observer:{ordinal}:db:{db}" and m["value"] == value and
                m["before_primary"]["value"] is None and m["set"]["value"] == "OK" and
                m["final_primary"]["value"] == value and set(m["final_replicas"]) == peers, "primary marker")
        add("marker_primary_set", db, span(m["set"]), key=key)
        add("marker_primary_final_read", db, span(m["final_primary"]), key=key, value=value)
        for peer in sorted(peers):
            positive = m["final_replicas"][peer]
            require(positive["value"] == {"status": 200, "body": value}, "final marker read/value")
            add("marker_follower_final_read", db, span(positive), key=key, peer=peer, value=value)
        if full:
            info = details[ordinal, db]
            seq = info["sequence"]
            if "source_available_ns" in info:
                add("marker_source_available", db, info["source_available_ns"], key=key, sequence=seq)
            for peer, probe in sorted(m["replicas"].items()):
                add("marker_follower_get_visible", db, [probe["last_negative"]["start_ns"], probe["first_positive"]["end_ns"]],
                    key=key, peer=peer, sequence=seq, source_to_get_ms=info["source_to_get_ms"].get(peer),
                    value=value, scope=info["scope"])
                current = info.get("disk_current_visible_ns", {}).get(peer)
                if current:
                    bounds = current["bounds"]
                    # Without a previous negative, only the positive read can be placed on the clock.
                    kind = "marker_disk_current_visible" if bounds[0] is not None else "marker_disk_current_positive_probe"
                    add(kind, db, bounds if bounds[0] is not None else span(current["first_positive"]),
                        key=key, peer=peer, sequence=seq, visibility_bounds_ns=bounds,
                        source_to_disk_current_ms=info.get("source_to_disk_current_ms", {}).get(peer))
    if full:
        for sample in full["sequence_lag_samples"]:
            add("follower_lag_sample", sample["db"], span(sample),
                status_lag_bounds=sample["sequence_lag_bounds"], current_lag_bounds=sample["disk_sequence_lag_bounds"])
    return events


def match_events(rows, events, start):
    for index, event in enumerate(events):
        event["window"] = rows[0]["window"]
        event["id"] = f"{event['window']}#e{index:04}"
        event["inside_seconds"], event["boundary_seconds"] = [], []
        key = f"{event['kind']}/db{event['db']}"
        for row in rows:
            classification = marker_clock_overlap(start, event["time_ns"], row["second"])
            if classification:
                counts = row["event_counts"][classification]
                counts[key] = counts.get(key, 0) + 1
                event[f"{classification}_seconds"].append(row["second"])


def replay(archive, manifest, frozen, observer):
    rows, events, windows, stage_checks = [], [], [], {}
    for stage, expected_count in STAGES.items():
        root = ZipPath(archive, f"data/{stage}/")
        readback = read_json(root / "ext4-readback.json")
        ledger = read_json(root / "run.json")
        slots = ledger["windows"]
        require(len(slots) == expected_count == readback["recorded_windows"] and readback["matched"]
                and readback["driver_exit_code"] == 0 and readback["attempt_states"] == {"completed": expected_count}
                and digest((root / "run.json").read_bytes()) == readback["ledger_sha256"], f"{stage}: ext4 readback")
        raw_files = 0
        for slot in slots:
            attempt, = slot["attempts"]
            require(attempt["state"] == "completed" and attempt["directory"].endswith("/" + slot["label"]), "attempt identity")
            folder = root / attempt["directory"]
            for name, expected_sha in attempt["files"].items():
                require(manifest.get((folder / name).at) == expected_sha, f"stage ledger mismatch: {name}")
            raw_files += len(attempt["files"])
            case, client, hist, common = (read_json(folder / name) for name in
                                          ("case.json", "client.json", "client-hist.json", "common-observer.json"))
            frozen_window = frozen.read_window(folder)
            require(frozen_window["primary_load_met"] and case["client_exit_code"] == 0 and
                    all(slot[k] == case[k] for k in ("arm", "read_percent", "rate")) and
                    client["duration_seconds"] == 30 and client["histogram_max_relative_error"] == 1/256,
                    "window load/identity/range changed")
            terminal = frozen.check_terminal(folder, case)
            frozen.offer_interval(case, client)
            alignment = case["measurement_alignment"]
            start = alignment["projected_start_monotonic_ns"]
            require(alignment["projection_consistent"] and start[1] - start[0] < SECOND, "cannot align clocks")
            require(common["passed"] and case["window_observer"]["sha256"] ==
                    manifest[(folder / "common-observer.json").at], "common observer digest")
            full = None
            if slot["arm"] == "E":
                detail = read_json(folder / "live-observer.json")
                evidence_path = folder / detail["evidence"]
                require(detail["passed"] and detail["witness_sha256"] == manifest[evidence_path.at],
                        "detailed observer input digest")
                witness = read_json(evidence_path)
                require(witness["markers"] == common["markers"] and witness["lineage"] == terminal["source"],
                        "different marker roster or source")
                full = observer.derived(witness, folder)
                require(all(detail[k] == v for k, v in full.items()), "raw segment/witness replay differs")
            window = f"{stage}/{slot['label']}"
            per_second = seconds_for(window, slot, start, client, hist)
            seen = observations(case, common, full)
            match_events(per_second, seen, start)
            rows.extend(per_second)
            events.extend(seen)
            windows.append({"id": window, "stage": stage, "arm": slot["arm"], "rate": slot["rate"],
                            "read_percent": slot["read_percent"], "started_utc": client["started_utc"],
                            "projected_start_monotonic_ns": start, "clock_uncertainty_ns": start[1] - start[0],
                            "raw_directory": folder.at, "raw_files": len(attempt["files"]), "events": len(seen),
                            "source_lineage": terminal["source"],
                            "triggered_seconds": [r["second"] for r in per_second if r["triggered"]],
                            "marker_reads": {"primary": len(common["markers"]),
                                             "followers": sum(len(m["final_replicas"]) for m in common["markers"]),
                                             "markers_with_exact_source": sum(m["scope"] == "exact-source-and-get"
                                                                             for m in full["markers"]) if full else 0},
                            "status_peers": sorted(case["terminal"].get("follower_current", {})),
                            "snapshots_enabled": slot["arm"] in "BDE", "detailed_observer": full is not None})
        require(raw_files == readback["verified_raw_files"], f"{stage}: raw file count")
        stage_checks[stage] = {"windows": len(slots), "raw_files": raw_files, "ledger_sha256": readback["ledger_sha256"]}
    require(len(windows) == len({w["id"] for w in windows}) == 44 and len(rows) == len({r["id"] for r in rows}) == 1320,
            "formal/recheck coverage")
    return rows, windows, events, stage_checks


def make_groups(rows):
    groups = defaultdict(list)
    for row in rows:
        groups[row["stage"], row["rate"], row["arm"]].append(row)
    detailed, counts = [], []
    for (stage, rate, arm), group in sorted(groups.items()):
        fired = [r for r in group if r["triggered"]]
        entry = {"stage": stage, "rate": rate, "read_percent": group[0]["read_percent"], "arm": arm,
                 "windows": len({r["window"] for r in group}), "seconds": len(group),
                 "triggered_windows": len({r["window"] for r in fired}), "triggered_seconds": len(fired)}
        counts.append(entry)
        detailed.append({**entry, "all": summarize(group), "triggered": summarize(fired),
                         "normal": summarize([r for r in group if not r["triggered"]])})
    return counts, detailed


def old_scan_check(archive, rows):
    expected = read_json(ZipPath(archive, "data/reviews/final-astra.json"))["secondary_descriptive_scan"]["counts"]
    actual = {}
    for stage, rate in sorted({(r["stage"], r["rate"]) for r in rows}):
        group = [r for r in rows if (r["stage"], r["rate"]) == (stage, rate)]
        fired = summarize([r for r in group if r["triggered"]])
        fraction = fired["set"]["fraction_of_elapsed_in_client_queue"]
        actual[f"{stage}/{rate}"] = {
            "planned_seconds": len(group), "set_success_p99_ge_100ms": fired["seconds"],
            "also_SET_queue_p99_ge_100ms": fired["set"]["seconds_queue_p99_ge_100ms"],
            "also_GET_queue_p99_ge_100ms": fired["get"]["seconds_queue_p99_ge_100ms"],
            "SETs_in_flagged_seconds": fired["set"]["offered"],
            "aggregate_queue_over_total_mean_latency": round(fraction, 5) if fraction is not None else None,
        }
    return {"agrees": actual == expected, "independent": actual, "signed": expected}


ELIGIBLE = {"snapshot_cutoff_published": "BDE", "follower_sequence_advanced": "CDE",
            "follower_lag_sample": "E", "marker_primary_set": "ABCDE", "marker_source_available": "E",
            "marker_disk_current_visible": "E", "marker_follower_get_visible": "E"}


def event_coverage(rows):
    output = {}
    for kind, arms in ELIGIBLE.items():
        eligible = [r for r in rows if r["arm"] in arms]
        result = {"eligible_seconds": len(eligible)}
        for dbs, name in (((0, 1), "either"), ((0,), "db0"), ((1,), "db1")):
            keys = [f"{kind}/db{db}" for db in dbs]
            hits, boundary_only = 0, 0
            for row in eligible:
                counts = row["event_counts"]
                if any(k in counts["inside"] for k in keys):
                    hits += 1
                elif any(k in counts["boundary"] for k in keys):
                    boundary_only += 1
            result[name] = {"seconds_with_inside_bracket": hits, "seconds_with_boundary_only": boundary_only}
        output[kind] = result
    return output


def interval_summary(events):
    grouped = defaultdict(list)
    for event in events:
        grouped[event["kind"]].append(event)
    return {kind: {"records": len(group), "with_known_inside_second": sum(bool(e["inside_seconds"]) for e in group),
                   "boundary_only": sum(bool(e["boundary_seconds"]) and not e["inside_seconds"] for e in group),
                   "outside_offer": sum(not e["boundary_seconds"] and not e["inside_seconds"] for e in group),
                   "bracket_width_ms_min_median_max": [v/1e6 for v in stats(
                       e["time_ns"][1] - e["time_ns"][0] for e in group)]}
            for kind, group in sorted(grouped.items())}


def stats(values):
    values = list(values)
    return [min(values), median(values), max(values)]


def lag_for_seconds(events, rows):
    selected = {(r["window"], r["second"]) for r in rows if r["arm"] == "E"}
    per_db = defaultdict(list)
    for event in events:
        if event["kind"] == "follower_lag_sample" and any(
                (event["window"], n) in selected for n in event["inside_seconds"]):
            per_db[event["db"]].append(event)
    result = {}
    for db, samples in sorted(per_db.items()):
        entry = {"samples_wholly_inside": len(samples)}
        for kind in ("status_lag_bounds", "current_lag_bounds"):
            entry[kind] = {peer: [max(e[kind][peer][i] for e in samples) for i in (0, 1)] for peer in ("a", "b")}
        result[str(db)] = entry
    return result


def control_metrics(indexed, matches):
    for match in matches:
        match["metrics"] = {}
        for side in ("trigger", "before", "after", "fallback"):
            if match[side] is None:
                continue
            r = indexed[match[side]]
            match["metrics"][side] = {op: {
                "requests": r[op]["offered"], "success_p99_us": r[op]["success_latency"]["p99_us"],
                "queue_p99_us": r[op]["client_queue_delay"]["p99_us"],
                "total_success_elapsed_ns": r[op]["sum_ns"]["success"],
                "total_client_queue_ns": r[op]["sum_ns"]["client_queue"]} for op in OPS}


def assemble(archive_path, output, public):
    archive_bytes = archive_path.read_bytes()
    require(digest(archive_bytes) == ARCHIVE_SHA256, "archive SHA-256 differs from the sealed package")
    with ZipFile(BytesIO(archive_bytes)) as archive, TemporaryDirectory() as tempdir:
        manifest = verify_manifest(archive)
        # Load only the verified frozen analysis functions, keeping the replay independent of checkout revisions.
        for name in manifest:
            if name.startswith(PINNED_ANALYZER) and name.endswith(".py"):
                (Path(tempdir) / Path(name).name).write_bytes(archive.read(name))
        sys.path.insert(0, tempdir)
        try:
            import golog_replica_set_tail_compare as frozen
            import golog_replica_live_observer as observer
            require(Path(frozen.__file__).parent == Path(tempdir) == Path(observer.__file__).parent, "unfrozen analyzer")
            rows, windows, events, stages = replay(archive, manifest, frozen, observer)
        finally:
            sys.path.remove(tempdir)
        windows_receipt = read_json(ZipPath(archive, "data/reviews/windows-formal-recheck-receipt.json"))
        require(windows_receipt["output_sha256"] == manifest["data/reviews/windows-formal-recheck.json"]
                and windows_receipt["exit_code"] == 0, "original Windows receipt")
        all_metrics = summarize(rows)
        actual = {k: sum(all_metrics[op][k] for op in OPS) for k in ("offered", "succeeded", "failures")}
        require(actual == windows_receipt["total_counts"] == {"offered": 6120000, "succeeded": 6120000, "failures": 0},
                "44-window client denominator")
        old_scan = old_scan_check(archive, rows)
    grouped_rows = defaultdict(list)
    indexed = {r["id"]: r for r in rows}
    for row in rows:
        grouped_rows[row["window"]].append(row)
    triggered = [r for r in rows if r["triggered"]]
    matches = [{"trigger": r["id"], **select_controls(r, grouped_rows)} for r in triggered]
    refs = [m[side] for m in matches for side in ("before", "after", "fallback") if m[side]]
    control_metrics(indexed, matches)
    unique = list(dict.fromkeys(refs))
    groups, extended = make_groups(rows)
    output.mkdir(parents=True, exist_ok=False)
    for name, data in (("seconds", rows), ("events", events), ("windows", windows), ("controls", matches)):
        save_rows(output / f"{name}.jsonl", data)
    save_json(output / "groups.json", extended)
    report = {
        "schema": 1,
        "replay_script_sha256_with_lf": digest(Path(__file__).read_bytes().replace(b"\r\n", b"\n")),
        "source_archive": {"file": archive_path.name, "bytes": len(archive_bytes), "sha256": ARCHIVE_SHA256,
                           "manifest_members_verified": len(manifest), "stages": stages,
                           "source_tar_sha256": manifest["data/source.tar"],
                           "signed_astra_sha256": manifest["data/reviews/final-astra.json"],
                           "prior_windows_replay_sha256": windows_receipt["output_sha256"]},
        "units_and_method": {
            "population": "正式两期各20窗及4个定向复验窗，每窗30个计划发出秒。归档的14个短试窗单列在前一阶段；本表均取正式或复验数据。",
            "trigger": "成功 SET 端到端 p99 ≥100000 µs。GET/SET 按原计划发出秒归属，排空期间完成的请求归回相应计划秒。连续慢秒共享一个实验窗口。",
            "latency": "计划时刻至完整回复的成功延迟含客户端等待。0.390625% 为稀疏直方图登记的最大相对误差；每秒仅有摘要。分组分位数栏是各秒分位数的最小/中位/最大值。",
            "weighted_time": "按每秒每类请求的 count × mean_us ×1000 四舍五入至 ns，并逐窗与直方图的整数 sum_ns 和 count 对账。排队份额是累计计划时刻至串行连接开始处理的等待时间除以同一批成功请求的累计端到端时间。此等待也可包含定时器或调度迟到。余下耗时含客户端准备、网络、服务端和回包后的处理。",
            "controls": "每个慢秒按时间各取同窗最近的前、后正常秒；任一侧空缺保留。整窗均慢时才从同阶段、同档、同臂、同观测模式最近的有正常秒窗口取最邻近的计划位置，并优先较早位置。对照可复用。",
            "time_bounds": "观测单调时钟以归档 UTC 桥、释放信号和原5ms余量复核。闭事件区间完全落入所有可能的同一半开计划秒记 inside；仅与某可能秒相交记 boundary。区间可横跨多个秒；单秒统计按秒去重。",
            "replication": "DB0 承载基准，DB1 为标记库。按库、来源及副本身份复核发布。共同观察器的主库 INFO 和从库 status 约每秒采样一次；跨采样区间只能说明期间序号至少推进一次。E 臂另外保存已封存序号与状态页、CURRENT 的配对采样区间。",
            "lag": "E 臂滞后以封存或快照的可取序号减从库序号衡量。每类样本按下界最大值和上界最大值界定采样点最大滞后，仅纳入整个采样都落入指定秒的记录。秒间与两次采样之间仍可能有更大滞后。",
            "snapshots_and_markers": "Rust 区间表示快照截止序号增加的可见时段。E 臂逐条回读封存帧和来源、双从读值，再计算来源可取、CURRENT 路径可见和正确 GET 的时界。A/B 无从库，C/D 只做终态标记 GET。路径可见时刻不能测出目录 fsync 的结束时刻。",
            "gaps": "旧数据缺逐请求相连的占队、取队、开始写出与回复时间；也缺同钟、细粒度主库磁盘同步/锁等待、设备服务耗时、宿主及来宾调度压力。所有1320秒的 initial_server_wait_observed 均为 null。聚合或观测区间重合不能识别最先慢下来的操作或服务端组件。",
        },
        "independent_counts": actual, "integer_histogram_count_and_sum_checks": len(windows)*len(OPS)*len(HISTOGRAMS),
        "all_seconds": all_metrics, "triggered": summarize(triggered), "stage_rate_arm_counts": groups,
        "triggered_window_ids": {w["id"]: w["triggered_seconds"] for w in windows if w["triggered_seconds"]},
        "controls": {"trigger_ids": len(matches), "references": len(refs), "unique_control_seconds": len(unique),
                     "missing_before": sum(m["before"] is None for m in matches),
                     "missing_after": sum(m["after"] is None for m in matches),
                     "fallbacks": sum(m["fallback"] is not None for m in matches),
                     "without_any": sum(not any(m[s] for s in ("before", "after", "fallback")) for m in matches),
                     "unique_control_metrics": summarize([indexed[k] for k in unique]),
                     "reference_weighted_metrics": summarize([indexed[k] for k in refs])},
        "clock_uncertainty_ms_min_median_max": [n/1e6 for n in stats(w["clock_uncertainty_ns"] for w in windows)],
        "marker_checks": dict(sum((Counter(w["marker_reads"]) for w in windows), Counter())),
        "observation_records": interval_summary(events), "coincidence_by_second": {
            "triggered": event_coverage(triggered), "unique_controls": event_coverage([indexed[k] for k in unique]),
            "normal": event_coverage([r for r in rows if not r["triggered"]])},
        "detailed_E_max_sampled_sequence_lag_bounds": {
            "triggered": lag_for_seconds(events, triggered),
            "unique_controls": lag_for_seconds(events, [indexed[k] for k in unique])},
        "comparison_to_prior_scan": old_scan,
        "reproduce": ["python -B scripts/golog_replica_stall_audit.py --archive dist/Kerniva-SET-tail-20261126-evidence.zip --outdir replay/retrospective --summary replay/report.json",
                      "在空白输出目录复算。对报告原始字节及下列逐秒、事件、窗口、对照与分组文件执行 SHA-256 交叉校验。"],
        "derived_files": {p.name: {"sha256": digest(p.read_bytes()), "bytes": p.stat().st_size}
                          for p in sorted(output.iterdir())},
    }
    public.parent.mkdir(parents=True, exist_ok=True)
    save_json(public, report)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--outdir", type=Path, required=True, help="a new directory for full event and second tables")
    parser.add_argument("--summary", type=Path, required=True, help="a new path for the public reproducible receipt")
    args = parser.parse_args()
    if args.outdir.exists() or args.summary.exists():
        parser.error("choose new output paths for a clean replay")
    report = assemble(args.archive, args.outdir, args.summary)
    print(json.dumps({"seconds": report["all_seconds"]["seconds"], "triggers": report["triggered"]["seconds"],
                      "report_sha256": digest(args.summary.read_bytes())}))


if __name__ == "__main__":
    main()
