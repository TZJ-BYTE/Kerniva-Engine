"""Reconcile capped request traces with the original histogram and scheduled-second counts."""
import argparse
import json
import math
import statistics
from collections import Counter, defaultdict
from functools import lru_cache
from pathlib import Path

NS = 1_000_000_000
SLOW = NS // 10
MASK = (1 << 64) - 1
OPS = ("get", "set")


def require(condition, detail):
    if not condition:
        raise ValueError(detail)


def read(path):
    return json.loads(path.read_bytes())


def mix(value):
    value = (value + 0x9e3779b97f4a7c15) & MASK
    value = ((value ^ (value >> 30)) * 0xbf58476d1ce4e5b9) & MASK
    value = ((value ^ (value >> 27)) * 0x94d049bb133111eb) & MASK
    return value ^ (value >> 31)


@lru_cache(maxsize=4)
def schedule(total, clients, rate, seed, stride, read_percent):
    by_group = defaultdict(Counter)
    uniform = set()
    for idx in range(total):
        op = int(mix(idx ^ seed) % 100 >= read_percent)
        group = by_group[idx % clients, idx // rate, op]
        group["offered"] += 1
        if mix(idx ^ seed ^ 0xd81b54a32d192ed3) % stride == 0:
            uniform.add(idx)
            group["uniform"] += 1
    return by_group, uniform


def bounds(index):
    if index < 512:
        return index, index
    shift = (index - 512) // 256 + 1
    m = (index - 512) % 256 + 256
    return m << shift, ((m + 1) << shift) - 1


def sparse_check(hist, summary):
    require(sum(b["count"] for b in hist["bins"]) == hist["count"] == summary["count"], "histogram count")
    for field, q in (("p99_us", .99), ("p99_9_us", .999)):
        rank, seen = math.ceil(hist["count"] * q), 0
        for b in hist["bins"]:
            seen += b["count"]
            if seen >= rank:
                expected = min(bounds(b["index"])[1], hist["max_ns"])
                require(expected == round(1000 * summary[field]), "histogram rank")
                break


def percentages(values):
    if not values:
        return None
    values = sorted(values)
    return {f"p{q}_ms": round(values[math.ceil(len(values) * q / 100) - 1] / 1e6, 6)
            for q in (50, 95, 99, 100)}


def breakdown(rows, prior):
    if not rows:
        return {"count": 0}
    spans = defaultdict(list)
    majority, equal, traced_previous, previous_exchange_overlaps_plan = 0, 0, 0, 0
    example = None
    for r in rows:
        a, b, c, d = (r[f] for f in ("planned_ns", "dequeued_ns", "send_start_ns", "completed_ns"))
        durations = {"queue": b-a, "preparation": c-b, "exchange": d-c, "total": d-a}
        for k, v in durations.items():
            spans[k].append(v)
        majority += 2*(b-a) > d-a
        equal += 2*(b-a) == d-a
        if prev := prior(r):
            traced_previous += 1
            previous_exchange_overlaps_plan += prev["send_start_ns"] <= a < prev["completed_ns"]
        if example is None or d-a > example["completed_ns"]-example["planned_ns"]:
            example = r
    return {"count": len(rows), "queue_strict_majority": majority,
            "queue_equal_half": equal, "queue_below_half": len(rows)-majority-equal,
            "sum_ns": {k: sum(v) for k, v in spans.items()},
            "durations": {k: percentages(v) for k, v in spans.items()},
            "traced_immediate_predecessors": traced_previous,
            "predecessor_exchange_spans_current_plan": previous_exchange_overlaps_plan,
            "max_total_example": example, "max_total_traced_predecessor": prior(example)}


def trace_audit(path, c, h, triggers, controls):
    with path.open(encoding="utf-8") as stream:
        meta = json.loads(next(stream))
        require(meta["type"] == "meta" and meta["started_utc"] == c["started_utc"], "trace epoch")
        clients, rate, total = c["clients"], c["target_rate"], c["offered"]
        require((meta["offered"], meta["clients"], meta["rate"], meta["seed"], meta["stride"],
                 meta["slow_threshold_ns"]) == (total, clients, rate, c["seed"], 16, SLOW), "trace options")
        expected, uniform = schedule(total, clients, rate, c["seed"], 16, c["read_percent"])
        rows, actual = {}, defaultdict(Counter)
        summary = None
        for line in stream:
            r = json.loads(line)
            require(summary is None, "data after trace summary")
            if r.get("type") == "summary":
                summary = r
                continue
            idx = r["index"]
            require(0 <= idx < total and idx not in rows and r["worker"] == idx % clients, "request identity")
            a,b,s,d = (r[f] for f in ("planned_ns","dequeued_ns","send_start_ns","completed_ns"))
            op = int(mix(idx ^ c["seed"]) % 100 >= c["read_percent"])
            require(a == idx * NS // rate and r["op"].lower() == OPS[op], "request schedule")
            require(r["outcome"] == "success" and a <= b <= s <= d, "request timing or outcome")
            require(r["uniform"] == (idx in uniform) and (r["uniform"] or d-a >= SLOW), "selection")
            group = actual[r["worker"], idx//rate, op]
            group["kept"] += 1
            group["kept_uniform"] += r["uniform"]
            group["kept_slow"] += d-a >= SLOW
            rows[idx] = r
    require(summary and summary["offered"] == total and summary["drops"] == 0 and
            summary["candidates"] == summary["kept"] == len(rows) and uniform <= rows.keys(), "trace completeness")
    coverage = summary["by_worker_second_get_set"]
    seconds = len(c["windows"])
    require(len(coverage) == clients and all(len(worker) == seconds for worker in coverage), "coverage shape")
    slow_total, by_second = Counter(), Counter()
    for worker, groups in enumerate(coverage):
        for sec, pair in enumerate(groups):
            require(len(pair) == 2, "coverage operations")
            for op, v in enumerate(pair):
                k = worker, sec, op
                require(v["offered"] == expected[k]["offered"] and v["drops"] == 0 and
                        v["uniform"] == v["kept_uniform"] == actual[k]["kept_uniform"] == expected[k]["uniform"] and
                        v["slow"] == v["kept_slow"] == actual[k]["kept_slow"] and
                        v["kept"] == actual[k]["kept"], f"worker-second mismatch {k}")
                by_second[sec, op] += v["offered"]
                slow_total[OPS[op]] += v["slow"]
    for sec, w in enumerate(c["windows"]):
        for op, name in enumerate(OPS):
            require(w["planned_second"] == sec and w[name]["offered"] == by_second[sec, op], "second denominators")
    for op in OPS:
        by_bin = h[op]["success"]["bins"]
        lower = sum(b["count"] for b in by_bin if bounds(b["index"])[0] >= SLOW)
        upper = sum(b["count"] for b in by_bin if bounds(b["index"])[1] >= SLOW)
        require(lower <= slow_total[op] <= upper, "exact slow count outside sparse-bin limits")
    exact = defaultdict(list)
    for r in rows.values():
        sec, op = r["index"] // rate, r["op"].lower()
        if r["completed_ns"] - r["planned_ns"] >= SLOW:
            exact["slow",sec,op].append(r)
        if r["uniform"]:
            exact["uniform",sec,op].append(r)
    prior = lambda r: rows.get(r["index"]-clients)
    selected = sorted(set(triggers).union(controls))
    return {"retained": len(rows), "uniform": len(uniform), "exact_slow": dict(slow_total),
            "drops": 0, "sampled_workers": len({r["worker"] for r in rows.values()}),
            "all_slow": {op: breakdown([r for (kind, _, o), group in exact.items()
                                         if kind == "slow" and op == o for r in group], prior) for op in OPS},
            "trigger_slow": {op: breakdown([r for sec in triggers for r in exact["slow",sec,op]], prior) for op in OPS},
            "matched_seconds": {sec: {op: {kind: breakdown(exact[kind,sec,op],prior)
                                         for kind in ("slow","uniform")} for op in OPS} for sec in selected}}


def wall_ns(stamp):
    from datetime import datetime
    import re
    date, digits, zone = re.fullmatch(r"(.*?)(?:\.(\d+))?(Z|[+-]\d\d:\d\d)", stamp).groups()
    return int(datetime.fromisoformat(date+zone.replace("Z","+00:00")).timestamp())*NS + int((digits or "").ljust(9,"0"))


def diff(a, b, keys):
    return {k: b[k]-a[k] for k in keys if k in a and k in b}


def telemetry(folder, root, phase, client, seconds):
    case = read(folder/"case.json")
    aligned = case["measurement_alignment"]["projected_start_monotonic_ns"]
    require(len(aligned) == 2 and aligned[0] <= aligned[1], "guest clock bracket")
    release = next(e for e in case["events"] if e["name"] == "client_start_release")
    name = phase+"--"+folder.name
    signals = root/"host-signals"
    receipt = read(signals/(name+".receipt.json"))
    ready = read(signals/(name+".ready.json"))
    host = list(map(json.loads, (signals/(name+".samples.jsonl")).read_bytes().splitlines()))
    require(receipt["received_close"] and len(host) == receipt["samples"] >= 2, "host sampling incomplete")
    guest_open = receipt["guest_open"]["guest_utc_ns"]
    host_ready = ready["host_utc_ns"]
    # on the same physical timeline: guest opens -> first host sample -> host ready -> guest releases.
    # The guest event uses microsecond UTC serialization; include its last possible nanosecond.
    bounds_ns = host_ready-wall_ns(release["utc"])-999, host[0]["host_utc_ns"]-guest_open
    require(bounds_ns[0] <= bounds_ns[1], "inconsistent guest/host causal bracket")
    start = wall_ns(client["started_utc"])
    # Include observed UTC vs monotonic interval divergence throughout this short window.
    drifts = [abs((s["host_utc_ns"]-host[0]["host_utc_ns"])-
                  (s["host_mono_before_ns"]-host[0]["host_mono_before_ns"])) for s in host]
    guest = list(map(json.loads, (folder/"guest-samples.jsonl").read_bytes().splitlines()))
    g = [item for item in guest if item["record"] == "sample"]
    require(guest[-1]["record"] == "summary" and len(g) == guest[-1]["sample_rows"], "guest sample count")
    guest_drift = max(abs((s["start"]["utc_unix_ns"]-g[0]["start"]["utc_unix_ns"])-
                          (s["start"]["monotonic_before_ns"]-g[0]["start"]["monotonic_before_ns"])) for s in g)
    pad = max(drifts) + guest_drift + max(s["host_mono_after_ns"]-s["host_mono_before_ns"] for s in host)
    intervals = {"host": [], "guest": []}
    for a,b in zip(host,host[1:]):
        cpu = diff(a["cpu_100ns"],b["cpu_100ns"],a["cpu_100ns"])
        total = cpu["kernel_including_idle"]+cpu["user"]
        intervals["host"].append({
            "earliest_offset_ns": a["host_utc_ns"]-bounds_ns[1]-pad-start,
            "latest_offset_ns": b["host_utc_ns"]-bounds_ns[0]+pad-start,
            "interval_ns": b["host_mono_before_ns"]-a["host_mono_before_ns"],
            "busy_pct": round(100*(total-cpu["idle"])/total, 3) if total else None,
            "disk_delta":diff(a["disk"],b["disk"],("reads","writes","read_bytes","write_bytes",
                                                   "read_time_100ns","write_time_100ns","idle_time_100ns"))})
    for a,b in zip(g,g[1:]):
        item = {"earliest_offset_ns": a["start"]["monotonic_before_ns"]-aligned[1],
                "latest_offset_ns": b["end"]["monotonic_after_ns"]-aligned[0],
                "interval_ns": b["start"]["monotonic_before_ns"]-a["start"]["monotonic_before_ns"]}
        for src in ("cpu","disk","primary_db0_info","psi_cpu","psi_io","psi_memory"):
            v,w = a["source"][src].get("value"),b["source"][src].get("value")
            if v is None or w is None:
                item[src] = None
            elif src.startswith("psi_"):
                item[src] = {k: w[k]["total"]-v[k]["total"] for k in w.keys() & v.keys()}
            elif src == "disk":
                item[src] = diff(v["fields"],w["fields"],("reads_completed","writes_completed","flushes_completed",
                                 "flush_ms","write_ms","io_ms","weighted_io_ms","sectors_written"))
            else:
                item[src] = diff(v,w,v)
        intervals["guest"].append(item)
    ids = {sec: {side: [i for i,row in enumerate(rows) if row["earliest_offset_ns"] < (sec+1)*NS and
                        row["latest_offset_ns"] > sec*NS] for side,rows in intervals.items()} for sec in sorted(seconds)}
    return {"host_minus_guest_utc_bracket_ns": bounds_ns, "observed_drift_and_read_padding_ns": pad,
            "physical_disk_number": receipt["disk"], "guest_device": guest[0]["device"],
            "guest_sample_summary":guest[-1], "intervals":intervals, "possibly_overlapping_intervals":ids,
            "interpretation":"Intervals can overlap adjacent planned seconds. Shared cumulative counters cannot identify an individual disk sync."}


def external_controls(cases):
    """Match an all-trigger window to normal seconds without crossing stage, arm or mode."""
    external, needed = {}, defaultdict(set)
    for name, (mode, start, normals) in cases.items():
        if normals:
            continue
        donors = [(abs(other_start-start),other_start,other,usable)
                  for other,(other_mode,other_start,usable) in cases.items()
                  if other != name and other[-1] == name[-1] and other_mode == mode and usable]
        if not donors:
            external[name] = {sec: None for sec in range(30)}
            continue
        _, _, donor, usable = min(donors)
        matches = {}
        for sec in range(30):
            nearest = min(usable, key=lambda i: (abs(i-sec),i))
            matches[sec] = {"case":donor,"second":nearest}
            needed[donor].add(nearest)
        external[name] = matches
    return external, needed


def audit_case(folder, root, stage, mode, extra_controls=(), fallback=None):
    c, h = read(folder/"client.json"), read(folder/"client-hist.json")
    require(c["offered"] == c["sent"] == c["succeeded"] == 180000 and c["failures"] == 0, "workload failures/count")
    for op in OPS:
        sparse_check(h[op]["success"],c[op]["success_latency"])
        sparse_check(h[op]["client_queue"],c[op]["client_queue_delay"])
    seconds = c["windows"]
    require(len(seconds) == 30, "planned second coverage")
    triggers = [w["planned_second"] for w in seconds if w["set"]["success_latency"]["p99_us"] >= SLOW/1000]
    normal = [s for s in range(30) if s not in triggers]
    pairs = {s:[max((i for i in normal if i<s), default=None),min((i for i in normal if i>s),default=None)]
             for s in triggers}
    controls = {i for p in pairs.values() for i in p if i is not None} | set(extra_controls)
    trace = folder/"client-trace.jsonl"
    require(trace.exists() == (mode == "ON"), "capture mode mismatch")
    if mode == "OFF":
        require(not (folder/"guest-samples.jsonl").exists(), "guest sampler present in OFF")
        require(not (root/"host-signals"/(stage+"--"+folder.name+".open.json")).exists(), "host sampler present in OFF")
    window = {"case":folder.name,"arm":folder.name[-1],"mode":mode,"started_utc":c["started_utc"],
            "offered":c["offered"],"success":c["succeeded"],"failures":c["failures"],
            "in_offer_success":sum(c[op]["succeeded_in_window"] for op in OPS),"actual_success_rate":c["actual_success_rate"],
            "set_p99_us":c["set"]["success_latency"]["p99_us"],"set_p999_us":c["set"]["success_latency"]["p99_9_us"],
            "get_p99_us":c["get"]["success_latency"]["p99_us"], "trigger_seconds":triggers,"control_seconds":pairs,
            "trace":trace_audit(trace,c,h,triggers,controls) if mode=="ON" else None,
            "telemetry":telemetry(folder,root,stage,c,set(triggers)|controls) if mode=="ON" else None}
    if fallback is not None:
        window["external_control_seconds"] = fallback
    return window


def pair_review(windows):
    results = {}
    for arm in ("A","E"):
        group = [w for w in windows if w["arm"]==arm]
        off = [w for w in group if w["mode"]=="OFF"]
        limits = {"set_p99_us":max(1000, .15*statistics.median(w["set_p99_us"] for w in off)),
                  "set_p999_us":max(2000,.25*statistics.median(w["set_p999_us"] for w in off)),
                  "actual_success_rate":120}
        pairs=[]
        for i in (0,2):
            left,right=group[i:i+2]
            require(left["mode"]!=right["mode"],"pair order")
            hi,lo = (left,right) if left["mode"]=="ON" else (right,left)
            delta = {k:hi[k]-lo[k] for k in limits}
            pairs.append({"on":hi["case"],"off":lo["case"],"on_minus_off":delta,
                          "outside_tolerance":[k for k in delta if abs(delta[k])>limits[k]]})
        results[arm] = {"thresholds":limits,"pairs":pairs}
    return results


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root",type=Path,required=True)
    parser.add_argument("--stage",required=True,choices=("calibration","formal-1","formal-2"))
    parser.add_argument("--out",type=Path,required=True)
    args=parser.parse_args()
    ledger=read(args.root/args.stage/"run.json")
    cases = {}
    for w in ledger["windows"]:
        c = read(args.root/args.stage/w["attempts"][-1]["directory"]/"client.json")
        usable = [sec["planned_second"] for sec in c["windows"] if sec["set"]["success_latency"]["p99_us"] < SLOW/1000]
        cases[w["label"]] = (w["mode"],wall_ns(c["started_utc"]),usable)
    external, needed = external_controls(cases)
    results=[]
    for w in ledger["windows"]:
        attempt=w["attempts"][-1]
        require(attempt["state"]=="completed","incomplete attempt")
        results.append(audit_case(args.root/args.stage/attempt["directory"],args.root,args.stage,w["mode"],
                                  needed[w["label"]],external.get(w["label"])))
    require(len(results)==8,"eight-window stage incomplete")
    report={"schema_version":1,"stage":args.stage,"windows":results,"sample_pairs":pair_review(results)}
    args.out.parent.mkdir(parents=True,exist_ok=True)
    args.out.write_bytes((json.dumps(report,sort_keys=True,ensure_ascii=False,separators=(",",":"))+"\n").encode())


if __name__=="__main__":
    main()
