"""Join primary Go GC pauses to same-request client timings on Linux CLOCK_MONOTONIC."""
import argparse
import bisect
import hashlib
import json
from collections import defaultdict
from pathlib import Path

from golog_stall_trace_report import trace_audit


def read(path):
    return json.loads(path.read_bytes())


def lines(path):
    return [json.loads(line) for line in path.read_bytes().splitlines()]


def require(condition, message):
    if not condition:
        raise ValueError(message)


def merged(ranges):
    out = []
    for a, b in sorted(ranges):
        if b <= a:
            continue
        if out and out[-1][1] >= a:
            out[-1] = out[-1][0], max(out[-1][1], b)
        else:
            out.append((a,b))
    return out


def overlap(a, b, events):
    return sum(max(0, min(b, end)-max(a, start)) for start, end in events)


def exposure(start, end, epoch, inner, outer):
    # All possible positions of this one request share the same unknown epoch.
    # Common intersection gives a lower bound; possible union gives an upper bound.
    lo, hi = epoch
    minimum = overlap(hi+start, lo+end, inner)
    maximum = min(end-start, overlap(lo+start, hi+end, outer))
    return minimum, maximum


def primary_info(capture):
    before = capture["gc_info_before"]["value"]
    after = capture["gc_info_after"]["value"]
    return {key: int(after[key])-int(before[key]) for key in ("num_gc","gc_pause_total_ns")}


def totals():
    return {"requests":0, "overlap_certain":0, "overlap_possible":0,
            "possibly_at_least_half_exchange":0, "sum_exchange_ns":0,
            "lower_overlap_ns":0, "upper_overlap_ns":0}


def count(bucket, minimum, maximum, exchange):
    bucket["requests"] += 1
    bucket["overlap_certain"] += minimum > 0
    bucket["overlap_possible"] += maximum > 0
    bucket["possibly_at_least_half_exchange"] += maximum > 0 and 2*maximum >= exchange
    bucket["sum_exchange_ns"] += exchange
    bucket["lower_overlap_ns"] += minimum
    bucket["upper_overlap_ns"] += maximum


def timing(row):
    return {k: row[k] for k in ("worker","index","op","planned_ns","dequeued_ns","send_start_ns","completed_ns")}


def request_comparison(rows, epochs, inner, outer, coverage):
    ordered = {r["index"]: r for r in rows}
    slow = [r for r in rows if r["completed_ns"]-r["planned_ns"] >= 100_000_000]
    short = defaultdict(list)
    for r in rows:
        if r["uniform"] and r["completed_ns"]-r["planned_ns"] < 100_000_000:
            short[r["worker"],r["op"]].append(r["index"])
    for group in short.values():
        group.sort()
    controls = set()
    links = {}
    for r in slow:
        matches = short[r["worker"],r["op"]]
        pos = bisect.bisect_left(matches,r["index"])
        pair = [matches[pos-1] if pos > 0 else None, matches[pos] if pos < len(matches) else None]
        controls.update(i for i in pair if i is not None)
        links[r["index"]] = pair
    datasets = {}
    examples = []
    for r in rows:
        start, end = r["send_start_ns"], r["completed_ns"]
        is_slow = r["index"] in links
        groups = []
        if is_slow:
            groups.append("all_slow")
        if r["index"] in controls:
            groups.append("matched_normal")
        if r["uniform"] and not is_slow:
            groups.append("uniform_normal")
        details = {}
        for method, epoch in epochs.items():
            require(coverage[0] <= epoch[0]+start and epoch[1]+end <= coverage[1], "request outside trace coverage")
            low, high = exposure(start,end,epoch,inner,outer)
            for group in groups:
                key = f"{method}/{group}/{r['op']}"
                count(datasets.setdefault(key,totals()),low,high,end-start)
            details[method] = {"exchange_overlap_ns":[low,high],
                               "before_send_overlap_ns":exposure(r["planned_ns"],start,epoch,inner,outer)}
        if is_slow:
            examples.append({**timing(r),"bounds":details,"matched_indices":links[r["index"]],
                             "immediate_predecessor":timing(ordered[r["index"]-64]) if r["index"]-64 in ordered else None})
    return datasets,examples


def backlog(rows, epoch, pauses, pad):
    if not rows:
        return {"slow_requests": 0}
    first = {}
    queue_long = exchange_long = short_exchange = 0
    for r in sorted(rows,key=lambda r:r["index"]):
        first.setdefault(r["worker"],r)
        queue_long += r["dequeued_ns"]-r["planned_ns"] >= 100_000_000
        exchange_long += r["completed_ns"]-r["send_start_ns"] >= 100_000_000
        short_exchange += r["completed_ns"]-r["send_start_ns"] < 50_000_000
    envelope = [epoch[0]+min(r["planned_ns"] for r in rows),epoch[1]+max(r["completed_ns"] for r in rows)]
    first_gc = min((a-pad for a,b in pauses),default=None)
    return {"slow_requests":len(rows),"slow_workers":len(first),"queue_at_least_100ms":queue_long,
            "exchange_at_least_100ms":exchange_long,"exchange_below_50ms":short_exchange,
            "first_slow_per_worker_exchange_at_least_100ms":sum(r["completed_ns"]-r["send_start_ns"] >= 100_000_000 for r in first.values()),
            "first_slow_per_worker_queue_below_50ms":sum(r["dequeued_ns"]-r["planned_ns"] < 50_000_000 for r in first.values()),
            "possible_slow_envelope_monotonic_ns":envelope,
            "first_gc_start_minus_latest_slow_end_ns":None if first_gc is None else first_gc-envelope[1]}


def evaluate(folder, mode):
    client = read(folder/"client.json")
    case = read(folder/"case.json")
    capture = read(folder/"capture.json")
    info = primary_info(capture)
    require(client["succeeded"] == client["offered"] == 180000 and client["failures"] == 0, "load accounting")
    require((client["clients"],client["target_rate"],client["read_percent"]) == (64,6000,95), "workload")
    trigger = [w["planned_second"] for w in client["windows"] if w["set"]["success_latency"]["p99_us"]>=100000]
    trace_path = folder/"client-trace.jsonl"
    c = trace_audit(trace_path,client,read(folder/"client-hist.json"),trigger,[])
    out = {"case":folder.name, "server_trace":mode, "primary_info_delta":info, "offered":client["offered"],
           "succeeded":client["succeeded"], "failures":client["failures"], "set_p99_us":client["set"]["success_latency"]["p99_us"],
           "get_p99_us":client["get"]["success_latency"]["p99_us"],"set_p999_us":client["set"]["success_latency"]["p99_9_us"],
           "set_max_us":client["set"]["success_latency"]["max_us"],"success_rate":client["actual_success_rate"],
           "trigger_seconds":trigger,"client_gc_count":client["client_num_gc_delta"],
           "client_gc_pause_ns":client["client_gc_pause_ns_delta"],"uniform":c["uniform"],"slow":c["exact_slow"],
           "retained":c["retained"],"drops":c["drops"]}
    raw = folder/"server-runtime.trace"
    require(raw.exists() == (mode=="ON"), "server trace mode mismatch")
    out["raw_sha256"] = {name:hashlib.sha256((folder/name).read_bytes()).hexdigest()
                         for name in ("client.json","client-hist.json","client-trace.jsonl","capture.json","case.json")}
    if mode == "OFF":
        return out,[]
    for name in ("server-runtime.trace","server-stw.jsonl"):
        out["raw_sha256"][name] = hashlib.sha256((folder/name).read_bytes()).hexdigest()
    meta,*observed = lines(folder/"server-stw.jsonl")
    require(meta["type"]=="meta", "trace metadata")
    proc_exit = next(e["monotonic_ns"] for e in case["events"] if e["name"]=="client_exited")
    proc_exit = min(proc_exit,capture["gc_info_after"]["start"]["monotonic_before_ns"])
    elapsed_ns = round(client["elapsed_seconds"]*1e9)
    physical = [case["client_release_interval"]["start_ns"],proc_exit-elapsed_ns+1]
    require(physical[0] <= physical[1], "inconsistent causal bracket")
    observed_epoch = case["measurement_alignment"]["projected_start_monotonic_ns"]
    near = [max(physical[0],observed_epoch[0]),min(physical[1],observed_epoch[1])]
    require(near[0] <= near[1], "inconsistent wall and causal brackets")
    anchors = meta["anchors"]
    offsets = [a["monotonic_ns"]-a["trace_ns"] for a in anchors]
    # The offset variation and the trace tick resolution are observed alignment sensitivity.
    pad = max(offsets)-min(offsets) + max(1000,meta.get("trace_tick_ns",0))
    coverage = (meta["coverage_start_monotonic_ns"]+pad,meta["coverage_end_monotonic_ns"]-pad)
    req_rows = lines(trace_path)[1:-1]
    measure = [physical[0],physical[1]+elapsed_ns]
    require(coverage[0] <= measure[0] and measure[1] <= coverage[1], "primary trace did not enclose measurement")
    pauses, reasons = [], defaultdict(list)
    for event in observed:
        if event["type"] != "stw":
            continue
        a,b = event["start_monotonic_ns"],event["end_monotonic_ns"]
        if a-pad <= measure[1] and b+pad >= measure[0]:
            reasons[event["reason"]].append(event["pause_ns"])
            if event["is_gc"]:
                pauses.append((a,b))
    outer = merged((a-pad,b+pad) for a,b in pauses)
    inner = merged((a+pad,b-pad) for a,b in pauses)
    values, examples = request_comparison(req_rows,{"causal":physical,"wall_5ms_sensitivity":near},inner,outer,coverage)
    out["slow_timing"] = backlog(examples,physical,pauses,pad)
    phases = {}
    for name, stats in meta["gc_phase_stats"].items():
        spans = [(r["observed_start_monotonic_ns"]-pad,r["observed_end_monotonic_ns"]+pad)
                 for r in observed if r["type"] == "gc_phase" and r["phase"] == name]
        require(len(spans) == stats["complete_count"]+stats["incomplete_count"], "phase count mismatch")
        phase_union = merged(spans)
        phases[name] = {"full_capture": stats, "slow_requests_with_possible_overlap":sum(
            overlap(physical[0]+r["planned_ns"],physical[1]+r["completed_ns"],phase_union)>0 for r in examples)}
    out["trace_analysis"] = {"anchors":len(anchors),"alignment_padding_ns":pad,"epoch_causal_ns":physical,
            "epoch_wall_5ms_ns":near,"inside_trace_monotonic_ns":coverage,
            "stw_in_possible_measurement":{reason:{"count":len(v),"sum_ns":sum(v),"max_ns":max(v)}
                                          for reason,v in sorted(reasons.items())},
            "gc_phase_exposure":phases,
            "gc_stw_count":len(pauses),"gc_stw_max_ns":max((b-a for a,b in pauses),default=0),
            "gc_stw_union_ns":sum(b-a for a,b in merged(pauses)), "overlap_by_group":values}
    return out,examples


def calibration(folder):
    meta,*events = lines(folder/"server-stw.jsonl")
    capture = read(folder/"capture.json")
    lo = capture["force_start"]["monotonic_before_ns"]
    hi = capture["force_end"]["monotonic_after_ns"]
    gc = [(r["start_monotonic_ns"],r["end_monotonic_ns"]) for r in events if r["type"] == "stw" and r["is_gc"]]
    offsets = [a["monotonic_ns"]-a["trace_ns"] for a in meta["anchors"]]
    pad = max(offsets)-min(offsets)+1000
    inside = sum(lo <= a-pad and b+pad <= hi for a,b in gc)
    before = [(a-pad,b+pad) for a,b in gc]
    control_start = meta["coverage_start_monotonic_ns"]+pad
    control_end = lo-pad
    negative = overlap(control_start,control_end,merged(before))
    require(inside > 0 and negative == 0 and control_end > control_start and primary_info(capture)["num_gc"] > 0,
            "positive or negative GC calibration failed")
    return {"primary_info_delta":primary_info(capture),"gc_stw_complete_inside_heap_request":inside,
            "pre_request_control_length_ns":control_end-control_start,"pre_request_overlap_ns":negative}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw",type=Path,required=True)
    parser.add_argument("--out",type=Path,required=True)
    parser.add_argument("--details",type=Path,required=True)
    args=parser.parse_args()
    matrix = ["OFF","ON","ON","OFF"]
    windows=[]
    evidence=[]
    for i,mode in enumerate(matrix,1):
        folder=args.raw/f"{i:02d}-r95-q6000-A"
        row, examples = evaluate(folder,mode)
        windows.append(row)
        evidence.extend(dict(case=folder.name,**e) for e in examples)
    args.details.parent.mkdir(parents=True,exist_ok=True)
    args.details.write_bytes(b"".join((json.dumps(row,sort_keys=True)+"\n").encode() for row in evidence))
    result = {"schema":1,"windows":windows,"calibration":calibration(args.raw/"calibration"),
              "slow_on_rows":len(evidence),"slow_on_detail_sha256":hashlib.sha256(args.details.read_bytes()).hexdigest(),
              "scope":"same-worker ordinary controls; GC STW includes rendezvous. Counter endpoints encompass the measured offer and drain. Wall sensitivity is separate from causal epoch bounds."}
    args.out.parent.mkdir(parents=True,exist_ok=True)
    args.out.write_bytes((json.dumps(result,indent=2,sort_keys=True)+"\n").encode())


if __name__=="__main__":
    main()
