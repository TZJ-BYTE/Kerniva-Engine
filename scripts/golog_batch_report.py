"""Reconcile a fixed-rate client window with the actual synchronous Go-log batches."""
import argparse
import json
from collections import Counter, defaultdict
from pathlib import Path

from golog_stall_trace_report import SLOW, mix, read, require, trace_audit


def records(path):
    with path.open(encoding="utf-8") as stream:
        for line in stream:
            yield json.loads(line)


def describe(values):
    ordered = sorted(values)
    if not ordered:
        return {}
    return {"p50": ordered[(len(ordered) - 1) // 2],
            "p99": ordered[(99 * len(ordered) + 99) // 100 - 1], "max": ordered[-1]}


def phases(row):
    b = row["batch"]
    points = [
        ("parsed", row["parsed_ns"]), ("dispatch_wait", row["dispatch_ns"]),
        ("execution_gate", row["execution_locked_ns"]), ("key_admission", row["key_locked_ns"]),
        ("prepare", row["queue_ns"]), ("batch_queue", b["dequeue_ns"]),
        ("writer_lock", b["locked_ns"]), ("batch_start", b["append_ns"]),
        ("frame_prepare", b["write_start_ns"]), ("write", b["write_end_ns"]),
        ("before_sync", b["sync_start_ns"]), ("sync", b["sync_end_ns"]),
        ("release", b["release_ns"]), ("caller_wakeup", row["result_ns"]),
        ("publish_and_encode", row["response_ready_ns"]), ("submit_reply", row["response_start_ns"]),
    ]
    require(row["response_ok"] and not b.get("failed") and 1 <= b["size"] <= 8
            and b["id"] > 0 and b["sequence"] > 0, "unsuccessful or incomplete batch")
    require(points[-1][1] <= row["response_end_ns"], "response clock order")
    result = {}
    for (_, left), (name, right) in zip(points, points[1:]):
        require(left > 0 and right >= left, f"server clock order at {name}")
        result[name] = right - left
    return result


def window(folder):
    client = read(folder / "client.json")
    capture = read(folder / "capture.json")
    trace = folder / "client-trace.jsonl"
    audit = trace_audit(trace, client, read(folder / "client-hist.json"), [], [])
    samples = [row for row in records(trace) if "index" in row]
    total, workers = client["offered"], client["clients"]
    require(client["failures"] == 0 and client["succeeded"] == total and all(
        client[op]["reconnect_attempts"] == 0 for op in ("get", "set")), "lost or reconnected client")
    result = {"window": folder.name, "gate_variant": capture["gate_arm"],
              "batch_trace": capture["batch_trace"], "requests": total, "failures": client["failures"],
              "success_rate": client["actual_success_rate"], "slow": audit["exact_slow"],
              "p99_us": {op: client[op]["success_latency"]["p99_us"] for op in ("get", "set")}}
    server_file = folder / "server-batches.jsonl"
    require(capture["pass"] and server_file.is_file() == capture["batch_trace"], "invalid capture receipt")
    if not server_file.exists():
        return result
    ports = {}
    retained = {}
    for row in samples:
        worker, port, ordinal = row["worker"], row["local_port"], row["conn_ordinal"]
        require(port > 0 and ports.setdefault(worker, port) == port, "worker switched connections")
        require(ordinal == row["index"] // workers + 1, "socket ordinal differs from schedule")
        retained[row["index"]] = row
    require(len(set(ports.values())) == len(ports) == workers, "incomplete connection census")
    by_port = {port: worker for worker, port in ports.items()}
    measured = {}
    observers = defaultdict(list)
    seen = 0
    summary = None
    for row in records(server_file):
        kind = row.get("type")
        require(summary is None, "data after server summary")
        if kind == "meta":
            require(seen == 0, "unexpected server header")
            continue
        if kind == "summary":
            summary = row
            continue
        seen += 1
        op = row["command"]
        worker = by_port.get(row["local_port"])
        if worker is None:
            if op in ("INFO", "SELECT") and row.get("execution_locked_ns"):
                observers[op].append(row)
            continue
        require(op == "SET", "measured socket had non-benchmark traffic")
        index = (row["conn_ordinal"] - 1) * workers + worker
        require(index not in measured and 0 <= index < total, "ambiguous socket ordinal")
        measured[index] = row
    require(summary and summary["drops"] == 0 and seen == summary["seen"] == summary["kept"],
            "incomplete server trace")
    expected = {i for i in range(total) if mix(i ^ client["seed"]) % 100 >= client["read_percent"]}
    require(expected == measured.keys() and len(expected) == client["set"]["succeeded"],
            "server batches differ from the client's complete SET schedule")

    timings, batches, sequences = defaultdict(list), {}, set()
    slow_sets, offsets = [], []
    for index in sorted(measured):
        row = measured[index]
        b = row["batch"]
        spans = phases(row)
        for name, value in spans.items():
            timings[name].append(value)
        signature = {k: v for k, v in b.items() if k != "sequence"}
        require(batches.setdefault(b["id"], signature) == signature, "inconsistent shared batch")
        require(b["sequence"] not in sequences, "repeated DB 0 commit sequence")
        sequences.add(b["sequence"])
        sample = retained.get(index)
        if sample is None:
            continue
        require(sample["op"] == "SET", "client/server operation mismatch")
        exchange = sample["completed_ns"] - sample["send_start_ns"]
        # Map server stamps to client elapsed time. Request parsing follows the
        # client send stamp, and reply submission precedes the completed read.
        offsets.append((sample["send_start_ns"] - row["parsed_ns"],
                        sample["completed_ns"] - row["response_start_ns"]))
        residual = exchange - sum(spans.values())
        require(residual >= 0, "server interval exceeds enclosing client exchange")
        total_ns = sample["completed_ns"] - sample["planned_ns"]
        if total_ns >= SLOW:
            slow_sets.append({"index": index, "worker": sample["worker"], "local_port": row["local_port"],
                              "ordinal": row["conn_ordinal"], "batch": b["id"], "size": b["size"],
                              "sequence": b["sequence"], "total_ns": total_ns,
                              "before_send_ns": total_ns - exchange, "exchange_ns": exchange,
                              "outside_server_ns": residual, "phases_ns": spans})
    require(len(slow_sets) == audit["exact_slow"]["set"], "unjoined slow SET")
    lo = max(a for a, _ in offsets)
    hi = min(b for _, b in offsets)
    require(lo <= hi, "no common client/server monotonic clock offset")
    all_slow = [r for r in samples if r["completed_ns"] - r["planned_ns"] >= SLOW]
    slow_observers = []
    for op, entries in observers.items():
        for row in entries:
            start, end = row["dispatch_ns"], row["execution_locked_ns"]
            if end - start < SLOW // 10 or end + hi < 0 or start + lo > client["duration_seconds"] * 10**9:
                continue
            slow_observers.append({
                "op": op, "port": row["local_port"], "ordinal": row["conn_ordinal"],
                "start_on_client_ns": [start + lo, start + hi], "admission_ns": end - start,
                "after_lock_to_reply_ready_ns": row["response_ready_ns"] - end,
                "slow_exchanges_definitely_intersecting": sum(
                    max(r["send_start_ns"], start + hi) < min(r["completed_ns"], end + lo)
                    for r in all_slow)})
    slow_observers.sort(key=lambda row: row["start_on_client_ns"])
    result.update(joined_sets=len(measured), joined_client_samples=len(offsets), batches=len(batches),
                  group_sizes=dict(sorted(Counter(b["size"] for b in batches.values()).items())),
                  phases_ns={k: describe(v) for k, v in timings.items()},
                  client_minus_server_clock_bounds_ns=[lo, hi], slow_observers=slow_observers,
                  observer_gate_ns={op: {"count": len(rows), **describe(
                      [r["execution_locked_ns"] - r["dispatch_ns"] for r in rows])}
                                    for op, rows in observers.items()},
                  earliest_slow_sets=slow_sets[:3],
                  longest_exchange=max(slow_sets, key=lambda r: r["exchange_ns"], default=None))
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("windows", type=Path, nargs="+")
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    results = [window(path) for path in args.windows]
    args.out.write_text(json.dumps(results, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
