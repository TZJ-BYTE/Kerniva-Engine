"""Summarize JBD2 phase overlap from the compact formal journal reports."""
import argparse
from collections import Counter
import json
from pathlib import Path


PHASES = ("jbd2_start_commit", "jbd2_commit_locking", "jbd2_commit_flushing",
          "jbd2_commit_logging", "jbd2_end_commit")
PHASE_INTERVALS = ("start_to_locking", "locking_to_flushing", "flushing_to_logging", "logging_to_end")


def union_duration(spans):
    total = 0
    previous_end = -1
    for begin, end in sorted(spans):
        total += max(0, end - max(begin, previous_end))
        previous_end = max(previous_end, end)
    return total


def phase_relation(episode, transaction):
    out, wake = episode["out_ns"], episode["wake_ns"]
    stages = transaction["phases"]
    if tuple(stage["event"] for stage in stages) != PHASES:
        raise ValueError("unexpected JBD2 phase sequence")
    times = [stage["ns"] for stage in stages]
    if times != sorted(times):
        raise ValueError("unordered JBD2 phase timestamps")
    start, locking, flushing, logging, end = times
    if (start, end) != (transaction["start_ns"], transaction["end_ns"]):
        raise ValueError("JBD2 phase timestamps disagree with transaction endpoints")
    return {
        "transaction": transaction["transaction"], "device": transaction["device"],
        "journal_tid": stages[0]["tid"],
        "start_minus_d_start_ns": start - out, "logging_minus_d_start_ns": logging - out,
        "end_minus_wakeup_ns": end - wake, "transaction_ns": end - start,
        "commit_overlap_ns": min(wake, end) - max(out, start),
        "phase_elapsed_ns": {"start_to_locking": locking - start, "locking_to_flushing": flushing - locking,
                             "flushing_to_logging": logging - flushing, "logging_to_end": end - logging},
        "logging_to_end_overlap_ns": max(0, min(wake, end) - max(out, logging)),
    }


def episodes(path):
    report = json.loads(path.read_bytes())
    for window in report["windows"]:
        if window["trace_mode"] != "journal" or not window["trace"]["lossless"]:
            continue
        transactions = {(row["device"], row["transaction"]): row
                        for row in window["journal"].get("overlapping_complete_transactions", [])
                        if row["complete_ordered_single_tid"]}
        for batch in window["slow_batches"]:
            device = batch["syscall"]["ext4"][0]["device"]
            for episode in batch["d_episodes"]:
                if episode["classification"] != "journal_commit_wait":
                    continue
                out, wake = episode["out_ns"], episode["wake_ns"]
                if wake - out != episode["d_state_ns"] or batch["scheduler_warnings"]:
                    raise ValueError("incomplete writer D-state accounting")
                listed = {(row["device"], row["transaction"]): row["overlap_ns"]
                          for row in episode["overlapping_complete_transactions"]}
                actual = {key: txn for key, txn in transactions.items() if key[0] == device
                          and out < txn["end_ns"] and txn["start_ns"] < wake}
                relations = [phase_relation(episode, txn) for txn in actual.values()]
                if listed != {(row["device"], row["transaction"]): row["commit_overlap_ns"] for row in relations}:
                    raise ValueError("recomputed overlap disagrees with the frozen report")
                covered = union_duration((max(out, txn["start_ns"]), min(wake, txn["end_ns"]))
                                         for txn in actual.values())
                phase_overlap = {
                    phase: union_duration((max(out, txn["phases"][i]["ns"]), min(wake, txn["phases"][i + 1]["ns"]))
                                          for txn in actual.values())
                    for i, phase in enumerate(PHASE_INTERVALS)
                }
                yield {
                    "report": path.name, "window": window["folder"], "batch_id": batch["batch_id"],
                    "device": device, "writer_tid": batch["syscall"]["tid"],
                    "user_sync_ns": batch["user_sync_ns"], "kernel_syscall_ns": batch["kernel_syscall_ns"],
                    "out_ns": out, "wakeup_ns": wake, "journal_wait_d_ns": wake - out,
                    "complete_transaction_overlap_ns": covered,
                    "complete_transaction_logging_to_end_overlap_ns": phase_overlap["logging_to_end"],
                    "phase_overlap_ns": phase_overlap,
                    "outside_complete_transactions_ns": wake - out - covered,
                    "overlapping_complete_transactions": relations,
                }


def summarize(paths):
    rows = [row for path in paths for row in episodes(path)]
    categories = {}
    timing = Counter()
    phase_totals = Counter()
    for row in rows:
        phase_totals.update(row["phase_overlap_ns"])
        transactions = row["overlapping_complete_transactions"]
        count = len(transactions)
        label = "zero" if not count else "one" if count == 1 else "multiple"
        category = categories.setdefault(label, Counter())
        category["episodes"] += 1
        for key in ("journal_wait_d_ns", "complete_transaction_overlap_ns", "complete_transaction_logging_to_end_overlap_ns",
                    "outside_complete_transactions_ns"):
            category[key] += row[key]
        for transaction in transactions:
            timing[("start_at_or_after_d_start" if transaction["start_minus_d_start_ns"] >= 0
                    else "start_before_d_start")] += 1
            timing[("end_at_or_before_wakeup" if transaction["end_minus_wakeup_ns"] <= 0
                    else "end_after_wakeup")] += 1
    ranked = sorted((row for row in rows if len(row["overlapping_complete_transactions"]) == 1),
                    key=lambda item: -item["user_sync_ns"])
    return {
        "schema": 1, "source_reports": [path.name for path in paths], "episodes": len(rows),
        "by_complete_transaction_count": {label: dict(categories.get(label, {"episodes": 0}))
                                          for label in ("zero", "one", "multiple")},
        "overlapping_transaction_order_counts": dict(timing),
        "phase_overlap_totals_ns": dict(phase_totals),
        "longest_two_single_overlap_examples": ranked[:2],
        "episodes_in_scope": rows,
        "method": "Intervals use the frozen monotonic trace timestamps. Overlapping transaction spans are unioned inside each writer D interval. "
                  "JBD2 logging-to-end means elapsed time between stage markers, which can include other work and waiting. "
                  "Stage totals are unioned separately and can overlap across transactions. Only complete transactions retained in "
                  "the supplied reports are considered. The writer stacks lack transaction IDs.",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("reports", nargs="+", type=Path)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()
    args.out.write_text(json.dumps(summarize(args.reports), indent=2, allow_nan=False) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
