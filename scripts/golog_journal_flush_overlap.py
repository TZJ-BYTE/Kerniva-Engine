"""Reconcile zero-length FF block observations against saved JBD2 D intervals."""
import argparse
from collections import Counter, defaultdict
from hashlib import sha256
import json
from pathlib import Path
import re

from golog_journal_phase_summary import union_duration


BLOCK = re.compile(
    rb"\s(\d+)\.(\d{6,9}): (block_rq_issue|block_rq_complete): "
    rb"(\d+,\d+) (\S+) (?:\d+ )?\([^)]*\) (\d+) \+ (\d+)(.*?)\r?\n?$"
)
ERROR = re.compile(rb"\[(-?\d+)\]$")
REQUEUE = re.compile(rb": block_rq_requeue: (\d+,\d+) ")
UINT64_MAX = (1 << 64) - 1


def block_event(line):
    match = BLOCK.search(line)
    if match is None:
        raise ValueError(f"unrecognized block event: {line!r}")
    sec, fraction, event, device, flags, sector, length, suffix = match.groups()
    stamp = int(sec) * 10**9 + int(fraction) * 10**(9 - len(fraction))
    if event == b"block_rq_complete":
        error = ERROR.search(suffix.strip())
        result = int(error[1]) if error else None
    else:
        result = None
    return stamp, event.endswith(b"issue"), device.decode("ascii"), flags, int(sector), int(length), result


def flush_observations(lines, devices, digest=None):
    pending, matches, uncertain = {}, [], []
    other = Counter()
    census = Counter()
    requeue_devices = set()
    for line_number, line in enumerate(lines, 1):
        if digest is not None:
            digest.update(line)
        if b": block_rq_" not in line:
            continue
        if b": block_rq_requeue:" in line:
            match = REQUEUE.search(line)
            if match is None:
                raise ValueError(f"unrecognized block requeue event: {line!r}")
            device = match[1].decode("ascii")
            if device in devices:
                census["observed_device_requeues"] += 1
                requeue_devices.add(device)
            continue
        if b": block_rq_issue:" not in line and b": block_rq_complete:" not in line:
            continue
        stamp, issue, device, flags, sector, length, result = block_event(line)
        if device not in devices:
            continue
        if flags != b"FF" or length != 0:
            other[device, "issue" if issue else "completion"] += 1
            continue
        census["ff_issues" if issue else "ff_completions"] += 1
        # The issue uses sector 0 and completion can use the sentinel
        # BLK_RQ_POS_END. Both are rendered for zero-sector FF on this device.
        if not issue and sector == UINT64_MAX:
            sector = 0
        key = device, flags.decode("ascii"), sector, length
        state = pending.get(key)
        event = {"ns": stamp, "result": result, "line": line_number}
        if issue:
            if state is None:
                pending[key] = {
                    "issues": [event], "completions": [], "outstanding": 1,
                    "other_issues_at_first_issue": other[device, "issue"],
                    "other_completions_at_first_issue": other[device, "completion"],
                }
            else:
                state["issues"].append(event)
                state["outstanding"] += 1
            continue
        if state is None:
            uncertain.append({"key": key, "start_ns": None, "end_ns": stamp,
                              "issues": 0, "completions": 1, "reason": "completion_without_observed_issue"})
            continue
        state["completions"].append(event)
        state["outstanding"] -= 1
        if state["outstanding"]:
            continue
        del pending[key]
        begin = state["issues"][0]["ns"]
        if len(state["issues"]) != 1 or result != 0 or stamp <= begin:
            reason = ("overlapping_identical_descriptions" if len(state["issues"]) != 1 else
                      "missing_or_failed_completion_status" if result != 0 else "unresolved_clock_order")
            uncertain.append({"key": key, "start_ns": min(begin, stamp), "end_ns": max(begin, stamp),
                              "issues": len(state["issues"]), "completions": len(state["completions"]),
                              "reason": reason})
            continue
        matches.append({
            "key": key, "issue_ns": begin, "complete_ns": stamp, "interval_ns": stamp - begin,
            "issue_line": state["issues"][0]["line"], "complete_line": line_number,
            "interleaved_other_issues": other[device, "issue"] - state["other_issues_at_first_issue"],
            "interleaved_other_completions": other[device, "completion"] - state["other_completions_at_first_issue"],
        })
    for key, state in pending.items():
        uncertain.append({"key": key, "start_ns": state["issues"][0]["ns"], "end_ns": None,
                          "issues": len(state["issues"]), "completions": len(state["completions"]),
                          "reason": "incomplete_at_trace_end"})
    if requeue_devices:
        # A printed requeue lacks a request ID. It can invalidate descriptor
        # pairing on the device even if an issue and completion look unique.
        for pair in matches:
            if pair["key"][0] in requeue_devices:
                uncertain.append({"key": pair["key"], "start_ns": pair["issue_ns"], "end_ns": pair["complete_ns"],
                                  "issues": 1, "completions": 1, "reason": "device_requeue_cannot_be_excluded"})
        matches = [pair for pair in matches if pair["key"][0] not in requeue_devices]
    return matches, uncertain, dict(census)


def relate(episode, pairs, uncertain):
    out, wake, device = episode["out_ns"], episode["wakeup_ns"], episode["device"]
    overlap = [row for row in pairs if row["key"][0] == device and row["issue_ns"] <= wake and out <= row["complete_ns"]]
    contained = [row for row in overlap if out < row["issue_ns"] and row["complete_ns"] < wake]
    possible_uncertain = [row for row in uncertain if row["key"][0] == device
                          and (row["start_ns"] is None or row["start_ns"] <= wake)
                          and (row["end_ns"] is None or out <= row["end_ns"])]
    return {
        "report": episode["report"], "window": episode["window"], "batch_id": episode["batch_id"], "device": device,
        "user_sync_ns": episode["user_sync_ns"], "journal_wait_d_ns": wake - out, "out_ns": out, "wakeup_ns": wake,
        "fully_contained_ff_intervals": contained,
        "overlapping_or_boundary_ff_intervals": [row for row in overlap if row not in contained],
        "contained_issue_to_complete_coverage_ns": union_duration((row["issue_ns"], row["complete_ns"])
                                                                  for row in contained),
        "all_overlap_coverage_ns": union_duration((max(row["issue_ns"], out), min(row["complete_ns"], wake))
                                                   for row in overlap),
        "uncertain_ff_groups_may_overlap": possible_uncertain,
    }


def summarize(root, phase):
    by_window = defaultdict(list)
    for row in phase["episodes_in_scope"]:
        by_window[row["report"].removesuffix("-analysis.json"), row["window"]].append(row)
    rows, windows = [], []
    for (stage, window), episodes in by_window.items():
        relative = f"{window}/kernel-trace.txt"
        trace = root / stage / relative
        manifest = json.loads((root / stage / "ext4-readback.json").read_bytes())
        if manifest["driver_exit_code"] != 0:
            raise ValueError(f"trace capture did not complete: {trace}")
        receipt = json.loads(trace.with_name("kernel-receipt.json").read_bytes())
        devices = {row["device"] for row in episodes}
        loss = receipt["loss"]
        block_filters = {name: expression for name, expression in receipt["filters"].items()
                         if name.startswith("block/")}
        if ("[mono]" not in receipt["clock"] or not loss or any(loss.values()) or
                not {"block/block_rq_issue", "block/block_rq_complete"}.issubset(block_filters)):
            raise ValueError(f"trace clock, loss or block event coverage failed validation: {trace}")
        digest = sha256()
        with trace.open("rb") as stream:
            pairs, uncertain, census = flush_observations(stream, devices, digest)
        if digest.hexdigest() != manifest["raw_sha256"][relative]:
            raise ValueError(f"trace readback SHA-256 mismatch: {trace}")
        windows.append({"raw_trace": f"{stage}/{relative}", "raw_sha256": digest.hexdigest(), **census,
                        "loss": loss, "block_filters": block_filters,
                        "singleton_successful_pairs": len(pairs), "uncertain_groups": len(uncertain),
                        "uncertain_group_reasons": dict(Counter(group["reason"] for group in uncertain))})
        rows.extend(relate(episode, pairs, uncertain) for episode in episodes)
    selected = [row for row in rows if row["fully_contained_ff_intervals"]]
    flagged = [row for row in rows if row["uncertain_ff_groups_may_overlap"]]
    return {
        "schema": 1, "windows": windows, "journal_d_episodes": len(rows),
        "episodes_with_contained_pairs": len(selected), "episodes_with_any_temporal_pair": sum(
            bool(row["fully_contained_ff_intervals"] or row["overlapping_or_boundary_ff_intervals"]) for row in rows),
        "episodes_with_contained_pairs_and_no_uncertain_group": sum(
            not row["uncertain_ff_groups_may_overlap"] for row in selected),
        "episodes_with_potentially_overlapping_uncertain_groups": len(flagged),
        "singleton_contained_pair_occurrences": sum(len(row["fully_contained_ff_intervals"]) for row in rows),
        "journal_wait_d_ns": sum(row["journal_wait_d_ns"] for row in rows),
        "contained_issue_to_complete_coverage_ns": sum(row["contained_issue_to_complete_coverage_ns"] for row in rows),
        "all_overlap_coverage_ns": sum(row["all_overlap_coverage_ns"] for row in rows),
        "largest_measured_sync_example": max(rows, key=lambda row: row["user_sync_ns"], default=None),
        "episodes": rows,
        "interpretation": "A successful singleton is inferred from one outstanding observed issue with the same device, FF flags, "
                          "zero length and compatible sector. The trace prints no request ID and the receipts list enabled block "
                          "tracepoints. Hidden retries or missing records remain possible. Concurrent identical descriptions, incomplete "
                          "endpoints, unresolved timestamp order and unsuccessful completions are uncertain. Non-FF on-device issue/complete "
                          "events may interleave. Time coverage uses tracefs displayed timestamps and a union within each D episode. "
                          "The measured intervals describe the shared guest block device without writer, transaction or physical request identity.",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--phase", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    result = summarize(args.root, json.loads(args.phase.read_bytes()))
    args.out.write_text(json.dumps(result, indent=2, allow_nan=False) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
