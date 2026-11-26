"""Read one DB0 fsync with switch-out stacks and same-device JBD2 tracepoints."""
import argparse
import json
from pathlib import Path
import re

from golog_batch_report import require
from golog_kernel_writer_report import (
    SWITCH, TRACE_COUNTS, TRACE_LINE, batch_inputs, match_batches, read, residuals,
    scheduler_spans, split_trace, verified_fd, verified_run,
)


FRAME = re.compile(r"^\s*=>\s+(\S+)")
JOURNAL_EVENT = re.compile(r"^(jbd2_\w+):\s+dev\s+(\d+,\d+)(.*)$")
TRANSACTION = re.compile(r"\btransaction\s+(\d+)\b")
COMMIT = ("jbd2_start_commit", "jbd2_commit_locking", "jbd2_commit_flushing",
          "jbd2_commit_logging", "jbd2_end_commit")
WAIT_COMMIT = {"jbd2_log_wait_commit"}
WAIT_WRITEBACK = {
    "filemap_fdatawait_range", "filemap_fdatawait_range_keep_errors", "__filemap_fdatawait_range",
    "wait_on_page_writeback", "folio_wait_writeback",
}


def parse_enriched(path):
    rows, counts = [], []
    with path.open(encoding="utf-8") as stream:
        for line in stream:
            if line.startswith("#"):
                if match := TRACE_COUNTS.search(line):
                    counts.append(tuple(map(int, match.groups())))
                continue
            if not line.strip():
                continue
            if match := FRAME.match(line):
                require(rows and "stack" in rows[-1], f"{path}: orphan frame")
                rows[-1]["stack"].append(match[1])
                continue
            match = TRACE_LINE.match(line)
            require(match is not None, f"{path}: unrecognized trace record {line.rstrip()}")
            comm, tid, cpu, sec, fraction, payload = match.groups()
            precision_ns = 10 ** (9 - len(fraction))
            stamp = int(sec) * 10**9 + int(fraction) * precision_ns
            require(not rows or rows[-1]["ns"] <= stamp, f"{path}: timestamp order")
            row = {"comm": comm, "tid": int(tid), "cpu": int(cpu), "ns": stamp,
                   "resolution_ns": precision_ns, "payload": payload.rstrip()}
            if row["payload"] in ("<stack trace>", "kernel_stack: <stack trace>"):
                row["stack"] = []
            rows.append(row)
    require(counts == [(len(rows), len(rows))], f"{path}: trace header counts {counts} vs {len(rows)}")
    return rows


def stack_pairs(rows):
    previous_by_cpu = {}
    pairs, unmatched = {}, []
    ambiguous = set()
    for row in rows:
        cpu = row["cpu"]
        previous = previous_by_cpu.get(cpu)
        if "stack" in row:
            switch = SWITCH.search(previous["payload"]) if previous and previous["payload"].startswith("sched_switch:") else None
            key = None
            if switch is not None:
                previous_tid, state, _ = switch.groups()
                if int(previous_tid) == previous["tid"] == row["tid"] and state.startswith("D"):
                    key = row["tid"], previous["ns"]
            frame_symbols = {symbol(frame) for frame in row["stack"]}
            if key is None or key in ambiguous or not (frame_symbols & {
                "trace_event_raw_event_sched_switch", "__traceiter_sched_switch",
            }):
                unmatched.append(row)
            elif key in pairs:
                ambiguous.add(key)
                unmatched.extend((pairs.pop(key), row))
            else:
                pairs[key] = {"tid": row["tid"], "cpu": cpu, "switch_ns": previous["ns"],
                              "stack_ns": row["ns"], "frames": row["stack"]}
        previous_by_cpu[cpu] = row
    return pairs, unmatched


def symbol(frame):
    # Remove the offset and compiler-generated symbol suffix after the function.
    return frame.split("+", 1)[0].split(".", 1)[0]


def classify_stack(frames):
    functions = {symbol(frame) for frame in frames}
    if "ext4_sync_file" not in functions:
        return "ext4_sync_file_not_observed_in_stack"
    commit = bool(functions & WAIT_COMMIT)
    writeback = bool(functions & WAIT_WRITEBACK)
    if commit and writeback:
        return "multiple_wait_paths_in_stack"
    if commit:
        return "journal_commit_wait"
    if writeback:
        return "file_writeback_wait"
    return "ext4_sleep_unclassified"


def journal_events(rows, device):
    events = []
    for row in rows:
        match = JOURNAL_EVENT.fullmatch(row["payload"])
        if match is None:
            continue
        event, dev, details = match.groups()
        if dev != device:
            continue
        txn = TRANSACTION.search(details)
        events.append({"event": event, "device": dev, "transaction": int(txn[1]) if txn else None,
                       "ns": row["ns"], "tid": row["tid"], "cpu": row["cpu"], "details": details.strip()})
    return events


def journal_transactions(events):
    groups = {}
    for event in events:
        if event["transaction"] is not None and event["event"] in COMMIT:
            groups.setdefault((event["device"], event["transaction"]), []).append(event)
    transactions = []
    for (device, txn), stages in groups.items():
        names = tuple(event["event"] for event in stages)
        tids = {event["tid"] for event in stages}
        complete = names == COMMIT and len(tids) == 1
        transactions.append({"device": device, "transaction": txn, "phases": stages,
                             "complete_ordered_single_tid": complete,
                             "start_ns": stages[0]["ns"] if names[0] == COMMIT[0] else None,
                             "end_ns": stages[-1]["ns"] if names[-1] == COMMIT[-1] else None})
    return transactions


def episode_report(episode, tid, pairs, transactions):
    out, wake, resumed = (episode[key] for key in ("out_ns", "wake_ns", "in_ns"))
    stack = pairs.get((tid, out))
    classified = (classify_stack(stack["frames"]) if stack and wake is not None
                  and out <= stack["stack_ns"] <= wake else "unclassified_missing_or_unbounded_stack")
    overlaps = [
        {"device": txn["device"], "transaction": txn["transaction"],
         "overlap_ns": min(wake, txn["end_ns"]) - max(out, txn["start_ns"])}
        for txn in transactions
        if wake is not None and txn["complete_ordered_single_tid"]
        and txn["start_ns"] < wake and out < txn["end_ns"]
    ]
    return {**episode, "d_state_ns": None if wake is None else wake - out,
            "wakeup_to_switch_in_ns": None if wake is None else resumed - wake,
            "classification": classified, "stack": stack, "overlapping_complete_transactions": overlaps}

def analyze(raw):
    run, names, verified_files = verified_run(raw)
    require(run["stage"] == "single" and names == ["single-write"], "expected single unmodified DB0 SET")
    folder = raw / names[0]
    capture = read(folder / "capture.json")
    require(not capture.get("strace_delay_exit_ms"), "expected unmodified fsync return")
    receipt = read(folder / "kernel-receipt.json")
    require("[mono]" in receipt["clock"] and receipt["instance_removed"]
            and all(receipt["loss"][key] == 0 for key in ("overrun", "commit overrun", "dropped events")),
            "kernel receipt does not establish a lossless private monotonic trace")
    allowed = {int(value) for value in receipt["thread_map"].values()}
    trigger = receipt["journal_stack_trigger"]
    require(trigger.startswith("stacktrace:unlimited if ") and trigger.endswith(" && prev_state == 2")
            and {int(tid) for tid in re.findall(r"prev_pid == (\d+)", trigger)} == allowed,
            "missing D-state stack trigger for mapped server threads")
    batches, origin, _, _ = batch_inputs(folder, capture)
    rows = parse_enriched(folder / "kernel-trace.txt")
    calls, sched, _, _, pairing, trace_count = split_trace(rows, allowed)
    require(len(batches) == len(calls) == 1 and not any(pairing.values()), "single syscall census failed")
    call, batch = calls[0], batches[0]
    candidates, narrowed, error, _ = match_batches(batches, calls, origin)
    require(error is None and list(candidates[0]) == [0] and call["tid"] in allowed,
            "single DB0 fsync timing did not match")
    start, stop = receipt["trace_start_ns"], receipt["trace_stop_ns"]
    require(start[0] <= start[1] <= batch["sync_start_ns"] + narrowed[0]
            and batch["sync_end_ns"] + narrowed[1] <= stop[0] <= stop[1], "trace did not cover single SET")

    observations = receipt["fd_observations"]
    times = [obs["guest_mono_before_ns"] for obs in observations]
    require(times == sorted(times) and all(obs["guest_mono_before_ns"] <= obs["guest_mono_after_ns"]
                                          for obs in observations), "invalid FD observation brackets")
    fd_evidence = verified_fd(call, observations, times)
    require(fd_evidence["ext4_validation"] == "inode_and_device_match", "DB0 FD and ext4 inode do not match")
    fs_enter, fs_exit = call["ext4"]
    device = fs_enter["device"]
    major, minor = map(int, device.split(","))
    require(all(receipt["filters"][f"jbd2/{name}"] == f"dev == {(major << 20) | minor}" for name in COMMIT),
            "journal trace filters differ from the active file ext4 device")

    thread_timeline = sched[call["tid"]]
    timing = scheduler_spans(call, thread_timeline, [row[0] for row in thread_timeline])
    require(not timing["warnings"], f"incomplete scheduler intervals: {timing['warnings']}")
    pairs, unmatched_stacks = stack_pairs(rows)
    journal = journal_events(rows, device)
    transactions = journal_transactions(journal)
    episodes = []
    for item in timing["episodes"]:
        if not item["out_state"].startswith("D"):
            continue
        episode = episode_report(item, call["tid"], pairs, transactions)
        if not fs_enter["ns"] < item["out_ns"] <= item["in_ns"] < fs_exit["ns"]:
            episode["classification"] = "unclassified_ext4_envelope"
        episodes.append(episode)
    matched_keys = {(call["tid"], item["out_ns"]) for item in episodes}
    return {
        "schema": 1, "verified_files": verified_files, "trace_entries": trace_count, "trace_clock": receipt["clock"],
        "trace_loss": receipt["loss"], "trace_trigger": trigger,
        "batch_id": batch["id"], "user_sync_ns": batch["sync_end_ns"] - batch["sync_start_ns"],
        "kernel_syscall_ns": call["exit_ns"] - call["enter_ns"], "syscall": call,
        "ext4_sync_ns": fs_exit["ns"] - fs_enter["ns"], "fd_evidence": fd_evidence,
        "residuals": residuals(batch, call, narrowed), "scheduler": timing, "d_episodes": episodes,
        "unpaired_stack_headers": unmatched_stacks,
        "stacks_outside_measured_d_episodes": [stack for key, stack in pairs.items() if key not in matched_keys],
        "journal_transactions": transactions,
        "journal_events_without_transaction_id": [event for event in journal if event["transaction"] is None],
        "interpretation": "Stack classifications apply only to the bracketed DB0 fsync on this thread. "
                          "Commit phases are grouped by observed device and transaction ID. The writer stack has no "
                          "transaction ID, so overlap identifies temporal context and does not establish transaction "
                          "identity for the writer or identify a physical disk request. Durations use rounded "
                          "timestamps from the trace text.",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()
    result = analyze(args.raw)
    args.out.write_text(json.dumps(result, indent=2, allow_nan=False) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
