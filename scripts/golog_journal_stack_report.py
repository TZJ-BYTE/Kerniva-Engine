"""Read DB0 fsync switch-out stacks and same-device JBD2 tracepoints."""
import argparse
from collections import Counter
import json
from pathlib import Path
import re

from golog_batch_report import phases, records, require
from golog_kernel_writer_report import (
    SWITCH, TRACE_COUNTS, TRACE_LINE, WAKE, batch_inputs, match_batches, read, residuals,
    reverse_batch_candidates, scheduler_spans, split_trace, verified_fd, verified_run,
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


def parse_enriched(path, stats=None):
    row, count, counts = None, 0, []
    with path.open(encoding="utf-8") as stream:
        for line in stream:
            if line.startswith("#"):
                if match := TRACE_COUNTS.search(line):
                    counts.append(tuple(map(int, match.groups())))
                continue
            if not line.strip():
                continue
            if match := FRAME.match(line):
                require(row and "stack" in row, f"{path}: orphan frame")
                row["stack"].append(match[1])
                continue
            match = TRACE_LINE.match(line)
            if match is None:
                raise ValueError(f"{path}: unrecognized trace record {line.rstrip()}")
            comm, tid, cpu, sec, fraction, payload = match.groups()
            precision_ns = 10 ** (9 - len(fraction))
            stamp = int(sec) * 10**9 + int(fraction) * precision_ns
            if row is not None:
                require(row["ns"] <= stamp, f"{path}: timestamp order")
                count += 1
                yield row
            row = {"comm": comm, "tid": int(tid), "cpu": int(cpu), "ns": stamp,
                   "resolution_ns": precision_ns, "payload": payload.rstrip()}
            if row["payload"] in ("<stack trace>", "kernel_stack: <stack trace>"):
                row["stack"] = []
        if row is not None:
            count += 1
            yield row
    require(len(counts) == 1 and counts[0][0] == count and counts[0][1] >= count,
            f"{path}: trace header counts {counts} vs {count}")
    if stats is None:
        require(counts[0][1] == count, f"{path}: trace header reports missing entries {counts}")
    else:
        stats.update(retained=count, written=counts[0][1], absent_from_ring=counts[0][1] - count)


def stack_pairs(rows):
    previous_by_cpu = {}
    pairs, unmatched = {}, []
    ambiguous = set()
    interval = None
    for row in rows:
        if row.get("selected_interval") != interval:
            previous_by_cpu.clear()
            interval = row.get("selected_interval")
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
    classes = []
    if functions & WAIT_COMMIT:
        classes.append("journal_commit_wait")
    if functions & WAIT_WRITEBACK:
        classes.append("file_writeback_wait")
    if functions & {"blk_mq_get_tag", "blk_mq_get_driver_tag"}:
        classes.append("block_queue_tag_wait")
    if {"submit_bio_wait", "wait_for_completion_io"} <= functions:
        classes.append("block_io_completion_wait")
    if len(classes) > 1:
        return "multiple_wait_paths_in_stack"
    return classes[0] if classes else "ext4_sleep_unclassified"


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

def analyze_single(raw, run, names, verified_files):
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
    rows = list(parse_enriched(folder / "kernel-trace.txt"))
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



SLOW_NS = 10_000_000
TAIL_NS = 100_000_000
WAIT_CLASSES = {"journal_commit_wait", "file_writeback_wait", "block_queue_tag_wait", "block_io_completion_wait"}


def scan_syscalls(path, allowed, stats):
    prefixes = ("sys_fsync", "sys_fdatasync", "ext4_sync_file_", "sched_process_fork:")
    counts = Counter()

    def candidates():
        for row in parse_enriched(path, stats):
            if "stack" in row:
                counts["stack_headers"] += 1
            elif row["payload"].startswith("jbd2_"):
                counts[row["payload"].split(":", 1)[0]] += 1
            elif row["payload"].startswith(prefixes):
                yield row

    calls, _, _, forks, pairing, _ = split_trace(candidates(), allowed)
    return calls, forks, pairing, dict(counts)


def selected_trace(path, selected, device):
    """Keep target-thread scheduling and nearby stacks; stream all other events past."""
    windows = iter(enumerate(sorted(selected, key=lambda call: call["enter_ns"])))
    current = next(windows, None)
    scheduler_rows, journal_rows = [], []
    stats = {}

    def scoped_records():
        nonlocal current
        for row in parse_enriched(path, stats):
            # Journal start/end may straddle a selected syscall boundary.
            if row["payload"].startswith("jbd2_"):
                journal_rows.append(row)
            stamp = row["ns"]
            while current is not None and stamp > current[1]["exit_ns"]:
                current = next(windows, None)
            if current is None or stamp < current[1]["enter_ns"]:
                continue
            index, call = current
            payload = row["payload"]
            if payload.startswith("sched_switch:"):
                switch = SWITCH.search(payload)
                require(switch is not None, f"invalid sched_switch line: {payload}")
                prev, _, next_tid = switch.groups()
                if call["tid"] in (int(prev), int(next_tid)):
                    scheduler_rows.append(row)
            elif payload.startswith(("sched_wakeup:", "sched_wakeup_new:")):
                wake = WAKE.search(payload)
                require(wake is not None, f"invalid sched_wakeup line: {payload}")
                if int(wake[1]) == call["tid"]:
                    scheduler_rows.append(row)
            row["selected_interval"] = index
            yield row

    pairs, unpaired = stack_pairs(scoped_records())
    _, timelines, _, _, _, _ = split_trace(scheduler_rows, {call["tid"] for call in selected})
    events = journal_events(journal_rows, device)
    transactions = journal_transactions(events)
    return timelines, pairs, unpaired, transactions, sum(event["transaction"] is None for event in events), stats


def ring_usage(receipt):
    buffer_kib = int(receipt["buffer_per_cpu_kb"])
    require(buffer_kib > 0, "invalid configured trace ring buffer size")
    cpus = []
    for cpu, raw in receipt["cpu_stats"].items():
        fields = {key.strip(): int(value.strip()) for key, value in (row.split(":", 1)
                  for row in raw.splitlines() if ":" in row) if value.strip().isdigit()}
        cpus.append({"cpu": cpu, "retained_bytes": fields["bytes"], "entries": fields["entries"],
                     "fraction_of_buffer": fields["bytes"] / (1024 * buffer_kib),
                     "overrun": fields["overrun"], "commit_overrun": fields["commit overrun"],
                     "dropped_events": fields["dropped events"]})
    return {"configured_kib_per_cpu": buffer_kib, "cpus": len(cpus),
            "hot_cpus": sorted(cpus, key=lambda row: (-row["retained_bytes"], row["cpu"]))[:5]}


def measure_slow_calls(batches, calls, all_batches, initial_origin, solve, receipt):
    matches, narrowed, error, calibration = solve
    observations = receipt["fd_observations"]
    times = [row["guest_mono_before_ns"] for row in observations]
    require(times == sorted(times) and all(row["guest_mono_before_ns"] <= row["guest_mono_after_ns"]
                                          for row in observations), "invalid FD observations")
    start, stop = receipt["trace_start_ns"], receipt["trace_stop_ns"]
    require(start[0] <= start[1] <= stop[0] <= stop[1], "invalid trace lifecycle clock")
    skipped, selected = [], []
    for batch, options in zip(batches, matches):
        begin, end = batch["sync_start_ns"], batch["sync_end_ns"]
        if end - begin < SLOW_NS:
            continue
        entry = {"batch_id": batch["id"], "user_sync_ns": end - begin}
        if error or len(options) != 1:
            skipped.append({**entry, "reason": error or ("no_candidate" if not options else "ambiguous"),
                            "candidate_call_indices": sorted(options)})
            continue
        index = next(iter(options))
        call = calls[index]
        if not start[1] <= begin + narrowed[0] <= end + narrowed[1] <= stop[0]:
            skipped.append({**entry, "reason": "not_fully_covered_by_trace", "call_index": index})
            continue
        fd_evidence = verified_fd(call, observations, times)
        if fd_evidence["ext4_validation"] != "inode_and_device_match":
            skipped.append({**entry, "reason": fd_evidence["ext4_validation"], "call_index": index,
                            "fd_evidence": fd_evidence})
            continue
        reverse = reverse_batch_candidates(call, all_batches, initial_origin)
        if [row["batch_id"] for row in reverse] != [batch["id"]]:
            skipped.append({**entry, "reason": "reverse_initial_clock_not_unique", "call_index": index,
                            "reverse_initial_clock_candidates": reverse})
            continue
        selected.append((batch, index, call, fd_evidence, reverse))
    quality = {"initial_origin_ns": initial_origin, "narrowed_origin_ns": narrowed, "join_error": error,
               "offset_calibration": calibration, "candidate_counts": dict(Counter(len(row) for row in matches)),
               "observed_syscalls": len(calls), "matched_unique_batches": sum(len(row) == 1 for row in matches) if not error else 0,
               "all_recorded_server_batches": len(all_batches)}
    return selected, skipped, quality


def summarize_slow(rows, cutoff):
    selected = [row for row in rows if row["user_sync_ns"] >= cutoff]
    classifications = Counter()
    for row in selected:
        classifications.update(row["classified_d_state_ns"])
    return {"batches": len(selected), "scheduler_complete": sum(not row["scheduler_warnings"] for row in selected),
            "user_sync_ns": sum(row["user_sync_ns"] for row in selected),
            "kernel_syscall_ns": sum(row["kernel_syscall_ns"] for row in selected),
            "on_cpu_ns": sum(row["on_cpu_ns"] for row in selected),
            "d_state_ns": sum(row["d_state_ns"] for row in selected),
            "other_sleep_ns": sum(row["other_sleep_ns"] for row in selected),
            "classified_d_state_ns": dict(classifications),
            "unclassified_d_state_ns": sum(row["unclassified_d_state_ns"] for row in selected),
            "wakeup_to_switch_in_ns": sum(row["wakeup_to_switch_in_ns"] for row in selected),
            "runnable_preempted_ns": sum(row["runnable_preempted_ns"] for row in selected),
            "unresolved_off_cpu_ns": sum(row["unresolved_off_cpu_ns"] for row in selected)}

def client_set_context(folder, selected_ids, client):
    retained, measured_ports = {}, set()
    for event in records(folder / "client-trace.jsonl"):
        if "index" not in event:
            continue
        measured_ports.add(event["local_port"])
        if event["op"] == "SET":
            retained[event["local_port"], event["conn_ordinal"]] = event
    slow = {key for key, row in retained.items() if row["completed_ns"] - row["planned_ns"] >= TAIL_NS}
    batch_samples, slow_samples = {}, []
    measured_members = Counter()
    offset = client["client_minus_server_clock_bounds_ns"]
    for row in records(folder / "server-batches.jsonl"):
        if row.get("command") != "SET":
            continue
        batch = row["batch"]
        batch_id = batch.get("id")
        key = row["local_port"], row["conn_ordinal"]
        if batch_id in selected_ids and key[0] in measured_ports:
            measured_members[batch_id] += 1
        if key not in retained or (batch_id not in selected_ids and key not in slow):
            continue
        event = retained[key]
        send_to_reply = event["completed_ns"] - event["send_start_ns"]
        server_intervals = phases(row)
        outside_traced_server = send_to_reply - sum(server_intervals.values())
        require(outside_traced_server >= 0, "client TCP exchange does not bound traced server interval")
        sample = {
            "index": event["index"], "batch_id": batch_id, "local_port": key[0], "conn_ordinal": key[1],
            "planned_client_elapsed_ns": event["planned_ns"], "send_client_elapsed_ns": event["send_start_ns"],
            "reply_client_elapsed_ns": event["completed_ns"],
            "planned_to_reply_ns": event["completed_ns"] - event["planned_ns"],
            "before_tcp_send_ns": event["send_start_ns"] - event["planned_ns"],
            "tcp_send_to_reply_ns": send_to_reply, "exchange_outside_traced_server_ns": outside_traced_server,
            "server_phases_ns": server_intervals,
            "sync_start_client_elapsed_ns": [batch["sync_start_ns"] + edge for edge in offset],
            "sync_end_client_elapsed_ns": [batch["sync_end_ns"] + edge for edge in offset],
        }
        if batch_id in selected_ids:
            batch_samples.setdefault(batch_id, []).append(sample)
        if key in slow:
            slow_samples.append(sample)
    require(len(slow_samples) == client["slow"]["set"], "incomplete ≥100 ms client SET socket join")
    return batch_samples, measured_members, sorted(slow_samples, key=lambda row: row["index"])


def load_window(folder):
    capture = read(folder / "capture.json")
    mode = capture["trace_mode"]
    require(mode in {"off", "basic", "journal"} and capture["kernel_mode"] == ("off" if mode == "off" else "on"),
            f"{folder}: contradictory trace mode")
    batches, origin, client, all_batches = batch_inputs(folder, capture)
    result = {
        "folder": folder.name, "trace_mode": mode,
        "raw_paths": {key: f"{folder.name}/{name}" for key, name in (
            ("capture", "capture.json"), ("server", "server-batches.jsonl"), ("client", "client-trace.jsonl"),
            ("clock", "case.json"))},
        "requests": client["requests"], "failures": client["failures"], "slow_requests": client["slow"],
        "p99_us": client["p99_us"], "measured_batches": len(batches),
        "user_sync_ge_10ms": sum(b["sync_end_ns"] - b["sync_start_ns"] >= SLOW_NS for b in batches),
        "user_sync_ge_100ms": sum(b["sync_end_ns"] - b["sync_start_ns"] >= TAIL_NS for b in batches),
        "max_user_sync_ns": max(b["sync_end_ns"] - b["sync_start_ns"] for b in batches),
    }
    trace = folder / "kernel-trace.txt"
    if mode == "off":
        require(not trace.exists(), "off control has a trace file")
        if client["slow"]["set"]:
            _, _, result["slow_client_set_samples"] = client_set_context(folder, set(), client)
        return result
    result["raw_paths"].update(trace=f"{folder.name}/{trace.name}", receipt=f"{folder.name}/kernel-receipt.json")
    receipt = read(folder / "kernel-receipt.json")
    allowed = {int(tid) for tid in receipt["thread_map"].values()}
    trigger = receipt.get("journal_stack_trigger")
    require("[mono]" in receipt["clock"] and receipt["instance_removed"], "invalid private trace receipt")
    if mode == "journal":
        require(trigger and trigger.startswith("stacktrace:unlimited if ") and trigger.endswith(" && prev_state == 2")
                and {int(tid) for tid in re.findall(r"prev_pid == (\d+)", trigger)} == allowed,
                "missing D-state stack trigger for mapped server threads")
    else:
        require(not trigger, "basic trace includes journal stack trigger")
    stats = {}
    calls, forks, pairing, stack_and_journal_events = scan_syscalls(trace, allowed, stats)
    require(all(call["tid"] in allowed for call in calls), "syscall outside mapped thread filter")
    solve = match_batches(batches, calls, origin)
    selected, skipped, quality = measure_slow_calls(batches, calls, all_batches, origin, solve, receipt)
    complete_ring = not stats["absent_from_ring"] and not any(receipt["loss"].values())
    if not complete_ring:
        skipped.extend({"batch_id": batch["id"], "user_sync_ns": batch["sync_end_ns"] - batch["sync_start_ns"],
                        "call_index": index, "reason": "trace_loss"}
                       for batch, index, _, _, _ in selected)
        selected = []
    result.update(
        trace={"events": stats, "loss": receipt["loss"], "ring": ring_usage(receipt),
               "thread_forks": forks, "thread_changes": receipt["thread_changes"],
               "syscall_pairing_issues": pairing, "clock": receipt["clock"], "stack_trigger": trigger,
               "stack_collection_enabled": mode == "journal", "lossless": complete_ring,
               "stack_and_journal_event_counts": stack_and_journal_events},
        join=quality, unmatched_slow_batches=skipped,
    )

    classified = []
    journal_stats = {"slow_syscall_context_scanned": bool(selected)}
    if selected:
        selected_calls = [call for _, _, call, _, _ in selected]
        devices = {call["ext4"][0]["device"] for call in selected_calls}
        require(len(devices) == 1, "DB0 changed ext4 device within the load window")
        device = next(iter(devices))
        if mode == "journal":
            major, minor = map(int, device.split(","))
            require(all(receipt["filters"][f"jbd2/{name}"] == f"dev == {(major << 20) | minor}" for name in COMMIT),
                    "journal filters differ from DB0 ext4 device")
        timelines, pairs, unpaired, transactions, without_id, reread_stats = selected_trace(trace, selected_calls, device)
        require(reread_stats == stats, "trace changed between scans")
        journal_stats.update(complete_transactions=sum(txn["complete_ordered_single_tid"] for txn in transactions),
                             incomplete_transactions=sum(not txn["complete_ordered_single_tid"] for txn in transactions),
                             events_without_transaction_id=without_id,
                             unpaired_stack_headers_in_matched_syscalls=len(unpaired))
        for batch, index, call, fd_evidence, reverse in selected:
            timeline = timelines.get(call["tid"], [])
            timing = scheduler_spans(call, timeline, [entry[0] for entry in timeline])
            episodes = []
            counts = Counter()
            unclassified = 0
            for item in timing["episodes"]:
                if not item["out_state"].startswith("D"):
                    continue
                episode = episode_report(item, call["tid"], pairs, transactions)
                if mode == "basic":
                    episode["classification"] = "unclassified_stack_collection_disabled"
                elif timing["warnings"]:
                    episode["classification"] = "unclassified_scheduler_warnings"
                elif not call["ext4"][0]["ns"] < item["out_ns"] <= item["in_ns"] < call["ext4"][1]["ns"]:
                    episode["classification"] = "unclassified_ext4_envelope"
                duration = episode["d_state_ns"] or 0
                if episode["classification"] in WAIT_CLASSES:
                    counts[episode["classification"]] += duration
                else:
                    unclassified += duration
                episodes.append(episode)
            require(sum(counts.values()) + unclassified == timing["d_state_ns"], "D state classification accounting")
            classified.append({
                "batch_id": batch["id"], "size": batch["size"], "call_index": index,
                "user_sync_ns": batch["sync_end_ns"] - batch["sync_start_ns"],
                "kernel_syscall_ns": call["exit_ns"] - call["enter_ns"],
                "syscall": call, "fd_evidence": fd_evidence, "reverse_initial_clock_candidates": reverse,
                "residuals": residuals(batch, call, solve[1]),
                "on_cpu_ns": timing["on_cpu_ns"], "other_sleep_ns": timing["other_sleep_ns"],
                "d_state_ns": timing["d_state_ns"], "classified_d_state_ns": dict(counts),
                "unclassified_d_state_ns": unclassified,
                "wakeup_to_switch_in_ns": timing["wakeup_to_switch_in_ns"],
                "runnable_preempted_ns": timing["runnable_preempted_ns"],
                "unresolved_off_cpu_ns": timing["unresolved_off_cpu_ns"], "scheduler_warnings": timing["warnings"],
                "d_episodes": episodes,
            })
        overlap_ids = {entry["transaction"] for row in classified for episode in row["d_episodes"]
                       for entry in episode["overlapping_complete_transactions"]}
        journal_stats["overlapping_complete_transactions"] = [txn for txn in transactions
                                                              if txn["transaction"] in overlap_ids]
    if classified or client["slow"]["set"]:
        batch_samples, measured_members, slow_client_samples = client_set_context(
            folder, {row["batch_id"] for row in classified}, client,
        )
        result["slow_client_set_samples"] = slow_client_samples
        for row in classified:
            batch_id = row["batch_id"]
            row["measured_set_members"] = measured_members[batch_id]
            row["retained_client_set_samples"] = batch_samples.get(batch_id, [])
    result.update(slow_batches=classified, journal=journal_stats,
                  summary_ge_10ms=summarize_slow(classified, SLOW_NS),
                  summary_ge_100ms=summarize_slow(classified, TAIL_NS))
    return result


def analyze_load(raw, run, names, verified_files):
    windows = [load_window(raw / name) for name in names]
    require([window["trace_mode"] for window in windows] == run["trace_modes"]
            and all(window["requests"] == run["duration_s"] * run["rate"] and not window["failures"]
                    for window in windows), "load evidence does not match the completed plan")
    return {
        "schema": 1, "stage": run["stage"], "verified_files": verified_files, "windows": windows,
        "interpretation": {
            "classification": "Thresholds apply to the measured Go scope around Sync. Only timing-unique batches "
                              "with an independently unique initial-clock reverse match, bracketed DB0 FD and "
                              "matching ext4 inode are classified. Basic traces provide scheduler timing only.",
            "unclassified": "Missing or ambiguous matches and lossy rings are reported separately. Unknown off-CPU "
                            "intervals are listed separately from observed D time with an unclassified stack.",
            "ring": "Hot CPUs are ranked by retained bytes divided by configured per-CPU bytes at stop.",
            "client_queue": "SETs are joined to the server by local TCP port and command ordinal. The planned-to-send "
                            "span measures client queueing and scheduling before TCP send. Send-to-reply is the "
                            "exchange duration. Its portion outside traced server phases can include client, network, "
                            "and untraced server work. All ≥100 ms SETs are retained; other SETs are sampled.",
            "journal": "Same-device transaction phases give temporal overlap. Writer stacks carry no JBD2 "
                       "transaction ID and do not identify a physical disk request.",
        },
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()
    run, names, verified_files = verified_run(args.raw)
    if run["stage"] == "single":
        result = analyze_single(args.raw, run, names, verified_files)
    else:
        result = analyze_load(args.raw, run, names, verified_files)
    args.out.write_text(json.dumps(result, indent=2, allow_nan=False) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
