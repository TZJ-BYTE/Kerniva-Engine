"""Reconcile DB0 Go-log batch timing with its tracefs syscall and scheduler records."""
import argparse
from bisect import bisect_left, bisect_right
from collections import Counter, defaultdict
from hashlib import file_digest
import json
from pathlib import Path
import re
import tempfile

from golog_batch_report import describe, records, require, window


TRACE_LINE = re.compile(
    r"^\s*(.*)-(\d+)\s+\[(\d+)\]\s+\S+\s+(\d+)\.(\d{6,9}):\s+(.*)$"
)
ENTER = re.compile(r"sys_(f(?:data)?sync)\(fd:\s*(\w+)\)")
EXIT = re.compile(r"sys_(f(?:data)?sync) ->\s*(\S+)")
SWITCH = re.compile(r"prev_pid=(\d+)\s+\S+\s+prev_state=(\S+)\s+==>.*?next_pid=(\d+)\b")
WAKE = re.compile(r"pid=(\d+)\b")
EXT4 = re.compile(r"ext4_sync_file_(entry|enter|exit): dev (\d+,\d+) ino (\d+) (.*)")
FORK = re.compile(r"\bpid=(\d+)\b.*\bchild_pid=(\d+)\b")
TRACE_COUNTS = re.compile(r"entries-in-buffer/entries-written:\s*(\d+)/(\d+)")
MS = 1_000_000


def read(path):
    return json.loads(path.read_bytes())


def parse_trace(path):
    count = 0
    previous_ns = 0
    header_counts = []
    with path.open(encoding="utf-8") as stream:
        for line in stream:
            if line.startswith("#"):
                if match := TRACE_COUNTS.search(line):
                    header_counts.append(tuple(map(int, match.groups())))
                continue
            if not line.strip():
                continue
            match = TRACE_LINE.match(line)
            require(match is not None, f"{path}: unrecognized trace record: {line.strip()}")
            comm, tid, cpu, sec, fraction, payload = match.groups()
            quantization_ns = 10 ** (9 - len(fraction))
            ts = int(sec) * 10**9 + int(fraction) * quantization_ns
            require(previous_ns <= ts, f"{path}: trace timestamp order")
            count += 1
            previous_ns = ts
            yield {"ns": ts, "resolution_ns": quantization_ns, "tid": int(tid),
                   "cpu": int(cpu), "comm": comm, "payload": payload}
    require(header_counts == [(count, count)],
            f"{path}: ftrace header reports missing entries: {header_counts}")


def split_trace(rows, allowed):
    pending = {}
    syscalls = []
    sched = defaultdict(list)
    blocks = []
    forks = []
    problems = Counter()
    count = 0
    for row in rows:
        count += 1
        ns, tid, payload = row["ns"], row["tid"], row["payload"]
        if match := ENTER.fullmatch(payload):
            call, fd = match.groups()
            # Formatted syscall events use hexadecimal argument rendering.
            if tid in pending:
                problems["enter_before_prior_exit"] += 1
            pending[tid] = {
                "kind": call, "fd": int(fd, 16), "tid": tid, "enter_ns": ns,
                "enter_resolution_ns": row["resolution_ns"], "ext4": [],
            }
        elif match := EXIT.fullmatch(payload):
            call, result = match.groups()
            start = pending.pop(tid, None)
            if start is None or start["kind"] != call:
                problems["exit_without_matching_entry"] += 1
                continue
            syscalls.append({**start, "exit_ns": ns, "exit_resolution_ns": row["resolution_ns"],
                             "result": int(result, 0)})
        elif payload.startswith("sched_switch:"):
            match = SWITCH.search(payload)
            require(match is not None, f"invalid sched_switch line: {payload}")
            prev, state, next_tid = match.groups()
            if int(prev) in allowed:
                sched[int(prev)].append((ns, "out", state))
            if int(next_tid) in allowed:
                sched[int(next_tid)].append((ns, "in", None))
        elif payload.startswith(("sched_wakeup:", "sched_wakeup_new:")):
            match = WAKE.search(payload)
            require(match is not None, f"invalid sched_wakeup line: {payload}")
            if int(match[1]) in allowed:
                sched[int(match[1])].append((ns, "wake", None))
        elif payload.startswith("block_rq_"):
            kind, detail = payload.split(": ", 1)
            device, flags, *_ = detail.split()
            blocks.append((ns, kind, device, flags))
        elif payload.startswith("sched_process_fork:"):
            match = FORK.search(payload)
            require(match is not None, f"invalid sched_process_fork line: {payload}")
            parent, child = map(int, match.groups())
            forks.append({"ns": ns, "parent_tid": parent, "child_tid": child, "in_filter": child in allowed})
        elif (match := EXT4.fullmatch(payload)) and tid in pending:
            phase, device, inode, detail = match.groups()
            pending[tid]["ext4"].append({"phase": phase, "device": device, "inode": int(inode),
                                         "detail": detail, "ns": ns})
    problems["enter_without_exit"] = len(pending)
    return sorted(syscalls, key=lambda row: row["enter_ns"]), sched, blocks, forks, dict(problems), count


def scheduler_spans(call, timeline, times):
    enter, exit_ = call["enter_ns"], call["exit_ns"]
    # Ordering is unavailable for tracepoints at the same rounded timestamp
    # on different CPUs. Do not use boundary events with equal timestamps.
    left, right = bisect_right(times, enter), bisect_left(times, exit_)
    edge_tie = (bisect_left(times, enter) != left or bisect_right(times, exit_) != right)
    running = True
    last = enter
    state = None
    wake = None
    parts = Counter()
    warnings = Counter()
    episodes = []
    if edge_tie:
        warnings["syscall_boundary_timestamp_tie"] += 1
    for ns, action, detail in timeline[left:right]:
        if action == "out":
            if not running:
                warnings["switch_out_while_off_cpu"] += 1
                continue
            parts["on_cpu_ns"] += ns - last
            running, last, state, wake = False, ns, detail, None
        elif action == "wake" and not running:
            if wake is None:
                wake = ns
            else:
                warnings["multiple_wakeups_off_cpu"] += 1
        elif action == "in":
            if running:
                warnings["switch_in_without_switch_out"] += 1
                continue
            if state.startswith("R"):
                parts["runnable_preempted_ns"] += ns - last
                if wake is not None:
                    warnings["wakeup_for_runnable_state"] += 1
            elif wake is not None:
                sleep_key = "d_state_ns" if state.startswith("D") else "other_sleep_ns"
                parts[sleep_key] += wake - last
                parts["wakeup_to_switch_in_ns"] += ns - wake
            else:
                parts["unresolved_off_cpu_ns"] += ns - last
                warnings["sleep_without_observed_wakeup"] += 1
            episodes.append({"out_ns": last, "out_state": state, "wake_ns": wake, "in_ns": ns})
            running, last = True, ns
    if running:
        parts["on_cpu_ns"] += exit_ - last
    else:
        parts["unresolved_off_cpu_ns"] += exit_ - last
        warnings["syscall_exit_without_switch_in"] += 1
    require(sum(parts.values()) == exit_ - enter, "scheduler accounting does not cover syscall")
    return {**{key: parts[key] for key in ("on_cpu_ns", "d_state_ns", "other_sleep_ns",
                                           "runnable_preempted_ns", "wakeup_to_switch_in_ns",
                                           "unresolved_off_cpu_ns")},
            "episodes": episodes, "warnings": dict(warnings)}


def block_context(blocks, times, enter, exit_):
    selected = blocks[bisect_left(times, enter):bisect_right(times, exit_)]
    counts = Counter((kind, device, flags) for _, kind, device, flags in selected)
    return [{"event": kind, "device": device, "flags": flags, "count": count}
            for (kind, device, flags), count in sorted(counts.items())]

def batch_inputs(folder, capture):
    meta, *events, summary = records(folder / "server-batches.jsonl")
    require(meta["type"] == "meta" and summary["type"] == "summary" and summary["drops"] == 0
            and summary["seen"] == summary["kept"] == len(events), "incomplete Go server trace")
    if folder.name.startswith("single-write"):
        measured = [event for event in events
                    if event["local_port"] == capture["local_port"] and event["command"] == "SET"]
        require(capture["reply"] == "OK" and len(measured) == 1, "single SET identity")
        event = measured[0]
        offset = (
            capture["start"]["monotonic_before_ns"] - event["parsed_ns"],
            capture["end"]["monotonic_after_ns"] - event["response_start_ns"],
        )
        client = None
    else:
        client = window(folder)
        ports = {row["local_port"] for row in records(folder / "client-trace.jsonl") if "index" in row}
        measured = [event for event in events if event["local_port"] in ports and event["command"] == "SET"]
        require(len(measured) == client["joined_sets"], "incomplete measurement SET census")
        alignment = read(folder / "case.json")["measurement_alignment"]
        require(alignment["projection_consistent"], "client-to-WSL monotonic projection inconsistent")
        offset = tuple(sum(pair) for pair in zip(alignment["projected_start_monotonic_ns"],
                                                client["client_minus_server_clock_bounds_ns"]))
    groups = defaultdict(list)
    for event in events:
        batch = event["batch"]
        if batch.get("id") and batch.get("sync_start_ns") and batch.get("sync_end_ns"):
            groups[batch["id"]].append(event)
    selected = {event["batch"]["id"] for event in measured}
    batches = []
    for batch_id in selected:
        members = groups[batch_id]
        batch = members[0]["batch"]
        signature = {key: value for key, value in batch.items() if key != "sequence"}
        require(len(members) == batch["size"] and batch["sync_start_ns"] <= batch["sync_end_ns"]
                and all(event["response_ok"] and not event["batch"].get("failed")
                        and {key: value for key, value in event["batch"].items() if key != "sequence"} == signature
                        for event in members), f"invalid measured batch {batch_id}")
        batches.append(batch)
    batches.sort(key=lambda batch: batch["sync_start_ns"])
    require(offset[0] <= offset[1] and batches and all(
        left["sync_end_ns"] <= right["sync_start_ns"] for left, right in zip(batches, batches[1:])
    ), "Go DB0 batch ordering or start reference inconsistent")
    if client is not None:
        require(len(batches) == client["batches"], "measured batch census differs from client join")
    all_batches = [members[0]["batch"] for members in groups.values()]
    require(all(0 < members[0]["batch"]["sync_start_ns"] <= members[0]["batch"]["sync_end_ns"]
                and all(event["batch"]["sync_start_ns"] == members[0]["batch"]["sync_start_ns"]
                        and event["batch"]["sync_end_ns"] == members[0]["batch"]["sync_end_ns"] for event in members)
                for members in groups.values()), "inconsistent sync envelope in recorded server batches")
    return batches, offset, client, all_batches


def active_fd_evidence(call, observations, times):
    enter, _ = syscall_stamp(call, "enter")
    _, exit_ = syscall_stamp(call, "exit")
    left = bisect_right(times, enter) - 1
    while left >= 0 and observations[left]["guest_mono_after_ns"] > enter:
        left -= 1
    right = bisect_left(times, exit_)
    if left < 0 or right >= len(observations):
        return {"kind": "fd_not_bracketed"}

    def names(observation):
        return {(entry["dev"], entry["ino"]) for entry in observation["active_fds"]
                if entry["fd"] == call["fd"] and entry["ino"] == observation["active_path_ino"]}

    identities = [names(observation) for observation in observations[left:right + 1]]
    known = set().union(*identities)
    if len(known) == 1 and all(value == known for value in identities):
        dev, ino = next(iter(known))
        return {"kind": "fd_stat_bracket", "device": dev, "inode": ino, "observation_indices": [left, right]}
    if known:
        return {"kind": "fd_generation_boundary", "observed_identities": sorted(known),
                "observation_indices": [left, right]}
    return {"kind": "not_observed_active_fd", "observation_indices": [left, right]}


def syscall_stamp(call, edge):
    # ftrace's ns2usecs adds 500 ns before formatting microseconds.
    value, step = call[f"{edge}_ns"], call[f"{edge}_resolution_ns"]
    return value - step // 2, value + (step - 1) // 2


def candidate_ranges(batches, calls, offset):
    enter_times = [call["enter_ns"] for call in calls]
    largest_resolution = max((call["enter_resolution_ns"] for call in calls), default=1)
    result = []
    for batch in batches:
        start, end = batch["sync_start_ns"], batch["sync_end_ns"]
        left = bisect_left(enter_times, start + offset[0] - (largest_resolution - 1) // 2)
        right = bisect_right(enter_times, end + offset[1] + largest_resolution // 2)
        options = {}
        for index in range(left, right):
            call = calls[index]
            if call["kind"] != "fsync" or call["result"] != 0:
                continue
            lo = max(offset[0], syscall_stamp(call, "exit")[0] - end)
            hi = min(offset[1], syscall_stamp(call, "enter")[1] - start)
            if lo <= hi:
                options[index] = (lo, hi)
        result.append(options)
    return result


def peak_support(rows):
    changes = Counter()
    for options in rows:
        end = None
        for low, high in sorted(options.values()):
            if end is None or low > end + 1:
                if end is not None:
                    changes[end + 1] -= 1
                changes[low] += 1
                end = high
            else:
                end = max(end, high)
        if end is not None:
            changes[end + 1] -= 1
    edges = sorted(key for key, change in changes.items() if change)
    maximum, active, peaks = 0, 0, []
    for start, stop in zip(edges, edges[1:]):
        active += changes[start]
        if active > maximum:
            maximum, peaks = active, [(start, stop - 1)]
        elif active == maximum:
            peaks.append((start, stop - 1))
    return {"support": maximum, "batches": len(rows), "peak_ranges_ns": peaks}


def match_batches(batches, calls, offset):
    independent = offset
    initial = candidate_ranges(batches, calls, independent)
    # Sparse absences can make an adjacent unrelated syscall the only candidate
    # under wide independent bounds. Require two independently fitted halves
    # to support the same offset before allowing a partial-census match.
    calibration = []
    if len(batches) > 1:
        halfway = len(batches) // 2
        calibration = [peak_support(initial[:halfway]), peak_support(initial[halfway:])]
        if any(len(fit["peak_ranges_ns"]) != 1 or 10 * fit["support"] < 9 * fit["batches"]
               for fit in calibration):
            return initial, independent, "offset_calibration_inconclusive", calibration
        first, second = [fit["peak_ranges_ns"][0] for fit in calibration]
        offset = max(first[0], second[0]), min(first[1], second[1])
        if offset[0] > offset[1]:
            return initial, independent, "offset_calibration_halves_disagree", calibration
    while True:
        ranges = candidate_ranges(batches, calls, offset)
        unique = [(next(iter(options)), next(iter(options.values()))) for options in ranges if len(options) == 1]
        lower = max((bounds[0] for _, bounds in unique), default=offset[0])
        upper = min((bounds[1] for _, bounds in unique), default=offset[1])
        indices = [index for index, _ in unique]
        if lower > upper or any(left >= right for left, right in zip(indices, indices[1:])):
            return ranges, offset, "inconsistent_unique_candidates", calibration
        if (lower, upper) == offset:
            # Recover the full common interval allowed by the chosen pairs and
            # independent clocks before calculating residual duration bounds.
            verified = [initial[i][next(iter(options))] for i, options in enumerate(ranges) if len(options) == 1]
            envelope = (max((lo for lo, _ in verified), default=independent[0]),
                        min((hi for _, hi in verified), default=independent[1]))
            return ranges, envelope, None, calibration
        offset = lower, upper


def residuals(batch, call, offset):
    enter = syscall_stamp(call, "enter")
    exit_ = syscall_stamp(call, "exit")
    start, end = batch["sync_start_ns"], batch["sync_end_ns"]
    return {
        "user_before_syscall_ns": [max(0, enter[0] - start - offset[1]), enter[1] - start - offset[0]],
        "user_after_syscall_ns": [max(0, end + offset[0] - exit_[1]), end + offset[1] - exit_[0]],
        "user_outside_syscall_ns": [max(0, end - start - (exit_[1] - enter[0])),
                                    end - start - max(0, exit_[0] - enter[1])],
    }


def reverse_batch_candidates(call, batches, offset):
    earliest_exit = syscall_stamp(call, "exit")[0]
    latest_enter = syscall_stamp(call, "enter")[1]
    candidates = []
    for batch in batches:
        lower = max(offset[0], earliest_exit - batch["sync_end_ns"])
        upper = min(offset[1], latest_enter - batch["sync_start_ns"])
        if lower <= upper:
            candidates.append({"batch_id": batch["id"], "feasible_offset_ns": [lower, upper]})
    return sorted(candidates, key=lambda item: item["batch_id"])


def summary(rows):
    stats = {"matched": len(rows), "measured_sync_ge_10ms": sum(row["user_sync_ns"] >= 10 * MS for row in rows)}
    for key in ("user_sync_ns", "kernel_syscall_ns"):
        stats[key] = describe([row[key] for row in rows])
    for key in ("on_cpu_ns", "d_state_ns", "other_sleep_ns", "wakeup_to_switch_in_ns",
                "runnable_preempted_ns", "unresolved_off_cpu_ns"):
        known = [row["scheduler"][key] for row in rows if not row["scheduler"]["warnings"]]
        stats[key] = describe(known)
    stats["scheduler_complete"] = sum(not row["scheduler"]["warnings"] for row in rows)
    stats["scheduler_warning_counts"] = dict(Counter(warning for row in rows for warning in row["scheduler"]["warnings"]))
    stats["fd_evidence_counts"] = dict(Counter(row["fd_evidence"]["kind"] for row in rows))
    stats["ext4_validation_counts"] = dict(Counter(row["fd_evidence"]["ext4_validation"] for row in rows))
    stats["ext4_sync_ns"] = describe([row["ext4_sync_ns"] for row in rows if row["ext4_sync_ns"] is not None])
    stats["outside_syscall_lower_ns"] = describe([row["residuals"]["user_outside_syscall_ns"][0] for row in rows])
    stats["outside_syscall_upper_ns"] = describe([row["residuals"]["user_outside_syscall_ns"][1] for row in rows])
    return stats

def linux_device(number):
    major = ((number >> 8) & 0xfff) | ((number >> 32) & 0xfffff000)
    minor = (number & 0xff) | ((number >> 12) & 0xffffff00)
    return f"{major},{minor}"


def verified_fd(call, observations, times):
    evidence = active_fd_evidence(call, observations, times)
    ext4 = call["ext4"]
    if len(ext4) != 2:
        return {**evidence, "ext4_validation": "missing_or_duplicate"}
    entered, exited = ext4
    if (entered["phase"] not in ("enter", "entry") or exited["phase"] != "exit"
            or (entered["device"], entered["inode"]) != (exited["device"], exited["inode"])
            or not entered["detail"].strip().endswith(f"datasync {int(call['kind'] == 'fdatasync')}")
            or exited["detail"] != "ret 0"):
        return {**evidence, "ext4_validation": "inconsistent"}
    identity = (entered["device"], entered["inode"])
    if evidence["kind"] == "fd_stat_bracket":
        agrees = (linux_device(evidence["device"]), evidence["inode"]) == identity
        return {**evidence, "ext4_validation": "inode_and_device_match" if agrees else "fd_inode_conflict"}
    if "observation_indices" not in evidence:
        return {**evidence, "ext4_validation": "not_bracketed"}
    left, right = evidence["observation_indices"]
    supports = [
        index for index in range(left, right + 1)
        if any((linux_device(entry["dev"]), entry["ino"]) == identity
               and entry["fd"] == call["fd"] and entry["ino"] == observations[index]["active_path_ino"]
               for entry in observations[index]["active_fds"])
    ]
    return {**evidence, "ext4_validation": "inode_seen_as_active_near_rotation" if supports
            else "inode_not_seen_as_active", "identity_observations": supports}


def analyze(folder):
    capture = read(folder / "capture.json")
    batches, origin, client, all_batches = batch_inputs(folder, capture)
    result = {
        "folder": folder.name, "mode": capture.get("kernel_mode", "single"), "measured_batches": len(batches),
        "user_sync_ns": describe([batch["sync_end_ns"] - batch["sync_start_ns"] for batch in batches]),
        "measured_sync_ge_10ms": sum(batch["sync_end_ns"] - batch["sync_start_ns"] >= 10 * MS for batch in batches),
    }
    if client is not None:
        result.update(requests=client["requests"], failures=client["failures"], slow_requests=client["slow"],
                      p99_us=client["p99_us"], longest_slow_set_exchange=client["longest_exchange"])
    else:
        result["ptrace_delay_exit_ms"] = capture.get("strace_delay_exit_ms", 0)
        start, end = capture["start"], capture["end"]
        result["client_request_elapsed_ns"] = [end["monotonic_before_ns"] - start["monotonic_after_ns"],
                                               end["monotonic_after_ns"] - start["monotonic_before_ns"]]
    if result["mode"] == "off":
        require(not (folder / "kernel-trace.txt").exists(), "off control contains a kernel trace")
        return result

    receipt = read(folder / "kernel-receipt.json")
    require("[mono]" in receipt["clock"] and receipt["instance_removed"]
            and all(receipt["loss"][key] == 0 for key in ("overrun", "commit overrun", "dropped events")),
            "kernel receipt does not establish lossless private monotonic tracing")
    observations = receipt["fd_observations"]
    observation_times = [row["guest_mono_before_ns"] for row in observations]
    require(observation_times == sorted(observation_times) and all(
        row["guest_mono_before_ns"] <= row["guest_mono_after_ns"] for row in observations
    ), "invalid active FD observation order")
    thread_map = receipt["thread_map"]
    allowed = {int(value) for value in thread_map.values()}
    calls, sched, blocks, forks, syscall_issues, trace_count = split_trace(
        parse_trace(folder / "kernel-trace.txt"), allowed,
    )
    require(all(call["tid"] in allowed for call in calls), "kernel syscall outside mapped primary thread census")
    sched_times = {tid: [entry[0] for entry in timeline] for tid, timeline in sched.items()}
    block_times = [entry[0] for entry in blocks]
    start, stop = receipt["trace_start_ns"], receipt["trace_stop_ns"]
    require(start[0] <= start[1] <= stop[0] <= stop[1], "invalid trace lifecycle clock")
    candidates, narrowed_origin, error, calibration = match_batches(batches, calls, origin)
    matched = []
    omissions = []
    used = set()
    for batch, options in zip(batches, candidates):
        ix, begin, end = batch["id"], batch["sync_start_ns"], batch["sync_end_ns"]
        if not options or len(options) > 1 or error:
            omissions.append({"batch_id": ix, "candidate_call_indices": sorted(options),
                              "user_sync_ns": end - begin,
                              "user_sync_start_guest_ns": [begin + edge for edge in narrowed_origin],
                              "reason": error or ("ambiguous" if options else "no_candidate")})
            continue
        call_ix = next(iter(options))
        call = calls[call_ix]
        used.add(call_ix)
        fd = verified_fd(call, observations, observation_times)
        timing = scheduler_spans(call, sched.get(call["tid"], []), sched_times.get(call["tid"], []))
        ext4_duration = (call["ext4"][1]["ns"] - call["ext4"][0]["ns"] if fd["ext4_validation"] in
                         ("inode_and_device_match", "inode_seen_as_active_near_rotation") else None)
        matched.append({
            "batch_id": ix, "size": batch["size"], "sequence": batch["sequence"], "call_index": call_ix,
            "user_sync_ns": end - begin, "kernel_syscall_ns": call["exit_ns"] - call["enter_ns"],
            "user_span_inside_trace": start[1] <= begin + narrowed_origin[0]
                                      and end + narrowed_origin[1] <= stop[0],
            "syscall": call, "fd_evidence": fd, "scheduler": timing,
            "ext4_sync_ns": ext4_duration,
            "residuals": residuals(batch, call, narrowed_origin),
            "concurrent_guest_block_events": block_context(blocks, block_times, call["enter_ns"], call["exit_ns"]),
        })
    unmatched = []
    for index, call in enumerate(calls):
        if index not in used:
            unmatched.append({"call_index": index, "syscall": call,
                              "fd_evidence": verified_fd(call, observations, observation_times)})
    confirmed = [row for row in matched if row["user_span_inside_trace"]
                 and row["fd_evidence"]["ext4_validation"] == "inode_and_device_match"]
    slow_confirmed = [row for row in confirmed if row["user_sync_ns"] >= 10 * MS]
    initial_clock_candidates = {}
    for index, call in enumerate(calls):
        if call["kind"] == "fsync" and call["result"] == 0 and call["exit_ns"] - call["enter_ns"] >= 10 * MS:
            initial_clock_candidates[index] = reverse_batch_candidates(call, all_batches, origin)
    reverse_verified = 0
    for row in slow_confirmed:
        index = row["call_index"]
        if index not in initial_clock_candidates:
            initial_clock_candidates[index] = reverse_batch_candidates(row["syscall"], all_batches, origin)
        alternatives = initial_clock_candidates[index]
        agrees = len(alternatives) == 1 and alternatives[0]["batch_id"] == row["batch_id"]
        row["reverse_initial_clock_check"] = {"unique_same_batch": agrees, "candidate_batches": alternatives}
        reverse_verified += agrees
    result.update(
        trace_entries=trace_count, complete_fsync_calls=len(calls), syscall_pairing_issues=syscall_issues,
        trace_clock=receipt["clock"], trace_loss=receipt["loss"],
        trace_thread_forks=forks, capture_thread_changes=receipt["thread_changes"],
        trace_start_ns=start, trace_stop_ns=stop,
        origin_initial_ns=origin, origin_narrowed_ns=narrowed_origin,
        offset_calibration=calibration, join_error=error,
        unique_timing_matches=summary(matched), confirmed_active_fd_matches=summary(confirmed),
        matched_rows=matched, unmatched_batches=omissions, unmatched_syscalls=unmatched,
        unmatched_syscall_fd_evidence_counts=dict(Counter(row["fd_evidence"]["ext4_validation"] for row in unmatched)),
        slow_confirmed=summary(slow_confirmed),
        reverse_initial_clock_check={"all_server_batches_examined": len(all_batches),
                                     "slow_confirmed_checked": len(slow_confirmed),
                                     "unique_same_batch": reverse_verified,
                                     "syscalls_examined": [
                                         {"call_index": index, "syscall_ns": calls[index]["exit_ns"] - calls[index]["enter_ns"],
                                          "candidate_batches": alternatives}
                                         for index, alternatives in sorted(initial_clock_candidates.items())]},
    )
    return result


def verified_run(raw):
    manifest = read(raw / "ext4-readback.json")
    run = read(raw / "run.json")
    require(manifest["driver_exit_code"] == run["driver_exit_code"] == 0 and run["windows"],
            "capture did not complete")
    names = [entry["folder"] for entry in run["windows"]]
    files = {"run.json"}
    for name in names:
        folder = raw / name
        for filename in ("capture.json", "server-batches.jsonl", "kernel-receipt.json", "kernel-trace.txt",
                         "case.json", "client-trace.jsonl", "client.json", "client-hist.json", "fsync.strace"):
            path = folder / filename
            if path.exists():
                files.add(path.relative_to(raw).as_posix())
    for relative in sorted(files):
        with (raw / relative).open("rb") as stream:
            require(file_digest(stream, "sha256").hexdigest() == manifest["raw_sha256"].get(relative),
                    f"capture readback hash mismatch: {relative}")
    return run, names, len(files)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    run, names, file_count = verified_run(args.raw)
    result = {
        "schema": 1, "stage": run["stage"], "verified_files": file_count,
        "interpretation": {
            "join": "Each unique timing match uses monotonic envelopes, successful syscall results and strictly increasing non-duplicated call indices. For multi-batch windows, both chronological halves must independently support the same peak offset range with at least 90% of batches feasible in each half. Confirmed rows also require matching ext4 inode/device and bracketing observations of the DB0 active FD. Near-rotation associations are retained as uncertain.",
            "scheduler": "On-CPU is scheduled-in elapsed time. D-state is from uninterruptible switch-out to observed wakeup, and runnable time is separated into preempted time and wakeup-to-switch-in. Neither D state nor elapsed syscall time identifies a disk request.",
            "device": "Guest block events are temporal context only. No block event is assigned to a particular batch or syscall.",
            "reverse_check": "Each confirmed slow syscall is checked against all recorded server sync batches, including batches outside the measured connections, using only the independent client-derived clock envelope.",
        },
        "windows": [analyze(args.raw / name) for name in names],
    }
    require(["on" if item["mode"] == "single" else item["mode"] for item in result["windows"]] == run["order"],
            "unexpected kernel tracing window order")
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=args.out.parent, delete=False) as stream:
        temporary = Path(stream.name)
        try:
            json.dump(result, stream, separators=(",", ":"), allow_nan=False)
            stream.write("\n")
            stream.close()
            temporary.replace(args.out)
        finally:
            stream.close()
            temporary.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
