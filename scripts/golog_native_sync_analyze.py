"""Associate one native File.Sync probe window with decoded classic ETW events."""

import argparse
from bisect import bisect_left, bisect_right
from collections import Counter, defaultdict
import csv
import json
import ntpath
from pathlib import Path
import re


FILE = "classic_file"
THREAD = "classic_thread"
DISK = "classic_disk"
EXPECTED = {
    (FILE, 73): {"IrpPtr", "TTID", "FileKey"},
    (FILE, 76): {"IrpPtr", "NtStatus"},
    (THREAD, 36): {"NewThreadId", "OldThreadId"},
    (THREAD, 50): {"TThreadId"},
    (DISK, 15): {"Irp"},
    (DISK, 14): {"Irp", "DiskNumber", "HighResResponseTime"},
}
FILE_NAME_OPCODES = {0, 32, 35, 36}
NT_VOLUME = re.compile(r"^\\device\\harddiskvolume[0-9]+(\\.*)$", re.I)


def number(value):
    if value is None:
        return None
    if isinstance(value, int):
        return value
    return int(value, 16 if value.lower().startswith("0x") else 10)


def read_samples(path):
    samples = []
    seen = set()
    previous_end = 0
    with path.open(encoding="utf-8-sig", newline="") as stream:
        for row in csv.DictReader(stream):
            index = int(row["index"])
            if index in seen:
                raise ValueError(f"duplicate probe index {index}")
            seen.add(index)
            begin, end = int(row["qpc_begin"]), int(row["qpc_end"])
            if row["outcome"] == "write_error" and begin == end == 0:
                samples.append({"index": index, "outcome": row["outcome"]})
                continue
            if not 0 < begin <= end or begin < previous_end:
                raise ValueError(f"invalid or overlapping QPC bounds at probe index {index}")
            previous_end = end
            samples.append({"index": index, "outcome": row["outcome"],
                            "qpc_begin": begin, "qpc_end": end})
    if not previous_end:
        raise ValueError("CSV contains no File.Sync QPC windows")
    return samples


def read_trace(path):
    header = summary = None
    schemas = defaultdict(set)
    events = defaultdict(list)
    with path.open("rb") as binary:
        utf16 = binary.read(2) in (b"\xff\xfe", b"\xfe\xff")
    with path.open(encoding="utf-16" if utf16 else "utf-8-sig") as stream:
        for line in stream:
            row = json.loads(line)
            kind = row["type"]
            if kind == "header":
                if header is not None:
                    raise ValueError("ETL JSONL contains more than one header")
                header = row
            elif kind == "summary":
                if summary is not None:
                    raise ValueError("ETL JSONL contains more than one summary")
                summary = row
            elif kind == "schema":
                key = row["provider"], row["opcode"], row["version"]
                schemas[key].update(field["name"] for field in row.get("fields", ()))
            elif kind == "event":
                events[row["provider"]].append(row)
    if header is None or summary is None:
        raise ValueError("ETL JSONL lacks its header or summary")
    for rows in events.values():
        rows.sort(key=lambda row: row["qpc"])
    return header, summary, schemas, events


def trace_checks(header, summary, schemas, tid, qpf):
    issues = []
    if not header.get("raw_timestamp") or header.get("clock_type") != 1:
        issues.append("etl_not_raw_qpc")
    if header.get("perf_frequency") != qpf:
        issues.append("qpc_frequency_mismatch")
    if summary.get("native_thread") != tid:
        issues.append("decoded_thread_mismatch")
    if summary.get("process_status") != 0:
        issues.append("etl_process_failed")

    loss = [header.get("buffers_lost"), header.get("events_lost"), summary.get("events_lost")]
    if any(value for value in loss if value is not None):
        issues.append("etl_reported_loss")
    if None in loss:
        issues.append("etl_loss_counters_missing")

    schema_issues = defaultdict(list)
    for (provider, opcode), expected in EXPECTED.items():
        variants = {version: fields for (name, code, version), fields in schemas.items()
                    if name == provider and code == opcode}
        if not variants:
            schema_issues[provider].append(f"opcode_{opcode}_not_observed")
        for version, fields in variants.items():
            missing = sorted(expected - fields)
            if missing:
                schema_issues[provider].append(
                    f"opcode_{opcode}_v{version}_schema_missing_" + "_".join(missing))
    for row in summary.get("decoder_counts", ()):
        provider, opcode = row["provider"], row["opcode"]
        if (provider, opcode) in EXPECTED:
            for counter in ("filter_unresolved", "raw_included"):
                if row.get(counter):
                    schema_issues[provider].append(f"opcode_{opcode}_has_{counter}")
    return issues, schema_issues


def normalized_path(value):
    value = value.replace("/", "\\")
    if value.startswith(("\\\\?\\", "\\??\\")):
        value = value[4:]
        if value.upper().startswith("UNC\\"):
            value = "\\\\" + value[4:]
    return ntpath.normpath(value).casefold()


def target_match(value, target, aliases):
    path = normalized_path(value)
    if path == target or path in aliases:
        return "exact"
    drive, suffix = ntpath.splitdrive(target)
    volume = NT_VOLUME.fullmatch(path)
    if drive and volume and volume[1] == suffix:
        return "volume_suffix_candidate"
    return "different"


def in_sample(samples, begins, qpc):
    position = bisect_right(begins, qpc) - 1
    if position >= 0 and qpc <= samples[position]["qpc_end"]:
        return samples[position]


def file_observations(rows, samples, begins, tid, pid, target, aliases):
    starts = defaultdict(list)
    completions = defaultdict(list)
    later_names = defaultdict(list)
    for row in rows:
        opcode, qpc = row["opcode"], row["qpc"]
        fields = row.get("fields", {})
        identity = number(fields.get("IrpPtr"))
        if opcode == 76 and identity and row.get("file_flush_start_qpc") is not None:
            completions[(identity, row["file_flush_start_qpc"])].append(row)
        elif opcode in FILE_NAME_OPCODES and fields.get("FileName"):
            key = number(fields.get("FileObject"))
            if key:
                later_names[key].append((qpc, fields["FileName"], opcode))
        elif opcode == 73 and number(fields.get("TTID")) == tid:
            sample = in_sample(samples, begins, qpc)
            if sample is None:
                continue
            record = {"start_qpc": qpc, "irp": fields.get("IrpPtr"),
                      "file_key": fields.get("FileKey"), "header_pid": row.get("pid")}
            sample["file_flushes"].append(record)
            if identity:
                starts[(identity, qpc)].append((sample, row, record))
    for key, observations in starts.items():
        ends = completions[key]
        for sample, row, record in observations:
            qpc, opcode = row["qpc"], row.get("file_path_source_opcode")
            path = row.get("file_path_candidate")
            if path:
                record.update(path=path, path_qpc=row.get("file_path_source_qpc"),
                              path_source_opcode=opcode, path_match=target_match(path, target, aliases))
            else:
                file_key = number(record["file_key"])
                names = [{"qpc": when, "path": name, "opcode": kind,
                          "path_match": target_match(name, target, aliases)}
                         for when, name, kind in later_names[file_key] if when >= qpc]
                if names:
                    record["later_path_candidates"] = names
            if len(observations) != 1 or len(ends) > 1:
                record["pair_status"] = "ambiguous_irp_or_timestamp"
            elif not ends:
                record["pair_status"] = "missing_op_end"
            else:
                end = ends[0]
                record["end_qpc"] = end["qpc"]
                record["duration_ticks"] = end["qpc"] - qpc
                record["nt_status"] = end.get("fields", {}).get("NtStatus")
                if not qpc <= end["qpc"] <= sample["qpc_end"]:
                    record["pair_status"] = "op_end_outside_call"
                elif record["nt_status"] is None:
                    record["pair_status"] = "op_end_missing_status"
                elif row.get("pid") != pid:
                    record["pair_status"] = "flush_header_pid_unconfirmed"
                elif record.get("path_match") != "exact":
                    record["pair_status"] = "target_path_unconfirmed"
                else:
                    record["pair_status"] = "matched"


def schedule_observations(rows, samples, begins, tid):
    for row in rows:
        qpc = row["qpc"]
        sample = in_sample(samples, begins, qpc)
        if sample is None:
            continue
        fields = row.get("fields", {})
        if row["opcode"] == 50 and number(fields.get("TThreadId")) == tid:
            sample["_schedule"].append(("ready", qpc, fields))
        elif row["opcode"] == 36:
            if number(fields.get("OldThreadId")) == tid:
                sample["_schedule"].append(("out", qpc, fields))
            if number(fields.get("NewThreadId")) == tid:
                sample["_schedule"].append(("in", qpc, fields))

    for sample in samples:
        switched_out = None
        ready_times = []
        partial = 0
        for action, qpc, fields in sample.pop("_schedule"):
            if action == "out":
                if switched_out is not None:
                    partial += 1
                switched_out = (qpc, fields)
                ready_times = []
            elif action == "ready":
                if switched_out is None:
                    partial += 1
                else:
                    ready_times.append(qpc)
            elif switched_out is None:
                partial += 1
            else:
                out_qpc, out_fields = switched_out
                segment = {"switch_out_qpc": out_qpc, "switch_in_qpc": qpc,
                           "ready_qpcs": ready_times, "off_cpu_ticks": qpc - out_qpc,
                           "old_thread_state": number(out_fields.get("OldThreadState")),
                           "old_thread_wait_reason": number(out_fields.get("OldThreadWaitReason"))}
                if len(ready_times) == 1:
                    segment["ready_to_switch_in_ticks"] = qpc - ready_times[0]
                else:
                    segment["ready_state"] = "multiple_ready" if ready_times else "no_ready_observed"
                sample["off_cpu_intervals"].append(segment)
                switched_out = None
                ready_times = []
        sample["partial_schedule_events"] = partial + (switched_out is not None)


def disk_observations(rows, samples, begins, filter_disk):
    pending = {}
    duplicates = set()
    counts = Counter()
    ends = [sample["qpc_end"] for sample in samples]
    for row in rows:
        fields = row.get("fields", {})
        identity = number(fields.get("Irp"))
        if not identity:
            counts["missing_irp"] += 1
            continue
        if row["opcode"] == 15:
            if identity in pending:
                duplicates.add(identity)
            pending[identity] = row
            continue
        start = pending.pop(identity, None)
        duplicate = identity in duplicates
        duplicates.discard(identity)
        disk = number(fields.get("DiskNumber"))
        if filter_disk is not None and disk != filter_disk:
            continue
        if start is None or duplicate:
            counts["completion_ambiguous_init" if duplicate else "completion_missing_init"] += 1
            continue
        begin, end = start["qpc"], row["qpc"]
        if end <= begin:
            counts["completion_invalid_order"] += 1
            continue
        counts["paired"] += 1
        candidate = {"irp": fields["Irp"], "disk": disk, "start_qpc": begin, "end_qpc": end,
                     "init_issuing_tid": number(start["fields"].get("IssuingThreadId")),
                     "completion_issuing_tid": number(fields.get("IssuingThreadId")),
                     "response_time_reported": number(fields.get("HighResResponseTime"))}
        for sample in samples[bisect_right(ends, begin):bisect_left(begins, end)]:
            overlap = min(sample["qpc_end"], end) - max(sample["qpc_begin"], begin)
            if overlap > 0:
                sample["disk_overlap_candidates"].append(dict(candidate, overlap_ticks=overlap))
    counts["init_without_completion"] = len(pending)
    return counts


def analyze(probe, etl, target_file, etw_paths, pid, tid, qpf, disk):
    entries = read_samples(probe)
    samples = [sample for sample in entries if "qpc_begin" in sample]
    begins = [sample["qpc_begin"] for sample in samples]
    header, summary, schemas, events = read_trace(etl)
    issues, schema_issues = trace_checks(header, summary, schemas, tid, qpf)
    first, last = summary.get("first_qpc"), summary.get("last_qpc")
    if not first or not last or first > last:
        issues.append("etl_observed_qpc_bounds_missing")
    conflict = {"etl_not_raw_qpc", "qpc_frequency_mismatch", "decoded_thread_mismatch"} & set(issues)
    for sample in samples:
        sample.update(file_flushes=[], off_cpu_intervals=[], _schedule=[], disk_overlap_candidates=[])
        sample["inside_observed_etl_qpc_bounds"] = bool(
            first and last and first <= sample["qpc_begin"] <= sample["qpc_end"] <= last)
        sample["sync_ticks"] = sample["qpc_end"] - sample["qpc_begin"]
        sample["sync_us"] = sample["sync_ticks"] * 1_000_000 / qpf

    if not conflict:
        file_observations(events[FILE], samples, begins, tid, pid, normalized_path(target_file),
                          {normalized_path(path) for path in etw_paths})
        schedule_observations(events[THREAD], samples, begins, tid)
        disk_counts = disk_observations(events[DISK], samples, begins, disk)
    else:
        disk_counts = {}
    matched = ready_observed = fully_in_bounds = 0
    file_pair_status = Counter()
    for sample in samples:
        sample.pop("_schedule", None)
        file_states = [row.get("pair_status", "missing_flush_irp") for row in sample["file_flushes"]]
        file_pair_status.update(file_states)
        matched += file_states.count("matched") == 1
        ready_observed += any("ready_to_switch_in_ticks" in row for row in sample["off_cpu_intervals"])
        fully_in_bounds += sample["inside_observed_etl_qpc_bounds"]
        sample["file_evidence"] = ("ambiguous" if file_states.count("matched") > 1
                                   else "matched" if file_states.count("matched") == 1
                                   else "insufficient")
        ready_segments = [row["ready_to_switch_in_ticks"] for row in sample["off_cpu_intervals"]
                          if "ready_to_switch_in_ticks" in row]
        sample["ready_to_switch_in_ticks_observed"] = sum(ready_segments) if ready_segments else None
        sample["schedule_evidence"] = ("partial" if schema_issues.get(THREAD) or
                                       sample.get("partial_schedule_events")
                                       else "observed" if ready_segments else "insufficient")
        sample["disk_evidence"] = (
            "overlap_candidates" if sample["disk_overlap_candidates"] else "insufficient")
        sample["trace_evidence"] = ("incomplete" if issues or not sample["inside_observed_etl_qpc_bounds"]
                                    else "within_observed_bounds")
    if fully_in_bounds != len(samples) and "etl_observed_qpc_bounds_missing" not in issues:
        issues.append("probe_partly_or_entirely_outside_observed_etl_qpc_bounds")
    return {
        "summary": {
            "pid": pid, "tid": tid, "qpc_frequency": qpf,
            "target_file": target_file, "target_etw_paths": etw_paths,
            "probe_first_qpc": begins[0], "probe_last_qpc": samples[-1]["qpc_end"],
            "etl_first_qpc": first, "etl_last_qpc": last,
            "etl_buffers_lost": header.get("buffers_lost"),
            "etl_header_events_lost": header.get("events_lost"),
            "etl_reader_events_lost": summary.get("events_lost"),
            "etl_matched_probe_boundary_count": fully_in_bounds,
            "probe_entries": len(entries), "probe_sync_entries": len(samples),
            "issues": issues, "decoder_issues": dict(schema_issues),
            "entries_with_one_target_file_pair": matched, "file_pair_status_counts": file_pair_status,
            "entries_with_ready_to_switch_in_observed": ready_observed,
            "disk_pair_counts": disk_counts,
            "disk_scope": "overlap candidates based on same-IRP DiskIo init and completion",
            "disk_number_filter": disk,
        },
        "entries": entries,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("probe_csv", type=Path)
    parser.add_argument("native_jsonl", type=Path)
    parser.add_argument("--target-file", required=True)
    parser.add_argument("--target-etw-path", action="append", default=[],
                        help="verified equivalent spelling of the target file in ETW")
    parser.add_argument("--pid", type=int, required=True)
    parser.add_argument("--tid", type=int, required=True)
    parser.add_argument("--qpf", type=int, required=True)
    parser.add_argument("--disk", type=int)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if min(args.pid, args.tid, args.qpf) <= 0 or args.disk is not None and args.disk < 0:
        parser.error("PID, TID and QPF must be positive and disk must be nonnegative if specified")
    try:
        result = analyze(args.probe_csv, args.native_jsonl, args.target_file, args.target_etw_path,
                         args.pid, args.tid, args.qpf, args.disk)
        with args.output.open("w", encoding="utf-8") as stream:
            json.dump(result, stream, ensure_ascii=False, separators=(",", ":"))
            stream.write("\n")
    except (OSError, ValueError, KeyError) as error:
        parser.error(str(error))
    print(json.dumps(result["summary"], ensure_ascii=True))


if __name__ == "__main__":
    main()
