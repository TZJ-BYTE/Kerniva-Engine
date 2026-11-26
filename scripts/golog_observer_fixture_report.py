#!/usr/bin/env python3
"""Verify the independent-DB observer fixture against strace, server phases and client clocks."""
import argparse
import json
from pathlib import Path
import re

ACTIVE_SYNC = re.compile(r"^\d+ (\d+)\.(\d{6}) fsync\(\d+<[^>]+/db_0/active\.log>")
CASES = {"controlled-A-fsync-delay": ("A", 350), "controlled-B-fsync-delay": ("B", 350),
         "controlled-B-fsync-baseline": ("B", 0)}


def require(condition, detail):
    if not condition:
        raise ValueError(detail)


def paired_call(records, call, ordinal):
    identity = call["local_port"], ordinal
    row = records[identity]
    require(row["command"] == call["command"][0] and row["response_ok"], f"invalid server response {identity}")
    return row


def fixture(folder, arm, delay):
    receipt = json.loads((folder / "fixture.json").read_bytes())
    require(receipt["pass"] and receipt["delay_exit_ms"] == delay and not any(receipt["clean"].values()),
            "fixture launch or cleanup failed")
    lines = (folder / "fsync.strace").read_text().splitlines()
    events = []
    pending = {}
    for line in lines:
        tid = line.split(maxsplit=1)[0]
        if "<... fsync resumed>" in line and tid in pending:
            entry, opening = pending.pop(tid)
            events.append((entry, opening + " " + line))
            continue
        match = ACTIVE_SYNC.match(line)
        if match:
            entry = int(match[1])*10**9 + int(match[2])*1000
            if "<unfinished ...>" in line:
                pending[tid] = entry, line
            else:
                events.append((entry, line))
    require(not pending, "unclosed active.log syscall")
    raw = [json.loads(line) for line in (folder / "server-batches.jsonl").read_bytes().splitlines()]
    rows = {(r["local_port"], r["conn_ordinal"]): r for r in raw if "command" in r}
    require(raw[0].get("type") == "meta" and raw[-1].get("type") == "summary" and
            raw[-1]["drops"] == 0 and raw[-1]["seen"] == raw[-1]["kept"] == len(rows) == len(raw)-2,
            "incomplete server phase trace")
    requests = receipt["requests"]
    require(len(requests) == 3, "missing fixture scenarios")
    pairs = []
    scenarios = []
    for ordinal, (request, expected) in enumerate(zip(requests, (None, "SELECT", "INFO")), 1):
        write, read, observer = request["write"], request["read"], request["observer"]
        require(write["command"][:2] == ["SET", "sync-fixture:write"] and write["reply"] == "OK" and
                read["command"] == ["GET", "sync-fixture:read"] and read["reply"] == "ready", "invalid fixture keys")
        reader_select = rows[(read["local_port"], 1)]
        require(reader_select["command"] == "SELECT" and reader_select["response_ok"], "unselected reader")
        writer = paired_call(rows, write, ordinal)
        pairs.append((write, writer))
        observation = None
        if expected:
            require(observer and observer["command"][0] == expected, "invalid observer")
            if expected == "SELECT":
                require(observer["command"] == ["SELECT", "1"] and observer["reply"] == "OK", "wrong SELECT reply")
            else:
                require(observer["command"] == ["INFO"] and "storage_backend:go-log" in observer["reply"], "wrong INFO reply")
            observation = paired_call(rows, observer, 1)
            pairs.append((observer, observation))
        else:
            require(observer is None, "unexpected observer")
        matched = [(entry, line) for entry, line in events
                   if write["start"]["utc_unix_ns"] <= entry <= write["end"]["utc_unix_ns"]]
        require(len(matched) == 1 and [matched[0][0]] == request["db0_active_fsync_entries_utc_ns"],
                "ambiguous active.log syscall")
        entry, line = matched[0]
        require("= 0" in line and ("(DELAYED)" in line) == bool(delay), "unexpected syscall outcome")
        scenarios.append((request, writer, observation, entry))

    # Each client began its TCP call before server parse and received its reply after server send began.
    # Intersect those constraints across all joined calls on this server iteration.
    lower = max(c["start"]["monotonic_before_ns"] - s["parsed_ns"] for c, s in pairs)
    upper = min(c["end"]["monotonic_after_ns"] - s["response_start_ns"] for c, s in pairs)
    require(lower <= upper, "server/client monotonic origin constraints conflict")
    results = []
    for request, writer, observation, entry in scenarios:
        read = request["read"]
        batch = writer["batch"]
        start, finish = batch["sync_start_ns"], batch["sync_end_ns"]
        read_start = read["start"]["monotonic_before_ns"]
        read_end = read["end"]["monotonic_after_ns"]
        earliest_end = read["end"]["monotonic_before_ns"]
        pre_margin = lower + finish - read_end
        post_margin = earliest_end - upper - finish
        details = dict(observer=observation["command"] if observation else None,
                       read_duration_upper_ns=read_end-read_start, db0_fsync_entry_utc_ns=entry,
                       sync_ns=finish-start, earliest_sync_return_minus_latest_read_end_ns=pre_margin,
                       earliest_read_end_minus_latest_sync_return_ns=post_margin)
        if observation:
            details["observer_response_ready_minus_sync_return_ns"] = observation["response_ready_ns"] - finish
            details["observer_gate_wait_ns"] = observation["execution_locked_ns"] - observation["dispatch_ns"]
            details["observer_gate_acquired_minus_sync_return_ns"] = observation["execution_locked_ns"] - finish
        if delay:
            require(finish-start >= delay*10**6 and read_start > upper+start and
                    read["start"]["monotonic_after_ns"] < lower+finish, "read did not begin within held sync")
            if observation:
                require(start < observation["dispatch_ns"] < finish and
                        observation["dispatch_ns"]+upper < read_start, "observer did not precede read during sync")
                if arm == "A":
                    require(observation["execution_locked_ns"] >= finish, "exclusive gate acquired before sync return")
                else:
                    require(observation["execution_locked_ns"] < finish, "shared gate failed to enter during sync")
            if arm == "B" or not observation:
                require(pre_margin > 0, "independent GET did not complete before sync return")
            else:
                require(post_margin > 0, "independent GET completion not proven after sync return")
        results.append(details)
    return dict(arm=arm, delay_ms=delay, server_origin_in_client_monotonic_ns=[lower,upper], samples=results)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("raw", type=Path)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()
    results = {name: fixture(args.raw/name, arm, delay) for name,(arm,delay) in CASES.items()}
    args.out.write_text(json.dumps({"pass": True, "cases": results}, indent=2) + "\n", encoding="utf8")


if __name__ == "__main__":
    main()
