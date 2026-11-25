#!/usr/bin/env python3
"""Run the conditionally approved no-slow-window follow-up from a frozen report."""

import argparse
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

from golog_replica_latency_run import (
    Blocked, Busy, boot_id, check_result, clear_pause, digest, exclusive, load_spec, mirror_block,
    read_json, request_stop, run_block, stop_reason, utcnow, verify_mirror, write_json,
)


EXIT_CODES = {"finished": 0, "review_required": 2, "paused": 75}


def wait_until(start, root, manifest, session):
    while utcnow() < start and not stop_reason(root, manifest):
        wall, monotonic = time.time(), time.monotonic()
        time.sleep(min(15, max(0, (start - utcnow()).total_seconds())))
        elapsed, interval = time.time() - wall, time.monotonic() - monotonic
        if abs(elapsed - interval) >= 2 or max(elapsed, interval) >= 30:
            session.setdefault("time_gaps", []).append({
                "previous_utc": datetime.fromtimestamp(wall, timezone.utc).isoformat(),
                "wall_seconds": elapsed, "monotonic_seconds": interval,
            })
    if reason := stop_reason(root, manifest):
        raise Blocked(reason)


def attached_file(record):
    path = Path(record["file"]).resolve(strict=True)
    if digest(path) != record["sha256"]:
        raise Blocked(f"approved input changed: {path}")
    return path


def analyse(root, manifest, config, output, extra=None):
    analyser = attached_file(config["analyser"])
    fixture = attached_file(config["fixture"])
    attached_file(config["approval"])
    attached_file(config["formal_queue"])
    command = ["/usr/bin/python3", "-B", str(analyser), "--fixture", str(fixture), "--schedule", str(root)]
    if extra:
        block, directory = extra
        command += ["--block", "supplement", block, "formal", str(directory)]
    with exclusive(Path(manifest["performance_lock"])):
        result = subprocess.run(command, capture_output=True, timeout=120)
    if result.returncode:
        raise Blocked(f"independent recomputation failed: {result.stderr.decode(errors='replace').strip()}")
    report = json.loads(result.stdout)
    write_json(output, report)
    return report


def eligibility(report):
    status = report["schedule"]
    return (status["eligible_for_no_slow_supplement"] is True
            and status["all_formal_evidence_valid"] is True
            and status["valid_complete_windows"] == 48 and status["slow_in_complete_windows"] == 0)


def verified_attempt(root, name, start, end):
    attempts = read_json(root / "ledger.json")["blocks"][name]["attempts"]
    if len(attempts) != 1 or attempts[0]["status"] != "passed" or attempts[0]["exit_code"] != 0:
        raise Blocked(f"{name} has incomplete, failed, or repeated attempts")
    attempt = attempts[0]
    first, last = (datetime.fromisoformat(attempt[key]) for key in ("started_at", "finished_at"))
    if not start <= first <= last <= end or attempt.get("time_gaps"):
        raise Blocked(f"{name} ran outside its period or during a time discontinuity")
    return attempt


def verify_completed(root, manifest, config, state, start, end):
    control = root / "control"
    archive = Path(config["archive_root"]).resolve(strict=True)
    with exclusive(control / "scheduler.lock", create=True), exclusive(Path(manifest["performance_lock"])):
        for item in config["blocks"]:
            spec = load_spec(root, attached_file(item))
            name = spec["id"]
            saved = state["blocks"][name]
            attempt = verified_attempt(root, name, start, end)
            checked = check_result(Path(attempt["result_dir"]), spec["expected_windows"], spec["evidence"])
            mirror = archive / "blocks" / name / f"attempt-{attempt['number']:03}"
            if (saved["valid_windows"] != 8 or saved["slow"] != 0
                    or checked != attempt["result_hashes"]
                    or checked["result.json"] != attempt["result_sha256"]
                    or checked["result.json"] != saved["result_sha256"]
                    or Path(saved["mirror"]) != mirror
                    or Path(attempt["mirror"]["directory"]) != mirror
                    or verify_mirror(mirror, checked) != attempt["mirror"]["sha256"]):
                raise Blocked(f"completed follow-up evidence differs: {name}")
            attached_file(state["audits"][name])
        attached_file(state["audits"]["formal"])
        attached_file(state["audits"]["before_start"])


def followup(root, configuration, resume):
    manifest = read_json(root / "run.json")
    configuration_sha = digest(configuration)
    config = read_json(configuration)
    archive = Path(config["archive_root"])
    control = root / "control"
    state_path = control / "no-slow-followup-state.json"
    start = datetime.fromisoformat(manifest["created_at"])
    gate, slot, end = (start + timedelta(hours=h) for h in (11, 13, 15))
    if len(config["blocks"]) != 2:
        raise Blocked("approval requires the two registered follow-up blocks")
    for record in (config["approval"], config["formal_queue"], config["analyser"], config["fixture"], *config["blocks"]):
        attached_file(record)

    with exclusive(control / "no-slow-followup.lock", create=True):
        state = read_json(state_path) if state_path.exists() else {
            "configuration_sha256": configuration_sha, "sessions": [], "blocks": {},
            "gate_at": gate.isoformat(), "slot_opens_at": slot.isoformat(), "slot_closes_at": end.isoformat(),
        }
        if state["configuration_sha256"] != configuration_sha:
            raise Blocked("follow-up configuration changed")
        if state.get("status") == "finished":
            verify_completed(root, manifest, config, state, slot, end)
            print("finished")
            return EXIT_CODES["finished"]
        if state.get("status") == "review_required":
            print(state.get("error", "review required"), file=sys.stderr)
            return EXIT_CODES["review_required"]
        session = {"started_at": utcnow().isoformat(), "pid": os.getpid(), "boot_id": boot_id(),
                   "controller_sha256": digest(Path(__file__))}
        if state["sessions"]:
            previous = state["sessions"][-1]
            if prior_error := state.pop("error", None):
                previous.setdefault("error", prior_error)
            if previous["boot_id"] != session["boot_id"] or "finished_at" not in previous:
                session["unobserved_since"] = previous.get("finished_at", previous["started_at"])
        if resume:
            with exclusive(control / "scheduler.lock", create=True):
                if paused := clear_pause(root, manifest):
                    session["paused_since"] = paused
        state["sessions"].append(session)
        state["status"] = "waiting_for_formal_review"
        write_json(state_path, state)

        def publish():
            mirror_control = archive / "control"
            mirror_control.mkdir(parents=True, exist_ok=True)
            write_json(mirror_control / state_path.name, state)
            for record in state.get("audits", {}).values():
                raw = attached_file(record)
                write_json(mirror_control / raw.name, read_json(raw))

        def audit(name, extra=None):
            path = control / f"no-slow-audit-{name}-{len(state['sessions']):03}.json"
            while utcnow() < end and not stop_reason(root, manifest):
                try:
                    report = analyse(root, manifest, config, path, extra)
                    state.setdefault("audits", {})[name] = {"file": str(path), "sha256": digest(path)}
                    write_json(state_path, state)
                    return report
                except Busy:
                    time.sleep(15)
            raise Blocked(stop_reason(root, manifest) or "follow-up slot expired before independent audit")

        try:
            wait_until(gate, root, manifest, session)
            if not eligibility(audit("formal")):
                raise Blocked("the three original periods did not produce independently valid 0 / 48 evidence")
            state["status"] = "waiting_for_supplement_slot"
            write_json(state_path, state)
            publish()
            wait_until(slot, root, manifest, session)
            if not eligibility(audit("before_start")):
                raise Blocked("original formal evidence no longer qualifies")
            state["status"] = "running"
            write_json(state_path, state)
            for item in config["blocks"]:
                spec_file = attached_file(item)
                name = load_spec(root, spec_file)["id"]
                while utcnow() < end and not stop_reason(root, manifest):
                    try:
                        status = run_block(root, spec_file, False, end.timestamp())
                        attempt = verified_attempt(root, name, slot, end)
                        path = mirror_block(root, name, archive)
                        break
                    except Busy:
                        time.sleep(15)
                else:
                    raise Blocked(stop_reason(root, manifest) or "follow-up slot expired")
                state["blocks"][name] = {"status": status, "mirror": str(path),
                                         "start": attempt["started_at"], "end": attempt["finished_at"],
                                         "result_sha256": attempt["result_sha256"]}
                write_json(state_path, state)
                report = audit(name, (name, Path(attempt["result_dir"])))
                checked, = [row for row in report["blocks"] if row["block"] == name]
                state["blocks"][name].update(valid_windows=checked["valid_windows"], slow=checked["slow_installs"])
                write_json(state_path, state)
                if not eligibility(report) or checked["valid_windows"] != 8 or checked["slow_installs"]:
                    raise Blocked(f"{name} needs review before any further work")
                mirror_block(root, name, archive, reclaim=True)
            state["status"] = "finished"
        except (Blocked, OSError, ValueError, KeyError, subprocess.SubprocessError) as error:
            reason = stop_reason(root, manifest)
            state.update(status="paused" if reason in {"paused", "scheduler_signal"} else "review_required", error=str(error))
            print(str(error), file=sys.stderr)
        finally:
            session["finished_at"] = utcnow().isoformat()
            write_json(state_path, state)
            publish()
        return EXIT_CODES[state["status"]]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()
    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    try:
        return followup(args.root.resolve(strict=True), args.config.resolve(strict=True), args.resume)
    except (Blocked, OSError, ValueError, KeyError) as error:
        print(str(error), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
