#!/usr/bin/env python3
"""Run separately approved replica experiment blocks on WSL and preserve each attempt."""

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import uuid


TOKEN = "REPLICA_LATENCY_ATTEMPT"
CHECKPOINT_DIR = "KERNIVA_LATENCY_WINDOW_CHECKPOINT_DIR"
GIB = 1 << 30
STOP = False
TRANSIENT_LOG = re.compile(r"address already in use|cannot bind to address", re.IGNORECASE)
TRANSIENT_ERROR = re.compile(
    r"^TimeoutError\(['\"](?:primary ready|source listener ready) did not complete"
    r"|^ConnectionRefusedError\(", re.IGNORECASE,
)


class Blocked(RuntimeError):
    """The block could not safely begin."""


class Busy(Blocked):
    """An exclusive lock is currently held."""


class AuditFailure(ValueError):
    """The block's independent result audit failed."""


def utcnow():
    return datetime.now(timezone.utc)


def read_json(path):
    return json.loads(path.read_bytes())


def sync_dir(path):
    fd = os.open(path, os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def write_json(path, value):
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent, delete=False) as output:
        try:
            json.dump(value, output, indent=2, sort_keys=True)
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
            os.replace(output.name, path)
            sync_dir(path.parent)
        finally:
            Path(output.name).unlink(missing_ok=True)


def digest(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def inputs(paths):
    result = {}
    for path in sorted({Path(value).resolve(strict=True) for value in paths}):
        files = sorted(path.rglob("*")) if path.is_dir() else [path]
        for item in files:
            if item.is_symlink():
                raise Blocked(f"unexpected symbolic link in inputs: {item}")
            if item.is_file():
                result[str(item)] = {"bytes": item.stat().st_size, "sha256": digest(item)}
    return result


def verify_inputs(expected):
    if not isinstance(expected, dict) or not expected:
        raise Blocked("input manifest is empty")
    for name, recorded in expected.items():
        path = Path(name)
        if not path.is_file() or path.stat().st_size != recorded["bytes"] or digest(path) != recorded["sha256"]:
            raise Blocked(f"input differs from its frozen manifest: {path}")


@contextmanager
def exclusive(path, create=False):
    if create:
        fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
    else:
        fd = os.open(path, os.O_RDONLY)
    with os.fdopen(fd, "rb") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise Busy(f"already held: {path}") from error
        yield lock.fileno()


def boot_id():
    return Path("/proc/sys/kernel/random/boot_id").read_text().strip()


def proc_identity(pid):
    fields = (Path("/proc") / str(pid) / "stat").read_text().rsplit(")", 1)[1].split()
    return {"pid": pid, "pgid": int(fields[2]), "session": int(fields[3]), "start_ticks": int(fields[19])}


def owned_groups(token):
    marker = f"{TOKEN}={token}".encode()
    groups = set()
    for proc in Path("/proc").iterdir():
        if not proc.name.isdecimal():
            continue
        try:
            if marker in (proc / "environ").read_bytes().split(b"\x00"):
                identity = proc_identity(int(proc.name))
                if identity["session"] == identity["pgid"]:
                    groups.add(identity["pgid"])
        except (PermissionError, ProcessLookupError, FileNotFoundError):
            continue
    return groups


def signal_group(group, sig):
    try:
        os.killpg(group, sig)
    except ProcessLookupError:
        pass


def clean_attempt(token):
    for group in owned_groups(token):
        signal_group(group, signal.SIGTERM)
    for _ in range(20):
        if not owned_groups(token):
            return
        time.sleep(0.05)
    for group in owned_groups(token):
        signal_group(group, signal.SIGKILL)


def request_stop(_signum, _frame):
    global STOP
    STOP = True


def safe_result_file(folder, relative):
    path = (folder / relative).resolve(strict=True)
    if not path.is_relative_to(folder) or not path.is_file():
        raise ValueError(f"invalid result path: {relative}")
    return path


def check_result(folder, windows, evidence, audit=None):
    folder = folder.resolve(strict=True)
    path = safe_result_file(folder, "result.json")
    result = read_json(path)
    if audit:
        try:
            report = subprocess.run([*audit, str(folder)], capture_output=True, check=True, timeout=120)
        except subprocess.CalledProcessError as error:
            raise AuditFailure(error.stderr.decode(errors="replace").strip()) from error
        except subprocess.TimeoutExpired as error:
            raise AuditFailure("independent result audit timed out") from error
        hashes = json.loads(report.stdout)
        if (result.get("passed") is not True or hashes.get("result.json") != digest(path)
                or any(digest(safe_result_file(folder, name)) != checksum for name, checksum in hashes.items())):
            raise AuditFailure("independent result audit returned mismatching evidence")
        return hashes
    if result.get("passed") is not True or len(result["windows"]) != windows:
        raise ValueError("incomplete or failed block result")
    hashes = {"result.json": digest(path)}
    for row in result["windows"]:
        name = row["file"]
        if name in hashes or len(Path(name).parts) != 2 or Path(name).name != "result.json":
            raise ValueError(f"invalid window result: {name}")
        window = safe_result_file(folder, name)
        data = read_json(window)
        if digest(window) != row["sha256"] or data.get("passed") is not True:
            raise ValueError(f"invalid window result: {window}")
        hashes[name] = row["sha256"]
        for file_name, definition in evidence.items():
            expected = data
            for key in definition["field"].split("."):
                expected = expected.get(key) if isinstance(expected, dict) else None
            if not expected:
                if definition.get("required"):
                    raise ValueError(f"missing window evidence checksum: {definition['field']}")
                continue
            relative = str(Path(name).parent / file_name)
            artifact = safe_result_file(folder, relative)
            hashes[relative] = digest(artifact)
            if hashes[relative] != expected:
                raise ValueError(f"invalid window evidence: {artifact}")
        if checksum := data.get("process", {}).get("log_sha256"):
            relative = f"{Path(name).parent.name}.log"
            log = safe_result_file(folder, relative)
            hashes[relative] = digest(log)
            if hashes[relative] != checksum:
                raise ValueError(f"invalid follower log: {log}")
    return hashes


def disk_space(root, manifest, reserve):
    host_free = shutil.disk_usage(manifest["host_volume"]).free
    ext4_free = shutil.disk_usage(root).free
    run_bytes = int(subprocess.check_output(["du", "-s", "-B1", "--", root]).split()[0])
    space = {
        "checked_at": utcnow().isoformat(), "host_free_bytes": host_free,
        "ext4_free_bytes": ext4_free, "run_bytes": run_bytes, "reserve_bytes": reserve,
    }
    if host_free - reserve < manifest["host_free_min_bytes"]:
        raise Blocked(f"host floor would be crossed: {space}")
    if ext4_free < reserve:
        raise Blocked(f"insufficient ext4 space: {space}")
    if run_bytes + reserve > manifest["maximum_run_bytes"]:
        raise Blocked(f"run disk budget would be crossed: {space}")
    return space


def stop_reason(root, manifest, launch_by=None):
    now = utcnow()
    if now >= datetime.fromisoformat(manifest["deadline_at"]):
        return "deadline"
    if (root / "control" / "paused.json").exists():
        return "paused"
    if STOP:
        return "scheduler_signal"
    if launch_by is not None and now.timestamp() >= launch_by:
        return "window_cutoff"
    return None


def record_checkpoints(attempt, folder):
    previous = attempt.get("window_checkpoints", {})
    current = {path.name: digest(path) for path in sorted((folder / "windows").glob("[0-9][0-9].json"))}
    if any(current.get(name) != checksum for name, checksum in previous.items()):
        raise Blocked("previously recorded window checkpoint changed")
    if current != previous:
        attempt["window_checkpoints"] = current
        return True
    return False


def run_child(command, cwd, attempt, folder, root, manifest, shared_fd, persist, runtime, cutoff):
    token = attempt["token"]
    (folder / "result").mkdir()
    checkpoints = folder / "windows"
    checkpoints.mkdir()
    with (folder / "driver.log").open("wb") as output:
        child = subprocess.Popen(
            [*command, "--root", str(folder / "result")], cwd=cwd,
            stdin=subprocess.DEVNULL, stdout=output, stderr=subprocess.STDOUT, start_new_session=True,
            env={**os.environ, TOKEN: token, CHECKPOINT_DIR: str(checkpoints)}, pass_fds=(shared_fd,),
        )
        try:
            try:
                attempt["process"] = {**proc_identity(child.pid), "boot_id": boot_id()}
            except FileNotFoundError:
                attempt["process"] = {"pid": child.pid, "boot_id": boot_id()}
            persist()
            end = time.monotonic() + runtime
            previous_wall, previous_mono = time.time(), time.monotonic()
            next_space_check = previous_mono + 2
            while child.poll() is None:
                if record_checkpoints(attempt, folder):
                    persist()
                wall, mono = time.time(), time.monotonic()
                wall_gap, monotonic_gap = wall - previous_wall, mono - previous_mono
                if abs(wall_gap - monotonic_gap) >= 2 or max(wall_gap, monotonic_gap) >= 5:
                    attempt.setdefault("time_gaps", []).append({
                        "previous_utc": datetime.fromtimestamp(previous_wall, timezone.utc).isoformat(),
                        "wall_seconds": wall_gap, "monotonic_seconds": monotonic_gap,
                    })
                    persist()
                previous_wall, previous_mono = wall, mono
                reason = stop_reason(root, manifest, cutoff)
                if reason is None and mono >= end:
                    reason = "block_timeout"
                if reason is None and mono >= next_space_check:
                    next_space_check = mono + 2
                    if shutil.disk_usage(manifest["host_volume"]).free < manifest["host_free_min_bytes"]:
                        reason = "host_space_floor"
                    ext4_free = shutil.disk_usage(root).free
                    if ext4_free <= 0:
                        reason = "ext4_exhausted"
                    if ext4_free <= attempt["space_before"]["ext4_free_bytes"] - attempt["space_before"]["reserve_bytes"]:
                        try:
                            disk_space(root, manifest, 0)
                        except Blocked:
                            reason = "disk_budget"
                if reason:
                    signal_group(child.pid, signal.SIGTERM)
                    try:
                        child.wait(timeout=3)
                    except subprocess.TimeoutExpired:
                        signal_group(child.pid, signal.SIGKILL)
                    return child.wait(), reason
                time.sleep(0.25)
            return child.returncode, None
        finally:
            clean_attempt(token)
            if child.poll() is None:
                signal_group(child.pid, signal.SIGKILL)
                child.wait()
            if record_checkpoints(attempt, folder):
                persist()


def classified_failure(folder, expected_windows, exit_code):
    if exit_code < 0:
        return "interrupted"
    result_dir = folder / "result"
    try:
        result = read_json(result_dir / "result.json")
    except (OSError, ValueError):
        result = {}
    if expected_windows and not result.get("windows") and not list(result_dir.glob("*/result.json")):
        if TRANSIENT_ERROR.search(result.get("error", "")):
            return "transient_failure"
        for path in [folder / "driver.log", *result_dir.glob("*.log")]:
            with path.open("rb") as log:
                log.seek(max(0, path.stat().st_size - (64 << 10)))
                if TRANSIENT_LOG.search(log.read().decode(errors="replace")):
                    return "transient_failure"
    return "failed"


def ledger_for(root, manifest):
    path = root / "ledger.json"
    if path.exists():
        ledger = read_json(path)
        if ledger["run_sha256"] != digest(root / "run.json"):
            raise Blocked("run manifest differs from its ledger")
        return path, ledger
    return path, {"run_sha256": digest(root / "run.json"), "run_id": manifest["id"], "blocks": {}, "sessions": []}


def load_spec(root, path):
    spec = read_json(path)
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]*", spec["id"]):
        raise Blocked("invalid block ID")
    if spec["expected_windows"] < 0 or spec["max_runtime_seconds"] <= 0:
        raise Blocked("invalid block runtime or window count")
    if spec.get("reserve_bytes", 0) < 0:
        raise Blocked("invalid block disk reserve")
    command = spec["command"]
    if not command or not Path(command[0]).is_absolute() or not Path(command[0]).is_file():
        raise Blocked("block executable must be an absolute file")
    if (audit := spec.get("audit")) and (not Path(audit[0]).is_absolute() or not Path(audit[0]).is_file()):
        raise Blocked("audit executable must be an absolute file")
    if spec.get("evidence") and any(Path(name).name != name for name in spec["evidence"]):
        raise Blocked("window evidence names must be local files")
    if not Path(spec["cwd"]).resolve(strict=True).is_relative_to(root):
        raise Blocked("block cwd must be inside its run directory")
    return spec


def clear_pause(root, manifest):
    paused = root / "control" / "paused.json"
    if paused.exists() and utcnow() < datetime.fromisoformat(manifest["deadline_at"]):
        requested_at = read_json(paused)["requested_at"]
        paused.unlink()
        sync_dir(paused.parent)
        return requested_at
    return None


def run_block(root, spec_file, resume, not_after=None, verify_only=False):
    manifest = read_json(root / "run.json")
    spec = load_spec(root, spec_file)
    runtime = spec["max_runtime_seconds"]
    expected_windows = spec["expected_windows"]
    evidence = spec.get("evidence", {})
    audit = spec.get("audit")
    with exclusive(root / "control" / "scheduler.lock", create=True):
        path, ledger = ledger_for(root, manifest)
        def persist():
            write_json(path, ledger)

        current_boot = boot_id()
        started_at = utcnow()
        session = {
            "started_at": started_at.isoformat(), "scheduler": {**proc_identity(os.getpid()), "boot_id": current_boot},
            "block_id": spec["id"], "resume_requested": resume, "verify_only": verify_only,
            "scheduler_sha256": digest(Path(__file__)),
        }
        if ledger["sessions"]:
            previous = ledger["sessions"][-1]
            previous_time = previous.get("finished_at", previous["started_at"])
            gap = (started_at - datetime.fromisoformat(previous_time)).total_seconds()
            if previous["scheduler"]["boot_id"] != current_boot or "finished_at" not in previous:
                session["unobserved_gap"] = {"since": previous_time, "wall_seconds": gap,
                                             "boot_changed": previous["scheduler"]["boot_id"] != current_boot}
        ledger["sessions"].append(session)
        persist()
        try:
            for block in ledger["blocks"].values():
                for attempt in block["attempts"]:
                    if attempt["status"] == "running":
                        clean_attempt(attempt["token"])
                        record_checkpoints(attempt, Path(attempt["result_dir"]).parent)
                        attempt.update(status="interrupted", finished_at=utcnow().isoformat(),
                                       exit_code=None, reason="scheduler_disappeared")
                        persist()
            if resume and utcnow() < datetime.fromisoformat(manifest["deadline_at"]):
                paused_since = clear_pause(root, manifest)
                if paused_since:
                    session["paused_since"] = paused_since
            input_file = root / "control" / "inputs.json"
            input_sha = digest(input_file)
            previous_input_shas = {
                attempt["input_manifest_sha256"] for block in ledger["blocks"].values()
                for attempt in block["attempts"]
            }
            previous_input_shas.add(ledger.get("input_manifest_sha256", input_sha))
            if previous_input_shas != {input_sha}:
                raise Blocked("frozen input manifest differs from the ledger")
            ledger["input_manifest_sha256"] = input_sha
            hashes = read_json(input_file)
            with exclusive(Path(manifest["performance_lock"])):
                verify_inputs(hashes)
                if spec.get("inputs"):
                    verify_inputs(spec["inputs"])
            if manifest.get("require_approval", True) and not spec.get("approval"):
                raise Blocked("block has no recorded approval")
            block = ledger["blocks"].setdefault(spec["id"], {
                "phase": spec["phase"], "spec_sha256": digest(spec_file), "spec_file": str(spec_file), "attempts": [],
            })
            if block["spec_sha256"] != digest(spec_file):
                raise Blocked("block specification changed, use a new block ID")
            for attempt in reversed(block["attempts"]):
                if attempt["status"] != "passed":
                    continue
                try:
                    recorded = attempt["result_hashes"]
                    with exclusive(Path(manifest["performance_lock"])):
                        checked = check_result(Path(attempt["result_dir"]), expected_windows, evidence, audit)
                    valid = all(checked.get(name) == checksum for name, checksum in recorded.items())
                except (OSError, ValueError, KeyError):
                    valid = False
                if valid:
                    session.update(status="skipped", attempt=attempt["number"])
                    return "skipped"
                attempt.update(status="damaged", invalidated_at=utcnow().isoformat())
                persist()
            if verify_only:
                session["status"] = "unverified"
                return None
            if block["attempts"] and block["attempts"][-1]["status"] == "failed":
                raise Blocked("previous failure retained for explicit review, use a new block ID for approved work")

            cutoff = datetime.fromisoformat(manifest["window_cutoff_at"]).timestamp()
            if not_after is not None:
                cutoff = min(cutoff, not_after)
            retries = 2 - sum(attempt["status"] == "transient_failure" for attempt in block["attempts"])
            if not retries:
                raise Blocked("transient retry already used")
            for _ in range(retries):
                reason = stop_reason(root, manifest, cutoff - runtime)
                if reason:
                    raise Blocked(reason)
                with exclusive(Path(manifest["performance_lock"])) as shared_fd:
                    space = disk_space(root, manifest, spec.get("reserve_bytes", manifest["default_block_reserve_bytes"]))
                    reason = stop_reason(root, manifest, cutoff - runtime)
                    if reason:
                        raise Blocked(reason)
                    number = len(block["attempts"]) + 1
                    folder = root / "blocks" / spec["id"] / f"attempt-{number:03}"
                    folder.mkdir(parents=True)
                    attempt = {
                        "number": number, "status": "running", "token": uuid.uuid4().hex,
                        "started_at": utcnow().isoformat(), "boot_id": current_boot, "space_before": space,
                        "input_manifest_sha256": input_sha,
                        "spec_sha256": block["spec_sha256"], "result_dir": str(folder / "result"),
                    }
                    block["attempts"].append(attempt)
                    persist()
                    code, reason = run_child(spec["command"], spec["cwd"], attempt, folder, root, manifest,
                                             shared_fd, persist, runtime, cutoff)
                    attempt.update(exit_code=code, finished_at=utcnow().isoformat())
                    result_file = folder / "result" / "result.json"
                    if result_file.is_file():
                        attempt["result_sha256"] = digest(result_file)
                    if reason:
                        attempt.update(status="interrupted", reason=reason)
                    elif code == 0:
                        try:
                            hashes = check_result(folder / "result", expected_windows, evidence, audit)
                            attempt.update(status="passed", result_sha256=hashes["result.json"], result_hashes=hashes)
                        except AuditFailure as error:
                            attempt.update(status="failed", reason=str(error))
                        except (OSError, ValueError, KeyError) as error:
                            attempt.update(status="damaged", reason=str(error))
                    else:
                        attempt["status"] = classified_failure(folder, expected_windows, code)
                    try:
                        attempt["space_after"] = disk_space(root, manifest, 0)
                    except Blocked as error:
                        attempt["space_after_error"] = str(error)
                    persist()
                if attempt["status"] != "transient_failure":
                    break
            session.update(status=attempt["status"], attempt=attempt["number"])
            if attempt["status"] != "passed":
                raise Blocked(f"attempt {attempt['number']} {attempt['status']}: {attempt.get('reason', 'see driver.log')}")
            return "passed"
        except (Blocked, OSError, ValueError) as error:
            session.setdefault("status", "blocked")
            session["error"] = str(error)
            raise
        finally:
            session["finished_at"] = utcnow().isoformat()
            persist()


def run_queue(root, queue_file, resume):
    manifest = read_json(root / "run.json")
    queue = read_json(queue_file)
    name = queue["id"]
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]*", name) or not queue.get("approval"):
        raise Blocked("invalid or unapproved queue")
    queue_digest = digest(queue_file)
    control = root / "control"
    state_file = control / f"{name}-state.json"
    with exclusive(control / "queue.lock", create=True):
        state = read_json(state_file) if state_file.exists() else {
            "queue_sha256": queue_digest, "slots": {}, "sessions": [],
        }
        if state["queue_sha256"] != queue_digest:
            raise Blocked("queue specification changed")
        previous_end = None
        seen_specs = set()
        seen_slots = set()
        cutoff = datetime.fromisoformat(manifest["window_cutoff_at"])
        for slot in queue["slots"]:
            start = datetime.fromisoformat(slot["not_before"])
            end = datetime.fromisoformat(slot["not_after"])
            if (not re.fullmatch(r"[a-z0-9][a-z0-9-]*", slot["id"]) or end <= start or end > cutoff
                    or slot["id"] in seen_slots or not slot["blocks"]
                    or (previous_end is not None and start < previous_end)):
                raise Blocked(f"invalid or overlapping slot: {slot['id']}")
            seen_slots.add(slot["id"])
            previous_end = end
            for item in slot["blocks"]:
                spec_file = safe_result_file(control, item["file"])
                if spec_file in seen_specs or digest(spec_file) != item["sha256"]:
                    raise Blocked(f"invalid or changed queued block: {spec_file}")
                seen_specs.add(spec_file)
        session = {"started_at": utcnow().isoformat(), "boot_id": boot_id(), "pid": os.getpid()}
        if state["sessions"]:
            previous = state["sessions"][-1]
            if previous["boot_id"] != session["boot_id"] or "finished_at" not in previous:
                session["unobserved_since"] = previous.get("finished_at", previous["started_at"])
        if resume:
            with exclusive(control / "scheduler.lock", create=True):
                paused = clear_pause(root, manifest)
            if paused:
                session["paused_since"] = paused
        state["sessions"].append(session)
        write_json(state_file, state)
        try:
            for slot in queue["slots"]:
                start = datetime.fromisoformat(slot["not_before"])
                end = datetime.fromisoformat(slot["not_after"])
                entry = state["slots"].setdefault(slot["id"], {
                    "not_before": slot["not_before"], "not_after": slot["not_after"], "blocks": {},
                })
                session["waiting_for"] = slot["id"]
                write_json(state_file, state)
                while utcnow() < start and not stop_reason(root, manifest):
                    before_wall, before_mono = time.time(), time.monotonic()
                    time.sleep(min(15, max(0, (start - utcnow()).total_seconds())))
                    wall_gap, mono_gap = time.time() - before_wall, time.monotonic() - before_mono
                    if abs(wall_gap - mono_gap) >= 2 or max(wall_gap, mono_gap) >= 30:
                        session.setdefault("time_gaps", []).append({
                            "previous_utc": datetime.fromtimestamp(before_wall, timezone.utc).isoformat(),
                            "wall_seconds": wall_gap, "monotonic_seconds": mono_gap,
                        })
                        write_json(state_file, state)
                reason = stop_reason(root, manifest)
                if reason:
                    raise Blocked(reason)
                for item in slot["blocks"]:
                    spec_file = safe_result_file(control, item["file"])
                    spec = load_spec(root, spec_file)
                    record = entry["blocks"].setdefault(spec["id"], {})
                    if record.get("status") == "missed":
                        continue
                    while not stop_reason(root, manifest):
                        if digest(spec_file) != item["sha256"]:
                            raise Blocked(f"queued block changed: {spec_file}")
                        late = utcnow().timestamp() + spec["max_runtime_seconds"] >= end.timestamp()
                        try:
                            status = run_block(root, spec_file, False, end.timestamp(), verify_only=late)
                            record.update(status=status or "missed", recorded_at=utcnow().isoformat())
                            if status and queue.get("archive_root"):
                                record["mirror"] = str(mirror_block(
                                    root, spec["id"], Path(queue["archive_root"]),
                                    reclaim=queue.get("reclaim_direct_payload", False),
                                ))
                            break
                        except Busy as error:
                            record.update(last_busy_at=utcnow().isoformat(), busy_reason=str(error))
                            write_json(state_file, state)
                            time.sleep(15)
                    else:
                        raise Blocked(stop_reason(root, manifest))
                    write_json(state_file, state)
            session.pop("waiting_for", None)
            session["status"] = "finished"
        except (Blocked, OSError, ValueError) as error:
            session.update(status="blocked", error=str(error))
            raise
        finally:
            session["finished_at"] = utcnow().isoformat()
            write_json(state_file, state)


def verify_mirror(destination, expected):
    index = read_json(destination / "evidence.json")
    if any(index["files"].get(f"result/{name}") != checksum for name, checksum in expected.items()):
        raise Blocked("mirror is missing required evidence")
    for rel, checksum in index["files"].items():
        if digest(safe_result_file(destination, rel)) != checksum:
            raise Blocked(f"mirror checksum mismatch: {rel}")
    return digest(destination / "evidence.json")


def copy_evidence(root, block, attempt, folder, destination):
    result = folder / "result"
    candidates = {
        folder / "driver.log": "driver.log",
        root / "run.json": "control/run.json",
        root / "control" / "inputs.json": "control/inputs.json",
    }
    spec = Path(block.get("spec_file", root / "control" / f"{folder.parent.name}.json"))
    if digest(spec) != block["spec_sha256"]:
        raise Blocked("frozen block specification changed")
    candidates[spec] = "control/block.json"
    for path in [*result.glob("*"), *result.glob("*/*")]:
        if path.is_file():
            candidates[path] = str(path.relative_to(folder))
    for name in attempt["result_hashes"]:
        path = safe_result_file(result, name)
        candidates[path] = str(path.relative_to(folder))
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=destination.parent, prefix=".mirror-") as temporary:
        staging = Path(temporary) / folder.name
        staging.mkdir()
        kept = {}
        for source, rel in candidates.items():
            output = staging / rel
            output.parent.mkdir(exist_ok=True, parents=True)
            shutil.copyfile(source, output)
            with output.open("rb") as copied:
                os.fsync(copied.fileno())
            kept[rel] = digest(source)
        write_json(staging / "evidence.json", {"files": kept, "block": folder.parent.name, "attempt": attempt})
        verify_mirror(staging, attempt["result_hashes"])
        staging.rename(destination)
        sync_dir(destination.parent)


def reclaim_direct_payload(folder):
    result = folder / "result"
    block = read_json(result / "result.json")
    if block.get("db") != 15 or block.get("keys") != 256 or len(block["windows"]) != 8:
        raise Blocked("reclamation requires a verified eight-window DB 15 direct block")
    payloads = [result / "primary-data"]
    for window in block["windows"]:
        path = safe_result_file(result, window["file"])
        payloads.append(path.parent / "b")
    for path in payloads:
        if path.is_symlink() or not path.resolve().is_relative_to(result):
            raise Blocked(f"invalid payload path: {path}")
    removed = []
    for path in payloads:
        if path.is_dir():
            shutil.rmtree(path)
            sync_dir(path.parent)
            removed.append(str(path.relative_to(folder)))
    return removed


def mirror_block(root, block_id, target, reclaim=False):
    manifest = read_json(root / "run.json")
    with exclusive(root / "control" / "scheduler.lock", create=True), exclusive(Path(manifest["performance_lock"])):
        ledger_file, ledger = ledger_for(root, manifest)
        block = ledger["blocks"][block_id]
        attempt = next(item for item in reversed(block["attempts"]) if item["status"] == "passed")
        folder = Path(attempt["result_dir"]).parent
        expected = attempt["result_hashes"]
        result = folder / "result"
        actual = {name: digest(safe_result_file(result, name)) for name in expected}
        if actual != expected:
            raise Blocked("block evidence differs from the ledger")
        destination = target.resolve() / "blocks" / block_id / folder.name
        if not destination.exists():
            copy_evidence(root, block, attempt, folder, destination)
        mirror_sha = verify_mirror(destination, expected)
        attempt["mirror"] = {"directory": str(destination), "sha256": mirror_sha, "verified_at": utcnow().isoformat()}
        write_json(ledger_file, ledger)
        if reclaim:
            removed = reclaim_direct_payload(folder)
            if removed:
                attempt["reclaimed"] = {"paths": removed, "at": utcnow().isoformat()}
                write_json(ledger_file, ledger)
        return destination


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="operation", required=True)
    run = sub.add_parser("run")
    run.add_argument("--root", type=Path, required=True)
    run.add_argument("--block", type=Path, required=True)
    run.add_argument("--resume", action="store_true")
    queue = sub.add_parser("queue")
    queue.add_argument("--root", type=Path, required=True)
    queue.add_argument("--queue", type=Path, required=True)
    queue.add_argument("--resume", action="store_true")
    pause = sub.add_parser("pause")
    pause.add_argument("--root", type=Path, required=True)
    freeze = sub.add_parser("fingerprint")
    freeze.add_argument("--output", type=Path)
    freeze.add_argument("paths", nargs="+")
    mirror = sub.add_parser("mirror")
    mirror.add_argument("--root", type=Path, required=True)
    mirror.add_argument("--block", required=True)
    mirror.add_argument("--target", type=Path, required=True)
    mirror.add_argument("--reclaim-direct-payload", action="store_true")
    args = parser.parse_args()
    os.umask(0o077)
    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    try:
        if args.operation == "run":
            run_block(args.root.resolve(strict=True), args.block.resolve(strict=True), args.resume)
        elif args.operation == "queue":
            run_queue(args.root.resolve(strict=True), args.queue.resolve(strict=True), args.resume)
        elif args.operation == "pause":
            write_json(args.root / "control" / "paused.json", {"requested_at": utcnow().isoformat()})
        elif args.operation == "mirror":
            print(mirror_block(args.root.resolve(strict=True), args.block, args.target, args.reclaim_direct_payload))
        else:
            fingerprints = inputs(args.paths)
            if args.output:
                if args.output.exists():
                    raise Blocked(f"input manifest already exists: {args.output}")
                write_json(args.output, fingerprints)
            else:
                print(json.dumps(fingerprints, indent=2, sort_keys=True))
    except (Blocked, OSError, ValueError, KeyError, StopIteration) as error:
        print(str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
