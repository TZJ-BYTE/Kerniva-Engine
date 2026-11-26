#!/usr/bin/env python3
"""Compare the baseline, INFO/SELECT fix, and final PING fix under a held fsync."""

import argparse
import fcntl
import os
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace

from golog_batch_capture import gc, slow_sync_fixture


def measured_outcome(fixture):
    results = {}
    for row in fixture["requests"]:
        hold_end = row["db0_active_fsync_entries_utc_ns"][0] + fixture["delay_exit_ms"] * 10**6
        observer = row["observer"]
        operations = ("write", "read", "observer") if observer else ("write", "read")
        results["PING" if observer else "none"] = {
            "read_completed_during_hold": row["read"]["end"]["utc_unix_ns"] < hold_end,
            "ping_completed_during_hold": observer["end"]["utc_unix_ns"] < hold_end if observer else None,
            "elapsed_ns": {name: row[name]["end"]["monotonic_after_ns"] - row[name]["start"]["monotonic_before_ns"]
                           for name in operations},
        }
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--setup", type=Path, required=True)
    parser.add_argument("--a-bin", type=Path, required=True)
    parser.add_argument("--b-bin", type=Path, required=True)
    parser.add_argument("--c-bin", type=Path, required=True)
    parser.add_argument("--a-revision", required=True)
    parser.add_argument("--b-revision", required=True)
    parser.add_argument("--c-revision", required=True)
    parser.add_argument("--mirror", type=Path, required=True)
    args = parser.parse_args()

    setup = gc.live.read_json(args.setup)
    home = Path.home()
    if gc.live.check.RUNNER.filesystem_type(home) != "ext4":
        raise RuntimeError("capture requires WSL ext4")
    prohibited = {"GODEBUG", "GOGC", "GOMEMLIMIT", "GOMAXPROCS", "GOTRACEBACK", "KERNIVA_GOLOG_BATCH_TRACE"}
    if inherited := prohibited & os.environ.keys():
        raise RuntimeError(f"unexpected tracing or Go runtime options: {sorted(inherited)}")
    bins = dict(A=args.a_bin.resolve(), B=args.b_bin.resolve(), C=args.c_bin.resolve())
    revisions = dict(A=args.a_revision, B=args.b_revision, C=args.c_revision)
    reference = gc.verified_group(Path(setup["root"]) / "bin", args.setup.parent / "diagnostic-binaries.sha256")
    binaries = {arm: {name: gc.stall.digest(path / name) for name in reference} for arm, path in bins.items()}
    for name in set(reference) - {"kerniva-server"}:
        if len({binaries[arm][name] for arm in bins}) != 1:
            raise RuntimeError(f"binary mismatch across variants for {name}")
    if len({binaries[arm]["kerniva-server"] for arm in bins}) != len(bins):
        raise RuntimeError("each variant must have a distinct primary binary")

    receipt = {
        "argv": sys.argv, "setup": setup, "start": gc.stall.clock(), "batch_trace": False,
        "runner_sha256": {name: gc.stall.digest(Path(__file__).with_name(name)) for name in
                          (Path(__file__).name, "golog_batch_capture.py")},
        "variants": {arm: {"revision": revisions[arm], "bin": str(bins[arm]), "binaries": binaries[arm]} for arm in bins},
        "differences": {"A": "baseline", "B": "A plus INFO/SELECT sharing", "C": "B plus PING sharing"},
        "cases": {},
    }
    status = 2
    with Path(setup["performance_lock"]).open("rb") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        mirror = args.mirror.resolve()
        mirror.mkdir(parents=True, exist_ok=False)
        root = Path(tempfile.mkdtemp(prefix="kerniva-ping-gate.", dir=home))
        print(f"ext4 {root}", flush=True)
        try:
            for arm, path in bins.items():
                options = SimpleNamespace(bin=path, fixture_trace=False,
                                          child_process_options=gc.settled.child_process_options())
                fixture = slow_sync_fixture(root, options, f"controlled-{arm}-ping-gate", 350, (None, ("PING",)))
                outcomes = measured_outcome(fixture)
                receipt["cases"][arm] = outcomes
                if (not fixture["pass"] or not outcomes["none"]["read_completed_during_hold"]
                        or outcomes["PING"]["read_completed_during_hold"] != (arm == "C")
                        or outcomes["PING"]["ping_completed_during_hold"] != (arm == "C")):
                    raise RuntimeError(f"unexpected independently observed PING gate behavior for arm {arm}")
            status = 0
        finally:
            receipt.update(end=gc.stall.clock(), driver_exit_code=status)
            gc.settled.write_json(root / "run.json", receipt)
            gc.mirror_evidence(root, mirror, status)
    return status


if __name__ == "__main__":
    sys.exit(main())
