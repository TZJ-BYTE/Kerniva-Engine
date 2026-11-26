"""Compare one-second and default log rotation on the same durable gnet primary."""
import argparse
from datetime import timedelta
import fcntl
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
from unittest.mock import patch

from golog_batch_capture import recovery
from golog_batch_report import require
from golog_group_limit_check import PassiveWindow, gc, run_window


DURATION = 65
# Reverse the age ordering between loads as well as within each pair.
PLAN = [(95, 6000, age) for age in (1, 30, 30, 1)] + [(50, 3000, age) for age in (30, 1, 1, 30)]


def primary_for_age(age):
    class Primary(gc.TOPOLOGY):
        def start(self, label, *cmd, env=None):
            config = Path(cmd[-1])
            settings = gc.live.read_json(config)
            settings["go_log_segment_age_seconds"] = age
            gc.settled.write_json(config, settings)
            if label == "primary":
                env = {**(self.env if env is None else env),
                       "KERNIVA_GOLOG_BATCH_TRACE": str(self.root / "server-batches.jsonl")}
            return super().start(label, *cmd, env=env)
    return Primary


class SealCounts(PassiveWindow):
    def __init__(self, folder, storage, *_args, **_options):
        self.folder = folder
        self.storage = storage

    def snapshot(self):
        start = gc.stall.clock()
        segments = {db: sorted(path.name for path in (self.storage / f"db_{db}").glob("log-*.seg"))
                    for db in gc.live.DATABASES}
        return {"start": start, "segments": segments, "end": gc.stall.clock()}

    def start(self):
        self.before = self.snapshot()
        return super().start()

    def finish(self):
        result = {"before_release": self.before, "after_client": self.snapshot()}
        gc.settled.write_json(self.folder / "sealed-files.json", result)
        return result


def run_case(root, options, position, read_percent, rate, age, ports):
    folder = root / f"{position:02d}-r{read_percent}-q{rate}-A"

    def bench(argv, **kwargs):
        argv.extend(("-request-trace-out", str(folder / "client-trace.jsonl"), "-request-trace-stride", str(gc.STRIDE)))
        return subprocess.Popen(argv, **kwargs)

    with patch.object(gc.live, "subprocess", SimpleNamespace(Popen=bench, DEVNULL=subprocess.DEVNULL)):
        result = run_window(root, options, position, (read_percent, rate), 8, ports,
                            topology=primary_for_age(age), observer_factory=SealCounts)
    age_seen = gc.live.read_json(folder / "config.json")["go_log_segment_age_seconds"]
    result.update(age_seconds=age, batch_trace=True, gate_arm="C")
    result["pass"] = result["pass"] and age_seen == age and (folder / "sealed-files.json").is_file()
    gc.settled.write_json(folder / "capture.json", result)
    require(result["pass"], f"{folder}: incomplete or invalid capture")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--setup", type=Path, required=True)
    parser.add_argument("--bin", type=Path, required=True)
    parser.add_argument("--binary-manifest", type=Path, required=True)
    parser.add_argument("--mirror", type=Path, required=True)
    args = parser.parse_args()
    setup = gc.live.read_json(args.setup)
    bins = args.bin.resolve()
    binaries = gc.verified_group(bins, args.binary_manifest)
    require(set(binaries) == set(gc.live.BINARIES), "missing frozen binary")
    require(gc.live.check.RUNNER.filesystem_type(Path.home()) == "ext4", "WSL ext4 required")
    require(not {"GOGC", "GODEBUG", "GOMEMLIMIT", "GOMAXPROCS", "KERNIVA_GOLOG_BATCH_TRACE", "LD_PRELOAD"}
            .intersection(os.environ), "unexpected runtime override")
    mirror = args.mirror.resolve()
    with Path(setup["performance_lock"]).open("rb") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        mirror.mkdir(parents=True, exist_ok=False)
        root = Path(tempfile.mkdtemp(prefix="kerniva-rotation.", dir=Path.home()))
        options = SimpleNamespace(bin=bins, stage="rotation", warmup=gc.WARMUP, duration=DURATION,
                                  stop_opening=gc.live.now() + timedelta(minutes=18),
                                  child_process_options=gc.settled.child_process_options())
        sources = [Path(__file__), Path(__file__).with_name("golog_group_limit_check.py")]
        result = {"ext4": str(root), "argv": sys.argv, "plan": PLAN, "binaries": binaries, "duration": DURATION,
                  "scripts": {path.name: gc.stall.digest(path) for path in sources},
                  "warmup": gc.WARMUP, "start": gc.stall.clock(), "windows": [],
                  "decision": "Both load profiles use reversed adjacent pairs. Require zero failures and "
                              "confirmed recovery for both ages. Report per-window tails, actual seals and "
                              "cutoff rate; do not classify a sporadic tail as fixed with only two pairs."}
        gc.settled.write_json(mirror / "plan.json", result)
        print(f"ext4 {root}", flush=True)
        status = 2
        try:
            ports = set()
            for i, (read_percent, rate, age) in enumerate(PLAN, 1):
                window = run_case(root, options, i, read_percent, rate, age, ports)
                result["windows"].append(window)
                gc.settled.write_json(root / "run.json", result)
                print(f"{i}: age {age}, {window['succeeded']} successful, {window['failures']} failed", flush=True)
            for age in (1, 30):
                folder = root / f"recovery-age-{age}"
                folder.mkdir()
                with patch.object(gc.live, "Topology", primary_for_age(age)):
                    recovered = recovery(folder, options)
                config = gc.live.read_json(folder / "primary-crash-recovery" / "config.json")
                require(recovered["pass"] and config["go_log_segment_age_seconds"] == age,
                        f"age {age} recovery failed")
                # The common mirror copies one level below each run directory.
                gc.settled.write_json(folder / "config.json", config)
                gc.settled.write_json(folder / "recovery.json", recovered)
            status = 0
        finally:
            result.update(driver_exit_code=status, end=gc.stall.clock())
            gc.settled.write_json(root / "run.json", result)
            gc.mirror_evidence(root, mirror, status)


if __name__ == "__main__":
    main()
