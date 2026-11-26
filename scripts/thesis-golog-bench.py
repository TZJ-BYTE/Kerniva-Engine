#!/usr/bin/env python3
"""Run reproducible Go-log, previous Kerniva, and Redis TCP comparisons on ext4."""

import argparse
from collections import defaultdict
import csv
import importlib.util
import json
import os
from pathlib import Path
import shlex
import shutil
import signal
import subprocess
import sys
import tempfile
import time

SCRIPTS = Path(__file__).resolve().parent
REPO = SCRIPTS.parent
RUNNER = SCRIPTS / "thesis-bench-linux.py"
ANALYZER = SCRIPTS / "thesis-analyze.py"

PROFILES = {
    "smoke": {"keys": 256, "clients": 4, "rounds": 1, "duration": 2, "warmup": 0,
              "rates": {"memory": [500], "none": [250], "sync": [250]}, "workloads": ["uniform"]},
    "quick": {"keys": 10000, "clients": 16, "rounds": 3, "duration": 8, "warmup": 1,
              "rates": {"memory": [20000, 45000], "none": [10000, 35000], "sync": [1500, 3500]},
              "workloads": ["uniform"]},
    "formal": {"keys": 100000, "clients": 64, "rounds": 6, "duration": 30, "warmup": 5,
               "rates": {"memory": [20000, 45000, 90000], "none": [10000, 45000, 90000],
                         "sync": [1500, 3500, 6000]}, "workloads": ["uniform", "hotspot"]},
}
PAIRS = ("go-memory:rust-memory", "redis:go-memory", "legacy-rust:go-log",
         "redis:go-log", "rust-memory-sync:go-log", "go-log:go-log-maintained",
         "go-log:go-log-grouped", "redis:go-log-grouped", "go-log-grouped-baseline:go-log-grouped",
         "go-log-grouped:go-log-grouped-maintained")
REDIS_REWRITE_PAIRS = ("redis:redis-rewrite", "redis-rewrite:go-log-maintained",
                       "redis-rewrite:go-log-grouped-maintained")


def load_runner():
    spec = importlib.util.spec_from_file_location("thesis_bench_linux", RUNNER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def log_command(arguments, log):
    command = [str(argument) for argument in arguments]
    with log.open("w", encoding="utf-8") as stream:
        stream.write("$ " + shlex.join(command) + "\n")
        stream.flush()
        subprocess.run(command, cwd=REPO, stdout=stream, stderr=subprocess.STDOUT, check=True)


def command_output(*arguments):
    return subprocess.check_output(arguments, cwd=REPO, text=True).strip()


def artifacts(runner, root, args):
    directory = root / "artifacts"
    directory.mkdir(exist_ok=True)
    paths = {name: path.expanduser().absolute() if path else directory / filename
             for name, path, filename in (
                 ("server", args.server, "kerniva-server"),
                 ("library", args.library, "libkerniva_engine.so"),
                 ("benchmark", args.client, "kerniva-bench"),
                 ("maintainer", args.maintainer, "kerniva-maintainer"))}
    wanted = ["server", "library", "benchmark"] + (["maintainer"] if args.maintenance else [])
    rust_build = [name for name in ("library", "maintainer")
                  if name in wanted and not paths[name].is_file() and getattr(args, name) is None]
    if rust_build:
        target = directory / "target"
        log_command(["cargo", "build", "--locked", "--release", "--manifest-path",
                     "kerniva_engine/Cargo.toml", "--target-dir", target], root / "build-rust.log")
        for name in rust_build:
            shutil.copy2(target / "release" / paths[name].name, paths[name])
    for name, package, option in (("server", "./cmd/server", "server"),
                                  ("benchmark", "./scripts/bench", "client")):
        if not paths[name].is_file() and getattr(args, option) is None:
            log_command(["go", "build", "-mod=readonly", "-trimpath", "-o", paths[name], package],
                        root / f"build-{name}.log")
    paths["legacy_server"] = args.legacy_server.expanduser().absolute() if args.legacy_server else paths["server"]
    paths["legacy_library"] = args.legacy_library.expanduser().absolute() if args.legacy_library else paths["library"]
    paths["redis"] = Path(shutil.which(args.redis) or args.redis).expanduser().absolute()
    wanted.extend(("legacy_server", "legacy_library", "redis"))
    if args.go_log_grouped_baseline:
        paths["grouped_baseline"] = args.go_log_grouped_baseline.expanduser().absolute()
        wanted.append("grouped_baseline")
    for name in wanted:
        if not paths[name].is_file() or runner.filesystem_type(paths[name]) != "ext4":
            raise ValueError(f"{name} must be an existing file on ext4: {paths[name]}")
    return {name: {"path": str(paths[name]), "sha256": runner.file_hash(paths[name])}
            for name in wanted}


def kernel_variant(name, backend, evidence, *, previous=False, extra=None):
    environment = {"KERNIVA_STORAGE_BACKEND": backend}
    if extra:
        environment.update(extra)
    return {"name": name, "engine": "kerniva",
            "server": evidence["legacy_server" if previous else "server"]["path"],
            "library": evidence["legacy_library" if previous else "library"]["path"],
            "environment": environment}


def variant_sets(evidence, args):
    redis = {"name": "redis", "engine": "redis", "server": evidence["redis"]["path"]}
    go_log = kernel_variant("go-log", "go-log", evidence,
                            extra={"KERNIVA_COLD_START_STRATEGY": "load_all"})
    legacy = kernel_variant("legacy-rust", "legacy", evidence, previous=True,
                            extra={"KERNIVA_COLD_START_STRATEGY": "load_all"})
    controls = {"memory": [kernel_variant("go-memory", "legacy", evidence, previous=True), redis],
                "none": [go_log, legacy, redis], "sync": [go_log, legacy, redis]}
    if args.go_log_group_max > 1:
        grouped = kernel_variant("go-log-grouped", "go-log", evidence, extra={
            "KERNIVA_COLD_START_STRATEGY": "load_all",
            "KERNIVA_GO_LOG_GROUP_MAX": str(args.go_log_group_max),
        })
        if "grouped_baseline" in evidence:
            controls["sync"].append(dict(grouped, name="go-log-grouped-baseline",
                                         server=evidence["grouped_baseline"]["path"]))
        controls["sync"].append(grouped)
    if args.experiments:
        controls["memory"].append(kernel_variant("rust-memory", "rust-memory", evidence,
                                                 previous=True, extra={"KERNIVA_RUST_MEMORY_SHARDS": "16"}))
        controls["sync"].append(kernel_variant("rust-memory-sync", "rust-memory-sync", evidence, previous=True,
                                               extra={"KERNIVA_RUST_MEMORY_SHARDS": "16",
                                                      "KERNIVA_RUST_MEMORY_READ_DURING_SYNC": "true",
                                                      "KERNIVA_RUST_MEMORY_GROUP_MAX": "1"}))
    if args.maintenance:
        maintainer = evidence["maintainer"]
        window = {
            "THESIS_MAINTAINER_PATH": maintainer["path"],
            "THESIS_MAINTAINER_SHA256": maintainer["sha256"],
        }
        redis_controls = []
        if args.redis_rewrite:
            redis_controls = [
                dict(redis, environment={"THESIS_REDIS_REWRITE": "control"}),
                dict(redis, name="redis-rewrite", environment={"THESIS_REDIS_REWRITE": "manual"}),
            ]
        controls["maintenance"] = [go_log, dict(go_log, name="go-log-maintained",
                                                environment={**go_log["environment"], **window}),
                                   *redis_controls]
        if args.go_log_group_max > 1:
            controls["maintenance-sync"] = [
                grouped, dict(grouped, name="go-log-grouped-maintained",
                              environment={**grouped["environment"], **window}),
                *redis_controls,
            ]
    return controls


class MaintenanceWindow:
    """Observe one independent maintainer process during a measured TCP window."""

    def __init__(self, runner, environment, attempt):
        self.runner = runner
        self.environment = environment
        self.attempt = attempt
        self.process = self.identity = None
        self.first_sample = self.last_sample = self.started_utc = self.observed_utc = None
        self.rss_peak_bytes = 0
        self.before = {}
        self.observed = {}
        self.root = attempt / "data" / "go-log-v1"

    def checkpoints(self):
        cutoffs = {}
        for marker in self.root.glob("db_*/CURRENT"):
            try:
                cutoffs[marker.parent.name] = int(marker.read_text().strip())
            except FileNotFoundError:
                pass
        return cutoffs

    def start(self):
        path = self.environment["THESIS_MAINTAINER_PATH"]
        if self.runner.file_hash(path) != self.environment["THESIS_MAINTAINER_SHA256"]:
            raise RuntimeError("Frozen maintainer executable changed")
        self.arguments = [path, "--data-dir", str(self.root), "--interval", "1",
                          "--max-age", "1", "--min-sealed-bytes", "1"]
        self.before = self.checkpoints()
        with (self.attempt / "maintainer.stdout.log").open("w", encoding="utf-8") as stdout, (
                self.attempt / "maintainer.stderr.log").open("w", encoding="utf-8") as stderr:
            self.process = subprocess.Popen(self.arguments, stdout=stdout, stderr=stderr,
                                            cwd=self.attempt, start_new_session=True)
        self.identity = self.runner.process_identity(self.process.pid)
        self.started_utc = self.runner.utc_now()
        if self.identity:
            self.runner.write_json(self.attempt / "maintainer-running.json", self.identity)

    def observe(self, _state, read_sample):
        if self.process is None:
            self.start()
        if self.process.poll() is not None:
            raise RuntimeError(f"Maintainer exited during workload: {self.process.returncode}")
        sample = read_sample(self.process.pid)
        if sample:
            self.first_sample = self.first_sample or sample
            self.last_sample = sample
            self.rss_peak_bytes = max(self.rss_peak_bytes, sample["working_set_bytes"])
        self.observed = self.checkpoints()
        self.observed_utc = self.runner.utc_now()

    def finish(self, require_completion=False):
        completed = self.runner.utc_now()
        if self.process is not None:
            process = self.process
            self.process = None
            exit_status = process.poll()
            if exit_status is None:
                if self.identity is None:
                    raise RuntimeError("Maintainer process identity is unavailable")
                self.runner.signal_owned(self.identity, signal.SIGTERM)
                self.runner.wait_for_process(process, self.identity, 5)
            if process.poll() is not None:
                (self.attempt / "maintainer-running.json").unlink(missing_ok=True)
            advanced = sum(cutoff > self.before.get(db, 0) for db, cutoff in self.observed.items())
            self.runner.write_json(self.attempt / "maintainer.json", {
                "arguments": self.arguments, "started_utc": self.started_utc, "finished_utc": completed,
                "checkpoint_before": self.before, "checkpoint_observed": self.observed,
                "checkpoint_observed_utc": self.observed_utc, "process_identity": self.identity,
                "databases_advanced": advanced, "exit_before_stop": exit_status,
                "sampled_peak_rss_bytes": self.rss_peak_bytes or None,
                "first_process_sample": self.first_sample, "last_process_sample": self.last_sample,
            })
            if not require_completion:
                return
            if exit_status is not None:
                raise RuntimeError(f"Maintainer exited during workload: {exit_status}")
            if advanced > 0:
                return
        if require_completion:
            raise RuntimeError("No snapshot publication observed within this window")


class RedisRewriteWindow:
    """Trigger one manual AOF rewrite and observe its completion under load."""

    def __init__(self, runner, attempt):
        self.runner = runner
        self.attempt = attempt
        self.port = None
        self.finished = False
        self.record = {"completion": None, "child_samples": [], "status_samples": []}

    def status(self):
        raw = self.runner.command(self.port, "INFO", "persistence")
        fields = dict(line.split(":", 1) for line in raw.splitlines() if ":" in line)
        return {key: value for key, value in fields.items() if key.startswith("aof_")}

    def start(self):
        config = (self.attempt / "redis.conf").read_text().splitlines()
        self.port = int(next(line.split()[1] for line in config if line.startswith("port ")))
        setting = self.runner.command(self.port, "CONFIG", "GET", "auto-aof-rewrite-percentage")
        if setting != ["auto-aof-rewrite-percentage", "0"]:
            raise RuntimeError("Redis automatic AOF rewrite is enabled in the controlled experiment")
        before = self.record["before"] = self.status()
        if (before.get("aof_enabled") != "1" or before.get("aof_rewrite_in_progress") != "0"
                or before.get("aof_rewrite_scheduled") != "0" or "aof_rewrites" not in before):
            raise RuntimeError("Redis must expose AOF counters and be idle before the rewrite")
        self.record["trigger_utc"] = self.runner.utc_now()
        self.record["trigger_reply"] = self.runner.command(self.port, "BGREWRITEAOF")

    def observe(self, state, read_sample):
        if self.record["completion"] is not None:
            return
        if self.port is None:
            self.start()
        server_pid = state["server"]["pid"]
        children = Path(f"/proc/{server_pid}/task/{server_pid}/children")
        if children.is_file():
            for pid in map(int, children.read_text().split()):
                process = read_sample(pid)
                if process:
                    self.record["child_samples"].append(dict(process, pid=pid, utc=self.runner.utc_now()))
        status = self.status()
        sample = {"utc": self.runner.utc_now(), "status": status}
        self.record["status_samples"].append(sample)
        if not self.runner.identity_matches(state["client"]):
            return
        rewrites = int(status["aof_rewrites"]) - int(self.record["before"]["aof_rewrites"])
        if rewrites > 1:
            raise RuntimeError("Redis performed more than one rewrite in the controlled window")
        if (rewrites == 1 and status.get("aof_rewrite_in_progress") == "0"
                and status.get("aof_rewrite_scheduled") == "0"):
            if status.get("aof_last_bgrewrite_status") != "ok":
                raise RuntimeError("Redis background AOF rewrite failed")
            self.record["server_identity"] = state["server"]
            self.record["completion"] = sample

    def finish(self, require_completion=False):
        if not self.finished and self.attempt.is_dir():
            self.runner.write_json(self.attempt / "redis-rewrite.json", self.record)
            self.finished = True
        if require_completion and self.record["completion"] is None:
            raise RuntimeError("No Redis AOF rewrite completion observed within this window")


def install_extensions(runner):
    """Reuse the original runner and verify the new storage-specific evidence."""
    base_case = runner.run_case
    base_backend = runner.validate_memory_backend
    base_wal = runner.validate_wal_measurement
    base_sample = runner.read_process_sample
    base_configuration = runner.write_server_configuration

    def write_server_configuration(attempt, data, port, variant, case, options):
        configuration, arguments = base_configuration(attempt, data, port, variant, case, options)
        if "THESIS_REDIS_REWRITE" in variant["environment"]:
            setting = "auto-aof-rewrite-percentage"
            config = attempt / "redis.conf"
            config.write_text(config.read_text().replace(f"{setting} {configuration[setting]}\n",
                                                          f"{setting} 0\n"), encoding="utf-8")
            configuration[setting] = 0
        return configuration, arguments

    def validate_backend(variant, info):
        base_backend(variant, info)
        name = variant["name"]
        if variant["environment"].get("KERNIVA_STORAGE_BACKEND") == "go-log":
            if info.get("storage_backend") != "go-log" or info.get("persistence_enabled") != "true":
                raise ValueError("INFO did not validate the Go-log persistent backend")
            for key in ("last_sequence", "sync_count"):
                if int(info[key]) < 0:
                    raise ValueError(f"INFO returned a negative {key}")
            expected_group_max = variant["environment"].get("KERNIVA_GO_LOG_GROUP_MAX", "1")
            if info.get("log_group_max", "1") != expected_group_max:
                raise ValueError(f"INFO did not validate log_group_max={expected_group_max}")
            if expected_group_max != "1" and any(int(info[key]) < 0 for key in ("log_group_batches", "log_group_requests")):
                raise ValueError("INFO returned negative Go-log group counters")
        elif name in ("go-memory", "legacy-rust"):
            expected = str(name == "legacy-rust").lower()
            if info.get("persistence_enabled") != expected:
                raise ValueError(f"INFO persistence_enabled mismatch for {name}")

    def validate_measurement(before, after, measurement):
        base_wal(before, after, measurement)
        if after.get("storage_backend") != "go-log":
            return
        committed = int(after["last_sequence"]) - int(before["last_sequence"])
        synced = int(after["sync_count"]) - int(before["sync_count"])
        if committed < 0 or synced < 0:
            raise ValueError("Go-log counters regressed")
        if measurement["failures"] == 0 and committed != measurement["set"]["succeeded"]:
            raise ValueError("Go-log committed sequences do not match acknowledged SET count")
        group_batches = int(after.get("log_group_batches", "0")) - int(before.get("log_group_batches", "0"))
        group_requests = int(after.get("log_group_requests", "0")) - int(before.get("log_group_requests", "0"))
        if group_batches < 0 or not 2 * group_batches <= group_requests <= committed:
            raise ValueError("Go-log group counters do not match committed requests")
        if measurement["durability"] == "sync" and synced + group_requests - group_batches != committed:
            raise ValueError("Go-log sync count does not account for all committed requests")

    def run_case(case_directory, variant, case, plan, benchmark, options, args, attempt_number):
        environment = variant["environment"]
        attempt = case_directory / f"attempt-{attempt_number:04d}"
        redis_rewrite = environment.get("THESIS_REDIS_REWRITE")
        if "THESIS_MAINTAINER_PATH" in environment:
            observer = MaintenanceWindow(runner, environment, attempt)
        elif redis_rewrite == "manual":
            observer = RedisRewriteWindow(runner, attempt)
        elif redis_rewrite == "control":
            observer = None
        else:
            return base_case(case_directory, variant, case, plan, benchmark, options, args, attempt_number)

        def sample(pid, sample_private_memory=False):
            result = base_sample(pid, sample_private_memory)
            if (attempt / "start").exists():
                state = json.loads((case_directory / "running.json").read_text())
                if pid == state["server"]["pid"] and state["client"] and runner.identity_matches(state["client"]):
                    observer.observe(state, base_sample)
            return result

        def verify_and_finish(before, after, measurement):
            validate_measurement(before, after, measurement)
            if observer:
                observer.finish(require_completion=True)
            elif before.get("aof_rewrites") != after.get("aof_rewrites"):
                raise RuntimeError("Redis control performed an AOF rewrite during the window")

        if observer:
            runner.read_process_sample = sample
        runner.validate_wal_measurement = verify_and_finish
        try:
            return base_case(case_directory, variant, case, plan, benchmark, options, args, attempt_number)
        finally:
            runner.read_process_sample = base_sample
            runner.validate_wal_measurement = validate_measurement
            if observer:
                observer.finish()

    runner.write_server_configuration = write_server_configuration
    runner.validate_memory_backend = validate_backend
    runner.validate_wal_measurement = validate_measurement
    runner.run_case = run_case


def parse_csv(value):
    items = value.split(",")
    if not all(items) or len(items) != len(set(items)):
        raise argparse.ArgumentTypeError("Expected distinct, nonempty comma-separated values")
    return items


def rate_overrides(values):
    overrides = {}
    for value in values:
        mode, separator, text = value.partition("=")
        if not separator or mode not in ("memory", "none", "sync") or mode in overrides:
            raise ValueError("--rate requires distinct MODE=RATE[,RATE] values")
        rates = [int(item) for item in parse_csv(text)]
        if any(rate <= 0 for rate in rates):
            raise ValueError("Arrival rates must be positive")
        overrides[mode] = rates
    return overrides


def parse_arguments(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=tuple(PROFILES), default="formal")
    parser.add_argument("--run-root", type=Path, help="Ext4 output directory, default logs/go-log-*")
    parser.add_argument("--server", type=Path, help="Prebuilt candidate server on ext4")
    parser.add_argument("--library", type=Path, help="Prebuilt candidate Rust library on ext4")
    parser.add_argument("--client", type=Path, help="Prebuilt fixed-rate client on ext4")
    parser.add_argument("--legacy-server", type=Path, help="Frozen baseline server, defaults to candidate binary")
    parser.add_argument("--legacy-library", type=Path, help="Frozen baseline Rust library")
    parser.add_argument("--maintainer", type=Path, help="Prebuilt independent Rust maintainer")
    parser.add_argument("--redis", default="redis-server")
    parser.add_argument("--durabilities", type=parse_csv, default=["memory", "none", "sync"])
    parser.add_argument("--workloads", type=parse_csv, help="uniform,hotspot,insert,miss")
    parser.add_argument("--network", choices=("std", "gnet"), default="gnet")
    parser.add_argument("--read-percent", type=int, default=95)
    parser.add_argument("--rate", action="append", default=[], help="MODE=RATE[,RATE], repeat by durability")
    parser.add_argument("--rounds", type=int)
    parser.add_argument("--duration", type=float)
    parser.add_argument("--warmup", type=float)
    parser.add_argument("--keys", type=int)
    parser.add_argument("--clients", type=int)
    parser.add_argument("--budget-seconds", type=int, default=43200)
    parser.add_argument("--experiments", action=argparse.BooleanOptionalAction, default=True,
                        help="Include historical Rust-memory variants")
    parser.add_argument("--go-log-group-max", type=int, choices=range(1, 33), default=1,
                        help="Add a paired Go-log sync run with this maximum group size")
    parser.add_argument("--go-log-grouped-baseline", type=Path,
                        help="Compare a frozen Go server with matching grouping and Rust library")
    parser.add_argument("--maintenance", action=argparse.BooleanOptionalAction, default=None,
                        help="Include managed/unmanaged Go-log paired windows; defaults on in quick/formal")
    parser.add_argument("--maintenance-only", action="store_true",
                        help="Run only the paired maintenance windows")
    parser.add_argument("--redis-rewrite", action="store_true",
                        help="Add matched Redis AOF control and forced rewrite windows")
    parser.add_argument("--keep-data", action="store_true")
    parser.add_argument("--resume", action="store_true", help="Resume the same saved plan")
    parser.add_argument("--plan-only", action="store_true", help="Freeze artifacts and generate plans")
    args = parser.parse_args(argv)
    profile = PROFILES[args.profile]
    if not set(args.durabilities) <= {"memory", "none", "sync"}:
        parser.error("Allowed durability values are memory, none, sync")
    if args.go_log_group_max > 1 and "sync" not in args.durabilities:
        parser.error("--go-log-group-max needs sync durability")
    if args.go_log_grouped_baseline and (args.go_log_group_max == 1 or args.maintenance_only):
        parser.error("--go-log-grouped-baseline requires a regular sync group with --go-log-group-max above 1")
    args.workloads = args.workloads or profile["workloads"]
    if not set(args.workloads) <= {"uniform", "hotspot", "insert", "miss"}:
        parser.error("Allowed workloads are uniform, hotspot, insert, miss")
    if (args.legacy_server is None) != (args.legacy_library is None):
        parser.error("--legacy-server and --legacy-library must be supplied together")
    if args.maintenance is None:
        args.maintenance = args.profile != "smoke" or args.maintenance_only or args.redis_rewrite
    if not any(mode in ("none", "sync") for mode in args.durabilities):
        args.maintenance = False
    if not args.maintenance and (args.maintenance_only or args.redis_rewrite):
        parser.error("--maintenance-only and --redis-rewrite need a persistent maintenance group")
    for name in ("keys", "clients", "rounds", "duration", "warmup"):
        if getattr(args, name) is None:
            setattr(args, name, profile[name])
    if args.maintenance and args.duration <= 2:
        parser.error("Maintenance needs a measurement window longer than two seconds")
    try:
        args.rates = dict(profile["rates"], **rate_overrides(args.rate))
    except (ValueError, argparse.ArgumentTypeError) as error:
        parser.error(str(error))
    return args


def stage_specs(args, controls):
    if not args.maintenance_only:
        for durability in args.durabilities:
            for workload in args.workloads:
                yield {"name": f"{durability}-{workload}", "durability": durability,
                       "workload": workload, "rates": args.rates[durability],
                       "segment_size": 4 << 20, "variants": controls[durability]}
    if args.maintenance:
        for durability in (mode for mode in args.durabilities if mode != "memory"):
            yield {"name": f"{durability}-maintenance", "durability": durability,
                   "workload": "uniform", "rates": args.rates[durability][:1],
                   "segment_size": 64 << 10,
                   "variants": controls.get(f"maintenance-{durability}", controls["maintenance"])}


def recover_sidecars(runner, root):
    for marker in (root / "measurements").glob("*/cases/*/*/attempt-*/maintainer-running.json"):
        identity = json.loads(marker.read_text())
        if runner.identity_matches(identity):
            runner.signal_owned(identity, signal.SIGTERM)
            deadline = time.monotonic() + 5
            while runner.identity_matches(identity) and time.monotonic() < deadline:
                time.sleep(0.05)
            if runner.identity_matches(identity):
                runner.signal_owned(identity, signal.SIGKILL)
        marker.unlink()


def run_plan(runner, root, evidence, args, deadline):
    controls = variant_sets(evidence, args)
    statuses = {}
    for stage in stage_specs(args, controls):
        name = stage["name"]
        manifest = root / f"manifest-{name}.json"
        runner.write_json(manifest, {"benchmark": evidence["benchmark"]["path"], "variants": stage["variants"]})
        command = ["--run-root", str(root), "--manifest", str(manifest), "--suite", name,
                   "--group", f"{args.network}:{args.read_percent}:" + ",".join(map(str, stage["rates"])),
                   "--duration", str(args.duration), "--warmup", str(args.warmup),
                   "--keys", str(args.keys), "--clients", str(args.clients), "--rounds", str(args.rounds),
                   "--durability", stage["durability"], "--deadline", str(deadline), "--timeout-ms", "2000",
                   "--segment-size-bytes", str(stage["segment_size"]), "--retries", "1",
                   "--hot-percent", "100" if stage["workload"] == "hotspot" else "0", "--hot-keys", "1",
                   "--verbose"]
        if args.keep_data:
            command.append("--keep-data")
        if stage["workload"] == "insert":
            command.append("--insert-writes")
        if stage["workload"] == "miss":
            command.append("--missing-reads")
        if args.plan_only:
            command.append("--plan-only")
        try:
            statuses[name] = runner.run_suite(runner.parse_arguments(command))
        except Exception as error:
            statuses[name] = {"error": str(error), "type": type(error).__name__}
        runner.write_json(root / "stage-status.json", statuses)
        if statuses[name] == 2:
            break
    return statuses


def write_report(root):
    analysis = root / "analysis"
    merged = defaultdict(lambda: {"failures": 0, "offered": 0})
    with (analysis / "runs.csv").open(encoding="utf-8-sig", newline="") as stream:
        for run in csv.DictReader(stream):
            counts = merged[(run["suite"], run["target_rate"], run["variant"])]
            counts["failures"] += int(run["failures"])
            counts["offered"] += int(run["offered"])

    def metric(row, key, multiplier=1):
        value = row.get(key + "_median")
        return f"{float(value) * multiplier:.3f}" if value else "—"

    summary = ["# Go 日志与 Rust 快照架构压测", "",
               "纯内存、缓冲追加和同步写入分别比较。Redis AOF 的同步配置按组核验。", "",
               "延迟为计划发送至读完回复的客户端时间。表中数值为各轮中位数，成功率分母包含所有计划请求。",
               "每轮零失败、实际吞吐至少为目标的 98% 且队列没有持续上涨才计为达标。",
               "比较尾延迟时采用双方同轮达标的数据。失败窗口继续保留在原始结果中。", "",
               "| 场景 | 方案 | 目标请求/s | 实际请求/s | 达标轮 | 失败/计划 | GET p50/p95/p99/p99.9 ms | SET p50/p95/p99/p99.9 ms | 服务 CPU 单核% | 服务峰值 RSS MiB |",
               "| --- | --- | ---: | ---: | ---: | ---: | --- | --- | ---: | ---: |"]
    with (analysis / "summary.csv").open(encoding="utf-8-sig", newline="") as stream:
        for row in csv.DictReader(stream):
            counts = merged[(row["suite"], row["target_rate"], row["variant"])]
            tail = lambda name: "/".join(metric(row, f"{name}_success_latency_{q}", 0.001)
                                         for q in ("p50_us", "p95_us", "p99_us", "p99_9_us"))
            summary.append(f"| {row['suite']} | {row['variant']} | {row['target_rate']} | "
                           f"{metric(row, 'actual_success_rate')} | {row['stable_rounds']}/{row['rounds']} | "
                           f"{counts['failures']}/{counts['offered']} | {tail('get')} | {tail('set')} | "
                           f"{metric(row, 'cpu_percent_one_core')} | "
                           f"{metric(row, 'rss_sampled_peak_bytes', 1 / 2**20)} |")
    maintenance = sorted((root / "measurements").glob("*/cases/*/go-log-maintained/maintainer.json"))
    if maintenance:
        summary.extend(["", "## Rust 维护进程", "",
                        "这一组服务 CPU 和 RSS 只计算 Go。清单切换与维护进程采样保存在各窗口的 maintainer.json，回收量保存在 maintainer.stdout.log。",
                        f"观察到 {len(maintenance)} 个对照窗口发布了快照。"])
    rewrites = sorted((root / "measurements").glob("*/cases/*/redis-rewrite/redis-rewrite.json"))
    if rewrites:
        summary.extend(["", "## Redis AOF 重写", "",
                        f"观察到 {len(rewrites)} 个窗口在压测期间完成手动重写。Redis 对照组关闭自动重写。",
                        "重写命令、进度、写时复制内存量和可采集的子进程样本保存在各窗口的 redis-rewrite.json。"])
    summary.extend(["", "各轮数据、采样和配对范围保存在 [analysis](analysis/) 与 [measurements](measurements/)。", ""])
    (root / "results.md").write_text("\n".join(summary), encoding="utf-8")


def main():
    args = parse_arguments()
    if sys.platform != "linux":
        raise ValueError("The benchmark runs on Linux or WSL 2")
    runner = load_runner()
    if args.run_root:
        root = args.run_root.expanduser().resolve()
        root.mkdir(parents=True, exist_ok=True)
    else:
        parent = REPO / "logs"
        parent.mkdir(exist_ok=True)
        root = Path(tempfile.mkdtemp(prefix="go-log-", dir=parent))
    if runner.filesystem_type(root) != "ext4":
        raise ValueError(f"Run directory must reside on ext4: {root}")
    import fcntl
    with (root / "orchestrator.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if not args.resume and any(path != root / "orchestrator.lock" for path in root.iterdir()):
            raise ValueError("Run directory contains existing evidence; use --resume with the same plan")
        recover_sidecars(runner, root)
        evidence = artifacts(runner, root, args)
        frozen = {"commit": command_output("git", "rev-parse", "HEAD"),
                  "dirty": bool(command_output("git", "status", "--porcelain")),
                  "artifacts": evidence, "runner_sha256": runner.file_hash(RUNNER),
                  "orchestrator_sha256": runner.file_hash(__file__),
                  "analyzer_sha256": runner.file_hash(ANALYZER),
                  "go_version": command_output("go", "version"),
                  "rust_version": command_output("rustc", "--version"),
                  "redis_version": command_output(evidence["redis"]["path"], "--version"),
                  "profiles": {field: getattr(args, field) for field in
                               ("profile", "network", "read_percent", "workloads", "durabilities",
                                "keys", "clients", "duration", "warmup", "rounds", "rates",
                                "maintenance", "maintenance_only", "redis_rewrite", "experiments",
                                "go_log_group_max")}}
        provenance = root / "provenance.json"
        if provenance.exists() and json.loads(provenance.read_text()) != frozen:
            raise ValueError("Saved plan or frozen binaries differ; start a new run")
        runner.write_json(provenance, frozen)
        print(f"Run directory: {root}", flush=True)
        install_extensions(runner)
        stages = run_plan(runner, root, evidence, args, time.time() + args.budget_seconds)
        if not args.plan_only:
            try:
                pairs = PAIRS + (REDIS_REWRITE_PAIRS if args.redis_rewrite else ())
                log_command([sys.executable, ANALYZER, root, "--no-figures",
                             *(argument for pair in pairs for argument in ("--pair", pair))],
                            root / "analyzer.log")
                write_report(root)
            except Exception as error:
                stages["analysis"] = {"error": str(error), "type": type(error).__name__}
        failed = [stage for stage, code in stages.items() if code != 0]
        result = {"status": "incomplete" if failed else ("planned" if args.plan_only else "complete"),
                  "run_root": str(root), "stages": stages, "failed_or_partial": failed}
        runner.write_json(root / "result.json", result)
        print(json.dumps(result, ensure_ascii=False), flush=True)
        return int(bool(failed))


if __name__ == "__main__":
    raise SystemExit(main())
