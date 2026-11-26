"""Prepare or run a gnet WAL comparison on native NTFS or Linux ext4."""
import argparse
from contextlib import contextmanager
import ctypes
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import traceback

from golog_persistence_modes_check import command, digest, require, wait_for


PROFILES = ((50, 3000), (95, 6000))
CLIENTS, KEYS, SEED, DURATION, WARMUP, AGE = 64, 100_000, 42, 65, 5, 30
OPS = ("get", "set")
FAILURES = ("failures", "queue_drops", "queue_timeouts", "request_timeouts",
            "network_errors", "reply_errors", "reconnect_attempts")
BASE_SETTINGS = {
    "host": "127.0.0.1", "network_type": "gnet", "db_count": 2, "storage_backend": "go-log",
    "max_memory": 536_870_912, "max_memory_policy": "noeviction", "persistence_enabled": True,
    "go_log_enabled": True, "snapshot_enabled": False, "cold_start_strategy": "load_all",
    "persistence_write_mode": "strong", "persistence_durability": "sync", "engine_segment_size": 4 << 20,
    "go_log_segment_age_seconds": AGE, "go_log_group_max": 8, "log_level": "error", "pprof_enabled": False,
}


def save_json(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def histogram_upper(index):
    if index < 512:
        return index
    shift, mantissa = divmod(index - 512, 256)
    return ((mantissa + 257) << (shift + 1)) - 1


def threshold_bounds(histogram, threshold=100_000_000):
    certain = possible = count = 0
    for item in histogram["bins"] or ():
        index, samples = item["index"], item["count"]
        lower = histogram_upper(index - 1) + 1 if index else 0
        upper = min(histogram_upper(index), histogram["max_ns"])
        certain += samples if lower >= threshold else 0
        possible += samples if upper >= threshold else 0
        count += samples
    require(count == histogram["count"], "histogram bin count differs from total")
    return {"certain": certain, "possible": possible}


def exact_threshold_counts(path, bounds):
    result = dict.fromkeys(OPS, 0)
    summary = None
    with path.open(encoding="utf-8") as stream:
        for line in stream:
            row = json.loads(line)
            if row.get("type") == "summary":
                summary = row
            if ("index" in row and row["outcome"] == "success"
                    and row["completed_ns"] - row["planned_ns"] >= 100_000_000):
                result[row["op"].lower()] += 1
    require(summary is not None and summary["drops"] == 0, "request trace incomplete")
    for op, count in result.items():
        require(bounds[op]["certain"] <= count <= bounds[op]["possible"], "trace disagrees with histogram")
    return result


def mixed_argv(bench, port, path, profile, trace):
    read, rate = profile
    argv = [str(bench), "-mode", "mixed", "-addr", f"127.0.0.1:{port}",
            "-network", "gnet", "-durability", "sync", "-clients", str(CLIENTS),
            "-keys", str(KEYS), "-seed", str(SEED), "-rate", str(rate), "-read-percent", str(read),
            "-duration", f"{DURATION}s", "-warmup", f"{WARMUP}s", "-timeout", "2s",
            "-histogram-out", str(path / "client-hist.json"),
            "-ready-file", str(path / "client.ready"), "-start-file", str(path / "client.start")]
    if trace:
        argv.extend(("-request-trace-out", str(path / "client-trace.jsonl"), "-request-trace-stride", "16"))
    return argv


def fields(port):
    raw = command(port, "INFO")
    values = dict(line.split(":", 1) for line in raw.splitlines() if ":" in line and not line.startswith("#"))
    for key, expected in {
        "storage_backend": "go-log", "persistence_durability": "sync",
        "go_log_enabled": "true", "snapshot_enabled": "false", "log_group_max": str(BASE_SETTINGS["go_log_group_max"]),
    }.items():
        require(values.get(key) == expected, f"unexpected INFO {key}={values.get(key)!r}")
    counters = {key: int(values[key]) for key in ("last_sequence", "sync_count", "sealed_segments", "sealed_bytes")}
    return {"raw": raw, "counters": counters, "dbsize": command(port, "DBSIZE")}


def stop_owned(child, crash=False, marker=None):
    if child.poll() is None:
        if crash or marker is None:
            child.kill()
        else:
            marker.touch(exist_ok=False)
    try:
        return child.wait(timeout=25)
    except subprocess.TimeoutExpired:
        child.kill()
        child.wait(timeout=10)
        raise


@contextmanager
def run_server(options, path, data, environment, receipt):
    path.mkdir()
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]
    config = {**BASE_SETTINGS, "port": port, "data_dir": str(data), "log_path": str(path / "server.log")}
    config_path, stop_marker = path / "config.json", path / "stop"
    save_json(config_path, config)
    argv = [str(options.server), "-config", str(config_path), "-stop-file", str(stop_marker)]
    with (path / "console.log").open("wb") as console:
        child = subprocess.Popen(argv, cwd=path, env=environment, stdout=console, stderr=subprocess.STDOUT)
    receipt.update(pid=child.pid, port=port, argv=argv)

    def listening():
        require(child.poll() is None, f"server exited {child.returncode}: {path / 'console.log'}")
        try:
            return command(port, "PING") == "PONG"
        except OSError:
            return False

    try:
        wait_for(listening, "server did not listen", timeout=60)
        yield child, port, stop_marker
    finally:
        receipt["exit"] = stop_owned(child)


def sealed(data):
    return sorted(str(path.relative_to(data)) for path in data.rglob("log-*.seg"))


def run_window(options, path, profile, environment, window):
    path.mkdir()
    data = path / "primary-data"
    read, rate = profile
    with run_server(options, path / "first", data, environment, window.setdefault("first", {})) as (server, port, _):
        require(command(port, "SET", "live-load:seed", "live-load-db0") == "OK", "could not seed")
        argv = mixed_argv(options.bench, port, path, profile, options.trace)
        window["bench_argv"] = argv
        with (path / "client.json").open("wb") as output, (path / "client.stderr.log").open("wb") as errors:
            client = subprocess.Popen(argv, cwd=path, env=environment, stdout=output, stderr=errors,
                                      stdin=subprocess.DEVNULL)
        window["client_pid"] = client.pid

        def client_ready():
            require(client.poll() is None, f"client exited before ready: {client.returncode}")
            return (path / "client.ready").is_file()

        try:
            wait_for(client_ready, "client prefill/warmup did not finish", timeout=180)
            before = fields(port)
            window["sealed_before"] = sealed(data)
            window["measurement_release_utc"] = utc_now()
            window["measurement_release_ns"] = time.monotonic_ns()
            (path / "client.start").touch(exist_ok=False)
            code = client.wait(timeout=DURATION + 25)
            window["measurement_join_ns"] = time.monotonic_ns()
            window["client_exit"] = code
            require(code == 0, f"fixed-rate client exited {code}")
        finally:
            stop_owned(client)
        after = fields(port)
        window["sealed_after"] = sealed(data)
        window["before"] = before
        window["after"] = after
        counters = {key: after["counters"][key] - before["counters"][key] for key in before["counters"]}
        window["counter_delta"] = counters

        report = read_json(path / "client.json")
        histogram = read_json(path / "client-hist.json")
        require((report["network"], report["durability"], report["clients"], report["keys"], report["seed"],
                 report["read_percent"], report["target_rate"], report["duration_seconds"])
                == ("gnet", "sync", CLIENTS, KEYS, SEED, read, rate, DURATION), "client parameters differ")
        require([row["planned_second"] for row in report["windows"]] == list(range(DURATION)),
                "client measurement windows missing")
        metrics = window["metrics"] = {
            key: report[key] for key in ("offered", "sent", "succeeded", "failures", "actual_success_rate",
                                         "window_success_rate", "drain_seconds")
        }
        bounds = {}
        for op in OPS:
            summary = report[op]
            require(histogram[op]["success"]["count"] == summary["succeeded"], "latency histogram lost success")
            bounds[op] = threshold_bounds(histogram[op]["success"])
            metrics[op] = {
                **{key: summary[key] for key in ("sent", "succeeded", "succeeded_in_window", *FAILURES)},
                "success_latency_us": summary["success_latency"], "ge100ms": bounds[op],
                "ge500ms": threshold_bounds(histogram[op]["success"], 500_000_000),
                "queue_ge100ms": threshold_bounds(histogram[op]["client_queue"]),
                "replies_after_cutoff": summary["succeeded"] - summary["succeeded_in_window"],
            }
        metrics["succeeded_at_cutoff"] = sum(metrics[op]["succeeded_in_window"] for op in OPS)
        metrics["committed_set_without_successful_reply"] = counters["last_sequence"] - report["set"]["succeeded"]
        if options.trace:
            metrics["ge100ms_exact"] = exact_threshold_counts(path / "client-trace.jsonl", bounds)
        window["delivery_pass"] = (
            all(metrics[key] == rate * DURATION for key in ("offered", "sent", "succeeded"))
            and all(metrics[op][key] == 0 for op in OPS for key in FAILURES)
            and all(sum(row[op][key] for op in OPS) == rate
                    for row in report["windows"] for key in ("offered", "sent", "succeeded"))
            and all(row[op][key] == 0 for row in report["windows"] for op in OPS for key in FAILURES)
            and metrics["committed_set_without_successful_reply"] == 0
        )
        for args, expected in (
            (("SET", "recovery:overwritten", "before"), "OK"),
            (("SET", "recovery:overwritten", "after"), "OK"),
            (("SET", "recovery:deleted", "transient"), "OK"),
            (("DEL", "recovery:deleted"), 1),
        ):
            require(command(port, *args) == expected, f"unexpected recovery seed response: {args}")
        window["pre_crash"] = fields(port)
        window["forced_exit"] = stop_owned(server, crash=True)
        require(window["forced_exit"] != 0, "forced process termination returned success")

    with run_server(options, path / "reopened", data, environment, window.setdefault("reopened", {})) as (
            server, port, stop_marker):
        reopened = fields(port)
        recovered = {
            "overwritten": command(port, "GET", "recovery:overwritten"),
            "deleted": command(port, "GET", "recovery:deleted"),
            "benchmark_seed": command(port, "GET", "live-load:seed"),
        }
        window["post_crash"] = {"info": reopened, "values": recovered}
        require(recovered == {"overwritten": "after", "deleted": None, "benchmark_seed": "live-load-db0"},
                "crash recovery restored wrong values")
        expect = window["pre_crash"]["counters"]["last_sequence"]
        require(reopened["counters"]["last_sequence"] == expect, "last durable sequence changed during recovery")
        require(reopened["dbsize"] == window["pre_crash"]["dbsize"], "recovered key count differs")
        require(command(port, "SET", "recovery:reopened", "ready") == "OK", "could not resume writes")
        require(fields(port)["counters"]["last_sequence"] == expect + 1, "resumed sequence did not advance")
        require(stop_owned(server, marker=stop_marker) == 0, "reopened process did not stop cleanly")
    window["recovery_pass"] = True


def ntfs(path):
    volume = ctypes.create_unicode_buffer(32768)
    if not ctypes.windll.kernel32.GetVolumePathNameW(str(path), volume, len(volume)):
        raise ctypes.WinError()
    filesystem = ctypes.create_unicode_buffer(32)
    result = ctypes.windll.kernel32.GetVolumeInformationW(
        volume, None, 0, None, None, None, filesystem, len(filesystem),
    )
    return bool(result) and filesystem.value == "NTFS"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", type=Path, required=True)
    parser.add_argument("--bench", type=Path, required=True)
    parser.add_argument("--library", type=Path, required=True)
    parser.add_argument("--scratch-parent", type=Path, required=True, help="existing directory on the NTFS or ext4 volume to test")
    parser.add_argument("--output", type=Path, required=True, help="new evidence directory")
    parser.add_argument("--execute", action="store_true", help="start measurement processes")
    parser.add_argument("--trace", action="store_true", help="separate instrumented run with exact 100 ms counts")
    parser.add_argument("--reverse", action="store_true", help="run the 95%%-read profile first")
    parser.add_argument("--high-write-only", action="store_true", help="run only 50%% writes at 3,000 requests/s")
    parser.add_argument("--group-max", type=int, default=8, help="maximum ready writes per sync group")
    options = parser.parse_args()
    require(1 <= options.group_max <= 32, "explicit sync group size must be from 1 to 32")
    BASE_SETTINGS["go_log_group_max"] = options.group_max
    for key in ("server", "bench", "library"):
        setattr(options, key, getattr(options, key).resolve(strict=True))
    options.scratch_parent = options.scratch_parent.resolve(strict=True)
    output = options.output.resolve()
    require(options.scratch_parent.is_dir(), "scratch parent must be a directory")
    if os.name == "nt":
        require(ntfs(options.scratch_parent), "Windows scratch parent must be on NTFS")
        filesystem = "NTFS"
    else:
        require(sys.platform == "linux", "benchmark supports Windows NTFS and Linux ext4")
        filesystem = subprocess.check_output(
            ["findmnt", "-n", "-o", "FSTYPE", "-T", str(options.scratch_parent)], text=True,
        ).strip()
        require(filesystem == "ext4", "Linux scratch parent must be on ext4")
    output.mkdir(parents=True, exist_ok=False)
    plan = PROFILES[:1] if options.high_write_only else PROFILES
    if options.reverse:
        plan = tuple(reversed(plan))
    record = {
        "started_utc": utc_now(), "mode": "execute" if options.execute else "prepare", "config": BASE_SETTINGS,
        "artifact_sha256": {name: {"path": str(getattr(options, name)), "sha256": digest(getattr(options, name))}
                            for name in ("server", "bench", "library")},
        "script_sha256": digest(Path(__file__)), "scratch_parent": str(options.scratch_parent),
        "platform": sys.platform, "filesystem": filesystem,
        "profiles": [{"read_percent": read, "rate": rate,
                      "bench_argv_template": mixed_argv(options.bench, "PORT", Path("WINDOW_DIR"), (read, rate),
                                                       options.trace)} for read, rate in plan],
        "trace": options.trace,
    }
    save_json(output / "plan.json", record)
    if not options.execute:
        return 0
    scratch = Path(tempfile.mkdtemp(prefix="kerniva-native-gnet-", dir=options.scratch_parent))
    record.update(scratch=str(scratch), windows=[])
    environment = {key: value for key, value in os.environ.items() if not key.startswith("KERNIVA_")}
    for key in ("GOGC", "GODEBUG", "GOMAXPROCS", "GOMEMLIMIT", "GOTRACEBACK"):
        environment.pop(key, None)
    environment["KERNIVA_ENGINE_LIBRARY"] = str(options.library)
    try:
        for position, profile in enumerate(plan, start=1):
            read, rate = profile
            label = f"{position:02d}-r{read}-q{rate}"
            window = {"folder": label, "profile": [read, rate]}
            record["windows"].append(window)
            path = scratch / label
            try:
                run_window(options, path, profile, environment, window)
            except Exception:
                window["error"] = traceback.format_exc()
            finally:
                if path.exists():
                    shutil.copytree(path, output / label, ignore=shutil.ignore_patterns("primary-data"))
                save_json(output / "results.json", record)
    finally:
        record["ended_utc"] = utc_now()
        record["pass"] = len(record["windows"]) == len(plan) and all(
            window.get("delivery_pass") and window.get("recovery_pass") for window in record["windows"])
        save_json(output / "results.json", record)
    return 0 if record["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
