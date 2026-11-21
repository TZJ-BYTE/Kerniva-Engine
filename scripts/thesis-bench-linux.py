#!/usr/bin/env python3
"""Run isolated Linux Kerniva/Redis experiments with the existing Go client."""

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import signal
import shutil
import socket
import subprocess
import sys
import time
import traceback
from datetime import datetime, timezone


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def write_json(path, value):
    path = Path(path)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(value, handle, indent=2, allow_nan=False)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def file_hash(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def absolute_path(value, base):
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = base / path
    # Redis selects its entry point from argv[0], so preserve its symlink name.
    return os.path.abspath(path)


def parse_group(value):
    try:
        network, read_percent, rates = value.split(":")
        group = {"network": network, "read_percent": int(read_percent),
                 "rates": [int(rate) for rate in rates.split(",")]}
    except (ValueError, TypeError) as error:
        raise argparse.ArgumentTypeError("group must be NETWORK:READ_PERCENT:RATE[,RATE]") from error
    if network not in ("std", "gnet") or not 0 <= group["read_percent"] <= 100:
        raise argparse.ArgumentTypeError("group requires std/gnet and a read percentage from 0 to 100")
    if not group["rates"] or any(rate <= 0 for rate in group["rates"]):
        raise argparse.ArgumentTypeError("group rates must be positive")
    if len(set(group["rates"])) != len(group["rates"]):
        raise argparse.ArgumentTypeError("group rates must be distinct")
    return group


def build_cases(groups, variants, rounds, seed, options):
    orders = []
    for names in (variants, list(reversed(variants))):
        orders.extend(names[offset:] + names[:offset] for offset in range(len(names)))
    cases = []
    for round_number in range(1, rounds + 1):
        round_seed = seed + round_number - 1
        for group in groups:
            for rate in group["rates"]:
                case_id = (f"{group['network']}-{options['durability']}-r{group['read_percent']}"
                           f"-q{rate}-k{options['keys']}-h{options['hot_percent']}"
                           f"-{options['hot_keys']}-c{options['clients']}-round{round_number}")
                for position, variant in enumerate(orders[(round_number - 1) % len(orders)], start=1):
                    cases.append({"id": case_id, "round": round_number, "network": group["network"],
                                  "read_percent": group["read_percent"], "rate": rate,
                                  "durability": options["durability"], "keys": options["keys"],
                                  "hot_percent": options["hot_percent"], "hot_keys": options["hot_keys"],
                                  "clients": options["clients"], "seed": round_seed,
                                  "variant_name": variant, "position": position})
    identities = [(case["id"], case["variant_name"]) for case in cases]
    if len(identities) != len(set(identities)):
        raise ValueError("Duplicate configuration groups produce duplicate case identities")
    return cases


def load_manifest(path, selected):
    path = Path(path).resolve()
    raw = json.loads(path.read_text(encoding="utf-8-sig"))
    benchmark = absolute_path(raw["benchmark"], path.parent)
    variants = []
    for entry in raw["variants"]:
        name = entry["name"]
        if selected and name not in selected:
            continue
        if not name or any(character not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for character in name):
            raise ValueError("Variant names must contain only letters, numbers, underscores and hyphens")
        engine = entry.get("engine", "redis" if name == "redis" else "kerniva")
        if engine not in ("kerniva", "redis"):
            raise ValueError(f"Unsupported engine {engine}")
        server = absolute_path(entry["server"], path.parent)
        library = absolute_path(entry["library"], path.parent) if engine == "kerniva" else None
        variant = {"name": name, "engine": engine, "server": server,
                   "server_realpath": os.path.realpath(server), "server_sha256": file_hash(server),
                   "library": library, "library_sha256": file_hash(library) if library else None,
                   "environment": {key: str(value) for key, value in entry.get("environment", {}).items()}}
        if engine == "redis":
            version = subprocess.run([server, "--version"], capture_output=True, text=True, timeout=10, check=True)
            variant["version"] = version.stdout.strip()
        variants.append(variant)
    names = [variant["name"] for variant in variants]
    if not names or len(names) != len(set(names)):
        raise ValueError("Manifest must contain distinct selected variants")
    if selected and set(selected) != set(names):
        raise ValueError("A selected variant is missing from the manifest")
    if len(names) > 4:
        raise ValueError("At most four variants are supported per suite")
    if selected:
        variants.sort(key=lambda variant: selected.index(variant["name"]))
    return benchmark, variants


def filesystem_type(path):
    absolute = os.path.realpath(path)
    matches = []
    for line in Path("/proc/self/mountinfo").read_text().splitlines():
        before, after = line.split(" - ", 1)
        mount = before.split()[4].replace("\\040", " ").replace("\\134", "\\")
        if absolute == mount or absolute.startswith(mount.rstrip("/") + "/"):
            matches.append((len(mount), after.split()[0]))
    return max(matches)[1] if matches else None


def process_identity(pid):
    try:
        stat = Path(f"/proc/{pid}/stat").read_text()
        fields = stat[stat.rfind(")") + 2:].split()
        return {"pid": pid, "start_ticks": int(fields[19]), "session": int(fields[3]),
                "executable": os.path.realpath(f"/proc/{pid}/exe")}
    except (FileNotFoundError, ProcessLookupError):
        return None


def identity_matches(expected):
    actual = process_identity(expected["pid"])
    return actual == expected


def signal_owned(identity, number):
    if not identity_matches(identity):
        if process_identity(identity["pid"]) is None:
            return False
        raise RuntimeError(f"Process identity changed for PID {identity['pid']}")
    if identity["session"] != identity["pid"]:
        raise RuntimeError("Owned process is missing its isolated session")
    try:
        os.killpg(identity["pid"], number)
        return True
    except ProcessLookupError:
        return False


def read_process_sample(pid, sample_private_memory=False):
    try:
        stat = Path(f"/proc/{pid}/stat").read_text()
        fields = stat[stat.rfind(")") + 2:].split()
        status = {}
        for line in Path(f"/proc/{pid}/status").read_text().splitlines():
            key, _, value = line.partition(":")
            status[key] = value.strip()
        io = {}
        try:
            for line in Path(f"/proc/{pid}/io").read_text().splitlines():
                key, value = line.split(":", 1)
                io[key] = int(value)
        except PermissionError:
            pass
        private = None
        if sample_private_memory:
            try:
                private = sum(int(line.split()[1]) * 1024
                              for line in Path(f"/proc/{pid}/smaps_rollup").read_text().splitlines()
                              if line.startswith(("Private_Clean:", "Private_Dirty:", "Private_Hugetlb:")))
            except (FileNotFoundError, PermissionError):
                pass
        return {"cpu_seconds": (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK"),
                "working_set_bytes": int(status["VmRSS"].split()[0]) * 1024,
                "private_bytes": private, "threads": int(fields[17]),
                "read_bytes": io.get("read_bytes"), "write_bytes": io.get("write_bytes"),
                "rchar": io.get("rchar"), "wchar": io.get("wchar"),
                "voluntary_context_switches": int(status.get("voluntary_ctxt_switches", "0")),
                "involuntary_context_switches": int(status.get("nonvoluntary_ctxt_switches", "0"))}
    except (FileNotFoundError, ProcessLookupError, KeyError):
        return None


def system_sample():
    memory = {}
    for line in Path("/proc/meminfo").read_text().splitlines():
        key, value = line.split(":", 1)
        if key in ("MemTotal", "MemAvailable", "SwapTotal", "SwapFree", "Dirty", "Writeback"):
            memory[key] = int(value.split()[0]) * 1024
    return {"memory_bytes": memory, "load_average": list(os.getloadavg())}


def read_resp(reader):
    header = reader.readline()
    if not header.endswith(b"\r\n"):
        raise RuntimeError("Incomplete RESP header")
    kind, value = header[:1], header[1:-2]
    if kind == b"-":
        raise RuntimeError("RESP error " + value.decode("utf-8", errors="replace"))
    if kind == b"+":
        return value.decode("utf-8")
    if kind == b":":
        return int(value)
    if kind == b"$":
        length = int(value)
        if length == -1:
            return None
        payload = reader.read(length + 2)
        if len(payload) != length + 2 or payload[-2:] != b"\r\n":
            raise RuntimeError("Incomplete RESP bulk payload")
        return payload[:-2].decode("utf-8")
    if kind == b"*":
        count = int(value)
        return None if count == -1 else [read_resp(reader) for _ in range(count)]
    raise RuntimeError("Unsupported RESP reply type")


def command(port, *arguments, timeout=3):
    encoded = [str(argument).encode("utf-8") for argument in arguments]
    request = b"*" + str(len(encoded)).encode() + b"\r\n"
    request += b"".join(b"$" + str(len(argument)).encode() + b"\r\n" + argument + b"\r\n" for argument in encoded)
    with socket.create_connection(("127.0.0.1", port), timeout=timeout) as connection:
        connection.settimeout(timeout)
        connection.sendall(request)
        with connection.makefile("rb") as reader:
            return read_resp(reader)


def server_info(port):
    raw = command(port, "INFO")
    return {key: value.strip() for line in raw.splitlines()
            if ":" in line for key, value in [line.split(":", 1)]}


def metadata_sample(data_directory):
    files = []
    for path in sorted(data_directory.rglob("*")):
        if path.is_file() and ("checkpoint" in path.name or path.suffix == ".seg"):
            try:
                stat = path.stat()
                files.append({"path": str(path.relative_to(data_directory)), "size": stat.st_size,
                              "mtime_ns": stat.st_mtime_ns, "inode": stat.st_ino})
            except FileNotFoundError:
                continue
    return {"utc": utc_now(), "files": files,
            "scope": "filesystem endpoint observations; maintenance durations require engine events"}


def wait_for_process(process, identity, timeout):
    try:
        process.wait(timeout=timeout)
        return True
    except subprocess.TimeoutExpired:
        signal_owned(identity, signal.SIGTERM)
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        signal_owned(identity, signal.SIGKILL)
        process.wait(timeout=5)
    return False


def stop_server(process, identity, variant, stop_file, timeout):
    if process.poll() is not None:
        if process.returncode:
            raise RuntimeError(f"Server exited with code {process.returncode}")
        return
    if variant["engine"] == "kerniva":
        stop_file.write_text("stop\n", encoding="utf-8")
    else:
        signal_owned(identity, signal.SIGTERM)
    graceful = wait_for_process(process, identity, timeout)
    if not graceful or process.returncode:
        raise RuntimeError(f"Server shutdown required escalation or returned {process.returncode}")


def cleanup_previous(state_path):
    if not state_path.exists():
        return
    state = json.loads(state_path.read_text())
    for name in ("client", "server"):
        identity = state.get(name)
        if not identity or process_identity(identity["pid"]) is None:
            continue
        signal_owned(identity, signal.SIGTERM)
        deadline = time.monotonic() + 5
        while identity_matches(identity) and time.monotonic() < deadline:
            time.sleep(0.1)
        if identity_matches(identity):
            signal_owned(identity, signal.SIGKILL)
    state_path.unlink(missing_ok=True)


def check_deadline(deadline, required_seconds=0):
    if deadline is not None and time.time() + required_seconds >= deadline:
        raise TimeoutError("Suite deadline reached")


def bounded_wait(predicate, process, server, seconds, deadline, description):
    timeout = time.monotonic() + seconds
    while not predicate():
        if process.poll() is not None:
            raise RuntimeError(f"{description} process exited with code {process.returncode}")
        if server is not None and server.poll() is not None:
            raise RuntimeError(f"Server exited during {description}")
        check_deadline(deadline)
        if time.monotonic() >= timeout:
            raise TimeoutError(f"{description} deadline exceeded")
        time.sleep(0.05)


def numeric_delta(before, after, key):
    if before.get(key) is None or after.get(key) is None:
        return None
    return int(after[key]) - int(before[key])


def summarize_resources(samples, before, after):
    first, last = samples[0], samples[-1]
    seconds = last["seconds"] - first["seconds"]
    cpu = last["cpu_seconds"] - first["cpu_seconds"]
    private = [sample["private_bytes"] for sample in samples if sample["private_bytes"] is not None]
    client = [sample["client"] for sample in samples if sample["client"] is not None]
    return {"sample_interval_seconds": 1, "resource_window_seconds": seconds,
            "cpu_seconds": cpu, "cpu_percent_one_core": 100 * cpu / seconds,
            "cpu_percent_machine": 100 * cpu / seconds / len(os.sched_getaffinity(0)),
            "rss_peak_bytes": max(sample["working_set_bytes"] for sample in samples),
            "private_peak_bytes": max(private) if private else None,
            "private_memory_definition": "Optional Linux smaps_rollup Private_Clean + Private_Dirty + Private_Hugetlb; null when observer disabled",
            "heap_alloc_before_bytes": int(before["heap_alloc_bytes"]) if "heap_alloc_bytes" in before else None,
            "heap_alloc_after_bytes": int(after["heap_alloc_bytes"]) if "heap_alloc_bytes" in after else None,
            "go_num_gc_delta": numeric_delta(before, after, "num_gc"),
            "go_pause_ns_delta": numeric_delta(before, after, "gc_pause_total_ns"),
            "read_bytes": numeric_delta(first, last, "read_bytes") if first.get("read_bytes") is not None else None,
            "write_bytes": numeric_delta(first, last, "write_bytes") if first.get("write_bytes") is not None else None,
            "client_rss_sampled_peak_bytes": max(sample["working_set_bytes"] for sample in client) if client else None,
            "info_sampling": "before_start_marker_and_after_client_exit",
            "resource_scope": "start marker through client exit includes drain, reporting and controller polling"}


def validate_measurement(measurement, case, options):
    for operation in (measurement, measurement["get"], measurement["set"]):
        if operation["offered"] != operation["succeeded"] + operation["failures"]:
            raise ValueError("Client result has an inconsistent planned-request denominator")
    expected = {"seed": case["seed"], "target_rate": case["rate"], "keys": case["keys"],
                "clients": case["clients"], "read_percent": case["read_percent"], "value_bytes": 32}
    for key, value in expected.items():
        if measurement[key] != value:
            raise ValueError(f"Client result {key} does not match the planned configuration")
    if measurement["offered"] != case["rate"] * options["duration_ns"] // 1_000_000_000:
        raise ValueError("Client offered count does not match the fixed arrival plan")


def write_server_configuration(attempt, data, port, variant, case, options):
    if variant["engine"] == "redis":
        appendonly = "no" if options["durability"] == "memory" else "yes"
        appendfsync = "always" if options["durability"] == "sync" else "no"
        configuration = {"bind": "127.0.0.1", "port": port, "protected-mode": "yes",
                         "daemonize": "no", "databases": 1, "dir": str(data), "save": "",
                         "pidfile": str(attempt / "redis.pid"), "logfile": "",
                         "appendonly": appendonly, "appendfsync": appendfsync,
                         "no-appendfsync-on-rewrite": "no", "auto-aof-rewrite-percentage": 100,
                         "auto-aof-rewrite-min-size": "64mb", "maxmemory": options["max_memory_bytes"],
                         "maxmemory-policy": "noeviction"}
        config = attempt / "redis.conf"
        config.write_text("".join(f"{key} {json.dumps(value, ensure_ascii=False) if isinstance(value, str) else value}\n"
                                  for key, value in configuration.items()), encoding="utf-8")
        arguments = [variant["server"], str(config)]
    else:
        configuration = {"host": "127.0.0.1", "port": port, "network_type": case["network"],
                         "db_count": 1, "max_memory": options["max_memory_bytes"],
                         "max_memory_policy": "noeviction", "persistence_type": "kerniva",
                         "persistence_enabled": options["durability"] != "memory", "data_dir": str(data),
                         "cold_start_strategy": "lazy_load", "persistence_write_mode": "strong",
                         "persistence_durability": options["durability"] if options["durability"] != "memory" else "sync",
                         "engine_segment_size": options["segment_size_bytes"],
                         "engine_checkpoint_after_ops": options["checkpoint_after_ops"],
                         "log_level": "error", "log_path": str(attempt / "server.log"), "pprof_enabled": False}
        config = attempt / "config.json"
        write_json(config, configuration)
        arguments = [variant["server"], "-config", str(config), "-stop-file", str(attempt / "stop")]
    return configuration, arguments


def run_case(case_directory, variant, case, plan, benchmark, options, args, attempt_number):
    attempt = case_directory / f"attempt-{attempt_number:04d}"
    attempt.mkdir()
    data = attempt / "data"
    data.mkdir()
    state_path = case_directory / "running.json"
    stop_file, ready_file, start_file = [attempt / name for name in ("stop", "ready", "start")]
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    configuration, server_arguments = write_server_configuration(attempt, data, port, variant, case, options)
    environment = {key: value for key, value in os.environ.items() if not key.startswith("KERNIVA_")}
    environment.update({key: value.replace("{case_dir}", str(attempt))
                        for key, value in variant["environment"].items()})
    if variant["library"]:
        environment["KERNIVA_ENGINE_LIBRARY"] = variant["library"]
    state = {"server": None, "client": None, "attempt_directory": str(attempt), "started_utc": utc_now()}
    server = client = None
    result = None
    failures = []
    handles = []
    samples = []
    try:
        for name in ("server.stdout.log", "server.stderr.log", "client.jsonl", "client.stderr.log", "process.jsonl"):
            handles.append((attempt / name).open("w", encoding="utf-8", buffering=1))
        server_out, server_err, client_out, client_err, process_out = handles
        server = subprocess.Popen(server_arguments, cwd=attempt, env=environment, stdout=server_out,
                                  stderr=server_err, start_new_session=True)
        state["server"] = process_identity(server.pid)
        write_json(state_path, state)

        def ready():
            try:
                return command(port, "PING", timeout=0.25) == "PONG"
            except (OSError, RuntimeError):
                return False

        bounded_wait(ready, server, None, args.startup_timeout, args.deadline_epoch, "Server readiness")
        if variant["engine"] == "redis":
            effective = command(port, "CONFIG", "GET", "appendonly", "appendfsync", "save", "maxmemory", "maxmemory-policy")
            effective = dict(zip(effective[::2], effective[1::2]))
            write_json(attempt / "effective-redis-config.json", effective)
            for key in ("appendonly", "appendfsync", "save", "maxmemory", "maxmemory-policy"):
                if effective[key] != str(configuration[key]):
                    raise ValueError(f"Redis effective configuration mismatch for {key}")
        arguments = [benchmark, "-mode", "mixed", "-addr", f"127.0.0.1:{port}",
                     "-clients", str(case["clients"]), "-rate", str(case["rate"]),
                     "-duration", f"{options['duration_ns']}ns", "-warmup", f"{options['warmup_ns']}ns",
                     "-timeout", f"{options['timeout_ms']}ms", "-keys", str(case["keys"]),
                     "-read-percent", str(case["read_percent"]), "-hot-percent", str(case["hot_percent"]),
                     "-hot-keys", str(case["hot_keys"]), "-seed", str(case["seed"]),
                     "-queue-per-client", str(options["queue_per_client"]),
                     "-preload-batch", str(options["preload_batch"]),
                     "-reconnect-backoff", f"{options['reconnect_backoff_ms']}ms",
                     "-network", case["network"], "-durability", case["durability"],
                     "-histogram-out", str(attempt / "histogram.json"),
                     "-ready-file", str(ready_file), "-start-file", str(start_file)]
        write_json(attempt / "commands.json", {"server": server_arguments, "client": arguments,
                                               "engine_environment": {key: environment[key] for key in variant["environment"]}})
        client = subprocess.Popen(arguments, cwd=attempt, stdout=client_out, stderr=client_err, start_new_session=True)
        state["client"] = process_identity(client.pid)
        write_json(state_path, state)
        bounded_wait(ready_file.exists, client, server, args.preload_timeout,
                     args.deadline_epoch, "Client preload and warmup")
        info_before = server_info(port)
        write_json(attempt / "info-before.json", info_before)
        write_json(attempt / "metadata-before.json", metadata_sample(data))
        began = time.monotonic()

        def sample():
            observed = read_process_sample(server.pid, args.sample_private_memory)
            if observed is None:
                raise RuntimeError("Server process disappeared during resource sampling")
            observed.update(utc=utc_now(), seconds=time.monotonic() - began,
                            client=read_process_sample(client.pid, args.sample_private_memory), system=system_sample())
            samples.append(observed)
            process_out.write(json.dumps(observed, allow_nan=False) + "\n")

        sample()
        start_file.write_text("start\n", encoding="utf-8")
        next_sample = 1.0
        measurement_timeout = options["duration_seconds"] + options["timeout_ms"] / 1000 + args.measurement_extra_timeout
        while client.poll() is None:
            if server.poll() is not None:
                raise RuntimeError("Server exited during measurement")
            check_deadline(args.deadline_epoch)
            elapsed = time.monotonic() - began
            if elapsed > measurement_timeout:
                raise TimeoutError("Client measurement and drain deadline exceeded")
            if elapsed >= next_sample:
                sample()
                next_sample = math.floor(elapsed) + 1
            time.sleep(0.05)
        sample()
        client_out.flush()
        if client.returncode:
            raise RuntimeError(f"Benchmark client exited with code {client.returncode}")
        measurement = json.loads((attempt / "client.jsonl").read_text())
        validate_measurement(measurement, case, options)
        info_after = server_info(port)
        write_json(attempt / "info-after.json", info_after)
        write_json(attempt / "metadata-after.json", metadata_sample(data))
        result = {"status": "complete", "completed_utc": utc_now(), "plan_id": plan["plan_id"],
                  "case": case, "variant": variant, "client_sha256": plan["benchmark_sha256"],
                  "measurement": measurement, "resources": summarize_resources(samples, info_before, info_after),
                  "configuration": configuration, "observer": plan["observer"], "method_version": plan["method_version"],
                  "engine_network": "redis-tcp" if variant["engine"] == "redis" else case["network"],
                  "temporary_root": str(data), "attempt": attempt_number,
                  "process_identity": state,
                  "memory_budget_scope": "Kerniva payload/cache accounting; Redis allocator accounting; process RSS sampled separately",
                  "maintenance_observations": {"source": "info endpoints and filesystem endpoints",
                                               "begin_end_duration_available": False,
                                               "engine_event_environment": variant["environment"]}}
    except Exception as error:
        failures.append({"phase": "execution", "type": type(error).__name__, "message": str(error),
                         "traceback": traceback.format_exc(), "utc": utc_now()})
    finally:
        if client is not None and client.poll() is None:
            try:
                signal_owned(state["client"], signal.SIGTERM)
                wait_for_process(client, state["client"], 5)
            except Exception as error:
                failures.append({"phase": "client_cleanup", "type": type(error).__name__, "message": str(error), "utc": utc_now()})
        if server is not None:
            try:
                stop_server(server, state["server"], variant, stop_file, args.stop_timeout)
            except Exception as error:
                failures.append({"phase": "server_cleanup", "type": type(error).__name__, "message": str(error), "utc": utc_now()})
        for handle in handles:
            handle.close()
    if failures:
        failed = {"status": "failed", "case": case, "variant": variant, "attempt": attempt_number,
                  "errors": failures, "partial_result": result, "completed_utc": utc_now(), "plan_id": plan["plan_id"]}
        write_json(attempt / "attempt-failed.json", failed)
        write_json(case_directory / f"attempt-{attempt_number:04d}.failed.json", failed)
        if all(process is None or process.poll() is not None for process in (client, server)):
            state_path.unlink(missing_ok=True)
        return False
    result["temporary_data_removed"] = False
    if not args.keep_data:
        if data.is_symlink() or data.resolve().parent != attempt.resolve():
            raise RuntimeError("Data directory ownership changed before cleanup")
        shutil.rmtree(data)
        result["temporary_data_removed"] = True
    for artifact in attempt.iterdir():
        if artifact.is_file():
            destination = case_directory / artifact.name
            if destination.exists():
                destination.unlink()
            os.link(artifact, destination)
    result["artifact_sha256"] = {path.name: file_hash(path) for path in case_directory.iterdir()
                                if path.is_file() and path.name != "running.json" and not path.name.endswith(".failed.json")}
    write_json(case_directory / "result.json", result)
    state_path.unlink(missing_ok=True)
    return True


def completed_result_valid(path, plan, variant):
    result = json.loads(path.read_text())
    if result["status"] != "complete" or result["plan_id"] != plan["plan_id"]:
        return False
    if result["client_sha256"] != plan["benchmark_sha256"] or result["variant"] != variant:
        return False
    for name, expected in result["artifact_sha256"].items():
        artifact = path.parent / name
        if not artifact.is_file() or file_hash(artifact) != expected:
            return False
    return True


def run_suite(args):
    import fcntl

    if sys.platform != "linux":
        raise RuntimeError("The Linux runner requires Linux /proc")
    root = Path(args.run_root).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    selected = args.variants.split(",") if args.variants else None
    benchmark, variants = load_manifest(args.manifest, selected)
    for path in [str(root), benchmark] + [variant["server"] for variant in variants] + [variant["library"] for variant in variants if variant["library"]]:
        if filesystem_type(path) != "ext4":
            raise ValueError(f"Experiment path must reside on the WSL ext4 filesystem: {path}")
    options = {"duration_ns": round(args.duration_seconds * 1_000_000_000),
               "warmup_ns": round(args.warmup_seconds * 1_000_000_000), "keys": args.keys,
               "clients": args.clients, "hot_percent": args.hot_percent, "hot_keys": args.hot_keys,
               "queue_per_client": args.queue_per_client, "timeout_ms": args.timeout_ms,
               "reconnect_backoff_ms": args.reconnect_backoff_ms, "preload_batch": args.preload_batch,
               "max_memory_bytes": args.max_memory_bytes, "durability": args.durability,
               "segment_size_bytes": args.segment_size_bytes, "checkpoint_after_ops": args.checkpoint_after_ops}
    options["duration_seconds"] = options["duration_ns"] / 1_000_000_000
    options["warmup_seconds"] = options["warmup_ns"] / 1_000_000_000
    groups = args.group or [parse_group("std:95:5000,10000,15000"), parse_group("gnet:80:1250,2500,3750")]
    if args.rates:
        rates = [int(value) for value in args.rates.split(",")]
        if not rates or any(rate <= 0 for rate in rates) or len(rates) != len(set(rates)):
            raise ValueError("Override rates must be distinct positive integers")
        groups = [dict(group, rates=rates) for group in groups]
    cases = build_cases(groups, [variant["name"] for variant in variants], args.rounds, args.seed, options)
    if any(case["rate"] * options["duration_ns"] // 1_000_000_000 <= 0 for case in cases):
        raise ValueError("Each window must plan at least one request")
    observer = {"process_sampling": "Linux /proc stat, status and io at 1-second intervals",
                "sample_private_memory": args.sample_private_memory,
                "info_sampling": "after preload/warmup before start marker, and after client exit only",
                "info_during_measurement": False, "maintenance_metadata": "filesystem endpoints only",
                "native_event_environment": {variant["name"]: variant["environment"] for variant in variants},
                "resource_interval": "start-marker to client exit, including drain/reporting/polling"}
    plan = {"schema_version": 5, "method_version": "linux-proc-info-endpoints-v1", "suite": args.suite,
            "variants": variants, "benchmark": benchmark, "benchmark_sha256": file_hash(benchmark),
            "runner_sha256": file_hash(__file__), "created_from": "scripts/thesis-bench-linux.py",
            "groups": groups, "networks": [group["network"] for group in groups],
            "rounds": args.rounds, "seed": args.seed, "cases": cases, "window_count": len(cases),
            "durabilities": [args.durability], "observer": observer,
            "linux_release": os.uname().release, "cpu_affinity": sorted(os.sched_getaffinity(0)),
            "runtime_environment": {key: os.environ[key] for key in ("GOGC", "GOMEMLIMIT", "GOMAXPROCS") if key in os.environ},
            "filesystem": "ext4", **options}
    plan["plan_id"] = hashlib.sha256(json.dumps(plan, sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:16]
    suite = root / "measurements" / f"{args.suite}-{plan['plan_id']}"
    suite.mkdir(parents=True, exist_ok=True)
    by_name = {variant["name"]: variant for variant in variants}
    with (suite / "suite.lock").open("a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        write_json(suite / "plan.json", plan)
        if args.plan_only:
            print(json.dumps({"status": "planned", "windows": len(cases), "suite_directory": str(suite)}))
            return 0
        completed = skipped = failed = 0
        observed_overhead = args.window_overhead_seconds
        status = "running"
        print(json.dumps({"status": status, "windows": len(cases), "suite_directory": str(suite)}), flush=True)
        with (suite / "events.jsonl").open("a", encoding="utf-8", buffering=1) as events:
            def progress(event, case=None):
                record = {"utc": utc_now(), "event": event, "completed": completed, "skipped": skipped,
                          "failed": failed, "planned": len(cases), "deadline_epoch": args.deadline_epoch}
                if case is not None:
                    record["case"] = case
                events.write(json.dumps(record) + "\n")
                write_json(suite / "state.json", record)
                if args.verbose:
                    print(json.dumps(record), flush=True)

            progress("started")
            for case in cases:
                variant = by_name[case["variant_name"]]
                directory = suite / "cases" / case["id"] / variant["name"]
                directory.mkdir(parents=True, exist_ok=True)
                cleanup_previous(directory / "running.json")
                result_path = directory / "result.json"
                if result_path.exists():
                    if not args.resume:
                        raise RuntimeError("Existing result encountered with resume disabled")
                    if not completed_result_valid(result_path, plan, variant):
                        raise RuntimeError(f"Existing result failed its completion/hash checks: {result_path}")
                    skipped += 1
                    progress("resumed_complete", case)
                    continue
                try:
                    check_deadline(args.deadline_epoch, options["duration_seconds"] + options["warmup_seconds"] + observed_overhead)
                except TimeoutError:
                    status = "deadline"
                    progress(status, case)
                    break
                for path, expected in ((benchmark, plan["benchmark_sha256"]), (variant["server"], variant["server_sha256"]),
                                       (variant["library"], variant["library_sha256"])):
                    if path and file_hash(path) != expected:
                        raise RuntimeError(f"Frozen binary changed during the suite: {path}")
                progress("case_started", case)
                case_began = time.monotonic()
                previous = [int(path.name.split("-")[1]) for path in directory.glob("attempt-*") if path.is_dir()]
                attempt_number = max(previous, default=0) + 1
                success = False
                for retry in range(args.retries + 1):
                    try:
                        check_deadline(args.deadline_epoch, options["duration_seconds"] + options["warmup_seconds"] + observed_overhead)
                    except TimeoutError:
                        status = "deadline"
                        break
                    success = run_case(directory, variant, case, plan, benchmark, options, args, attempt_number + retry)
                    if success:
                        break
                    cleanup_previous(directory / "running.json")
                observed_overhead = max(observed_overhead, time.monotonic() - case_began
                                        - options["duration_seconds"] - options["warmup_seconds"])
                if success:
                    completed += 1
                    progress("case_complete", case)
                else:
                    failed += 1
                    progress("case_failed", case)
                if status == "deadline":
                    break
            if status != "deadline":
                status = "complete" if failed == 0 else "completed_with_failures"
            progress(status)
        summary = {"status": status, "planned": len(cases), "completed": completed, "skipped": skipped,
                   "failed": failed, "plan_id": plan["plan_id"], "suite_directory": str(suite), "finished_utc": utc_now()}
        write_json(suite / "summary.json", summary)
        print(json.dumps(summary), flush=True)
        return 2 if status == "deadline" else int(failed != 0)


def parse_arguments(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-root", required=True)
    parser.add_argument("--manifest", required=True, help="JSON object with benchmark and variants[{name,engine,server,library}]")
    parser.add_argument("--variants", help="Comma-separated manifest names, in initial order")
    parser.add_argument("--group", type=parse_group, action="append", help="Repeat NETWORK:READ_PERCENT:RATE[,RATE]; defaults to the two formal groups")
    parser.add_argument("--rates", "--rate", help="Override all group arrival rates with this comma-separated list")
    parser.add_argument("--rounds", type=int, default=6)
    parser.add_argument("--duration", "--duration-seconds", dest="duration_seconds", type=float, default=60)
    parser.add_argument("--warmup", "--warmup-seconds", dest="warmup_seconds", type=float, default=15)
    parser.add_argument("--keys", type=int, default=100000)
    parser.add_argument("--clients", type=int, default=64)
    parser.add_argument("--cache-bytes", "--max-memory-bytes", dest="max_memory_bytes", type=int, default=2147483648)
    parser.add_argument("--durability", choices=("memory", "none", "flush", "sync"), default="sync")
    parser.add_argument("--suite", default="core")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--hot-percent", type=int, default=0)
    parser.add_argument("--hot-keys", type=int, default=1000)
    parser.add_argument("--timeout-ms", type=int, default=2000)
    parser.add_argument("--queue-per-client", type=int, default=4096)
    parser.add_argument("--reconnect-backoff-ms", type=int, default=1000)
    parser.add_argument("--preload-batch", type=int, default=256)
    parser.add_argument("--segment-size-bytes", type=int, default=4194304)
    parser.add_argument("--checkpoint-after-ops", type=int, default=1024)
    parser.add_argument("--startup-timeout", type=float, default=30)
    parser.add_argument("--preload-timeout", type=float, default=300)
    parser.add_argument("--stop-timeout", type=float, default=30)
    parser.add_argument("--measurement-extra-timeout", type=float, default=30)
    parser.add_argument("--window-overhead-seconds", type=float, default=25,
                        help="Initial per-window setup/drain/cleanup estimate, increased from observed elapsed time")
    parser.add_argument("--deadline", "--deadline-utc", help="Unix seconds or ISO 8601 timestamp with timezone")
    parser.add_argument("--budget-seconds", type=float, help="Optional elapsed budget, additionally bounded by deadline")
    parser.add_argument("--retries", type=int, default=1)
    parser.add_argument("--resume", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--keep-data", action="store_true")
    parser.add_argument("--sample-private-memory", action="store_true",
                        help="Opt in to smaps_rollup private-memory sampling; disabled for the default low-overhead observer")
    parser.add_argument("--plan-only", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)
    positive = ("rounds", "keys", "clients", "max_memory_bytes", "timeout_ms", "preload_batch", "segment_size_bytes",
                "checkpoint_after_ops", "duration_seconds", "startup_timeout", "preload_timeout", "stop_timeout", "measurement_extra_timeout", "window_overhead_seconds")
    for name in positive:
        value = getattr(args, name)
        if not math.isfinite(value) or value <= 0:
            parser.error(f"{name} must be positive and finite")
    if args.warmup_seconds < 0 or not math.isfinite(args.warmup_seconds):
        parser.error("warmup must be nonnegative and finite")
    if not 0 <= args.hot_percent <= 100 or not 1 <= args.hot_keys <= args.keys:
        parser.error("hot-percent must be from 0 to 100 and hot-keys within the working set")
    if min(args.queue_per_client, args.reconnect_backoff_ms, args.retries, args.seed) < 0 or args.seed + args.rounds > 2**64:
        parser.error("queue, reconnect delay, retries and seed must be nonnegative; round seeds must fit uint64")
    if not args.suite or any(character not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for character in args.suite):
        parser.error("suite must contain only letters, numbers, underscores and hyphens")
    args.deadline_epoch = None
    if args.deadline:
        try:
            args.deadline_epoch = float(args.deadline)
        except ValueError:
            stamp = datetime.fromisoformat(args.deadline.replace("Z", "+00:00"))
            if stamp.tzinfo is None:
                parser.error("deadline timestamp requires a timezone")
            args.deadline_epoch = stamp.timestamp()
        if not math.isfinite(args.deadline_epoch):
            parser.error("deadline must be finite")
    if args.budget_seconds is not None:
        if not math.isfinite(args.budget_seconds) or args.budget_seconds <= 0:
            parser.error("budget-seconds must be positive and finite")
        budget_deadline = time.time() + args.budget_seconds
        args.deadline_epoch = min(args.deadline_epoch, budget_deadline) if args.deadline_epoch is not None else budget_deadline
    return args


def main():
    args = parse_arguments()
    try:
        return run_suite(args)
    except Exception as error:
        failure = {"status": "runner_failed", "type": type(error).__name__, "message": str(error),
                   "traceback": traceback.format_exc(), "utc": utc_now()}
        root = Path(args.run_root).expanduser().resolve()
        root.mkdir(parents=True, exist_ok=True)
        write_json(root / "runner-error.json", failure)
        with (root / "runner-errors.jsonl").open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(failure) + "\n")
        print(json.dumps({key: failure[key] for key in ("status", "type", "message")}), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
