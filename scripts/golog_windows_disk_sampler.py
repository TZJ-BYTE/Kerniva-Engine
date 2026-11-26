"""Buffer short-period Windows host samples for the physical disk behind a drive."""
import argparse
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import time

from golog_stall_host_sampler import HostCounters, atomic


def resolve_disk(drive):
    # The drive is validated before interpolation. Require the disk number and
    # the drive letter to agree with the enumerated PhysicalDisk instance.
    command = (
        "$ErrorActionPreference = 'Stop'; "
        "[Console]::OutputEncoding = [Text.UTF8Encoding]::new($false); "
        f"$partition = @(Get-Partition -DriveLetter '{drive}'); "
        "if ($partition.Count -ne 1) { throw 'Expected exactly one partition' }; "
        "$names = @(Get-CimInstance Win32_PerfRawData_PerfDisk_PhysicalDisk "
        "| Select-Object -ExpandProperty Name); "
        "[pscustomobject]@{ disk_number = [int]$partition[0].DiskNumber; "
        "partition_number = [int]$partition[0].PartitionNumber; "
        "instances = $names } | ConvertTo-Json -Compress"
    )
    result = subprocess.run(
        ["powershell.exe", "-NoProfile", "-NonInteractive", "-Command", command],
        capture_output=True, encoding="utf-8", errors="replace", check=False, timeout=30,
    )
    if result.returncode:
        raise RuntimeError(f"Could not map {drive}: to a physical disk: {result.stderr.strip()}")
    mapping = json.loads(result.stdout)
    disk_number = mapping["disk_number"]
    matches = [
        name for name in mapping["instances"]
        if name.split() and name.split()[0] == str(disk_number)
        and f"{drive}:" in name.upper().split()[1:]
    ]
    if len(matches) != 1:
        raise RuntimeError(f"Could not uniquely map {drive}: to a PhysicalDisk instance: {mapping}")
    return {"drive": f"{drive}:", **mapping, "instance": matches[0]}


def sample(counters, index):
    utc_before = time.time_ns()
    row = counters.sample()
    return {
        "index": index,
        "host_utc_before_ns": utc_before,
        **row,
        "host_utc_after_ns": time.time_ns(),
    }


def capture(counters, rows, interval_ns, end_ns, stop_file):
    start_ns = rows[0]["host_mono_before_ns"]
    next_ns = start_ns + interval_ns
    while True:
        if stop_file.exists():
            reason = "stop_file"
            break
        now_ns = time.perf_counter_ns()
        if now_ns >= end_ns:
            reason = "duration"
            break
        if now_ns >= next_ns:
            rows.append(sample(counters, len(rows)))
            elapsed_ns = time.perf_counter_ns() - start_ns
            next_ns = start_ns + (elapsed_ns // interval_ns + 1) * interval_ns
        else:
            time.sleep(min(next_ns - now_ns, end_ns - now_ns, 100_000_000) / 1_000_000_000)
    rows.append(sample(counters, len(rows)))
    return reason


def run(args):
    mapping = resolve_disk(args.drive)
    disk_number = mapping["disk_number"]
    counters = HostCounters(disk_number)
    rows = []
    reason = "error"
    folder = args.output_dir
    try:
        first = sample(counters, 0)
        if first["disk"]["storage_device_number"] != disk_number:
            raise RuntimeError(f"PhysicalDrive{disk_number} returned a different storage device: {first['disk']}")
        folder.mkdir(parents=True, exist_ok=True)
        metadata = {
            "host": platform.node(),
            "windows_version": platform.version(),
            "mapping": mapping,
            "pid": os.getpid(),
            "source": {"disk": "IOCTL_DISK_PERFORMANCE on PhysicalDrive", "cpu": "GetSystemTimes"},
            "interval_ms": args.interval_ms,
            "duration_sec": args.duration_sec,
            "stop_file": str(args.stop_file.resolve()),
            "units": {
                "host_utc_ns": "Unix epoch nanoseconds, read after the monotonic start and before the CPU and disk calls",
                "host_utc_before_ns / host_utc_after_ns": "Unix epoch nanoseconds bracketing the monotonic interval",
                "host_mono_before_ns / host_mono_after_ns": "Windows QueryPerformanceCounter via time.perf_counter_ns, normalized to nanoseconds",
                "cpu_100ns": "Cumulative CPU times in 100 ns units. Kernel includes idle. On systems with more than 64 logical processors, GetSystemTimes covers the caller's primary processor group.",
                "read_bytes / write_bytes": "Cumulative bytes",
                "read_time_100ns / write_time_100ns / idle_time_100ns": "Cumulative time in 100 ns units",
                "reads / writes / splits": "Cumulative counts, unsigned 32-bit with wraparound",
                "queue_depth": "Instantaneous number of outstanding requests",
                "device_query_time_100ns": "Disk performance provider query time in 100 ns units",
            },
            "scope": "Aggregate whole-disk and Windows CPU observations. Counter deltas include other activity. Queue depth is sampled at a point in time and can miss bursts between polls.",
            "flush_counter": None,
            "attribution": "Whole-disk observations do not identify individual requests, flushes, files, processes, or the WSL VHDX.",
        }
        atomic(folder / "metadata.json", metadata)
        rows.append(first)
        atomic(folder / "ready.json", {
            "pid": os.getpid(),
            "disk_number": disk_number,
            "instance": mapping["instance"],
            "first_sample_utc_ns": first["host_utc_ns"],
            "first_sample_mono_before_ns": first["host_mono_before_ns"],
            "first_sample_mono_after_ns": first["host_mono_after_ns"],
        })
        print(f"Sampling {mapping['drive']} on PhysicalDrive{disk_number}", flush=True)
        end_ns = first["host_mono_before_ns"] + args.duration_sec * 1_000_000_000
        reason = capture(counters, rows, args.interval_ms * 1_000_000, end_ns, args.stop_file)
    except KeyboardInterrupt:
        reason = "keyboard_interrupt"
    finally:
        counters.close()
        if rows:
            destination = folder / "samples.jsonl"
            temporary = destination.with_suffix(".jsonl.tmp")
            try:
                with temporary.open("x", encoding="utf-8", newline="\n") as stream:
                    for row in rows:
                        stream.write(json.dumps(row, separators=(",", ":")) + "\n")
                temporary.replace(destination)
            finally:
                temporary.unlink(missing_ok=True)
            atomic(folder / "metadata.json", {
                **metadata, "exit_reason": reason, "sample_count": len(rows),
                "last_sample_utc_ns": rows[-1]["host_utc_ns"],
            })


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--drive", default="F")
    parser.add_argument("--output-dir", required=True, type=Path, help="New or empty output directory")
    parser.add_argument("--interval-ms", default=40, type=int)
    parser.add_argument("--duration-sec", required=True, type=int, help="Maximum capture time after the first sample")
    parser.add_argument("--stop-file", type=Path, help="Stop when this path appears (default: output-dir/stop)")
    args = parser.parse_args()
    if platform.system() != "Windows":
        parser.error("Run with Windows Python")
    if not re.fullmatch(r"[a-zA-Z]:?", args.drive):
        parser.error("--drive must be a drive letter")
    if args.interval_ms <= 0 or args.duration_sec <= 0:
        parser.error("--interval-ms and --duration-sec must be positive")
    if args.output_dir.is_dir() and any(args.output_dir.iterdir()):
        parser.error("--output-dir must be new or empty")
    args.stop_file = args.stop_file or args.output_dir / "stop"
    if args.stop_file.exists():
        parser.error("--stop-file already exists")
    args.drive = args.drive[0].upper()
    try:
        run(args)
    except (OSError, RuntimeError, ValueError, subprocess.SubprocessError) as error:
        parser.exit(1, f"{error}\n")


if __name__ == "__main__":
    main()
