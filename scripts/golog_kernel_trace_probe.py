"""One private tracefs instance, one unprivileged ext4 write/fsync, no global tracing changes."""
import argparse
from collections import Counter
import json
import os
from pathlib import Path
import pwd
import re
import subprocess
import sys
import tempfile
import time
import uuid


def worker(marker_fd):
    with tempfile.TemporaryFile(dir=pwd.getpwuid(os.getuid()).pw_dir) as data:
        os.write(marker_fd, b"kerniva-worker-ready")
        os.close(marker_fd)
        print(os.fstat(data.fileno()).st_dev, data.fileno(), flush=True)
        if sys.stdin.buffer.readline() != b"go\n":
            return
        os.write(data.fileno(), bytes(4096))
        begin = time.monotonic_ns()
        os.fsync(data.fileno())
        print(begin, time.monotonic_ns(), flush=True)
        sys.stdin.buffer.readline()


def event_counts(trace):
    counts = Counter(re.findall(r"\d+\.\d+: (\w+):", trace))
    counts["sys_enter_fsync"] = len(re.findall(r": sys_fsync\(fd:", trace))
    counts["sys_exit_fsync"] = len(re.findall(r": sys_fsync ->", trace))
    return dict(counts)


def capture(output, username):
    user = pwd.getpwnam(username)
    if user.pw_uid == 0:
        raise ValueError("use an unprivileged account for the measured workload")
    output.mkdir(parents=True, exist_ok=False)
    instance = Path("/sys/kernel/tracing/instances") / ("kerniva-" + uuid.uuid4().hex)
    account = dict(user=user.pw_uid, group=user.pw_gid, extra_groups=[])
    report = {"instance": str(instance), "kernel": os.uname().release, "filters": {}, "missing": []}
    report["filesystem"] = subprocess.check_output(["findmnt", "-T", user.pw_dir, "-no", "SOURCE,FSTYPE"], text=True).strip()
    if not report["filesystem"].endswith(" ext4"):
        raise ValueError("temporary data must be ext4")
    instance.mkdir()
    try:
        def put(name, value):
            (instance / name).write_text(str(value))

        put("tracing_on", 0)
        put("trace_clock", "mono")
        put("buffer_size_kb", 128)
        report["clock"] = (instance / "trace_clock").read_text().strip()
        report["per_cpu_buffer_kb"] = (instance / "buffer_size_kb").read_text().strip()
        put("tracing_on", 1)
        with (instance / "trace_marker").open("wb", buffering=0) as marker, subprocess.Popen(
                [sys.executable, "-B", __file__, "--worker", str(marker.fileno())],
                pass_fds=(marker.fileno(),), stdin=subprocess.PIPE,
                              stdout=subprocess.PIPE, text=True, **account) as proc:
            try:
                device, fd = map(int, proc.stdout.readline().split())
                put("tracing_on", 0)
                handshake = (instance / "trace").read_text()
                matches = re.findall(r"-(\d+)\s+\[\d+\].*?tracing_mark_write: kerniva-worker-ready", handshake)
                if len(matches) != 1:
                    raise RuntimeError("target outer TID handshake failed")
                tid = int(matches[0])
                report["namespace_pid"] = proc.pid
                report["handshake"] = next(line for line in handshake.splitlines() if "kerniva-worker-ready" in line)
                put("trace", "")
                report.update(tid=tid, fd=fd, device=[os.major(device), os.minor(device)])
                filters = {"syscalls/sys_enter_fsync": f"common_pid == {tid}",
                           "syscalls/sys_exit_fsync": f"common_pid == {tid}",
                           "sched/sched_wakeup": f"pid == {tid}",
                           "sched/sched_switch": f"prev_pid == {tid} || next_pid == {tid}"}
                for kind in ("issue", "complete"):
                    filters["block/block_rq_" + kind] = f"dev == {(os.major(device) << 20) | os.minor(device)}"
                for name, expr in filters.items():
                    event = instance / "events" / name
                    if not event.exists():
                        report["missing"].append(name)
                        continue
                    (event / "filter").write_text(expr)
                    (event / "enable").write_text("1")
                    report["filters"][name] = (event / "filter").read_text().strip()
                put("tracing_on", 1)
                proc.stdin.write("go\n")
                proc.stdin.flush()
                report["user_fsync_bounds_ns"] = list(map(int, proc.stdout.readline().split()))
                if len(report["user_fsync_bounds_ns"]) != 2:
                    raise ValueError("worker fsync failed")
                put("tracing_on", 0)
            finally:
                proc.stdin.close()
                proc.wait(timeout=10)
        trace = (instance / "trace").read_text()
        (output / "trace.txt").write_text(trace)
        report["event_counts"] = event_counts(trace)
        report["per_cpu_stats"] = {p.parent.name: p.read_text() for p in (instance / "per_cpu").glob("cpu*/stats")}
        report["loss"] = {k: sum(int(re.search(r"^"+k+r":\s*(\d+)", s, re.M)[1])
                                 for s in report["per_cpu_stats"].values())
                          for k in ("overrun", "commit overrun", "dropped events")}
    finally:
        (instance / "tracing_on").write_text("0")
        instance.rmdir()
        report["instance_removed"] = not instance.exists()
        (output / "receipt.json").write_text(json.dumps(report, indent=2)+"\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker", type=int)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--user", default=os.environ.get("SUDO_USER"), help="unprivileged user with an ext4 home")
    args = parser.parse_args()
    if args.worker is not None:
        worker(args.worker)
    else:
        if not args.output or not args.user:
            parser.error("capture requires --output and --user (or SUDO_USER)")
        capture(args.output, args.user)
