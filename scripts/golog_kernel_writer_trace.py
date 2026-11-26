"""Bounded read-only observation of one live Kerniva server in a private tracefs instance."""
import argparse
import json
import os
from pathlib import Path
import re
import time
import uuid


def atomic_json(path, obj):
    tmp = path.with_suffix(".partial")
    with tmp.open("x") as stream:
        json.dump(obj, stream, indent=2)
        stream.write("\n")
    tmp.replace(path)


def put(root, name, value):
    (root / name).write_text(str(value))


def bracket(fn):
    start = time.monotonic_ns()
    fn()
    return [start, time.monotonic_ns()]


def filter_event(instance, name, expr):
    event = instance / "events" / name
    put(event, "filter", expr)
    put(event, "enable", 1)
    return (event / "filter").read_text().strip()


def thread_ids(proc):
    return sorted(int(p.name) for p in (proc / "task").iterdir())


def map_threads(instance, tids):
    """waitid(WNOWAIT) probes only our own calls; sched_process_wait prints kernel pid_nr."""
    put(instance, "tracing_on", 1)
    put(instance, "trace_marker", "kerniva-map-helper")
    text = (instance / "trace").read_text()
    helper = int(re.search(r"-(\d+)\s+\[\d+\].*tracing_mark_write: kerniva-map-helper", text)[1])
    name = "sched/sched_process_wait"
    filter_event(instance, name, f"common_pid == {helper}")
    for tid in tids:
        try:
            os.waitid(os.P_PID, tid, os.WEXITED | os.WNOHANG | os.WNOWAIT)
        except ChildProcessError:
            pass
    put(instance, "tracing_on", 0)
    text = (instance / "trace").read_text()
    outer = [int(t) for t in re.findall(r"sched_process_wait: comm=\S+ pid=(\d+)", text)]
    if len(outer) != len(tids) or len(set(outer)) != len(tids) or min(outer) <= 0:
        raise RuntimeError("ambiguous target thread mapping")
    put(instance, "events/" + name + "/enable", 0)
    put(instance, "trace", "")
    return dict(zip(tids, outer))


def fd_state(proc, active, known):
    start = time.monotonic_ns()
    try:
        target = active.stat()
    except FileNotFoundError:
        return dict(guest_mono_before_ns=start, guest_mono_after_ns=time.monotonic_ns(),
                    active_fds=[], active_path_ino=None)
    table = proc / "fd"
    def collect(entries):
        result = []
        for path in entries:
            try:
                stat = path.stat()
            except FileNotFoundError:
                continue
            if (stat.st_dev, stat.st_ino) == (target.st_dev, target.st_ino):
                result.append(dict(fd=int(path.name), dev=stat.st_dev, ino=stat.st_ino))
        return sorted(result, key=lambda item: item["fd"])
    found = collect([table / str(fd) for fd in known]) if known else []
    if not found:
        found = collect(table.iterdir())
    return dict(guest_mono_before_ns=start, guest_mono_after_ns=time.monotonic_ns(),
                active_fds=found, active_path_ino=target.st_ino)


def run(args):
    proc = Path("/proc") / str(args.pid)
    exe = (proc / "exe").resolve(strict=True)
    if exe.name != "kerniva-server":
        raise ValueError(f"expected kerniva-server, got {exe}")
    active = args.active_log.resolve(strict=True)
    if active.name != "active.log" or active.parent.name != "db_0":
        raise ValueError("expected db_0/active.log")
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=False)
    # The benchmark owner can signal stop without changing ownership of tracefs.
    os.chown(out, proc.stat().st_uid, proc.stat().st_gid)
    instance = Path("/sys/kernel/tracing/instances") / ("kerniva-writer-" + uuid.uuid4().hex)
    r = dict(pid=args.pid, exe=str(exe), active_log=str(active), instance=str(instance),
             filters={}, fd_observations=[], thread_changes=[], kernel=os.uname().release)
    instance.mkdir()
    stack_trigger = None
    try:
        put(instance, "tracing_on", 0)
        put(instance, "trace_clock", "mono")
        put(instance, "buffer_size_kb", 16384)
        r["clock"] = (instance / "trace_clock").read_text().strip()
        r["buffer_per_cpu_kb"] = (instance / "buffer_size_kb").read_text().strip()
        tids = thread_ids(proc)
        mapping = map_threads(instance, tids)
        r["thread_map"] = mapping
        def matching(field):
            return "(" + " || ".join(f"{field} == {tid}" for tid in mapping.values()) + ")"
        own = matching("common_pid")
        dev = active.stat().st_dev
        disk = f"dev == {(os.major(dev)<<20) | os.minor(dev)}"
        filters = {n: own for n in ("syscalls/sys_enter_fsync", "syscalls/sys_exit_fsync",
                                    "ext4/ext4_sync_file_enter", "ext4/ext4_sync_file_exit")}
        filters.update({"sched/sched_wakeup": matching("pid"),
                        "sched/sched_switch": matching("prev_pid")+" || "+matching("next_pid"),
                        "sched/sched_process_fork": matching("parent_pid"),
                        "block/block_rq_issue": disk, "block/block_rq_complete": disk})
        if args.journal_stacks:
            for phase in ("start_commit", "commit_locking", "commit_flushing", "commit_logging", "end_commit"):
                filters[f"jbd2/jbd2_{phase}"] = disk
        for name, expr in filters.items():
            r["filters"][name] = filter_event(instance, name, expr)
        if args.journal_stacks:
            switch = instance / "events/sched/sched_switch"
            expression = f'stacktrace if {matching("prev_pid")} && prev_state == 2'
            put(switch, "trigger", expression)
            stack_trigger = switch
            r["journal_stack_trigger"] = (switch / "trigger").read_text().strip()
        last = fd_state(proc, active, [])
        if not last["active_fds"]:
            raise RuntimeError("target has no open active log")
        r["fd_observations"].append(last)
        r["trace_start_ns"] = bracket(lambda: put(instance, "tracing_on", 1))
        atomic_json(out / "ready.json", dict(pid=args.pid, helper_pid=os.getpid(), instance=str(instance),
                                            helper_start_ticks=Path("/proc/self/stat").read_text().rsplit(")", 1)[1].split()[19],
                                            clock=r["clock"], thread_count=len(tids),
                                            trace_start_ns=r["trace_start_ns"], active_fds=last["active_fds"]))
        deadline = time.monotonic() + args.duration_sec
        last_tids = tids
        while time.monotonic() < deadline and not (out / "stop").exists():
            time.sleep(.08)
            obs = fd_state(proc, active, [x["fd"] for x in last["active_fds"]])
            if (obs["active_fds"], obs["active_path_ino"]) != (last["active_fds"], last["active_path_ino"]):
                r["fd_observations"].extend((last, obs))
            last = obs
            now_tids = thread_ids(proc)
            if now_tids != last_tids:
                r["thread_changes"].append(dict(at_ns=time.monotonic_ns(), added=sorted(set(now_tids)-set(last_tids)),
                                               removed=sorted(set(last_tids)-set(now_tids))))
                last_tids = now_tids
        r["fd_observations"].append(last)
        r["stop_reason"] = "stop_file" if (out / "stop").exists() else "deadline"
        r["trace_stop_ns"] = bracket(lambda: put(instance, "tracing_on", 0))
        stats = {p.parent.name:p.read_text() for p in (instance/"per_cpu").glob("cpu*/stats")}
        r["cpu_stats"] = stats
        r["loss"] = {k:sum(int(re.search("^"+k+r":\s*(\d+)",s,re.M)[1]) for s in stats.values())
                     for k in ("overrun","commit overrun","dropped events")}
        # Disk writes are deferred until the bounded measurement has ended.
        with (instance/"trace").open("rb") as src, (out/"trace.txt").open("xb") as dst:
            while data := src.read(1024*1024):
                dst.write(data)
    except Exception as error:
        if args.journal_stacks:
            r["error"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        try:
            put(instance, "tracing_on", 0)
            if stack_trigger is not None:
                put(stack_trigger, "trigger", "!stacktrace")
            instance.rmdir()
        finally:
            r["instance_removed"] = not instance.exists()
            atomic_json(out/"receipt.json",r)


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--pid", type=int, required=True)
    p.add_argument("--active-log", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--duration-sec", type=int, default=55, choices=range(1,121), metavar="1..120")
    p.add_argument("--journal-stacks", action="store_true")
    run(p.parse_args())
