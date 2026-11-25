"""Windows host sampler for isolated WSL benchmark windows (standard library only)."""
import argparse
import ctypes
import json
import os
import time
from ctypes import wintypes as win
from datetime import datetime
from pathlib import Path


class DiskPerformance(ctypes.Structure):
    _fields_ = [
        ("read_bytes", ctypes.c_int64), ("write_bytes", ctypes.c_int64),
        ("read_time_100ns", ctypes.c_int64), ("write_time_100ns", ctypes.c_int64),
        ("idle_time_100ns", ctypes.c_int64), ("reads", win.DWORD),
        ("writes", win.DWORD), ("queue_depth", win.DWORD), ("splits", win.DWORD),
        ("device_query_time_100ns", ctypes.c_int64), ("storage_device_number", win.DWORD),
        ("storage_manager_name", ctypes.c_wchar * 8),
    ]


def atomic(path, data):
    temp = path.with_name(path.name + ".tmp")
    temp.write_text(json.dumps(data, sort_keys=True) + "\n", encoding="utf-8")
    temp.replace(path)


class HostCounters:
    def __init__(self, number):
        self.k = ctypes.WinDLL("kernel32", use_last_error=True)
        self.k.CreateFileW.argtypes = [win.LPCWSTR, win.DWORD, win.DWORD, ctypes.c_void_p,
                                        win.DWORD, win.DWORD, win.HANDLE]
        self.k.CreateFileW.restype = win.HANDLE
        self.k.DeviceIoControl.argtypes = [win.HANDLE, win.DWORD, ctypes.c_void_p, win.DWORD,
                                           ctypes.c_void_p, win.DWORD, ctypes.POINTER(win.DWORD),
                                           ctypes.c_void_p]
        self.k.CloseHandle.argtypes = [win.HANDLE]
        self.handle = self.k.CreateFileW(r"\\.\PhysicalDrive" + str(number), 0, 3, None, 3, 0, None)
        if self.handle == ctypes.c_void_p(-1).value:
            raise ctypes.WinError(ctypes.get_last_error())

    def sample(self):
        before, utc = time.perf_counter_ns(), time.time_ns()
        cpu = [ctypes.c_uint64() for _ in range(3)]
        if not self.k.GetSystemTimes(*(ctypes.byref(field) for field in cpu)):
            raise ctypes.WinError(ctypes.get_last_error())
        perf, returned = DiskPerformance(), win.DWORD()
        if not self.k.DeviceIoControl(self.handle, 0x70020, None, 0, ctypes.byref(perf),
                                      ctypes.sizeof(perf), ctypes.byref(returned), None):
            raise ctypes.WinError(ctypes.get_last_error())
        if returned.value < DiskPerformance.storage_manager_name.offset + 16:
            raise RuntimeError("short DISK_PERFORMANCE response")
        return {"host_mono_before_ns": before, "host_utc_ns": utc,
                "host_mono_after_ns": time.perf_counter_ns(),
                "cpu_100ns": dict(zip(("idle", "kernel_including_idle", "user"),
                                      (field.value for field in cpu))),
                "disk": {key: getattr(perf, key) for key, _ in perf._fields_}}

    def close(self):
        self.k.CloseHandle(self.handle)


def capture(folder, signal, config, counters):
    opened = json.loads(signal.read_text(encoding="utf-8"))
    ident = signal.name.removesuffix(".open.json")
    if opened["id"] != ident or opened["run_id"] != config.run_id:
        raise ValueError(f"unexpected signal identity {signal}")
    close = folder / (ident + ".close.json")
    rows = [counters.sample()]
    common = {"run_id": config.run_id, "id": ident, "pid": os.getpid(), "disk": config.disk,
              "period_s": 1, "scope": "Windows system CPU and entire physical drive"}
    ready = {**common, "host_utc_ns": time.time_ns(), "first_sample_utc_ns": rows[0]["host_utc_ns"]}
    atomic(folder / (ident + ".ready.json"), ready)
    start = time.monotonic()
    next_sample = start + 1
    limit = start + opened["duration_seconds"] + 90
    while not close.exists() and time.monotonic() < limit:
        time.sleep(min(0.2, max(0, next_sample - time.monotonic())))
        if time.monotonic() >= next_sample:
            rows.append(counters.sample())
            next_sample = start + (int(time.monotonic() - start) + 1)
    rows.append(counters.sample())
    out = folder / (ident + ".samples.jsonl")
    temporary = out.with_suffix(".tmp")
    with temporary.open("w", encoding="utf-8", newline="\n") as stream:
        for row in rows:
            stream.write(json.dumps(row, sort_keys=True) + "\n")
    temporary.replace(out)
    atomic(folder / (ident + ".receipt.json"), {**common, "samples": len(rows),
           "first_utc_ns": rows[0]["host_utc_ns"], "last_utc_ns": rows[-1]["host_utc_ns"],
           "received_close": close.exists(), "guest_open": opened,
           "guest_close": json.loads(close.read_text(encoding="utf-8")) if close.exists() else None})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--disk", type=int, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--until", required=True, help="timezone-aware ISO 8601 deadline")
    config = parser.parse_args()
    deadline = datetime.fromisoformat(config.until)
    if deadline.tzinfo is None:
        parser.error("--until requires a UTC offset")
    folder = config.directory
    folder.mkdir(parents=True, exist_ok=True)
    counters = HostCounters(config.disk)
    try:
        atomic(folder / "watcher-preflight.json", {"pid": os.getpid(), "run_id": config.run_id,
               "physical_disk_number": config.disk, "started": counters.sample()})
        while time.time() < deadline.timestamp() and not (folder / "stop").exists():
            for signal in sorted(folder.glob("*.open.json")):
                if not signal.with_name(signal.name.replace(".open.json", ".receipt.json")).exists():
                    capture(folder, signal, config, counters)
            time.sleep(.20)
    finally:
        counters.close()


if __name__ == "__main__":
    main()
