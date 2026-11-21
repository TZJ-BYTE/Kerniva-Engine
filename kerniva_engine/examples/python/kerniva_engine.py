"""
Kerniva Engine - Python binding via ctypes

Build first:
    cargo build --release

Usage:
    from kerniva_engine import KernivaEngine
    engine = KernivaEngine("./data")
    engine.put(b"hello", b"world")
    print(engine.get(b"hello"))
    engine.close()
"""

import ctypes
import os
import platform
from ctypes import c_uint8, c_uint32, c_int32, c_uint64, c_char_p, c_void_p, POINTER, Structure, pointer


class KernivaBuf(Structure):
    _fields_ = [("ptr", ctypes.POINTER(c_uint8)), ("len", ctypes.c_size_t)]


class KernivaStats(Structure):
    _fields_ = [
        ("keys", c_uint64),
        ("active_segment_id", c_uint64),
        ("segment_count", c_uint64),
        ("cached_readers", c_uint64),
        ("pinned_segments", c_uint64),
        ("pending_reclaims", c_uint64),
    ]


def _find_lib():
    base = os.path.dirname(os.path.abspath(__file__))
    target = os.path.join(base, "..", "target", "release")

    system = platform.system()
    if system == "Windows":
        candidates = [os.path.join(target, "kerniva_engine.dll")]
    elif system == "Darwin":
        candidates = [os.path.join(target, "libkerniva_engine.dylib")]
    else:
        candidates = [os.path.join(target, "libkerniva_engine.so")]

    for path in candidates:
        if os.path.exists(path):
            return path
    raise FileNotFoundError(f"Library not found. Build first: cargo build --release. Tried: {candidates}")


class KernivaEngine:
    def __init__(self, data_dir, segment_size_bytes=64 << 20, checkpoint_after_ops=10000, sync_policy=0):
        self._lib = ctypes.CDLL(_find_lib())

        # Set up function signatures
        self._lib.kerniva_buf_free.argtypes = [POINTER(c_uint8), ctypes.c_size_t]
        self._lib.kerniva_buf_free.restype = None

        self._lib.kerniva_engine_open.argtypes = [c_char_p, c_uint64, c_uint64, c_uint32, POINTER(KernivaBuf)]
        self._lib.kerniva_engine_open.restype = ctypes.c_void_p

        self._lib.kerniva_engine_close.argtypes = [c_void_p, POINTER(KernivaBuf)]
        self._lib.kerniva_engine_close.restype = c_int32

        self._lib.kerniva_engine_put.argtypes = [c_void_p, POINTER(c_uint8), ctypes.c_size_t,
                                                   POINTER(c_uint8), ctypes.c_size_t, POINTER(KernivaBuf)]
        self._lib.kerniva_engine_put.restype = c_int32

        self._lib.kerniva_engine_get.argtypes = [c_void_p, POINTER(c_uint8), ctypes.c_size_t,
                                                   POINTER(KernivaBuf), POINTER(KernivaBuf)]
        self._lib.kerniva_engine_get.restype = c_int32

        self._lib.kerniva_engine_delete.argtypes = [c_void_p, POINTER(c_uint8), ctypes.c_size_t, POINTER(KernivaBuf)]
        self._lib.kerniva_engine_delete.restype = c_int32

        self._lib.kerniva_engine_maybe_checkpoint.argtypes = [c_void_p, POINTER(KernivaBuf)]
        self._lib.kerniva_engine_maybe_checkpoint.restype = c_int32

        self._lib.kerniva_engine_should_checkpoint.argtypes = [c_void_p]
        self._lib.kerniva_engine_should_checkpoint.restype = c_int32

        self._lib.kerniva_engine_compact_until_stable.argtypes = [c_void_p, c_int32, POINTER(c_uint64), POINTER(KernivaBuf)]
        self._lib.kerniva_engine_compact_until_stable.restype = c_int32

        self._lib.kerniva_engine_stats.argtypes = [c_void_p, POINTER(KernivaStats), POINTER(KernivaBuf)]
        self._lib.kerniva_engine_stats.restype = c_int32

        err = KernivaBuf()
        data_dir_bytes = data_dir.encode("utf-8")
        self._handle = self._lib.kerniva_engine_open(
            data_dir_bytes, segment_size_bytes, checkpoint_after_ops, sync_policy, pointer(err)
        )
        if not self._handle:
            msg = ctypes.string_at(err.ptr, err.len).decode() if err.ptr else "unknown error"
            self._free_buf(err)
            raise RuntimeError(f"Failed to open engine: {msg}")

    def _check(self, rc, err, op):
        if rc != 0:
            msg = ctypes.string_at(err.ptr, err.len).decode() if err.ptr else "unknown error"
            self._free_buf(err)
            raise RuntimeError(f"{op} failed: {msg}")

    def _free_buf(self, buf):
        if buf.ptr and buf.len:
            self._lib.kerniva_buf_free(buf.ptr, buf.len)

    def put(self, key: bytes, value: bytes):
        err = KernivaBuf()
        rc = self._lib.kerniva_engine_put(self._handle, key, len(key), value, len(value), pointer(err))
        self._check(rc, err, "put")

    def get(self, key: bytes) -> bytes | None:
        value_buf = KernivaBuf()
        err = KernivaBuf()
        rc = self._lib.kerniva_engine_get(self._handle, key, len(key), pointer(value_buf), pointer(err))
        if rc == 0:
            return None
        if rc < 0:
            self._check(rc, err, "get")
        result = bytes(ctypes.string_at(value_buf.ptr, value_buf.len))
        self._free_buf(value_buf)
        return result

    def delete(self, key: bytes):
        err = KernivaBuf()
        rc = self._lib.kerniva_engine_delete(self._handle, key, len(key), pointer(err))
        self._check(rc, err, "delete")

    def maybe_checkpoint(self):
        err = KernivaBuf()
        rc = self._lib.kerniva_engine_maybe_checkpoint(self._handle, pointer(err))
        self._check(rc, err, "maybe_checkpoint")

    def should_checkpoint(self) -> bool:
        return self._lib.kerniva_engine_should_checkpoint(self._handle) != 0

    def compact(self, max_rounds=16) -> int:
        rewritten = c_uint64(0)
        err = KernivaBuf()
        rc = self._lib.kerniva_engine_compact_until_stable(self._handle, max_rounds, pointer(rewritten), pointer(err))
        self._check(rc, err, "compact")
        return rewritten.value

    def stats(self) -> dict:
        s = KernivaStats()
        err = KernivaBuf()
        rc = self._lib.kerniva_engine_stats(self._handle, pointer(s), pointer(err))
        self._check(rc, err, "stats")
        return {
            "keys": s.keys,
            "active_segment_id": s.active_segment_id,
            "segment_count": s.segment_count,
            "cached_readers": s.cached_readers,
            "pinned_segments": s.pinned_segments,
            "pending_reclaims": s.pending_reclaims,
        }

    def iter_all(self):
        raise NotImplementedError("Use kerniva_engine_iter_* functions directly")

    def close(self):
        if self._handle:
            err = KernivaBuf()
            self._lib.kerniva_engine_close(self._handle, pointer(err))
            self._handle = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


if __name__ == "__main__":
    import shutil, tempfile

    data_dir = tempfile.mkdtemp(prefix="kerniva_example_")
    try:
        with KernivaEngine(data_dir) as engine:
            engine.put(b"hello", b"world")
            engine.put(b"foo", b"bar" * 100)

            val = engine.get(b"hello")
            print(f"get hello = {val}")

            engine.delete(b"hello")
            print(f"get hello after delete = {engine.get(b"hello")}")

            engine.maybe_checkpoint()
            s = engine.stats()
            print(f"stats: {s}")

            rewritten = engine.compact()
            print(f"compact: {rewritten} entries rewritten")
    finally:
        shutil.rmtree(data_dir, ignore_errors=True)
