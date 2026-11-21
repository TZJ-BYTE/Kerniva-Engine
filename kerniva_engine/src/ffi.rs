use std::ffi::{CStr, CString, c_char};
use std::ptr;

use crate::batch::WriteOp;
use crate::engine::{Engine, EngineStats};
use crate::iterator::EngineIterator;
use crate::options::{Options, SyncPolicy};
use crate::record::{MAX_BATCH_OPERATIONS, MAX_VALUE_BYTES, RECORD_HEADER_LEN, validate_lengths};
use crate::snapshot::EngineSnapshot;

#[repr(C)]
pub struct KernivaWriteOp {
    pub kind: u32,
    pub key_ptr: *const u8,
    pub key_len: usize,
    pub value_ptr: *const u8,
    pub value_len: usize,
}

#[repr(C)]
pub struct KernivaBuf {
    pub ptr: *mut u8,
    pub len: usize,
}

#[repr(C)]
pub struct KernivaStats {
    pub keys: u64,
    pub active_segment_id: u64,
    pub segment_count: u64,
    pub cached_readers: u64,
    pub pinned_segments: u64,
    pub pending_reclaims: u64,
}

pub struct SnapshotIterHandle {
    _snapshot: EngineSnapshot,
    iter: EngineIterator,
}

fn set_error(err_out: *mut KernivaBuf, message: String) {
    if err_out.is_null() {
        return;
    }
    unsafe {
        *err_out = bytes_to_buf(message.into_bytes());
    }
}

fn bytes_to_buf(bytes: Vec<u8>) -> KernivaBuf {
    let boxed = bytes.into_boxed_slice();
    let len = boxed.len();
    let ptr = Box::into_raw(boxed) as *mut u8;
    KernivaBuf { ptr, len }
}

fn stats_to_ffi(stats: EngineStats) -> KernivaStats {
    KernivaStats {
        keys: stats.keys as u64,
        active_segment_id: stats.active_segment_id,
        segment_count: stats.segment_count as u64,
        cached_readers: stats.cached_readers as u64,
        pinned_segments: stats.pinned_segments as u64,
        pending_reclaims: stats.pending_reclaims as u64,
    }
}

fn parse_sync_policy(raw: u32) -> SyncPolicy {
    match raw {
        1 => SyncPolicy::Flush,
        2 => SyncPolicy::SyncData,
        _ => SyncPolicy::None,
    }
}

unsafe fn bytes_from_raw<'a>(ptr: *const u8, len: usize) -> &'a [u8] {
    if ptr.is_null() || len == 0 {
        &[]
    } else {
        unsafe { std::slice::from_raw_parts(ptr, len) }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn kerniva_buf_free(ptr: *mut u8, len: usize) {
    if ptr.is_null() || len == 0 {
        return;
    }
    unsafe {
        let slice_ptr = ptr::slice_from_raw_parts_mut(ptr, len);
        drop(Box::from_raw(slice_ptr));
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn kerniva_engine_open(
    data_dir: *const c_char,
    segment_size_bytes: u64,
    checkpoint_after_ops: u64,
    sync_policy: u32,
    err_out: *mut KernivaBuf,
) -> *mut Engine {
    if sync_policy > 2 {
        set_error(err_out, "unknown synchronization policy".into());
        return ptr::null_mut();
    }
    if data_dir.is_null() {
        set_error(err_out, "data_dir must not be null".to_string());
        return ptr::null_mut();
    }

    let data_dir = unsafe { CStr::from_ptr(data_dir) };
    let Ok(data_dir) = data_dir.to_str() else {
        set_error(err_out, "data_dir must be valid UTF-8".to_string());
        return ptr::null_mut();
    };

    let mut options = Options::new(data_dir);
    options.segment_size_bytes = segment_size_bytes;
    options.checkpoint_after_ops = checkpoint_after_ops;
    options.sync_policy = parse_sync_policy(sync_policy);

    match Engine::open(options) {
        Ok(engine) => Box::into_raw(Box::new(engine)),
        Err(err) => {
            set_error(err_out, err.to_string());
            ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn kerniva_engine_close(engine: *mut Engine, err_out: *mut KernivaBuf) -> i32 {
    if engine.is_null() {
        return 0;
    }

    let engine = unsafe { Box::from_raw(engine) };
    match engine.close() {
        Ok(()) => 0,
        Err(err) => {
            set_error(err_out, err.to_string());
            -1
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn kerniva_engine_put(
    engine: *mut Engine,
    key_ptr: *const u8,
    key_len: usize,
    value_ptr: *const u8,
    value_len: usize,
    err_out: *mut KernivaBuf,
) -> i32 {
    if validate_lengths(key_len, value_len).is_err()
        || (key_ptr.is_null() && key_len != 0)
        || (value_ptr.is_null() && value_len != 0)
    {
        set_error(err_out, "invalid key/value buffer or length".into());
        return -1;
    }
    let Some(engine) = (unsafe { engine.as_ref() }) else {
        set_error(err_out, "engine handle must not be null".to_string());
        return -1;
    };

    let key = unsafe { bytes_from_raw(key_ptr, key_len) };
    let value = unsafe { bytes_from_raw(value_ptr, value_len) };
    match engine.put(key, value) {
        Ok(()) => 0,
        Err(err) => {
            set_error(err_out, err.to_string());
            -1
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn kerniva_engine_get(
    engine: *mut Engine,
    key_ptr: *const u8,
    key_len: usize,
    value_out: *mut KernivaBuf,
    err_out: *mut KernivaBuf,
) -> i32 {
    if validate_lengths(key_len, 0).is_err()
        || (key_ptr.is_null() && key_len != 0)
        || value_out.is_null()
    {
        set_error(err_out, "invalid key or output buffer".into());
        return -1;
    }
    let Some(engine) = (unsafe { engine.as_ref() }) else {
        set_error(err_out, "engine handle must not be null".to_string());
        return -1;
    };

    let key = unsafe { bytes_from_raw(key_ptr, key_len) };
    match engine.get(key) {
        Ok(Some(value)) => {
            if !value_out.is_null() {
                unsafe {
                    *value_out = bytes_to_buf(value);
                }
            }
            1
        }
        Ok(None) => 0,
        Err(err) => {
            set_error(err_out, err.to_string());
            -1
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn kerniva_engine_delete(
    engine: *mut Engine,
    key_ptr: *const u8,
    key_len: usize,
    err_out: *mut KernivaBuf,
) -> i32 {
    if validate_lengths(key_len, 0).is_err() || (key_ptr.is_null() && key_len != 0) {
        set_error(err_out, "invalid key buffer or length".into());
        return -1;
    }
    let Some(engine) = (unsafe { engine.as_ref() }) else {
        set_error(err_out, "engine handle must not be null".to_string());
        return -1;
    };

    let key = unsafe { bytes_from_raw(key_ptr, key_len) };
    match engine.delete(key) {
        Ok(()) => 0,
        Err(err) => {
            set_error(err_out, err.to_string());
            -1
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn kerniva_engine_clear(engine: *mut Engine, err_out: *mut KernivaBuf) -> i32 {
    let Some(engine) = (unsafe { engine.as_ref() }) else {
        set_error(err_out, "engine handle must not be null".into());
        return -1;
    };
    match engine.clear() {
        Ok(()) => 0,
        Err(err) => {
            set_error(err_out, err.to_string());
            -1
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn kerniva_engine_write_batch(
    engine: *mut Engine,
    operations: *const KernivaWriteOp,
    operations_len: usize,
    err_out: *mut KernivaBuf,
) -> i32 {
    let Some(engine) = (unsafe { engine.as_ref() }) else {
        set_error(err_out, "engine handle must not be null".into());
        return -1;
    };
    let addressable_operations = isize::MAX as usize / std::mem::size_of::<KernivaWriteOp>();
    if operations_len > MAX_BATCH_OPERATIONS
        || operations_len > addressable_operations
        || (operations.is_null() && operations_len != 0)
    {
        set_error(err_out, "invalid batch operation buffer or count".into());
        return -1;
    }
    let operations = if operations_len == 0 {
        &[][..]
    } else {
        unsafe { std::slice::from_raw_parts(operations, operations_len) }
    };
    let mut encoded_size = 4_usize;
    for operation in operations {
        encoded_size = encoded_size
            .saturating_add(RECORD_HEADER_LEN)
            .saturating_add(operation.key_len)
            .saturating_add(operation.value_len);
        if !matches!(operation.kind, 1 | 2)
            || (operation.kind == 2 && operation.value_len != 0)
            || validate_lengths(operation.key_len, operation.value_len).is_err()
            || encoded_size > MAX_VALUE_BYTES
            || (operation.key_ptr.is_null() && operation.key_len != 0)
            || (operation.value_ptr.is_null() && operation.value_len != 0)
        {
            set_error(err_out, "invalid batch operation or size".into());
            return -1;
        }
    }
    let mut batch = Vec::with_capacity(operations.len());
    for operation in operations {
        let key = unsafe { bytes_from_raw(operation.key_ptr, operation.key_len) }.to_vec();
        let value = unsafe { bytes_from_raw(operation.value_ptr, operation.value_len) }.to_vec();
        batch.push(if operation.kind == 1 {
            WriteOp::Put(key, value)
        } else {
            WriteOp::Delete(key)
        });
    }
    match engine.write_batch(&batch) {
        Ok(()) => 0,
        Err(err) => {
            set_error(err_out, err.to_string());
            -1
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn kerniva_engine_sync(engine: *mut Engine, err_out: *mut KernivaBuf) -> i32 {
    let Some(engine) = (unsafe { engine.as_ref() }) else {
        set_error(err_out, "engine handle must not be null".into());
        return -1;
    };
    match engine.sync() {
        Ok(()) => 0,
        Err(err) => {
            set_error(err_out, err.to_string());
            -1
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn kerniva_engine_checkpoint(engine: *mut Engine, err_out: *mut KernivaBuf) -> i32 {
    let Some(engine) = (unsafe { engine.as_ref() }) else {
        set_error(err_out, "engine handle must not be null".into());
        return -1;
    };
    match engine.checkpoint() {
        Ok(()) => 0,
        Err(err) => {
            set_error(err_out, err.to_string());
            -1
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn kerniva_engine_compact_until_stable(
    engine: *mut Engine,
    max_rounds: usize,
    rewritten_out: *mut u64,
    err_out: *mut KernivaBuf,
) -> i32 {
    let Some(engine) = (unsafe { engine.as_ref() }) else {
        set_error(err_out, "engine handle must not be null".to_string());
        return -1;
    };

    match engine.compact_until_stable(max_rounds) {
        Ok(rewritten) => {
            if !rewritten_out.is_null() {
                unsafe {
                    *rewritten_out = rewritten as u64;
                }
            }
            0
        }
        Err(err) => {
            set_error(err_out, err.to_string());
            -1
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn kerniva_engine_maybe_compact(
    engine: *mut Engine,
    rewritten_out: *mut u64,
    err_out: *mut KernivaBuf,
) -> i32 {
    let Some(engine) = (unsafe { engine.as_ref() }) else {
        set_error(err_out, "engine handle must not be null".into());
        return -1;
    };
    match engine.maybe_compact() {
        Ok(rewritten) => {
            if !rewritten_out.is_null() {
                unsafe { *rewritten_out = rewritten as u64 };
            }
            0
        }
        Err(error) => {
            set_error(err_out, error.to_string());
            -1
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn kerniva_engine_maybe_checkpoint(
    engine: *mut Engine,
    err_out: *mut KernivaBuf,
) -> i32 {
    if engine.is_null() {
        set_error(err_out, "engine handle is null".to_string());
        return -1;
    }
    let engine = unsafe { &*engine };
    match engine.maybe_checkpoint() {
        Ok(()) => 0,
        Err(e) => {
            set_error(err_out, e.to_string());
            -1
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn kerniva_engine_should_checkpoint(engine: *const Engine) -> i32 {
    if engine.is_null() {
        return 0;
    }
    let engine = unsafe { &*engine };
    if engine.should_checkpoint() { 1 } else { 0 }
}

#[unsafe(no_mangle)]
pub extern "C" fn kerniva_engine_stats(
    engine: *mut Engine,
    stats_out: *mut KernivaStats,
    err_out: *mut KernivaBuf,
) -> i32 {
    let Some(engine) = (unsafe { engine.as_ref() }) else {
        set_error(err_out, "engine handle must not be null".to_string());
        return -1;
    };

    if stats_out.is_null() {
        set_error(err_out, "stats_out must not be null".to_string());
        return -1;
    }

    unsafe {
        *stats_out = stats_to_ffi(engine.stats());
    }
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn kerniva_engine_iter_open(
    engine: *mut Engine,
    err_out: *mut KernivaBuf,
) -> *mut SnapshotIterHandle {
    let Some(engine) = (unsafe { engine.as_ref() }) else {
        set_error(err_out, "engine handle must not be null".to_string());
        return ptr::null_mut();
    };

    let snapshot = engine.snapshot();
    let iter = snapshot.iter();
    Box::into_raw(Box::new(SnapshotIterHandle {
        _snapshot: snapshot,
        iter,
    }))
}

#[unsafe(no_mangle)]
pub extern "C" fn kerniva_engine_iter_next(
    iter_handle: *mut SnapshotIterHandle,
    key_out: *mut KernivaBuf,
    value_out: *mut KernivaBuf,
    err_out: *mut KernivaBuf,
) -> i32 {
    let Some(iter_handle) = (unsafe { iter_handle.as_mut() }) else {
        set_error(err_out, "iterator handle must not be null".to_string());
        return -1;
    };

    match iter_handle.iter.next_entry() {
        Ok(Some((key, value))) => {
            unsafe {
                if !key_out.is_null() {
                    *key_out = bytes_to_buf(key);
                }
                if !value_out.is_null() {
                    *value_out = bytes_to_buf(value);
                }
            }
            1
        }
        Ok(None) => 0,
        Err(err) => {
            set_error(err_out, err.to_string());
            -1
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn kerniva_engine_iter_close(iter_handle: *mut SnapshotIterHandle) {
    if iter_handle.is_null() {
        return;
    }
    unsafe {
        drop(Box::from_raw(iter_handle));
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn kerniva_engine_version() -> *mut c_char {
    CString::new("kerniva_engine").unwrap().into_raw()
}

#[unsafe(no_mangle)]
pub extern "C" fn kerniva_cstring_free(ptr: *mut c_char) {
    if ptr.is_null() {
        return;
    }
    unsafe {
        drop(CString::from_raw(ptr));
    }
}
