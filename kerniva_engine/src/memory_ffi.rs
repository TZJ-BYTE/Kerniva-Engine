use std::mem::{MaybeUninit, size_of};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::{ptr, slice};

use crate::{
    MemoryBatchOp, MemoryCondition, MemoryEngine, MemoryError, MemoryRead, MemoryWriteOptions,
};

const OK: i32 = 0;
const MISSING: i32 = 1;
const BUFFER_TOO_SMALL: i32 = 2;
const PAYLOAD_LIMIT: i32 = 3;
const INVALID_INTEGER: i32 = 4;
const INVALID_EXPIRATION: i32 = 5;
const INVALID_ARGUMENT: i32 = -1;
const INTERNAL: i32 = -2;

type ApiResult<T> = Result<T, i32>;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct KernivaMemorySlice {
    pub data: *const u8,
    pub len: usize,
}

#[repr(C)]
pub struct KernivaMemoryPair {
    pub key: KernivaMemorySlice,
    pub value: KernivaMemorySlice,
}

#[repr(C)]
pub struct KernivaMemoryPutOptions {
    pub ttl_ms: u64,
    pub condition: u32,
}

#[repr(C)]
pub struct KernivaMemorySpan {
    pub offset: usize,
    pub len: usize,
    pub found: usize,
}

#[repr(C)]
pub struct KernivaMemoryStats {
    pub keys: usize,
    pub payload_bytes: usize,
    pub shards: usize,
}

fn status(error: MemoryError) -> i32 {
    match error {
        MemoryError::InvalidShardCount => INVALID_ARGUMENT,
        MemoryError::InvalidExpiration => INVALID_EXPIRATION,
        MemoryError::PayloadLimitExceeded => PAYLOAD_LIMIT,
        _ => INTERNAL,
    }
}

fn ffi_call(operation: impl FnOnce() -> ApiResult<i32>) -> i32 {
    match catch_unwind(AssertUnwindSafe(operation)) {
        Ok(Ok(code) | Err(code)) => code,
        Err(_) => INTERNAL,
    }
}

fn check_region<T>(data: *const T, count: usize) -> ApiResult<()> {
    if count == 0
        || (!data.is_null()
            && data.is_aligned()
            && count
                .checked_mul(size_of::<T>())
                .is_some_and(|bytes| bytes <= isize::MAX as usize))
    {
        return Ok(());
    }
    Err(INVALID_ARGUMENT)
}

unsafe fn input<'a, T>(data: *const T, count: usize) -> ApiResult<&'a [T]> {
    check_region(data, count)?;
    if count == 0 {
        return Ok(&[]);
    }
    // The caller provides initialized values valid for the duration of the call.
    Ok(unsafe { slice::from_raw_parts(data, count) })
}

unsafe fn engine_ref<'a>(handle: *mut MemoryEngine) -> ApiResult<&'a MemoryEngine> {
    check_region(handle, 1)?;
    Ok(unsafe { &*handle })
}

unsafe fn output_bytes<'a>(dst: *mut u8, dst_len: usize) -> ApiResult<&'a mut [MaybeUninit<u8>]> {
    check_region(dst, dst_len)?;
    if dst_len == 0 {
        return Ok(&mut []);
    }
    // MaybeUninit permits C callers to supply uninitialized output storage.
    Ok(unsafe { slice::from_raw_parts_mut(dst.cast(), dst_len) })
}

unsafe fn input_keys<'a>(
    keys: *const KernivaMemorySlice,
    count: usize,
) -> ApiResult<Vec<&'a [u8]>> {
    let keys = unsafe { input(keys, count)? };
    let mut borrowed_keys = Vec::new();
    borrowed_keys
        .try_reserve_exact(count)
        .map_err(|_| INTERNAL)?;
    for key in keys {
        borrowed_keys.push(unsafe { input(key.data, key.len)? });
    }
    Ok(borrowed_keys)
}

#[unsafe(no_mangle)]
pub extern "C" fn kerniva_memory_abi_version() -> u32 {
    1
}

/// # Safety
///
/// The caller must supply writable storage for one engine pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kerniva_memory_open(
    shards: usize,
    limited: u32,
    limit: usize,
    out: *mut *mut MemoryEngine,
) -> i32 {
    ffi_call(|| {
        check_region(out, 1)?;
        unsafe { out.write(ptr::null_mut()) };
        let limit = match limited {
            0 => None,
            1 => Some(limit),
            _ => return Err(INVALID_ARGUMENT),
        };
        let engine = MemoryEngine::new(shards, limit).map_err(status)?;
        unsafe { out.write(Box::into_raw(Box::new(engine))) };
        Ok(OK)
    })
}

/// # Safety
///
/// A non-null handle must have been returned by open and no calls may be in progress.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kerniva_memory_close(handle: *mut MemoryEngine) -> i32 {
    ffi_call(|| {
        check_region(handle, 1)?;
        drop(unsafe { Box::from_raw(handle) });
        Ok(OK)
    })
}

/// # Safety
///
/// The handle must remain open. Input and output must be valid for their lengths,
/// with output disjoint from input and from the written length.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kerniva_memory_get_into(
    handle: *mut MemoryEngine,
    key: *const u8,
    key_len: usize,
    dst: *mut u8,
    dst_len: usize,
    needed: *mut usize,
) -> i32 {
    ffi_call(|| {
        check_region(needed, 1)?;
        let engine = unsafe { engine_ref(handle)? };
        let key = unsafe { input(key, key_len)? };
        let dst = unsafe { output_bytes(dst, dst_len)? };
        let (code, len) = match engine.get_into_uninit(key, dst).map_err(status)? {
            MemoryRead::Missing => (MISSING, 0),
            MemoryRead::Copied(len) => (OK, len),
            MemoryRead::BufferTooSmall(len) => (BUFFER_TOO_SMALL, len),
        };
        unsafe { needed.write(len) };
        Ok(code)
    })
}

/// # Safety
///
/// The handle must remain open and input bytes must be valid for their lengths.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kerniva_memory_put(
    handle: *mut MemoryEngine,
    key: *const u8,
    key_len: usize,
    value: *const u8,
    value_len: usize,
) -> i32 {
    ffi_call(|| {
        let engine = unsafe { engine_ref(handle)? };
        let key = unsafe { input(key, key_len)? };
        let value = unsafe { input(value, value_len)? };
        engine.put(key, value).map_err(status)?;
        Ok(OK)
    })
}

/// # Safety
///
/// The handle must remain open and key bytes must be valid for their length.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kerniva_memory_delete(
    handle: *mut MemoryEngine,
    key: *const u8,
    key_len: usize,
) -> i32 {
    ffi_call(|| {
        let engine = unsafe { engine_ref(handle)? };
        let key = unsafe { input(key, key_len)? };
        Ok(if engine.delete(key).map_err(status)? {
            OK
        } else {
            MISSING
        })
    })
}

/// # Safety
///
/// The handle and buffers must remain valid during the call. Options must
/// point to one initialized configuration.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kerniva_memory_put_options(
    handle: *mut MemoryEngine,
    key: *const u8,
    key_len: usize,
    value: *const u8,
    value_len: usize,
    options: *const KernivaMemoryPutOptions,
) -> i32 {
    ffi_call(|| {
        let engine = unsafe { engine_ref(handle)? };
        let key = unsafe { input(key, key_len)? };
        let value = unsafe { input(value, value_len)? };
        let options = &unsafe { input(options, 1)? }[0];
        let condition = match options.condition {
            0 => MemoryCondition::Any,
            1 => MemoryCondition::Missing,
            2 => MemoryCondition::Present,
            _ => return Err(INVALID_ARGUMENT),
        };
        let applied = engine
            .put_relative(key, value, condition, options.ttl_ms)
            .map_err(status)?;
        Ok(if applied { OK } else { MISSING })
    })
}

/// # Safety
///
/// The handle and key must remain valid. The TTL pointer must point to one
/// initialized integer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kerniva_memory_expire(
    handle: *mut MemoryEngine,
    key: *const u8,
    key_len: usize,
    ttl_ms: *const i64,
) -> i32 {
    ffi_call(|| {
        let engine = unsafe { engine_ref(handle)? };
        let key = unsafe { input(key, key_len)? };
        let ttl_ms = unsafe { input(ttl_ms, 1)? }[0];
        let applied = engine.expire_relative(key, ttl_ms).map_err(status)?;
        Ok(if applied { OK } else { MISSING })
    })
}

/// # Safety
///
/// The handle and key must remain valid. The output must be writable for one
/// integer and disjoint from inputs.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kerniva_memory_pttl(
    handle: *mut MemoryEngine,
    key: *const u8,
    key_len: usize,
    out: *mut i64,
) -> i32 {
    ffi_call(|| {
        check_region(out, 1)?;
        let engine = unsafe { engine_ref(handle)? };
        let key = unsafe { input(key, key_len)? };
        let ttl_ms = engine.remaining_ttl_ms(key).map_err(status)?;
        unsafe { out.write(ttl_ms) };
        Ok(OK)
    })
}

/// # Safety
///
/// The handle, key and delta must remain valid. The output must be writable
/// for one integer and disjoint from inputs.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kerniva_memory_increment(
    handle: *mut MemoryEngine,
    key: *const u8,
    key_len: usize,
    delta: *const i64,
    out: *mut i64,
) -> i32 {
    ffi_call(|| {
        check_region(out, 1)?;
        let engine = unsafe { engine_ref(handle)? };
        let key = unsafe { input(key, key_len)? };
        let delta = unsafe { input(delta, 1)? }[0];
        let value = engine
            .increment(key, delta)
            .map_err(status)?
            .ok_or(INVALID_INTEGER)?;
        unsafe { out.write(value) };
        Ok(OK)
    })
}

/// # Safety
///
/// The handle, key array and referenced keys must remain valid. The output
/// must be writable for one count and disjoint from inputs.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kerniva_memory_count_existing(
    handle: *mut MemoryEngine,
    keys: *const KernivaMemorySlice,
    count: usize,
    out: *mut usize,
) -> i32 {
    ffi_call(|| {
        check_region(out, 1)?;
        let engine = unsafe { engine_ref(handle)? };
        let keys = unsafe { input_keys(keys, count)? };
        let count = engine.count_existing(&keys).map_err(status)?;
        unsafe { out.write(count) };
        Ok(OK)
    })
}

/// # Safety
///
/// The handle, key array and referenced keys must remain valid. The output
/// must be writable for one count and disjoint from inputs.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kerniva_memory_delete_many(
    handle: *mut MemoryEngine,
    keys: *const KernivaMemorySlice,
    count: usize,
    out: *mut usize,
) -> i32 {
    ffi_call(|| {
        check_region(out, 1)?;
        let engine = unsafe { engine_ref(handle)? };
        let keys = unsafe { input_keys(keys, count)? };
        let count = engine.delete_many(&keys).map_err(status)?;
        unsafe { out.write(count) };
        Ok(OK)
    })
}

/// # Safety
///
/// The handle must remain open. The pair array and all referenced key and value
/// bytes must be valid for their lengths.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kerniva_memory_mset(
    handle: *mut MemoryEngine,
    pairs: *const KernivaMemoryPair,
    count: usize,
) -> i32 {
    ffi_call(|| {
        let engine = unsafe { engine_ref(handle)? };
        let pairs = unsafe { input(pairs, count)? };
        let mut operations = Vec::new();
        operations.try_reserve_exact(count).map_err(|_| INTERNAL)?;
        for pair in pairs {
            operations.push(MemoryBatchOp::Put {
                key: unsafe { input(pair.key.data, pair.key.len)? },
                value: unsafe { input(pair.value.data, pair.value.len)? },
                options: MemoryWriteOptions::default(),
            });
        }
        // All operations are unconditional and set no deadline.
        if !engine.apply_batch_at(&operations, 0).map_err(status)? {
            return Err(INTERNAL);
        }
        Ok(OK)
    })
}

/// # Safety
///
/// The handle must remain open. Inputs and outputs must be valid for their
/// lengths, and every output region must be disjoint from other arguments.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kerniva_memory_mget_into(
    handle: *mut MemoryEngine,
    keys: *const KernivaMemorySlice,
    count: usize,
    dst: *mut u8,
    dst_len: usize,
    spans: *mut KernivaMemorySpan,
    needed: *mut usize,
) -> i32 {
    ffi_call(|| {
        check_region(dst, dst_len)?;
        check_region(spans, count)?;
        check_region(needed, 1)?;
        let engine = unsafe { engine_ref(handle)? };
        let keys = unsafe { input_keys(keys, count)? };
        let values = engine.get_many(&keys).map_err(status)?;
        let total = values.iter().flatten().try_fold(0usize, |len, entry| {
            len.checked_add(entry.value.len()).ok_or(INTERNAL)
        })?;
        unsafe { needed.write(total) };
        if total > dst_len {
            return Ok(BUFFER_TOO_SMALL);
        }

        let mut offset = 0;
        for (index, entry) in values.iter().enumerate() {
            let (len, found) = match entry {
                Some(entry) => {
                    let len = entry.value.len();
                    if len != 0 {
                        unsafe {
                            ptr::copy_nonoverlapping(entry.value.as_ptr(), dst.add(offset), len)
                        };
                    }
                    (len, 1)
                }
                None => (0, 0),
            };
            unsafe {
                spans
                    .add(index)
                    .write(KernivaMemorySpan { offset, len, found })
            };
            offset += len;
        }
        Ok(OK)
    })
}

/// # Safety
///
/// The handle must remain open and the caller must supply writable storage for
/// one stats structure.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kerniva_memory_stats(
    handle: *mut MemoryEngine,
    out: *mut KernivaMemoryStats,
) -> i32 {
    ffi_call(|| {
        check_region(out, 1)?;
        let engine = unsafe { engine_ref(handle)? };
        let stats = engine.stats().map_err(status)?;
        unsafe {
            out.write(KernivaMemoryStats {
                keys: stats.keys,
                payload_bytes: stats.payload_bytes,
                shards: stats.shards,
            });
        }
        Ok(OK)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bytes(value: &[u8]) -> KernivaMemorySlice {
        KernivaMemorySlice {
            data: value.as_ptr(),
            len: value.len(),
        }
    }

    #[test]
    fn get_into_accepts_uninitialized_storage_and_distinguishes_empty_from_missing() {
        let mut handle = ptr::null_mut();
        let mut needed = usize::MAX;
        let mut buffer = [MaybeUninit::<u8>::uninit(); 4];
        unsafe {
            assert_eq!(kerniva_memory_open(4, 0, 0, &mut handle), OK);
            assert_eq!(
                kerniva_memory_get_into(handle, ptr::null(), 0, ptr::null_mut(), 0, &mut needed),
                MISSING
            );
            assert_eq!(needed, 0);
            assert_eq!(
                kerniva_memory_put(handle, ptr::null(), 0, ptr::null(), 0),
                OK
            );
            assert_eq!(
                kerniva_memory_get_into(handle, ptr::null(), 0, ptr::null_mut(), 0, &mut needed),
                OK
            );
            assert_eq!(needed, 0);

            let value = b"a\0bc";
            assert_eq!(
                kerniva_memory_put(handle, ptr::null(), 0, value.as_ptr(), value.len()),
                OK
            );
            assert_eq!(
                kerniva_memory_get_into(
                    handle,
                    ptr::null(),
                    0,
                    buffer.as_mut_ptr().cast(),
                    3,
                    &mut needed
                ),
                BUFFER_TOO_SMALL
            );
            assert_eq!(needed, value.len());
            assert_eq!(
                kerniva_memory_get_into(
                    handle,
                    ptr::null(),
                    0,
                    buffer.as_mut_ptr().cast(),
                    4,
                    &mut needed
                ),
                OK
            );
            assert_eq!(buffer.map(|byte| byte.assume_init()), *value);
            assert_eq!(kerniva_memory_close(handle), OK);
        }
    }

    #[test]
    fn mget_short_buffer_keeps_outputs_and_mset_rejects_entire_over_budget_batch() {
        let mut handle = ptr::null_mut();
        let mut needed = usize::MAX;
        let keys = [bytes(b"a"), bytes(b"missing"), bytes(b"b")];
        let pairs = [
            KernivaMemoryPair {
                key: keys[0],
                value: bytes(b"xy"),
            },
            KernivaMemoryPair {
                key: keys[2],
                value: bytes(b""),
            },
        ];
        let mut buffer = [0xff; 2];
        let mut spans = std::array::from_fn::<_, 3, _>(|_| KernivaMemorySpan {
            offset: usize::MAX,
            len: usize::MAX,
            found: usize::MAX,
        });
        unsafe {
            assert_eq!(kerniva_memory_open(4, 1, 4, &mut handle), OK);
            assert_eq!(kerniva_memory_mset(handle, pairs.as_ptr(), 2), OK);
            assert_eq!(
                kerniva_memory_mget_into(
                    handle,
                    keys.as_ptr(),
                    3,
                    buffer.as_mut_ptr(),
                    1,
                    spans.as_mut_ptr(),
                    &mut needed
                ),
                BUFFER_TOO_SMALL
            );
            assert_eq!(needed, 2);
            assert_eq!(buffer, [0xff; 2]);
            assert!(spans.iter().all(|span| span.found == usize::MAX));

            let rejected = [
                KernivaMemoryPair {
                    key: keys[0],
                    value: bytes(b"z"),
                },
                KernivaMemoryPair {
                    key: keys[2],
                    value: bytes(b"large"),
                },
            ];
            assert_eq!(
                kerniva_memory_mset(handle, rejected.as_ptr(), 2),
                PAYLOAD_LIMIT
            );
            assert_eq!(
                kerniva_memory_mget_into(
                    handle,
                    keys.as_ptr(),
                    3,
                    buffer.as_mut_ptr(),
                    2,
                    spans.as_mut_ptr(),
                    &mut needed
                ),
                OK
            );
            assert_eq!(buffer, *b"xy");
            assert_eq!(
                spans.map(|span| (span.offset, span.len, span.found)),
                [(0, 2, 1), (2, 0, 0), (2, 0, 1)]
            );
            assert_eq!(kerniva_memory_close(handle), OK);
        }
    }

    #[test]
    fn open_rejects_invalid_options_and_nonempty_null_input_is_invalid() {
        let mut handle = ptr::null_mut();
        unsafe {
            assert_eq!(kerniva_memory_open(3, 0, 0, &mut handle), INVALID_ARGUMENT);
            assert!(handle.is_null());
            assert_eq!(kerniva_memory_open(1, 2, 0, &mut handle), INVALID_ARGUMENT);
            assert!(handle.is_null());
            assert_eq!(kerniva_memory_open(1, 0, 0, &mut handle), OK);
            assert_eq!(
                kerniva_memory_put(handle, ptr::null(), 1, ptr::null(), 0),
                INVALID_ARGUMENT
            );
            assert_eq!(kerniva_memory_close(handle), OK);
        }
    }
}
