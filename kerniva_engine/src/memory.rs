use std::collections::{HashMap, hash_map::RandomState};
use std::fmt::{self, Display};
use std::hash::BuildHasher;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

mod batch;
mod durable;
mod strings;

pub use batch::MemoryBatchOp;
pub use durable::{DurableMemoryEngine, DurableMemoryError, DurableReadMode, DurableWalStats};

type Shard = RwLock<ShardState>;

#[derive(Default)]
struct ShardState {
    entries: HashMap<Box<[u8]>, MemoryValue>,
    last_version: u64,
}

/// An in-process byte-oriented KV store backed entirely by memory.
pub struct MemoryEngine {
    shards: Vec<Shard>,
    routing: RandomState,
    payload_bytes: AtomicUsize,
    max_payload_bytes: Option<usize>,
}

/// A consistent view of resident keys and payload, including expired entries awaiting removal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MemoryStats {
    pub keys: usize,
    pub payload_bytes: usize,
    pub shards: usize,
    pub max_payload_bytes: Option<usize>,
}

/// A read handle retaining the immutable value and its metadata after the read lock is released.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemoryValue {
    pub value: Arc<[u8]>,
    /// An opaque token for conditional writes to the same key in this engine.
    pub version: u64,
    pub expires_at_ms: Option<u64>,
}

impl MemoryValue {
    fn visible_at(&self, now_ms: u64) -> bool {
        self.expires_at_ms.is_none_or(|deadline| now_ms < deadline)
    }

    fn visible_now(&self) -> bool {
        self.expires_at_ms
            .is_none_or(|deadline| unix_time_ms() < deadline)
    }
}

/// Result of copying into a caller-supplied buffer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MemoryRead {
    Missing,
    Copied(usize),
    BufferTooSmall(usize),
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum MemoryCondition {
    #[default]
    Any,
    Missing,
    Present,
    Version(u64),
}

impl MemoryCondition {
    fn matches(self, visible_version: Option<u64>) -> bool {
        match self {
            Self::Any => true,
            Self::Missing => visible_version.is_none(),
            Self::Present => visible_version.is_some(),
            Self::Version(expected) => visible_version == Some(expected),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MemoryWriteOptions {
    pub condition: MemoryCondition,
    /// Absolute milliseconds since the Unix epoch. None clears any previous expiration.
    pub expires_at_ms: Option<u64>,
}

impl MemoryWriteOptions {
    fn validate_expiration(self, now_ms: u64) -> Result<(), MemoryError> {
        if self
            .expires_at_ms
            .is_some_and(|deadline| deadline <= now_ms)
        {
            return Err(MemoryError::InvalidExpiration);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MemoryError {
    InvalidShardCount,
    PayloadLimitExceeded,
    CapacityOverflow,
    LockPoisoned,
    InvalidExpiration,
    VersionExhausted,
}

impl Display for MemoryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::InvalidShardCount => "shard count must be a positive power of two",
            Self::PayloadLimitExceeded => "resident payload budget would be exceeded",
            Self::CapacityOverflow => "required memory capacity is unavailable",
            Self::LockPoisoned => "memory shard lock is poisoned",
            Self::InvalidExpiration => "expiration must be after the current time",
            Self::VersionExhausted => "shard write version is exhausted",
        };
        f.write_str(message)
    }
}

impl std::error::Error for MemoryError {}

impl MemoryEngine {
    /// Limits the sum of resident key and value lengths. Pass None for no payload limit.
    ///
    /// The limit excludes map capacity, allocator overhead and values held by old readers.
    pub fn new(shard_count: usize, max_payload_bytes: Option<usize>) -> Result<Self, MemoryError> {
        if !shard_count.is_power_of_two() {
            return Err(MemoryError::InvalidShardCount);
        }

        let mut shards = Vec::new();
        shards
            .try_reserve_exact(shard_count)
            .map_err(|_| MemoryError::CapacityOverflow)?;
        shards.resize_with(shard_count, RwLock::default);
        Ok(Self {
            shards,
            routing: RandomState::new(),
            payload_bytes: AtomicUsize::new(0),
            max_payload_bytes,
        })
    }

    /// Reads an immutable value, checking the wall clock only for expiring entries.
    pub fn get(&self, key: &[u8]) -> Result<Option<Arc<[u8]>>, MemoryError> {
        let shard = self
            .shard(key)
            .read()
            .map_err(|_| MemoryError::LockPoisoned)?;
        let value = shard.entries.get(key).filter(|entry| entry.visible_now());
        Ok(value.map(|entry| Arc::clone(&entry.value)))
    }

    /// Copies a value with no intermediate allocation. A short buffer is untouched.
    pub fn get_into(&self, key: &[u8], dst: &mut [u8]) -> Result<MemoryRead, MemoryError> {
        self.copy_value(key, dst.len(), |bytes| {
            dst[..bytes.len()].copy_from_slice(bytes);
        })
    }

    pub(crate) fn get_into_uninit(
        &self,
        key: &[u8],
        dst: &mut [MaybeUninit<u8>],
    ) -> Result<MemoryRead, MemoryError> {
        self.copy_value(key, dst.len(), |bytes| {
            for (target, byte) in dst.iter_mut().zip(bytes) {
                target.write(*byte);
            }
        })
    }

    fn copy_value(
        &self,
        key: &[u8],
        dst_len: usize,
        copy: impl FnOnce(&[u8]),
    ) -> Result<MemoryRead, MemoryError> {
        let shard = self
            .shard(key)
            .read()
            .map_err(|_| MemoryError::LockPoisoned)?;
        let Some(entry) = shard.entries.get(key).filter(|entry| entry.visible_now()) else {
            return Ok(MemoryRead::Missing);
        };
        let len = entry.value.len();
        if len > dst_len {
            return Ok(MemoryRead::BufferTooSmall(len));
        }
        copy(&entry.value);
        Ok(MemoryRead::Copied(len))
    }

    /// Reads value and version with the caller's operation timestamp.
    pub fn get_entry_at(
        &self,
        key: &[u8],
        now_ms: u64,
    ) -> Result<Option<MemoryValue>, MemoryError> {
        let shard = self
            .shard(key)
            .read()
            .map_err(|_| MemoryError::LockPoisoned)?;
        Ok(shard
            .entries
            .get(key)
            .filter(|entry| entry.visible_at(now_ms))
            .cloned())
    }

    pub fn get_many(&self, keys: &[&[u8]]) -> Result<Vec<Option<MemoryValue>>, MemoryError> {
        self.get_many_at(keys, unix_time_ms())
    }

    /// Stores an unconditional, non-expiring value.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<(), MemoryError> {
        let _ = self.put_with_at(key, value, MemoryWriteOptions::default(), 0)?;
        Ok(())
    }

    /// Returns the new version, or None if the condition failed.
    pub fn put_with_at(
        &self,
        key: &[u8],
        value: &[u8],
        options: MemoryWriteOptions,
        now_ms: u64,
    ) -> Result<Option<u64>, MemoryError> {
        options.validate_expiration(now_ms)?;
        let mut shard = self
            .shard(key)
            .write()
            .map_err(|_| MemoryError::LockPoisoned)?;
        self.put_locked(&mut shard, key, value, options, now_ms)
    }

    fn put_locked(
        &self,
        shard: &mut ShardState,
        key: &[u8],
        value: &[u8],
        options: MemoryWriteOptions,
        now_ms: u64,
    ) -> Result<Option<u64>, MemoryError> {
        let next_version = shard.last_version.checked_add(1);
        let previous = shard.entries.get_mut(key);
        let visible_version = previous
            .as_deref()
            .filter(|entry| entry.visible_at(now_ms))
            .map(|entry| entry.version);
        if !options.condition.matches(visible_version) {
            return Ok(None);
        }
        let version = next_version.ok_or(MemoryError::VersionExhausted)?;
        let replacement = || MemoryValue {
            value: Arc::from(value),
            version,
            expires_at_ms: options.expires_at_ms,
        };
        if let Some(previous) = previous {
            let previous_len = previous.value.len();
            self.reserve_payload(value.len().saturating_sub(previous_len))?;
            *previous = replacement();
            self.release_payload(previous_len.saturating_sub(value.len()));
        } else {
            let needed = key
                .len()
                .checked_add(value.len())
                .ok_or(MemoryError::CapacityOverflow)?;
            shard
                .entries
                .try_reserve(1)
                .map_err(|_| MemoryError::CapacityOverflow)?;
            self.reserve_payload(needed)?;
            shard.entries.insert(Box::from(key), replacement());
        }
        shard.last_version = version;
        Ok(Some(version))
    }

    /// Removes the resident entry and reports whether it was logically present.
    pub fn delete(&self, key: &[u8]) -> Result<bool, MemoryError> {
        self.remove(key, None)
    }

    pub fn delete_at(&self, key: &[u8], now_ms: u64) -> Result<bool, MemoryError> {
        self.remove(key, Some(now_ms))
    }

    fn remove(&self, key: &[u8], now_ms: Option<u64>) -> Result<bool, MemoryError> {
        let mut shard = self
            .shard(key)
            .write()
            .map_err(|_| MemoryError::LockPoisoned)?;
        let Some(entry) = shard.entries.remove(key) else {
            return Ok(false);
        };
        self.release_payload(key.len() + entry.value.len());
        Ok(entry
            .expires_at_ms
            .is_none_or(|deadline| now_ms.unwrap_or_else(unix_time_ms) < deadline))
    }

    /// Reclaims expired entries one shard at a time. Intended for off-path maintenance.
    pub fn purge_expired_at(&self, now_ms: u64) -> Result<usize, MemoryError> {
        let mut removed = 0;
        for shard in &self.shards {
            let mut shard = shard.write().map_err(|_| MemoryError::LockPoisoned)?;
            let mut released = 0;
            shard.entries.retain(|key, entry| {
                let visible = entry.visible_at(now_ms);
                if !visible {
                    removed += 1;
                    released += key.len() + entry.value.len();
                }
                visible
            });
            self.release_payload(released);
        }
        Ok(removed)
    }

    /// Takes read locks in shard order so that counters describe the same state.
    pub fn stats(&self) -> Result<MemoryStats, MemoryError> {
        let guards = self
            .shards
            .iter()
            .map(RwLock::read)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| MemoryError::LockPoisoned)?;
        Ok(MemoryStats {
            keys: guards.iter().map(|shard| shard.entries.len()).sum(),
            payload_bytes: self.payload_bytes.load(Relaxed),
            shards: self.shards.len(),
            max_payload_bytes: self.max_payload_bytes,
        })
    }

    fn shard_index(&self, key: &[u8]) -> usize {
        self.routing.hash_one(key) as usize & (self.shards.len() - 1)
    }

    fn shard(&self, key: &[u8]) -> &Shard {
        &self.shards[self.shard_index(key)]
    }

    // All changes to the budget happen under a shard write lock. The atomic makes
    // reservations across shards indivisible, while the shard locks publish entries.
    fn reserve_payload(&self, bytes: usize) -> Result<(), MemoryError> {
        if bytes > 0 {
            self.payload_bytes
                .fetch_update(Relaxed, Relaxed, |previous| {
                    previous
                        .checked_add(bytes)
                        .filter(|&next| self.max_payload_bytes.is_none_or(|limit| next <= limit))
                })
                .map_err(|_| MemoryError::PayloadLimitExceeded)?;
        }
        Ok(())
    }

    fn release_payload(&self, bytes: usize) {
        if bytes > 0 {
            self.payload_bytes.fetch_sub(bytes, Relaxed);
        }
    }
}

fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            duration.as_millis().min(u64::MAX as u128) as u64
        })
}
