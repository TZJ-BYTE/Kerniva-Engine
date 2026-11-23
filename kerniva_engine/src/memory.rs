use std::collections::{HashMap, hash_map::RandomState};
use std::fmt::{self, Display};
use std::hash::BuildHasher;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, RwLock};

type Shard = RwLock<HashMap<Box<[u8]>, Arc<[u8]>>>;

/// An in-process byte-oriented KV store backed entirely by memory.
pub struct MemoryEngine {
    shards: Vec<Shard>,
    routing: RandomState,
    payload_bytes: AtomicUsize,
    max_payload_bytes: Option<usize>,
}

/// A consistent view of the resident key count and payload budget.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MemoryStats {
    pub keys: usize,
    pub payload_bytes: usize,
    pub shards: usize,
    pub max_payload_bytes: Option<usize>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MemoryError {
    InvalidShardCount,
    PayloadLimitExceeded,
    CapacityOverflow,
    LockPoisoned,
}

impl Display for MemoryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::InvalidShardCount => "shard count must be a positive power of two",
            Self::PayloadLimitExceeded => "resident payload budget would be exceeded",
            Self::CapacityOverflow => "required memory capacity is unavailable",
            Self::LockPoisoned => "memory shard lock is poisoned",
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

    /// Retains the current immutable value after releasing the shard read lock.
    pub fn get(&self, key: &[u8]) -> Result<Option<Arc<[u8]>>, MemoryError> {
        let shard = self
            .shard(key)
            .read()
            .map_err(|_| MemoryError::LockPoisoned)?;
        Ok(shard.get(key).cloned())
    }

    /// Publishes the new value after any needed budget reservation succeeds.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<(), MemoryError> {
        let mut shard = self
            .shard(key)
            .write()
            .map_err(|_| MemoryError::LockPoisoned)?;
        if let Some(previous) = shard.get_mut(key) {
            let previous_len = previous.len();
            self.reserve_payload(value.len().saturating_sub(previous_len))?;
            *previous = Arc::from(value);
            self.release_payload(previous_len.saturating_sub(value.len()));
            return Ok(());
        }

        let needed = key
            .len()
            .checked_add(value.len())
            .ok_or(MemoryError::CapacityOverflow)?;
        shard
            .try_reserve(1)
            .map_err(|_| MemoryError::CapacityOverflow)?;
        self.reserve_payload(needed)?;
        shard.insert(Box::from(key), Arc::from(value));
        Ok(())
    }

    pub fn delete(&self, key: &[u8]) -> Result<bool, MemoryError> {
        let mut shard = self
            .shard(key)
            .write()
            .map_err(|_| MemoryError::LockPoisoned)?;
        let Some(value) = shard.remove(key) else {
            return Ok(false);
        };
        self.release_payload(key.len() + value.len());
        Ok(true)
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
            keys: guards.iter().map(|shard| shard.len()).sum(),
            payload_bytes: self.payload_bytes.load(Relaxed),
            shards: self.shards.len(),
            max_payload_bytes: self.max_payload_bytes,
        })
    }

    fn shard(&self, key: &[u8]) -> &Shard {
        let index = self.routing.hash_one(key) as usize & (self.shards.len() - 1);
        &self.shards[index]
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
