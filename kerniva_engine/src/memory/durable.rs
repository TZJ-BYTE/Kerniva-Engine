//! Optional synchronous persistence for the sharded in-memory store.
//!
//! A dedicated directory contains one checksummed WAL. Each commit records
//! the final edits for its keys, absolute deadlines and a global version
//! watermark. On reopen, a complete frame is replayed atomically and an
//! incomplete final frame is discarded. A complete frame with a bad checksum
//! fails recovery.
//!
//! The default write mode holds affected shard locks through synchronization.
//! With read-committed mode, affected readers can see the previous commit
//! while a new frame synchronizes. The log grows with every commit. This
//! format has no compaction or checkpoint.

use std::fmt;
use std::io;
use std::path::Path;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, Mutex, MutexGuard};

use crate::EngineError;
use crate::directory_lock::{self, DirectoryLock};

use super::batch::PreparedBatch;
use super::{
    MemoryBatchOp, MemoryCondition, MemoryEngine, MemoryError, MemoryRead, MemoryStats,
    MemoryValue, MemoryWriteOptions, ShardState, unix_time_ms,
};

mod wal;

use wal::{RecoveredEdit, Wal};

/// A sharded byte store with a synchronous write-ahead log.
///
/// All writes either synchronize one complete frame before publishing, or
/// return an error. A lost response can leave a complete frame eligible for
/// recovery. Dropping the engine releases its directory lock and closes the
/// file. There is no background work or deferred synchronization.
pub struct DurableMemoryEngine {
    memory: MemoryEngine,
    wal: Mutex<Wal>,
    read_mode: DurableReadMode,
    _directory_lock: Arc<DirectoryLock>,
}

/// Controls access to affected shards while a frame synchronizes.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum DurableReadMode {
    /// Readers wait behind an in-progress commit on affected shards.
    #[default]
    HoldAffectedShards,
    /// Readers can see the previous committed values while a new commit synchronizes.
    ReadCommitted,
}

#[derive(Debug)]
pub enum DurableMemoryError {
    Memory(MemoryError),
    Storage(EngineError),
    /// The write failed and the log could not be restored to the previous boundary.
    UncertainCommit {
        write: io::Error,
        rollback: EngineError,
    },
    /// Reopen the engine to determine the result of a previously failed commit.
    ReopenRequired,
}

impl fmt::Display for DurableMemoryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Memory(error) => fmt::Display::fmt(error, f),
            Self::Storage(error) => fmt::Display::fmt(error, f),
            Self::UncertainCommit { write, rollback } => {
                write!(f, "WAL write failed: {write}. Rollback failed: {rollback}")
            }
            Self::ReopenRequired => f.write_str("memory WAL requires reopen after a failed commit"),
        }
    }
}

impl std::error::Error for DurableMemoryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Memory(error) => Some(error),
            Self::Storage(error) => Some(error),
            Self::UncertainCommit { write, .. } => Some(write),
            Self::ReopenRequired => None,
        }
    }
}

impl From<MemoryError> for DurableMemoryError {
    fn from(error: MemoryError) -> Self {
        Self::Memory(error)
    }
}

impl From<EngineError> for DurableMemoryError {
    fn from(error: EngineError) -> Self {
        Self::Storage(error)
    }
}

impl From<io::Error> for DurableMemoryError {
    fn from(error: io::Error) -> Self {
        Self::Storage(error.into())
    }
}

/// WAL counters since the most recent open. A rollback has its own sync and
/// does not contribute to the commit synchronization timing.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DurableWalStats {
    pub log_bytes: u64,
    pub commits: u64,
    pub failed_commits: u64,
    pub sync_attempts: u64,
    pub sync_elapsed_ns: u128,
    pub requires_reopen: bool,
}

impl DurableMemoryEngine {
    pub(crate) const MAX_GROUP_PUTS: usize = 8;

    /// Opens an existing dedicated directory. Expired values are discarded at
    /// one captured wall-clock timestamp. The budget applies to the final
    /// recovered view, so replay can temporarily use more memory. Reopening
    /// with a different shard count preserves tokens.
    pub fn open(
        directory: impl AsRef<Path>,
        shards: usize,
        max_payload_bytes: Option<usize>,
    ) -> Result<Self, DurableMemoryError> {
        Self::open_at(directory, shards, max_payload_bytes, unix_time_ms())
    }

    /// Opens a durable store with an explicit reader synchronization mode.
    pub fn open_with_read_mode(
        directory: impl AsRef<Path>,
        shards: usize,
        max_payload_bytes: Option<usize>,
        read_mode: DurableReadMode,
    ) -> Result<Self, DurableMemoryError> {
        Self::open_at_with_read_mode(
            directory,
            shards,
            max_payload_bytes,
            unix_time_ms(),
            read_mode,
        )
    }

    /// Opens with an explicit recovery timestamp, primarily for deterministic replay.
    pub fn open_at(
        directory: impl AsRef<Path>,
        shards: usize,
        max_payload_bytes: Option<usize>,
        now_ms: u64,
    ) -> Result<Self, DurableMemoryError> {
        Self::open_at_with_read_mode(
            directory,
            shards,
            max_payload_bytes,
            now_ms,
            DurableReadMode::default(),
        )
    }

    /// Opens with an explicit recovery timestamp and reader synchronization mode.
    pub fn open_at_with_read_mode(
        directory: impl AsRef<Path>,
        shards: usize,
        max_payload_bytes: Option<usize>,
        now_ms: u64,
        read_mode: DurableReadMode,
    ) -> Result<Self, DurableMemoryError> {
        let mut memory = MemoryEngine::new(shards, None)?;
        let directory = directory.as_ref();
        if !std::fs::metadata(directory)?.is_dir() {
            return Err(EngineError::InvalidOptions(
                "memory WAL path must be an existing directory".into(),
            )
            .into());
        }
        let directory_lock = directory_lock::acquire(directory)?;
        if directory.join("segments").try_exists()? || directory.join("meta").try_exists()? {
            return Err(EngineError::InvalidOptions(
                "memory WAL requires a directory separate from the legacy storage engine".into(),
            )
            .into());
        }
        let wal = Wal::open(directory, |edit| replay(&mut memory, edit, now_ms))?;
        let payload = memory.shards.iter_mut().try_fold(0usize, |sum, shard| {
            let shard = shard.get_mut().map_err(|_| MemoryError::LockPoisoned)?;
            shard.entries.iter().try_fold(sum, |sum, (key, entry)| {
                sum.checked_add(key.len())
                    .and_then(|sum| sum.checked_add(entry.value.len()))
                    .ok_or(MemoryError::CapacityOverflow)
            })
        })?;
        memory.max_payload_bytes = max_payload_bytes;
        memory.payload_bytes.store(payload, Relaxed);
        if max_payload_bytes.is_some_and(|limit| payload > limit) {
            return Err(MemoryError::PayloadLimitExceeded.into());
        }
        Ok(Self {
            memory,
            wal: Mutex::new(wal),
            read_mode,
            _directory_lock: directory_lock,
        })
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Arc<[u8]>>, DurableMemoryError> {
        Ok(self.memory.get(key)?)
    }

    pub fn get_into(&self, key: &[u8], dst: &mut [u8]) -> Result<MemoryRead, DurableMemoryError> {
        Ok(self.memory.get_into(key, dst)?)
    }

    pub(crate) fn get_into_uninit(
        &self,
        key: &[u8],
        dst: &mut [std::mem::MaybeUninit<u8>],
    ) -> Result<MemoryRead, DurableMemoryError> {
        Ok(self.memory.get_into_uninit(key, dst)?)
    }

    pub(crate) fn get_many(
        &self,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<MemoryValue>>, DurableMemoryError> {
        Ok(self.memory.get_many(keys)?)
    }

    pub fn get_entry_at(
        &self,
        key: &[u8],
        now_ms: u64,
    ) -> Result<Option<MemoryValue>, DurableMemoryError> {
        Ok(self.memory.get_entry_at(key, now_ms)?)
    }

    pub fn get_many_at(
        &self,
        keys: &[&[u8]],
        now_ms: u64,
    ) -> Result<Vec<Option<MemoryValue>>, DurableMemoryError> {
        Ok(self.memory.get_many_at(keys, now_ms)?)
    }

    pub fn stats(&self) -> Result<MemoryStats, DurableMemoryError> {
        Ok(self.memory.stats()?)
    }

    /// Reads counters from the writer. It can wait behind an in-progress commit.
    pub fn wal_stats(&self) -> Result<DurableWalStats, DurableMemoryError> {
        self.wal
            .lock()
            .map(|wal| wal.stats)
            .map_err(|_| DurableMemoryError::ReopenRequired)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<(), DurableMemoryError> {
        self.put_with_at(key, value, MemoryWriteOptions::default(), 0)?;
        Ok(())
    }

    /// Attempts to synchronize several independent ordinary puts together.
    /// False allows the caller to retry each put in the original order. An
    /// error applies to the whole attempt, including uncertain durability.
    pub(crate) fn try_group_puts(
        &self,
        puts: &[(&[u8], &[u8])],
    ) -> Result<bool, DurableMemoryError> {
        if !(2..=Self::MAX_GROUP_PUTS).contains(&puts.len()) {
            return Ok(false);
        }
        // The experiment handles small groups and fits comfortably in one
        // existing WAL frame even when all keys differ.
        let request_bytes = puts.iter().try_fold(0usize, |total, (key, value)| {
            total.checked_add(key.len())?.checked_add(value.len())
        });
        if request_bytes.is_none_or(|bytes| bytes > 64 * 1024) {
            return Ok(false);
        }

        let mut wal = self.writer()?;
        if wal.version.checked_add(puts.len() as u64).is_none() {
            return Ok(false);
        }
        for &(key, value) in puts {
            let shard = self
                .memory
                .shard(key)
                .read()
                .map_err(|_| MemoryError::LockPoisoned)?;
            let eligible = shard.entries.get(key).is_some_and(|entry| {
                entry.expires_at_ms.is_none() && entry.value.len() == value.len()
            });
            if !eligible {
                return Ok(false);
            }
        }
        // Every put (including repeated keys) preserves the resident payload
        // at its own position. No other writer can alter that starting state.
        let options = MemoryWriteOptions::default();
        let mut operations = [MemoryBatchOp::Put {
            key: &[],
            value: &[],
            options,
        }; Self::MAX_GROUP_PUTS];
        for (operation, &(key, value)) in operations.iter_mut().zip(puts) {
            *operation = MemoryBatchOp::Put {
                key,
                value,
                options,
            };
        }
        Ok(self
            .commit(&mut wal, &operations[..puts.len()], 0)?
            .is_some())
    }

    /// Returns the stored version, or None when the condition fails.
    pub fn put_with_at(
        &self,
        key: &[u8],
        value: &[u8],
        options: MemoryWriteOptions,
        now_ms: u64,
    ) -> Result<Option<u64>, DurableMemoryError> {
        let mut wal = self.writer()?;
        self.commit(
            &mut wal,
            &[MemoryBatchOp::Put {
                key,
                value,
                options,
            }],
            now_ms,
        )
    }

    pub fn delete(&self, key: &[u8]) -> Result<bool, DurableMemoryError> {
        self.delete_at(key, unix_time_ms())
    }

    pub fn delete_at(&self, key: &[u8], now_ms: u64) -> Result<bool, DurableMemoryError> {
        let mut wal = self.writer()?;
        let previous = self
            .memory
            .shard(key)
            .read()
            .map_err(|_| MemoryError::LockPoisoned)?
            .entries
            .get(key)
            .map(|entry| entry.visible_at(now_ms));
        let Some(was_visible) = previous else {
            return Ok(false);
        };
        self.commit(
            &mut wal,
            &[MemoryBatchOp::Delete {
                key,
                condition: MemoryCondition::Any,
            }],
            now_ms,
        )?;
        Ok(was_visible)
    }

    pub fn apply_batch_at(
        &self,
        operations: &[MemoryBatchOp<'_>],
        now_ms: u64,
    ) -> Result<bool, DurableMemoryError> {
        let mut wal = self.writer()?;
        Ok(self.commit(&mut wal, operations, now_ms)?.is_some())
    }

    fn writer(&self) -> Result<MutexGuard<'_, Wal>, DurableMemoryError> {
        let wal = self
            .wal
            .lock()
            .map_err(|_| DurableMemoryError::ReopenRequired)?;
        if wal.stats.requires_reopen {
            return Err(DurableMemoryError::ReopenRequired);
        }
        Ok(wal)
    }

    fn commit(
        &self,
        wal: &mut Wal,
        operations: &[MemoryBatchOp<'_>],
        now_ms: u64,
    ) -> Result<Option<u64>, DurableMemoryError> {
        if operations.is_empty() {
            return Ok(Some(wal.version));
        }
        let mut version = wal.version;
        let Some(prepared) = self.memory.prepare_batch_at(operations, now_ms, |_| {
            version = version
                .checked_add(1)
                .ok_or(MemoryError::VersionExhausted)?;
            Ok(version)
        })?
        else {
            return Ok(None);
        };
        // The WAL mutex excludes every other writer, so admission can inspect
        // the committed counter and leave it alone until publication.
        let final_payload = self
            .memory
            .payload_bytes
            .load(Relaxed)
            .checked_sub(prepared.before)
            .and_then(|unchanged| unchanged.checked_add(prepared.after))
            .ok_or(MemoryError::CapacityOverflow)?;
        if self
            .memory
            .max_payload_bytes
            .is_some_and(|limit| final_payload > limit)
        {
            return Err(MemoryError::PayloadLimitExceeded.into());
        }
        let encoded = wal::encode(&prepared, version)?;
        let changes = own_changes(&prepared)?;

        let mut guards = Vec::new();
        guards
            .try_reserve_exact(prepared.indices.len())
            .map_err(|_| MemoryError::CapacityOverflow)?;
        let PreparedBatch {
            indices, shards, ..
        } = prepared;

        let release_during_sync = self.read_mode == DurableReadMode::ReadCommitted;
        if release_during_sync {
            drop(shards);
        } else {
            guards.extend(shards.into_iter().map(|shard| shard.state));
        }
        wal.append(&encoded, version)?;

        if release_during_sync {
            for index in indices {
                // Read guards cannot poison a shard. Preflight held each write
                // lock and the WAL mutex excludes every other writer.
                let state = self.memory.shards[index]
                    .write()
                    .expect("no other writer can poison a private durable shard");
                guards.push(state);
            }
        }
        for (state, changes) in guards.iter_mut().zip(changes) {
            publish(state, changes);
        }
        self.memory.payload_bytes.store(final_payload, Relaxed);
        Ok(Some(version))
    }
}

fn replay(memory: &mut MemoryEngine, edit: RecoveredEdit, now_ms: u64) {
    let key = match &edit {
        RecoveredEdit::Put(key, _) | RecoveredEdit::Delete(key) => key,
    };
    let index = memory.shard_index(key);
    let shard = memory.shards[index]
        .get_mut()
        .expect("unpublished recovery shard cannot be poisoned");
    match edit {
        RecoveredEdit::Put(key, entry) if entry.visible_at(now_ms) => {
            shard.entries.insert(key.into_boxed_slice(), entry);
        }
        RecoveredEdit::Put(key, _) | RecoveredEdit::Delete(key) => {
            shard.entries.remove(key.as_slice());
        }
    }
}

enum PreparedEdit<'input> {
    Put(Box<[u8]>, MemoryValue),
    Delete(&'input [u8]),
}

fn publish(state: &mut ShardState, changes: Vec<PreparedEdit<'_>>) {
    for edit in changes {
        match edit {
            PreparedEdit::Put(key, entry) => {
                state.entries.insert(key, entry);
            }
            PreparedEdit::Delete(key) => {
                state.entries.remove(key);
            }
        }
    }
}

fn own_changes<'input>(
    prepared: &PreparedBatch<'_, 'input>,
) -> Result<Vec<Vec<PreparedEdit<'input>>>, MemoryError> {
    let mut shards = Vec::new();
    shards
        .try_reserve_exact(prepared.shards.len())
        .map_err(|_| MemoryError::CapacityOverflow)?;
    for shard in &prepared.shards {
        let mut owned = Vec::new();
        owned
            .try_reserve_exact(shard.changes.len())
            .map_err(|_| MemoryError::CapacityOverflow)?;
        for (&key, pending) in &shard.changes {
            let change = match pending {
                Some(entry) => PreparedEdit::Put(
                    Box::from(key),
                    MemoryValue {
                        value: Arc::from(entry.bytes),
                        version: entry.version,
                        expires_at_ms: entry.expires_at_ms,
                    },
                ),
                None => PreparedEdit::Delete(key),
            };
            owned.push(change);
        }
        shards.push(owned);
    }
    Ok(shards)
}

#[cfg(test)]
mod tests;
