use std::collections::HashMap;
use std::sync::{Arc, RwLockWriteGuard};

use super::{
    MemoryCondition, MemoryEngine, MemoryError, MemoryValue, MemoryWriteOptions, ShardState,
};

/// Operations in a batch are evaluated in input order, including repeated keys.
#[derive(Clone, Copy, Debug)]
pub enum MemoryBatchOp<'a> {
    Put {
        key: &'a [u8],
        value: &'a [u8],
        options: MemoryWriteOptions,
    },
    Delete {
        key: &'a [u8],
        condition: MemoryCondition,
    },
}

impl<'a> MemoryBatchOp<'a> {
    fn key(self) -> &'a [u8] {
        match self {
            Self::Put { key, .. } | Self::Delete { key, .. } => key,
        }
    }
}

pub(super) struct PendingValue<'a> {
    pub(super) bytes: &'a [u8],
    pub(super) version: u64,
    pub(super) expires_at_ms: Option<u64>,
}

pub(super) struct BatchShard<'engine, 'input> {
    pub(super) state: RwLockWriteGuard<'engine, ShardState>,
    pub(super) changes: HashMap<&'input [u8], Option<PendingValue<'input>>>,
    last_version: u64,
}

pub(super) struct PreparedBatch<'engine, 'input> {
    pub(super) shards: Vec<BatchShard<'engine, 'input>>,
    pub(super) indices: Vec<usize>,
    pub(super) before: usize,
    pub(super) after: usize,
}

impl<'input> BatchShard<'_, 'input> {
    fn stage(
        &mut self,
        operation: MemoryBatchOp<'input>,
        now_ms: u64,
        next_version: &mut impl FnMut(u64) -> Result<u64, MemoryError>,
    ) -> Result<bool, MemoryError> {
        let key = operation.key();
        let visible_version = match self.changes.get(key) {
            Some(pending) => pending.as_ref().map(|entry| entry.version),
            None => self
                .state
                .entries
                .get(key)
                .filter(|entry| entry.visible_at(now_ms))
                .map(|entry| entry.version),
        };

        let change = match operation {
            MemoryBatchOp::Put { value, options, .. } => {
                options.validate_expiration(now_ms)?;
                if !options.condition.matches(visible_version) {
                    return Ok(false);
                }
                self.last_version = next_version(self.last_version)?;
                Some(PendingValue {
                    bytes: value,
                    version: self.last_version,
                    expires_at_ms: options.expires_at_ms,
                })
            }
            MemoryBatchOp::Delete { condition, .. } => {
                if !condition.matches(visible_version) {
                    return Ok(false);
                }
                None
            }
        };

        if let Some(previous) = self.changes.get_mut(key) {
            *previous = change;
        } else {
            self.changes
                .try_reserve(1)
                .map_err(|_| MemoryError::CapacityOverflow)?;
            self.changes.insert(key, change);
        }
        Ok(true)
    }

    fn prepare(&mut self, before: &mut usize, after: &mut usize) -> Result<(), MemoryError> {
        let mut inserts = 0;
        for (key, pending) in &self.changes {
            if let Some(entry) = self.state.entries.get(*key) {
                add_payload(before, key.len(), entry.value.len())?;
            } else if pending.is_some() {
                inserts += 1;
            }
            if let Some(entry) = pending {
                add_payload(after, key.len(), entry.bytes.len())?;
            }
        }
        self.state
            .entries
            .try_reserve(inserts)
            .map_err(|_| MemoryError::CapacityOverflow)
    }

    fn publish(&mut self) {
        for (key, pending) in self.changes.drain() {
            match pending {
                Some(entry) => {
                    let replacement = MemoryValue {
                        value: Arc::from(entry.bytes),
                        version: entry.version,
                        expires_at_ms: entry.expires_at_ms,
                    };
                    if let Some(current) = self.state.entries.get_mut(key) {
                        *current = replacement;
                    } else {
                        self.state.entries.insert(Box::from(key), replacement);
                    }
                }
                None => {
                    self.state.entries.remove(key);
                }
            }
        }
        self.state.last_version = self.last_version;
    }
}

fn add_payload(total: &mut usize, key_len: usize, value_len: usize) -> Result<(), MemoryError> {
    *total = total
        .checked_add(key_len)
        .and_then(|size| size.checked_add(value_len))
        .ok_or(MemoryError::CapacityOverflow)?;
    Ok(())
}

impl MemoryEngine {
    /// Reads a coherent snapshot of the requested keys, preserving their order.
    pub fn get_many_at(
        &self,
        keys: &[&[u8]],
        now_ms: u64,
    ) -> Result<Vec<Option<MemoryValue>>, MemoryError> {
        let indices = self.touched_shards(keys.iter().copied())?;
        let guards = indices
            .iter()
            .map(|index| {
                self.shards[*index]
                    .read()
                    .map_err(|_| MemoryError::LockPoisoned)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut values = Vec::new();
        values
            .try_reserve_exact(keys.len())
            .map_err(|_| MemoryError::CapacityOverflow)?;
        for key in keys {
            let position = indices
                .binary_search(&self.shard_index(key))
                .expect("requested shard is locked");
            let value = guards[position]
                .entries
                .get(*key)
                .filter(|entry| entry.visible_at(now_ms))
                .cloned();
            values.push(value);
        }
        Ok(values)
    }

    /// Applies a batch across its affected shards. A condition failure returns
    /// false and keeps the previous values, versions and payload count.
    pub fn apply_batch_at(
        &self,
        operations: &[MemoryBatchOp<'_>],
        now_ms: u64,
    ) -> Result<bool, MemoryError> {
        let Some(mut prepared) = self.prepare_batch_at(operations, now_ms, |previous| {
            previous.checked_add(1).ok_or(MemoryError::VersionExhausted)
        })?
        else {
            return Ok(false);
        };
        self.reserve_payload(prepared.after.saturating_sub(prepared.before))?;
        for shard in &mut prepared.shards {
            shard.publish();
        }
        self.release_payload(prepared.before.saturating_sub(prepared.after));
        // All write guards are still held after every entry and version is published.
        Ok(true)
    }

    pub(super) fn prepare_batch_at<'engine, 'input>(
        &'engine self,
        operations: &[MemoryBatchOp<'input>],
        now_ms: u64,
        mut next_version: impl FnMut(u64) -> Result<u64, MemoryError>,
    ) -> Result<Option<PreparedBatch<'engine, 'input>>, MemoryError> {
        let indices = self.touched_shards(operations.iter().copied().map(MemoryBatchOp::key))?;
        let mut shards = Vec::new();
        shards
            .try_reserve_exact(indices.len())
            .map_err(|_| MemoryError::CapacityOverflow)?;
        for index in &indices {
            let state = self.shards[*index]
                .write()
                .map_err(|_| MemoryError::LockPoisoned)?;
            let last_version = state.last_version;
            shards.push(BatchShard {
                state,
                changes: HashMap::new(),
                last_version,
            });
        }

        for &operation in operations {
            let position = indices
                .binary_search(&self.shard_index(operation.key()))
                .expect("modified shard is locked");
            if !shards[position].stage(operation, now_ms, &mut next_version)? {
                return Ok(None);
            }
        }

        let mut before = 0;
        let mut after = 0;
        for shard in &mut shards {
            shard.prepare(&mut before, &mut after)?;
        }
        Ok(Some(PreparedBatch {
            shards,
            indices,
            before,
            after,
        }))
    }

    pub(super) fn touched_shards<'a>(
        &self,
        keys: impl ExactSizeIterator<Item = &'a [u8]>,
    ) -> Result<Vec<usize>, MemoryError> {
        let mut indices = Vec::new();
        indices
            .try_reserve_exact(keys.len())
            .map_err(|_| MemoryError::CapacityOverflow)?;
        indices.extend(keys.map(|key| self.shard_index(key)));
        indices.sort_unstable();
        indices.dedup();
        Ok(indices)
    }
}
