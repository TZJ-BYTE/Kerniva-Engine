use super::{MemoryCondition, MemoryEngine, MemoryError, MemoryWriteOptions, unix_time_ms};

impl MemoryEngine {
    pub(crate) fn put_relative(
        &self,
        key: &[u8],
        value: &[u8],
        condition: MemoryCondition,
        ttl_ms: u64,
    ) -> Result<bool, MemoryError> {
        let now = unix_time_ms();
        let expires_at_ms = if ttl_ms == 0 {
            None
        } else {
            Some(relative_deadline(now, ttl_ms)?)
        };
        let options = MemoryWriteOptions {
            condition,
            expires_at_ms,
        };
        Ok(self.put_with_at(key, value, options, now)?.is_some())
    }

    pub(crate) fn expire_relative(&self, key: &[u8], ttl_ms: i64) -> Result<bool, MemoryError> {
        if ttl_ms <= 0 {
            return self.delete(key);
        }
        let mut shard = self
            .shard(key)
            .write()
            .map_err(|_| MemoryError::LockPoisoned)?;
        let now = unix_time_ms();
        let deadline = relative_deadline(now, ttl_ms as u64)?;
        let version = shard.last_version.checked_add(1);
        let Some(entry) = shard
            .entries
            .get_mut(key)
            .filter(|entry| entry.visible_at(now))
        else {
            return Ok(false);
        };
        let version = version.ok_or(MemoryError::VersionExhausted)?;
        entry.expires_at_ms = Some(deadline);
        entry.version = version;
        shard.last_version = version;
        Ok(true)
    }

    pub(crate) fn remaining_ttl_ms(&self, key: &[u8]) -> Result<i64, MemoryError> {
        let shard = self
            .shard(key)
            .read()
            .map_err(|_| MemoryError::LockPoisoned)?;
        let Some(entry) = shard.entries.get(key) else {
            return Ok(-2);
        };
        let Some(deadline) = entry.expires_at_ms else {
            return Ok(-1);
        };
        let now = unix_time_ms();
        if now >= deadline {
            return Ok(-2);
        }
        i64::try_from(deadline - now).map_err(|_| MemoryError::InvalidExpiration)
    }

    pub(crate) fn count_existing(&self, keys: &[&[u8]]) -> Result<usize, MemoryError> {
        Ok(self
            .get_many(keys)?
            .iter()
            .filter(|value| value.is_some())
            .count())
    }

    pub(crate) fn delete_many(&self, keys: &[&[u8]]) -> Result<usize, MemoryError> {
        let indices = self.touched_shards(keys.iter().copied())?;
        let mut guards = Vec::new();
        guards
            .try_reserve_exact(indices.len())
            .map_err(|_| MemoryError::CapacityOverflow)?;
        for &index in &indices {
            guards.push(
                self.shards[index]
                    .write()
                    .map_err(|_| MemoryError::LockPoisoned)?,
            );
        }
        let now = unix_time_ms();
        let mut removed = 0;
        let mut released = 0;
        for &key in keys {
            let position = indices
                .binary_search(&self.shard_index(key))
                .expect("modified shard is locked");
            if let Some(entry) = guards[position].entries.remove(key) {
                removed += usize::from(entry.visible_at(now));
                released += key.len() + entry.value.len();
            }
        }
        self.release_payload(released);
        Ok(removed)
    }

    pub(crate) fn increment(&self, key: &[u8], delta: i64) -> Result<Option<i64>, MemoryError> {
        let mut shard = self
            .shard(key)
            .write()
            .map_err(|_| MemoryError::LockPoisoned)?;
        let now = unix_time_ms();
        let (current, expires_at_ms) =
            match shard.entries.get(key).filter(|entry| entry.visible_at(now)) {
                Some(entry) => {
                    let Some(value) = parse_integer(&entry.value) else {
                        return Ok(None);
                    };
                    (value, entry.expires_at_ms)
                }
                None => (0, None),
            };
        let Some(value) = current.checked_add(delta) else {
            return Ok(None);
        };
        let formatted = value.to_string();
        let options = MemoryWriteOptions {
            condition: MemoryCondition::Any,
            expires_at_ms,
        };
        self.put_locked(&mut shard, key, formatted.as_bytes(), options, now)?;
        Ok(Some(value))
    }
}

fn relative_deadline(now: u64, ttl_ms: u64) -> Result<u64, MemoryError> {
    now.checked_add(ttl_ms)
        .filter(|deadline| *deadline <= i64::MAX as u64)
        .ok_or(MemoryError::InvalidExpiration)
}

fn parse_integer(bytes: &[u8]) -> Option<i64> {
    match bytes {
        [b'0'] => Some(0),
        [b'1'..=b'9', ..] | [b'-', b'1'..=b'9', ..] => {
            std::str::from_utf8(bytes).ok()?.parse().ok()
        }
        _ => None,
    }
}
