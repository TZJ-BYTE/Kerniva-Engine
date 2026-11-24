use std::collections::{BTreeMap, btree_map};
use std::fs;
use std::iter::Peekable;
use std::path::Path;

use crate::error::{EngineError, Result};

use super::format::{LogRecord, corrupt, next_sequence, replay_log_records};

pub(crate) const MAX_DELTA_RANGES: usize = 128;
const TARGET_BLOCK_BYTES: usize = 1 << 20;
const RECORD_HEADER_BYTES: usize = 9;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct GoLogRange {
    pub first: u64,
    pub last: u64,
}

type Changes = BTreeMap<Vec<u8>, Option<Vec<u8>>>;

pub(crate) struct Delta {
    pub reset: bool,
    changes: Peekable<btree_map::IntoIter<Vec<u8>, Option<Vec<u8>>>>,
}

impl Delta {
    // The caller owns the staging directory and keeps the sealed files immutable.
    pub(crate) fn load(directory: &Path, after: u64, ranges: &[GoLogRange]) -> Result<Self> {
        if !(1..=MAX_DELTA_RANGES).contains(&ranges.len()) {
            return Err(EngineError::InvalidInput(
                "invalid Go-log delta range count".into(),
            ));
        }

        let mut reset = false;
        let mut changes = Changes::new();
        let mut previous = after;
        for &GoLogRange { first, last } in ranges {
            let expected = next_sequence(previous)?;
            if first != expected || last < first {
                return Err(corrupt(format!(
                    "expected delta sequence {expected}, found range {first}..{last}"
                )));
            }
            let path = directory.join(format!("log-{first:020}-{last:020}.seg"));
            if !fs::symlink_metadata(&path)?.is_file() {
                return Err(corrupt("invalid sealed delta file type"));
            }
            let end =
                replay_log_records(&path, previous, previous, false, |record| match record {
                    LogRecord::Put(key, value) => {
                        changes.insert(key.to_vec(), Some(value.to_vec()));
                    }
                    LogRecord::Delete(key) => {
                        changes.insert(key.to_vec(), None);
                    }
                    LogRecord::Reset => {
                        reset = true;
                        changes.clear();
                    }
                })?;
            if end.sequence != last {
                return Err(corrupt(format!(
                    "sealed log {} ends at {}, expected {last}",
                    path.display(),
                    end.sequence
                )));
            }
            previous = last;
        }
        Ok(Self {
            reset,
            changes: changes.into_iter().peekable(),
        })
    }

    pub(crate) fn next_block(&mut self) -> Result<Option<Vec<u8>>> {
        let mut block = Vec::new();
        while let Some((key, value)) = self.changes.peek() {
            let size = RECORD_HEADER_BYTES + key.len() + value.as_ref().map_or(0, Vec::len);
            if !block.is_empty() && block.len() + size > TARGET_BLOCK_BYTES {
                break;
            }
            block.try_reserve(size).map_err(std::io::Error::other)?;
            let (key, value) = self.changes.next().expect("peeked delta entry");
            let kind = if value.is_some() { 1 } else { 2 };
            let value = value.as_deref().unwrap_or_default();
            block.push(kind);
            // A validated log frame bounds both lengths below u32::MAX.
            block.extend_from_slice(&(key.len() as u32).to_le_bytes());
            block.extend_from_slice(&(value.len() as u32).to_le_bytes());
            block.extend_from_slice(&key);
            block.extend_from_slice(value);
        }
        Ok((!block.is_empty()).then_some(block))
    }
}
