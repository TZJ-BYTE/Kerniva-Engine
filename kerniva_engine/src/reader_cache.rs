use std::collections::BTreeMap;
use std::fs::File;
use std::path::Path;

use crate::error::{EngineError, Result};

#[derive(Debug, Default)]
pub struct ReaderCache {
    files: BTreeMap<u64, (File, u64)>,
    clock: u64,
}

impl ReaderCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn open_clone(&mut self, segment_id: u64, path: &Path) -> Result<File> {
        self.clock = self.clock.wrapping_add(1);
        if let Some((file, last_used)) = self.files.get_mut(&segment_id) {
            *last_used = self.clock;
            return Ok(file.try_clone()?);
        }

        let file = File::open(path).map_err(|err| {
            if err.kind() == std::io::ErrorKind::NotFound {
                EngineError::SegmentNotFound(segment_id)
            } else {
                EngineError::Io(err)
            }
        })?;

        let clone = file.try_clone()?;
        const MAX_CACHED_READERS: usize = 128;
        if self.files.len() >= MAX_CACHED_READERS {
            let oldest = self
                .files
                .iter()
                .min_by_key(|(_, (_, last_used))| *last_used)
                .map(|(id, _)| *id);
            if let Some(oldest) = oldest {
                self.files.remove(&oldest);
            }
        }
        self.files.insert(segment_id, (file, self.clock));
        Ok(clone)
    }

    pub fn remove(&mut self, segment_id: u64) {
        self.files.remove(&segment_id);
    }

    pub fn clear(&mut self) {
        self.files.clear();
    }

    pub(crate) fn len(&self) -> usize {
        self.files.len()
    }
}
