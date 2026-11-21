use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crate::directory_lock::DirectoryLock;
use crate::error::Result;
use crate::iterator::EngineIterator;
use crate::options::Options;
use crate::reader_cache::ReaderCache;
use crate::record::ValueLocation;
use crate::segment::read_value;

#[derive(Debug, Default)]
struct SnapshotState {
    pins: BTreeMap<u64, usize>,
    pending_delete: BTreeSet<u64>,
}

#[derive(Debug, Default)]
pub(crate) struct SnapshotRegistry {
    inner: Mutex<SnapshotState>,
}

impl SnapshotRegistry {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn pin_segments(&self, segments: &[u64]) {
        let mut state = self.inner.lock().expect("snapshot registry lock poisoned");
        for segment_id in segments {
            *state.pins.entry(*segment_id).or_insert(0) += 1;
        }
    }

    pub(crate) fn schedule_delete(&self, segment_id: u64) -> bool {
        let mut state = self.inner.lock().expect("snapshot registry lock poisoned");
        if state.pins.get(&segment_id).copied().unwrap_or(0) == 0 {
            true
        } else {
            state.pending_delete.insert(segment_id);
            false
        }
    }

    pub(crate) fn release_segments(&self, segments: &[u64]) -> Vec<u64> {
        let mut state = self.inner.lock().expect("snapshot registry lock poisoned");
        let mut releasable = Vec::new();

        for segment_id in segments {
            if let Some(pin_count) = state.pins.get_mut(segment_id) {
                if *pin_count > 1 {
                    *pin_count -= 1;
                } else {
                    state.pins.remove(segment_id);
                    if state.pending_delete.remove(segment_id) {
                        releasable.push(*segment_id);
                    }
                }
            }
        }

        releasable
    }

    pub(crate) fn stats(&self) -> (usize, usize) {
        let state = self.inner.lock().expect("snapshot registry lock poisoned");
        (state.pins.len(), state.pending_delete.len())
    }

    pub(crate) fn is_pending_delete(&self, segment_id: u64) -> bool {
        self.inner
            .lock()
            .expect("snapshot registry lock poisoned")
            .pending_delete
            .contains(&segment_id)
    }
}

#[derive(Debug)]
pub struct EngineSnapshot {
    data_dir: PathBuf,
    reader_cache: Arc<Mutex<ReaderCache>>,
    lease: Arc<SnapshotLease>,
    entries: Arc<Vec<(Vec<u8>, ValueLocation)>>,
}

impl EngineSnapshot {
    pub(crate) fn new(
        options: &Options,
        reader_cache: Arc<Mutex<ReaderCache>>,
        registry: Arc<SnapshotRegistry>,
        directory_lock: Arc<DirectoryLock>,
        entries: Vec<(Vec<u8>, ValueLocation)>,
    ) -> Self {
        let pinned_segments: Vec<u64> = entries
            .iter()
            .map(|(_, location)| location.segment_id)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        registry.pin_segments(&pinned_segments);

        let lease = Arc::new(SnapshotLease {
            data_dir: options.data_dir.clone(),
            reader_cache: Arc::clone(&reader_cache),
            registry,
            pinned_segments,
            _directory_lock: directory_lock,
        });

        Self {
            data_dir: options.data_dir.clone(),
            reader_cache,
            lease,
            entries: Arc::new(entries),
        }
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let Ok(position) = self
            .entries
            .binary_search_by(|(entry_key, _)| entry_key.as_slice().cmp(key))
        else {
            return Ok(None);
        };
        let (_, location) = &self.entries[position];

        let path = self
            .data_dir
            .join("segments")
            .join(format!("{:010}.seg", location.segment_id));
        let file = self
            .reader_cache
            .lock()
            .expect("reader cache lock poisoned")
            .open_clone(location.segment_id, &path)?;
        Ok(Some(read_value(&file, location, key)?))
    }

    pub fn iter(&self) -> EngineIterator {
        let options = Options::new(&self.data_dir);
        EngineIterator::new(
            &options,
            Arc::clone(&self.reader_cache),
            Arc::clone(&self.lease),
            self.entries.clone(),
        )
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[derive(Debug)]
pub(crate) struct SnapshotLease {
    data_dir: PathBuf,
    reader_cache: Arc<Mutex<ReaderCache>>,
    registry: Arc<SnapshotRegistry>,
    pinned_segments: Vec<u64>,
    _directory_lock: Arc<DirectoryLock>,
}

impl Drop for SnapshotLease {
    fn drop(&mut self) {
        let releasable = self.registry.release_segments(&self.pinned_segments);
        if releasable.is_empty() {
            return;
        }

        let mut cache = self
            .reader_cache
            .lock()
            .expect("reader cache lock poisoned");
        for segment_id in releasable {
            cache.remove(segment_id);
            let path = self
                .data_dir
                .join("segments")
                .join(format!("{segment_id:010}.seg"));
            let _ = fs::remove_file(path);
        }
    }
}
