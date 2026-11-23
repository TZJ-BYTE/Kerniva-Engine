use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::Write;
use std::sync::{Arc, Mutex, RwLock};

use crate::batch::WriteOp;
use crate::checkpoint;
use crate::directory_lock::{self, DirectoryLock};
use crate::error::{EngineError, Result};
use crate::iterator::EngineIterator;
use crate::options::{Options, SyncPolicy};
use crate::reader_cache::ReaderCache;
use crate::record::{RECORD_HEADER_LEN, Record, ValueLocation, validate_lengths};
use crate::segment::{
    list_segment_ids, open_active_segment, read_value, scan_segment, truncate_segment,
};
use crate::snapshot::{EngineSnapshot, SnapshotRegistry};

#[derive(Debug)]
struct WriterState {
    active_segment_id: u64,
    active_len: u64,
    active_file: File,
    ops_since_checkpoint: u64,
    write_error: Option<String>,
    #[cfg(test)]
    fail_next_write_after: Option<usize>,
    #[cfg(test)]
    sync_pause: Option<SyncPause>,
}

#[cfg(test)]
#[derive(Debug)]
struct SyncPause {
    entered: std::sync::mpsc::Sender<()>,
    release: Arc<(Mutex<bool>, std::sync::Condvar)>,
    fail: bool,
}

#[derive(Debug)]
pub struct Engine {
    options: Options,
    // Writers take this mutex before the index lock. Readers never take it.
    writer: Mutex<WriterState>,
    index: RwLock<BTreeMap<Vec<u8>, ValueLocation>>,
    reader_cache: Arc<Mutex<ReaderCache>>,
    snapshot_registry: Arc<SnapshotRegistry>,
    directory_lock: Arc<DirectoryLock>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct EngineStats {
    pub keys: usize,
    pub active_segment_id: u64,
    pub segment_count: usize,
    pub cached_readers: usize,
    pub pinned_segments: usize,
    pub pending_reclaims: usize,
}

impl Engine {
    pub fn open(options: Options) -> Result<Self> {
        options.validate()?;
        fs::create_dir_all(options.data_dir())?;
        let directory_lock = directory_lock::acquire(options.data_dir())?;
        if options.data_dir().join("memory-v1.wal").try_exists()? {
            return Err(EngineError::InvalidOptions(
                "legacy engine requires a directory separate from the durable memory store".into(),
            ));
        }
        fs::create_dir_all(options.segments_dir())?;
        fs::create_dir_all(options.meta_dir())?;
        let checkpoint = checkpoint::load(&options.checkpoint_path())?;
        let segment_ids = list_segment_ids(&options.segments_dir())?;
        let segment_lengths: BTreeMap<u64, u64> = segment_ids
            .iter()
            .map(|id| fs::metadata(options.segment_path(*id)).map(|metadata| (*id, metadata.len())))
            .collect::<std::io::Result<_>>()?;
        if let Some(checkpoint) = &checkpoint {
            let active_length = segment_lengths
                .get(&checkpoint.active_segment_id)
                .ok_or(EngineError::SegmentNotFound(checkpoint.active_segment_id))?;
            if checkpoint.active_offset > *active_length {
                return Err(EngineError::Corruption(
                    "checkpoint exceeds active segment length".into(),
                ));
            }
            for location in checkpoint.index.values() {
                let length = segment_lengths
                    .get(&location.segment_id)
                    .ok_or(EngineError::SegmentNotFound(location.segment_id))?;
                if location
                    .value_offset
                    .checked_add(location.value_len as u64)
                    .is_none_or(|end| end > *length)
                {
                    return Err(EngineError::Corruption(
                        "checkpoint value exceeds segment length".into(),
                    ));
                }
            }
        }
        let checkpoint_segment_id = checkpoint
            .as_ref()
            .map_or(0, |state| state.active_segment_id);
        let checkpoint_offset = checkpoint.as_ref().map_or(0, |state| state.active_offset);
        let mut index = checkpoint.map_or_else(BTreeMap::new, |state| state.index);
        let (active_segment_id, active_len) = if segment_ids.is_empty() {
            let file = open_active_segment(&options.segment_path(1))?;
            file.sync_all()?;
            checkpoint::sync_directory(&options.segments_dir())?;
            (1, 0)
        } else {
            let active_segment_id = *segment_ids.last().expect("segment list is not empty");
            let mut active_len = 0;
            for segment_id in segment_ids {
                if checkpoint_segment_id > 0 && segment_id < checkpoint_segment_id {
                    continue;
                }
                let offset = if segment_id == checkpoint_segment_id {
                    checkpoint_offset
                } else {
                    0
                };
                let path = options.segment_path(segment_id);
                let summary = scan_segment(&path, segment_id, offset, &mut index)?;
                if summary.truncated_tail && segment_id != active_segment_id {
                    return Err(EngineError::Corruption(format!(
                        "truncated sealed segment {segment_id}"
                    )));
                }
                if segment_id == active_segment_id {
                    if summary.truncated_tail {
                        truncate_segment(&path, summary.valid_end)?;
                    }
                    active_len = summary.valid_end;
                }
            }
            (active_segment_id, active_len)
        };
        let active_file = open_active_segment(&options.segment_path(active_segment_id))?;
        Ok(Self {
            options,
            writer: Mutex::new(WriterState {
                active_segment_id,
                active_len,
                active_file,
                ops_since_checkpoint: 0,
                write_error: None,
                #[cfg(test)]
                fail_next_write_after: None,
                #[cfg(test)]
                sync_pause: None,
            }),
            index: RwLock::new(index),
            reader_cache: Arc::new(Mutex::new(ReaderCache::new())),
            snapshot_registry: Arc::new(SnapshotRegistry::new()),
            directory_lock,
        })
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        validate_lengths(key.len(), value.len())?;
        let record = Record::put(key, value);
        let mut state = self.writer.lock().expect("writer lock poisoned");
        let location = self.append_record_locked(&mut state, &record, true)?;
        self.index
            .write()
            .expect("index lock poisoned")
            .insert(key.to_vec(), location);
        state.ops_since_checkpoint += 1;
        Ok(())
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        validate_lengths(key.len(), 0)?;
        let record = Record::delete(key);
        let mut state = self.writer.lock().expect("writer lock poisoned");
        self.append_record_locked(&mut state, &record, true)?;
        self.index.write().expect("index lock poisoned").remove(key);
        state.ops_since_checkpoint += 1;
        Ok(())
    }

    pub fn write_batch(&self, operations: &[WriteOp]) -> Result<()> {
        if operations.is_empty() {
            return Ok(());
        }
        let record = Record::batch(operations)?;
        // Parse and validate the complete batch before any disk or index mutation.
        let mut state = self.writer.lock().expect("writer lock poisoned");
        let location = self.append_record_locked(&mut state, &record, true)?;
        let mut offset = location.value_offset + 4;
        // Publish every operation under one lock after the commit's sync policy succeeds.
        let mut index = self.index.write().expect("index lock poisoned");
        for operation in operations {
            match operation {
                WriteOp::Put(key, value) => {
                    index.insert(
                        key.clone(),
                        ValueLocation {
                            segment_id: location.segment_id,
                            record_offset: offset,
                            value_offset: offset + RECORD_HEADER_LEN as u64 + key.len() as u64,
                            value_len: value.len() as u32,
                        },
                    );
                    offset += (RECORD_HEADER_LEN + key.len() + value.len()) as u64;
                }
                WriteOp::Delete(key) => {
                    index.remove(key.as_slice());
                    offset += (RECORD_HEADER_LEN + key.len()) as u64;
                }
            }
        }
        state.ops_since_checkpoint += operations.len() as u64;
        Ok(())
    }

    pub fn clear(&self) -> Result<()> {
        let mut state = self.writer.lock().expect("writer lock poisoned");
        self.append_record_locked(&mut state, &Record::clear(), true)?;
        let previous = std::mem::take(&mut *self.index.write().expect("index lock poisoned"));
        drop(previous);
        state.ops_since_checkpoint += 1;
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let index = self.index.read().expect("index lock poisoned");
        let Some(location) = index.get(key).cloned() else {
            return Ok(None);
        };
        // Acquire an OS handle while the index read lock still protects reclamation.
        let path = self.options.segment_path(location.segment_id);
        let file = self
            .reader_cache
            .lock()
            .expect("reader cache lock poisoned")
            .open_clone(location.segment_id, &path)?;
        drop(index);
        Ok(Some(read_value(&file, &location, key)?))
    }

    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.index
            .read()
            .expect("index lock poisoned")
            .contains_key(key)
    }

    pub fn len(&self) -> usize {
        self.index.read().expect("index lock poisoned").len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn sync(&self) -> Result<()> {
        Self::sync_active_file(&mut self.writer.lock().expect("writer lock poisoned"))?;
        Ok(())
    }

    pub fn checkpoint(&self) -> Result<()> {
        self.write_checkpoint_locked(&mut self.writer.lock().expect("writer lock poisoned"))
    }

    /// The host calls this from its maintenance scheduler.
    pub fn maybe_checkpoint(&self) -> Result<()> {
        let mut state = self.writer.lock().expect("writer lock poisoned");
        if state.ops_since_checkpoint >= self.options.checkpoint_after_ops {
            self.write_checkpoint_locked(&mut state)?;
        }
        Ok(())
    }

    pub fn should_checkpoint(&self) -> bool {
        self.writer
            .lock()
            .expect("writer lock poisoned")
            .ops_since_checkpoint
            >= self.options.checkpoint_after_ops
    }

    pub fn snapshot(&self) -> EngineSnapshot {
        let index = self.index.read().expect("index lock poisoned");
        let entries = index
            .iter()
            .map(|(key, location)| (key.clone(), location.clone()))
            .collect();
        EngineSnapshot::new(
            &self.options,
            Arc::clone(&self.reader_cache),
            Arc::clone(&self.snapshot_registry),
            Arc::clone(&self.directory_lock),
            entries,
        )
    }

    pub fn stats(&self) -> EngineStats {
        let state = self.writer.lock().expect("writer lock poisoned");
        let keys = self.index.read().expect("index lock poisoned").len();
        let segment_count =
            list_segment_ids(&self.options.segments_dir()).map_or(0, |ids| ids.len());
        let cached_readers = self
            .reader_cache
            .lock()
            .expect("reader cache lock poisoned")
            .len();
        let (pinned_segments, pending_reclaims) = self.snapshot_registry.stats();
        EngineStats {
            keys,
            active_segment_id: state.active_segment_id,
            segment_count,
            cached_readers,
            pinned_segments,
            pending_reclaims,
        }
    }

    pub fn compact_once(&self) -> Result<usize> {
        let mut state = self.writer.lock().expect("writer lock poisoned");
        self.compact_once_locked(&mut state)
    }

    /// Reclaim sealed segments when obsolete bytes occupy at least half their size.
    pub fn maybe_compact(&self) -> Result<usize> {
        let mut state = self.writer.lock().expect("writer lock poisoned");
        let mut sealed_bytes = 0_u128;
        for segment_id in list_segment_ids(&self.options.segments_dir())? {
            if segment_id < state.active_segment_id
                && !self.snapshot_registry.is_pending_delete(segment_id)
            {
                sealed_bytes += fs::metadata(self.options.segment_path(segment_id))?.len() as u128;
            }
        }
        let live_bytes: u128 = self
            .index
            .read()
            .expect("index lock poisoned")
            .iter()
            .filter(|(_, location)| location.segment_id < state.active_segment_id)
            .map(|(key, location)| {
                RECORD_HEADER_LEN as u128 + key.len() as u128 + location.value_len as u128
            })
            .sum();
        let obsolete_bytes = sealed_bytes.saturating_sub(live_bytes);
        if obsolete_bytes == 0 || obsolete_bytes * 2 < sealed_bytes {
            return Ok(0);
        }
        self.compact_once_locked(&mut state)
    }

    fn compact_once_locked(&self, state: &mut WriterState) -> Result<usize> {
        let active_id = state.active_segment_id;
        let old_segments: Vec<u64> = list_segment_ids(&self.options.segments_dir())?
            .into_iter()
            .filter(|id| *id < active_id)
            .collect();
        if old_segments.is_empty() {
            return Ok(0);
        }
        let live: Vec<_> = self
            .index
            .read()
            .expect("index lock poisoned")
            .iter()
            .filter(|(_, location)| location.segment_id < active_id)
            .map(|(key, location)| (key.clone(), location.clone()))
            .collect();
        let mut rewritten = 0;
        // The live index avoids scanning obsolete values and batch envelopes.
        for (key, location) in live {
            let file = self
                .reader_cache
                .lock()
                .expect("reader cache lock poisoned")
                .open_clone(
                    location.segment_id,
                    &self.options.segment_path(location.segment_id),
                )?;
            let value = read_value(&file, &location, &key)?;
            let record = Record::put(&key, &value);
            let new_location = self.append_record_locked(state, &record, false)?;
            self.index
                .write()
                .expect("index lock poisoned")
                .insert(key, new_location);
            rewritten += 1;
        }
        state.ops_since_checkpoint += rewritten as u64;
        // Even an all-obsolete segment can contain tombstones needed by the previous checkpoint.
        self.write_checkpoint_locked(state)?;
        for segment_id in old_segments {
            if self.snapshot_registry.schedule_delete(segment_id) {
                self.delete_segment_file(segment_id)?;
            }
        }
        checkpoint::sync_directory(&self.options.segments_dir())?;
        Ok(rewritten)
    }

    pub fn compact_until_stable(&self, max_rounds: usize) -> Result<usize> {
        let mut total_rewritten = 0;
        for _ in 0..max_rounds {
            let before = list_segment_ids(&self.options.segments_dir())?.len();
            if before <= 1 {
                break;
            }
            let rewritten = self.compact_once()?;
            total_rewritten += rewritten;
            let after = list_segment_ids(&self.options.segments_dir())?.len();
            // Live data exceeding one segment must not be rewritten indefinitely.
            if rewritten == 0 || after >= before {
                break;
            }
        }
        Ok(total_rewritten)
    }

    pub fn close(&self) -> Result<()> {
        let mut state = self.writer.lock().expect("writer lock poisoned");
        self.write_checkpoint_locked(&mut state)?;
        self.reader_cache
            .lock()
            .expect("reader cache lock poisoned")
            .clear();
        Ok(())
    }

    pub fn iter(&self) -> EngineIterator {
        self.snapshot().iter()
    }

    fn append_record_locked(
        &self,
        state: &mut WriterState,
        record: &Record,
        sync: bool,
    ) -> Result<ValueLocation> {
        record.validate()?;
        if let Some(error) = &state.write_error {
            return Err(EngineError::Corruption(format!(
                "engine requires reopen after write failure: {error}"
            )));
        }
        let record_len = record.encoded_len() as u64;
        if state.active_len > 0
            && state
                .active_len
                .checked_add(record_len)
                .is_none_or(|length| length > self.options.segment_size_bytes)
        {
            self.rotate_segment_locked(state)?;
        }
        let record_offset = state.active_len;
        let encoded = record.encode();
        let result = Self::write_encoded(state, &encoded).and_then(|()| {
            if sync {
                Self::apply_sync_policy(state, self.options.sync_policy)
            } else {
                Ok(())
            }
        });
        if let Err(error) = result {
            // Prevent a later successful commit from being appended after a torn record.
            // Windows append-only handles cannot truncate. Use a separate writable handle.
            let rollback = truncate_segment(
                &self.options.segment_path(state.active_segment_id),
                record_offset,
            );
            if let Err(rollback_error) = rollback {
                state.write_error = Some(format!("{error}; rollback failed: {rollback_error}"));
            }
            return Err(error.into());
        }
        state.active_len += encoded.len() as u64;
        Ok(ValueLocation {
            segment_id: state.active_segment_id,
            record_offset,
            value_offset: record_offset + RECORD_HEADER_LEN as u64 + record.key.len() as u64,
            value_len: record.value.len() as u32,
        })
    }

    fn rotate_segment_locked(&self, state: &mut WriterState) -> Result<()> {
        Self::sync_active_file(state)?;
        let next_id = state
            .active_segment_id
            .checked_add(1)
            .ok_or_else(|| EngineError::InvalidInput("segment identifier overflow".into()))?;
        let file = open_active_segment(&self.options.segment_path(next_id))?;
        file.sync_all()?;
        checkpoint::sync_directory(&self.options.segments_dir())?;
        state.active_file = file;
        state.active_segment_id = next_id;
        state.active_len = 0;
        Ok(())
    }

    fn write_encoded(state: &mut WriterState, encoded: &[u8]) -> std::io::Result<()> {
        #[cfg(test)]
        if let Some(length) = state.fail_next_write_after.take() {
            state
                .active_file
                .write_all(&encoded[..length.min(encoded.len())])?;
            return Err(std::io::Error::other("injected write failure"));
        }
        state.active_file.write_all(encoded)
    }

    fn write_checkpoint_locked(&self, state: &mut WriterState) -> Result<()> {
        if let Some(error) = &state.write_error {
            return Err(EngineError::Corruption(format!(
                "checkpoint blocked after write failure: {error}"
            )));
        }
        Self::sync_active_file(state)?;
        // The writer mutex freezes this view. GETs can share the read lock during I/O.
        let index = self.index.read().expect("index lock poisoned");
        checkpoint::write_index(
            &self.options.checkpoint_path(),
            state.active_segment_id,
            state.active_len,
            &index,
        )?;
        state.ops_since_checkpoint = 0;
        Ok(())
    }

    fn apply_sync_policy(state: &mut WriterState, policy: SyncPolicy) -> std::io::Result<()> {
        match policy {
            SyncPolicy::None => Ok(()),
            SyncPolicy::Flush => state.active_file.flush(),
            SyncPolicy::SyncData => Self::sync_active_file(state),
        }
    }

    fn sync_active_file(state: &mut WriterState) -> std::io::Result<()> {
        #[cfg(test)]
        if let Some(pause) = state.sync_pause.take() {
            pause.entered.send(()).expect("sync observer dropped");
            let (released, changed) = &*pause.release;
            let released = released.lock().expect("sync pause lock poisoned");
            drop(
                changed
                    .wait_while(released, |released| !*released)
                    .expect("sync pause lock poisoned"),
            );
            if pause.fail {
                return Err(std::io::Error::other("injected synchronization failure"));
            }
        }
        state.active_file.sync_all()
    }

    fn delete_segment_file(&self, segment_id: u64) -> Result<()> {
        self.reader_cache
            .lock()
            .expect("reader cache lock poisoned")
            .remove(segment_id);
        match fs::remove_file(self.options.segment_path(segment_id)) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err.into()),
        }
    }
}
#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::mpsc;
    use std::thread;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use super::*;

    fn temp_dir(name: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time went backwards")
            .as_nanos();
        std::env::temp_dir().join(format!("kerniva_kerniva_engine_{name}_{unique}"))
    }

    struct PausedSync {
        entered: mpsc::Receiver<()>,
        release: Arc<(Mutex<bool>, std::sync::Condvar)>,
    }

    impl PausedSync {
        fn install(engine: &Engine, fail: bool) -> Self {
            let (entered_tx, entered) = mpsc::channel();
            let release = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
            engine.writer.lock().unwrap().sync_pause = Some(SyncPause {
                entered: entered_tx,
                release: Arc::clone(&release),
                fail,
            });
            Self { entered, release }
        }

        fn wait(&self) {
            self.entered.recv_timeout(Duration::from_secs(5)).unwrap();
        }

        fn resume(&self) {
            let (released, changed) = &*self.release;
            *released.lock().unwrap() = true;
            changed.notify_all();
        }
    }

    impl Drop for PausedSync {
        fn drop(&mut self) {
            self.resume();
        }
    }

    #[test]
    fn fsync_does_not_block_cold_get_of_committed_values() {
        let dir = temp_dir("fsync_read");
        let mut options = Options::new(&dir);
        options.sync_policy = SyncPolicy::SyncData;
        let engine = Arc::new(Engine::open(options).unwrap());
        engine.put(b"previous", b"safe").unwrap();
        let pause = PausedSync::install(&engine, false);
        let writer_engine = Arc::clone(&engine);
        let writer = thread::spawn(move || writer_engine.put(b"new", b"pending"));
        pause.wait();
        assert!(engine.writer.try_lock().is_err());
        assert!(engine.index.try_read().is_ok());
        let (read_tx, read_rx) = mpsc::channel();
        let reader_engine = Arc::clone(&engine);
        let reader = thread::spawn(move || {
            read_tx.send(reader_engine.get(b"previous")).unwrap();
        });
        let before_release = read_rx.recv_timeout(Duration::from_secs(2));
        pause.resume();
        writer.join().unwrap().unwrap();
        reader.join().unwrap();
        assert_eq!(before_release.unwrap().unwrap(), Some(b"safe".to_vec()));
        println!("after: a cold GET completed while the writer remained paused before fsync");
        drop(engine);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn synchronous_mutations_keep_the_previous_view_until_publication() {
        for mutation in ["put", "delete", "batch", "clear"] {
            for fail_sync in [false, true] {
                let dir = temp_dir("sync_publication");
                let mut options = Options::new(&dir);
                options.sync_policy = SyncPolicy::SyncData;
                let engine = Arc::new(Engine::open(options.clone()).unwrap());
                engine.put(b"left", b"old-left").unwrap();
                engine.put(b"right", b"old-right").unwrap();
                let path = options.segment_path(1);
                let previous_bytes = fs::read(&path).unwrap();
                let pause = PausedSync::install(&engine, fail_sync);
                let writer_engine = Arc::clone(&engine);
                let writer = thread::spawn(move || match mutation {
                    "put" => writer_engine.put(b"left", b"new-left"),
                    "delete" => writer_engine.delete(b"left"),
                    "batch" => writer_engine.write_batch(&[
                        WriteOp::put("left", "new-left"),
                        WriteOp::delete("right"),
                        WriteOp::put("new", "new-value"),
                    ]),
                    "clear" => writer_engine.clear(),
                    _ => unreachable!(),
                });
                pause.wait();
                let (read_tx, read_rx) = mpsc::channel();
                let reader_engine = Arc::clone(&engine);
                let reader = thread::spawn(move || {
                    let snapshot = reader_engine.snapshot();
                    let values = [b"left".as_slice(), b"right", b"new"]
                        .map(|key| reader_engine.get(key).unwrap());
                    read_tx
                        .send((snapshot, values, reader_engine.len()))
                        .unwrap();
                });
                let before_release = read_rx.recv_timeout(Duration::from_secs(2));
                pause.resume();
                let committed = writer.join().unwrap();
                reader.join().unwrap();
                let (snapshot, values, length) = before_release.unwrap();
                let original = [
                    Some(b"old-left".to_vec()),
                    Some(b"old-right".to_vec()),
                    None,
                ];
                assert_eq!(values, original, "{mutation}, fail={fail_sync}");
                assert_eq!(length, 2);
                assert_eq!(snapshot.get(b"left").unwrap(), original[0]);
                assert_eq!(snapshot.get(b"right").unwrap(), original[1]);
                assert_eq!(snapshot.get(b"new").unwrap(), None);
                let expected = if fail_sync {
                    assert!(committed.is_err());
                    assert_eq!(fs::read(&path).unwrap(), previous_bytes);
                    original
                } else {
                    committed.unwrap();
                    match mutation {
                        "put" => [
                            Some(b"new-left".to_vec()),
                            Some(b"old-right".to_vec()),
                            None,
                        ],
                        "delete" => [None, Some(b"old-right".to_vec()), None],
                        "batch" => [
                            Some(b"new-left".to_vec()),
                            None,
                            Some(b"new-value".to_vec()),
                        ],
                        "clear" => [None, None, None],
                        _ => unreachable!(),
                    }
                };
                for (key, value) in [b"left".as_slice(), b"right", b"new"]
                    .into_iter()
                    .zip(&expected)
                {
                    assert_eq!(engine.get(key).unwrap().as_ref(), value.as_ref());
                    assert_eq!(engine.contains_key(key), value.is_some());
                }
                // A successful rollback permits subsequent commits and checkpoints.
                engine.put(b"after", b"safe").unwrap();
                engine.checkpoint().unwrap();
                drop(snapshot);
                drop(engine);
                let reopened = Engine::open(options).unwrap();
                for (key, value) in [b"left".as_slice(), b"right", b"new"]
                    .into_iter()
                    .zip(expected)
                {
                    assert_eq!(reopened.get(key).unwrap(), value);
                }
                assert_eq!(reopened.get(b"after").unwrap(), Some(b"safe".to_vec()));
                drop(reopened);
                fs::remove_dir_all(dir).unwrap();
            }
        }
    }

    #[test]
    fn synchronization_and_maintenance_leave_committed_reads_available() {
        for operation in ["sync", "checkpoint", "close", "compact", "rotation"] {
            let dir = temp_dir("maintenance_reads");
            let mut options = Options::new(&dir);
            options.segment_size_bytes = 4096;
            options.sync_policy = SyncPolicy::SyncData;
            let engine = Arc::new(Engine::open(options.clone()).unwrap());
            engine.put(b"alpha", &[1; 512]).unwrap();
            engine.put(b"obsolete", &[2; 3072]).unwrap();
            engine.put(b"obsolete", &[3; 3072]).unwrap();
            let snapshot = engine.snapshot();
            let pause = PausedSync::install(&engine, false);
            let writer_engine = Arc::clone(&engine);
            let writer = thread::spawn(move || match operation {
                "sync" => writer_engine.sync(),
                "checkpoint" => writer_engine.checkpoint(),
                "close" => writer_engine.close(),
                "compact" => writer_engine.compact_once().map(|_| ()),
                "rotation" => writer_engine.put(b"rotation", &[4; 2048]),
                _ => unreachable!(),
            });
            pause.wait();
            let (read_tx, read_rx) = mpsc::channel();
            let reader_engine = Arc::clone(&engine);
            let reader = thread::spawn(move || {
                let view = reader_engine.snapshot();
                read_tx
                    .send((reader_engine.get(b"alpha"), view.get(b"obsolete")))
                    .unwrap();
            });
            let before_release = read_rx.recv_timeout(Duration::from_secs(2));
            let source_still_present = options.segment_path(1).exists();
            pause.resume();
            writer.join().unwrap().unwrap();
            reader.join().unwrap();
            let (alpha, obsolete) = before_release.unwrap();
            assert_eq!(alpha.unwrap(), Some(vec![1; 512]), "{operation}");
            assert_eq!(obsolete.unwrap(), Some(vec![3; 3072]), "{operation}");
            assert!(source_still_present);
            assert_eq!(snapshot.get(b"alpha").unwrap(), Some(vec![1; 512]));
            drop(snapshot);
            if operation == "compact" {
                assert!(!options.segment_path(1).exists());
            }
            drop(engine);
            let reopened = Engine::open(options).unwrap();
            assert_eq!(reopened.get(b"alpha").unwrap(), Some(vec![1; 512]));
            assert_eq!(reopened.get(b"obsolete").unwrap(), Some(vec![3; 3072]));
            drop(reopened);
            fs::remove_dir_all(dir).unwrap();
        }
    }

    #[test]
    fn failed_sync_with_failed_rollback_poisons_only_further_mutations() {
        let dir = temp_dir("sync_poison");
        let mut options = Options::new(&dir);
        options.sync_policy = SyncPolicy::SyncData;
        let engine = Arc::new(Engine::open(options.clone()).unwrap());
        engine.put(b"previous", b"safe").unwrap();
        assert_eq!(engine.get(b"previous").unwrap(), Some(b"safe".to_vec()));
        let pause = PausedSync::install(&engine, true);
        let writer_engine = Arc::clone(&engine);
        let writer = thread::spawn(move || writer_engine.put(b"new", b"unconfirmed"));
        pause.wait();
        let path = options.segment_path(1);
        let moved = path.with_extension("saved");
        // The still-open file remains readable, while rollback cannot reopen its old path.
        fs::rename(&path, &moved).unwrap();
        pause.resume();
        assert!(writer.join().unwrap().is_err());
        fs::rename(&moved, &path).unwrap();
        assert_eq!(engine.get(b"previous").unwrap(), Some(b"safe".to_vec()));
        assert_eq!(engine.get(b"new").unwrap(), None);
        assert!(engine.put(b"next", b"rejected").is_err());
        assert!(engine.clear().is_err());
        assert!(engine.checkpoint().is_err());
        assert!(engine.writer.lock().unwrap().write_error.is_some());
        drop(engine);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn failed_batch_write_rolls_back_bytes_and_keeps_previous_index() {
        for failed_after in [25, usize::MAX] {
            let dir = temp_dir("batch_failure");
            let engine = Engine::open(Options::new(&dir)).unwrap();
            engine.put(b"previous", b"safe").unwrap();
            let path = dir.join("segments").join("0000000001.seg");
            let previous_length = fs::metadata(&path).unwrap().len();
            engine.writer.lock().unwrap().fail_next_write_after = Some(failed_after);
            let result = engine.write_batch(&[
                WriteOp::delete("previous"),
                WriteOp::put("first", "one"),
                WriteOp::put("second", "two"),
            ]);
            assert!(result.is_err());
            assert_eq!(fs::metadata(&path).unwrap().len(), previous_length);
            assert_eq!(engine.get(b"previous").unwrap(), Some(b"safe".to_vec()));
            assert_eq!(engine.get(b"first").unwrap(), None);
            assert_eq!(engine.get(b"second").unwrap(), None);
            engine.put(b"next", b"successful").unwrap();
            drop(engine);
            let reopened = Engine::open(Options::new(&dir)).unwrap();
            assert_eq!(reopened.get(b"previous").unwrap(), Some(b"safe".to_vec()));
            assert_eq!(reopened.get(b"next").unwrap(), Some(b"successful".to_vec()));
            drop(reopened);
            fs::remove_dir_all(dir).unwrap();
        }
    }

    #[test]
    fn failed_clear_rolls_back_marker_and_keeps_previous_index() {
        for failed_after in [7, usize::MAX] {
            let dir = temp_dir("clear_failure");
            let engine = Engine::open(Options::new(&dir)).unwrap();
            engine.put(b"previous", b"safe").unwrap();
            let path = dir.join("segments").join("0000000001.seg");
            let before = fs::read(&path).unwrap();
            engine.writer.lock().unwrap().fail_next_write_after = Some(failed_after);
            assert!(engine.clear().is_err());
            assert_eq!(fs::read(&path).unwrap(), before);
            assert_eq!(engine.get(b"previous").unwrap(), Some(b"safe".to_vec()));
            engine.put(b"next", b"successful").unwrap();
            drop(engine);
            let reopened = Engine::open(Options::new(&dir)).unwrap();
            assert_eq!(reopened.get(b"previous").unwrap(), Some(b"safe".to_vec()));
            assert_eq!(reopened.get(b"next").unwrap(), Some(b"successful".to_vec()));
            drop(reopened);
            fs::remove_dir_all(dir).unwrap();
        }
    }

    #[test]
    fn put_get_delete_roundtrip() {
        let dir = temp_dir("roundtrip");
        let options = Options::new(&dir);
        let engine = Engine::open(options).expect("open engine");

        engine.put(b"alpha", b"one").expect("put alpha");
        engine.put(b"beta", b"two").expect("put beta");

        assert_eq!(
            engine.get(b"alpha").expect("get alpha"),
            Some(b"one".to_vec())
        );
        assert_eq!(
            engine.get(b"beta").expect("get beta"),
            Some(b"two".to_vec())
        );

        engine.delete(b"alpha").expect("delete alpha");
        assert_eq!(engine.get(b"alpha").expect("get deleted"), None);

        std::fs::remove_dir_all(dir).expect("cleanup temp dir");
    }

    #[test]
    fn recovers_after_reopen() {
        let dir = temp_dir("reopen");
        {
            let mut options = Options::new(&dir);
            options.checkpoint_after_ops = 2;
            let engine = Engine::open(options).expect("open engine");
            engine.put(b"k1", b"v1").expect("put k1");
            engine.put(b"k2", b"v2").expect("put k2");
            engine.put(b"k3", b"v3").expect("put k3");
            engine.checkpoint().expect("checkpoint");
        }

        let engine = Engine::open(Options::new(&dir)).expect("reopen engine");
        assert_eq!(engine.get(b"k1").expect("get k1"), Some(b"v1".to_vec()));
        assert_eq!(engine.get(b"k2").expect("get k2"), Some(b"v2".to_vec()));
        assert_eq!(engine.get(b"k3").expect("get k3"), Some(b"v3".to_vec()));

        std::fs::remove_dir_all(dir).expect("cleanup temp dir");
    }

    #[test]
    fn rotates_segments_and_keeps_latest_value() {
        let dir = temp_dir("rotate");
        let mut options = Options::new(&dir);
        options.segment_size_bytes = 4 * 1024;
        options.checkpoint_after_ops = 10;
        let engine = Engine::open(options).expect("open engine");

        for idx in 0..20 {
            let key = format!("k{:02}", idx % 4);
            let value = format!("value-{idx:02}-{}", "x".repeat(512));
            engine.put(key.as_bytes(), value.as_bytes()).expect("put");
        }

        let latest = engine
            .get(b"k00")
            .expect("get latest")
            .expect("value should exist");
        assert!(latest.starts_with(b"value-16-"));

        let segment_count = list_segment_ids(&dir.join("segments"))
            .expect("list segments")
            .len();
        assert!(segment_count > 1, "expected rotated segments");

        std::fs::remove_dir_all(dir).expect("cleanup temp dir");
    }

    #[test]
    fn truncates_partial_tail_on_recovery() {
        let dir = temp_dir("truncate_tail");
        {
            let engine = Engine::open(Options::new(&dir)).expect("open engine");
            engine.put(b"good", b"value").expect("put good");
            engine.close().expect("close engine");
        }

        let active_path = dir.join("segments").join("0000000001.seg");
        {
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .open(&active_path)
                .expect("open segment");
            file.write_all(b"RDG2junk").expect("write partial garbage");
            file.flush().expect("flush partial");
        }

        let engine = Engine::open(Options::new(&dir)).expect("reopen after tail corruption");
        assert_eq!(
            engine.get(b"good").expect("read recovered value"),
            Some(b"value".to_vec())
        );

        std::fs::remove_dir_all(dir).expect("cleanup temp dir");
    }

    #[test]
    fn write_batch_updates_index_atomically_in_memory() {
        let dir = temp_dir("batch");
        let engine = Engine::open(Options::new(&dir)).expect("open engine");

        let batch = vec![
            WriteOp::put("alpha", "one"),
            WriteOp::put("beta", "two"),
            WriteOp::put("alpha", "three"),
            WriteOp::delete("beta"),
        ];
        engine.write_batch(&batch).expect("write batch");

        assert_eq!(
            engine.get(b"alpha").expect("get alpha"),
            Some(b"three".to_vec())
        );
        assert_eq!(engine.get(b"beta").expect("get beta"), None);
        assert_eq!(engine.len(), 1);

        std::fs::remove_dir_all(dir).expect("cleanup temp dir");
    }

    #[test]
    fn compact_once_rewrites_live_entries_and_reclaims_old_segments() {
        let dir = temp_dir("compact_once");
        let mut options = Options::new(&dir);
        options.segment_size_bytes = 4 * 1024;
        options.checkpoint_after_ops = 100;
        let engine = Engine::open(options).expect("open engine");

        for idx in 0..12 {
            let key = format!("key-{idx:02}");
            let value = format!("value-{idx:02}-{}", "x".repeat(512));
            engine.put(key.as_bytes(), value.as_bytes()).expect("put");
        }
        let before_segments = list_segment_ids(&dir.join("segments")).expect("list segments");
        let original_active = *before_segments
            .last()
            .expect("segments should not be empty");
        assert!(
            before_segments.len() > 1,
            "expected multiple segments before compaction"
        );

        let rewritten = engine.compact_once().expect("compact once");
        assert!(rewritten > 0, "expected cleaner to rewrite live entries");

        for idx in 0..12 {
            let key = format!("key-{idx:02}");
            let expected = format!("value-{idx:02}-{}", "x".repeat(512)).into_bytes();
            assert_eq!(
                engine.get(key.as_bytes()).expect("get after compaction"),
                Some(expected)
            );
        }

        let after_segments = list_segment_ids(&dir.join("segments")).expect("list segments after");
        assert!(
            after_segments
                .iter()
                .all(|segment_id| *segment_id >= original_active),
            "expected all pre-active segments to be reclaimed"
        );
        assert!(
            !dir.join("segments").join("0000000001.seg").exists(),
            "expected oldest segment to be reclaimed"
        );

        std::fs::remove_dir_all(dir).expect("cleanup temp dir");
    }

    #[test]
    fn reader_cache_reuses_opened_segment_and_evicts_reclaimed_files() {
        let dir = temp_dir("reader_cache");
        let mut options = Options::new(&dir);
        options.segment_size_bytes = 4 * 1024;
        options.checkpoint_after_ops = 100;
        let engine = Engine::open(options).expect("open engine");

        for idx in 0..8 {
            let key = format!("key-{idx:02}");
            let value = format!("value-{idx:02}-{}", "x".repeat(512));
            engine.put(key.as_bytes(), value.as_bytes()).expect("put");
        }

        assert_eq!(
            engine.get(b"key-00").expect("first read"),
            Some(format!("value-00-{}", "x".repeat(512)).into_bytes())
        );
        assert_eq!(
            engine
                .reader_cache
                .lock()
                .expect("reader cache lock poisoned")
                .len(),
            1
        );

        assert_eq!(
            engine.get(b"key-00").expect("second read"),
            Some(format!("value-00-{}", "x".repeat(512)).into_bytes())
        );
        assert_eq!(
            engine
                .reader_cache
                .lock()
                .expect("reader cache lock poisoned")
                .len(),
            1
        );

        engine.compact_once().expect("compact once");
        assert_eq!(
            engine
                .reader_cache
                .lock()
                .expect("reader cache lock poisoned")
                .len(),
            0
        );

        std::fs::remove_dir_all(dir).expect("cleanup temp dir");
    }

    #[test]
    fn snapshot_preserves_old_view_until_release() {
        let dir = temp_dir("snapshot");
        let mut options = Options::new(&dir);
        options.segment_size_bytes = 4 * 1024;
        options.checkpoint_after_ops = 100;
        let engine = Engine::open(options).expect("open engine");

        for idx in 0..6 {
            let key = format!("filler-{idx:02}");
            let value = format!("fill-{idx:02}-{}", "x".repeat(512));
            engine
                .put(key.as_bytes(), value.as_bytes())
                .expect("put filler");
        }
        engine.put(b"alpha", b"old-value").expect("put old value");
        for idx in 0..6 {
            let key = format!("tail-{idx:02}");
            let value = format!("tail-{idx:02}-{}", "y".repeat(512));
            engine
                .put(key.as_bytes(), value.as_bytes())
                .expect("put tail");
        }

        let snapshot = engine.snapshot();
        assert_eq!(
            snapshot.get(b"alpha").expect("snapshot get old"),
            Some(b"old-value".to_vec())
        );

        engine.put(b"alpha", b"new-value").expect("put new value");
        assert_eq!(
            engine.get(b"alpha").expect("engine get new"),
            Some(b"new-value".to_vec())
        );

        let before = engine.stats();
        assert!(before.pinned_segments > 0, "expected pinned segments");

        let rewritten = engine.compact_once().expect("compact with snapshot");
        assert!(rewritten > 0, "expected live entries rewritten");
        assert_eq!(
            snapshot.get(b"alpha").expect("snapshot get preserved"),
            Some(b"old-value".to_vec())
        );

        let during = engine.stats();
        assert!(
            during.pending_reclaims > 0,
            "expected old segments to wait for snapshot release"
        );
        assert!(
            dir.join("segments").join("0000000001.seg").exists(),
            "expected old segment to stay while snapshot is alive"
        );

        drop(snapshot);

        let after = engine.stats();
        assert_eq!(after.pinned_segments, 0);
        assert_eq!(after.pending_reclaims, 0);
        assert!(
            !dir.join("segments").join("0000000001.seg").exists(),
            "expected delayed segment deletion after snapshot release"
        );

        std::fs::remove_dir_all(dir).expect("cleanup temp dir");
    }

    #[test]
    fn compact_until_stable_converges_to_single_segment() {
        let dir = temp_dir("compact_stable");
        let mut options = Options::new(&dir);
        options.segment_size_bytes = 4 * 1024;
        options.checkpoint_after_ops = 100;
        let engine = Engine::open(options).expect("open engine");

        for idx in 0..24 {
            let key = format!("key-{:02}", idx % 6);
            let value = format!("value-{idx:02}-{}", "z".repeat(512));
            engine.put(key.as_bytes(), value.as_bytes()).expect("put");
        }

        let before = engine.stats();
        assert!(
            before.segment_count > 1,
            "expected multiple segments before compaction"
        );

        let rewritten = engine
            .compact_until_stable(16)
            .expect("compact until stable");
        assert!(rewritten > 0, "expected some entries to be rewritten");

        let after = engine.stats();
        assert_eq!(
            after.segment_count, 1,
            "expected single segment after stabilization"
        );

        for idx in 0..6 {
            let key = format!("key-{:02}", idx);
            let expected = format!("value-{:02}-{}", 18 + idx, "z".repeat(512)).into_bytes();
            assert_eq!(
                engine
                    .get(key.as_bytes())
                    .expect("get after stable compaction"),
                Some(expected)
            );
        }

        std::fs::remove_dir_all(dir).expect("cleanup temp dir");
    }
}
