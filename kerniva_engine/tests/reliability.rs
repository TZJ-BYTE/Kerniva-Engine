use std::fs::{self, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use kerniva_engine::{
    DurableMemoryEngine, DurableMemoryError, Engine, EngineError, Options, SyncPolicy, WriteOp,
};

struct TempDir(PathBuf);

impl TempDir {
    fn new(name: &str) -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        Self(std::env::temp_dir().join(format!(
            "kerniva_reliability_{name}_{}_{nonce}",
            std::process::id()
        )))
    }

    fn path(&self) -> &Path {
        &self.0
    }
    fn options(&self) -> Options {
        Options::new(self.path())
    }
    fn segment(&self, id: u64) -> PathBuf {
        self.0.join("segments").join(format!("{id:010}.seg"))
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn maintenance_skips_sealed_segments_without_garbage() {
    let dir = TempDir::new("maintenance_live");
    let mut options = dir.options();
    options.segment_size_bytes = 4096;
    let engine = Engine::open(options).unwrap();
    for index in 0..8 {
        engine
            .put(format!("key-{index}").as_bytes(), &[42; 3072])
            .unwrap();
    }
    let before = engine.stats();
    let lengths: Vec<_> = (1..=before.active_segment_id)
        .map(|id| fs::metadata(dir.segment(id)).unwrap().len())
        .collect();
    for _ in 0..10 {
        assert_eq!(engine.maybe_compact().unwrap(), 0);
    }
    let after = engine.stats();
    assert_eq!(after.active_segment_id, before.active_segment_id);
    assert_eq!(after.segment_count, before.segment_count);
    for (index, length) in lengths.iter().enumerate() {
        assert_eq!(
            fs::metadata(dir.segment(index as u64 + 1)).unwrap().len(),
            *length
        );
    }
    assert!(!dir.path().join("meta").join("checkpoint.bin").exists());
}

#[test]
fn maintenance_reclaims_majority_garbage_and_skips_pending_pins() {
    let dir = TempDir::new("maintenance_garbage");
    let mut options = dir.options();
    options.segment_size_bytes = 4096;
    let engine = Engine::open(options.clone()).unwrap();
    for index in 0..4 {
        engine
            .put(format!("key-{index}").as_bytes(), &[1; 3072])
            .unwrap();
    }
    let snapshot = engine.snapshot();
    for index in 0..4 {
        engine
            .put(format!("key-{index}").as_bytes(), &[2; 3072])
            .unwrap();
    }
    assert_eq!(engine.maybe_compact().unwrap(), 3);
    let active_after = engine.stats().active_segment_id;
    for _ in 0..3 {
        assert_eq!(engine.maybe_compact().unwrap(), 0);
        assert_eq!(engine.stats().active_segment_id, active_after);
    }
    assert_eq!(snapshot.get(b"key-0").unwrap(), Some(vec![1; 3072]));
    drop(snapshot);
    assert_eq!(engine.stats().segment_count, 4);
    drop(engine);
    let reopened = Engine::open(options).unwrap();
    for index in 0..4 {
        assert_eq!(
            reopened.get(format!("key-{index}").as_bytes()).unwrap(),
            Some(vec![2; 3072])
        );
    }
}

#[test]
fn concurrent_reads_keep_values_independent() {
    let dir = TempDir::new("parallel_reads");
    let engine = Arc::new(Engine::open(dir.options()).unwrap());
    let values: Vec<Vec<u8>> = (0..16).map(|index| vec![index as u8 + 1; 65536]).collect();
    for (index, value) in values.iter().enumerate() {
        engine
            .put(format!("key-{index}").as_bytes(), value)
            .unwrap();
    }
    thread::scope(|scope| {
        for worker in 0..8 {
            let engine = Arc::clone(&engine);
            let values = &values;
            scope.spawn(move || {
                for iteration in 0..500 {
                    let index = (worker + iteration) % values.len();
                    assert_eq!(
                        engine
                            .get(format!("key-{index}").as_bytes())
                            .unwrap()
                            .as_ref(),
                        Some(&values[index])
                    );
                }
            });
        }
    });
}

#[test]
fn reads_survive_segment_reclamation() {
    let dir = TempDir::new("read_compact");
    let mut options = dir.options();
    options.segment_size_bytes = 65536;
    let engine = Arc::new(Engine::open(options).unwrap());
    let values: Vec<Vec<u8>> = (0..8).map(|index| vec![index as u8; 8192]).collect();
    for (index, value) in values.iter().enumerate() {
        engine
            .put(format!("stable-{index}").as_bytes(), value)
            .unwrap();
    }
    thread::scope(|scope| {
        for worker in 0..4 {
            let engine = Arc::clone(&engine);
            let values = &values;
            scope.spawn(move || {
                for iteration in 0..500 {
                    let index = (iteration + worker) % 8;
                    assert_eq!(
                        engine
                            .get(format!("stable-{index}").as_bytes())
                            .unwrap()
                            .as_ref(),
                        Some(&values[index])
                    );
                }
            });
        }
        scope.spawn(|| {
            for round in 0..8 {
                for index in 0..16 {
                    engine
                        .put(format!("changing-{index}").as_bytes(), &vec![round; 8192])
                        .unwrap();
                }
                engine.compact_once().unwrap();
            }
        });
    });
}

#[test]
fn concurrent_batches_clear_snapshots_and_maintenance_preserve_atomic_views() {
    let dir = TempDir::new("concurrent_commit_views");
    let mut options = dir.options();
    options.segment_size_bytes = 4096;
    options.sync_policy = SyncPolicy::SyncData;
    let engine = Arc::new(Engine::open(options.clone()).unwrap());
    engine
        .write_batch(&[
            WriteOp::put("left", vec![0; 512]),
            WriteOp::put("right", vec![0; 512]),
        ])
        .unwrap();
    thread::scope(|scope| {
        for _ in 0..4 {
            let engine = Arc::clone(&engine);
            scope.spawn(move || {
                for _ in 0..300 {
                    let snapshot = engine.snapshot();
                    let left = snapshot.get(b"left").unwrap();
                    let right = snapshot.get(b"right").unwrap();
                    assert_eq!(left, right);
                    assert_eq!(snapshot.len(), if left.is_some() { 2 } else { 0 });
                    if let Some(value) = engine.get(b"left").unwrap() {
                        assert_eq!(value.len(), 512);
                        assert!(value.iter().all(|byte| *byte == value[0]));
                    }
                }
            });
        }
        let writer = Arc::clone(&engine);
        scope.spawn(move || {
            for round in 0..64 {
                if round % 9 == 0 {
                    writer.clear().unwrap();
                }
                writer
                    .write_batch(&[
                        WriteOp::put("left", vec![round; 512]),
                        WriteOp::put("right", vec![round; 512]),
                    ])
                    .unwrap();
            }
        });
        let maintenance = Arc::clone(&engine);
        scope.spawn(move || {
            for _ in 0..24 {
                maintenance.compact_once().unwrap();
                maintenance.checkpoint().unwrap();
            }
        });
    });
    engine.close().unwrap();
    drop(engine);
    let reopened = Engine::open(options).unwrap();
    assert_eq!(reopened.len(), 2);
    assert_eq!(reopened.get(b"left").unwrap(), Some(vec![63; 512]));
    assert_eq!(reopened.get(b"right").unwrap(), Some(vec![63; 512]));
}

#[test]
fn zero_rewrite_compaction_preserves_deletion_on_recovery() {
    let dir = TempDir::new("obsolete_compact");
    let mut options = dir.options();
    options.segment_size_bytes = 4096;
    {
        let engine = Engine::open(options.clone()).unwrap();
        engine.put(b"ghost", b"old").unwrap();
        engine.checkpoint().unwrap();
        engine.delete(b"ghost").unwrap();
        engine.put(b"filler", &vec![7; 8192]).unwrap();
        assert_eq!(engine.compact_once().unwrap(), 0);
        // Dropping the engine intentionally bypasses the graceful checkpoint.
    }
    let engine = Engine::open(options).unwrap();
    assert_eq!(engine.get(b"ghost").unwrap(), None);
    assert_eq!(engine.get(b"filler").unwrap(), Some(vec![7; 8192]));
}

#[test]
fn directory_lock_is_exclusive_and_released_on_drop() {
    let dir = TempDir::new("exclusive");
    let engine = Engine::open(dir.options()).unwrap();
    assert!(matches!(
        Engine::open(dir.options()),
        Err(EngineError::Locked(_))
    ));
    drop(engine);
    Engine::open(dir.options()).unwrap();
}

#[test]
fn snapshot_and_iterator_keep_their_segments_and_directory_lock() {
    let dir = TempDir::new("iterator_lease");
    let mut options = dir.options();
    options.segment_size_bytes = 4096;
    let engine = Engine::open(options.clone()).unwrap();
    engine.put(b"alpha", b"old").unwrap();
    engine.put(b"filler", &vec![1; 8192]).unwrap();
    let snapshot = engine.snapshot();
    let mut iterator = snapshot.iter();
    drop(snapshot);
    engine.put(b"alpha", b"new").unwrap();
    engine.compact_once().unwrap();
    assert_eq!(
        iterator.next_entry().unwrap(),
        Some((b"alpha".to_vec(), b"old".to_vec()))
    );
    drop(engine);
    assert!(matches!(
        Engine::open(options.clone()),
        Err(EngineError::Locked(_))
    ));
    drop(iterator);
    let reopened = Engine::open(options).unwrap();
    assert_eq!(reopened.get(b"alpha").unwrap(), Some(b"new".to_vec()));
}

#[test]
fn clear_appends_one_fixed_size_marker_and_preserves_snapshot() {
    let dir = TempDir::new("clear_marker");
    let engine = Engine::open(dir.options()).unwrap();
    let operations: Vec<_> = (0..2000)
        .map(|index| WriteOp::put(format!("key-{index}"), vec![42; 1024]))
        .collect();
    engine.write_batch(&operations).unwrap();
    let snapshot = engine.snapshot();
    let before = fs::metadata(dir.segment(1)).unwrap().len();
    engine.clear().unwrap();
    assert_eq!(fs::metadata(dir.segment(1)).unwrap().len(), before + 20);
    assert!(engine.is_empty());
    assert_eq!(engine.get(b"key-0").unwrap(), None);
    assert_eq!(snapshot.get(b"key-0").unwrap(), Some(vec![42; 1024]));
    drop(snapshot);
    engine.put(b"after", b"clear").unwrap();
    drop(engine);
    let reopened = Engine::open(dir.options()).unwrap();
    assert_eq!(reopened.len(), 1);
    assert_eq!(reopened.get(b"after").unwrap(), Some(b"clear".to_vec()));
    assert_eq!(reopened.get(b"key-0").unwrap(), None);
}

#[test]
fn every_partial_clear_marker_recovers_with_legacy_checkpoint() {
    let original = TempDir::new("clear_source");
    let engine = Engine::open(original.options()).unwrap();
    engine.put(b"old", b"safe").unwrap();
    engine.put(b"deleted", b"unused").unwrap();
    engine.delete(b"deleted").unwrap();
    engine.checkpoint().unwrap();
    let mut checkpoint = fs::read(original.path().join("meta").join("checkpoint.bin")).unwrap();
    checkpoint[8..12].copy_from_slice(&1_u32.to_le_bytes());
    checkpoint.truncate(checkpoint.len() - 4);
    let base_length = fs::metadata(original.segment(1)).unwrap().len() as usize;
    engine.clear().unwrap();
    drop(engine);
    let segment = fs::read(original.segment(1)).unwrap();
    assert_eq!(segment.len() - base_length, 20);
    for tail in 0..=20 {
        let dir = TempDir::new("clear_cut");
        fs::create_dir_all(dir.path().join("segments")).unwrap();
        fs::create_dir_all(dir.path().join("meta")).unwrap();
        fs::write(dir.segment(1), &segment[..base_length + tail]).unwrap();
        fs::write(dir.path().join("meta").join("checkpoint.bin"), &checkpoint).unwrap();
        let engine = Engine::open(dir.options()).unwrap();
        assert_eq!(
            engine.get(b"old").unwrap(),
            (tail < 20).then(|| b"safe".to_vec()),
            "clear tail {tail}"
        );
        assert_eq!(engine.get(b"deleted").unwrap(), None);
        engine.put(b"after", b"recovery").unwrap();
        drop(engine);
        let reopened = Engine::open(dir.options()).unwrap();
        assert_eq!(reopened.get(b"after").unwrap(), Some(b"recovery".to_vec()));
        assert_eq!(
            reopened.get(b"old").unwrap(),
            (tail < 20).then(|| b"safe".to_vec())
        );
    }
}

#[test]
fn clear_remains_effective_after_zero_rewrite_compaction() {
    let dir = TempDir::new("clear_compact");
    let mut options = dir.options();
    options.segment_size_bytes = 4096;
    let engine = Engine::open(options.clone()).unwrap();
    engine.put(b"old", b"safe").unwrap();
    engine.checkpoint().unwrap();
    let snapshot = engine.snapshot();
    engine.clear().unwrap();
    engine.put(b"after", &[7; 8192]).unwrap();
    assert_eq!(engine.compact_once().unwrap(), 0);
    assert_eq!(snapshot.get(b"old").unwrap(), Some(b"safe".to_vec()));
    drop(snapshot);
    assert!(!dir.segment(1).exists());
    drop(engine);
    let reopened = Engine::open(options).unwrap();
    assert_eq!(reopened.get(b"old").unwrap(), None);
    assert_eq!(reopened.get(b"after").unwrap(), Some(vec![7; 8192]));
}

#[test]
fn corrupted_clear_marker_fails_without_truncating_committed_data() {
    let dir = TempDir::new("clear_corrupt");
    let engine = Engine::open(dir.options()).unwrap();
    engine.put(b"old", b"safe").unwrap();
    engine.clear().unwrap();
    drop(engine);
    let mut bytes = fs::read(dir.segment(1)).unwrap();
    let checksum_offset = bytes.len() - 4;
    bytes[checksum_offset] ^= 1;
    fs::write(dir.segment(1), &bytes).unwrap();
    assert!(matches!(
        Engine::open(dir.options()),
        Err(EngineError::Corruption(_))
    ));
    assert_eq!(fs::read(dir.segment(1)).unwrap(), bytes);
}

#[test]
fn every_partial_batch_tail_recovers_without_a_prefix_commit() {
    let original = TempDir::new("batch_source");
    let engine = Engine::open(original.options()).unwrap();
    engine.put(b"old", b"safe").unwrap();
    let base_length = fs::metadata(original.segment(1)).unwrap().len() as usize;
    engine
        .write_batch(&[
            WriteOp::put("first", "one"),
            WriteOp::delete("old"),
            WriteOp::put("second", "two"),
        ])
        .unwrap();
    drop(engine);
    let segment = fs::read(original.segment(1)).unwrap();
    for tail in 0..=segment.len() - base_length {
        let dir = TempDir::new("batch_cut");
        fs::create_dir_all(dir.path().join("segments")).unwrap();
        fs::write(dir.segment(1), &segment[..base_length + tail]).unwrap();
        let engine = Engine::open(dir.options()).unwrap();
        let complete = base_length + tail == segment.len();
        assert_eq!(
            engine.get(b"first").unwrap(),
            complete.then(|| b"one".to_vec()),
            "tail {tail}"
        );
        assert_eq!(
            engine.get(b"second").unwrap(),
            complete.then(|| b"two".to_vec()),
            "tail {tail}"
        );
        assert_eq!(
            engine.get(b"old").unwrap(),
            (!complete).then(|| b"safe".to_vec()),
            "tail {tail}"
        );
        engine.put(b"after", b"recovery").unwrap();
        drop(engine);
        let reopened = Engine::open(dir.options()).unwrap();
        assert_eq!(reopened.get(b"after").unwrap(), Some(b"recovery".to_vec()));
    }
}

#[test]
fn batches_recover_duplicate_keys_and_survive_compaction() {
    let dir = TempDir::new("batch_restart");
    let mut options = dir.options();
    options.segment_size_bytes = 4096;
    {
        let engine = Engine::open(options.clone()).unwrap();
        engine
            .write_batch(&[
                WriteOp::put("alpha", "one"),
                WriteOp::put("alpha", "two"),
                WriteOp::put("beta", "unused"),
                WriteOp::delete("beta"),
                WriteOp::put("gamma", "three"),
            ])
            .unwrap();
        engine.put(b"filler", &vec![1; 8192]).unwrap();
        engine.compact_once().unwrap();
    }
    let engine = Engine::open(options).unwrap();
    assert_eq!(engine.get(b"alpha").unwrap(), Some(b"two".to_vec()));
    assert_eq!(engine.get(b"beta").unwrap(), None);
    assert_eq!(engine.get(b"gamma").unwrap(), Some(b"three".to_vec()));
}

#[test]
fn legacy_checkpoint_version_one_remains_readable() {
    let dir = TempDir::new("checkpoint_v1");
    {
        let engine = Engine::open(dir.options()).unwrap();
        engine.put(b"legacy", b"kept").unwrap();
        engine.checkpoint().unwrap();
    }
    let path = dir.path().join("meta").join("checkpoint.bin");
    let mut bytes = fs::read(&path).unwrap();
    bytes[8..12].copy_from_slice(&1_u32.to_le_bytes());
    bytes.truncate(bytes.len() - 4);
    fs::write(path, bytes).unwrap();
    let engine = Engine::open(dir.options()).unwrap();
    assert_eq!(engine.get(b"legacy").unwrap(), Some(b"kept".to_vec()));
}

#[test]
fn checkpoint_corruption_is_reported_without_mutating_segments() {
    let dir = TempDir::new("checkpoint_corrupt");
    {
        let engine = Engine::open(dir.options()).unwrap();
        engine.put(b"key", b"value").unwrap();
        engine.checkpoint().unwrap();
    }
    let before = fs::read(dir.segment(1)).unwrap();
    let path = dir.path().join("meta").join("checkpoint.bin");
    let mut checkpoint = fs::read(&path).unwrap();
    checkpoint[20] ^= 1;
    fs::write(path, checkpoint).unwrap();
    assert!(matches!(
        Engine::open(dir.options()),
        Err(EngineError::Corruption(_))
    ));
    assert_eq!(fs::read(dir.segment(1)).unwrap(), before);
}

#[test]
fn corrupted_complete_records_are_not_truncated_as_partial_tails() {
    let dir = TempDir::new("record_corrupt");
    {
        let engine = Engine::open(dir.options()).unwrap();
        engine.put(b"key", b"value").unwrap();
    }
    let mut bytes = fs::read(dir.segment(1)).unwrap();
    let final_byte = bytes.len() - 1;
    bytes[final_byte] ^= 1;
    fs::write(dir.segment(1), &bytes).unwrap();
    assert!(matches!(
        Engine::open(dir.options()),
        Err(EngineError::Corruption(_))
    ));
    assert_eq!(fs::read(dir.segment(1)).unwrap(), bytes);
}

#[test]
fn value_reads_verify_checksums_even_after_checkpoint_recovery() {
    let dir = TempDir::new("read_checksum");
    {
        let engine = Engine::open(dir.options()).unwrap();
        engine.put(b"key", b"value").unwrap();
        engine.checkpoint().unwrap();
    }
    let path = dir.segment(1);
    let mut file = OpenOptions::new().write(true).open(path).unwrap();
    file.seek(SeekFrom::End(-1)).unwrap();
    file.write_all(b"X").unwrap();
    drop(file);
    let engine = Engine::open(dir.options()).unwrap();
    assert!(matches!(
        engine.get(b"key"),
        Err(EngineError::Corruption(_))
    ));
}

#[test]
fn sealed_segment_truncation_is_reported() {
    let dir = TempDir::new("sealed_tail");
    let mut options = dir.options();
    options.segment_size_bytes = 4096;
    {
        let engine = Engine::open(options.clone()).unwrap();
        engine.put(b"old", b"value").unwrap();
        engine.put(b"new", &vec![1; 8192]).unwrap();
    }
    let path = dir.segment(1);
    let length = fs::metadata(&path).unwrap().len();
    OpenOptions::new()
        .write(true)
        .open(path)
        .unwrap()
        .set_len(length - 1)
        .unwrap();
    assert!(matches!(
        Engine::open(options),
        Err(EngineError::Corruption(_))
    ));
}

#[test]
fn incomplete_large_lengths_are_discarded_before_allocation() {
    let dir = TempDir::new("length_limits");
    {
        let engine = Engine::open(dir.options()).unwrap();
        engine.put(b"key", b"value").unwrap();
    }
    let mut bytes = fs::read(dir.segment(1)).unwrap();
    bytes[8..12].copy_from_slice(&u32::MAX.to_le_bytes());
    fs::write(dir.segment(1), bytes).unwrap();
    let engine = Engine::open(dir.options()).unwrap();
    assert_eq!(engine.len(), 0);
    assert_eq!(fs::metadata(dir.segment(1)).unwrap().len(), 0);
}

#[test]
fn synchronous_batches_are_recoverable_without_close() {
    let dir = TempDir::new("sync_batch");
    let mut options = dir.options();
    options.sync_policy = SyncPolicy::SyncData;
    {
        let engine = Engine::open(options.clone()).unwrap();
        engine
            .write_batch(&[WriteOp::put("a", "one"), WriteOp::put("b", "two")])
            .unwrap();
    }
    let engine = Engine::open(options).unwrap();
    assert_eq!(engine.get(b"a").unwrap(), Some(b"one".to_vec()));
    assert_eq!(engine.get(b"b").unwrap(), Some(b"two".to_vec()));
}

#[test]
fn crash_writer_fixture() {
    use std::io::{Read, Write};
    let Some(path) = std::env::var_os("KERNIVA_CRASH_TEST_DIR") else {
        return;
    };
    let mode = std::env::var("KERNIVA_CRASH_TEST_MODE").unwrap();
    let mut options = Options::new(PathBuf::from(path));
    options.segment_size_bytes = 4096;
    options.sync_policy = SyncPolicy::SyncData;
    let engine = Engine::open(options).unwrap();
    engine.put(b"old", b"safe").unwrap();
    if mode == "batch" {
        engine
            .write_batch(&[
                WriteOp::put("a", "one"),
                WriteOp::put("b", "two"),
                WriteOp::delete("old"),
            ])
            .unwrap();
    } else if mode == "clear" {
        engine.checkpoint().unwrap();
        engine.clear().unwrap();
    } else {
        engine.checkpoint().unwrap();
        engine.delete(b"old").unwrap();
        engine.put(b"filler", &vec![3; 8192]).unwrap();
        assert_eq!(engine.compact_once().unwrap(), 0);
    }
    println!("KERNIVA_CRASH_READY");
    std::io::stdout().flush().unwrap();
    let mut input = [0_u8; 1];
    let _ = std::io::stdin().read_exact(&mut input);
    panic!("parent should terminate this fixture");
}

fn terminate_writer(mode: &str) -> TempDir {
    use std::io::{BufRead, BufReader};
    use std::process::{Command, Stdio};
    let dir = TempDir::new("process_crash");
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "crash_writer_fixture", "--nocapture"])
        .env("KERNIVA_CRASH_TEST_DIR", dir.path())
        .env("KERNIVA_CRASH_TEST_MODE", mode)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let stdout = BufReader::new(child.stdout.take().unwrap());
    let mut ready = false;
    for line in stdout.lines() {
        if line.unwrap().contains("KERNIVA_CRASH_READY") {
            ready = true;
            break;
        }
    }
    if !ready {
        let status = child.wait().unwrap();
        panic!("crash fixture failed before commit: {status}");
    }
    child.kill().unwrap();
    assert!(!child.wait().unwrap().success());
    dir
}

#[test]
fn acknowledged_sync_batch_survives_process_termination() {
    let dir = terminate_writer("batch");
    let engine = Engine::open(dir.options()).unwrap();
    assert_eq!(engine.get(b"a").unwrap(), Some(b"one".to_vec()));
    assert_eq!(engine.get(b"b").unwrap(), Some(b"two".to_vec()));
    assert_eq!(engine.get(b"old").unwrap(), None);
}

#[test]
fn acknowledged_sync_clear_survives_process_termination() {
    let dir = terminate_writer("clear");
    let engine = Engine::open(dir.options()).unwrap();
    assert!(engine.is_empty());
    assert_eq!(engine.get(b"old").unwrap(), None);
}

#[test]
fn zero_rewrite_compaction_survives_process_termination() {
    let dir = terminate_writer("compact");
    let engine = Engine::open(dir.options()).unwrap();
    assert_eq!(engine.get(b"old").unwrap(), None);
    assert_eq!(engine.get(b"filler").unwrap(), Some(vec![3; 8192]));
}

#[test]
fn storage_engines_keep_directory_formats_separate() {
    let root = TempDir::new("directory_formats");
    let legacy_path = root.path().join("legacy");
    let legacy = Engine::open(Options::new(&legacy_path)).unwrap();
    assert!(legacy_path.join("segments").is_dir());
    assert!(legacy_path.join("meta").is_dir());
    legacy.put(b"key", b"legacy").unwrap();
    drop(legacy);
    assert!(matches!(
        DurableMemoryEngine::open(&legacy_path, 4, None),
        Err(DurableMemoryError::Storage(EngineError::InvalidOptions(_)))
    ));
    let reopened = Engine::open(Options::new(&legacy_path)).unwrap();
    assert_eq!(
        reopened.get(b"key").unwrap().as_deref(),
        Some(&b"legacy"[..])
    );

    let memory_path = root.path().join("memory");
    fs::create_dir(&memory_path).unwrap();
    let durable = DurableMemoryEngine::open(&memory_path, 4, None).unwrap();
    durable.put(b"key", b"durable").unwrap();
    assert!(matches!(
        Engine::open(Options::new(&memory_path)),
        Err(EngineError::Locked(_))
    ));
    drop(durable);
    assert!(matches!(
        Engine::open(Options::new(&memory_path)),
        Err(EngineError::InvalidOptions(_))
    ));
    assert!(!memory_path.join("segments").exists());
    assert!(!memory_path.join("meta").exists());
    let reopened = DurableMemoryEngine::open(&memory_path, 4, None).unwrap();
    assert_eq!(
        reopened.get(b"key").unwrap().as_deref(),
        Some(&b"durable"[..])
    );
}
