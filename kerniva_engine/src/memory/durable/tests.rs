use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::wal::{InjectedFailure, SyncPause, WAL_NAME};
use super::*;

const READ_MODES: [DurableReadMode; 2] = [
    DurableReadMode::HoldAffectedShards,
    DurableReadMode::ReadCommitted,
];

struct TestEngine {
    engine: Option<Arc<DurableMemoryEngine>>,
    directory: std::path::PathBuf,
}

impl TestEngine {
    fn new(name: &str) -> Self {
        Self::with_read_mode(name, DurableReadMode::default(), None)
    }

    fn with_read_mode(
        name: &str,
        read_mode: DurableReadMode,
        max_payload_bytes: Option<usize>,
    ) -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "kerniva_memory_wal_{name}_{}_{nonce}",
            std::process::id(),
        ));
        std::fs::create_dir(&directory).unwrap();
        let engine = Arc::new(
            DurableMemoryEngine::open_at_with_read_mode(
                &directory,
                4,
                max_payload_bytes,
                0,
                read_mode,
            )
            .unwrap(),
        );
        Self {
            engine: Some(engine),
            directory,
        }
    }

    fn engine(&self) -> &Arc<DurableMemoryEngine> {
        self.engine.as_ref().unwrap()
    }
}

impl Drop for TestEngine {
    fn drop(&mut self) {
        drop(self.engine.take());
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

fn put<'a>(key: &'a [u8], value: &'a [u8]) -> MemoryBatchOp<'a> {
    MemoryBatchOp::Put {
        key,
        value,
        options: MemoryWriteOptions::default(),
    }
}

#[test]
fn partial_write_and_sync_failure_restore_the_previous_commit() {
    for read_mode in READ_MODES {
        check_rollback_after_io_error(read_mode);
    }
}

fn check_rollback_after_io_error(read_mode: DurableReadMode) {
    let test = TestEngine::with_read_mode("rollback", read_mode, None);
    let engine = test.engine();
    engine.put(b"a", b"old").unwrap();
    let version = engine.get_entry_at(b"a", 0).unwrap().unwrap().version;
    let previous = engine.wal_stats().unwrap();
    let path = test.directory.join(WAL_NAME);
    let committed = std::fs::read(&path).unwrap();

    for failure in [
        InjectedFailure {
            partial_write: Some(11),
            ..InjectedFailure::default()
        },
        InjectedFailure {
            sync: true,
            ..InjectedFailure::default()
        },
    ] {
        engine.wal.lock().unwrap().failure = Some(failure);
        assert!(matches!(
            engine.apply_batch_at(&[put(b"a", b"unpublished"), put(b"b", b"new")], 0),
            Err(DurableMemoryError::Storage(EngineError::Io(_)))
        ));
        assert_eq!(std::fs::read(&path).unwrap(), committed);
        assert_eq!(
            engine.get_entry_at(b"a", 0).unwrap().unwrap().version,
            version
        );
        assert_eq!(engine.get(b"a").unwrap().as_deref(), Some(&b"old"[..]));
        assert!(engine.get(b"b").unwrap().is_none());
        assert_eq!(engine.stats().unwrap().payload_bytes, 4);
        assert!(!engine.wal_stats().unwrap().requires_reopen);
    }

    let stats = engine.wal_stats().unwrap();
    assert_eq!(stats.log_bytes, previous.log_bytes);
    assert_eq!(stats.failed_commits, previous.failed_commits + 2);
    assert_eq!(stats.sync_attempts, previous.sync_attempts + 1);
    engine.put(b"a", b"committed").unwrap();
    assert_eq!(
        engine.get_entry_at(b"a", 0).unwrap().unwrap().version,
        version + 1
    );
    assert_eq!(
        engine.get(b"a").unwrap().as_deref(),
        Some(&b"committed"[..])
    );
}

#[test]
fn rejected_batch_changes_neither_versions_nor_wal_in_both_modes() {
    for read_mode in READ_MODES {
        let test = TestEngine::with_read_mode("budget", read_mode, Some(4));
        let engine = test.engine();
        engine.put(b"a", b"x").unwrap();
        let original = engine.get_entry_at(b"a", 0).unwrap().unwrap();
        let before = engine.stats().unwrap();
        let wal = engine.wal_stats().unwrap();

        assert!(
            !engine
                .apply_batch_at(
                    &[
                        put(b"a", b"x2"),
                        MemoryBatchOp::Delete {
                            key: b"absent",
                            condition: MemoryCondition::Present
                        },
                    ],
                    0
                )
                .unwrap()
        );
        assert!(matches!(
            engine.apply_batch_at(&[put(b"a", b"x2"), put(b"b", b"y")], 0),
            Err(DurableMemoryError::Memory(
                MemoryError::PayloadLimitExceeded
            ))
        ));
        assert_eq!(engine.get_entry_at(b"a", 0).unwrap().unwrap(), original);
        assert!(engine.get(b"b").unwrap().is_none());
        assert_eq!(engine.stats().unwrap(), before);
        assert_eq!(engine.wal_stats().unwrap(), wal);

        assert!(
            engine
                .apply_batch_at(&[put(b"a", b"z"), put(b"b", b"y")], 0)
                .unwrap()
        );
        let entries = engine.get_many_at(&[b"a", b"b"], 0).unwrap();
        assert_eq!(entries[0].as_ref().unwrap().version, original.version + 1);
        assert_eq!(entries[1].as_ref().unwrap().version, original.version + 2);
        assert_eq!(engine.stats().unwrap().payload_bytes, 4);
    }
}

#[test]
fn uncertain_commit_freezes_writes_until_reopened() {
    for read_mode in READ_MODES {
        check_uncertain_commit_and_reopen(read_mode);
    }
}

fn check_uncertain_commit_and_reopen(read_mode: DurableReadMode) {
    let mut test = TestEngine::with_read_mode("uncertain", read_mode, None);
    let engine = test.engine();
    engine.put(b"a", b"old").unwrap();
    engine.wal.lock().unwrap().failure = Some(InjectedFailure {
        sync: true,
        rollback: true,
        ..InjectedFailure::default()
    });
    assert!(matches!(
        engine.put(b"a", b"unconfirmed"),
        Err(DurableMemoryError::UncertainCommit { .. })
    ));
    assert!(engine.wal_stats().unwrap().requires_reopen);
    assert!(matches!(
        engine.put(b"b", b"blocked"),
        Err(DurableMemoryError::ReopenRequired)
    ));
    assert!(matches!(
        engine.delete(b"a"),
        Err(DurableMemoryError::ReopenRequired)
    ));
    assert_eq!(engine.get(b"a").unwrap().as_deref(), Some(&b"old"[..]));
    assert_eq!(engine.stats().unwrap().payload_bytes, 4);

    drop(test.engine.take());
    let reopened =
        DurableMemoryEngine::open_at_with_read_mode(&test.directory, 4, None, 0, read_mode)
            .unwrap();
    // The injected rollback never ran, so replay sees the complete unconfirmed frame.
    assert_eq!(
        reopened.get(b"a").unwrap().as_deref(),
        Some(&b"unconfirmed"[..])
    );
    assert!(reopened.get(b"b").unwrap().is_none());
    assert!(!reopened.wal_stats().unwrap().requires_reopen);
    reopened.put(b"b", b"unblocked").unwrap();
}

#[test]
fn sync_blocks_affected_reads_and_leaves_unrelated_reads_available() {
    let test = TestEngine::new("visibility");
    let engine = test.engine();
    let key = b"blocked";
    let unrelated = (0u16..)
        .map(u16::to_le_bytes)
        .find(|candidate| engine.memory.shard_index(candidate) != engine.memory.shard_index(key))
        .unwrap();
    engine.put(key, b"old").unwrap();
    engine.put(&unrelated, b"independent").unwrap();
    let precommit = engine.get(key).unwrap().unwrap();

    let (entered, syncing) = mpsc::channel();
    let (finished, release) = mpsc::channel();
    engine.wal.lock().unwrap().pause = Some(SyncPause { entered, release });

    let writer = Arc::clone(engine);
    let writer = thread::spawn(move || writer.put(key, b"new").unwrap());
    syncing.recv_timeout(Duration::from_secs(2)).unwrap();

    let other = Arc::clone(engine);
    let changed = Arc::clone(engine);
    let (other_result, other_read) = mpsc::channel();
    let (changed_result, changed_read) = mpsc::channel();
    let (reading, attempted) = mpsc::channel();
    let other = thread::spawn(move || {
        let _ = other_result.send(other.get(&unrelated).unwrap());
    });
    let changed = thread::spawn(move || {
        let _ = reading.send(());
        let _ = changed_result.send(changed.get(key).unwrap());
    });

    let unaffected = other_read.recv_timeout(Duration::from_secs(2));
    let started = attempted.recv_timeout(Duration::from_secs(2));
    let premature = changed_read.recv_timeout(Duration::from_millis(30));
    finished.send(()).unwrap();

    writer.join().unwrap();
    other.join().unwrap();
    changed.join().unwrap();
    assert_eq!(unaffected.unwrap().as_deref(), Some(&b"independent"[..]));
    started.unwrap();
    assert_eq!(premature, Err(mpsc::RecvTimeoutError::Timeout));
    assert_eq!(changed_read.recv().unwrap().as_deref(), Some(&b"new"[..]));
    assert_eq!(precommit.as_ref(), b"old");
}

#[test]
fn read_committed_returns_old_batch_and_payload_through_sync() {
    let test = TestEngine::with_read_mode("read_committed", DurableReadMode::ReadCommitted, None);
    let engine = test.engine();
    let key = b"first";
    let second = (0u16..)
        .map(u16::to_le_bytes)
        .find(|candidate| engine.memory.shard_index(candidate) != engine.memory.shard_index(key))
        .unwrap();
    engine.put(key, b"a").unwrap();
    engine.put(&second, b"b").unwrap();
    let before = engine.stats().unwrap();
    let version = engine.get_entry_at(key, 0).unwrap().unwrap().version;

    let (entered, syncing) = mpsc::channel();
    let (finished, release) = mpsc::channel();
    engine.wal.lock().unwrap().pause = Some(SyncPause { entered, release });

    let writer = Arc::clone(engine);
    let writer = thread::spawn(move || {
        writer
            .apply_batch_at(
                &[
                    MemoryBatchOp::Put {
                        key,
                        value: b"longer-a",
                        options: MemoryWriteOptions::default(),
                    },
                    MemoryBatchOp::Put {
                        key: &second,
                        value: b"longer-b",
                        options: MemoryWriteOptions::default(),
                    },
                ],
                0,
            )
            .unwrap()
    });
    syncing.recv_timeout(Duration::from_secs(2)).unwrap();

    let reader = Arc::clone(engine);
    let (reported, report) = mpsc::channel();
    let reader = thread::spawn(move || {
        let values = reader.get_many_at(&[key, &second], 0).unwrap();
        reported.send((values, reader.stats().unwrap())).unwrap();
    });
    let during = report.recv_timeout(Duration::from_secs(2));

    let (attempting, attempted) = mpsc::channel();
    let (responded, response) = mpsc::channel();
    let contender = Arc::clone(engine);
    let contender = thread::spawn(move || {
        attempting.send(()).unwrap();
        let result = contender.put_with_at(
            key,
            b"stale",
            MemoryWriteOptions {
                condition: MemoryCondition::Version(version),
                expires_at_ms: None,
            },
            0,
        );
        responded.send(result).unwrap();
    });
    let started = attempted.recv_timeout(Duration::from_secs(2));
    let premature = response.recv_timeout(Duration::from_millis(30));
    finished.send(()).unwrap();
    reader.join().unwrap();
    assert!(writer.join().unwrap());
    contender.join().unwrap();
    started.unwrap();
    assert!(matches!(premature, Err(mpsc::RecvTimeoutError::Timeout)));
    assert_eq!(response.recv().unwrap().unwrap(), None);

    let (values, stats) = during.unwrap();
    assert_eq!(values[0].as_ref().unwrap().value.as_ref(), b"a");
    assert_eq!(values[1].as_ref().unwrap().value.as_ref(), b"b");
    assert_eq!(stats, before);
    let values = engine.get_many_at(&[key, &second], 0).unwrap();
    assert_eq!(values[0].as_ref().unwrap().value.as_ref(), b"longer-a");
    assert_eq!(values[1].as_ref().unwrap().value.as_ref(), b"longer-b");
    assert_eq!(
        engine.stats().unwrap().payload_bytes,
        before.payload_bytes + 14
    );
}
