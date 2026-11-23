use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::wal::{InjectedFailure, SyncPause, WAL_NAME};
use super::*;

struct TestEngine {
    engine: Option<Arc<DurableMemoryEngine>>,
    directory: std::path::PathBuf,
}

impl TestEngine {
    fn new(name: &str) -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "kerniva_memory_wal_{name}_{}_{nonce}",
            std::process::id(),
        ));
        std::fs::create_dir(&directory).unwrap();
        let engine = Arc::new(DurableMemoryEngine::open_at(&directory, 4, None, 0).unwrap());
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

#[test]
fn partial_write_and_sync_failure_restore_the_previous_commit() {
    let test = TestEngine::new("rollback");
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
            engine.put(b"a", b"unpublished"),
            Err(DurableMemoryError::Storage(EngineError::Io(_)))
        ));
        assert_eq!(std::fs::read(&path).unwrap(), committed);
        assert_eq!(
            engine.get_entry_at(b"a", 0).unwrap().unwrap().version,
            version
        );
        assert_eq!(engine.get(b"a").unwrap().as_deref(), Some(&b"old"[..]));
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
fn uncertain_commit_freezes_writes_until_reopened() {
    let mut test = TestEngine::new("uncertain");
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
    let reopened = DurableMemoryEngine::open_at(&test.directory, 4, None, 0).unwrap();
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
