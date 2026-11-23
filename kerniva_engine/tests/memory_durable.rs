use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use kerniva_engine::{
    DurableMemoryEngine, DurableMemoryError, EngineError, MemoryBatchOp, MemoryCondition,
    MemoryError, MemoryWriteOptions,
};

const WAL: &str = "memory-v1.wal";
const KILL_TEST_PATH: &str = "KERNIVA_MEMORY_DURABLE_KILL_TEST_PATH";

struct TempDir(PathBuf);

impl TempDir {
    fn new(name: &str) -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "kerniva_memory_durable_{name}_{}_{nonce}",
            std::process::id()
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }

    fn open(&self, max_payload_bytes: Option<usize>, now_ms: u64) -> DurableMemoryEngine {
        DurableMemoryEngine::open_at(self.path(), 4, max_payload_bytes, now_ms).unwrap()
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
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
fn reopen_preserves_version_ttl_and_deleted_keys_across_shard_counts() {
    let dir = TempDir::new("reopen");
    let engine = dir.open(None, 1);
    engine.put(b"ttl", b"old").unwrap();
    let expires = engine
        .put_with_at(
            b"ttl",
            b"new",
            MemoryWriteOptions {
                condition: MemoryCondition::Present,
                expires_at_ms: Some(100),
            },
            5,
        )
        .unwrap()
        .unwrap();
    engine.put(b"stable", b"x").unwrap();
    let stable = engine.get_entry_at(b"stable", 5).unwrap().unwrap().version;
    engine.put(b"gone", b"x").unwrap();
    assert!(engine.delete_at(b"gone", 5).unwrap());
    drop(engine);

    assert!(matches!(
        DurableMemoryEngine::open_at(dir.path(), 16, Some(7), 99),
        Err(DurableMemoryError::Memory(
            MemoryError::PayloadLimitExceeded
        ))
    ));
    let engine = DurableMemoryEngine::open_at(dir.path(), 16, None, 99).unwrap();
    let entry = engine.get_entry_at(b"ttl", 99).unwrap().unwrap();
    assert_eq!(entry.value.as_ref(), b"new");
    assert_eq!(entry.version, expires);
    assert_eq!(entry.expires_at_ms, Some(100));
    assert!(engine.get_entry_at(b"gone", 99).unwrap().is_none());
    assert_eq!(
        engine.get_entry_at(b"stable", 99).unwrap().unwrap().version,
        stable
    );
    drop(engine);

    let engine = dir.open(Some(7), 100);
    // Recovering after the latest write's deadline must hide the older nonexpiring value.
    assert!(engine.get_entry_at(b"ttl", 100).unwrap().is_none());
    assert_eq!(engine.stats().unwrap().payload_bytes, 7);
    let next = engine
        .put_with_at(
            b"stable",
            b"y",
            MemoryWriteOptions {
                condition: MemoryCondition::Version(stable),
                expires_at_ms: None,
            },
            100,
        )
        .unwrap()
        .unwrap();
    assert!(next > stable + 1);
    assert!(engine.get_entry_at(b"gone", 100).unwrap().is_none());
}

#[test]
fn rejected_batches_leave_log_and_versions_unchanged() {
    let dir = TempDir::new("budget");
    let engine = dir.open(Some(5), 0);
    engine.put(b"a", b"a").unwrap();
    let before = engine.wal_stats().unwrap();
    let initial_version = engine.get_entry_at(b"a", 0).unwrap().unwrap().version;

    assert!(
        !engine
            .apply_batch_at(
                &[
                    put(b"a", b"b"),
                    MemoryBatchOp::Delete {
                        key: b"missing",
                        condition: MemoryCondition::Present
                    },
                ],
                0,
            )
            .unwrap()
    );
    assert!(matches!(
        engine.apply_batch_at(&[put(b"a", b"b"), put(b"b", b"oversized")], 0),
        Err(DurableMemoryError::Memory(
            MemoryError::PayloadLimitExceeded
        ))
    ));
    assert_eq!(engine.wal_stats().unwrap(), before);
    assert_eq!(engine.stats().unwrap().payload_bytes, 2);
    let entry = engine.get_entry_at(b"a", 0).unwrap().unwrap();
    assert_eq!(entry.value.as_ref(), b"a");
    assert_eq!(entry.version, initial_version);

    // Subsequent operations see prior staged changes. The discarded Put still
    // consumes a token after this batch commits.
    assert!(
        engine
            .apply_batch_at(
                &[
                    put(b"b", b"temporary oversized value"),
                    MemoryBatchOp::Delete {
                        key: b"b",
                        condition: MemoryCondition::Present
                    },
                    put(b"a", b"done"),
                ],
                0,
            )
            .unwrap()
    );
    assert_eq!(engine.wal_stats().unwrap().commits, before.commits + 1);
    assert_eq!(engine.stats().unwrap().payload_bytes, 5);
    drop(engine);

    let engine = dir.open(Some(5), 0);
    let entry = engine.get_entry_at(b"a", 0).unwrap().unwrap();
    assert_eq!(entry.value.as_ref(), b"done");
    assert_eq!(entry.version, initial_version + 2);
    assert!(engine.get_entry_at(b"b", 0).unwrap().is_none());
}

#[test]
fn truncated_batch_tail_is_discarded_and_complete_corruption_is_reported() {
    let dir = TempDir::new("tail");
    let path = dir.path().join(WAL);
    let engine = dir.open(None, 0);
    engine.put(b"a", b"old").unwrap();
    let before = fs::read(&path).unwrap();
    assert!(
        engine
            .apply_batch_at(&[put(b"a", b"new"), put(b"b", b"new")], 0)
            .unwrap()
    );
    drop(engine);
    let completed = fs::read(&path).unwrap();

    for bytes in [1, 19, 20, completed.len() - before.len() - 1] {
        fs::write(&path, &completed[..before.len() + bytes]).unwrap();
        let engine = dir.open(None, 0);
        assert_eq!(engine.get(b"a").unwrap().as_deref(), Some(&b"old"[..]));
        assert!(engine.get(b"b").unwrap().is_none());
        assert_eq!(fs::read(&path).unwrap(), before);
    }

    let mut corrupt = completed;
    *corrupt.last_mut().unwrap() ^= 1;
    fs::write(&path, &corrupt).unwrap();
    assert!(matches!(
        DurableMemoryEngine::open_at(dir.path(), 4, None, 0),
        Err(DurableMemoryError::Storage(EngineError::Corruption(_)))
    ));
    assert_eq!(fs::read(&path).unwrap(), corrupt);
}

#[test]
fn acknowledged_batch_survives_process_kill() {
    let dir = TempDir::new("kill");
    let mut writer = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "durability_kill_test_worker"])
        .env(KILL_TEST_PATH, dir.path())
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    let marker = dir.path().join("committed");
    let acknowledged = loop {
        if marker.is_file() {
            break true;
        }
        if Instant::now() >= deadline || writer.try_wait().unwrap().is_some() {
            break false;
        }
        thread::sleep(Duration::from_millis(10));
    };
    let killed = writer.kill();
    let status = writer.wait().unwrap();
    assert!(
        acknowledged,
        "durability subprocess exited before acknowledging: {status}"
    );
    killed.unwrap();
    assert!(!status.success());

    let engine = dir.open(None, 0);
    let values = engine.get_many_at(&[b"a", b"b", b"missing"], 0).unwrap();
    assert_eq!(values[0].as_ref().unwrap().value.as_ref(), b"1");
    assert_eq!(values[1].as_ref().unwrap().value.as_ref(), b"2");
    assert!(values[2].is_none());
}

#[test]
fn durability_kill_test_worker() {
    let Some(path) = std::env::var_os(KILL_TEST_PATH) else {
        return;
    };
    let path = PathBuf::from(path);
    let engine = DurableMemoryEngine::open_at(&path, 4, None, 0).unwrap();
    assert!(
        engine
            .apply_batch_at(&[put(b"a", b"1"), put(b"b", b"2")], 0)
            .unwrap()
    );
    fs::write(path.join("committed"), b"ack").unwrap();
    thread::sleep(Duration::from_secs(10));
    drop(engine);
}

#[test]
fn dedicated_directory_is_locked_and_has_its_own_format() {
    let dir = TempDir::new("directory");
    let engine = dir.open(None, 0);
    assert!(matches!(
        DurableMemoryEngine::open_at(dir.path(), 4, None, 0),
        Err(DurableMemoryError::Storage(EngineError::Locked(_)))
    ));
    drop(engine);
    let path = dir.path().join(WAL);
    fs::write(path, b"OLDWAL01").unwrap();
    assert!(matches!(
        DurableMemoryEngine::open_at(dir.path(), 4, None, 0),
        Err(DurableMemoryError::Storage(EngineError::Corruption(_)))
    ));

    fs::create_dir(dir.path().join("segments")).unwrap();
    assert!(matches!(
        DurableMemoryEngine::open_at(dir.path(), 4, None, 0),
        Err(DurableMemoryError::Storage(EngineError::InvalidOptions(_)))
    ));
}
