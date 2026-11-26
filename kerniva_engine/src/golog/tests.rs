use super::*;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

static NEXT_TEST: AtomicU64 = AtomicU64::new(0);

struct TestDirectory {
    root: PathBuf,
    database: PathBuf,
}

impl TestDirectory {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "kerniva_go_log_{}_{}_{:?}",
            std::process::id(),
            NEXT_TEST.fetch_add(1, Ordering::Relaxed),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
        ));
        let database = root.join("db_0");
        fs::create_dir_all(&database).unwrap();
        Self { root, database }
    }

    fn segment(&self, first: u64, last: u64) -> PathBuf {
        self.database
            .join(format!("log-{first:020}-{last:020}.seg"))
    }

    fn seal(&self, first: u64, last: u64, bytes: &[u8]) -> PathBuf {
        let path = self.segment(first, last);
        fs::write(&path, bytes).unwrap();
        path
    }

    fn write_active(&self, bytes: &[u8]) {
        fs::write(self.database.join(ACTIVE), bytes).unwrap();
    }

    fn maintain(&self, force: bool) -> Option<MaintenanceOutcome> {
        let worker = Maintainer::open(self.root.clone(), MaintenanceOptions::default()).unwrap();
        worker.run_pass(force).unwrap().pop().unwrap().1.unwrap()
    }

    fn maintain_with_retention(&self, force: bool, retain: u64) -> Option<MaintenanceOutcome> {
        let worker = Maintainer::open_with_retention(
            self.root.clone(),
            MaintenanceOptions::default(),
            retain,
        )
        .unwrap();
        worker.run_pass(force).unwrap().pop().unwrap().1.unwrap()
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn batch(sequence: u64, records: &[(u8, &[u8], &[u8])]) -> Vec<u8> {
    let mut body = sequence.to_le_bytes().to_vec();
    body.extend_from_slice(&(records.len() as u32).to_le_bytes());
    for (kind, key, value) in records {
        body.push(*kind);
        body.extend_from_slice(&(key.len() as u32).to_le_bytes());
        body.extend_from_slice(&(value.len() as u32).to_le_bytes());
        body.extend_from_slice(key);
        body.extend_from_slice(value);
    }
    let mut output = (body.len() as u32).to_le_bytes().to_vec();
    output.extend_from_slice(&format::crc32(&body).to_le_bytes());
    output.extend_from_slice(&body);
    output
}

fn log(frames: &[&[u8]]) -> Vec<u8> {
    let mut output = b"KGLLOG01".to_vec();
    for frame in frames {
        output.extend_from_slice(frame);
    }
    output
}

#[test]
fn live_maintainer_respects_snapshot_policy_even_for_forced_pass() {
    let dir = TestDirectory::new();
    let segment = dir.seal(1, 1, &log(&[&batch(1, &[(1, b"k", b"value")])]));
    let worker = Maintainer::open(dir.root.clone(), MaintenanceOptions::default()).unwrap();
    let policy = dir.root.join(SNAPSHOT_MAINTENANCE);
    fs::write(&policy, b"0").unwrap();
    assert!(worker.run_pass(true).unwrap().is_empty());
    assert!(segment.exists());
    assert!(!dir.database.join(CURRENT).exists());
    fs::write(&policy, b"invalid").unwrap();
    assert!(worker.run_pass(true).is_err());
    assert!(segment.exists());
    fs::write(&policy, b"1").unwrap();
    assert!(
        worker
            .run_pass(true)
            .unwrap()
            .pop()
            .unwrap()
            .1
            .unwrap()
            .is_some()
    );
    assert!(dir.database.join(CURRENT).is_file());
}

#[test]
fn restores_complete_batches_and_reports_truncated_active_tail() {
    let directory = TestDirectory::new();
    assert_eq!(format::crc32(b"123456789"), 0xcbf4_3926);
    directory.seal(1, 1, &log(&[&batch(1, &[(1, b"a", b"old")])]));
    let whole = log(&[&batch(2, &[(1, b"a", b"new"), (1, b"b", b"retained")])]);
    let mut torn = whole.clone();
    torn.extend_from_slice(&batch(3, &[(2, b"b", b"")])[..18]);
    directory.write_active(&torn);
    let restored = recover(&directory.database).unwrap();
    assert_eq!(restored.last_sequence, 2);
    assert_eq!(restored.active_end, whole.len() as u64);
    assert_eq!(restored.state[b"a".as_slice()], b"new");
    assert_eq!(restored.state[b"b".as_slice()], b"retained");
}

#[test]
fn rejects_complete_corruption_and_missing_commits() {
    let directory = TestDirectory::new();
    let mut damaged = log(&[&batch(1, &[(1, b"k", b"lost")])]);
    *damaged.last_mut().unwrap() ^= 0x80;
    directory.write_active(&damaged);
    assert!(recover(&directory.database).is_err());

    directory.write_active(b"KGLLOG01");
    let sealed = directory.seal(2, 2, &log(&[&batch(2, &[(1, b"k", b"gap")])]));
    assert!(recover(&directory.database).is_err());
    assert!(
        Maintainer::open(directory.root.clone(), MaintenanceOptions::default())
            .unwrap()
            .run_pass(true)
            .unwrap()
            .pop()
            .unwrap()
            .1
            .is_err()
    );
    fs::remove_file(sealed).unwrap();
    directory.seal(1, 2, &log(&[&batch(1, &[(1, b"k", b"short")])]));
    assert!(recover(&directory.database).is_err());
}

#[test]
fn delta_folds_selected_commits_and_resets() {
    let directory = TestDirectory::new();
    directory.seal(1, 10, b"corrupt old history");
    directory.seal(16, 16, b"unselected corrupt segment");
    directory.write_active(b"unselected active file");
    fs::write(directory.database.join(CURRENT), b"unselected cutoff").unwrap();
    directory.seal(
        11,
        12,
        &log(&[
            &batch(11, &[(1, b"changed", b"old"), (1, b"removed", b"old")]),
            &batch(12, &[(1, b"changed", b"new"), (2, b"removed", b"")]),
        ]),
    );
    directory.seal(
        13,
        15,
        &log(&[
            &batch(13, &[(3, b"", b"")]),
            &batch(14, &[(1, b"after", b"value"), (1, b"lost", b"value")]),
            &batch(15, &[(1, b"", b""), (2, b"lost", b"")]),
        ]),
    );

    let ranges = [
        GoLogRange {
            first: 11,
            last: 12,
        },
        GoLogRange {
            first: 13,
            last: 15,
        },
    ];
    let mut delta = Delta::load(&directory.database, 10, &ranges[..1]).unwrap();
    assert!(!delta.reset);
    let expected = batch(0, &[(1, b"changed", b"new"), (2, b"removed", b"")]);
    assert_eq!(delta.next_block().unwrap().unwrap(), expected[20..]);
    assert!(delta.next_block().unwrap().is_none());

    let mut delta = Delta::load(&directory.database, 10, &ranges).unwrap();
    assert!(delta.reset);
    let expected = batch(
        0,
        &[(1, b"", b""), (1, b"after", b"value"), (2, b"lost", b"")],
    );
    assert_eq!(delta.next_block().unwrap().unwrap(), expected[20..]);
    assert!(delta.next_block().unwrap().is_none());

    directory.seal(17, 17, &log(&[&batch(17, &[(3, b"", b"")])]));
    let mut delta = Delta::load(
        &directory.database,
        16,
        &[GoLogRange {
            first: 17,
            last: 17,
        }],
    )
    .unwrap();
    assert!(delta.reset);
    assert!(delta.next_block().unwrap().is_none());
}

#[test]
fn delta_rejects_incomplete_or_disagreeing_segments() {
    let directory = TestDirectory::new();
    directory.seal(7, 7, &log(&[&batch(7, &[(1, b"good", b"first")])]));
    let ranges = [
        GoLogRange { first: 7, last: 7 },
        GoLogRange { first: 8, last: 9 },
    ];
    let first = batch(8, &[(1, b"good", b"second")]);
    let second = batch(9, &[(2, b"old", b"")]);
    let complete = log(&[&first, &second]);
    let mut bad_crc = complete.clone();
    *bad_crc.last_mut().unwrap() ^= 0x80;

    for (label, bytes) in [
        ("CRC", bad_crc),
        ("magic", [b"KGLLOG02".as_slice(), &complete[8..]].concat()),
        (
            "sequence",
            log(&[&first, &batch(10, &[(1, b"bad", b"seq")])]),
        ),
        (
            "truncated frame header",
            complete[..8 + first.len() + 3].to_vec(),
        ),
        (
            "truncated frame payload",
            complete[..complete.len() - 1].to_vec(),
        ),
        ("bad range end", log(&[&first])),
        (
            "trailing commit",
            log(&[&first, &second, &batch(10, &[(3, b"", b"")])]),
        ),
        (
            "bad frame record",
            log(&[&first, &batch(9, &[(2, b"key", b"value")])]),
        ),
    ] {
        directory.seal(8, 9, &bytes);
        assert!(
            Delta::load(&directory.database, 6, &ranges).is_err(),
            "{label}"
        );
    }
    directory.seal(8, 9, &complete);
    assert!(Delta::load(&directory.database, 6, &ranges).is_ok());
    assert!(Delta::load(&directory.database, 6, &ranges[1..]).is_err());
    assert!(Delta::load(&directory.database, 6, &[]).is_err());
    assert!(Delta::load(&directory.database, 6, &[ranges[0]; MAX_DELTA_RANGES + 1]).is_err());
    assert!(Delta::load(&directory.database, u64::MAX, &ranges).is_err());
}

#[test]
fn delta_does_not_split_an_oversized_record_between_blocks() {
    let directory = TestDirectory::new();
    let large = vec![0x9f; (1 << 20) + 7];
    let entries = [
        (1, b"a".as_slice(), b"small".as_slice()),
        (1, b"b", large.as_slice()),
        (2, b"c", b""),
    ];
    directory.seal(1, 1, &log(&[&batch(1, &entries)]));
    let mut delta =
        Delta::load(&directory.database, 0, &[GoLogRange { first: 1, last: 1 }]).unwrap();
    for entry in entries {
        assert_eq!(
            delta.next_block().unwrap().unwrap(),
            batch(0, &[entry])[20..]
        );
    }
    assert!(delta.next_block().unwrap().is_none());
}

#[test]
fn compacts_only_sealed_commits_and_preserves_active_overwrite_and_delete() {
    let directory = TestDirectory::new();
    let sealed = directory.seal(
        1,
        2,
        &log(&[
            &batch(1, &[(1, b"changed", b"old"), (1, b"removed", b"present")]),
            &batch(2, &[(1, b"", b"value")]),
        ]),
    );
    let active = log(&[&batch(3, &[(1, b"changed", b"new"), (2, b"removed", b"")])]);
    directory.write_active(&active);
    assert!(directory.maintain(false).is_none());
    let first = directory.maintain(true).unwrap();
    assert!(first.snapshot);
    assert_eq!(first.cutoff, 2);
    assert_eq!(first.reclaimed_segments, 1);
    assert!(!sealed.exists());
    let snapshot = load_state(&directory.database, 2).unwrap();
    assert_eq!(snapshot[b"changed".as_slice()], b"old");
    let restored = recover(&directory.database).unwrap();
    assert_eq!(restored.last_sequence, 3);
    assert_eq!(restored.active_end, active.len() as u64);
    assert_eq!(restored.state[b"changed".as_slice()], b"new");
    assert_eq!(restored.state[b"".as_slice()], b"value");
    assert!(!restored.state.contains_key(b"removed".as_slice()));

    directory.seal(3, 3, &active);
    directory.write_active(&log(&[&batch(4, &[(3, b"", b"")])]));
    assert_eq!(directory.maintain(true).unwrap().cutoff, 3);
    assert!(!snapshot_path(&directory.database, 2).exists());
    assert!(recover(&directory.database).unwrap().state.is_empty());
}

#[test]
fn resumes_reclamation_after_manifest_was_already_published() {
    let directory = TestDirectory::new();
    let sealed = directory.seal(1, 1, &log(&[&batch(1, &[(1, b"k", b"value")])]));
    let state = State::from([(b"k".to_vec(), b"value".to_vec())]);
    write_snapshot(&snapshot_path(&directory.database, 1), 1, &state).unwrap();
    publish_cutoff(&directory.database, 1).unwrap();
    let outcome = directory.maintain(false).unwrap();
    assert!(!outcome.snapshot);
    assert_eq!(outcome.reclaimed_segments, 1);
    assert!(!sealed.exists());
    assert_eq!(recover(&directory.database).unwrap().state, state);
}

#[test]
fn rolls_retained_segments_forward_and_reclaims_them_when_retention_is_disabled() {
    let directory = TestDirectory::new();
    let first = directory.seal(1, 1, &log(&[&batch(1, &[(1, b"k", b"one")])]));
    let second = directory.seal(2, 2, &log(&[&batch(2, &[(1, b"k", b"two")])]));
    let third = directory.seal(3, 3, &log(&[&batch(3, &[(1, b"k", b"three")])]));
    let fourth = log(&[&batch(4, &[(1, b"later", b"four")])]);
    directory.write_active(&fourth);

    let result = directory.maintain_with_retention(true, 2).unwrap();
    assert!(result.snapshot);
    assert_eq!(result.cutoff, 3);
    assert_eq!(result.reclaimed_segments, 1);
    assert!(!first.exists());
    assert!(second.is_file() && third.is_file());
    assert!(directory.maintain_with_retention(false, 2).is_none());
    let restored = recover(&directory.database).unwrap();
    assert_eq!(restored.last_sequence, 4);
    assert_eq!(restored.state[b"k".as_slice()], b"three");
    assert_eq!(restored.state[b"later".as_slice()], b"four");

    let fourth_path = directory.segment(4, 4);
    fs::rename(directory.database.join(ACTIVE), &fourth_path).unwrap();
    let result = directory.maintain_with_retention(true, 2).unwrap();
    assert!(result.snapshot);
    assert_eq!(result.cutoff, 4);
    assert_eq!(result.reclaimed_segments, 1);
    assert!(!second.exists());
    assert!(!snapshot_path(&directory.database, 3).exists());
    assert!(third.is_file() && fourth_path.is_file());

    let result = directory.maintain(false).unwrap();
    assert!(!result.snapshot);
    assert_eq!(result.cutoff, 4);
    assert_eq!(result.reclaimed_segments, 2);
    assert!(!third.exists() && !fourth_path.exists());
    assert_eq!(recover(&directory.database).unwrap().state, restored.state);
    assert!(matches!(
        Maintainer::open_with_retention(
            directory.root.clone(),
            MaintenanceOptions::default(),
            MAX_RETAINED_SEGMENTS + 1,
        ),
        Err(EngineError::InvalidInput(_))
    ));
}

#[test]
fn retains_only_a_contiguous_suffix_within_the_byte_budget() {
    let segment = |first, last, bytes| Sealed {
        path: PathBuf::new(),
        first,
        last,
        bytes,
        age: Duration::ZERO,
    };

    let segments = [
        segment(1, 1, 128),
        segment(3, 4, MAX_RETAINED_BYTES - 128),
        segment(5, 6, 128),
        segment(7, 7, MAX_RETAINED_BYTES + 1),
        segment(8, 8, 128),
    ];
    let reclaimable = |cutoff, retain| {
        reclaimable_segments(&segments, cutoff, retain)
            .into_iter()
            .map(|segment| segment.last)
            .collect::<Vec<_>>()
    };
    assert_eq!(reclaimable(6, 16), [1]);
    assert_eq!(reclaimable(6, 1), [1, 4]);
    assert_eq!(reclaimable(7, 16), [1, 4, 6, 7]);
    assert_eq!(reclaimable(8, 16), [1, 4, 6, 7]);

    let over_budget = [segment(3, 4, MAX_RETAINED_BYTES), segment(5, 6, 128)];
    assert_eq!(reclaimable_segments(&over_budget, 6, 16).len(), 1);

    let gapped = [segment(1, 2, 128), segment(4, 4, 128)];
    assert_eq!(reclaimable_segments(&gapped, 4, 16).len(), 1);
    assert_eq!(reclaimable_segments(&gapped, 5, 16).len(), 2);
}

const CRASH_ROOT: &str = "KERNIVA_GO_LOG_CRASH_ROOT";
const CRASH_PHASE: &str = "KERNIVA_GO_LOG_CRASH_PHASE";
const CRASH_RETENTION: &str = "KERNIVA_GO_LOG_CRASH_RETENTION";
const CHILD_READY: &str = "child-ready";

pub(super) fn maintenance_barrier(phase: &str) {
    if std::env::var(CRASH_PHASE).as_deref() != Ok(phase) {
        return;
    }
    let root = PathBuf::from(std::env::var_os(CRASH_ROOT).unwrap());
    fs::write(root.join(CHILD_READY), []).unwrap();
    let _ = std::io::stdin().read_exact(&mut [0_u8]);
    panic!("parent should terminate the maintenance fixture at {phase}");
}

#[test]
fn maintenance_crash_child() {
    let Some(root) = std::env::var_os(CRASH_ROOT) else {
        return;
    };
    let retain = std::env::var(CRASH_RETENTION).unwrap().parse().unwrap();
    let worker =
        Maintainer::open_with_retention(PathBuf::from(root), MaintenanceOptions::default(), retain)
            .unwrap();
    worker.run_pass(true).unwrap().pop().unwrap().1.unwrap();
    panic!("maintenance fixture missed its interruption point");
}

#[test]
fn recovers_and_resumes_after_maintenance_process_termination() {
    for (phase, cutoff, removed, retained) in [
        ("snapshot-synced", 1, 0, 0),
        ("current-synced", 3, 0, 0),
        ("first-reclaim", 3, 1, 0),
        ("first-reclaim", 3, 1, 1),
    ] {
        let directory = TestDirectory::new();
        directory.seal(1, 1, &log(&[&batch(1, &[(1, b"kept", b"one")])]));
        assert_eq!(directory.maintain(true).unwrap().cutoff, 1);

        let second = log(&[&batch(
            2,
            &[(1, b"changed", b"two"), (1, b"removed", b"two")],
        )]);
        let third = log(&[&batch(3, &[(1, b"changed", b"three")])]);
        let second_path = directory.seal(2, 2, &second);
        let third_path = directory.seal(3, 3, &third);
        let suffix = log(&[&batch(4, &[(1, b"late", b"four")])]);
        directory.write_active(&suffix);

        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "golog::tests::maintenance_crash_child"])
            .env(CRASH_ROOT, &directory.root)
            .env(CRASH_PHASE, phase)
            .env(CRASH_RETENTION, retained.to_string())
            .stdin(Stdio::piped())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        while !directory.root.join(CHILD_READY).exists() {
            if let Some(status) = child.try_wait().unwrap() {
                panic!("maintenance fixture exited before {phase}: {status}");
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("maintenance fixture did not reach {phase}");
            }
            std::thread::sleep(Duration::from_millis(10));
        }

        let suffix_path = directory.segment(4, 4);
        fs::rename(directory.database.join(ACTIVE), &suffix_path).unwrap();
        let active = log(&[&batch(5, &[(1, b"changed", b"five"), (2, b"removed", b"")])]);
        directory.write_active(&active);
        child.kill().unwrap();
        assert!(!child.wait().unwrap().success(), "{phase}");

        assert_eq!(load_cutoff(&directory.database).unwrap(), cutoff, "{phase}");
        assert!(snapshot_path(&directory.database, 3).is_file(), "{phase}");
        assert_eq!(second_path.exists(), removed == 0, "{phase}");
        assert!(third_path.is_file(), "{phase}");
        assert!(snapshot_path(&directory.database, 1).is_file(), "{phase}");

        let expected = State::from([
            (b"changed".to_vec(), b"five".to_vec()),
            (b"kept".to_vec(), b"one".to_vec()),
            (b"late".to_vec(), b"four".to_vec()),
        ]);
        let assert_recovered = || {
            let restored = recover(&directory.database).unwrap();
            assert_eq!(restored.last_sequence, 5, "{phase}");
            assert_eq!(restored.active_end, active.len() as u64, "{phase}");
            assert_eq!(restored.state, expected, "{phase}");
        };
        assert_recovered();

        let options = MaintenanceOptions {
            min_sealed_bytes: (second.len() + third.len()) as u64,
            max_age: Duration::MAX,
        };
        let worker =
            Maintainer::open_with_retention(directory.root.clone(), options, retained).unwrap();
        let mut pass = worker.run_pass(false).unwrap();
        let outcome = pass.pop().unwrap().1.unwrap().unwrap();
        let republished = cutoff == 1;
        assert_eq!(outcome.snapshot, republished, "{phase}");
        assert_eq!(outcome.cutoff, if republished { 4 } else { 3 }, "{phase}");
        assert_eq!(
            outcome.reclaimed_segments,
            if republished {
                3
            } else {
                2 - removed - retained as usize
            },
            "{phase}"
        );
        assert!(!second_path.exists(), "{phase}");
        assert_eq!(third_path.exists(), retained == 1, "{phase}");
        assert_eq!(suffix_path.exists(), !republished, "{phase}");
        assert!(!snapshot_path(&directory.database, 1).exists(), "{phase}");
        assert!(
            worker
                .run_pass(false)
                .unwrap()
                .pop()
                .unwrap()
                .1
                .unwrap()
                .is_none(),
            "{phase}"
        );
        assert_recovered();
    }
}

#[test]
fn validates_snapshot_and_ignores_unpublished_cutoffs() {
    let directory = TestDirectory::new();
    directory.seal(1, 1, &log(&[&batch(1, &[(1, b"k", b"value")])]));
    fs::write(snapshot_path(&directory.database, 2), b"abandoned").unwrap();
    assert_eq!(recover(&directory.database).unwrap().last_sequence, 1);
    directory.maintain(true).unwrap();
    let path = snapshot_path(&directory.database, 1);
    let mut bytes = fs::read(&path).unwrap();
    *bytes.last_mut().unwrap() ^= 1;
    fs::write(path, bytes).unwrap();
    assert!(recover(&directory.database).is_err());
}

#[test]
fn supports_missing_and_torn_active_header_and_exhausted_sequence() {
    let directory = TestDirectory::new();
    assert_eq!(recover(&directory.database).unwrap().active_end, 0);
    directory.write_active(b"KGL");
    assert_eq!(recover(&directory.database).unwrap().active_end, 0);
    directory.write_active(b"KGLLOG00");
    assert!(recover(&directory.database).is_err());

    directory.write_active(b"KGLLOG01");
    write_snapshot(
        &snapshot_path(&directory.database, u64::MAX),
        u64::MAX,
        &State::new(),
    )
    .unwrap();
    publish_cutoff(&directory.database, u64::MAX).unwrap();
    let state = recover(&directory.database).unwrap();
    assert_eq!(state.last_sequence, u64::MAX);
    assert_eq!(state.active_end, 8);
}
