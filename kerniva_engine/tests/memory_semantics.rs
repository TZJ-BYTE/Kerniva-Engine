use std::sync::{Arc, Barrier};
use std::thread;

use kerniva_engine::{
    MemoryBatchOp, MemoryCondition, MemoryEngine, MemoryError, MemoryWriteOptions,
};

fn options(condition: MemoryCondition, expires_at_ms: Option<u64>) -> MemoryWriteOptions {
    MemoryWriteOptions {
        condition,
        expires_at_ms,
    }
}

fn put_at(engine: &MemoryEngine, key: &[u8], value: &[u8], expires: Option<u64>, now: u64) -> u64 {
    engine
        .put_with_at(key, value, options(MemoryCondition::Any, expires), now)
        .unwrap()
        .expect("unconditional write must succeed")
}

#[test]
fn expiration_is_inclusive_and_reading_does_not_reclaim_resident_payload() {
    let engine = MemoryEngine::new(4, None).unwrap();
    let version = put_at(&engine, b"key", b"value", Some(200), 100);
    let live = engine.get_entry_at(b"key", 199).unwrap().unwrap();
    assert_eq!(live.value.as_ref(), b"value");
    assert_eq!(live.version, version);
    assert_eq!(live.expires_at_ms, Some(200));
    assert!(engine.get_entry_at(b"key", 200).unwrap().is_none());
    assert!(engine.get_entry_at(b"key", 201).unwrap().is_none());
    assert_eq!(engine.stats().unwrap().keys, 1);
    assert_eq!(engine.stats().unwrap().payload_bytes, 8);
    assert_eq!(live.value.as_ref(), b"value");

    put_at(&engine, b"empty", b"", Some(300), 200);
    assert!(engine.get_entry_at(b"empty", 299).unwrap().is_some());
    assert!(engine.get_entry_at(b"empty", 300).unwrap().is_none());
}

#[test]
fn invalid_expiration_preserves_the_previous_value_version_and_deadline() {
    let engine = MemoryEngine::new(2, Some(16)).unwrap();
    let version = put_at(&engine, b"k", b"old", Some(500), 100);
    for deadline in [0, 99, 100] {
        assert!(matches!(
            engine.put_with_at(
                b"k",
                b"new",
                options(MemoryCondition::Any, Some(deadline)),
                100
            ),
            Err(MemoryError::InvalidExpiration)
        ));
        let entry = engine.get_entry_at(b"k", 100).unwrap().unwrap();
        assert_eq!(entry.value.as_ref(), b"old");
        assert_eq!(entry.version, version);
        assert_eq!(entry.expires_at_ms, Some(500));
    }
    assert_eq!(engine.stats().unwrap().payload_bytes, 4);
}

#[test]
fn purge_removes_only_expired_entries_and_returns_budget_without_invalidating_readers() {
    let engine = MemoryEngine::new(8, Some(17)).unwrap();
    put_at(&engine, b"a", b"alpha", Some(200), 100);
    put_at(&engine, b"b", b"beta", Some(300), 100);
    put_at(&engine, b"c", b"gamma", None, 100);
    let retained = engine.get_entry_at(b"a", 100).unwrap().unwrap();
    assert_eq!(engine.purge_expired_at(199).unwrap(), 0);
    assert_eq!(engine.purge_expired_at(200).unwrap(), 1);
    assert_eq!(engine.purge_expired_at(200).unwrap(), 0);
    assert_eq!(engine.stats().unwrap().keys, 2);
    assert_eq!(engine.stats().unwrap().payload_bytes, 11);
    assert_eq!(engine.purge_expired_at(300).unwrap(), 1);
    assert_eq!(engine.stats().unwrap().payload_bytes, 6);
    assert_eq!(
        engine
            .get_entry_at(b"c", u64::MAX)
            .unwrap()
            .unwrap()
            .value
            .as_ref(),
        b"gamma"
    );
    assert_eq!(retained.value.as_ref(), b"alpha");

    put_at(&engine, b"d", &[7; 10], None, 300);
    assert_eq!(engine.stats().unwrap().payload_bytes, 17);
}

#[test]
fn missing_and_present_conditions_use_logical_expiration() {
    let engine = MemoryEngine::new(4, None).unwrap();
    assert!(
        engine
            .put_with_at(b"k", b"x", options(MemoryCondition::Present, None), 100)
            .unwrap()
            .is_none()
    );
    let first = engine
        .put_with_at(
            b"k",
            b"old",
            options(MemoryCondition::Missing, Some(200)),
            100,
        )
        .unwrap()
        .unwrap();
    assert!(
        engine
            .put_with_at(b"k", b"x", options(MemoryCondition::Missing, None), 100)
            .unwrap()
            .is_none()
    );
    let second = engine
        .put_with_at(
            b"k",
            b"live",
            options(MemoryCondition::Present, Some(200)),
            100,
        )
        .unwrap()
        .unwrap();
    assert_ne!(first, second);
    assert!(
        engine
            .put_with_at(b"k", b"x", options(MemoryCondition::Present, None), 200)
            .unwrap()
            .is_none()
    );
    assert!(
        engine
            .put_with_at(
                b"k",
                b"x",
                options(MemoryCondition::Version(second), None),
                200
            )
            .unwrap()
            .is_none()
    );
    let renewed = engine
        .put_with_at(
            b"k",
            b"renewed",
            options(MemoryCondition::Missing, None),
            200,
        )
        .unwrap()
        .unwrap();
    assert_ne!(second, renewed);
    let entry = engine.get_entry_at(b"k", 200).unwrap().unwrap();
    assert_eq!(entry.value.as_ref(), b"renewed");
    assert_eq!(entry.expires_at_ms, None);
    assert_eq!(engine.stats().unwrap().keys, 1);
    assert_eq!(engine.stats().unwrap().payload_bytes, 8);
}

#[test]
fn cas_and_budget_failures_preserve_live_state() {
    let engine = MemoryEngine::new(4, Some(6)).unwrap();
    let initial = put_at(&engine, b"k", b"old", Some(500), 100);
    assert!(
        engine
            .put_with_at(
                b"k",
                b"bad",
                options(MemoryCondition::Version(initial.wrapping_add(1)), Some(600)),
                100
            )
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        engine.put_with_at(
            b"k",
            b"toolarge",
            options(MemoryCondition::Version(initial), Some(600)),
            100
        ),
        Err(MemoryError::PayloadLimitExceeded)
    ));
    let unchanged = engine.get_entry_at(b"k", 100).unwrap().unwrap();
    assert_eq!(unchanged.value.as_ref(), b"old");
    assert_eq!(unchanged.version, initial);
    assert_eq!(unchanged.expires_at_ms, Some(500));
    assert_eq!(engine.stats().unwrap().payload_bytes, 4);

    let updated = engine
        .put_with_at(
            b"k",
            b"newer",
            options(MemoryCondition::Version(initial), Some(600)),
            100,
        )
        .unwrap()
        .unwrap();
    assert_ne!(updated, initial);
    assert!(
        engine
            .put_with_at(
                b"k",
                b"bad",
                options(MemoryCondition::Version(initial), None),
                100
            )
            .unwrap()
            .is_none()
    );
    assert_eq!(engine.stats().unwrap().payload_bytes, 6);
}

#[test]
fn versions_do_not_match_after_delete_or_expire_and_recreate() {
    let engine = MemoryEngine::new(4, None).unwrap();
    let first = put_at(&engine, b"k", b"first", None, 100);
    assert!(engine.delete_at(b"k", 100).unwrap());
    assert!(!engine.delete_at(b"k", 100).unwrap());
    let second = put_at(&engine, b"k", b"second", Some(200), 100);
    assert_ne!(first, second);
    assert!(
        engine
            .put_with_at(
                b"k",
                b"bad",
                options(MemoryCondition::Version(first), None),
                100
            )
            .unwrap()
            .is_none()
    );

    let third = engine
        .put_with_at(b"k", b"third", options(MemoryCondition::Missing, None), 200)
        .unwrap()
        .unwrap();
    assert_ne!(second, third);
    assert!(
        engine
            .put_with_at(
                b"k",
                b"bad",
                options(MemoryCondition::Version(second), None),
                200
            )
            .unwrap()
            .is_none()
    );
    assert_eq!(
        engine
            .get_entry_at(b"k", 200)
            .unwrap()
            .unwrap()
            .value
            .as_ref(),
        b"third"
    );
}

#[test]
fn default_get_checks_wall_time_and_delete_at_reclaims_expired_payload() {
    let engine = MemoryEngine::new(4, Some(12)).unwrap();
    put_at(&engine, b"old", b"x", Some(1), 0);
    put_at(&engine, b"live", b"y", None, 0);
    assert!(engine.get(b"old").unwrap().is_none());
    assert_eq!(engine.get(b"live").unwrap().unwrap().as_ref(), b"y");
    assert_eq!(engine.stats().unwrap().payload_bytes, 9);
    engine.delete_at(b"old", 1).unwrap();
    assert_eq!(engine.stats().unwrap().payload_bytes, 5);
    assert_eq!(engine.stats().unwrap().keys, 1);
    assert!(!engine.delete_at(b"old", 1).unwrap());
}

#[test]
fn only_one_concurrent_cas_can_replace_the_same_version() {
    const WRITERS: usize = 16;
    let engine = Arc::new(MemoryEngine::new(16, None).unwrap());
    let initial = put_at(&engine, b"k", b"old", None, 100);
    let start = Arc::new(Barrier::new(WRITERS));
    let handles: Vec<_> = (0..WRITERS)
        .map(|id| {
            let engine = Arc::clone(&engine);
            let start = Arc::clone(&start);
            thread::spawn(move || {
                start.wait();
                (
                    id as u8,
                    engine
                        .put_with_at(
                            b"k",
                            &[id as u8],
                            options(MemoryCondition::Version(initial), None),
                            100,
                        )
                        .unwrap(),
                )
            })
        })
        .collect();
    let winners: Vec<_> = handles
        .into_iter()
        .filter_map(|handle| {
            let (value, version) = handle.join().unwrap();
            version.map(|version| (value, version))
        })
        .collect();
    assert_eq!(winners.len(), 1);
    let entry = engine.get_entry_at(b"k", 100).unwrap().unwrap();
    assert_eq!(entry.value.as_ref(), &[winners[0].0]);
    assert_eq!(entry.version, winners[0].1);
    assert_eq!(engine.stats().unwrap().payload_bytes, 2);
}

#[test]
fn batch_condition_and_budget_failures_leave_no_partial_changes_or_reservations() {
    let engine = MemoryEngine::new(8, Some(10)).unwrap();
    let a = put_at(&engine, b"a", b"aa", Some(500), 100);
    let b = put_at(&engine, b"b", b"bb", None, 100);
    let conditional = [
        MemoryBatchOp::Put {
            key: b"a",
            value: b"11",
            options: options(MemoryCondition::Any, None),
        },
        MemoryBatchOp::Delete {
            key: b"b",
            condition: MemoryCondition::Version(b.wrapping_add(1)),
        },
    ];
    assert!(!engine.apply_batch_at(&conditional, 100).unwrap());
    let too_large = [
        MemoryBatchOp::Put {
            key: b"b",
            value: b"cc",
            options: options(MemoryCondition::Any, None),
        },
        MemoryBatchOp::Put {
            key: b"a",
            value: b"12345678",
            options: options(MemoryCondition::Any, None),
        },
    ];
    assert!(matches!(
        engine.apply_batch_at(&too_large, 100),
        Err(MemoryError::PayloadLimitExceeded)
    ));
    let a_after = engine.get_entry_at(b"a", 100).unwrap().unwrap();
    let b_after = engine.get_entry_at(b"b", 100).unwrap().unwrap();
    assert_eq!(a_after.value.as_ref(), b"aa");
    assert_eq!(a_after.version, a);
    assert_eq!(a_after.expires_at_ms, Some(500));
    assert_eq!(b_after.value.as_ref(), b"bb");
    assert_eq!(b_after.version, b);
    assert_eq!(engine.stats().unwrap().payload_bytes, 6);
    put_at(&engine, b"c", b"123", None, 100);
    assert_eq!(engine.stats().unwrap().payload_bytes, 10);
}

#[test]
fn batch_checks_final_net_budget_rather_than_intermediate_operation_sizes() {
    let engine = MemoryEngine::new(16, Some(8)).unwrap();
    put_at(&engine, b"a", b"1234567", None, 100);
    let retained = engine.get_entry_at(b"a", 100).unwrap().unwrap();
    let operations = [
        MemoryBatchOp::Put {
            key: b"b",
            value: b"7654321",
            options: options(MemoryCondition::Missing, None),
        },
        MemoryBatchOp::Delete {
            key: b"a",
            condition: MemoryCondition::Present,
        },
    ];
    assert!(engine.apply_batch_at(&operations, 100).unwrap());
    assert!(engine.get_entry_at(b"a", 100).unwrap().is_none());
    assert_eq!(
        engine
            .get_entry_at(b"b", 100)
            .unwrap()
            .unwrap()
            .value
            .as_ref(),
        b"7654321"
    );
    assert_eq!(engine.stats().unwrap().keys, 1);
    assert_eq!(engine.stats().unwrap().payload_bytes, 8);
    assert_eq!(retained.value.as_ref(), b"1234567");
}

#[test]
fn duplicate_batch_keys_and_conditions_are_evaluated_in_operation_order() {
    let engine = MemoryEngine::new(4, Some(5)).unwrap();
    let original = put_at(&engine, b"k", b"old", None, 100);
    let operations = [
        MemoryBatchOp::Put {
            key: b"k",
            value: b"x",
            options: options(MemoryCondition::Present, Some(500)),
        },
        MemoryBatchOp::Delete {
            key: b"k",
            condition: MemoryCondition::Present,
        },
        MemoryBatchOp::Put {
            key: b"k",
            value: b"last",
            options: options(MemoryCondition::Missing, None),
        },
    ];
    assert!(engine.apply_batch_at(&operations, 100).unwrap());
    let committed = engine.get_entry_at(b"k", 100).unwrap().unwrap();
    assert_eq!(committed.value.as_ref(), b"last");
    assert_eq!(committed.expires_at_ms, None);
    assert_ne!(committed.version, original);
    assert_eq!(engine.stats().unwrap().payload_bytes, 5);

    let rejected = [
        MemoryBatchOp::Delete {
            key: b"k",
            condition: MemoryCondition::Version(committed.version),
        },
        MemoryBatchOp::Put {
            key: b"k",
            value: b"x",
            options: options(MemoryCondition::Present, None),
        },
    ];
    assert!(!engine.apply_batch_at(&rejected, 100).unwrap());
    assert_eq!(
        engine.get_entry_at(b"k", 100).unwrap().unwrap().version,
        committed.version
    );
    assert_eq!(engine.stats().unwrap().payload_bytes, 5);
    assert!(engine.apply_batch_at(&[], 100).unwrap());
}

#[test]
fn invalid_expiration_in_later_batch_operation_rolls_back_earlier_operations() {
    let engine = MemoryEngine::new(4, None).unwrap();
    let original = put_at(&engine, b"a", b"old", None, 100);
    let operations = [
        MemoryBatchOp::Put {
            key: b"a",
            value: b"new",
            options: options(MemoryCondition::Any, None),
        },
        MemoryBatchOp::Put {
            key: b"b",
            value: b"bad",
            options: options(MemoryCondition::Any, Some(100)),
        },
    ];
    assert!(matches!(
        engine.apply_batch_at(&operations, 100),
        Err(MemoryError::InvalidExpiration)
    ));
    let entry = engine.get_entry_at(b"a", 100).unwrap().unwrap();
    assert_eq!(entry.value.as_ref(), b"old");
    assert_eq!(entry.version, original);
    assert!(engine.get_entry_at(b"b", 100).unwrap().is_none());
    assert_eq!(engine.stats().unwrap().payload_bytes, 4);
}

#[test]
fn get_many_preserves_request_order_duplicates_and_expiration() {
    let engine = MemoryEngine::new(8, None).unwrap();
    let a_version = put_at(&engine, b"a", b"alpha", None, 100);
    put_at(&engine, b"b", b"beta", Some(200), 100);
    let entries = engine
        .get_many_at(&[b"b", b"a", b"missing", b"a"], 200)
        .unwrap();
    assert_eq!(entries.len(), 4);
    assert!(entries[0].is_none());
    assert_eq!(entries[1].as_ref().unwrap().value.as_ref(), b"alpha");
    assert_eq!(entries[1].as_ref().unwrap().version, a_version);
    assert!(entries[2].is_none());
    assert_eq!(entries[3].as_ref().unwrap().version, a_version);
    assert!(engine.get_many_at(&[], 200).unwrap().is_empty());
}

#[test]
fn concurrent_batch_readers_never_observe_a_partially_published_generation() {
    const KEYS: usize = 32;
    let engine = Arc::new(MemoryEngine::new(16, None).unwrap());
    let keys: Vec<_> = (0..KEYS)
        .map(|id| format!("batch-key-{id}").into_bytes())
        .collect();
    for key in &keys {
        put_at(&engine, key, b"a", None, 100);
    }
    let start = Barrier::new(2);
    thread::scope(|scope| {
        let writer = scope.spawn(|| {
            start.wait();
            for generation in 0..500 {
                let value = [b'a' + (generation % 2) as u8];
                let operations: Vec<_> = keys
                    .iter()
                    .map(|key| MemoryBatchOp::Put {
                        key,
                        value: &value,
                        options: options(MemoryCondition::Any, None),
                    })
                    .collect();
                assert!(engine.apply_batch_at(&operations, 100).unwrap());
            }
        });
        let refs: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();
        start.wait();
        for _ in 0..1_000 {
            let entries = engine.get_many_at(&refs, 100).unwrap();
            assert_eq!(entries.len(), KEYS);
            let first = entries[0].as_ref().unwrap().value[0];
            assert!(first == b'a' || first == b'b');
            for entry in entries {
                assert_eq!(entry.unwrap().value.as_ref(), &[first]);
            }
        }
        writer.join().unwrap();
    });
}

#[test]
fn concurrent_expiration_cleanup_cannot_delete_a_renewed_value() {
    let engine = MemoryEngine::new(8, Some(5)).unwrap();
    for _ in 0..32 {
        put_at(&engine, b"k", b"old", Some(200), 100);
        let start = Barrier::new(2);
        thread::scope(|scope| {
            let cleanup = scope.spawn(|| {
                start.wait();
                engine.purge_expired_at(200).unwrap();
            });
            start.wait();
            put_at(&engine, b"k", b"new!", Some(300), 200);
            cleanup.join().unwrap();
        });
        let renewed = engine.get_entry_at(b"k", 200).unwrap().unwrap();
        assert_eq!(renewed.value.as_ref(), b"new!");
        assert_eq!(renewed.expires_at_ms, Some(300));
        assert_eq!(engine.stats().unwrap().keys, 1);
        assert_eq!(engine.stats().unwrap().payload_bytes, 5);
    }
}
