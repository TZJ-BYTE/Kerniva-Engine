use std::sync::{Arc, Barrier};
use std::thread;

use kerniva_engine::{MemoryEngine, MemoryError};

#[test]
fn shard_count_must_be_a_positive_power_of_two() {
    for shards in [0, 3, 7, usize::MAX] {
        assert!(matches!(
            MemoryEngine::new(shards, None),
            Err(MemoryError::InvalidShardCount)
        ));
    }
    for shards in [1, 2, 16, 256] {
        let engine = MemoryEngine::new(shards, None).unwrap();
        let stats = engine.stats().unwrap();
        assert_eq!(stats.shards, shards);
        assert_eq!(stats.keys, 0);
        assert_eq!(stats.payload_bytes, 0);
        assert_eq!(stats.max_payload_bytes, None);
    }
    assert!(matches!(
        MemoryEngine::new(usize::MAX / 2 + 1, None),
        Err(MemoryError::CapacityOverflow)
    ));
}

#[test]
fn keys_and_values_are_binary_and_empty_values_are_present() {
    let engine = MemoryEngine::new(4, None).unwrap();
    assert!(engine.get(b"missing").unwrap().is_none());
    assert!(!engine.delete(b"missing").unwrap());

    engine.put(b"", b"").unwrap();
    engine.put(b"\0\xff\r\n", b"\xff\0\r\n").unwrap();
    assert_eq!(engine.get(b"").unwrap().unwrap().as_ref(), b"");
    assert_eq!(
        engine.get(b"\0\xff\r\n").unwrap().unwrap().as_ref(),
        b"\xff\0\r\n"
    );
    let stats = engine.stats().unwrap();
    assert_eq!(stats.keys, 2);
    assert_eq!(stats.payload_bytes, 8);
    assert!(engine.delete(b"").unwrap());
    assert!(!engine.delete(b"").unwrap());
    assert_eq!(engine.stats().unwrap().keys, 1);
    assert_eq!(engine.stats().unwrap().payload_bytes, 8);
}

#[test]
fn put_owns_input_and_read_handles_survive_replacement_and_delete() {
    let engine = MemoryEngine::new(2, None).unwrap();
    let mut key = b"key".to_vec();
    let mut value = b"original".to_vec();
    engine.put(&key, &value).unwrap();
    key.fill(b'x');
    value.fill(b'x');
    let old = engine.get(b"key").unwrap().unwrap();
    assert_eq!(old.as_ref(), b"original");
    assert!(engine.get(b"xxx").unwrap().is_none());

    engine.put(b"key", b"replacement").unwrap();
    let replacement = engine.get(b"key").unwrap().unwrap();
    assert!(engine.delete(b"key").unwrap());
    assert!(engine.get(b"key").unwrap().is_none());
    assert_eq!(old.as_ref(), b"original");
    assert_eq!(replacement.as_ref(), b"replacement");
    assert_eq!(engine.stats().unwrap().payload_bytes, 0);
}

#[test]
fn zero_payload_limit_allows_only_an_empty_key_and_value() {
    let engine = MemoryEngine::new(1, Some(0)).unwrap();
    engine.put(b"", b"").unwrap();
    engine.put(b"", b"").unwrap();
    for (key, value) in [(b"k".as_slice(), b"".as_slice()), (b"", b"v")] {
        assert!(matches!(
            engine.put(key, value),
            Err(MemoryError::PayloadLimitExceeded)
        ));
    }
    let stats = engine.stats().unwrap();
    assert_eq!(stats.keys, 1);
    assert_eq!(stats.payload_bytes, 0);
    assert_eq!(stats.max_payload_bytes, Some(0));
    assert_eq!(engine.get(b"").unwrap().unwrap().as_ref(), b"");
}

#[test]
fn rejected_growth_preserves_values_and_replacement_charges_only_the_new_payload() {
    let engine = MemoryEngine::new(8, Some(10)).unwrap();
    engine.put(b"a", b"123").unwrap();
    engine.put(b"b", b"12345").unwrap();
    assert_eq!(engine.stats().unwrap().payload_bytes, 10);
    assert!(matches!(
        engine.put(b"a", b"1234"),
        Err(MemoryError::PayloadLimitExceeded)
    ));
    assert!(matches!(
        engine.put(b"c", b""),
        Err(MemoryError::PayloadLimitExceeded)
    ));
    assert_eq!(engine.get(b"a").unwrap().unwrap().as_ref(), b"123");
    assert_eq!(engine.get(b"b").unwrap().unwrap().as_ref(), b"12345");
    assert!(engine.get(b"c").unwrap().is_none());
    assert_eq!(engine.stats().unwrap().keys, 2);
    assert_eq!(engine.stats().unwrap().payload_bytes, 10);

    engine.put(b"b", b"123").unwrap();
    engine.put(b"a", b"12345").unwrap();
    engine.put(b"a", b"abcde").unwrap();
    assert_eq!(engine.stats().unwrap().payload_bytes, 10);
    assert!(engine.delete(b"b").unwrap());
    engine.put(b"c", b"xyz").unwrap();
    assert_eq!(engine.stats().unwrap().payload_bytes, 10);
    assert_eq!(engine.stats().unwrap().keys, 2);
}

#[test]
fn retained_read_handles_are_outside_the_resident_payload_budget() {
    let engine = MemoryEngine::new(4, Some(6)).unwrap();
    engine.put(b"a", b"12345").unwrap();
    let old = engine.get(b"a").unwrap().unwrap();
    engine.put(b"a", b"12").unwrap();
    engine.put(b"bb", b"3").unwrap();
    assert_eq!(engine.stats().unwrap().payload_bytes, 6);
    assert_eq!(old.as_ref(), b"12345");

    assert!(engine.delete(b"a").unwrap());
    engine.put(b"cc", b"4").unwrap();
    assert_eq!(engine.stats().unwrap().payload_bytes, 6);
    assert_eq!(old.as_ref(), b"12345");
}

#[test]
fn concurrent_inserts_cannot_oversubscribe_the_shared_payload_limit() {
    const WRITERS: usize = 32;
    const ENTRY_BYTES: usize = 8;
    const CAPACITY: usize = 8;
    let engine = Arc::new(MemoryEngine::new(64, Some(CAPACITY * ENTRY_BYTES)).unwrap());
    let ready = Arc::new(Barrier::new(WRITERS + 1));
    let writers: Vec<_> = (0..WRITERS)
        .map(|id| {
            let engine = Arc::clone(&engine);
            let ready = Arc::clone(&ready);
            thread::spawn(move || {
                let key = [id as u8, 0xff];
                let value = [id as u8; 6];
                ready.wait();
                (key, value, engine.put(&key, &value))
            })
        })
        .collect();
    ready.wait();
    for _ in 0..WRITERS {
        let stats = engine.stats().unwrap();
        assert!(stats.keys <= CAPACITY);
        assert_eq!(stats.payload_bytes, stats.keys * ENTRY_BYTES);
    }
    let mut inserted = Vec::new();
    for writer in writers {
        let (key, value, result) = writer.join().unwrap();
        match result {
            Ok(()) => {
                assert_eq!(engine.get(&key).unwrap().unwrap().as_ref(), value);
                inserted.push(key);
            }
            Err(MemoryError::PayloadLimitExceeded) => {
                assert!(engine.get(&key).unwrap().is_none());
            }
            Err(error) => panic!("unexpected concurrent write error: {error:?}"),
        }
    }
    assert_eq!(inserted.len(), CAPACITY);
    assert_eq!(engine.stats().unwrap().keys, CAPACITY);
    assert_eq!(
        engine.stats().unwrap().payload_bytes,
        CAPACITY * ENTRY_BYTES
    );

    for key in inserted {
        assert!(engine.delete(&key).unwrap());
    }
    assert_eq!(engine.stats().unwrap().keys, 0);
    assert_eq!(engine.stats().unwrap().payload_bytes, 0);
    engine.put(b"x", &[0; CAPACITY * ENTRY_BYTES - 1]).unwrap();
    assert_eq!(
        engine.stats().unwrap().payload_bytes,
        CAPACITY * ENTRY_BYTES
    );
}

#[test]
fn concurrent_replacement_exposes_whole_immutable_values_and_keeps_accounting_exact() {
    const WRITERS: usize = 4;
    const VALUE_BYTES: usize = 128;
    let engine = Arc::new(MemoryEngine::new(16, Some(1 + VALUE_BYTES)).unwrap());
    engine.put(b"k", &[0; VALUE_BYTES]).unwrap();
    let ready = Arc::new(Barrier::new(WRITERS + 1));
    let writers: Vec<_> = (1..=WRITERS)
        .map(|id| {
            let engine = Arc::clone(&engine);
            let ready = Arc::clone(&ready);
            thread::spawn(move || {
                ready.wait();
                for _ in 0..500 {
                    engine.put(b"k", &[id as u8; VALUE_BYTES]).unwrap();
                }
            })
        })
        .collect();
    ready.wait();
    for _ in 0..2_000 {
        let value = engine.get(b"k").unwrap().unwrap();
        assert_eq!(value.len(), VALUE_BYTES);
        assert!(value.iter().all(|byte| *byte == value[0]));
        assert!(value[0] <= WRITERS as u8);
    }
    for writer in writers {
        writer.join().unwrap();
    }
    let stats = engine.stats().unwrap();
    assert_eq!(stats.keys, 1);
    assert_eq!(stats.payload_bytes, 1 + VALUE_BYTES);
}
