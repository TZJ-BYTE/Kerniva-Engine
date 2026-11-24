package database

import (
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/TZJ-BYTE/Kerniva/config"
)

func TestGoLogNewKeyFailurePreservesConcurrentCommit(t *testing.T) {
	db, cfg := openGroupedGoLogTestDB(t, 4096)
	failedKey := "failing-new-key"
	otherKey := samePersistentShard(failedKey)
	watchKeyForTest(t, db, failedKey)
	watchKeyForTest(t, db, otherKey)
	const otherValue = "committed"
	injected := errors.New("injected new-key commit failure")
	barrier := blockPersistentKey(t, db, failedKey, injected)
	failed := make(chan error, 1)
	go func() { failed <- db.SetStringBytes([]byte(failedKey), []byte("private")) }()
	waitPersistentSignal(t, barrier.entered)

	used := atomic.LoadInt64(&db.usedMemory)
	if reserved := db.pendingMemory.Load(); reserved == 0 || used != reserved || used > db.maxMemory {
		t.Fatalf("pending reservation = %d, accounted = %d, limit = %d", reserved, used, db.maxMemory)
	}
	if db.KeyVersion(failedKey) != 0 {
		t.Fatal("pending new key changed the WATCH version")
	}
	other := make(chan error, 1)
	go func() { other <- db.SetStringBytes([]byte(otherKey), []byte(otherValue)) }()
	if err := waitPersistentError(t, other); err != nil {
		t.Fatal(err)
	}
	requireString(t, db, otherKey, otherValue)
	changed := db.ChangeChannel()

	read := make(chan error, 1)
	go func() {
		value, exists, err := db.GetStringCopy([]byte(failedKey))
		if err == nil && exists {
			err = fmt.Errorf("failed new key returned %q", value)
		}
		read <- err
	}()
	select {
	case err := <-read:
		t.Fatalf("same-key GET completed before the pending SET: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	barrier.unblock()
	if err := waitPersistentError(t, failed); !errors.Is(err, injected) {
		t.Fatalf("new-key write error = %v, want %v", err, injected)
	}
	if err := waitPersistentError(t, read); err != nil {
		t.Fatal(err)
	}
	select {
	case <-changed:
		t.Fatal("failed new key notified a database change")
	default:
	}
	if db.KeyVersion(failedKey) != 0 || db.KeyVersion(otherKey) != 1 {
		t.Fatal("new-key writes changed the wrong WATCH versions")
	}
	wantUsed := int64(len(otherKey) + len(otherValue) + 24)
	if pending, used := db.pendingMemory.Load(), atomic.LoadInt64(&db.usedMemory); pending != 0 || used != wantUsed {
		t.Fatalf("pending memory = %d, used = %d, want 0 and %d", pending, used, wantUsed)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewDatabaseWithConfig(0, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	requireString(t, reopened, otherKey, otherValue)
	if value, exists, err := reopened.GetStringCopy([]byte(failedKey)); exists || err != nil {
		t.Fatalf("reopened failed new key = %q, %t, %v", value, exists, err)
	}
}

func TestGoLogNewKeyBudgetFallsBackAndRejectsOverflow(t *testing.T) {
	const key, value = "key", "value"
	const budget = len(key) + len(value) + 24
	db, _ := openGroupedGoLogTestDB(t, int64(budget))
	if err := db.SetStringBytes([]byte(key), []byte(value)); err != nil {
		t.Fatalf("new key that fits resident memory failed: %v", err)
	}
	before := db.goLog.Stats()
	watchKeyForTest(t, db, "overflow")
	if err := db.SetStringBytes([]byte("overflow"), []byte("value")); err == nil || !strings.Contains(err.Error(), "OOM") {
		t.Fatalf("new key beyond resident limit returned %v", err)
	}
	requireString(t, db, key, value)
	if result, exists, err := db.GetStringCopy([]byte("overflow")); exists || err != nil {
		t.Fatalf("overflow key returned %q, %t, %v", result, exists, err)
	}
	if db.goLog.Stats().LastSequence != before.LastSequence || db.KeyVersion("overflow") != 0 ||
		db.pendingMemory.Load() != 0 || atomic.LoadInt64(&db.usedMemory) != int64(budget) {
		t.Fatal("rejected new key changed the log, WATCH version or memory budget")
	}
}

func TestGoLogMissingReadsDuringPendingCommit(t *testing.T) {
	db, _ := openGroupedGoLogTestDB(t, 4096)
	pendingKey := "pending"
	missing := samePersistentShard(pendingKey)
	watchKeyForTest(t, db, missing)
	barrier := blockPersistentKey(t, db, pendingKey, nil)
	write := make(chan error, 1)
	go func() { write <- db.SetStringBytes([]byte(pendingKey), []byte("committed")) }()
	waitPersistentSignal(t, barrier.entered)

	before := db.goLog.Stats()
	used := atomic.LoadInt64(&db.usedMemory)
	changed := db.ChangeChannel()
	read := make(chan error, 1)
	go func() {
		key := []byte(missing)
		if value, exists := db.Get(missing); value != nil || exists {
			read <- fmt.Errorf("Get returned %v, %t", value, exists)
			return
		}
		if value, exists := db.GetBytes(key); value != nil || exists {
			read <- fmt.Errorf("GetBytes returned %v, %t", value, exists)
			return
		}
		if value, exists, err := db.GetStringCopy(key); value != "" || exists || err != nil {
			read <- fmt.Errorf("GetStringCopy returned %q, %t, %v", value, exists, err)
			return
		}
		if value, exists, err := db.GetStringBytesCopy(key); value != nil || exists || err != nil {
			read <- fmt.Errorf("GetStringBytesCopy returned %q, %t, %v", value, exists, err)
			return
		}
		read <- nil
	}()
	if err := waitPersistentError(t, read); err != nil {
		t.Fatal(err)
	}
	if db.goLog.Stats().LastSequence != before.LastSequence || db.KeyVersion(missing) != 0 || atomic.LoadInt64(&db.usedMemory) != used {
		t.Fatal("missing reads changed the log, WATCH version or memory accounting")
	}
	select {
	case <-changed:
		t.Fatal("missing reads notified a database change")
	default:
	}
	barrier.unblock()
	if err := waitPersistentError(t, write); err != nil {
		t.Fatal(err)
	}
	requireString(t, db, pendingKey, "committed")
}

func TestGoLogTTLReadRetainsExclusiveFallbackAcrossDeadline(t *testing.T) {
	db, cfg := openGroupedGoLogTestDB(t, 4096)
	const key = "ttl"
	if set, err := db.SetStringBytesWithOptions([]byte(key), []byte("value"), 60_000, false, false, false); !set || err != nil {
		t.Fatalf("SET with TTL = %t, %v", set, err)
	}
	before := db.goLog.Stats()
	version := watchKeyForTest(t, db, key)
	parallel := db.beginKeyOperation(key, false)
	if parallel {
		db.endKeyOperation(key, false, parallel)
		t.Fatal("Go-log TTL key entered shared lookup")
	}
	// Simulate the deadline passing between lock selection and the actual lookup.
	db.shards[getShardIndex(key)].data[key].ExpireTime = time.Now().Add(-time.Second).UnixMilli()
	value, exists, err := db.getStringBytesCopyLocked([]byte(key))
	db.endKeyOperation(key, false, parallel)
	if value != nil || exists || err != nil {
		t.Fatalf("expired lookup = %q, %t, %v", value, exists, err)
	}
	if db.goLog.Stats().LastSequence != before.LastSequence+1 || db.KeyVersion(key) != version+1 {
		t.Fatal("expired lookup did not append a deletion and update WATCH")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewDatabaseWithConfig(0, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	if value, exists, err := reopened.GetStringBytesCopy([]byte(key)); value != nil || exists || err != nil {
		t.Fatalf("reopened expired lookup = %q, %t, %v", value, exists, err)
	}
}

func openGroupedGoLogTestDB(t *testing.T, budget int64) (*Database, *DatabaseConfig) {
	t.Helper()
	cfg := &DatabaseConfig{
		Type: Persistent, StorageBackend: config.StorageGoLog, DataDir: t.TempDir(),
		ColdStartStrategy: "load_all", Durability: "sync", MaxMemory: budget,
		MaxMemoryPolicy: "noeviction", GoLogGroupMax: 8,
	}
	db, err := NewDatabaseWithConfig(0, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, cfg
}
