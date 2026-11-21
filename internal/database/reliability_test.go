package database

import (
	"bytes"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/TZJ-BYTE/Kerniva/config"
	"github.com/TZJ-BYTE/Kerniva/internal/datastruct"
)

func persistentTestDB(t *testing.T, budget int64) (*Database, *DatabaseConfig) {
	t.Helper()
	cfg := &DatabaseConfig{Type: Persistent, DataDir: t.TempDir(), ColdStartStrategy: "lazy_load", Durability: "sync", MaxMemory: budget, MaxMemoryPolicy: "noeviction"}
	db, err := NewDatabaseWithConfig(0, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, cfg
}

func requireString(t *testing.T, db *Database, key, expected string) {
	t.Helper()
	got, found, err := db.GetStringCopy([]byte(key))
	if err != nil || !found || got != expected {
		t.Fatalf("GET %q = %q, %v, %v, want %q", key, got, found, err, expected)
	}
}

func TestFailedCommitRestoresCacheAndVersions(t *testing.T) {
	db, _ := persistentTestDB(t, 4096)
	if err := db.SetStringBytes([]byte("a"), []byte("old")); err != nil {
		t.Fatal(err)
	}
	version := db.KeyVersion("a")
	if err := db.rustEngine.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.MSetStringBytes([][]byte{[]byte("a"), []byte("new"), []byte("b"), []byte("new")}); err == nil {
		t.Fatal("closed engine accepted batch")
	}
	requireString(t, db, "a", "old")
	if db.KeyVersion("a") != version {
		t.Fatal("failed commit changed WATCH version")
	}
	if db.shards[getShardIndex("b")].data["b"] != nil {
		t.Fatal("failed commit published new key")
	}
	if err := db.Clear(); err == nil {
		t.Fatal("closed engine accepted clear")
	}
	requireString(t, db, "a", "old")
}

func TestMemoryBudgetConcurrentAdmissionAndAtomicBatch(t *testing.T) {
	db, err := NewDatabaseWithConfig(0, &DatabaseConfig{MaxMemory: 2048, MaxMemoryPolicy: "noeviction"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var accepted atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if db.SetStringBytes([]byte(fmt.Sprintf("k%02d", i)), bytes.Repeat([]byte("x"), 128)) == nil {
				accepted.Add(1)
			}
		}(i)
	}
	wg.Wait()
	used := atomic.LoadInt64(&db.usedMemory)
	if used > 2048 || used < 0 || accepted.Load() == 0 {
		t.Fatalf("memory=%d accepted=%d", used, accepted.Load())
	}
	if err := db.MSetStringBytes([][]byte{[]byte("a"), []byte("first"), []byte("b"), bytes.Repeat([]byte("z"), 4096)}); err == nil {
		t.Fatal("oversized MSET succeeded")
	}
	if db.Exists("a") || db.Exists("b") {
		t.Fatal("failed MSET published a partial batch")
	}
	if atomic.LoadInt64(&db.usedMemory) != used {
		t.Fatal("failed MSET changed memory accounting")
	}
}

func TestPersistentCacheBoundedAcrossRestart(t *testing.T) {
	db, cfg := persistentTestDB(t, 96)
	for i := 0; i < 24; i++ {
		if err := db.SetStringBytes([]byte(fmt.Sprint(i)), []byte("01234567890123456789012345678901")); err != nil {
			t.Fatal(err)
		}
	}
	if db.Size() != 24 {
		t.Fatalf("keys=%d", db.Size())
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewDatabaseWithConfig(0, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	for i := 0; i < 24; i++ {
		requireString(t, reopened, fmt.Sprint(i), "01234567890123456789012345678901")
		if atomic.LoadInt64(&reopened.usedMemory) > cfg.MaxMemory {
			t.Fatal("read cache exceeds budget")
		}
	}
	if err := reopened.Clear(); err != nil {
		t.Fatal(err)
	}
	if reopened.Size() != 0 {
		t.Fatal("clear left persisted keys")
	}
}

func TestEmptyKeyExpiryAndUnrelatedWatchVersion(t *testing.T) {
	db := NewDatabase(0)
	defer db.Close()
	if err := db.Set("", &datastruct.DataValue{Value: &datastruct.String{Data: "expired"}, ExpireTime: time.Now().Add(-time.Second).UnixMilli()}); err != nil {
		t.Fatal(err)
	}
	db.expireSample(time.Now().UnixMilli(), 8192)
	if db.shards[getShardIndex("")].data[""] != nil {
		t.Fatal("empty key was skipped by expiry")
	}
	before := db.ModEpoch()
	if err := db.SetStringBytes([]byte("other"), []byte("x")); err != nil {
		t.Fatal(err)
	}
	if db.ModEpoch() != before {
		t.Fatal("single-key mutation changed whole-DB epoch")
	}
	value, _ := db.Get("other")
	value.Value.(*datastruct.BytesString).Data[0] = 'y'
	requireString(t, db, "other", "x")
}

func TestManagerUsesConfiguredBudgetAndPropagatesOpenErrors(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.PersistenceEnabled = false
	cfg.DBCount = 1
	cfg.MaxMemory = 4096
	cfg.MaxMemoryPolicy = "allkeys-random"
	manager, err := NewDBManagerChecked(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	db := manager.GetDefaultDB()
	if db.maxMemory != 4096 || db.evictionPolicy != "allkeys-random" {
		t.Fatal("configured budget or policy was ignored")
	}
	persistent, settings := persistentTestDB(t, 4096)
	_ = persistent
	cfg.PersistenceEnabled = true
	cfg.DataDir = settings.DataDir
	// The manager uses db_0, so hold that exact directory open first.
	settings.DataDir = cfg.DataDir + "/db_0"
	held, err := NewDatabaseWithConfig(0, settings)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	if opened, err := NewDBManagerChecked(cfg); err == nil {
		opened.Close()
		t.Fatal("manager silently accepted an engine lock failure")
	}
}

func TestIntegerOverflowPreservesValue(t *testing.T) {
	db := NewDatabase(0)
	defer db.Close()
	if err := db.SetStringBytes([]byte("max"), []byte("9223372036854775807")); err != nil {
		t.Fatal(err)
	}
	if _, err := db.IncrStringBytes([]byte("max")); err == nil {
		t.Fatal("overflow accepted")
	}
	requireString(t, db, "max", "9223372036854775807")
	if err := db.SetStringBytes([]byte("negative"), []byte("-1")); err != nil {
		t.Fatal(err)
	}
	if got, err := db.DecrByStringBytes([]byte("negative"), math.MinInt64); err != nil || got != math.MaxInt64 {
		t.Fatalf("DECRBY MinInt64 = %d, %v", got, err)
	}
}

func TestConcurrentMemoryReadsOwnTheirBuffers(t *testing.T) {
	db := NewDatabase(0)
	defer db.Close()
	var wg sync.WaitGroup
	for worker := 0; worker < 16; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			key := []byte(fmt.Sprintf("key-%d", worker%4))
			for iteration := 0; iteration < 1000; iteration++ {
				if err := db.SetStringBytes(key, []byte("01234567890123456789012345678901")); err != nil {
					t.Error(err)
					return
				}
				value, found, err := db.GetStringBytesCopy(key)
				if err != nil || !found || len(value) != 32 {
					t.Errorf("concurrent GET = %q, %v, %v", value, found, err)
					return
				}
				value[0] = 'x'
			}
		}(worker)
	}
	wg.Wait()
	requireString(t, db, "key-0", "01234567890123456789012345678901")
}

func TestEvictionPreservesAcceptedWriteAndRollsBackOversizedBatch(t *testing.T) {
	db, err := NewDatabaseWithConfig(0, &DatabaseConfig{MaxMemory: 96, MaxMemoryPolicy: "allkeys-random"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.SetStringBytes([]byte("old"), []byte("01234567890123456789012345678901")); err != nil {
		t.Fatal(err)
	}
	if err := db.SetStringBytes([]byte("new"), []byte("01234567890123456789012345678901")); err != nil {
		t.Fatal(err)
	}
	requireString(t, db, "new", "01234567890123456789012345678901")
	if err := db.SetStringBytes([]byte("big"), bytes.Repeat([]byte("z"), 128)); err == nil {
		t.Fatal("oversized value was acknowledged")
	}
	if db.Exists("big") {
		t.Fatal("oversized value leaked after rollback")
	}
	requireString(t, db, "new", "01234567890123456789012345678901")
	if err := db.MSetStringBytes([][]byte{[]byte("one"), bytes.Repeat([]byte("a"), 40), []byte("two"), bytes.Repeat([]byte("b"), 40)}); err == nil {
		t.Fatal("oversized batch was acknowledged")
	}
	if db.Exists("one") || db.Exists("two") {
		t.Fatal("oversized batch partially visible")
	}
	requireString(t, db, "new", "01234567890123456789012345678901")
}

func TestWatchVersionRetainsOwnedKeyAfterOverwrite(t *testing.T) {
	db := NewDatabase(0)
	defer db.Close()
	key := []byte("watched")
	for i := 0; i < 3; i++ {
		if err := db.SetStringBytes(key, []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	copy(key, "changed")
	if version := db.KeyVersion("watched"); version != 3 {
		t.Fatalf("WATCH version changed with request buffer reuse, got %d", version)
	}
}
