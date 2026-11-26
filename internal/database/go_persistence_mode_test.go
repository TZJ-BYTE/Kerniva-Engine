package database

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/TZJ-BYTE/Kerniva/config"
)

func goPersistenceTestConfig(t *testing.T, log, snapshot bool) *config.Config {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.DataDir = filepath.Join(t.TempDir(), "data")
	cfg.StorageBackend = config.StorageGoLog
	cfg.ColdStartStrategy = "load_all"
	cfg.PersistenceEnabled = true
	cfg.GoLogEnabled, cfg.SnapshotEnabled = &log, &snapshot
	cfg.DBCount = 2
	return cfg
}

func openGoPersistenceTestManager(t *testing.T, cfg *config.Config) *DBManager {
	t.Helper()
	m, err := NewDBManagerChecked(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := m.Close(); err != nil {
			t.Error(err)
		}
	})
	return m
}

func closeGoPersistenceTestManager(t *testing.T, m *DBManager) {
	t.Helper()
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
}

func requireNoGoPersistencePath(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stat %s = %v, want not exist", path, err)
	}
}

func TestManagerGoSnapshotOnlyAndPureMemory(t *testing.T) {
	cfg := goPersistenceTestConfig(t, false, true)
	cfg.SnapshotIntervalSeconds = 1
	m := openGoPersistenceTestManager(t, cfg)
	db1, err := m.GetDBByIndex(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := db1.SetStringBytes([]byte("key"), []byte("snapshot data")); err != nil {
		t.Fatal(err)
	}

	snapshotDir := filepath.Join(cfg.DataDir, "go-snapshot-v1")
	deadline := time.Now().Add(5 * time.Second)
	for {
		entries, err := snapshotGenerations(snapshotDir)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		if len(entries) != 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("periodic snapshot was never published")
		}
		time.Sleep(20 * time.Millisecond)
	}
	requireNoGoPersistencePath(t, filepath.Join(cfg.DataDir, "go-log-v1"))
	closeGoPersistenceTestManager(t, m)

	memory := *cfg
	disabled := false
	memory.SnapshotEnabled = &disabled
	m = openGoPersistenceTestManager(t, &memory)
	db1, _ = m.GetDBByIndex(1)
	if _, ok := db1.Get("key"); ok {
		t.Fatal("pure memory loaded an old snapshot")
	}
	closeGoPersistenceTestManager(t, m)

	m = openGoPersistenceTestManager(t, cfg)
	db1, _ = m.GetDBByIndex(1)
	requireString(t, db1, "key", "snapshot data")
	if _, ok := m.GetDefaultDB().Get("key"); ok {
		t.Fatal("loaded snapshot entry into database 0")
	}

	memory.DataDir = filepath.Join(cfg.DataDir, "unused")
	m = openGoPersistenceTestManager(t, &memory)
	closeGoPersistenceTestManager(t, m)
	requireNoGoPersistencePath(t, memory.DataDir)
}

func TestManagerGoPersistenceHandoff(t *testing.T) {
	cfg := goPersistenceTestConfig(t, true, false)
	m := openGoPersistenceTestManager(t, cfg)
	if err := m.GetDefaultDB().SetStringBytes([]byte("key"), []byte("WAL value")); err != nil {
		t.Fatal(err)
	}
	closeGoPersistenceTestManager(t, m)
	requireNoGoPersistencePath(t, filepath.Join(cfg.DataDir, "go-snapshot-v1"))

	snapshotOnly := *cfg
	disabled, enabled := false, true
	snapshotOnly.GoLogEnabled, snapshotOnly.SnapshotEnabled = &disabled, &enabled
	if m, err := NewDBManagerChecked(&snapshotOnly); err == nil {
		_ = m.Close()
		t.Fatal("snapshot-only ignored authoritative WAL")
	} else if !strings.Contains(err.Error(), "WAL is authoritative") {
		t.Fatal(err)
	}

	both := *cfg
	both.SnapshotEnabled = &enabled
	m = openGoPersistenceTestManager(t, &both)
	requireString(t, m.GetDefaultDB(), "key", "WAL value")
	closeGoPersistenceTestManager(t, m)

	// WAL-only may append commits newer than the last combined snapshot.
	m = openGoPersistenceTestManager(t, cfg)
	if err := m.GetDefaultDB().SetStringBytes([]byte("key"), []byte("log-only update")); err != nil {
		t.Fatal(err)
	}
	closeGoPersistenceTestManager(t, m)
	m = openGoPersistenceTestManager(t, &both)
	requireString(t, m.GetDefaultDB(), "key", "log-only update")
	closeGoPersistenceTestManager(t, m)

	m = openGoPersistenceTestManager(t, &snapshotOnly)
	requireString(t, m.GetDefaultDB(), "key", "log-only update")
	if err := m.GetDefaultDB().SetStringBytes([]byte("key"), []byte("newer snapshot value")); err != nil {
		t.Fatal(err)
	}
	closeGoPersistenceTestManager(t, m)

	if m, err := NewDBManagerChecked(&both); err == nil {
		_ = m.Close()
		t.Fatal("dual mode accepted WAL after snapshot-only updates")
	} else if !strings.Contains(err.Error(), "snapshots are authoritative") {
		t.Fatal(err)
	}
	m = openGoPersistenceTestManager(t, &snapshotOnly)
	requireString(t, m.GetDefaultDB(), "key", "newer snapshot value")
}

func TestManagerFreshGoSnapshotImportsWALAfterInterruption(t *testing.T) {
	cfg := goPersistenceTestConfig(t, false, true)
	m := openGoPersistenceTestManager(t, cfg)
	db1, _ := m.GetDBByIndex(1)
	if err := db1.SetStringBytes([]byte("key"), []byte("source snapshot")); err != nil {
		t.Fatal(err)
	}
	closeGoPersistenceTestManager(t, m)

	mode, err := openGoPersistenceMode(cfg.DataDir, true, true, cfg.DBCount)
	if err != nil {
		t.Fatal(err)
	}
	if err := mode.writeState(goModeImport, mode.generation); err != nil {
		t.Fatal(err)
	}
	partial, err := NewDatabaseWithConfig(0, &DatabaseConfig{
		Type: Persistent, StorageBackend: config.StorageGoLog,
		DataDir: filepath.Join(mode.logDir, "db_0"), ColdStartStrategy: "load_all",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := partial.SetStringBytes([]byte("stale"), []byte("partial import")); err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(partial.Close(), mode.close()); err != nil {
		t.Fatal(err)
	}

	both := *cfg
	enabled := true
	both.GoLogEnabled = &enabled
	m = openGoPersistenceTestManager(t, &both)
	db1, _ = m.GetDBByIndex(1)
	requireString(t, db1, "key", "source snapshot")
	if _, ok := m.GetDefaultDB().Get("stale"); ok {
		t.Fatal("partial baseline import was recovered as committed data")
	}
	closeGoPersistenceTestManager(t, m)

	logOnly := both
	disabled := false
	logOnly.SnapshotEnabled = &disabled
	m = openGoPersistenceTestManager(t, &logOnly)
	db1, _ = m.GetDBByIndex(1)
	requireString(t, db1, "key", "source snapshot")
}

func TestManagerFreshGoSnapshotEnablesWAL(t *testing.T) {
	cfg := goPersistenceTestConfig(t, false, true)
	m := openGoPersistenceTestManager(t, cfg)
	if err := m.GetDefaultDB().SetStringBytes([]byte("key"), []byte("source snapshot")); err != nil {
		t.Fatal(err)
	}
	closeGoPersistenceTestManager(t, m)

	enabled, disabled := true, false
	cfg.GoLogEnabled = &enabled
	cfg.SnapshotEnabled = &disabled
	m = openGoPersistenceTestManager(t, cfg)
	requireString(t, m.GetDefaultDB(), "key", "source snapshot")
	closeGoPersistenceTestManager(t, m)
	m = openGoPersistenceTestManager(t, cfg)
	requireString(t, m.GetDefaultDB(), "key", "source snapshot")
}

func TestManagerCloseRejectsWritesAfterFinalSnapshotCapture(t *testing.T) {
	cfg := goPersistenceTestConfig(t, true, true)
	m := openGoPersistenceTestManager(t, cfg)
	db0 := m.GetDefaultDB()
	db1, _ := m.GetDBByIndex(1)
	if err := db1.SetStringBytes([]byte("key"), []byte("accepted before close")); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(cfg.DataDir, "go-snapshot-v1")
	if err := ensureSnapshotDirectory(dir); err != nil {
		t.Fatal(err)
	}
	snapshotLock, err := openSnapshotLock(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = snapshotLock.Close() })
	done := make(chan error, 1)
	go func() { done <- m.Close() }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		db1.expireMu.Lock()
		cleanerStopped := db1.expireStop == nil
		db1.expireMu.Unlock()
		if cleanerStopped {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("manager did not stop the expiration cleaner")
		}
		time.Sleep(time.Millisecond)
	}

	// Block the old per-database shutdown path after the complete snapshot
	// becomes visible. A direct write to DB 1 must still be rejected.
	func() {
		db0.expireMu.Lock()
		defer db0.expireMu.Unlock()
		if err := snapshotLock.Close(); err != nil {
			t.Fatal(err)
		}
		for {
			generations, err := snapshotGenerations(dir)
			if err != nil {
				t.Fatal(err)
			}
			if len(generations) != 0 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("manager did not publish its final snapshot")
			}
			time.Sleep(time.Millisecond)
		}
		if err := db1.SetStringBytes([]byte("key"), []byte("accepted after capture")); !errors.Is(err, errDatabaseClosed) {
			t.Fatalf("write after final capture returned %v, want closed", err)
		}
	}()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	disabled := false
	cfg.GoLogEnabled = &disabled
	m = openGoPersistenceTestManager(t, cfg)
	db1, _ = m.GetDBByIndex(1)
	requireString(t, db1, "key", "accepted before close")
}

func TestManagerWALDatabaseExtentSurvivesConfigurationChanges(t *testing.T) {
	cfg := goPersistenceTestConfig(t, true, false)
	m := openGoPersistenceTestManager(t, cfg)
	db1, _ := m.GetDBByIndex(1)
	if err := db1.SetStringBytes([]byte("key"), []byte("last original database")); err != nil {
		t.Fatal(err)
	}
	closeGoPersistenceTestManager(t, m)

	smaller := *cfg
	smaller.DBCount = 1
	m = openGoPersistenceTestManager(t, &smaller)
	closeGoPersistenceTestManager(t, m)

	larger := *cfg
	larger.DBCount = 3
	m = openGoPersistenceTestManager(t, &larger)
	db1, _ = m.GetDBByIndex(1)
	requireString(t, db1, "key", "last original database")
	closeGoPersistenceTestManager(t, m)

	// The last DB was valid even without any committed keys. A directory
	// containing only lock artifacts must not substitute for it.
	wal := filepath.Join(cfg.DataDir, "go-log-v1")
	missing := filepath.Join(wal, "db_2")
	if err := os.Rename(missing, filepath.Join(cfg.DataDir, "preserved-db_2")); err != nil {
		t.Fatal(err)
	}
	assertWALMissing := func() {
		t.Helper()
		if m, err := NewDBManagerChecked(&smaller); err == nil {
			_ = m.Close()
			t.Fatal("WAL started without its recorded trailing database")
		} else if !strings.Contains(err.Error(), "authoritative WAL database "+missing) {
			t.Fatal(err)
		}
	}
	assertWALMissing()
	if err := os.Mkdir(missing, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(missing, ".writer.lock"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	assertWALMissing()
}

func TestManagerWALDatabaseExtentUpgradesLegacyRecord(t *testing.T) {
	cfg := goPersistenceTestConfig(t, true, false)
	m := openGoPersistenceTestManager(t, cfg)
	db1, _ := m.GetDBByIndex(1)
	if err := db1.SetStringBytes([]byte("key"), []byte("legacy WAL")); err != nil {
		t.Fatal(err)
	}
	closeGoPersistenceTestManager(t, m)

	modePath := filepath.Join(cfg.DataDir, goPersistenceModeFile)
	if err := os.WriteFile(modePath, []byte("wal 0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	m = openGoPersistenceTestManager(t, cfg)
	db1, _ = m.GetDBByIndex(1)
	requireString(t, db1, "key", "legacy WAL")
	closeGoPersistenceTestManager(t, m)

	mode, err := os.ReadFile(modePath)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(mode); got != "wal 0 2\n" {
		t.Fatalf("upgraded mode = %q, want historical WAL extent of 2", got)
	}
}

func TestManagerLegacyWALRejectsGapBetweenDatabases(t *testing.T) {
	cfg := goPersistenceTestConfig(t, true, false)
	cfg.DBCount = 3
	m := openGoPersistenceTestManager(t, cfg)
	closeGoPersistenceTestManager(t, m)

	modePath := filepath.Join(cfg.DataDir, goPersistenceModeFile)
	if err := os.WriteFile(modePath, []byte("wal 0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	missing := filepath.Join(cfg.DataDir, "go-log-v1", "db_1")
	if err := os.Rename(missing, filepath.Join(cfg.DataDir, "preserved-db_1")); err != nil {
		t.Fatal(err)
	}
	if m, err := NewDBManagerChecked(cfg); err == nil {
		_ = m.Close()
		t.Fatal("WAL started with a gap in its legacy database layout")
	} else if !strings.Contains(err.Error(), "authoritative WAL database "+missing) {
		t.Fatal(err)
	}
}
