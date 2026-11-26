package database

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/TZJ-BYTE/Kerniva/config"
	"github.com/TZJ-BYTE/Kerniva/internal/golog"
	"github.com/TZJ-BYTE/Kerniva/internal/gologstate"
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
	db1, _ := m.GetDBByIndex(1)
	if err := db1.SetStringBytes([]byte("removed"), []byte("old WAL")); err != nil {
		t.Fatal(err)
	}
	closeGoPersistenceTestManager(t, m)
	requireNoGoPersistencePath(t, filepath.Join(cfg.DataDir, "go-snapshot-v1"))

	snapshotOnly := *cfg
	disabled, enabled := false, true
	snapshotOnly.GoLogEnabled, snapshotOnly.SnapshotEnabled = &disabled, &enabled
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
	db1, _ = m.GetDBByIndex(1)
	if !db1.Delete("removed") {
		t.Fatal("snapshot lost DB 1 before deletion")
	}
	closeGoPersistenceTestManager(t, m)

	m = openGoPersistenceTestManager(t, &both)
	requireString(t, m.GetDefaultDB(), "key", "newer snapshot value")
	db1, _ = m.GetDBByIndex(1)
	if _, ok := db1.Get("removed"); ok {
		t.Fatal("restored a key deleted while the WAL was disabled")
	}
	closeGoPersistenceTestManager(t, m)
	m = openGoPersistenceTestManager(t, cfg)
	requireString(t, m.GetDefaultDB(), "key", "newer snapshot value")
	db1, _ = m.GetDBByIndex(1)
	if _, ok := db1.Get("removed"); ok {
		t.Fatal("WAL restart resurrected a key missing from the snapshot")
	}
}

func TestManagerRetainedWALImportRejectsUnsafeHistory(t *testing.T) {
	for _, scenario := range []string{"source-identity", "source-lease", "writer-lock", "corrupt-segment", "excluded-db"} {
		t.Run(scenario, func(t *testing.T) {
			wal, snapshot := prepareWALExportTest(t)
			m := openGoPersistenceTestManager(t, &snapshot)
			closeGoPersistenceTestManager(t, m)
			root := filepath.Join(wal.DataDir, gologstate.WALDir)
			db1 := filepath.Join(root, "db_1")
			switch scenario {
			case "source-identity":
				if err := os.WriteFile(filepath.Join(root, gologstate.SourceIDFile), []byte("source"), 0o600); err != nil {
					t.Fatal(err)
				}
			case "source-lease":
				lease, err := os.OpenFile(filepath.Join(root, gologstate.SourceLeaseFile), os.O_CREATE|os.O_RDWR, 0o600)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = lease.Close() })
				if err := tryLockSnapshotFile(lease); err != nil {
					t.Fatal(err)
				}
			case "writer-lock":
				lock, err := golog.LockWriterDirectory(db1)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = lock.Close() })
			case "corrupt-segment":
				segments, err := filepath.Glob(filepath.Join(db1, "log-*.seg"))
				if err != nil || len(segments) != 1 {
					t.Fatalf("expected one retained segment, got %v (%v)", segments, err)
				}
				if err := os.WriteFile(segments[0], []byte("damaged retained segment"), 0o600); err != nil {
					t.Fatal(err)
				}
			case "excluded-db":
				wal.DBCount = 1
			}
			before, gen, _, err := gologstate.Read(wal.DataDir)
			if err != nil || before != goModeSnapshot || gen == 0 {
				t.Fatalf("expected committed snapshot, got %q %d (%v)", before, gen, err)
			}
			if m, err := NewDBManagerChecked(wal); err == nil {
				_ = m.Close()
				t.Fatal("accepted unsafe WAL import")
			}
			after, genAfter, _, err := gologstate.Read(wal.DataDir)
			if err != nil || after != before || genAfter != gen {
				t.Fatalf("failed import changed authority to %q %d (%v)", after, genAfter, err)
			}
			m = openGoPersistenceTestManager(t, &snapshot)
			db, _ := m.GetDBByIndex(1)
			requireString(t, db, "key", "db one")
		})
	}
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

func TestSnapshotImportRejectsVanishedSelectedGeneration(t *testing.T) {
	cfg := goPersistenceTestConfig(t, false, true)
	m := openGoPersistenceTestManager(t, cfg)
	if err := m.GetDefaultDB().SetStringBytes([]byte("only-copy"), []byte("snapshot")); err != nil {
		t.Fatal(err)
	}
	closeGoPersistenceTestManager(t, m)
	mode, err := openGoPersistenceMode(cfg.DataDir, true, false, cfg.DBCount)
	if err != nil {
		t.Fatal(err)
	}
	defer mode.close()
	if err := os.Rename(snapshotPath(mode.snapshotDir, mode.generation), filepath.Join(t.TempDir(), "held-snapshot")); err != nil {
		t.Fatal(err)
	}
	dbs := []*Database{NewDatabase(0), NewDatabase(1)}
	if err := mode.importSnapshot(dbs); err == nil || !strings.Contains(err.Error(), "snapshot generation") {
		t.Fatalf("missing selected snapshot should fail before opening a WAL: %v", err)
	}
	phase, generation, _, err := gologstate.Read(cfg.DataDir)
	if err != nil || phase != goModeSnapshot || generation != mode.generation {
		t.Fatalf("authority changed after failed import: %q %d (%v)", phase, generation, err)
	}
	requireNoGoPersistencePath(t, mode.logDir)
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
