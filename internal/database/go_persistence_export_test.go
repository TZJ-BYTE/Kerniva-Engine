package database

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/TZJ-BYTE/Kerniva/config"
	"github.com/TZJ-BYTE/Kerniva/internal/golog"
)

func prepareWALExportTest(t *testing.T) (*config.Config, config.Config) {
	t.Helper()
	cfg := goPersistenceTestConfig(t, true, false)
	m := openGoPersistenceTestManager(t, cfg)
	for id, value := range []string{"db zero", "db one"} {
		db, _ := m.GetDBByIndex(id)
		if err := db.SetStringBytes([]byte("key"), []byte(value)); err != nil {
			t.Fatal(err)
		}
	}
	closeGoPersistenceTestManager(t, m)
	snapshot := *cfg
	off, on := false, true
	snapshot.GoLogEnabled, snapshot.SnapshotEnabled = &off, &on
	return cfg, snapshot
}

func TestManagerDirectWALToSnapshot(t *testing.T) {
	for _, variant := range []string{"marked", "legacy-v1", "interrupted-after-snapshot"} {
		t.Run(variant, func(t *testing.T) {
			cfg, snapshot := prepareWALExportTest(t)
			modeFile := filepath.Join(cfg.DataDir, goPersistenceModeFile)
			snapshotDir := filepath.Join(cfg.DataDir, "go-snapshot-v1")
			switch variant {
			case "legacy-v1":
				if err := os.Remove(modeFile); err != nil {
					t.Fatal(err)
				}
			case "interrupted-after-snapshot":
				empty := goPersistenceTestConfig(t, false, false)
				m := openGoPersistenceTestManager(t, empty)
				if err := SaveSnapshot(snapshotDir, m.databases); err != nil {
					t.Fatal(err)
				}
			}

			m := openGoPersistenceTestManager(t, &snapshot)
			for id, value := range []string{"db zero", "db one"} {
				db, _ := m.GetDBByIndex(id)
				requireString(t, db, "key", value)
				if db.goLog != nil {
					t.Fatalf("database %d opened a WAL writer", id)
				}
			}
			marker, err := os.ReadFile(modeFile)
			if err != nil || !strings.HasPrefix(string(marker), "snapshot ") {
				t.Fatalf("before close, mode = %q, err = %v", marker, err)
			}
			closeGoPersistenceTestManager(t, m)
			m = openGoPersistenceTestManager(t, &snapshot)
			db, _ := m.GetDBByIndex(1)
			requireString(t, db, "key", "db one")
		})
	}
}

func TestManagerDirectWALToSnapshotRejectsUnsafeSource(t *testing.T) {
	for _, variant := range []string{"corrupt", "missing-db", "locked-writer", "excluded-db"} {
		t.Run(variant, func(t *testing.T) {
			cfg, snapshot := prepareWALExportTest(t)
			modeFile := filepath.Join(cfg.DataDir, goPersistenceModeFile)
			db1 := filepath.Join(cfg.DataDir, "go-log-v1", "db_1")
			before, err := os.ReadFile(modeFile)
			if err != nil {
				t.Fatal(err)
			}
			switch variant {
			case "corrupt":
				segments, err := filepath.Glob(filepath.Join(db1, "log-*.seg"))
				if err != nil || len(segments) != 1 {
					t.Fatalf("sealed segment: %v, %v", segments, err)
				}
				raw, err := os.ReadFile(segments[0])
				if err != nil {
					t.Fatal(err)
				}
				raw[len(raw)-1] ^= 0xff
				if err := os.WriteFile(segments[0], raw, 0o600); err != nil {
					t.Fatal(err)
				}
			case "missing-db":
				if err := os.Rename(db1, db1+".preserved"); err != nil {
					t.Fatal(err)
				}
			case "locked-writer":
				lock, err := golog.LockWriterDirectory(db1)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = lock.Close() })
			case "excluded-db":
				snapshot.DBCount = 1
			}

			if m, err := NewDBManagerChecked(&snapshot); err == nil {
				_ = m.Close()
				t.Fatal("accepted an unsafe WAL source")
			}
			after, err := os.ReadFile(modeFile)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatalf("mode changed from %q to %q, err = %v", before, after, err)
			}
			requireNoGoPersistencePath(t, filepath.Join(cfg.DataDir, "go-snapshot-v1"))
		})
	}
}
