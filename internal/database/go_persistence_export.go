package database

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/TZJ-BYTE/Kerniva/internal/golog"
	"github.com/TZJ-BYTE/Kerniva/internal/rustengine"
)

// exportWAL runs with the Go persistence root locked and WAL maintenance
// disabled. Hold every source writer lock until the new authority is published.
func (m *goPersistenceMode) exportWAL(databases []*Database) (err error) {
	directories := make([]string, m.walExportDBCount)
	locks := make([]*os.File, 0, len(directories))
	defer func() {
		for _, lock := range locks {
			err = errors.Join(err, lock.Close())
		}
	}()
	for id := range directories {
		dir := filepath.Join(m.logDir, fmt.Sprintf("db_%d", id))
		lock, err := golog.LockWriterDirectory(dir)
		if err != nil {
			return err
		}
		locks = append(locks, lock)
		if err := checkWALDatabase(dir); err != nil {
			return fmt.Errorf("open authoritative WAL database %s: %w", dir, err)
		}
		directories[id] = dir
	}
	for id, dir := range directories {
		if _, err := rustengine.RecoverGoLog(dir, databases[id].restoreGoLogEntry); err != nil {
			return fmt.Errorf("restore WAL database %d for Go snapshot: %w", id, err)
		}
	}

	if m.state == "" {
		// A legacy root has implicit WAL authority. Make it explicit before
		// publication so interruption cannot leave an ambiguous pair of stores.
		m.walDBCount = len(directories)
		if err := m.writeState(goModeWAL, 0); err != nil {
			return err
		}
	}
	if err := SaveSnapshot(m.snapshotDir, databases); err != nil {
		return fmt.Errorf("publish initial Go snapshot from WAL: %w", err)
	}
	return m.recordSnapshotGeneration()
}
