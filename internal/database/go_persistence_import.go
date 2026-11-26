package database

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/TZJ-BYTE/Kerniva/internal/golog"
	"github.com/TZJ-BYTE/Kerniva/internal/gologstate"
	"github.com/TZJ-BYTE/Kerniva/internal/rustengine"
)

// importSnapshot keeps the existing history and its sequence numbers. All
// writers remain owned by the live databases through publication and serving.
func (m *goPersistenceMode) importSnapshot(dbs []*Database) (err error) {
	if err := loadSnapshotGeneration(m.snapshotDir, dbs, &m.generation); err != nil {
		return fmt.Errorf("restore Go snapshot for WAL import: %w", err)
	}
	lease, err := os.OpenFile(filepath.Join(m.logDir, gologstate.SourceLeaseFile), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, lease.Close()) }()
	if err := tryLockSnapshotFile(lease); err != nil {
		return fmt.Errorf("lock replication source for snapshot import: %w", err)
	}
	if _, err := os.Lstat(filepath.Join(m.logDir, gologstate.SourceIDFile)); err == nil {
		return fmt.Errorf("snapshot import requires a standalone WAL: preserve the existing replication source directory and re-establish the replica baseline before enabling the WAL")
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	for _, db := range dbs {
		// Existing values have been superseded by the authoritative snapshot.
		// The WAL reader still validates each segment and recovers its offset.
		if err := db.openGoLog(db.config.Options, nil); err != nil {
			return fmt.Errorf("open WAL for snapshot database %d: %w", db.id, err)
		}
		if m.generation == 0 && db.goLog.Stats().LastSequence != 0 {
			return fmt.Errorf("no published Go snapshot for retained WAL database %d", db.id)
		}
	}
	if err := m.writeState(goModeImport, m.generation); err != nil {
		return err
	}
	for _, db := range dbs {
		if err := importSnapshotDatabase(db.goLog, db); err != nil {
			return fmt.Errorf("import snapshot database %d into WAL: %w", db.id, err)
		}
		if err := db.goLog.Seal(); err != nil {
			return fmt.Errorf("publish snapshot baseline for database %d: %w", db.id, err)
		}
	}
	return m.startWriting()
}

func importSnapshotDatabase(writer *golog.Writer, db *Database) error {
	if writer.Stats().LastSequence != 0 {
		if err := writer.Reset(); err != nil {
			return err
		}
	}
	const batchSize = 4 << 20
	var operations []rustengine.WriteOp
	var batchBytes int
	for i := range db.shards {
		for key, value := range db.shards[i].data {
			if value.IsExpired() {
				continue
			}
			encoded, err := value.Serialize()
			if err != nil {
				return fmt.Errorf("encode key %q: %w", key, err)
			}
			size := len(key) + len(encoded) + 9
			if len(operations) > 0 && batchBytes+size > batchSize {
				if err := writer.WriteBatch(operations); err != nil {
					return err
				}
				operations = operations[:0]
				batchBytes = 0
			}
			operations = append(operations, rustengine.WriteOp{Key: []byte(key), Value: encoded})
			batchBytes += size
		}
	}
	return writer.WriteBatch(operations)
}
