package database

import (
	"errors"
	"fmt"
	"path/filepath"

	"github.com/TZJ-BYTE/Kerniva/config"
	"github.com/TZJ-BYTE/Kerniva/internal/golog"
	"github.com/TZJ-BYTE/Kerniva/internal/rustengine"
)

// importSnapshot builds a WAL only when no previous WAL could carry user data.
// The on-disk import state means an interrupted run may reset its own partial
// WAL, while the snapshot retains authority until all imported logs are durable.
func (m *goPersistenceMode) importSnapshot(cfg *config.Config, options *EngineOptions, maxMemory int64) error {
	dbs := make([]*Database, cfg.DBCount)
	for i := range dbs {
		db, err := NewDatabaseWithConfig(i, &DatabaseConfig{
			MaxMemory:       maxMemory,
			MaxMemoryPolicy: cfg.MaxMemoryPolicy,
		})
		if err != nil {
			return err
		}
		dbs[i] = db
	}
	if err := LoadSnapshot(m.snapshotDir, dbs); err != nil {
		return fmt.Errorf("restore Go snapshot for WAL import: %w", err)
	}
	if err := m.writeState(goModeImport, m.generation); err != nil {
		return err
	}
	for _, db := range dbs {
		dir := filepath.Join(m.logDir, fmt.Sprintf("db_%d", db.id))
		writer, err := golog.Open(dir, golog.Options{SegmentBytes: options.SegmentSizeBytes}, nil)
		if err != nil {
			return fmt.Errorf("open WAL for snapshot database %d: %w", db.id, err)
		}
		if err := errors.Join(importSnapshotDatabase(writer, db), writer.Close()); err != nil {
			return fmt.Errorf("import snapshot database %d into WAL: %w", db.id, err)
		}
	}
	return nil
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
