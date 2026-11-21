package database

import (
	"fmt"

	"github.com/TZJ-BYTE/Kerniva/internal/datastruct"
	"github.com/TZJ-BYTE/Kerniva/internal/rustengine"
)

func rustSyncPolicy(durability string) rustengine.SyncPolicy {
	switch durability {
	case "wal", "flush":
		return rustengine.SyncFlush
	case "wal_fsync", "lsm", "sync":
		return rustengine.SyncData
	case "none":
		return rustengine.SyncNone
	default:
		return rustengine.SyncData
	}
}

func (db *Database) hasPersistence() bool {
	return db != nil && db.rustEngine != nil
}

func (db *Database) persistenceMode() string {
	switch {
	case db == nil:
		return "Memory"
	case db.rustEngine != nil:
		return "Kerniva"
	default:
		return "Memory"
	}
}

func (db *Database) persistenceGet(key string) ([]byte, bool, error) {
	if db != nil && db.mutation != nil {
		if operation, ok := db.mutation.writes[key]; ok {
			return operation.Value, !operation.Delete, nil
		}
	}
	switch {
	case db == nil:
		return nil, false, nil
	case db.rustEngine != nil:
		return db.rustEngine.Get(stringToBytesRO(key))
	default:
		return nil, false, nil
	}
}

func (db *Database) persistenceReset() error {
	switch {
	case db == nil:
		return nil
	case db.rustEngine != nil:
		return db.rustEngine.Reset()
	default:
		return nil
	}
}

func (db *Database) persistenceClose() error {
	switch {
	case db == nil:
		return nil
	case db.rustEngine != nil:
		return db.rustEngine.Close()
	default:
		return nil
	}
}

func (db *Database) persistenceStats() map[string]interface{} {
	switch {
	case db == nil:
		return map[string]interface{}{}
	case db.rustEngine != nil:
		stats, err := db.rustEngine.Stats()
		if err != nil {
			return map[string]interface{}{
				"error": err.Error(),
			}
		}
		return map[string]interface{}{
			"keys":              stats.Keys,
			"active_segment_id": stats.ActiveSegmentID,
			"segment_count":     stats.SegmentCount,
			"cached_readers":    stats.CachedReaders,
			"pinned_segments":   stats.PinnedSegments,
			"pending_reclaims":  stats.PendingReclaims,
		}
	default:
		return map[string]interface{}{}
	}
}

func (db *Database) loadAllFromPersistence() error {
	switch {
	case db == nil:
		return fmt.Errorf("database is nil")
	case db.rustEngine != nil:
		allData, err := db.rustEngine.LoadAllKeys()
		if err != nil {
			return fmt.Errorf("failed to load keys from rust engine: %v", err)
		}
		for key, valueBytes := range allData {
			dataValue, err := deserializeDataValue(valueBytes)
			if err != nil {
				return fmt.Errorf("decode persisted key %q: %w", key, err)
			}
			if dataValue.IsExpired() {
				if err := db.persistenceDelete(key); err != nil {
					return err
				}
				continue
			}
			shard := db.getShard(key)
			shard.lock.Lock()
			if shard.data == nil {
				shard.data = make(map[string]*datastruct.DataValue)
			}
			shard.data[key] = dataValue
			shard.lock.Unlock()
			db.updateMemoryUsage(int64(len(key)) + dataValue.ApproximateSize())
			db.trimReadCache()
		}
		return nil
	default:
		return fmt.Errorf("persistence engine not initialized")
	}
}

func (db *Database) persistencePut(key string, value []byte) error {
	if db == nil || db.rustEngine == nil {
		return nil
	}
	if db.mutation != nil {
		db.mutation.writes[key] = rustengine.WriteOp{Key: []byte(key), Value: value}
		return nil
	}
	return db.rustEngine.Put(stringToBytesRO(key), value)
}

func (db *Database) persistenceDelete(key string) error {
	if db == nil || db.rustEngine == nil {
		return nil
	}
	if db.mutation != nil {
		db.mutation.writes[key] = rustengine.WriteOp{Key: []byte(key), Delete: true}
		return nil
	}
	return db.rustEngine.Delete(stringToBytesRO(key))
}

func (db *Database) persistenceCompact(maxRounds int) (uint64, error) {
	if db == nil || db.rustEngine == nil {
		return 0, nil
	}
	return db.rustEngine.CompactUntilStable(maxRounds)
}

func (db *Database) persistenceCheckpoint() error {
	if db == nil || db.rustEngine == nil {
		return nil
	}
	return db.rustEngine.MaybeCheckpoint()
}
