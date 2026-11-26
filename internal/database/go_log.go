package database

import (
	"fmt"
	"sync/atomic"

	"github.com/TZJ-BYTE/Kerniva/internal/datastruct"
	"github.com/TZJ-BYTE/Kerniva/internal/golog"
)

func (db *Database) openGoLog(options *EngineOptions, restore func(key, value []byte) error) error {
	if db.config.ColdStartStrategy != "load_all" || db.evictionPolicy != "noeviction" {
		return fmt.Errorf("go-log requires a fully resident database with noeviction")
	}
	log, err := golog.Open(db.config.DataDir, golog.Options{
		SegmentBytes: options.SegmentSizeBytes,
		MaxAge:       db.config.GoLogSegmentAge,
		GroupMax:     db.config.GoLogGroupMax,
		Sync:         db.persistenceDurability() == "sync",
	}, restore)
	if err != nil {
		return fmt.Errorf("open Go log: %w", err)
	}
	db.goLog = log
	db.writeBatch = log.WriteBatch
	return nil
}

func (db *Database) restoreGoLogEntry(key, value []byte) error {
	decoded, err := deserializeDataValue(value)
	if err != nil {
		return fmt.Errorf("decode log key %q: %w", key, err)
	}
	if decoded.IsExpired() {
		return nil
	}
	size := int64(len(key)) + decoded.ApproximateSize()
	if size > db.maxMemory-atomic.LoadInt64(&db.usedMemory) {
		return fmt.Errorf("restored database exceeds maxmemory")
	}
	stableKey := string(key)
	shard := db.getShard(stableKey)
	if shard.data == nil {
		shard.data = make(map[string]*datastruct.DataValue)
	}
	shard.data[stableKey] = decoded
	db.updateMemoryUsage(size)
	return nil
}
