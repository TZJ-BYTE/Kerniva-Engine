package database

import (
	"strings"
	"sync"
)

type keyVersionShard struct {
	mu   sync.RWMutex
	keys map[string]*watchedKeyVersion
}

type watchedKeyVersion struct {
	version  uint64
	watchers int
}

// WatchKeyVersion starts tracking changes until a matching UnwatchKeyVersion.
func (db *Database) WatchKeyVersion(key string) uint64 {
	shard := &db.versions[getShardIndex(key)]
	shard.mu.Lock()
	defer shard.mu.Unlock()
	entry := shard.keys[key]
	if entry == nil {
		if shard.keys == nil {
			shard.keys = make(map[string]*watchedKeyVersion)
		}
		entry = &watchedKeyVersion{}
		shard.keys[strings.Clone(key)] = entry
	}
	entry.watchers++
	return entry.version
}

// UnwatchKeyVersion releases the caller's watch on key.
func (db *Database) UnwatchKeyVersion(key string) {
	if db == nil {
		return
	}
	shard := &db.versions[getShardIndex(key)]
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if entry := shard.keys[key]; entry != nil {
		entry.watchers--
		if entry.watchers == 0 {
			delete(shard.keys, key)
			if len(shard.keys) == 0 {
				shard.keys = nil
			}
		}
	}
}

func (db *Database) BumpKeyVersions(keys ...string) {
	if db == nil {
		return
	}
	if db.mutation != nil {
		for _, key := range keys {
			db.mutation.versions[key] = struct{}{}
		}
		return
	}
	for _, key := range keys {
		shard := &db.versions[getShardIndex(key)]
		shard.mu.Lock()
		if entry := shard.keys[key]; entry != nil {
			entry.version++
		}
		shard.mu.Unlock()
	}
	db.notifyChange()
}

func (db *Database) KeyVersion(key string) uint64 {
	if db == nil {
		return 0
	}
	shard := &db.versions[getShardIndex(key)]
	shard.mu.RLock()
	var v uint64
	if entry := shard.keys[key]; entry != nil {
		v = entry.version
	}
	shard.mu.RUnlock()
	return v
}

func (db *Database) ModEpoch() uint64 {
	if db == nil {
		return 0
	}
	return db.modEpoch.Load()
}

func (db *Database) ResetVersionState() {
	if db == nil {
		return
	}
	// The epoch invalidates old watches. Retain their entries until the last
	// connection releases them so a new watch can safely share the counter.
	db.modEpoch.Add(1)
	db.notifyChange()
}
