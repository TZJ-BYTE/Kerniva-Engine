package database

import (
	"strings"
	"sync"
)

type keyVersionShard struct {
	mu   sync.RWMutex
	keys map[string]*uint64
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
		version := &db.versions[getShardIndex(key)]
		version.mu.Lock()
		if version.keys == nil {
			version.keys = make(map[string]*uint64)
		}
		counter := version.keys[key]
		if counter == nil {
			counter = new(uint64)
			version.keys[strings.Clone(key)] = counter
		}
		*counter++
		version.mu.Unlock()
	}
	db.notifyChange()
}

func (db *Database) KeyVersion(key string) uint64 {
	if db == nil {
		return 0
	}
	version := &db.versions[getShardIndex(key)]
	version.mu.RLock()
	var v uint64
	if counter := version.keys[key]; counter != nil {
		v = *counter
	}
	version.mu.RUnlock()
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
	db.modEpoch.Add(1)
	for i := range db.versions {
		version := &db.versions[i]
		version.mu.Lock()
		version.keys = nil
		version.mu.Unlock()
	}
	db.notifyChange()
}
