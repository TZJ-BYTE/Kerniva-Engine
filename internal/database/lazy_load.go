package database

import (
	"fmt"
	"strings"

	"github.com/TZJ-BYTE/Kerniva/internal/datastruct"
)

func (db *Database) getLiveValue(key string) (*datastruct.DataValue, bool, error) {
	if db == nil {
		return nil, false, nil
	}

	if dv, ok := db.getLiveValueFromMemory(key); ok {
		return dv, true, nil
	}

	if !db.hasPersistence() {
		return nil, false, nil
	}

	return db.loadLiveValueFromPersistence(key)
}

func (db *Database) getLiveValueFromMemory(key string) (*datastruct.DataValue, bool) {
	sh := db.getShard(key)
	sh.lock.RLock()
	dv, exists := sh.data[key]
	sh.lock.RUnlock()
	if !exists || dv == nil {
		return nil, false
	}
	if !dv.IsExpired() {
		if strings.Contains(db.evictionPolicy, "lru") {
			sh.lock.Lock()
			dv.UpdateLastAccessed()
			sh.lock.Unlock()
		}
		return dv, true
	}

	sh.lock.Lock()
	cur := sh.data[key]
	if cur != nil && cur.IsExpired() {
		delete(sh.data, key)
		db.updateMemoryUsage(-int64(len(key)) - cur.ApproximateSize())
		sh.lock.Unlock()
		if err := db.persistenceDelete(key); err != nil {
			db.recordLSMError(err)
		}
		db.BumpKeyVersions(key)
		return nil, false
	}
	sh.lock.Unlock()

	if cur == nil {
		return nil, false
	}
	return cur, true
}

func (db *Database) loadLiveValueFromPersistence(key string) (*datastruct.DataValue, bool, error) {
	valueBytes, found, err := db.persistenceGet(key)
	if err != nil {
		return nil, false, err
	}
	if !found {
		return nil, false, nil
	}

	dv, err := deserializeDataValue(valueBytes)
	if err != nil {
		return nil, false, fmt.Errorf("deserialize persisted key %s: %w", key, err)
	}
	if dv.IsExpired() {
		if err := db.persistenceDelete(key); err != nil {
			return nil, false, err
		}
		db.BumpKeyVersions(key)
		return nil, false, nil
	}

	sh := db.getShard(key)
	var memDelta int64

	sh.lock.Lock()
	if sh.data == nil {
		sh.data = make(map[string]*datastruct.DataValue)
	}

	if cur, exists := sh.data[key]; exists && cur != nil {
		if !cur.IsExpired() {
			sh.lock.Unlock()
			return cur, true, nil
		}
		delete(sh.data, key)
		memDelta -= int64(len(key)) + cur.ApproximateSize()
	}

	stableKey := strings.Clone(key)
	sh.data[stableKey] = dv
	memDelta += int64(len(stableKey)) + dv.ApproximateSize()
	sh.lock.Unlock()

	if memDelta != 0 {
		db.updateMemoryUsage(memDelta)
	}

	return dv, true, nil
}

func (db *Database) scanPersistedValues() (map[string]*datastruct.DataValue, error) {
	if db == nil || !db.hasPersistence() {
		return map[string]*datastruct.DataValue{}, nil
	}

	raw, err := db.rustEngine.LoadAllKeys()
	if err != nil {
		return nil, err
	}

	out := make(map[string]*datastruct.DataValue, len(raw))
	for key, valueBytes := range raw {
		dv, err := deserializeDataValue(valueBytes)
		if err != nil {
			return nil, fmt.Errorf("deserialize persisted key %q: %w", key, err)
		}
		if dv == nil || dv.IsExpired() {
			if dv != nil && dv.IsExpired() {
				if err := db.persistenceDelete(key); err != nil {
					return nil, err
				}
				db.BumpKeyVersions(key)
			}
			continue
		}
		out[key] = dv
	}
	return out, nil
}

func dataTypeName(dv *datastruct.DataValue) string {
	if dv == nil {
		return "none"
	}

	switch dv.Value.(type) {
	case *datastruct.String, *datastruct.BytesString:
		return "string"
	case *datastruct.List:
		return "list"
	case *datastruct.Hash:
		return "hash"
	case *datastruct.Set:
		return "set"
	case *datastruct.ZSet:
		return "zset"
	case *datastruct.Stream:
		return "stream"
	default:
		return "none"
	}
}
