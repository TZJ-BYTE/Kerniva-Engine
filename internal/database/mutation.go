package database

import (
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/TZJ-BYTE/Kerniva/internal/datastruct"
	"github.com/TZJ-BYTE/Kerniva/internal/rustengine"
	"github.com/TZJ-BYTE/Kerniva/internal/writetrace"
)

var errDatabaseClosed = errors.New("database is closed")

func (db *Database) beginKeyOperation(key string, write bool) bool {
	db.operationMu.RLock()
	if write {
		db.operationMu.RUnlock()
		db.operationMu.Lock()
		return false
	}
	db.lockHotKey(key, strings.Contains(db.evictionPolicy, "lru"))
	if db.hasPersistence() {
		sh := db.getShard(key)
		sh.lock.RLock()
		value := sh.data[key]
		// Go-log keeps all live keys resident, so a missing key needs no disk lookup.
		// TTL values can cross their deadline after this check. Their normal
		// lookup may delete persistent state, so retain the exclusive path.
		canReadShared := (value == nil && db.goLog != nil) || (value != nil && value.ExpireTime == 0)
		sh.lock.RUnlock()
		if !canReadShared {
			lock := &db.hotLocks[getShardIndex(key)]
			if strings.Contains(db.evictionPolicy, "lru") {
				lock.Unlock()
			} else {
				lock.RUnlock()
			}
			db.operationMu.RUnlock()
			db.operationMu.Lock()
			return false
		}
	}
	return true
}

func (db *Database) beginSetOperation(key string) bool {
	db.operationMu.RLock()
	if db.evictionPolicy != "noeviction" {
		db.operationMu.RUnlock()
		db.operationMu.Lock()
		return false
	}
	db.lockHotKey(key, true)
	return true
}

// The caller retains operationMu.RLock while waiting. The committing writer
// needs only this stripe lock to publish, so lifecycle writers can drain safely.
func (db *Database) lockHotKey(key string, exclusive bool) {
	index := getShardIndex(key)
	lock := &db.hotLocks[index]
	for {
		if exclusive {
			lock.Lock()
		} else {
			lock.RLock()
		}
		done := db.pendingStringWrites[index][key]
		if done == nil {
			return
		}
		if exclusive {
			lock.Unlock()
		} else {
			lock.RUnlock()
		}
		<-done
	}
}

// The caller owns the key's hot lock. Reserve the complete size delta before
// publishing a value so simultaneous writes cannot exceed the memory budget.
func (db *Database) setMemoryString(key, value []byte) error {
	k := bytesToString(key)
	sh := db.getShard(k)
	sh.lock.Lock()
	defer sh.lock.Unlock()
	old := sh.data[k]
	delta := int64(len(value)) + 24
	if old == nil {
		delta += int64(len(k))
	} else {
		delta -= old.ApproximateSize()
	}
	for {
		used := atomic.LoadInt64(&db.usedMemory)
		if delta > 0 && delta > db.maxMemory-used {
			return fmt.Errorf("OOM command not allowed when used memory exceeds 'maxmemory'")
		}
		if atomic.CompareAndSwapInt64(&db.usedMemory, used, used+delta) {
			break
		}
	}
	if sh.data == nil {
		sh.data = make(map[string]*datastruct.DataValue)
	}
	if old == nil {
		k = strings.Clone(k)
		old = &datastruct.DataValue{}
		sh.data[k] = old
	}
	if bytes, ok := old.Value.(*datastruct.BytesString); ok && cap(bytes.Data) >= len(value) {
		bytes.Data = bytes.Data[:len(value)]
		copy(bytes.Data, value)
	} else {
		old.Value = &datastruct.BytesString{Data: append([]byte(nil), value...)}
	}
	old.ExpireTime = 0
	old.LastAccessedAt = time.Now().UnixMilli()
	db.BumpKeyVersions(k)
	return nil
}

func (db *Database) endKeyOperation(key string, write, parallel bool) {
	if !parallel {
		db.trimReadCache()
		db.operationMu.Unlock()
		return
	}
	lock := &db.hotLocks[getShardIndex(key)]
	if write || strings.Contains(db.evictionPolicy, "lru") {
		lock.Unlock()
	} else {
		lock.RUnlock()
	}
	db.operationMu.RUnlock()
}

type memoryBefore struct {
	value *datastruct.DataValue
	data  []byte
	size  int64
}

// A mutation owns operationMu exclusively. Durable operations are collected
// into one engine batch, and cache state is restored if the batch fails.
type mutationState struct {
	before     map[string]memoryBefore
	writes     map[string]rustengine.WriteOp
	versions   map[string]struct{}
	usedMemory int64
	err        error
	trace      *writetrace.Request
}

func (db *Database) beginMutation() {
	db.mutation = &mutationState{
		before:     make(map[string]memoryBefore),
		writes:     make(map[string]rustengine.WriteOp),
		versions:   make(map[string]struct{}),
		usedMemory: atomic.LoadInt64(&db.usedMemory),
	}
}

func (db *Database) captureMemoryValue(key string, sh *shard) {
	tx := db.mutation
	if _, captured := tx.before[key]; captured {
		return
	}
	previous := memoryBefore{value: sh.data[key]}
	if previous.value != nil {
		previous.size = int64(len(key)) + previous.value.ApproximateSize()
		var err error
		previous.data, err = previous.value.Serialize()
		if err != nil && tx.err == nil {
			tx.err = fmt.Errorf("capture key before mutation: %w", err)
		}
	}
	tx.before[key] = previous
}

func (db *Database) finishMutation(operationErr error) error {
	tx := db.mutation
	used := tx.usedMemory
	for key, previous := range tx.before {
		used -= previous.size
		if value := db.shards[getShardIndex(key)].data[key]; value != nil {
			used += int64(len(key)) + value.ApproximateSize()
		}
	}
	atomic.StoreInt64(&db.usedMemory, used)
	if operationErr == nil {
		operationErr = tx.err
	}
	if operationErr == nil && !db.evictIfNeeded() {
		operationErr = fmt.Errorf("OOM command not allowed when used memory exceeds 'maxmemory'")
	}
	if operationErr == nil && len(tx.writes) > 0 {
		operations := make([]rustengine.WriteOp, 0, len(tx.writes))
		for _, operation := range tx.writes {
			operations = append(operations, operation)
		}
		operationErr = db.writeBatchTraced(operations, tx.trace)
	}
	db.mutation = nil
	if operationErr != nil {
		for key, previous := range tx.before {
			sh := &db.shards[getShardIndex(key)]
			if previous.value == nil {
				delete(sh.data, key)
				continue
			}
			value, err := deserializeDataValue(previous.data)
			if err != nil {
				value = previous.value
			}
			if sh.data == nil {
				sh.data = make(map[string]*datastruct.DataValue)
			}
			sh.data[strings.Clone(key)] = value
		}
		atomic.StoreInt64(&db.usedMemory, tx.usedMemory)
		db.recordLSMError(operationErr)
		return operationErr
	}
	if len(tx.versions) > 0 {
		keys := make([]string, 0, len(tx.versions))
		for key := range tx.versions {
			keys = append(keys, key)
		}
		db.BumpKeyVersions(keys...)
	}
	db.trimReadCache()
	return nil
}

func (db *Database) writeBatchTraced(operations []rustengine.WriteOp, row *writetrace.Request) error {
	if row != nil && db.goLog != nil {
		return db.goLog.WriteBatchTraced(operations, row)
	}
	return db.writeBatch(operations)
}

func (db *Database) endReadOperation() {
	db.trimReadCache()
	db.operationMu.Unlock()
}

// Persistent values remain in the log when their decoded cache entries leave
// memory. Cache removal does not modify logical keys or WATCH versions.
func (db *Database) trimReadCache() {
	if db.rustEngine == nil {
		return
	}
	for i := range db.shards {
		if atomic.LoadInt64(&db.usedMemory) <= db.maxMemory {
			return
		}
		sh := &db.shards[i]
		sh.lock.Lock()
		for key, value := range sh.data {
			delete(sh.data, key)
			if value != nil {
				db.updateMemoryUsage(-int64(len(key)) - value.ApproximateSize())
			}
			if atomic.LoadInt64(&db.usedMemory) <= db.maxMemory {
				break
			}
		}
		sh.lock.Unlock()
	}
}

func (db *Database) cloneValue(value *datastruct.DataValue) *datastruct.DataValue {
	if value == nil {
		return nil
	}
	data, err := value.Serialize()
	if err != nil {
		db.recordLSMError(err)
		return nil
	}
	copy, err := deserializeDataValue(data)
	if err != nil {
		db.recordLSMError(err)
		return nil
	}
	return copy
}
