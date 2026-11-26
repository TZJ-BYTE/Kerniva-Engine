package database

import (
	"strings"
	"sync/atomic"
	"time"

	"github.com/TZJ-BYTE/Kerniva/internal/datastruct"
	"github.com/TZJ-BYTE/Kerniva/internal/rustengine"
	"github.com/TZJ-BYTE/Kerniva/internal/writetrace"
)

// setPersistentString enters and returns with the lifecycle read lock and the
// stripe write lock held. The pending entry orders same-key operations while
// other keys in this stripe can proceed during the durable write.
// Returning handled=false asks the caller to recheck under operationMu.Lock.
func (db *Database) setPersistentString(key, value []byte, row *writetrace.Request) (handled bool, err error) {
	k := bytesToString(key)
	sh := db.getShard(k)
	sh.lock.RLock()
	previous := sh.data[k]
	sh.lock.RUnlock()
	var oldSize int64
	if previous == nil {
		// Go-log keeps all live keys resident, so a cache miss is safe to insert.
		if db.goLog == nil || db.config.GoLogGroupMax <= 1 {
			return false, nil
		}
	} else {
		if previous.ExpireTime != 0 {
			return false, nil
		}
		switch previous.Value.(type) {
		case *datastruct.String, *datastruct.BytesString:
		default:
			return false, nil
		}
		oldSize = int64(len(key)) + previous.ApproximateSize()
	}

	cacheSize := int64(len(key)) + int64(len(value)) + 24
	// V3 strings contain a 22-byte header. Serialize starts with capacity 128
	// and may grow its buffer. Reserve a conservative bound before allocating.
	encodedBudget := max(int64(128), 2*(int64(len(value))+22))
	reservation := cacheSize + encodedBudget
	for {
		used := atomic.LoadInt64(&db.usedMemory)
		if reservation > db.maxMemory-used {
			return false, nil
		}
		if atomic.CompareAndSwapInt64(&db.usedMemory, used, used+reservation) {
			break
		}
	}
	db.pendingMemory.Add(reservation)
	memoryDelta := -reservation
	stableKey := strings.Clone(k)
	index := getShardIndex(stableKey)
	var pendingDone chan struct{}
	defer func() {
		db.updateMemoryUsage(memoryDelta)
		db.pendingMemory.Add(-reservation)
		if pendingDone != nil {
			delete(db.pendingStringWrites[index], stableKey)
			close(pendingDone)
		}
	}()

	next := &datastruct.DataValue{
		Value:          &datastruct.BytesString{Data: append([]byte(nil), value...)},
		LastAccessedAt: time.Now().UnixMilli(),
	}
	encoded, err := next.Serialize()
	if err != nil {
		db.recordLSMError(err)
		return true, err
	}
	if db.pendingStringWrites[index] == nil {
		db.pendingStringWrites[index] = make(map[string]chan struct{})
	}
	pendingDone = make(chan struct{})
	db.pendingStringWrites[index][stableKey] = pendingDone
	lock := &db.hotLocks[index]
	lock.Unlock()
	err = db.writeBatchTraced([]rustengine.WriteOp{{Key: stringToBytesRO(stableKey), Value: encoded}}, row)
	lock.Lock()
	if err != nil {
		db.recordLSMError(err)
		return true, err
	}

	// Same-key waiters remain asleep until cache, WATCH and budget publication
	// complete. The pending value was private throughout the durable write.
	sh.lock.Lock()
	if sh.data == nil {
		sh.data = make(map[string]*datastruct.DataValue)
	}
	sh.data[stableKey] = next
	sh.lock.Unlock()
	memoryDelta += cacheSize - oldSize
	db.BumpKeyVersions(stableKey)
	return true, nil
}
