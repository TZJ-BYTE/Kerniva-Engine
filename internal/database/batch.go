package database

import (
	"fmt"
	"strings"
	"time"

	"github.com/TZJ-BYTE/Kerniva/internal/datastruct"
	"github.com/TZJ-BYTE/Kerniva/internal/protocol"
	"github.com/TZJ-BYTE/Kerniva/pkg/logger"
)

type shardKV struct {
	idx int
	key string
	val []byte
}

type lsmPut struct {
	key   string
	bytes []byte
}

func (db *Database) mSetStringBytesLocked(args [][]byte) error {
	if len(args) < 2 || len(args)%2 != 0 {
		return fmt.Errorf("ERR wrong number of arguments for 'mset' command")
	}

	if !db.evictIfNeeded() {
		return fmt.Errorf("OOM command not allowed when used memory (%d) > 'maxmemory' (%d)", db.usedMemory, db.maxMemory)
	}

	groups := make([][]shardKV, ShardCount)
	for i := 0; i < len(args); i += 2 {
		k := bytesToString(args[i])
		db.getShard(k)
		si := getShardIndex(k)
		groups[si] = append(groups[si], shardKV{key: k, val: args[i+1]})
	}

	var memDeltaTotal int64
	touchedKeys := make([]string, 0, len(args)/2)
	nowMs := time.Now().UnixMilli()

	for si := 0; si < ShardCount; si++ {
		items := groups[si]
		if len(items) == 0 {
			continue
		}
		shard := &db.shards[si]
		var puts []lsmPut
		shard.lock.Lock()
		if shard.data == nil {
			shard.data = make(map[string]*datastruct.DataValue)
		}
		for _, it := range items {
			k := it.key
			value := it.val

			var memDelta int64
			dv, exists := shard.data[k]
			if exists && dv != nil && (dv.ExpireTime == 0 || nowMs <= dv.ExpireTime) {
				oldSize := dv.ApproximateSize()
				dv.ExpireTime = 0
				switch vv := dv.Value.(type) {
				case *datastruct.BytesString:
					if cap(vv.Data) >= len(value) {
						vv.Data = vv.Data[:len(value)]
					} else {
						vv.Data = make([]byte, len(value))
					}
					copy(vv.Data, value)
				case *datastruct.String:
					b := make([]byte, len(value))
					copy(b, value)
					dv.Value = &datastruct.BytesString{Data: b}
				default:
					b := make([]byte, len(value))
					copy(b, value)
					dv.Value = &datastruct.BytesString{Data: b}
				}
				dv.LastAccessedAt = nowMs
				memDelta = dv.ApproximateSize() - oldSize
			} else {
				stableKey := strings.Clone(k)
				b := make([]byte, len(value))
				copy(b, value)
				dv = &datastruct.DataValue{
					Value:          &datastruct.BytesString{Data: b},
					ExpireTime:     0,
					LastAccessedAt: nowMs,
				}
				shard.data[stableKey] = dv
				memDelta = int64(len(stableKey)) + dv.ApproximateSize()
				k = stableKey
			}

			memDeltaTotal += memDelta
			touchedKeys = append(touchedKeys, k)

			if db.hasPersistence() {
				dataBytes, err := dv.Serialize()
				if err == nil {
					puts = append(puts, lsmPut{key: k, bytes: dataBytes})
				} else {
					db.recordLSMError(err)
					if db.persistenceWriteMode() == "weak" {
						logger.Error("Failed to serialize value for key %s: %v", k, err)
					} else {
						shard.lock.Unlock()
						return err
					}
				}
			}
		}
		shard.lock.Unlock()

		if db.hasPersistence() && len(puts) > 0 {
			for _, p := range puts {
				if err := db.persistencePut(p.key, p.bytes); err != nil {
					return err
				}
			}
		}
	}

	if memDeltaTotal != 0 {
		db.updateMemoryUsage(memDeltaTotal)
	}
	if len(touchedKeys) > 0 {
		db.BumpKeyVersions(touchedKeys...)
	}

	return nil
}

func (db *Database) mGetStringsLocked(keys [][]byte) []string {
	out := make([]string, len(keys))
	groups := make([][]shardKV, ShardCount)
	for i, kb := range keys {
		k := bytesToString(kb)
		if _, _, err := db.getLiveValue(k); err != nil {
			db.recordLSMError(err)
			continue
		}
		si := getShardIndex(k)
		groups[si] = append(groups[si], shardKV{idx: i, key: k})
	}

	now := time.Now().UnixMilli()
	for si := 0; si < ShardCount; si++ {
		items := groups[si]
		if len(items) == 0 {
			continue
		}
		shard := &db.shards[si]
		shard.lock.RLock()
		for _, it := range items {
			dv := shard.data[it.key]
			if dv == nil {
				continue
			}
			if dv.ExpireTime > 0 && now > dv.ExpireTime {
				continue
			}
			switch v := dv.Value.(type) {
			case *datastruct.String:
				out[it.idx] = v.Data
			case *datastruct.BytesString:
				b := make([]byte, len(v.Data))
				copy(b, v.Data)
				out[it.idx] = protocol.BytesToString(b)
			default:
				out[it.idx] = ""
			}
			if db.keyHeat != nil {
				db.keyHeat.Add(it.key)
			}
		}
		shard.lock.RUnlock()
	}

	return out
}

func (db *Database) mGetStringCopiesLocked(keys [][]byte) ([]string, []bool, error) {
	out := make([]string, len(keys))
	ok := make([]bool, len(keys))

	for i, kb := range keys {
		k := bytesToString(kb)
		dv, exists, err := db.getLiveValue(k)
		if err != nil {
			return nil, nil, err
		}
		if !exists || dv == nil {
			continue
		}

		switch v := dv.Value.(type) {
		case *datastruct.String:
			out[i] = v.Data
			ok[i] = true
		case *datastruct.BytesString:
			b := make([]byte, len(v.Data))
			copy(b, v.Data)
			out[i] = protocol.BytesToString(b)
			ok[i] = true
		case nil:
		default:
			return nil, nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}

		if ok[i] && db.keyHeat != nil {
			db.keyHeat.Add(k)
		}
	}

	return out, ok, nil
}

func (db *Database) mSetNXStringBytesLocked(args [][]byte) (bool, error) {
	if len(args) < 2 || len(args)%2 != 0 {
		return false, fmt.Errorf("ERR wrong number of arguments for 'msetnx' command")
	}

	for i := 0; i < len(args); i += 2 {
		if db.existsLocked(bytesToString(args[i])) {
			return false, nil
		}
	}
	if err := db.mSetStringBytesLocked(args); err != nil {
		return false, err
	}
	return true, nil
}

func (db *Database) MSetStringBytes(args [][]byte) (err error) {
	db.operationMu.Lock()
	defer db.operationMu.Unlock()
	if db.closed {
		err = errDatabaseClosed
		return
	}

	db.beginMutation()
	defer func() {
		err = db.finishMutation(err)
	}()

	return db.mSetStringBytesLocked(args)
}

func (db *Database) MGetStrings(keys [][]byte) (result0 []string) {
	db.operationMu.Lock()
	defer db.endReadOperation()
	if db.closed {
		return
	}
	return db.mGetStringsLocked(keys)
}

func (db *Database) MGetStringCopies(keys [][]byte) (result0 []string, result1 []bool, err error) {
	db.operationMu.Lock()
	defer db.endReadOperation()
	if db.closed {
		err = errDatabaseClosed
		return
	}
	return db.mGetStringCopiesLocked(keys)
}

func (db *Database) MSetNXStringBytes(args [][]byte) (result0 bool, err error) {
	db.operationMu.Lock()
	defer db.operationMu.Unlock()
	if db.closed {
		err = errDatabaseClosed
		return
	}

	db.beginMutation()
	defer func() {
		err = db.finishMutation(err)
	}()

	return db.mSetNXStringBytesLocked(args)
}
