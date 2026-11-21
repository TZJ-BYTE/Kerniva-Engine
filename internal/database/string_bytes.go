package database

import (
	"fmt"
	"math"
	"math/bits"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/TZJ-BYTE/Kerniva/internal/datastruct"
	"github.com/TZJ-BYTE/Kerniva/pkg/logger"
)

func (db *Database) setStringBytesLocked(key, value []byte) error {
	if !db.evictIfNeeded() {
		return fmt.Errorf("OOM command not allowed when used memory (%d) > 'maxmemory' (%d)", atomic.LoadInt64(&db.usedMemory), db.maxMemory)
	}

	k := bytesToString(key)
	shard := db.getShard(k)

	var memDelta int64

	shard.lock.Lock()
	if shard.data == nil {
		shard.data = make(map[string]*datastruct.DataValue)
	}

	dv, exists := shard.data[k]
	if exists && dv != nil && !dv.IsExpired() {
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
		dv.LastAccessedAt = time.Now().UnixMilli()
		memDelta = dv.ApproximateSize() - oldSize
	} else {
		k = strings.Clone(k)
		b := make([]byte, len(value))
		copy(b, value)
		dv = &datastruct.DataValue{
			Value:          &datastruct.BytesString{Data: b},
			ExpireTime:     0,
			LastAccessedAt: time.Now().UnixMilli(),
		}
		shard.data[k] = dv
		memDelta = int64(len(k)) + dv.ApproximateSize()
	}
	shard.lock.Unlock()

	if memDelta != 0 {
		db.updateMemoryUsage(memDelta)
	}

	if db.hasPersistence() {
		dataBytes, err := dv.Serialize()
		if err != nil {
			db.recordLSMError(err)
			if db.persistenceWriteMode() == "weak" {
				logger.Error("Failed to serialize value for key %s: %v", k, err)
			} else {
				return err
			}
		} else {
			if err := db.persistencePut(k, dataBytes); err != nil {
				return err
			}
		}
	}

	db.BumpKeyVersions(k)
	return nil
}

func (db *Database) setStringBytesWithOptionsLocked(key, value []byte, ttlMs int64, keepTTL bool, nx bool, xx bool) (bool, error) {
	return db.setStringBytesWithExpiryOptionsLocked(key, value, ttlMs, 0, keepTTL, nx, xx)
}

func (db *Database) setStringBytesWithExpiryOptionsLocked(key, value []byte, ttlMs int64, expireAtMs int64, keepTTL bool, nx bool, xx bool) (bool, error) {
	if nx && xx {
		return false, fmt.Errorf("ERR syntax error")
	}
	if keepTTL && (ttlMs != 0 || expireAtMs != 0) {
		return false, fmt.Errorf("ERR syntax error")
	}
	if ttlMs != 0 && expireAtMs != 0 {
		return false, fmt.Errorf("ERR syntax error")
	}
	if !db.evictIfNeeded() {
		return false, fmt.Errorf("OOM command not allowed when used memory (%d) > 'maxmemory' (%d)", atomic.LoadInt64(&db.usedMemory), db.maxMemory)
	}

	k := bytesToString(key)
	if _, _, err := db.getLiveValue(k); err != nil {
		return false, err
	}
	shard := db.getShard(k)
	nowMs := time.Now().UnixMilli()

	var memDelta int64
	var stableKey string
	var dv *datastruct.DataValue

	shard.lock.Lock()
	if shard.data == nil {
		shard.data = make(map[string]*datastruct.DataValue)
	}

	cur, exists := shard.data[k]
	if exists && cur != nil && cur.IsExpired() {
		delete(shard.data, k)
		memDelta -= int64(len(k)) + cur.ApproximateSize()
		cur = nil
		exists = false
	}

	if nx && exists {
		shard.lock.Unlock()
		if memDelta != 0 {
			db.updateMemoryUsage(memDelta)
		}
		return false, nil
	}
	if xx && !exists {
		shard.lock.Unlock()
		if memDelta != 0 {
			db.updateMemoryUsage(memDelta)
		}
		return false, nil
	}

	expireAt := int64(0)
	if keepTTL && exists && cur != nil {
		expireAt = cur.ExpireTime
	} else if expireAtMs != 0 {
		expireAt = expireAtMs
	} else if ttlMs > 0 {
		expireAt = nowMs + ttlMs
	}

	b := make([]byte, len(value))
	copy(b, value)

	if exists && cur != nil && (cur.ExpireTime == 0 || nowMs <= cur.ExpireTime) {
		oldSize := cur.ApproximateSize()
		cur.Value = &datastruct.BytesString{Data: b}
		cur.ExpireTime = expireAt
		cur.LastAccessedAt = nowMs
		memDelta += cur.ApproximateSize() - oldSize
		stableKey = k
		dv = cur
	} else {
		stableKey = strings.Clone(k)
		dv = &datastruct.DataValue{
			Value:          &datastruct.BytesString{Data: b},
			ExpireTime:     expireAt,
			LastAccessedAt: nowMs,
		}
		shard.data[stableKey] = dv
		memDelta += int64(len(stableKey)) + dv.ApproximateSize()
	}
	shard.lock.Unlock()

	if memDelta != 0 {
		db.updateMemoryUsage(memDelta)
	}

	if db.hasPersistence() {
		dataBytes, err := dv.Serialize()
		if err != nil {
			db.recordLSMError(err)
			if db.persistenceWriteMode() == "weak" {
				logger.Error("Failed to serialize value for key %s: %v", stableKey, err)
				return true, nil
			}
			return true, err
		}
		if err := db.persistencePut(stableKey, dataBytes); err != nil {
			return true, err
		}
	}

	db.BumpKeyVersions(stableKey)
	return true, nil
}

func (db *Database) incrStringBytesLocked(key []byte) (int64, error) {
	return db.incrByStringBytes(key, 1)
}

func (db *Database) incrByStringBytesLocked(key []byte, delta int64) (int64, error) {
	return db.incrByStringBytes(key, delta)
}

func (db *Database) decrStringBytesLocked(key []byte) (int64, error) {
	return db.incrByStringBytes(key, -1)
}

func (db *Database) decrByStringBytesLocked(key []byte, delta int64) (int64, error) {
	return db.changeStringInteger(key, delta, true)
}

func (db *Database) incrByStringBytes(key []byte, delta int64) (int64, error) {
	return db.changeStringInteger(key, delta, false)
}

func (db *Database) changeStringInteger(key []byte, delta int64, subtract bool) (int64, error) {
	if !db.evictIfNeeded() {
		return 0, fmt.Errorf("OOM command not allowed when used memory (%d) > 'maxmemory' (%d)", atomic.LoadInt64(&db.usedMemory), db.maxMemory)
	}

	k := bytesToString(key)
	if _, _, err := db.getLiveValue(k); err != nil {
		return 0, err
	}
	shard := db.getShard(k)

	var out int64
	var memDelta int64
	var dv *datastruct.DataValue

	shard.lock.Lock()
	if shard.data == nil {
		shard.data = make(map[string]*datastruct.DataValue)
	}

	cur, exists := shard.data[k]
	if !exists || cur == nil || cur.IsExpired() {
		k = strings.Clone(k)
		out = delta
		if subtract {
			if delta == math.MinInt64 {
				shard.lock.Unlock()
				return 0, fmt.Errorf("ERR increment or decrement would overflow")
			}
			out = -delta
		}
		b := make([]byte, 0, 32)
		b = strconv.AppendInt(b, out, 10)
		dv = &datastruct.DataValue{
			Value:          &datastruct.BytesString{Data: b},
			ExpireTime:     0,
			LastAccessedAt: time.Now().UnixMilli(),
		}
		shard.data[k] = dv
		memDelta = int64(len(k)) + dv.ApproximateSize()
	} else {
		dv = cur
		oldSize := dv.ApproximateSize()
		var s string
		switch vv := dv.Value.(type) {
		case *datastruct.BytesString:
			s = bytesToString(vv.Data)
		case *datastruct.String:
			s = vv.Data
		default:
			shard.lock.Unlock()
			return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}

		current, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			shard.lock.Unlock()
			return 0, fmt.Errorf("ERR value is not an integer or out of range")
		}

		if subtract {
			if (delta > 0 && current < math.MinInt64+delta) || (delta < 0 && current > math.MaxInt64+delta) {
				shard.lock.Unlock()
				return 0, fmt.Errorf("ERR increment or decrement would overflow")
			}
			out = current - delta
		} else {
			if (delta > 0 && current > math.MaxInt64-delta) || (delta < 0 && current < math.MinInt64-delta) {
				shard.lock.Unlock()
				return 0, fmt.Errorf("ERR increment or decrement would overflow")
			}
			out = current + delta
		}
		switch vv := dv.Value.(type) {
		case *datastruct.BytesString:
			vv.Data = strconv.AppendInt(vv.Data[:0], out, 10)
		case *datastruct.String:
			b := make([]byte, 0, 32)
			b = strconv.AppendInt(b, out, 10)
			dv.Value = &datastruct.BytesString{Data: b}
		}
		dv.LastAccessedAt = time.Now().UnixMilli()
		memDelta = dv.ApproximateSize() - oldSize
	}
	shard.lock.Unlock()

	if memDelta != 0 {
		db.updateMemoryUsage(memDelta)
	}

	if db.hasPersistence() {
		dataBytes, err := dv.Serialize()
		if err != nil {
			db.recordLSMError(err)
			if db.persistenceWriteMode() == "weak" {
				logger.Error("Failed to serialize value for key %s: %v", k, err)
			} else {
				return 0, err
			}
		} else {
			if err := db.persistencePut(k, dataBytes); err != nil {
				return 0, err
			}
		}
	}

	db.BumpKeyVersions(k)
	return out, nil
}

func (db *Database) appendStringBytesLocked(key, suffix []byte) (int64, error) {
	if !db.evictIfNeeded() {
		return 0, fmt.Errorf("OOM command not allowed when used memory (%d) > 'maxmemory' (%d)", atomic.LoadInt64(&db.usedMemory), db.maxMemory)
	}

	k := bytesToString(key)
	if _, _, err := db.getLiveValue(k); err != nil {
		return 0, err
	}

	shard := db.getShard(k)
	nowMs := time.Now().UnixMilli()

	var memDelta int64
	var stableKey string
	var dv *datastruct.DataValue

	shard.lock.Lock()
	if shard.data == nil {
		shard.data = make(map[string]*datastruct.DataValue)
	}

	cur, exists := shard.data[k]
	if exists && cur != nil && !cur.IsExpired() {
		oldSize := cur.ApproximateSize()
		switch vv := cur.Value.(type) {
		case *datastruct.BytesString:
			vv.Data = append(vv.Data, suffix...)
		case *datastruct.String:
			b := make([]byte, 0, len(vv.Data)+len(suffix))
			b = append(b, vv.Data...)
			b = append(b, suffix...)
			cur.Value = &datastruct.BytesString{Data: b}
		default:
			shard.lock.Unlock()
			return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		cur.LastAccessedAt = nowMs
		memDelta = cur.ApproximateSize() - oldSize
		stableKey = k
		dv = cur
	} else {
		stableKey = strings.Clone(k)
		b := make([]byte, len(suffix))
		copy(b, suffix)
		dv = &datastruct.DataValue{
			Value:          &datastruct.BytesString{Data: b},
			ExpireTime:     0,
			LastAccessedAt: nowMs,
		}
		shard.data[stableKey] = dv
		memDelta = int64(len(stableKey)) + dv.ApproximateSize()
	}
	shard.lock.Unlock()

	if memDelta != 0 {
		db.updateMemoryUsage(memDelta)
	}

	if db.hasPersistence() {
		dataBytes, err := dv.Serialize()
		if err != nil {
			db.recordLSMError(err)
			if db.persistenceWriteMode() == "weak" {
				logger.Error("Failed to serialize value for key %s: %v", stableKey, err)
			} else {
				return 0, err
			}
		} else {
			if err := db.persistencePut(stableKey, dataBytes); err != nil {
				return 0, err
			}
		}
	}

	switch vv := dv.Value.(type) {
	case *datastruct.BytesString:
		db.BumpKeyVersions(stableKey)
		return int64(len(vv.Data)), nil
	case *datastruct.String:
		db.BumpKeyVersions(stableKey)
		return int64(len(vv.Data)), nil
	default:
		return 0, nil
	}
}

func (db *Database) strLenLocked(key []byte) (int64, error) {
	b, ok, err := db.getStringBytesCopyLocked(key)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, nil
	}
	return int64(len(b)), nil
}

func (db *Database) incrByFloatStringBytesLocked(key []byte, delta float64) (string, error) {
	if !db.evictIfNeeded() {
		return "", fmt.Errorf("OOM command not allowed when used memory (%d) > 'maxmemory' (%d)", atomic.LoadInt64(&db.usedMemory), db.maxMemory)
	}

	k := bytesToString(key)
	if _, _, err := db.getLiveValue(k); err != nil {
		return "", err
	}
	shard := db.getShard(k)

	var out float64
	var memDelta int64
	var dv *datastruct.DataValue

	shard.lock.Lock()
	if shard.data == nil {
		shard.data = make(map[string]*datastruct.DataValue)
	}

	cur, exists := shard.data[k]
	if !exists || cur == nil || cur.IsExpired() {
		k = strings.Clone(k)
		out = delta
		b := []byte(strconv.FormatFloat(out, 'f', -1, 64))
		dv = &datastruct.DataValue{
			Value:          &datastruct.BytesString{Data: b},
			ExpireTime:     0,
			LastAccessedAt: time.Now().UnixMilli(),
		}
		shard.data[k] = dv
		memDelta = int64(len(k)) + dv.ApproximateSize()
	} else {
		dv = cur
		oldSize := dv.ApproximateSize()
		var s string
		switch vv := dv.Value.(type) {
		case *datastruct.BytesString:
			s = bytesToString(vv.Data)
		case *datastruct.String:
			s = vv.Data
		default:
			shard.lock.Unlock()
			return "", fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}

		current, err := strconv.ParseFloat(s, 64)
		if err != nil {
			shard.lock.Unlock()
			return "", fmt.Errorf("ERR value is not a valid float")
		}
		out = current + delta
		b := []byte(strconv.FormatFloat(out, 'f', -1, 64))
		dv.Value = &datastruct.BytesString{Data: b}
		dv.LastAccessedAt = time.Now().UnixMilli()
		memDelta = dv.ApproximateSize() - oldSize
	}
	shard.lock.Unlock()

	if memDelta != 0 {
		db.updateMemoryUsage(memDelta)
	}
	if db.hasPersistence() {
		dataBytes, err := dv.Serialize()
		if err != nil {
			db.recordLSMError(err)
			if db.persistenceWriteMode() == "weak" {
				logger.Error("Failed to serialize value for key %s: %v", k, err)
			} else {
				return "", err
			}
		} else if err := db.persistencePut(k, dataBytes); err != nil {
			return "", err
		}
	}

	db.BumpKeyVersions(k)
	return strconv.FormatFloat(out, 'f', -1, 64), nil
}

func (db *Database) getRangeStringBytesLocked(key []byte, start, end int) (string, error) {
	b, ok, err := db.getStringBytesCopyLocked(key)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", nil
	}
	length := len(b)
	lo, hi, ok := normalizeRange(length, start, end)
	if !ok {
		return "", nil
	}
	return string(b[lo : hi+1]), nil
}

func (db *Database) setRangeStringBytesLocked(key []byte, offset int, value []byte) (int64, error) {
	if offset < 0 {
		return 0, fmt.Errorf("ERR offset is out of range")
	}
	if len(value) == 0 {
		b, ok, err := db.getStringBytesCopyLocked(key)
		if err != nil {
			return 0, err
		}
		if !ok {
			return 0, nil
		}
		return int64(len(b)), nil
	}
	if !db.evictIfNeeded() {
		return 0, fmt.Errorf("OOM command not allowed when used memory (%d) > 'maxmemory' (%d)", atomic.LoadInt64(&db.usedMemory), db.maxMemory)
	}

	k := bytesToString(key)
	if _, _, err := db.getLiveValue(k); err != nil {
		return 0, err
	}
	shard := db.getShard(k)
	nowMs := time.Now().UnixMilli()

	var memDelta int64
	var stableKey string
	var dv *datastruct.DataValue
	var outLen int64

	shard.lock.Lock()
	if shard.data == nil {
		shard.data = make(map[string]*datastruct.DataValue)
	}

	cur, exists := shard.data[k]
	if exists && cur != nil && !cur.IsExpired() {
		oldSize := cur.ApproximateSize()
		var data []byte
		switch vv := cur.Value.(type) {
		case *datastruct.BytesString:
			data = append([]byte(nil), vv.Data...)
		case *datastruct.String:
			data = []byte(vv.Data)
		default:
			shard.lock.Unlock()
			return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		newLen := offset + len(value)
		if newLen > len(data) {
			expanded := make([]byte, newLen)
			copy(expanded, data)
			data = expanded
		}
		copy(data[offset:], value)
		cur.Value = &datastruct.BytesString{Data: data}
		cur.LastAccessedAt = nowMs
		memDelta = cur.ApproximateSize() - oldSize
		stableKey = k
		dv = cur
		outLen = int64(len(data))
	} else {
		stableKey = strings.Clone(k)
		data := make([]byte, offset+len(value))
		copy(data[offset:], value)
		dv = &datastruct.DataValue{
			Value:          &datastruct.BytesString{Data: data},
			ExpireTime:     0,
			LastAccessedAt: nowMs,
		}
		shard.data[stableKey] = dv
		memDelta = int64(len(stableKey)) + dv.ApproximateSize()
		outLen = int64(len(data))
	}
	shard.lock.Unlock()

	if memDelta != 0 {
		db.updateMemoryUsage(memDelta)
	}
	if db.hasPersistence() {
		dataBytes, err := dv.Serialize()
		if err != nil {
			db.recordLSMError(err)
			if db.persistenceWriteMode() == "weak" {
				logger.Error("Failed to serialize value for key %s: %v", stableKey, err)
			} else {
				return 0, err
			}
		} else if err := db.persistencePut(stableKey, dataBytes); err != nil {
			return 0, err
		}
	}

	db.BumpKeyVersions(stableKey)
	return outLen, nil
}

func (db *Database) getBitStringBytesLocked(key []byte, offset int64) (int64, error) {
	if offset < 0 {
		return 0, fmt.Errorf("ERR bit offset is not an integer or out of range")
	}
	b, ok, err := db.getStringBytesCopyLocked(key)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, nil
	}
	byteIndex := int(offset / 8)
	if byteIndex >= len(b) {
		return 0, nil
	}
	bitIndex := 7 - uint(offset%8)
	if b[byteIndex]&(1<<bitIndex) != 0 {
		return 1, nil
	}
	return 0, nil
}

func (db *Database) setBitStringBytesLocked(key []byte, offset int64, bit int) (int64, error) {
	if offset < 0 {
		return 0, fmt.Errorf("ERR bit offset is not an integer or out of range")
	}
	if bit != 0 && bit != 1 {
		return 0, fmt.Errorf("ERR bit is not an integer or out of range")
	}
	if !db.evictIfNeeded() {
		return 0, fmt.Errorf("OOM command not allowed when used memory (%d) > 'maxmemory' (%d)", atomic.LoadInt64(&db.usedMemory), db.maxMemory)
	}

	k := bytesToString(key)
	if _, _, err := db.getLiveValue(k); err != nil {
		return 0, err
	}
	shard := db.getShard(k)
	nowMs := time.Now().UnixMilli()

	var memDelta int64
	var stableKey string
	var dv *datastruct.DataValue
	var oldBit int64

	shard.lock.Lock()
	if shard.data == nil {
		shard.data = make(map[string]*datastruct.DataValue)
	}
	cur, exists := shard.data[k]
	byteIndex := int(offset / 8)
	mask := byte(1 << (7 - uint(offset%8)))

	if exists && cur != nil && !cur.IsExpired() {
		oldSize := cur.ApproximateSize()
		var data []byte
		switch vv := cur.Value.(type) {
		case *datastruct.BytesString:
			data = append([]byte(nil), vv.Data...)
		case *datastruct.String:
			data = []byte(vv.Data)
		default:
			shard.lock.Unlock()
			return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		if byteIndex >= len(data) {
			expanded := make([]byte, byteIndex+1)
			copy(expanded, data)
			data = expanded
		}
		if data[byteIndex]&mask != 0 {
			oldBit = 1
		}
		if bit == 1 {
			data[byteIndex] |= mask
		} else {
			data[byteIndex] &^= mask
		}
		cur.Value = &datastruct.BytesString{Data: data}
		cur.LastAccessedAt = nowMs
		memDelta = cur.ApproximateSize() - oldSize
		stableKey = k
		dv = cur
	} else {
		stableKey = strings.Clone(k)
		data := make([]byte, byteIndex+1)
		if bit == 1 {
			data[byteIndex] |= mask
		}
		dv = &datastruct.DataValue{
			Value:          &datastruct.BytesString{Data: data},
			ExpireTime:     0,
			LastAccessedAt: nowMs,
		}
		shard.data[stableKey] = dv
		memDelta = int64(len(stableKey)) + dv.ApproximateSize()
	}
	shard.lock.Unlock()

	if memDelta != 0 {
		db.updateMemoryUsage(memDelta)
	}
	if db.hasPersistence() {
		dataBytes, err := dv.Serialize()
		if err != nil {
			db.recordLSMError(err)
			if db.persistenceWriteMode() == "weak" {
				logger.Error("Failed to serialize value for key %s: %v", stableKey, err)
			} else {
				return 0, err
			}
		} else if err := db.persistencePut(stableKey, dataBytes); err != nil {
			return 0, err
		}
	}
	db.BumpKeyVersions(stableKey)
	return oldBit, nil
}

func (db *Database) bitCountStringBytesLocked(key []byte, start, end *int) (int64, error) {
	b, ok, err := db.getStringBytesCopyLocked(key)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, nil
	}
	if start != nil && end != nil {
		lo, hi, ok := normalizeRange(len(b), *start, *end)
		if !ok {
			return 0, nil
		}
		b = b[lo : hi+1]
	}
	var count int64
	for _, by := range b {
		count += int64(bits.OnesCount8(by))
	}
	return count, nil
}

func (db *Database) bitOpStringBytesLocked(op string, destKey []byte, sourceKeys [][]byte) (int64, error) {
	if len(sourceKeys) == 0 {
		return 0, fmt.Errorf("ERR wrong number of arguments for 'bitop' command")
	}

	switch op {
	case "AND", "OR", "XOR":
	case "NOT":
		if len(sourceKeys) != 1 {
			return 0, fmt.Errorf("ERR BITOP NOT must be called with a single source key")
		}
	default:
		return 0, fmt.Errorf("ERR syntax error")
	}

	sources := make([][]byte, len(sourceKeys))
	maxLen := 0
	for i := range sourceKeys {
		data, ok, err := db.getStringBytesCopyLocked(sourceKeys[i])
		if err != nil {
			return 0, err
		}
		if ok {
			sources[i] = data
			if len(data) > maxLen {
				maxLen = len(data)
			}
		}
	}

	result := make([]byte, maxLen)
	switch op {
	case "AND":
		for i := 0; i < maxLen; i++ {
			var value byte
			if len(sources) > 0 && i < len(sources[0]) {
				value = sources[0][i]
			}
			for j := 1; j < len(sources); j++ {
				var current byte
				if i < len(sources[j]) {
					current = sources[j][i]
				}
				value &= current
			}
			result[i] = value
		}
	case "OR":
		for i := 0; i < maxLen; i++ {
			var value byte
			for j := range sources {
				if i < len(sources[j]) {
					value |= sources[j][i]
				}
			}
			result[i] = value
		}
	case "XOR":
		for i := 0; i < maxLen; i++ {
			var value byte
			for j := range sources {
				if i < len(sources[j]) {
					value ^= sources[j][i]
				}
			}
			result[i] = value
		}
	case "NOT":
		for i := range sources[0] {
			result[i] = ^sources[0][i]
		}
	}

	if err := db.setStringBytesLocked(destKey, result); err != nil {
		return 0, err
	}
	return int64(len(result)), nil
}

func (db *Database) bitPosStringBytesLocked(key []byte, bit int, start, end *int64, mode string) (int64, error) {
	if bit != 0 && bit != 1 {
		return 0, fmt.Errorf("ERR bit is not an integer or out of range")
	}

	data, ok, err := db.getStringBytesCopyLocked(key)
	if err != nil {
		return 0, err
	}
	if !ok || len(data) == 0 {
		if bit == 0 && start == nil && end == nil {
			return 0, nil
		}
		return -1, nil
	}

	unit := strings.ToUpper(mode)
	if unit == "" {
		unit = "BYTE"
	}
	if unit != "BYTE" && unit != "BIT" {
		return 0, fmt.Errorf("ERR syntax error")
	}

	rangeSpecified := start != nil || end != nil
	bitStart := 0
	bitEnd := len(data)*8 - 1

	if rangeSpecified {
		if unit == "BYTE" {
			startByte := int64(0)
			endByte := int64(len(data) - 1)
			if start != nil {
				startByte = *start
			}
			if end != nil {
				endByte = *end
			}
			lo, hi, ok := normalizeRange(len(data), int(startByte), int(endByte))
			if !ok {
				return -1, nil
			}
			bitStart = lo * 8
			bitEnd = hi*8 + 7
		} else {
			totalBits := len(data) * 8
			startBit := int64(0)
			endBit := int64(totalBits - 1)
			if start != nil {
				startBit = *start
			}
			if end != nil {
				endBit = *end
			}
			lo, hi, ok := normalizeRange(totalBits, int(startBit), int(endBit))
			if !ok {
				return -1, nil
			}
			bitStart = lo
			bitEnd = hi
		}
	}

	for pos := bitStart; pos <= bitEnd; pos++ {
		byteIndex := pos / 8
		mask := byte(1 << (7 - uint(pos%8)))
		currentBit := 0
		if data[byteIndex]&mask != 0 {
			currentBit = 1
		}
		if currentBit == bit {
			return int64(pos), nil
		}
	}

	if bit == 0 && !rangeSpecified {
		return int64(len(data) * 8), nil
	}
	return -1, nil
}

func (db *Database) SetStringBytes(key, value []byte) (err error) {
	k := bytesToString(key)
	parallel := db.beginSetOperation(k)
	if parallel && db.hasPersistence() && !db.closed {
		handled, writeErr := db.setPersistentString(key, value)
		if handled {
			db.endKeyOperation(k, true, true)
			return writeErr
		}
		// Fall back only after releasing both lower-level locks. Another
		// command may change the key before the exclusive path rechecks it.
		db.endKeyOperation(k, true, true)
		db.operationMu.Lock()
		parallel = false
	}
	defer db.endKeyOperation(k, true, parallel)
	if db.closed {
		err = errDatabaseClosed
		return
	}
	if parallel {
		return db.setMemoryString(key, value)
	}

	db.beginMutation()
	defer func() {
		err = db.finishMutation(err)
	}()

	return db.setStringBytesLocked(key, value)
}

func (db *Database) SetStringBytesWithOptions(key, value []byte, ttlMs int64, keepTTL bool, nx bool, xx bool) (result0 bool, err error) {
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

	return db.setStringBytesWithOptionsLocked(key, value, ttlMs, keepTTL, nx, xx)
}

func (db *Database) SetStringBytesWithExpiryOptions(key, value []byte, ttlMs int64, expireAtMs int64, keepTTL bool, nx bool, xx bool) (result0 bool, err error) {
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

	return db.setStringBytesWithExpiryOptionsLocked(key, value, ttlMs, expireAtMs, keepTTL, nx, xx)
}

func (db *Database) IncrStringBytes(key []byte) (result0 int64, err error) {
	parallel := db.beginKeyOperation(bytesToString(key), true)
	defer db.endKeyOperation(bytesToString(key), true, parallel)
	if db.closed {
		err = errDatabaseClosed
		return
	}

	db.beginMutation()
	defer func() {
		err = db.finishMutation(err)
	}()

	return db.incrStringBytesLocked(key)
}

func (db *Database) IncrByStringBytes(key []byte, delta int64) (result0 int64, err error) {
	parallel := db.beginKeyOperation(bytesToString(key), true)
	defer db.endKeyOperation(bytesToString(key), true, parallel)
	if db.closed {
		err = errDatabaseClosed
		return
	}

	db.beginMutation()
	defer func() {
		err = db.finishMutation(err)
	}()

	return db.incrByStringBytesLocked(key, delta)
}

func (db *Database) DecrStringBytes(key []byte) (result0 int64, err error) {
	parallel := db.beginKeyOperation(bytesToString(key), true)
	defer db.endKeyOperation(bytesToString(key), true, parallel)
	if db.closed {
		err = errDatabaseClosed
		return
	}

	db.beginMutation()
	defer func() {
		err = db.finishMutation(err)
	}()

	return db.decrStringBytesLocked(key)
}

func (db *Database) DecrByStringBytes(key []byte, delta int64) (result0 int64, err error) {
	parallel := db.beginKeyOperation(bytesToString(key), true)
	defer db.endKeyOperation(bytesToString(key), true, parallel)
	if db.closed {
		err = errDatabaseClosed
		return
	}

	db.beginMutation()
	defer func() {
		err = db.finishMutation(err)
	}()

	return db.decrByStringBytesLocked(key, delta)
}

func (db *Database) AppendStringBytes(key, suffix []byte) (result0 int64, err error) {
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

	return db.appendStringBytesLocked(key, suffix)
}

func (db *Database) StrLen(key []byte) (result0 int64, err error) {
	db.operationMu.Lock()
	defer db.endReadOperation()
	if db.closed {
		err = errDatabaseClosed
		return
	}
	return db.strLenLocked(key)
}

func (db *Database) IncrByFloatStringBytes(key []byte, delta float64) (result0 string, err error) {
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

	return db.incrByFloatStringBytesLocked(key, delta)
}

func (db *Database) GetRangeStringBytes(key []byte, start, end int) (result0 string, err error) {
	db.operationMu.Lock()
	defer db.endReadOperation()
	if db.closed {
		err = errDatabaseClosed
		return
	}
	return db.getRangeStringBytesLocked(key, start, end)
}

func (db *Database) SetRangeStringBytes(key []byte, offset int, value []byte) (result0 int64, err error) {
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

	return db.setRangeStringBytesLocked(key, offset, value)
}

func (db *Database) GetBitStringBytes(key []byte, offset int64) (result0 int64, err error) {
	db.operationMu.Lock()
	defer db.endReadOperation()
	if db.closed {
		err = errDatabaseClosed
		return
	}
	return db.getBitStringBytesLocked(key, offset)
}

func (db *Database) SetBitStringBytes(key []byte, offset int64, bit int) (result0 int64, err error) {
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

	return db.setBitStringBytesLocked(key, offset, bit)
}

func (db *Database) BitCountStringBytes(key []byte, start, end *int) (result0 int64, err error) {
	db.operationMu.Lock()
	defer db.endReadOperation()
	if db.closed {
		err = errDatabaseClosed
		return
	}
	return db.bitCountStringBytesLocked(key, start, end)
}

func (db *Database) BitOpStringBytes(op string, destKey []byte, sourceKeys [][]byte) (result0 int64, err error) {
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

	return db.bitOpStringBytesLocked(op, destKey, sourceKeys)
}

func (db *Database) BitPosStringBytes(key []byte, bit int, start, end *int64, mode string) (result0 int64, err error) {
	db.operationMu.Lock()
	defer db.endReadOperation()
	if db.closed {
		err = errDatabaseClosed
		return
	}
	return db.bitPosStringBytesLocked(key, bit, start, end, mode)
}
