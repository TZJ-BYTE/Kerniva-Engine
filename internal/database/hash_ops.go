package database

import (
	"fmt"
	"math/rand/v2"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/TZJ-BYTE/Kerniva/internal/datastruct"
	"github.com/TZJ-BYTE/Kerniva/pkg/logger"
)

func (db *Database) hSetLocked(key, field, value string) (int64, error) {
	if _, _, err := db.getLiveValue(key); err != nil {
		return 0, err
	}
	if !db.evictIfNeeded() {
		return 0, fmt.Errorf("OOM command not allowed when used memory (%d) > 'maxmemory' (%d)", db.usedMemory, db.maxMemory)
	}

	sh := db.getShard(key)
	now := time.Now().UnixMilli()

	var memDelta int64
	var stableKey string
	var lsmBytes []byte
	var created int64
	var serErr error

	sh.lock.Lock()
	if sh.data == nil {
		sh.data = make(map[string]*datastruct.DataValue)
	}
	dv, exists := sh.data[key]
	if !exists {
		stableKey = strings.Clone(key)
		dv = &datastruct.DataValue{ExpireTime: 0, LastAccessedAt: now}
		sh.data[stableKey] = dv
		memDelta += int64(len(stableKey))
	} else {
		stableKey = key
	}

	oldSize := dv.ApproximateSize()
	var h *datastruct.Hash
	if dv.Value == nil || (dv.ExpireTime > 0 && now > dv.ExpireTime) {
		h = &datastruct.Hash{Data: make(map[string]string)}
		dv.Value = h
		dv.ExpireTime = 0
		created = 1
	} else {
		hv, ok := dv.Value.(*datastruct.Hash)
		if !ok {
			sh.lock.Unlock()
			return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		h = hv
		if h.Data == nil {
			h.Data = make(map[string]string)
		}
		if _, ok := h.Data[field]; !ok {
			created = 1
		}
	}

	h.Data[strings.Clone(field)] = strings.Clone(value)
	dv.LastAccessedAt = now
	newSize := dv.ApproximateSize()
	memDelta += newSize - oldSize

	if db.hasPersistence() {
		b, err := dv.Serialize()
		if err == nil {
			lsmBytes = b
		} else {
			serErr = err
		}
	}
	sh.lock.Unlock()

	if serErr != nil {
		db.recordLSMError(serErr)
		if db.persistenceWriteMode() == "weak" {
			logger.Error("Failed to serialize value for key %s: %v", stableKey, serErr)
		} else {
			return 0, serErr
		}
	}

	if memDelta != 0 {
		db.updateMemoryUsage(memDelta)
	}
	if lsmBytes != nil {
		if err := db.persistencePut(stableKey, lsmBytes); err != nil {
			return 0, err
		}
	}
	db.BumpKeyVersions(stableKey)
	return created, nil
}

func (db *Database) hMSetLocked(key string, fields []string, values []string) error {
	if len(fields) != len(values) {
		return fmt.Errorf("ERR wrong number of arguments for 'hmset' command")
	}
	if len(fields) == 0 {
		return nil
	}
	if _, _, err := db.getLiveValue(key); err != nil {
		return err
	}
	if !db.evictIfNeeded() {
		return fmt.Errorf("OOM command not allowed when used memory (%d) > 'maxmemory' (%d)", db.usedMemory, db.maxMemory)
	}

	sh := db.getShard(key)
	now := time.Now().UnixMilli()

	var memDelta int64
	var stableKey string
	var lsmBytes []byte
	var serErr error

	sh.lock.Lock()
	if sh.data == nil {
		sh.data = make(map[string]*datastruct.DataValue)
	}
	dv, exists := sh.data[key]
	if !exists {
		stableKey = strings.Clone(key)
		dv = &datastruct.DataValue{ExpireTime: 0, LastAccessedAt: now}
		sh.data[stableKey] = dv
		memDelta += int64(len(stableKey))
	} else {
		stableKey = key
	}

	oldSize := dv.ApproximateSize()
	var h *datastruct.Hash
	if dv.Value == nil || (dv.ExpireTime > 0 && now > dv.ExpireTime) {
		h = &datastruct.Hash{Data: make(map[string]string)}
		dv.Value = h
		dv.ExpireTime = 0
	} else {
		hv, ok := dv.Value.(*datastruct.Hash)
		if !ok {
			sh.lock.Unlock()
			return fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		h = hv
		if h.Data == nil {
			h.Data = make(map[string]string)
		}
	}

	for i := range fields {
		h.Data[strings.Clone(fields[i])] = strings.Clone(values[i])
	}
	dv.LastAccessedAt = now
	newSize := dv.ApproximateSize()
	memDelta += newSize - oldSize

	if db.hasPersistence() {
		b, err := dv.Serialize()
		if err == nil {
			lsmBytes = b
		} else {
			serErr = err
		}
	}
	sh.lock.Unlock()

	if serErr != nil {
		db.recordLSMError(serErr)
		if db.persistenceWriteMode() == "weak" {
			logger.Error("Failed to serialize value for key %s: %v", stableKey, serErr)
		} else {
			return serErr
		}
	}

	if memDelta != 0 {
		db.updateMemoryUsage(memDelta)
	}
	if lsmBytes != nil {
		if err := db.persistencePut(stableKey, lsmBytes); err != nil {
			return err
		}
	}
	db.BumpKeyVersions(stableKey)
	return nil
}

func (db *Database) hGetLocked(key, field string) (string, bool, error) {
	if _, _, err := db.getLiveValue(key); err != nil {
		return "", false, err
	}
	sh := db.getShard(key)
	now := time.Now().UnixMilli()

	sh.lock.RLock()
	dv, exists := sh.data[key]
	if !exists || dv == nil || (dv.ExpireTime > 0 && now > dv.ExpireTime) {
		sh.lock.RUnlock()
		return "", false, nil
	}
	h, ok := dv.Value.(*datastruct.Hash)
	if !ok {
		sh.lock.RUnlock()
		return "", false, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	if h.Data == nil {
		sh.lock.RUnlock()
		return "", false, nil
	}
	v, ok := h.Data[field]
	sh.lock.RUnlock()
	return v, ok, nil
}

func (db *Database) hMGetLocked(key string, fields []string) ([]string, error) {
	out := make([]string, len(fields))
	if _, _, err := db.getLiveValue(key); err != nil {
		return nil, err
	}
	sh := db.getShard(key)
	now := time.Now().UnixMilli()

	sh.lock.RLock()
	dv, exists := sh.data[key]
	if !exists || dv == nil || (dv.ExpireTime > 0 && now > dv.ExpireTime) {
		sh.lock.RUnlock()
		return out, nil
	}
	h, ok := dv.Value.(*datastruct.Hash)
	if !ok {
		sh.lock.RUnlock()
		return nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	for i, f := range fields {
		if h.Data == nil {
			out[i] = ""
			continue
		}
		out[i] = h.Data[f]
	}
	sh.lock.RUnlock()
	return out, nil
}

func (db *Database) hMGetWithExistsLocked(key string, fields []string) ([]string, []bool, error) {
	out := make([]string, len(fields))
	ok := make([]bool, len(fields))
	if _, _, err := db.getLiveValue(key); err != nil {
		return nil, nil, err
	}
	sh := db.getShard(key)
	now := time.Now().UnixMilli()

	sh.lock.RLock()
	dv, exists := sh.data[key]
	if !exists || dv == nil || (dv.ExpireTime > 0 && now > dv.ExpireTime) {
		sh.lock.RUnlock()
		return out, ok, nil
	}
	h, ht := dv.Value.(*datastruct.Hash)
	if !ht {
		sh.lock.RUnlock()
		return nil, nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	for i, f := range fields {
		if h.Data == nil {
			continue
		}
		v, okk := h.Data[f]
		if okk {
			out[i] = v
			ok[i] = true
		}
	}
	sh.lock.RUnlock()
	return out, ok, nil
}

func (db *Database) hDelLocked(key string, fields []string) (int64, error) {
	if len(fields) == 0 {
		return 0, nil
	}
	if _, _, err := db.getLiveValue(key); err != nil {
		return 0, err
	}

	sh := db.getShard(key)
	now := time.Now().UnixMilli()

	var memDelta int64
	var deleted int64
	var lsmBytes []byte
	var deleteKey bool
	var serErr error

	sh.lock.Lock()
	dv, exists := sh.data[key]
	if !exists || dv == nil || (dv.ExpireTime > 0 && now > dv.ExpireTime) {
		sh.lock.Unlock()
		return 0, nil
	}
	h, ok := dv.Value.(*datastruct.Hash)
	if !ok {
		sh.lock.Unlock()
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	oldSize := dv.ApproximateSize()
	if h.Data != nil {
		for _, f := range fields {
			if _, ok := h.Data[f]; ok {
				delete(h.Data, f)
				deleted++
			}
		}
	}
	if h.Size() == 0 {
		val := sh.data[key]
		delete(sh.data, key)
		deleteKey = true
		memDelta = -int64(len(key)) - val.ApproximateSize()
	} else {
		dv.LastAccessedAt = now
		newSize := dv.ApproximateSize()
		memDelta = newSize - oldSize
		if db.hasPersistence() {
			b, err := dv.Serialize()
			if err == nil {
				lsmBytes = b
			} else {
				serErr = err
			}
		}
	}
	sh.lock.Unlock()

	if serErr != nil {
		db.recordLSMError(serErr)
		if db.persistenceWriteMode() == "weak" {
			logger.Error("Failed to serialize value for key %s: %v", key, serErr)
		} else {
			return 0, serErr
		}
	}

	if memDelta != 0 {
		db.updateMemoryUsage(memDelta)
	}
	if deleteKey {
		if err := db.persistenceDelete(key); err != nil {
			return 0, err
		}
	} else if lsmBytes != nil {
		if err := db.persistencePut(key, lsmBytes); err != nil {
			return 0, err
		}
	}
	if deleted > 0 {
		db.BumpKeyVersions(key)
	}
	return deleted, nil
}

func (db *Database) hLenLocked(key string) (int64, error) {
	if _, _, err := db.getLiveValue(key); err != nil {
		return 0, err
	}
	sh := db.getShard(key)
	now := time.Now().UnixMilli()

	sh.lock.RLock()
	dv, exists := sh.data[key]
	if !exists || dv == nil || (dv.ExpireTime > 0 && now > dv.ExpireTime) {
		sh.lock.RUnlock()
		return 0, nil
	}
	h, ok := dv.Value.(*datastruct.Hash)
	if !ok {
		sh.lock.RUnlock()
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	n := int64(h.Size())
	sh.lock.RUnlock()
	return n, nil
}

func (db *Database) hExistsLocked(key, field string) (bool, error) {
	_, ok, err := db.hGetLocked(key, field)
	return ok, err
}

func (db *Database) hSetNXLocked(key, field, value string) (int64, error) {
	if _, _, err := db.getLiveValue(key); err != nil {
		return 0, err
	}

	exists, err := db.hExistsLocked(key, field)
	if err != nil {
		return 0, err
	}
	if exists {
		return 0, nil
	}
	return db.hSetLocked(key, field, value)
}

func (db *Database) hStrLenLocked(key, field string) (int64, error) {
	value, ok, err := db.hGetLocked(key, field)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, nil
	}
	return int64(len(value)), nil
}

type hashEntry struct {
	field string
	value string
}

func (db *Database) sortedHashEntries(key string) ([]hashEntry, error) {
	if _, _, err := db.getLiveValue(key); err != nil {
		return nil, err
	}
	sh := db.getShard(key)
	now := time.Now().UnixMilli()

	sh.lock.RLock()
	dv, exists := sh.data[key]
	if !exists || dv == nil || (dv.ExpireTime > 0 && now > dv.ExpireTime) {
		sh.lock.RUnlock()
		return []hashEntry{}, nil
	}
	h, ok := dv.Value.(*datastruct.Hash)
	if !ok {
		sh.lock.RUnlock()
		return nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	out := make([]hashEntry, 0, len(h.Data))
	for field, value := range h.Data {
		out = append(out, hashEntry{field: field, value: value})
	}
	sh.lock.RUnlock()

	sort.Slice(out, func(i, j int) bool {
		return out[i].field < out[j].field
	})
	return out, nil
}

func flattenHashEntries(entries []hashEntry, withValues bool) []string {
	if withValues {
		out := make([]string, 0, len(entries)*2)
		for _, entry := range entries {
			out = append(out, entry.field, entry.value)
		}
		return out
	}

	out := make([]string, 0, len(entries))
	for _, entry := range entries {
		out = append(out, entry.field)
	}
	return out
}

func (db *Database) hKeysLocked(key string) ([]string, error) {
	entries, err := db.sortedHashEntries(key)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(entries))
	for _, entry := range entries {
		out = append(out, entry.field)
	}
	return out, nil
}

func (db *Database) hValsLocked(key string) ([]string, error) {
	entries, err := db.sortedHashEntries(key)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(entries))
	for _, entry := range entries {
		out = append(out, entry.value)
	}
	return out, nil
}

func (db *Database) hGetAllLocked(key string) ([]string, error) {
	entries, err := db.sortedHashEntries(key)
	if err != nil {
		return nil, err
	}
	return flattenHashEntries(entries, true), nil
}

func (db *Database) hScanLocked(key string, cursor int, pattern string, count int) (int, []string, error) {
	if count <= 0 {
		count = 10
	}

	entries, err := db.sortedHashEntries(key)
	if err != nil {
		return 0, nil, err
	}
	if cursor < 0 || cursor >= len(entries) {
		return 0, []string{}, nil
	}

	out := make([]string, 0, count*2)
	idx := cursor
	matched := 0
	for idx < len(entries) && matched < count {
		entry := entries[idx]
		ok := true
		if pattern != "*" {
			ok, err = path.Match(pattern, entry.field)
			if err != nil {
				return 0, nil, fmt.Errorf("ERR invalid pattern")
			}
		}
		if ok {
			out = append(out, entry.field, entry.value)
			matched++
		}
		idx++
	}

	if idx >= len(entries) {
		return 0, out, nil
	}
	return idx, out, nil
}

func (db *Database) hRandFieldLocked(key string) (string, bool, error) {
	entries, err := db.sortedHashEntries(key)
	if err != nil {
		return "", false, err
	}
	if len(entries) == 0 {
		return "", false, nil
	}
	return entries[rand.IntN(len(entries))].field, true, nil
}

func (db *Database) hRandFieldsLocked(key string, count int, withValues bool) ([]string, error) {
	entries, err := db.sortedHashEntries(key)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 || count == 0 {
		return []string{}, nil
	}

	if count > 0 {
		limit := count
		if limit > len(entries) {
			limit = len(entries)
		}
		selected := make([]hashEntry, 0, limit)
		for _, idx := range rand.Perm(len(entries))[:limit] {
			selected = append(selected, entries[idx])
		}
		return flattenHashEntries(selected, withValues), nil
	}

	limit := -count
	selected := make([]hashEntry, 0, limit)
	for i := 0; i < limit; i++ {
		selected = append(selected, entries[rand.IntN(len(entries))])
	}
	return flattenHashEntries(selected, withValues), nil
}

func (db *Database) hIncrByLocked(key, field string, delta int64) (int64, error) {
	if _, _, err := db.getLiveValue(key); err != nil {
		return 0, err
	}
	if !db.evictIfNeeded() {
		return 0, fmt.Errorf("OOM command not allowed when used memory (%d) > 'maxmemory' (%d)", db.usedMemory, db.maxMemory)
	}

	sh := db.getShard(key)
	now := time.Now().UnixMilli()

	var memDelta int64
	var stableKey string
	var lsmBytes []byte
	var out int64
	var serErr error

	sh.lock.Lock()
	if sh.data == nil {
		sh.data = make(map[string]*datastruct.DataValue)
	}
	dv, exists := sh.data[key]
	if !exists {
		stableKey = strings.Clone(key)
		dv = &datastruct.DataValue{ExpireTime: 0, LastAccessedAt: now}
		sh.data[stableKey] = dv
		memDelta += int64(len(stableKey))
	} else {
		stableKey = key
	}

	oldSize := dv.ApproximateSize()
	var h *datastruct.Hash
	if dv.Value == nil || (dv.ExpireTime > 0 && now > dv.ExpireTime) {
		h = &datastruct.Hash{Data: make(map[string]string)}
		dv.Value = h
		dv.ExpireTime = 0
	} else {
		hv, ok := dv.Value.(*datastruct.Hash)
		if !ok {
			sh.lock.Unlock()
			return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		h = hv
		if h.Data == nil {
			h.Data = make(map[string]string)
		}
	}

	cur := int64(0)
	if s, ok := h.Data[field]; ok && s != "" {
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			sh.lock.Unlock()
			return 0, fmt.Errorf("ERR hash value is not an integer")
		}
		cur = v
	}
	out = cur + delta
	h.Data[strings.Clone(field)] = strconv.FormatInt(out, 10)

	dv.LastAccessedAt = now
	newSize := dv.ApproximateSize()
	memDelta += newSize - oldSize

	if db.hasPersistence() {
		b, err := dv.Serialize()
		if err == nil {
			lsmBytes = b
		} else {
			serErr = err
		}
	}
	sh.lock.Unlock()

	if serErr != nil {
		db.recordLSMError(serErr)
		if db.persistenceWriteMode() == "weak" {
			logger.Error("Failed to serialize value for key %s: %v", stableKey, serErr)
		} else {
			return 0, serErr
		}
	}

	if memDelta != 0 {
		db.updateMemoryUsage(memDelta)
	}
	if lsmBytes != nil {
		if err := db.persistencePut(stableKey, lsmBytes); err != nil {
			return 0, err
		}
	}
	db.BumpKeyVersions(stableKey)
	return out, nil
}

func (db *Database) hIncrByFloatLocked(key, field string, delta float64) (float64, error) {
	if _, _, err := db.getLiveValue(key); err != nil {
		return 0, err
	}
	if !db.evictIfNeeded() {
		return 0, fmt.Errorf("OOM command not allowed when used memory (%d) > 'maxmemory' (%d)", db.usedMemory, db.maxMemory)
	}

	sh := db.getShard(key)
	now := time.Now().UnixMilli()

	var memDelta int64
	var stableKey string
	var lsmBytes []byte
	var out float64
	var serErr error

	sh.lock.Lock()
	if sh.data == nil {
		sh.data = make(map[string]*datastruct.DataValue)
	}
	dv, exists := sh.data[key]
	if !exists {
		stableKey = strings.Clone(key)
		dv = &datastruct.DataValue{ExpireTime: 0, LastAccessedAt: now}
		sh.data[stableKey] = dv
		memDelta += int64(len(stableKey))
	} else {
		stableKey = key
	}

	oldSize := dv.ApproximateSize()
	var h *datastruct.Hash
	if dv.Value == nil || (dv.ExpireTime > 0 && now > dv.ExpireTime) {
		h = &datastruct.Hash{Data: make(map[string]string)}
		dv.Value = h
		dv.ExpireTime = 0
	} else {
		hv, ok := dv.Value.(*datastruct.Hash)
		if !ok {
			sh.lock.Unlock()
			return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		h = hv
		if h.Data == nil {
			h.Data = make(map[string]string)
		}
	}

	cur := float64(0)
	if s, ok := h.Data[field]; ok && s != "" {
		v, err := strconv.ParseFloat(s, 64)
		if err != nil {
			sh.lock.Unlock()
			return 0, fmt.Errorf("ERR hash value is not a float")
		}
		cur = v
	}
	out = cur + delta
	h.Data[strings.Clone(field)] = strconv.FormatFloat(out, 'f', -1, 64)

	dv.LastAccessedAt = now
	newSize := dv.ApproximateSize()
	memDelta += newSize - oldSize

	if db.hasPersistence() {
		b, err := dv.Serialize()
		if err == nil {
			lsmBytes = b
		} else {
			serErr = err
		}
	}
	sh.lock.Unlock()

	if serErr != nil {
		db.recordLSMError(serErr)
		if db.persistenceWriteMode() == "weak" {
			logger.Error("Failed to serialize value for key %s: %v", stableKey, serErr)
		} else {
			return 0, serErr
		}
	}

	if memDelta != 0 {
		db.updateMemoryUsage(memDelta)
	}
	if lsmBytes != nil {
		if err := db.persistencePut(stableKey, lsmBytes); err != nil {
			return 0, err
		}
	}
	db.BumpKeyVersions(stableKey)
	return out, nil
}

func (db *Database) HSet(key, field, value string) (result0 int64, err error) {
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

	return db.hSetLocked(key, field, value)
}

func (db *Database) HMSet(key string, fields []string, values []string) (err error) {
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

	return db.hMSetLocked(key, fields, values)
}

func (db *Database) HGet(key, field string) (result0 string, result1 bool, err error) {
	db.operationMu.Lock()
	defer db.endReadOperation()
	if db.closed {
		err = errDatabaseClosed
		return
	}
	return db.hGetLocked(key, field)
}

func (db *Database) HMGet(key string, fields []string) (result0 []string, err error) {
	db.operationMu.Lock()
	defer db.endReadOperation()
	if db.closed {
		err = errDatabaseClosed
		return
	}
	return db.hMGetLocked(key, fields)
}

func (db *Database) HMGetWithExists(key string, fields []string) (result0 []string, result1 []bool, err error) {
	db.operationMu.Lock()
	defer db.endReadOperation()
	if db.closed {
		err = errDatabaseClosed
		return
	}
	return db.hMGetWithExistsLocked(key, fields)
}

func (db *Database) HDel(key string, fields []string) (result0 int64, err error) {
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

	return db.hDelLocked(key, fields)
}

func (db *Database) HLen(key string) (result0 int64, err error) {
	db.operationMu.Lock()
	defer db.endReadOperation()
	if db.closed {
		err = errDatabaseClosed
		return
	}
	return db.hLenLocked(key)
}

func (db *Database) HExists(key, field string) (result0 bool, err error) {
	db.operationMu.Lock()
	defer db.endReadOperation()
	if db.closed {
		err = errDatabaseClosed
		return
	}
	return db.hExistsLocked(key, field)
}

func (db *Database) HSetNX(key, field, value string) (result0 int64, err error) {
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

	return db.hSetNXLocked(key, field, value)
}

func (db *Database) HStrLen(key, field string) (result0 int64, err error) {
	db.operationMu.Lock()
	defer db.endReadOperation()
	if db.closed {
		err = errDatabaseClosed
		return
	}
	return db.hStrLenLocked(key, field)
}

func (db *Database) HKeys(key string) (result0 []string, err error) {
	db.operationMu.Lock()
	defer db.endReadOperation()
	if db.closed {
		err = errDatabaseClosed
		return
	}
	return db.hKeysLocked(key)
}

func (db *Database) HVals(key string) (result0 []string, err error) {
	db.operationMu.Lock()
	defer db.endReadOperation()
	if db.closed {
		err = errDatabaseClosed
		return
	}
	return db.hValsLocked(key)
}

func (db *Database) HGetAll(key string) (result0 []string, err error) {
	db.operationMu.Lock()
	defer db.endReadOperation()
	if db.closed {
		err = errDatabaseClosed
		return
	}
	return db.hGetAllLocked(key)
}

func (db *Database) HScan(key string, cursor int, pattern string, count int) (result0 int, result1 []string, err error) {
	db.operationMu.Lock()
	defer db.endReadOperation()
	if db.closed {
		err = errDatabaseClosed
		return
	}
	return db.hScanLocked(key, cursor, pattern, count)
}

func (db *Database) HRandField(key string) (result0 string, result1 bool, err error) {
	db.operationMu.Lock()
	defer db.endReadOperation()
	if db.closed {
		err = errDatabaseClosed
		return
	}
	return db.hRandFieldLocked(key)
}

func (db *Database) HRandFields(key string, count int, withValues bool) (result0 []string, err error) {
	db.operationMu.Lock()
	defer db.endReadOperation()
	if db.closed {
		err = errDatabaseClosed
		return
	}
	return db.hRandFieldsLocked(key, count, withValues)
}

func (db *Database) HIncrBy(key, field string, delta int64) (result0 int64, err error) {
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

	return db.hIncrByLocked(key, field, delta)
}

func (db *Database) HIncrByFloat(key, field string, delta float64) (result0 float64, err error) {
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

	return db.hIncrByFloatLocked(key, field, delta)
}
