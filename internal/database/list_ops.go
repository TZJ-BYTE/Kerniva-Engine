package database

import (
	"fmt"
	"strings"
	"time"

	"github.com/TZJ-BYTE/Kerniva/internal/datastruct"
	"github.com/TZJ-BYTE/Kerniva/pkg/logger"
)

func listPop(list *datastruct.List, fromLeft bool) (string, bool) {
	if fromLeft {
		return list.PopLeft()
	}
	return list.PopRight()
}

func listPush(list *datastruct.List, value string, toLeft bool) {
	if toLeft {
		list.PushLeft(value)
		return
	}
	list.PushRight(value)
}

func listPopCount(list *datastruct.List, fromLeft bool, count int) []string {
	if count <= 0 || len(list.Data) == 0 {
		return []string{}
	}
	if count > len(list.Data) {
		count = len(list.Data)
	}

	out := make([]string, count)
	if fromLeft {
		copy(out, list.Data[:count])
		list.Data = list.Data[count:]
		return out
	}

	start := len(list.Data) - count
	for i := 0; i < count; i++ {
		out[i] = list.Data[len(list.Data)-1-i]
	}
	list.Data = list.Data[:start]
	return out
}

func listPositions(data []string, element string, rank, count, maxLen int) []int {
	if len(data) == 0 {
		return nil
	}

	if rank > 0 {
		limit := len(data)
		if maxLen > 0 && maxLen < limit {
			limit = maxLen
		}
		matchesSeen := 0
		out := make([]int, 0, 1)
		for i := 0; i < limit; i++ {
			if data[i] != element {
				continue
			}
			matchesSeen++
			if matchesSeen < rank {
				continue
			}
			out = append(out, i)
			if count < 0 || (count > 0 && len(out) >= count) {
				break
			}
		}
		return out
	}

	start := len(data) - 1
	minIndex := 0
	if maxLen > 0 && maxLen < len(data) {
		minIndex = len(data) - maxLen
	}
	matchesSeen := 0
	out := make([]int, 0, 1)
	for i := start; i >= minIndex; i-- {
		if data[i] != element {
			continue
		}
		matchesSeen++
		if matchesSeen < -rank {
			continue
		}
		out = append(out, i)
		if count < 0 || (count > 0 && len(out) >= count) {
			break
		}
	}
	return out
}

func (db *Database) pushList(key string, values []string, left bool, mustExist bool) (int, error) {
	if len(values) == 0 {
		return 0, nil
	}
	if _, exists, err := db.getLiveValue(key); err != nil {
		return 0, err
	} else if mustExist && !exists {
		return 0, nil
	}
	if !db.evictIfNeeded() {
		return 0, fmt.Errorf("OOM command not allowed when used memory (%d) > 'maxmemory' (%d)", db.usedMemory, db.maxMemory)
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
		if mustExist {
			sh.lock.Unlock()
			return 0, nil
		}
		stableKey = strings.Clone(key)
		dv = &datastruct.DataValue{ExpireTime: 0, LastAccessedAt: now}
		sh.data[stableKey] = dv
		memDelta += int64(len(stableKey))
	} else {
		stableKey = key
	}

	oldSize := dv.ApproximateSize()
	var list *datastruct.List
	if dv.Value == nil || (dv.ExpireTime > 0 && now > dv.ExpireTime) {
		list = &datastruct.List{Data: make([]string, 0)}
		dv.Value = list
		dv.ExpireTime = 0
	} else {
		l, ok := dv.Value.(*datastruct.List)
		if !ok {
			sh.lock.Unlock()
			return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		list = l
	}

	if left {
		n := len(values)
		newData := make([]string, n+len(list.Data))
		for i := 0; i < n; i++ {
			newData[n-1-i] = strings.Clone(values[i])
		}
		copy(newData[n:], list.Data)
		list.Data = newData
	} else {
		for i := range values {
			list.Data = append(list.Data, strings.Clone(values[i]))
		}
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
	return len(list.Data), nil
}

func (db *Database) lPushLocked(key string, values []string) (int, error) {
	return db.pushList(key, values, true, false)
}

func (db *Database) rPushLocked(key string, values []string) (int, error) {
	return db.pushList(key, values, false, false)
}

func (db *Database) lPushXLocked(key string, values []string) (int, error) {
	return db.pushList(key, values, true, true)
}

func (db *Database) rPushXLocked(key string, values []string) (int, error) {
	return db.pushList(key, values, false, true)
}

func (db *Database) popList(key string, fromLeft bool, count int) ([]string, bool, error) {
	if _, _, err := db.getLiveValue(key); err != nil {
		return nil, false, err
	}
	sh := db.getShard(key)
	now := time.Now().UnixMilli()

	var memDelta int64
	var lsmBytes []byte
	var deleteKey bool
	var serErr error

	sh.lock.Lock()
	dv, exists := sh.data[key]
	if !exists || dv == nil || (dv.ExpireTime > 0 && now > dv.ExpireTime) {
		sh.lock.Unlock()
		return nil, false, nil
	}
	list, ok := dv.Value.(*datastruct.List)
	if !ok {
		sh.lock.Unlock()
		return nil, false, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	if count == 0 {
		sh.lock.Unlock()
		return []string{}, true, nil
	}

	oldSize := dv.ApproximateSize()
	values := listPopCount(list, fromLeft, count)
	if len(values) == 0 {
		sh.lock.Unlock()
		return []string{}, true, nil
	}
	if len(list.Data) == 0 {
		delete(sh.data, key)
		deleteKey = true
		memDelta = -int64(len(key)) - oldSize
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
			return nil, false, serErr
		}
	}

	if memDelta != 0 {
		db.updateMemoryUsage(memDelta)
	}
	if deleteKey {
		if err := db.persistenceDelete(key); err != nil {
			return nil, false, err
		}
	} else if lsmBytes != nil {
		if err := db.persistencePut(key, lsmBytes); err != nil {
			return nil, false, err
		}
	}
	db.BumpKeyVersions(key)
	return values, true, nil
}

func (db *Database) lPopLocked(key string) (string, bool, error) {
	values, ok, err := db.popList(key, true, 1)
	if err != nil || !ok || len(values) == 0 {
		return "", ok, err
	}
	return values[0], true, nil
}

func (db *Database) lPopCountLocked(key string, count int) ([]string, bool, error) {
	return db.popList(key, true, count)
}

func (db *Database) rPopLocked(key string) (string, bool, error) {
	values, ok, err := db.popList(key, false, 1)
	if err != nil || !ok || len(values) == 0 {
		return "", ok, err
	}
	return values[0], true, nil
}

func (db *Database) rPopCountLocked(key string, count int) ([]string, bool, error) {
	return db.popList(key, false, count)
}

func (db *Database) lLenLocked(key string) (int, error) {
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
	list, ok := dv.Value.(*datastruct.List)
	if !ok {
		sh.lock.RUnlock()
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	n := len(list.Data)
	sh.lock.RUnlock()
	return n, nil
}

func (db *Database) lRangeLocked(key string, start, stop int) ([]string, error) {
	if _, _, err := db.getLiveValue(key); err != nil {
		return nil, err
	}
	sh := db.getShard(key)
	now := time.Now().UnixMilli()

	sh.lock.RLock()
	dv, exists := sh.data[key]
	if !exists || dv == nil || (dv.ExpireTime > 0 && now > dv.ExpireTime) {
		sh.lock.RUnlock()
		return []string{}, nil
	}
	list, ok := dv.Value.(*datastruct.List)
	if !ok {
		sh.lock.RUnlock()
		return nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	s := list.Range(start, stop)
	out := make([]string, len(s))
	copy(out, s)
	sh.lock.RUnlock()
	return out, nil
}

func (db *Database) lIndexLocked(key string, index int) (string, bool, error) {
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
	list, ok := dv.Value.(*datastruct.List)
	if !ok {
		sh.lock.RUnlock()
		return "", false, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	length := len(list.Data)
	if index < 0 {
		index = length + index
	}
	if index < 0 || index >= length {
		sh.lock.RUnlock()
		return "", false, nil
	}
	out := list.Data[index]
	sh.lock.RUnlock()
	return out, true, nil
}

func (db *Database) lSetLocked(key string, index int, value string) error {
	if _, _, err := db.getLiveValue(key); err != nil {
		return err
	}

	sh := db.getShard(key)
	now := time.Now().UnixMilli()

	var memDelta int64
	var lsmBytes []byte
	var serErr error

	sh.lock.Lock()
	dv, exists := sh.data[key]
	if !exists || dv == nil || (dv.ExpireTime > 0 && now > dv.ExpireTime) {
		sh.lock.Unlock()
		return fmt.Errorf("ERR no such key")
	}
	list, ok := dv.Value.(*datastruct.List)
	if !ok {
		sh.lock.Unlock()
		return fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	length := len(list.Data)
	if index < 0 {
		index = length + index
	}
	if index < 0 || index >= length {
		sh.lock.Unlock()
		return fmt.Errorf("ERR index out of range")
	}

	oldSize := dv.ApproximateSize()
	list.Data[index] = strings.Clone(value)
	dv.LastAccessedAt = now
	memDelta = dv.ApproximateSize() - oldSize

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
			logger.Error("Failed to serialize value for key %s: %v", key, serErr)
		} else {
			return serErr
		}
	}
	if memDelta != 0 {
		db.updateMemoryUsage(memDelta)
	}
	if lsmBytes != nil {
		if err := db.persistencePut(key, lsmBytes); err != nil {
			return err
		}
	}
	db.BumpKeyVersions(key)
	return nil
}

func (db *Database) lTrimLocked(key string, start, stop int) error {
	if _, _, err := db.getLiveValue(key); err != nil {
		return err
	}

	sh := db.getShard(key)
	now := time.Now().UnixMilli()

	var memDelta int64
	var lsmBytes []byte
	var deleteKey bool
	var serErr error

	sh.lock.Lock()
	dv, exists := sh.data[key]
	if !exists || dv == nil || (dv.ExpireTime > 0 && now > dv.ExpireTime) {
		sh.lock.Unlock()
		return nil
	}
	list, ok := dv.Value.(*datastruct.List)
	if !ok {
		sh.lock.Unlock()
		return fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	oldSize := dv.ApproximateSize()
	lo, hi, hasRange := normalizeRange(len(list.Data), start, stop)
	if !hasRange {
		delete(sh.data, key)
		deleteKey = true
		memDelta = -int64(len(key)) - oldSize
	} else {
		trimmed := make([]string, hi-lo+1)
		copy(trimmed, list.Data[lo:hi+1])
		list.Data = trimmed
		dv.LastAccessedAt = now
		memDelta = dv.ApproximateSize() - oldSize
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
			return serErr
		}
	}
	if memDelta != 0 {
		db.updateMemoryUsage(memDelta)
	}
	if deleteKey {
		if err := db.persistenceDelete(key); err != nil {
			return err
		}
		db.BumpKeyVersions(key)
		return nil
	}
	if lsmBytes != nil {
		if err := db.persistencePut(key, lsmBytes); err != nil {
			return err
		}
	}
	db.BumpKeyVersions(key)
	return nil
}

func (db *Database) lPosLocked(key, element string, rank, count, maxLen int) ([]int, error) {
	if _, _, err := db.getLiveValue(key); err != nil {
		return nil, err
	}

	sh := db.getShard(key)
	now := time.Now().UnixMilli()

	sh.lock.RLock()
	dv, exists := sh.data[key]
	if !exists || dv == nil || (dv.ExpireTime > 0 && now > dv.ExpireTime) {
		sh.lock.RUnlock()
		return []int{}, nil
	}
	list, ok := dv.Value.(*datastruct.List)
	if !ok {
		sh.lock.RUnlock()
		return nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	out := listPositions(list.Data, element, rank, count, maxLen)
	sh.lock.RUnlock()
	return out, nil
}

func (db *Database) lRemLocked(key string, count int, value string) (int, error) {
	if _, _, err := db.getLiveValue(key); err != nil {
		return 0, err
	}

	sh := db.getShard(key)
	now := time.Now().UnixMilli()

	var removed int
	var memDelta int64
	var lsmBytes []byte
	var deleteKey bool
	var serErr error

	sh.lock.Lock()
	dv, exists := sh.data[key]
	if !exists || dv == nil || (dv.ExpireTime > 0 && now > dv.ExpireTime) {
		sh.lock.Unlock()
		return 0, nil
	}
	list, ok := dv.Value.(*datastruct.List)
	if !ok {
		sh.lock.Unlock()
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	oldSize := dv.ApproximateSize()
	var filtered []string
	switch {
	case count == 0:
		filtered = make([]string, 0, len(list.Data))
		for _, item := range list.Data {
			if item == value {
				removed++
				continue
			}
			filtered = append(filtered, item)
		}
	case count > 0:
		filtered = make([]string, 0, len(list.Data))
		for _, item := range list.Data {
			if item == value && removed < count {
				removed++
				continue
			}
			filtered = append(filtered, item)
		}
	default:
		limit := -count
		removeAt := make([]bool, len(list.Data))
		for i := len(list.Data) - 1; i >= 0 && removed < limit; i-- {
			if list.Data[i] == value {
				removeAt[i] = true
				removed++
			}
		}
		filtered = make([]string, 0, len(list.Data)-removed)
		for i, item := range list.Data {
			if removeAt[i] {
				continue
			}
			filtered = append(filtered, item)
		}
	}

	if removed == 0 {
		sh.lock.Unlock()
		return 0, nil
	}
	if len(filtered) == 0 {
		delete(sh.data, key)
		deleteKey = true
		memDelta = -int64(len(key)) - oldSize
	} else {
		list.Data = filtered
		dv.LastAccessedAt = now
		memDelta = dv.ApproximateSize() - oldSize
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
	db.BumpKeyVersions(key)
	return removed, nil
}

func (db *Database) lMoveLocked(source, destination string, fromLeft, toLeft bool) (string, bool, error) {
	if _, _, err := db.getLiveValue(source); err != nil {
		return "", false, err
	}
	if _, _, err := db.getLiveValue(destination); err != nil {
		return "", false, err
	}
	if !db.evictIfNeeded() {
		return "", false, fmt.Errorf("OOM command not allowed when used memory (%d) > 'maxmemory' (%d)", db.usedMemory, db.maxMemory)
	}

	if source == destination {
		sh := db.getShard(source)
		now := time.Now().UnixMilli()

		var moved string
		var memDelta int64
		var lsmBytes []byte
		var serErr error

		sh.lock.Lock()
		dv, exists := sh.data[source]
		if !exists || dv == nil || (dv.ExpireTime > 0 && now > dv.ExpireTime) {
			sh.lock.Unlock()
			return "", false, nil
		}
		list, ok := dv.Value.(*datastruct.List)
		if !ok {
			sh.lock.Unlock()
			return "", false, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		if len(list.Data) == 0 {
			sh.lock.Unlock()
			return "", false, nil
		}

		oldSize := dv.ApproximateSize()
		moved, _ = listPop(list, fromLeft)
		listPush(list, strings.Clone(moved), toLeft)
		dv.LastAccessedAt = now
		memDelta = dv.ApproximateSize() - oldSize
		if db.hasPersistence() {
			lsmBytes, serErr = dv.Serialize()
		}
		sh.lock.Unlock()

		if serErr != nil {
			db.recordLSMError(serErr)
			if db.persistenceWriteMode() == "weak" {
				logger.Error("Failed to serialize list %s: %v", source, serErr)
			} else {
				return "", false, serErr
			}
		}
		if memDelta != 0 {
			db.updateMemoryUsage(memDelta)
		}
		if lsmBytes != nil {
			if err := db.persistencePut(source, lsmBytes); err != nil {
				return "", false, err
			}
		}
		db.BumpKeyVersions(source)
		return moved, true, nil
	}

	srcIdx := getShardIndex(source)
	dstIdx := getShardIndex(destination)
	first, second := srcIdx, dstIdx
	if first > second {
		first, second = second, first
	}
	s1 := &db.shards[first]
	s2 := &db.shards[second]
	if first == second {
		s2 = s1
	}

	now := time.Now().UnixMilli()
	var memDelta int64
	var moved string
	var srcDelete bool
	var dstStableKey string
	var srcBytes []byte
	var dstBytes []byte
	var srcSerErr error
	var dstSerErr error

	s1.lock.Lock()
	if s2 != s1 {
		s2.lock.Lock()
	}

	srcShard := &db.shards[srcIdx]
	dstShard := &db.shards[dstIdx]

	srcDV, exists := srcShard.data[source]
	if !exists || srcDV == nil || (srcDV.ExpireTime > 0 && now > srcDV.ExpireTime) {
		if s2 != s1 {
			s2.lock.Unlock()
		}
		s1.lock.Unlock()
		return "", false, nil
	}
	srcList, ok := srcDV.Value.(*datastruct.List)
	if !ok {
		if s2 != s1 {
			s2.lock.Unlock()
		}
		s1.lock.Unlock()
		return "", false, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	if len(srcList.Data) == 0 {
		if s2 != s1 {
			s2.lock.Unlock()
		}
		s1.lock.Unlock()
		return "", false, nil
	}

	if dstShard.data == nil {
		dstShard.data = make(map[string]*datastruct.DataValue)
	}
	dstDV, dstExists := dstShard.data[destination]
	if !dstExists {
		dstStableKey = strings.Clone(destination)
		dstDV = &datastruct.DataValue{ExpireTime: 0, LastAccessedAt: now}
		dstShard.data[dstStableKey] = dstDV
		memDelta += int64(len(dstStableKey))
	} else {
		dstStableKey = destination
	}
	var dstList *datastruct.List
	if dstDV.Value == nil || (dstDV.ExpireTime > 0 && now > dstDV.ExpireTime) {
		dstList = &datastruct.List{Data: make([]string, 0, 1)}
		dstDV.Value = dstList
		dstDV.ExpireTime = 0
	} else {
		lv, ok := dstDV.Value.(*datastruct.List)
		if !ok {
			if s2 != s1 {
				s2.lock.Unlock()
			}
			s1.lock.Unlock()
			return "", false, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		dstList = lv
	}

	oldSrcSize := srcDV.ApproximateSize()
	moved, _ = listPop(srcList, fromLeft)
	if len(srcList.Data) == 0 {
		delete(srcShard.data, source)
		srcDelete = true
		memDelta -= int64(len(source)) + oldSrcSize
	} else {
		srcDV.LastAccessedAt = now
		memDelta += srcDV.ApproximateSize() - oldSrcSize
	}

	oldDstSize := dstDV.ApproximateSize()
	listPush(dstList, strings.Clone(moved), toLeft)
	dstDV.LastAccessedAt = now
	memDelta += dstDV.ApproximateSize() - oldDstSize

	if db.hasPersistence() {
		if !srcDelete {
			srcBytes, srcSerErr = srcDV.Serialize()
		}
		dstBytes, dstSerErr = dstDV.Serialize()
	}

	if s2 != s1 {
		s2.lock.Unlock()
	}
	s1.lock.Unlock()

	if srcSerErr != nil {
		db.recordLSMError(srcSerErr)
		if db.persistenceWriteMode() != "weak" {
			return "", false, srcSerErr
		}
		logger.Error("Failed to serialize list %s: %v", source, srcSerErr)
	}
	if dstSerErr != nil {
		db.recordLSMError(dstSerErr)
		if db.persistenceWriteMode() != "weak" {
			return "", false, dstSerErr
		}
		logger.Error("Failed to serialize list %s: %v", destination, dstSerErr)
	}
	if memDelta != 0 {
		db.updateMemoryUsage(memDelta)
	}
	if srcDelete {
		if err := db.persistenceDelete(source); err != nil {
			return "", false, err
		}
	} else if srcBytes != nil {
		if err := db.persistencePut(source, srcBytes); err != nil {
			return "", false, err
		}
	}
	if dstBytes != nil {
		if err := db.persistencePut(dstStableKey, dstBytes); err != nil {
			return "", false, err
		}
	}
	db.BumpKeyVersions(source, dstStableKey)
	return moved, true, nil
}

func (db *Database) rPopLPushLocked(source, destination string) (string, bool, error) {
	return db.lMoveLocked(source, destination, false, true)
}

func (db *Database) lMPopLocked(keys []string, fromLeft bool, count int) (string, []string, bool, error) {
	for _, key := range keys {
		values, ok, err := db.popList(key, fromLeft, count)
		if err != nil {
			return "", nil, false, err
		}
		if !ok {
			continue
		}
		if len(values) == 0 {
			continue
		}
		return key, values, true, nil
	}
	return "", nil, false, nil
}

func (db *Database) LPush(key string, values []string) (result0 int, err error) {
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

	return db.lPushLocked(key, values)
}

func (db *Database) RPush(key string, values []string) (result0 int, err error) {
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

	return db.rPushLocked(key, values)
}

func (db *Database) LPushX(key string, values []string) (result0 int, err error) {
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

	return db.lPushXLocked(key, values)
}

func (db *Database) RPushX(key string, values []string) (result0 int, err error) {
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

	return db.rPushXLocked(key, values)
}

func (db *Database) LPop(key string) (result0 string, result1 bool, err error) {
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

	return db.lPopLocked(key)
}

func (db *Database) LPopCount(key string, count int) (result0 []string, result1 bool, err error) {
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

	return db.lPopCountLocked(key, count)
}

func (db *Database) RPop(key string) (result0 string, result1 bool, err error) {
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

	return db.rPopLocked(key)
}

func (db *Database) RPopCount(key string, count int) (result0 []string, result1 bool, err error) {
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

	return db.rPopCountLocked(key, count)
}

func (db *Database) LLen(key string) (result0 int, err error) {
	db.operationMu.Lock()
	defer db.endReadOperation()
	if db.closed {
		err = errDatabaseClosed
		return
	}
	return db.lLenLocked(key)
}

func (db *Database) LRange(key string, start, stop int) (result0 []string, err error) {
	db.operationMu.Lock()
	defer db.endReadOperation()
	if db.closed {
		err = errDatabaseClosed
		return
	}
	return db.lRangeLocked(key, start, stop)
}

func (db *Database) LIndex(key string, index int) (result0 string, result1 bool, err error) {
	db.operationMu.Lock()
	defer db.endReadOperation()
	if db.closed {
		err = errDatabaseClosed
		return
	}
	return db.lIndexLocked(key, index)
}

func (db *Database) LSet(key string, index int, value string) (err error) {
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

	return db.lSetLocked(key, index, value)
}

func (db *Database) LTrim(key string, start, stop int) (err error) {
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

	return db.lTrimLocked(key, start, stop)
}

func (db *Database) LPos(key, element string, rank, count, maxLen int) (result0 []int, err error) {
	db.operationMu.Lock()
	defer db.endReadOperation()
	if db.closed {
		err = errDatabaseClosed
		return
	}
	return db.lPosLocked(key, element, rank, count, maxLen)
}

func (db *Database) LRem(key string, count int, value string) (result0 int, err error) {
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

	return db.lRemLocked(key, count, value)
}

func (db *Database) LMove(source, destination string, fromLeft, toLeft bool) (result0 string, result1 bool, err error) {
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

	return db.lMoveLocked(source, destination, fromLeft, toLeft)
}

func (db *Database) RPopLPush(source, destination string) (result0 string, result1 bool, err error) {
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

	return db.rPopLPushLocked(source, destination)
}

func (db *Database) LMPop(keys []string, fromLeft bool, count int) (result0 string, result1 []string, result2 bool, err error) {
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

	return db.lMPopLocked(keys, fromLeft, count)
}
