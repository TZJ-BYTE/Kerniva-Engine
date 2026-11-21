package database

import (
	"fmt"
	"math/rand"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/TZJ-BYTE/Kerniva/internal/datastruct"
	"github.com/TZJ-BYTE/Kerniva/pkg/logger"
)

func (db *Database) sAddLocked(key string, members []string) (int64, error) {
	if len(members) == 0 {
		return 0, nil
	}
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
	var added int64
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
	var s *datastruct.Set
	if dv.Value == nil || (dv.ExpireTime > 0 && now > dv.ExpireTime) {
		s = &datastruct.Set{Data: make(map[string]struct{})}
		dv.Value = s
		dv.ExpireTime = 0
	} else {
		sv, ok := dv.Value.(*datastruct.Set)
		if !ok {
			sh.lock.Unlock()
			return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		s = sv
		if s.Data == nil {
			s.Data = make(map[string]struct{})
		}
	}

	for _, member := range members {
		if s.Add(strings.Clone(member)) {
			added++
		}
	}
	dv.LastAccessedAt = now
	memDelta += dv.ApproximateSize() - oldSize

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
	return added, nil
}

func (db *Database) sRemLocked(key string, members []string) (int64, error) {
	if len(members) == 0 {
		return 0, nil
	}
	if _, _, err := db.getLiveValue(key); err != nil {
		return 0, err
	}

	sh := db.getShard(key)
	now := time.Now().UnixMilli()

	var memDelta int64
	var removed int64
	var lsmBytes []byte
	var deleteKey bool
	var serErr error

	sh.lock.Lock()
	dv, exists := sh.data[key]
	if !exists || dv == nil || (dv.ExpireTime > 0 && now > dv.ExpireTime) {
		sh.lock.Unlock()
		return 0, nil
	}

	s, ok := dv.Value.(*datastruct.Set)
	if !ok {
		sh.lock.Unlock()
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	oldSize := dv.ApproximateSize()
	for _, member := range members {
		if s.Remove(member) {
			removed++
		}
	}

	if len(s.Data) == 0 {
		delete(sh.data, key)
		deleteKey = true
		memDelta = -int64(len(key)) - oldSize
	} else {
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
	if removed > 0 {
		db.BumpKeyVersions(key)
	}
	return removed, nil
}

func (db *Database) sIsMemberLocked(key, member string) (bool, error) {
	if _, _, err := db.getLiveValue(key); err != nil {
		return false, err
	}
	sh := db.getShard(key)
	now := time.Now().UnixMilli()

	sh.lock.RLock()
	dv, exists := sh.data[key]
	if !exists || dv == nil || (dv.ExpireTime > 0 && now > dv.ExpireTime) {
		sh.lock.RUnlock()
		return false, nil
	}
	s, ok := dv.Value.(*datastruct.Set)
	if !ok {
		sh.lock.RUnlock()
		return false, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	out := s.Contains(member)
	sh.lock.RUnlock()
	return out, nil
}

func (db *Database) sMembersLocked(key string) ([]string, error) {
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
	s, ok := dv.Value.(*datastruct.Set)
	if !ok {
		sh.lock.RUnlock()
		return nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	members := s.Members()
	sort.Strings(members)
	sh.lock.RUnlock()
	return members, nil
}

func (db *Database) sCardLocked(key string) (int64, error) {
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
	s, ok := dv.Value.(*datastruct.Set)
	if !ok {
		sh.lock.RUnlock()
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	out := int64(len(s.Data))
	sh.lock.RUnlock()
	return out, nil
}

func (db *Database) sInterLocked(keys []string) ([]string, error) {
	result, err := db.setOp(keys, "inter")
	if err != nil {
		return nil, err
	}
	sort.Strings(result)
	return result, nil
}

func (db *Database) sUnionLocked(keys []string) ([]string, error) {
	result, err := db.setOp(keys, "union")
	if err != nil {
		return nil, err
	}
	sort.Strings(result)
	return result, nil
}

func (db *Database) sDiffLocked(keys []string) ([]string, error) {
	result, err := db.setOp(keys, "diff")
	if err != nil {
		return nil, err
	}
	sort.Strings(result)
	return result, nil
}

func (db *Database) sInterCardLocked(keys []string, limit int64) (int64, error) {
	result, err := db.setOp(keys, "inter")
	if err != nil {
		return 0, err
	}
	card := int64(len(result))
	if limit > 0 && card > limit {
		return limit, nil
	}
	return card, nil
}

func (db *Database) sMIsMemberLocked(key string, members []string) ([]int64, error) {
	results := make([]int64, len(members))
	for i, member := range members {
		ok, err := db.sIsMemberLocked(key, member)
		if err != nil {
			return nil, err
		}
		if ok {
			results[i] = 1
		}
	}
	return results, nil
}

func (db *Database) sPopLocked(key string) (string, bool, error) {
	if _, _, err := db.getLiveValue(key); err != nil {
		return "", false, err
	}

	sh := db.getShard(key)
	now := time.Now().UnixMilli()

	var memDelta int64
	var lsmBytes []byte
	var deleteKey bool
	var serErr error
	var out string

	sh.lock.Lock()
	dv, exists := sh.data[key]
	if !exists || dv == nil || (dv.ExpireTime > 0 && now > dv.ExpireTime) {
		sh.lock.Unlock()
		return "", false, nil
	}

	s, ok := dv.Value.(*datastruct.Set)
	if !ok {
		sh.lock.Unlock()
		return "", false, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	if len(s.Data) == 0 {
		sh.lock.Unlock()
		return "", false, nil
	}

	oldSize := dv.ApproximateSize()
	idx := rand.Intn(len(s.Data))
	i := 0
	for member := range s.Data {
		if i == idx {
			out = member
			delete(s.Data, member)
			break
		}
		i++
	}

	if len(s.Data) == 0 {
		delete(sh.data, key)
		deleteKey = true
		memDelta = -int64(len(key)) - oldSize
	} else {
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
			return "", false, serErr
		}
	}
	if memDelta != 0 {
		db.updateMemoryUsage(memDelta)
	}
	if deleteKey {
		if err := db.persistenceDelete(key); err != nil {
			return "", false, err
		}
	} else if lsmBytes != nil {
		if err := db.persistencePut(key, lsmBytes); err != nil {
			return "", false, err
		}
	}
	db.BumpKeyVersions(key)
	return out, true, nil
}

func (db *Database) sRandMemberLocked(key string) (string, bool, error) {
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
	s, ok := dv.Value.(*datastruct.Set)
	if !ok {
		sh.lock.RUnlock()
		return "", false, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	if len(s.Data) == 0 {
		sh.lock.RUnlock()
		return "", false, nil
	}
	idx := rand.Intn(len(s.Data))
	i := 0
	out := ""
	for member := range s.Data {
		if i == idx {
			out = member
			break
		}
		i++
	}
	sh.lock.RUnlock()
	return out, true, nil
}

func (db *Database) sScanLocked(key string, cursor int, pattern string, count int) (int, []string, error) {
	if _, _, err := db.getLiveValue(key); err != nil {
		return 0, nil, err
	}

	sh := db.getShard(key)
	now := time.Now().UnixMilli()

	sh.lock.RLock()
	dv, exists := sh.data[key]
	if !exists || dv == nil || (dv.ExpireTime > 0 && now > dv.ExpireTime) {
		sh.lock.RUnlock()
		return 0, []string{}, nil
	}
	s, ok := dv.Value.(*datastruct.Set)
	if !ok {
		sh.lock.RUnlock()
		return 0, nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	members := s.Members()
	sort.Strings(members)
	sh.lock.RUnlock()

	if cursor >= len(members) {
		return 0, []string{}, nil
	}

	result := make([]string, 0, count)
	idx := cursor
	for idx < len(members) && len(result) < count {
		member := members[idx]
		if pattern == "*" {
			result = append(result, member)
		} else {
			matched, err := path.Match(pattern, member)
			if err != nil {
				return 0, nil, err
			}
			if matched {
				result = append(result, member)
			}
		}
		idx++
	}

	nextCursor := 0
	if idx < len(members) {
		nextCursor = idx
	}
	return nextCursor, result, nil
}

func (db *Database) sMoveLocked(source, destination, member string) (bool, error) {
	if _, _, err := db.getLiveValue(source); err != nil {
		return false, err
	}
	if _, _, err := db.getLiveValue(destination); err != nil {
		return false, err
	}
	if !db.evictIfNeeded() {
		return false, fmt.Errorf("OOM command not allowed when used memory (%d) > 'maxmemory' (%d)", db.usedMemory, db.maxMemory)
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
	var srcDelete bool
	var dstStableKey string
	var srcBytes []byte
	var dstBytes []byte
	var srcSerErr error
	var dstSerErr error
	moved := false

	s1.lock.Lock()
	if s2 != s1 {
		s2.lock.Lock()
	}

	srcShard := &db.shards[srcIdx]
	dstShard := &db.shards[dstIdx]

	srcDV, srcExists := srcShard.data[source]
	if !srcExists || srcDV == nil || (srcDV.ExpireTime > 0 && now > srcDV.ExpireTime) {
		if s2 != s1 {
			s2.lock.Unlock()
		}
		s1.lock.Unlock()
		return false, nil
	}

	srcSet, ok := srcDV.Value.(*datastruct.Set)
	if !ok {
		if s2 != s1 {
			s2.lock.Unlock()
		}
		s1.lock.Unlock()
		return false, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	if !srcSet.Contains(member) {
		if s2 != s1 {
			s2.lock.Unlock()
		}
		s1.lock.Unlock()
		return false, nil
	}

	oldSrcSize := srcDV.ApproximateSize()
	srcSet.Remove(member)
	if len(srcSet.Data) == 0 {
		delete(srcShard.data, source)
		srcDelete = true
		memDelta -= int64(len(source)) + oldSrcSize
	} else {
		srcDV.LastAccessedAt = now
		memDelta += srcDV.ApproximateSize() - oldSrcSize
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

	oldDstSize := dstDV.ApproximateSize()
	var dstSet *datastruct.Set
	if dstDV.Value == nil || (dstDV.ExpireTime > 0 && now > dstDV.ExpireTime) {
		dstSet = &datastruct.Set{Data: make(map[string]struct{})}
		dstDV.Value = dstSet
		dstDV.ExpireTime = 0
	} else {
		sv, ok := dstDV.Value.(*datastruct.Set)
		if !ok {
			if s2 != s1 {
				s2.lock.Unlock()
			}
			s1.lock.Unlock()
			return false, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		dstSet = sv
		if dstSet.Data == nil {
			dstSet.Data = make(map[string]struct{})
		}
	}
	dstSet.Add(strings.Clone(member))
	dstDV.LastAccessedAt = now
	memDelta += dstDV.ApproximateSize() - oldDstSize
	moved = true

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
			return false, srcSerErr
		}
		logger.Error("Failed to serialize source set %s: %v", source, srcSerErr)
	}
	if dstSerErr != nil {
		db.recordLSMError(dstSerErr)
		if db.persistenceWriteMode() != "weak" {
			return false, dstSerErr
		}
		logger.Error("Failed to serialize destination set %s: %v", destination, dstSerErr)
	}
	if memDelta != 0 {
		db.updateMemoryUsage(memDelta)
	}
	if srcDelete {
		if err := db.persistenceDelete(source); err != nil {
			return false, err
		}
	} else if srcBytes != nil {
		if err := db.persistencePut(source, srcBytes); err != nil {
			return false, err
		}
	}
	if dstBytes != nil {
		if err := db.persistencePut(dstStableKey, dstBytes); err != nil {
			return false, err
		}
	}
	if moved {
		db.BumpKeyVersions(source, dstStableKey)
	}
	return moved, nil
}

func (db *Database) sInterStoreLocked(destination string, keys []string) (int64, error) {
	return db.setStore(destination, keys, "inter")
}

func (db *Database) sUnionStoreLocked(destination string, keys []string) (int64, error) {
	return db.setStore(destination, keys, "union")
}

func (db *Database) sDiffStoreLocked(destination string, keys []string) (int64, error) {
	return db.setStore(destination, keys, "diff")
}

func (db *Database) setStore(destination string, keys []string, mode string) (int64, error) {
	result, err := db.setOp(keys, mode)
	if err != nil {
		return 0, err
	}
	sort.Strings(result)

	if len(result) == 0 {
		if _, err := db.deleteWithErrorLocked(destination); err != nil {
			return 0, err
		}
		return 0, nil
	}
	if !db.evictIfNeeded() {
		return 0, fmt.Errorf("OOM command not allowed when used memory (%d) > 'maxmemory' (%d)", db.usedMemory, db.maxMemory)
	}

	set := &datastruct.Set{Data: make(map[string]struct{}, len(result))}
	for _, member := range result {
		set.Add(strings.Clone(member))
	}
	if err := db.setLocked(destination, &datastruct.DataValue{
		Value:          set,
		ExpireTime:     0,
		LastAccessedAt: time.Now().UnixMilli(),
	}); err != nil {
		return 0, err
	}
	return int64(len(result)), nil
}

func (db *Database) setOp(keys []string, mode string) ([]string, error) {
	if len(keys) == 0 {
		return []string{}, nil
	}

	sets := make([]map[string]struct{}, len(keys))
	for i, key := range keys {
		if _, _, err := db.getLiveValue(key); err != nil {
			return nil, err
		}
		sh := db.getShard(key)
		now := time.Now().UnixMilli()

		sh.lock.RLock()
		dv, exists := sh.data[key]
		if !exists || dv == nil || (dv.ExpireTime > 0 && now > dv.ExpireTime) {
			sh.lock.RUnlock()
			sets[i] = map[string]struct{}{}
			continue
		}
		s, ok := dv.Value.(*datastruct.Set)
		if !ok {
			sh.lock.RUnlock()
			return nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		copied := make(map[string]struct{}, len(s.Data))
		for member := range s.Data {
			copied[member] = struct{}{}
		}
		sh.lock.RUnlock()
		sets[i] = copied
	}

	switch mode {
	case "union":
		out := make(map[string]struct{})
		for _, set := range sets {
			for member := range set {
				out[member] = struct{}{}
			}
		}
		return mapKeys(out), nil
	case "diff":
		out := make(map[string]struct{}, len(sets[0]))
		for member := range sets[0] {
			out[member] = struct{}{}
		}
		for _, set := range sets[1:] {
			for member := range set {
				delete(out, member)
			}
		}
		return mapKeys(out), nil
	default:
		out := make(map[string]struct{}, len(sets[0]))
		for member := range sets[0] {
			out[member] = struct{}{}
		}
		for _, set := range sets[1:] {
			for member := range out {
				if _, ok := set[member]; !ok {
					delete(out, member)
				}
			}
		}
		return mapKeys(out), nil
	}
}

func mapKeys(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for key := range m {
		out = append(out, key)
	}
	return out
}

func (db *Database) SAdd(key string, members []string) (result0 int64, err error) {
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

	return db.sAddLocked(key, members)
}

func (db *Database) SRem(key string, members []string) (result0 int64, err error) {
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

	return db.sRemLocked(key, members)
}

func (db *Database) SIsMember(key, member string) (result0 bool, err error) {
	db.operationMu.Lock()
	defer db.endReadOperation()
	if db.closed {
		err = errDatabaseClosed
		return
	}
	return db.sIsMemberLocked(key, member)
}

func (db *Database) SMembers(key string) (result0 []string, err error) {
	db.operationMu.Lock()
	defer db.endReadOperation()
	if db.closed {
		err = errDatabaseClosed
		return
	}
	return db.sMembersLocked(key)
}

func (db *Database) SCard(key string) (result0 int64, err error) {
	db.operationMu.Lock()
	defer db.endReadOperation()
	if db.closed {
		err = errDatabaseClosed
		return
	}
	return db.sCardLocked(key)
}

func (db *Database) SInter(keys []string) (result0 []string, err error) {
	db.operationMu.Lock()
	defer db.endReadOperation()
	if db.closed {
		err = errDatabaseClosed
		return
	}
	return db.sInterLocked(keys)
}

func (db *Database) SUnion(keys []string) (result0 []string, err error) {
	db.operationMu.Lock()
	defer db.endReadOperation()
	if db.closed {
		err = errDatabaseClosed
		return
	}
	return db.sUnionLocked(keys)
}

func (db *Database) SDiff(keys []string) (result0 []string, err error) {
	db.operationMu.Lock()
	defer db.endReadOperation()
	if db.closed {
		err = errDatabaseClosed
		return
	}
	return db.sDiffLocked(keys)
}

func (db *Database) SInterCard(keys []string, limit int64) (result0 int64, err error) {
	db.operationMu.Lock()
	defer db.endReadOperation()
	if db.closed {
		err = errDatabaseClosed
		return
	}
	return db.sInterCardLocked(keys, limit)
}

func (db *Database) SMIsMember(key string, members []string) (result0 []int64, err error) {
	db.operationMu.Lock()
	defer db.endReadOperation()
	if db.closed {
		err = errDatabaseClosed
		return
	}
	return db.sMIsMemberLocked(key, members)
}

func (db *Database) SPop(key string) (result0 string, result1 bool, err error) {
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

	return db.sPopLocked(key)
}

func (db *Database) SRandMember(key string) (result0 string, result1 bool, err error) {
	db.operationMu.Lock()
	defer db.endReadOperation()
	if db.closed {
		err = errDatabaseClosed
		return
	}
	return db.sRandMemberLocked(key)
}

func (db *Database) SScan(key string, cursor int, pattern string, count int) (result0 int, result1 []string, err error) {
	db.operationMu.Lock()
	defer db.endReadOperation()
	if db.closed {
		err = errDatabaseClosed
		return
	}
	return db.sScanLocked(key, cursor, pattern, count)
}

func (db *Database) SMove(source, destination, member string) (result0 bool, err error) {
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

	return db.sMoveLocked(source, destination, member)
}

func (db *Database) SInterStore(destination string, keys []string) (result0 int64, err error) {
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

	return db.sInterStoreLocked(destination, keys)
}

func (db *Database) SUnionStore(destination string, keys []string) (result0 int64, err error) {
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

	return db.sUnionStoreLocked(destination, keys)
}

func (db *Database) SDiffStore(destination string, keys []string) (result0 int64, err error) {
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

	return db.sDiffStoreLocked(destination, keys)
}
