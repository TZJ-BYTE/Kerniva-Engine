package database

import (
	"fmt"
	"math"
	"math/rand"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/TZJ-BYTE/Kerniva/internal/datastruct"
	"github.com/TZJ-BYTE/Kerniva/pkg/logger"
)

func (db *Database) zAddLocked(key string, entries []datastruct.ZSetMember) (int64, error) {
	if len(entries) == 0 {
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
	var zs *datastruct.ZSet
	if dv.Value == nil || (dv.ExpireTime > 0 && now > dv.ExpireTime) {
		zs = &datastruct.ZSet{Data: make(map[string]float64)}
		dv.Value = zs
		dv.ExpireTime = 0
	} else {
		zv, ok := dv.Value.(*datastruct.ZSet)
		if !ok {
			sh.lock.Unlock()
			return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		zs = zv
		if zs.Data == nil {
			zs.Data = make(map[string]float64)
		}
	}

	stableEntries := make([]datastruct.ZSetMember, len(entries))
	for i, entry := range entries {
		if math.IsNaN(entry.Score) {
			sh.lock.Unlock()
			return 0, fmt.Errorf("ERR value is not a valid float")
		}
		stableEntries[i] = datastruct.ZSetMember{Member: strings.Clone(entry.Member), Score: entry.Score}
	}
	added = zs.AddMany(stableEntries)
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

func (db *Database) zRemLocked(key string, members []string) (int64, error) {
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

	zs, ok := dv.Value.(*datastruct.ZSet)
	if !ok {
		sh.lock.Unlock()
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	oldSize := dv.ApproximateSize()
	for _, member := range members {
		if zs.Remove(member) {
			removed++
		}
	}

	if len(zs.Data) == 0 {
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

func (db *Database) zCardLocked(key string) (int64, error) {
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
	zs, ok := dv.Value.(*datastruct.ZSet)
	if !ok {
		sh.lock.RUnlock()
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	out := int64(len(zs.Data))
	sh.lock.RUnlock()
	return out, nil
}

func (db *Database) zScoreLocked(key, member string) (float64, bool, error) {
	if _, _, err := db.getLiveValue(key); err != nil {
		return 0, false, err
	}
	sh := db.getShard(key)
	now := time.Now().UnixMilli()

	sh.lock.RLock()
	dv, exists := sh.data[key]
	if !exists || dv == nil || (dv.ExpireTime > 0 && now > dv.ExpireTime) {
		sh.lock.RUnlock()
		return 0, false, nil
	}
	zs, ok := dv.Value.(*datastruct.ZSet)
	if !ok {
		sh.lock.RUnlock()
		return 0, false, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	score, ok := zs.Score(member)
	sh.lock.RUnlock()
	return score, ok, nil
}

func (db *Database) zMScoreLocked(key string, members []string) ([]*float64, error) {
	if _, _, err := db.getLiveValue(key); err != nil {
		return nil, err
	}
	sh := db.getShard(key)
	now := time.Now().UnixMilli()

	sh.lock.RLock()
	dv, exists := sh.data[key]
	if !exists || dv == nil || (dv.ExpireTime > 0 && now > dv.ExpireTime) {
		sh.lock.RUnlock()
		return make([]*float64, len(members)), nil
	}
	zs, ok := dv.Value.(*datastruct.ZSet)
	if !ok {
		sh.lock.RUnlock()
		return nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	out := make([]*float64, len(members))
	for i, member := range members {
		if score, exists := zs.Score(member); exists {
			scoreCopy := score
			out[i] = &scoreCopy
		}
	}
	sh.lock.RUnlock()
	return out, nil
}

func (db *Database) zIncrByLocked(key, member string, delta float64) (float64, error) {
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
	var zs *datastruct.ZSet
	if dv.Value == nil || (dv.ExpireTime > 0 && now > dv.ExpireTime) {
		zs = &datastruct.ZSet{Data: make(map[string]float64)}
		dv.Value = zs
		dv.ExpireTime = 0
	} else {
		zv, ok := dv.Value.(*datastruct.ZSet)
		if !ok {
			sh.lock.Unlock()
			return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		zs = zv
		if zs.Data == nil {
			zs.Data = make(map[string]float64)
		}
	}

	current, _ := zs.Score(member)
	out = current + delta
	zs.Add(strings.Clone(member), out)
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
	return out, nil
}

func (db *Database) zRangeLocked(key string, start, stop int, reverse bool, withScores bool) ([]string, error) {
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
	zs, ok := dv.Value.(*datastruct.ZSet)
	if !ok {
		sh.lock.RUnlock()
		return nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	scores := zs.Scores
	length := len(scores)
	if reverse && length > 0 {
		reversed := make([]datastruct.ZSetMember, length)
		for i := range scores {
			reversed[i] = scores[length-1-i]
		}
		scores = reversed
	}

	lo, hi, ok := normalizeRange(length, start, stop)
	if !ok {
		sh.lock.RUnlock()
		return []string{}, nil
	}

	out := make([]string, 0, (hi-lo+1)*2)
	for _, item := range scores[lo : hi+1] {
		out = append(out, item.Member)
		if withScores {
			out = append(out, strconv.FormatFloat(item.Score, 'f', -1, 64))
		}
	}
	sh.lock.RUnlock()
	return out, nil
}

func (db *Database) zCountLocked(key string, min, max float64) (int64, error) {
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
	zs, ok := dv.Value.(*datastruct.ZSet)
	if !ok {
		sh.lock.RUnlock()
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	out := int64(len(zs.RangeByScore(min, max)))
	sh.lock.RUnlock()
	return out, nil
}

func (db *Database) zRankLocked(key, member string, reverse bool) (int64, bool, error) {
	if _, _, err := db.getLiveValue(key); err != nil {
		return 0, false, err
	}
	sh := db.getShard(key)
	now := time.Now().UnixMilli()

	sh.lock.RLock()
	dv, exists := sh.data[key]
	if !exists || dv == nil || (dv.ExpireTime > 0 && now > dv.ExpireTime) {
		sh.lock.RUnlock()
		return 0, false, nil
	}
	zs, ok := dv.Value.(*datastruct.ZSet)
	if !ok {
		sh.lock.RUnlock()
		return 0, false, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	scores := zs.Scores
	if reverse {
		for i := len(scores) - 1; i >= 0; i-- {
			if scores[i].Member == member {
				rank := int64(len(scores) - 1 - i)
				sh.lock.RUnlock()
				return rank, true, nil
			}
		}
		sh.lock.RUnlock()
		return 0, false, nil
	}
	for i, item := range scores {
		if item.Member == member {
			sh.lock.RUnlock()
			return int64(i), true, nil
		}
	}
	sh.lock.RUnlock()
	return 0, false, nil
}

func (db *Database) zRangeByScoreLocked(key string, min, max float64, withScores bool, reverse bool) ([]string, error) {
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
	zs, ok := dv.Value.(*datastruct.ZSet)
	if !ok {
		sh.lock.RUnlock()
		return nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	members := zs.RangeByScore(min, max)
	if reverse {
		for i, j := 0, len(members)-1; i < j; i, j = i+1, j-1 {
			members[i], members[j] = members[j], members[i]
		}
	}

	out := make([]string, 0, len(members)*2)
	for _, item := range members {
		out = append(out, item.Member)
		if withScores {
			out = append(out, strconv.FormatFloat(item.Score, 'f', -1, 64))
		}
	}
	sh.lock.RUnlock()
	return out, nil
}

func (db *Database) zUnionLocked(keys []string, weights []float64, aggregate string) ([]datastruct.ZSetMember, error) {
	return db.zsetOp(keys, weights, aggregate, "union")
}

func (db *Database) zInterLocked(keys []string, weights []float64, aggregate string) ([]datastruct.ZSetMember, error) {
	return db.zsetOp(keys, weights, aggregate, "inter")
}

func (db *Database) zDiffLocked(keys []string) ([]datastruct.ZSetMember, error) {
	return db.zsetOp(keys, nil, "", "diff")
}

func (db *Database) zUnionStoreLocked(destination string, keys []string, weights []float64, aggregate string) (int64, error) {
	return db.zsetStore(destination, keys, weights, aggregate, "union")
}

func (db *Database) zInterStoreLocked(destination string, keys []string, weights []float64, aggregate string) (int64, error) {
	return db.zsetStore(destination, keys, weights, aggregate, "inter")
}

func (db *Database) zDiffStoreLocked(destination string, keys []string) (int64, error) {
	return db.zsetStore(destination, keys, nil, "", "diff")
}

func (db *Database) zRandMemberLocked(key string) (string, float64, bool, error) {
	if _, _, err := db.getLiveValue(key); err != nil {
		return "", 0, false, err
	}

	sh := db.getShard(key)
	now := time.Now().UnixMilli()

	sh.lock.RLock()
	dv, exists := sh.data[key]
	if !exists || dv == nil || (dv.ExpireTime > 0 && now > dv.ExpireTime) {
		sh.lock.RUnlock()
		return "", 0, false, nil
	}
	zs, ok := dv.Value.(*datastruct.ZSet)
	if !ok {
		sh.lock.RUnlock()
		return "", 0, false, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	if len(zs.Scores) == 0 {
		sh.lock.RUnlock()
		return "", 0, false, nil
	}
	item := zs.Scores[rand.Intn(len(zs.Scores))]
	sh.lock.RUnlock()
	return item.Member, item.Score, true, nil
}

func (db *Database) zRandMembersLocked(key string, count int, withScores bool) ([]string, error) {
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
	zs, ok := dv.Value.(*datastruct.ZSet)
	if !ok {
		sh.lock.RUnlock()
		return nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	if len(zs.Scores) == 0 || count == 0 {
		sh.lock.RUnlock()
		return []string{}, nil
	}

	items := zs.Scores
	out := make([]string, 0, len(items)*2)
	if count > 0 {
		if count > len(items) {
			count = len(items)
		}
		perm := rand.Perm(len(items))
		out = make([]string, 0, count*2)
		for i := 0; i < count; i++ {
			item := items[perm[i]]
			out = append(out, item.Member)
			if withScores {
				out = append(out, strconv.FormatFloat(item.Score, 'f', -1, 64))
			}
		}
		sh.lock.RUnlock()
		return out, nil
	}

	dupCount := -count
	out = make([]string, 0, dupCount*2)
	for i := 0; i < dupCount; i++ {
		item := items[rand.Intn(len(items))]
		out = append(out, item.Member)
		if withScores {
			out = append(out, strconv.FormatFloat(item.Score, 'f', -1, 64))
		}
	}
	sh.lock.RUnlock()
	return out, nil
}

func (db *Database) zRemRangeByRankLocked(key string, start, stop int) (int64, error) {
	if _, _, err := db.getLiveValue(key); err != nil {
		return 0, err
	}

	sh := db.getShard(key)
	now := time.Now().UnixMilli()

	var memDelta int64
	var lsmBytes []byte
	var deleteKey bool
	var serErr error
	var removed int64

	sh.lock.Lock()
	dv, exists := sh.data[key]
	if !exists || dv == nil || (dv.ExpireTime > 0 && now > dv.ExpireTime) {
		sh.lock.Unlock()
		return 0, nil
	}
	zs, ok := dv.Value.(*datastruct.ZSet)
	if !ok {
		sh.lock.Unlock()
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	oldSize := dv.ApproximateSize()
	lo, hi, ok := normalizeRange(len(zs.Scores), start, stop)
	if !ok {
		sh.lock.Unlock()
		return 0, nil
	}
	for _, item := range zs.Scores[lo : hi+1] {
		if zs.Remove(item.Member) {
			removed++
		}
	}

	if len(zs.Data) == 0 {
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

func (db *Database) zRemRangeByScoreLocked(key string, min, max float64) (int64, error) {
	if _, _, err := db.getLiveValue(key); err != nil {
		return 0, err
	}

	sh := db.getShard(key)
	now := time.Now().UnixMilli()

	var memDelta int64
	var lsmBytes []byte
	var deleteKey bool
	var serErr error
	var removed int64

	sh.lock.Lock()
	dv, exists := sh.data[key]
	if !exists || dv == nil || (dv.ExpireTime > 0 && now > dv.ExpireTime) {
		sh.lock.Unlock()
		return 0, nil
	}
	zs, ok := dv.Value.(*datastruct.ZSet)
	if !ok {
		sh.lock.Unlock()
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	oldSize := dv.ApproximateSize()
	for _, item := range zs.RangeByScore(min, max) {
		if zs.Remove(item.Member) {
			removed++
		}
	}
	if len(zs.Data) == 0 {
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

func (db *Database) zPopLocked(key string, max bool) ([]string, error) {
	if _, _, err := db.getLiveValue(key); err != nil {
		return nil, err
	}

	sh := db.getShard(key)
	now := time.Now().UnixMilli()

	var memDelta int64
	var lsmBytes []byte
	var deleteKey bool
	var serErr error
	var item datastruct.ZSetMember
	var ok bool

	sh.lock.Lock()
	dv, exists := sh.data[key]
	if !exists || dv == nil || (dv.ExpireTime > 0 && now > dv.ExpireTime) {
		sh.lock.Unlock()
		return []string{}, nil
	}
	zs, zv := dv.Value.(*datastruct.ZSet)
	if !zv {
		sh.lock.Unlock()
		return nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	if len(zs.Scores) == 0 {
		sh.lock.Unlock()
		return []string{}, nil
	}

	oldSize := dv.ApproximateSize()
	if max {
		item = zs.Scores[len(zs.Scores)-1]
	} else {
		item = zs.Scores[0]
	}
	ok = zs.Remove(item.Member)
	if !ok {
		sh.lock.Unlock()
		return []string{}, nil
	}

	if len(zs.Data) == 0 {
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
			return nil, serErr
		}
	}
	if memDelta != 0 {
		db.updateMemoryUsage(memDelta)
	}
	if deleteKey {
		if err := db.persistenceDelete(key); err != nil {
			return nil, err
		}
	} else if lsmBytes != nil {
		if err := db.persistencePut(key, lsmBytes); err != nil {
			return nil, err
		}
	}
	db.BumpKeyVersions(key)
	return []string{item.Member, strconv.FormatFloat(item.Score, 'f', -1, 64)}, nil
}

func (db *Database) zScanLocked(key string, cursor int, pattern string, count int) (int, []string, error) {
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
	zs, ok := dv.Value.(*datastruct.ZSet)
	if !ok {
		sh.lock.RUnlock()
		return 0, nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	entries := zs.Scores
	result := make([]string, 0, count*2)
	idx := cursor
	for idx < len(entries) && len(result) < count*2 {
		entry := entries[idx]
		matched := true
		if pattern != "*" {
			ok, err := path.Match(pattern, entry.Member)
			if err != nil {
				sh.lock.RUnlock()
				return 0, nil, fmt.Errorf("ERR invalid pattern")
			}
			matched = ok
		}
		if matched {
			result = append(result, entry.Member, strconv.FormatFloat(entry.Score, 'f', -1, 64))
		}
		idx++
	}
	sh.lock.RUnlock()

	if idx >= len(entries) {
		return 0, result, nil
	}
	return idx, result, nil
}

func (db *Database) zsetStore(destination string, keys []string, weights []float64, aggregate string, mode string) (int64, error) {
	result, err := db.zsetOp(keys, weights, aggregate, mode)
	if err != nil {
		return 0, err
	}
	if len(result) == 0 {
		if _, err := db.deleteWithErrorLocked(destination); err != nil {
			return 0, err
		}
		return 0, nil
	}
	if !db.evictIfNeeded() {
		return 0, fmt.Errorf("OOM command not allowed when used memory (%d) > 'maxmemory' (%d)", db.usedMemory, db.maxMemory)
	}

	zs := &datastruct.ZSet{Data: make(map[string]float64, len(result))}
	for _, item := range result {
		zs.Add(strings.Clone(item.Member), item.Score)
	}
	if err := db.setLocked(destination, &datastruct.DataValue{
		Value:          zs,
		ExpireTime:     0,
		LastAccessedAt: time.Now().UnixMilli(),
	}); err != nil {
		return 0, err
	}
	return int64(len(result)), nil
}

func (db *Database) zsetOp(keys []string, weights []float64, aggregate string, mode string) ([]datastruct.ZSetMember, error) {
	if len(keys) == 0 {
		return []datastruct.ZSetMember{}, nil
	}
	if mode != "diff" {
		var err error
		aggregate, err = normalizeZSetAggregate(aggregate)
		if err != nil {
			return nil, err
		}
		weights, err = normalizeZSetWeights(len(keys), weights)
		if err != nil {
			return nil, err
		}
	}

	sets := make([]map[string]float64, len(keys))
	for i, key := range keys {
		snapshot, err := db.zsetSnapshot(key)
		if err != nil {
			return nil, err
		}
		sets[i] = snapshot
	}

	switch mode {
	case "union":
		result := make(map[string]float64)
		for i, set := range sets {
			weight := weights[i]
			for member, score := range set {
				weighted := score * weight
				if current, exists := result[member]; exists {
					result[member] = aggregateZSetScore(current, weighted, aggregate)
				} else {
					result[member] = weighted
				}
			}
		}
		return zsetMembersFromMap(result), nil
	case "diff":
		result := make(map[string]float64, len(sets[0]))
		for member, score := range sets[0] {
			result[member] = score
		}
		for _, set := range sets[1:] {
			for member := range set {
				delete(result, member)
			}
		}
		return zsetMembersFromMap(result), nil
	default:
		if len(sets[0]) == 0 {
			return []datastruct.ZSetMember{}, nil
		}
		result := make(map[string]float64, len(sets[0]))
		for member, score := range sets[0] {
			result[member] = score * weights[0]
		}
		for i, set := range sets[1:] {
			if len(set) == 0 {
				return []datastruct.ZSetMember{}, nil
			}
			weight := weights[i+1]
			for member, current := range result {
				score, exists := set[member]
				if !exists {
					delete(result, member)
					continue
				}
				result[member] = aggregateZSetScore(current, score*weight, aggregate)
			}
			if len(result) == 0 {
				return []datastruct.ZSetMember{}, nil
			}
		}
		return zsetMembersFromMap(result), nil
	}
}

func (db *Database) zsetSnapshot(key string) (map[string]float64, error) {
	if _, _, err := db.getLiveValue(key); err != nil {
		return nil, err
	}

	sh := db.getShard(key)
	now := time.Now().UnixMilli()
	sh.lock.RLock()
	dv, exists := sh.data[key]
	if !exists || dv == nil || (dv.ExpireTime > 0 && now > dv.ExpireTime) {
		sh.lock.RUnlock()
		return map[string]float64{}, nil
	}
	zs, ok := dv.Value.(*datastruct.ZSet)
	if !ok {
		sh.lock.RUnlock()
		return nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	copied := make(map[string]float64, len(zs.Data))
	for member, score := range zs.Data {
		copied[member] = score
	}
	sh.lock.RUnlock()
	return copied, nil
}

func normalizeZSetWeights(numKeys int, weights []float64) ([]float64, error) {
	if len(weights) == 0 {
		weights = make([]float64, numKeys)
		for i := range weights {
			weights[i] = 1
		}
		return weights, nil
	}
	if len(weights) != numKeys {
		return nil, fmt.Errorf("ERR syntax error")
	}
	return weights, nil
}

func normalizeZSetAggregate(aggregate string) (string, error) {
	if aggregate == "" {
		return "SUM", nil
	}
	switch strings.ToUpper(aggregate) {
	case "SUM", "MIN", "MAX":
		return strings.ToUpper(aggregate), nil
	default:
		return "", fmt.Errorf("ERR syntax error")
	}
}

func aggregateZSetScore(current, next float64, aggregate string) float64 {
	switch aggregate {
	case "MIN":
		if next < current {
			return next
		}
		return current
	case "MAX":
		if next > current {
			return next
		}
		return current
	default:
		return current + next
	}
}

func zsetMembersFromMap(values map[string]float64) []datastruct.ZSetMember {
	if len(values) == 0 {
		return []datastruct.ZSetMember{}
	}
	zs := &datastruct.ZSet{Data: make(map[string]float64, len(values))}
	for member, score := range values {
		zs.Add(strings.Clone(member), score)
	}
	return zs.Scores
}

func normalizeRange(length, start, stop int) (int, int, bool) {
	if length == 0 {
		return 0, 0, false
	}
	if start < 0 {
		start = length + start
	}
	if stop < 0 {
		stop = length + stop
	}
	if start < 0 {
		start = 0
	}
	if stop >= length {
		stop = length - 1
	}
	if start > stop || start >= length {
		return 0, 0, false
	}
	return start, stop, true
}

func ParseZCountBound(s string) (float64, error) {
	switch strings.ToLower(s) {
	case "-inf":
		return math.Inf(-1), nil
	case "+inf", "inf":
		return math.Inf(1), nil
	default:
		return strconv.ParseFloat(s, 64)
	}
}

func (db *Database) ZAdd(key string, entries []datastruct.ZSetMember) (result0 int64, err error) {
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

	return db.zAddLocked(key, entries)
}

func (db *Database) ZRem(key string, members []string) (result0 int64, err error) {
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

	return db.zRemLocked(key, members)
}

func (db *Database) ZCard(key string) (result0 int64, err error) {
	db.operationMu.Lock()
	defer db.endReadOperation()
	if db.closed {
		err = errDatabaseClosed
		return
	}
	return db.zCardLocked(key)
}

func (db *Database) ZScore(key, member string) (result0 float64, result1 bool, err error) {
	db.operationMu.Lock()
	defer db.endReadOperation()
	if db.closed {
		err = errDatabaseClosed
		return
	}
	return db.zScoreLocked(key, member)
}

func (db *Database) ZMScore(key string, members []string) (result0 []*float64, err error) {
	db.operationMu.Lock()
	defer db.endReadOperation()
	if db.closed {
		err = errDatabaseClosed
		return
	}
	return db.zMScoreLocked(key, members)
}

func (db *Database) ZIncrBy(key, member string, delta float64) (result0 float64, err error) {
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

	return db.zIncrByLocked(key, member, delta)
}

func (db *Database) ZRange(key string, start, stop int, reverse bool, withScores bool) (result0 []string, err error) {
	db.operationMu.Lock()
	defer db.endReadOperation()
	if db.closed {
		err = errDatabaseClosed
		return
	}
	return db.zRangeLocked(key, start, stop, reverse, withScores)
}

func (db *Database) ZCount(key string, min, max float64) (result0 int64, err error) {
	db.operationMu.Lock()
	defer db.endReadOperation()
	if db.closed {
		err = errDatabaseClosed
		return
	}
	return db.zCountLocked(key, min, max)
}

func (db *Database) ZRank(key, member string, reverse bool) (result0 int64, result1 bool, err error) {
	db.operationMu.Lock()
	defer db.endReadOperation()
	if db.closed {
		err = errDatabaseClosed
		return
	}
	return db.zRankLocked(key, member, reverse)
}

func (db *Database) ZRangeByScore(key string, min, max float64, withScores bool, reverse bool) (result0 []string, err error) {
	db.operationMu.Lock()
	defer db.endReadOperation()
	if db.closed {
		err = errDatabaseClosed
		return
	}
	return db.zRangeByScoreLocked(key, min, max, withScores, reverse)
}

func (db *Database) ZUnion(keys []string, weights []float64, aggregate string) (result0 []datastruct.ZSetMember, err error) {
	db.operationMu.Lock()
	defer db.endReadOperation()
	if db.closed {
		err = errDatabaseClosed
		return
	}
	return db.zUnionLocked(keys, weights, aggregate)
}

func (db *Database) ZInter(keys []string, weights []float64, aggregate string) (result0 []datastruct.ZSetMember, err error) {
	db.operationMu.Lock()
	defer db.endReadOperation()
	if db.closed {
		err = errDatabaseClosed
		return
	}
	return db.zInterLocked(keys, weights, aggregate)
}

func (db *Database) ZDiff(keys []string) (result0 []datastruct.ZSetMember, err error) {
	db.operationMu.Lock()
	defer db.endReadOperation()
	if db.closed {
		err = errDatabaseClosed
		return
	}
	return db.zDiffLocked(keys)
}

func (db *Database) ZUnionStore(destination string, keys []string, weights []float64, aggregate string) (result0 int64, err error) {
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

	return db.zUnionStoreLocked(destination, keys, weights, aggregate)
}

func (db *Database) ZInterStore(destination string, keys []string, weights []float64, aggregate string) (result0 int64, err error) {
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

	return db.zInterStoreLocked(destination, keys, weights, aggregate)
}

func (db *Database) ZDiffStore(destination string, keys []string) (result0 int64, err error) {
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

	return db.zDiffStoreLocked(destination, keys)
}

func (db *Database) ZRandMember(key string) (result0 string, result1 float64, result2 bool, err error) {
	db.operationMu.Lock()
	defer db.endReadOperation()
	if db.closed {
		err = errDatabaseClosed
		return
	}
	return db.zRandMemberLocked(key)
}

func (db *Database) ZRandMembers(key string, count int, withScores bool) (result0 []string, err error) {
	db.operationMu.Lock()
	defer db.endReadOperation()
	if db.closed {
		err = errDatabaseClosed
		return
	}
	return db.zRandMembersLocked(key, count, withScores)
}

func (db *Database) ZRemRangeByRank(key string, start, stop int) (result0 int64, err error) {
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

	return db.zRemRangeByRankLocked(key, start, stop)
}

func (db *Database) ZRemRangeByScore(key string, min, max float64) (result0 int64, err error) {
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

	return db.zRemRangeByScoreLocked(key, min, max)
}

func (db *Database) ZPop(key string, max bool) (result0 []string, err error) {
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

	return db.zPopLocked(key, max)
}

func (db *Database) ZScan(key string, cursor int, pattern string, count int) (result0 int, result1 []string, err error) {
	db.operationMu.Lock()
	defer db.endReadOperation()
	if db.closed {
		err = errDatabaseClosed
		return
	}
	return db.zScanLocked(key, cursor, pattern, count)
}
