package database

import (
	"errors"
	"math/rand"
	"strings"
	"time"

	"github.com/TZJ-BYTE/Kerniva/internal/datastruct"
	"github.com/TZJ-BYTE/Kerniva/pkg/logger"
)

func (db *Database) getExpireTimeLocked(key string) (int64, bool) {
	dv, exists, err := db.getLiveValue(key)
	if err != nil || !exists || dv == nil {
		if err != nil {
			db.recordLSMError(err)
			logger.Warn("Failed to read TTL for key %s: %v", key, err)
		}
		return 0, false
	}
	return dv.ExpireTime, true
}

func (db *Database) renameLocked(oldKey, newKey string, nx bool) (bool, error) {
	if oldKey == newKey {
		if nx {
			return false, nil
		}
		_, ok := db.getExpireTimeLocked(oldKey)
		if !ok {
			return false, errors.New("ERR no such key")
		}
		return true, nil
	}

	if _, _, err := db.getLiveValue(oldKey); err != nil {
		return false, err
	}
	if _, _, err := db.getLiveValue(newKey); err != nil {
		return false, err
	}

	oldIdx := getShardIndex(oldKey)
	newIdx := getShardIndex(newKey)
	first := oldIdx
	second := newIdx
	if first > second {
		first, second = second, first
	}
	s1 := &db.shards[first]
	s2 := &db.shards[second]
	if first == second {
		s2 = s1
	}

	var memDelta int64
	var lsmBytes []byte
	var doPut bool
	var doDeleteOld bool
	var stableNewKey string
	var serErr error

	s1.lock.Lock()
	if s2 != s1 {
		s2.lock.Lock()
	}

	now := time.Now().UnixMilli()

	oldShard := &db.shards[oldIdx]
	newShard := &db.shards[newIdx]
	oldVal, oldExists := oldShard.data[oldKey]
	if !oldExists || oldVal == nil || (oldVal.ExpireTime > 0 && now > oldVal.ExpireTime) {
		if s2 != s1 {
			s2.lock.Unlock()
		}
		s1.lock.Unlock()
		return false, errors.New("ERR no such key")
	}

	if nx {
		if nv, ok := newShard.data[newKey]; ok && nv != nil && !(nv.ExpireTime > 0 && now > nv.ExpireTime) {
			if s2 != s1 {
				s2.lock.Unlock()
			}
			s1.lock.Unlock()
			return false, nil
		}
	}

	if newShard.data == nil {
		newShard.data = make(map[string]*datastruct.DataValue)
	}

	newVal, newExists := newShard.data[newKey]
	var newOldSize int64
	if newVal != nil {
		newOldSize = newVal.ApproximateSize()
	}

	delete(oldShard.data, oldKey)

	stableNewKey = strings.Clone(newKey)
	if !newExists {
		memDelta += int64(len(stableNewKey))
	}
	newShard.data[stableNewKey] = oldVal

	if newExists {
		memDelta -= newOldSize
	}
	memDelta += -int64(len(oldKey))

	if db.hasPersistence() {
		b, err := oldVal.Serialize()
		if err == nil {
			lsmBytes = b
			doPut = true
			doDeleteOld = true
		} else {
			serErr = err
		}
	}

	if s2 != s1 {
		s2.lock.Unlock()
	}
	s1.lock.Unlock()

	if memDelta != 0 {
		db.updateMemoryUsage(memDelta)
	}
	if serErr != nil {
		db.recordLSMError(serErr)
		if db.persistenceWriteMode() == "weak" {
			logger.Error("Failed to serialize value for key %s: %v", oldKey, serErr)
		} else {
			return false, serErr
		}
	}
	if doDeleteOld {
		if err := db.persistenceDelete(oldKey); err != nil {
			return false, err
		}
	}
	if doPut {
		if err := db.persistencePut(stableNewKey, lsmBytes); err != nil {
			return false, err
		}
	}
	db.BumpKeyVersions(oldKey, stableNewKey)

	return true, nil
}

func (db *Database) expireLocked(key string, milliseconds int64) bool {
	ok, err := db.expireAt(key, time.Now().UnixMilli()+milliseconds)
	if err != nil {
		db.recordLSMError(err)
		logger.Warn("Failed to set expire for key %s: %v", key, err)
		return false
	}
	return ok
}

func (db *Database) expireWithOptionLocked(key string, milliseconds int64, option string) (bool, error) {
	return db.expireAtWithOption(key, time.Now().UnixMilli()+milliseconds, option)
}

func (db *Database) pExpireAtLocked(key string, expireAtMs int64) (bool, error) {
	return db.expireAt(key, expireAtMs)
}

func (db *Database) pExpireAtWithOptionLocked(key string, expireAtMs int64, option string) (bool, error) {
	return db.expireAtWithOption(key, expireAtMs, option)
}

func (db *Database) persistLocked(key string) (bool, error) {
	if _, _, err := db.getLiveValue(key); err != nil {
		return false, err
	}

	sh := db.getShard(key)
	sh.lock.Lock()
	dv, exists := sh.data[key]
	if !exists || dv == nil || dv.IsExpired() {
		sh.lock.Unlock()
		return false, nil
	}
	if dv.ExpireTime == 0 {
		sh.lock.Unlock()
		return false, nil
	}

	dv.ExpireTime = 0
	dv.LastAccessedAt = time.Now().UnixMilli()

	var serErr error
	var lsmBytes []byte
	if db.hasPersistence() {
		lsmBytes, serErr = dv.Serialize()
	}
	sh.lock.Unlock()

	if serErr != nil {
		db.recordLSMError(serErr)
		if db.persistenceWriteMode() == "weak" {
			logger.Error("Failed to serialize value for key %s: %v", key, serErr)
			return true, nil
		}
		return true, serErr
	}
	if lsmBytes != nil {
		if err := db.persistencePut(key, lsmBytes); err != nil {
			return true, err
		}
	}
	db.BumpKeyVersions(key)
	return true, nil
}

func (db *Database) typeOfLocked(key string) string {
	dv, exists, err := db.getLiveValue(key)
	if err != nil || !exists {
		if err != nil {
			db.recordLSMError(err)
			logger.Warn("Failed to detect type for key %s: %v", key, err)
		}
		return "none"
	}
	return dataTypeName(dv)
}

func (db *Database) expireAt(key string, expireAtMs int64) (bool, error) {
	if expireAtMs <= time.Now().UnixMilli() {
		return db.deleteWithErrorLocked(key)
	}
	if _, _, err := db.getLiveValue(key); err != nil {
		return false, err
	}

	sh := db.getShard(key)
	sh.lock.Lock()
	dv, exists := sh.data[key]
	if !exists || dv == nil || dv.IsExpired() {
		sh.lock.Unlock()
		return false, nil
	}

	dv.ExpireTime = expireAtMs
	dv.LastAccessedAt = time.Now().UnixMilli()

	var serErr error
	var lsmBytes []byte
	if db.hasPersistence() {
		lsmBytes, serErr = dv.Serialize()
	}
	sh.lock.Unlock()

	if serErr != nil {
		db.recordLSMError(serErr)
		if db.persistenceWriteMode() == "weak" {
			logger.Error("Failed to serialize value for key %s: %v", key, serErr)
			return true, nil
		}
		return true, serErr
	}
	if lsmBytes != nil {
		if err := db.persistencePut(key, lsmBytes); err != nil {
			return true, err
		}
	}
	db.BumpKeyVersions(key)
	return true, nil
}

func (db *Database) expireAtWithOption(key string, expireAtMs int64, option string) (bool, error) {
	if option == "" {
		return db.expireAt(key, expireAtMs)
	}

	if _, _, err := db.getLiveValue(key); err != nil {
		return false, err
	}

	sh := db.getShard(key)
	sh.lock.Lock()
	dv, exists := sh.data[key]
	if !exists || dv == nil || dv.IsExpired() {
		sh.lock.Unlock()
		return false, nil
	}

	currentExpireAt := dv.ExpireTime
	switch strings.ToUpper(option) {
	case "NX":
		if currentExpireAt != 0 {
			sh.lock.Unlock()
			return false, nil
		}
	case "XX":
		if currentExpireAt == 0 {
			sh.lock.Unlock()
			return false, nil
		}
	case "GT":
		if currentExpireAt == 0 || expireAtMs <= currentExpireAt {
			sh.lock.Unlock()
			return false, nil
		}
	case "LT":
		if currentExpireAt != 0 && expireAtMs >= currentExpireAt {
			sh.lock.Unlock()
			return false, nil
		}
	default:
		sh.lock.Unlock()
		return false, errors.New("ERR syntax error")
	}
	sh.lock.Unlock()

	return db.expireAt(key, expireAtMs)
}

func (db *Database) dumpValueLocked(key string) ([]byte, bool, error) {
	dv, exists, err := db.getLiveValue(key)
	if err != nil || !exists || dv == nil {
		return nil, false, err
	}
	data, err := dv.Serialize()
	if err != nil {
		return nil, false, err
	}
	return data, true, nil
}

func (db *Database) restoreSerializedValueLocked(key string, payload []byte, ttlMs int64, replace bool) error {
	if !replace && db.existsLocked(key) {
		return errors.New("BUSYKEY Target key name already exists.")
	}

	dv, err := datastruct.DeserializeDataValue(payload)
	if err != nil {
		return err
	}

	now := time.Now().UnixMilli()
	dv.LastAccessedAt = now
	if ttlMs >= 0 {
		if ttlMs == 0 {
			dv.ExpireTime = 0
		} else {
			dv.ExpireTime = now + ttlMs
		}
	}

	return db.setLocked(key, dv)
}

func (db *Database) CopyKeyTo(target *Database, sourceKey, targetKey string, replace bool) (bool, error) {
	if db == nil || target == nil {
		return false, nil
	}

	data, exists, err := db.DumpValue(sourceKey)
	if err != nil || !exists {
		return false, err
	}

	if !replace && target.Exists(targetKey) {
		return false, nil
	}

	if err := target.RestoreSerializedValue(targetKey, data, -1, replace); err != nil {
		if err.Error() == "BUSYKEY Target key name already exists." {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (db *Database) touchLocked(key string) (bool, error) {
	if _, _, err := db.getLiveValue(key); err != nil {
		return false, err
	}

	sh := db.getShard(key)
	sh.lock.Lock()
	dv, exists := sh.data[key]
	if !exists || dv == nil || dv.IsExpired() {
		sh.lock.Unlock()
		return false, nil
	}

	dv.LastAccessedAt = time.Now().UnixMilli()

	var serErr error
	var lsmBytes []byte
	if db.hasPersistence() {
		lsmBytes, serErr = dv.Serialize()
	}
	sh.lock.Unlock()

	if serErr != nil {
		db.recordLSMError(serErr)
		if db.persistenceWriteMode() == "weak" {
			logger.Error("Failed to serialize value for key %s: %v", key, serErr)
			return true, nil
		}
		return true, serErr
	}
	if lsmBytes != nil {
		if err := db.persistencePut(key, lsmBytes); err != nil {
			return true, err
		}
	}
	return true, nil
}

func (db *Database) randomKeyLocked() (string, bool) {
	keys := db.keysLocked()
	if len(keys) == 0 {
		return "", false
	}
	return keys[rand.Intn(len(keys))], true
}

func (db *Database) GetExpireTime(key string) (result0 int64, result1 bool) {
	db.operationMu.Lock()
	defer db.endReadOperation()
	if db.closed {
		return
	}
	return db.getExpireTimeLocked(key)
}

func (db *Database) Rename(oldKey, newKey string, nx bool) (result0 bool, err error) {
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

	return db.renameLocked(oldKey, newKey, nx)
}

func (db *Database) Expire(key string, milliseconds int64) (result0 bool) {
	db.operationMu.Lock()
	defer db.operationMu.Unlock()
	if db.closed {
		return
	}

	db.beginMutation()
	defer func() {
		if commitErr := db.finishMutation(nil); commitErr != nil {
			var zero0 bool
			result0 = zero0
		}
	}()

	return db.expireLocked(key, milliseconds)
}

func (db *Database) ExpireWithOption(key string, milliseconds int64, option string) (result0 bool, err error) {
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

	return db.expireWithOptionLocked(key, milliseconds, option)
}

func (db *Database) PExpireAt(key string, expireAtMs int64) (result0 bool, err error) {
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

	return db.pExpireAtLocked(key, expireAtMs)
}

func (db *Database) PExpireAtWithOption(key string, expireAtMs int64, option string) (result0 bool, err error) {
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

	return db.pExpireAtWithOptionLocked(key, expireAtMs, option)
}

func (db *Database) Persist(key string) (result0 bool, err error) {
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

	return db.persistLocked(key)
}

func (db *Database) TypeOf(key string) (result0 string) {
	db.operationMu.Lock()
	defer db.endReadOperation()
	if db.closed {
		return
	}
	return db.typeOfLocked(key)
}

func (db *Database) DumpValue(key string) (result0 []byte, result1 bool, err error) {
	db.operationMu.Lock()
	defer db.endReadOperation()
	if db.closed {
		err = errDatabaseClosed
		return
	}
	return db.dumpValueLocked(key)
}

func (db *Database) RestoreSerializedValue(key string, payload []byte, ttlMs int64, replace bool) (err error) {
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

	return db.restoreSerializedValueLocked(key, payload, ttlMs, replace)
}

func (db *Database) Touch(key string) (result0 bool, err error) {
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

	return db.touchLocked(key)
}

func (db *Database) RandomKey() (result0 string, result1 bool) {
	db.operationMu.Lock()
	defer db.endReadOperation()
	if db.closed {
		return
	}
	return db.randomKeyLocked()
}
