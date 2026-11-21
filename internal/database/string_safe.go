package database

import (
	"fmt"

	"github.com/TZJ-BYTE/Kerniva/internal/datastruct"
	"github.com/TZJ-BYTE/Kerniva/internal/protocol"
)

func (db *Database) getStringBytesCopyLocked(key []byte) ([]byte, bool, error) {
	k := bytesToString(key)
	dv, exists, err := db.getLiveValue(k)
	if err != nil {
		return nil, false, err
	}
	if !exists || dv == nil {
		return nil, false, nil
	}

	shard := db.getShard(k)
	shard.lock.RLock()
	switch v := dv.Value.(type) {
	case *datastruct.String:
		out := []byte(v.Data)
		shard.lock.RUnlock()
		return out, true, nil
	case *datastruct.BytesString:
		out := make([]byte, len(v.Data))
		copy(out, v.Data)
		shard.lock.RUnlock()
		return out, true, nil
	default:
		shard.lock.RUnlock()
		return nil, false, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
}

func (db *Database) getStringCopyLocked(key []byte) (string, bool, error) {
	b, ok, err := db.getStringBytesCopyLocked(key)
	if !ok || err != nil {
		return "", ok, err
	}
	return protocol.BytesToString(b), true, nil
}

func (db *Database) GetStringBytesCopy(key []byte) (result0 []byte, result1 bool, err error) {
	parallel := db.beginKeyOperation(bytesToString(key), false)
	defer db.endKeyOperation(bytesToString(key), false, parallel)
	if db.closed {
		err = errDatabaseClosed
		return
	}
	return db.getStringBytesCopyLocked(key)
}

func (db *Database) GetStringCopy(key []byte) (result0 string, result1 bool, err error) {
	parallel := db.beginKeyOperation(bytesToString(key), false)
	defer db.endKeyOperation(bytesToString(key), false, parallel)
	if db.closed {
		err = errDatabaseClosed
		return
	}
	return db.getStringCopyLocked(key)
}
