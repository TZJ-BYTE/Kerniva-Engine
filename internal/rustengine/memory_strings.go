package rustengine

import (
	"errors"
	"runtime"
	"unsafe"
)

// PutOptions sets a value conditionally. A zero TTL makes the key persistent.
func (e *MemoryEngine) PutOptions(key, value []byte, condition MemorySetCondition, ttlMilliseconds uint64) (bool, error) {
	if e == nil {
		return false, errMemoryEngineClosed
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.handle == 0 {
		return false, errMemoryEngineClosed
	}
	options := nativeMemoryPutOptions{TTLMillis: ttlMilliseconds, Condition: condition}
	result, _, _ := e.api.putOptions.Call(
		e.handle, uintptr(memoryBytesPointer(key)), uintptr(len(key)),
		uintptr(memoryBytesPointer(value)), uintptr(len(value)), uintptr(unsafe.Pointer(&options)),
	)
	runtime.KeepAlive(key)
	runtime.KeepAlive(value)
	return memoryApplied("put options", int32(result))
}

// Expire sets a relative TTL, or removes a present key for a nonpositive TTL.
func (e *MemoryEngine) Expire(key []byte, ttlMilliseconds int64) (bool, error) {
	if e == nil {
		return false, errMemoryEngineClosed
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.handle == 0 {
		return false, errMemoryEngineClosed
	}
	result, _, _ := e.api.expire.Call(
		e.handle, uintptr(memoryBytesPointer(key)), uintptr(len(key)), uintptr(unsafe.Pointer(&ttlMilliseconds)),
	)
	runtime.KeepAlive(key)
	return memoryApplied("expire", int32(result))
}

// PTTL returns remaining milliseconds, -1 for a persistent key or -2 for a missing key.
func (e *MemoryEngine) PTTL(key []byte) (int64, error) {
	if e == nil {
		return 0, errMemoryEngineClosed
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.handle == 0 {
		return 0, errMemoryEngineClosed
	}
	var ttl int64
	result, _, _ := e.api.pttl.Call(
		e.handle, uintptr(memoryBytesPointer(key)), uintptr(len(key)), uintptr(unsafe.Pointer(&ttl)),
	)
	runtime.KeepAlive(key)
	return ttl, memoryStatusError("pttl", int32(result))
}

// Increment applies a signed delta and retains the deadline of an existing value.
func (e *MemoryEngine) Increment(key []byte, by int64) (int64, error) {
	if e == nil {
		return 0, errMemoryEngineClosed
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.handle == 0 {
		return 0, errMemoryEngineClosed
	}
	var value int64
	result, _, _ := e.api.increment.Call(
		e.handle, uintptr(memoryBytesPointer(key)), uintptr(len(key)), uintptr(unsafe.Pointer(&by)),
		uintptr(unsafe.Pointer(&value)),
	)
	runtime.KeepAlive(key)
	return value, memoryStatusError("increment", int32(result))
}

func (e *MemoryEngine) CountExisting(keys [][]byte) (int64, error) {
	return e.memoryKeyCount(keys, false)
}

func (e *MemoryEngine) DeleteMany(keys [][]byte) (int64, error) {
	return e.memoryKeyCount(keys, true)
}

func (e *MemoryEngine) memoryKeyCount(keys [][]byte, remove bool) (int64, error) {
	if e == nil {
		return 0, errMemoryEngineClosed
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.handle == 0 {
		return 0, errMemoryEngineClosed
	}
	proc, operation := e.api.countExisting, "count existing"
	if remove {
		proc, operation = e.api.deleteMany, "delete many"
	}
	nativeKeys := make([]nativeMemorySlice, len(keys))
	for i, key := range keys {
		nativeKeys[i] = nativeMemorySlice{Data: memoryBytesPointer(key), Len: uintptr(len(key))}
	}
	var count uintptr
	result, _, _ := proc.Call(
		e.handle, uintptr(unsafe.Pointer(unsafe.SliceData(nativeKeys))), uintptr(len(keys)),
		uintptr(unsafe.Pointer(&count)),
	)
	runtime.KeepAlive(keys)
	runtime.KeepAlive(nativeKeys)
	if err := memoryStatusError(operation, int32(result)); err != nil {
		return 0, err
	}
	if count > uintptr(len(keys)) {
		return 0, errors.New("memory engine returned an invalid key count")
	}
	return int64(count), nil
}

func memoryApplied(operation string, status int32) (bool, error) {
	if status == 1 {
		return false, nil
	}
	return status == 0, memoryStatusError(operation, status)
}
