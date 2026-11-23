package rustengine

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"unsafe"
)

var (
	ErrMemoryBufferTooSmall = errors.New("memory engine output buffer is too small")
	ErrMemoryPayloadLimit   = errors.New("memory engine resident payload limit exceeded")
	errMemoryEngineClosed   = errors.New("memory engine is closed")
)

// MemorySpan describes a value inside the caller-owned MGET output buffer.
// Found is 1 for a present key (including an empty value), or 0 for a missing key.
type MemorySpan struct {
	Offset uintptr
	Len    uintptr
	Found  uintptr
}

type MemoryPair struct {
	Key   []byte
	Value []byte
}

// MemoryStats counts resident key/value payload bytes, excluding map capacity,
// allocator overhead and immutable values retained by other Rust readers.
type MemoryStats struct {
	Keys         uintptr
	PayloadBytes uintptr
	Shards       uintptr
}

type nativeMemorySlice struct {
	Data unsafe.Pointer
	Len  uintptr
}

type nativeMemoryPair struct {
	Key   nativeMemorySlice
	Value nativeMemorySlice
}

type memoryDLLAPI struct {
	open, close, getInto, put, delete, mset, mgetInto, stats *nativeProc
}

var (
	memoryAPIMu sync.Mutex
	memoryAPI   *memoryDLLAPI
)

// MemoryEngine owns a native memory handle. Calls may execute concurrently;
// Close waits for all active calls and prevents subsequent calls from using it.
// A MemoryEngine must not be copied.
type MemoryEngine struct {
	mu     sync.RWMutex
	handle uintptr
	api    *memoryDLLAPI
}

// OpenMemory creates a directory-free Rust memory store. A nil limit means
// unlimited resident payload; a non-nil zero limit permits only zero payload.
func OpenMemory(shards int, maxPayloadBytes *uint64) (*MemoryEngine, error) {
	if shards <= 0 || shards&(shards-1) != 0 {
		return nil, errors.New("memory shard count must be a positive power of two")
	}
	var limited, limit uintptr
	if maxPayloadBytes != nil {
		limited = 1
		limit = uintptr(*maxPayloadBytes)
		if uint64(limit) != *maxPayloadBytes {
			return nil, errors.New("memory payload limit exceeds the native address size")
		}
	}
	native, err := ensureMemoryAPI()
	if err != nil {
		return nil, err
	}
	var handle uintptr
	result, _, _ := native.open.Call(
		uintptr(shards), limited, limit, uintptr(unsafe.Pointer(&handle)),
	)
	if err := memoryStatusError("open", int32(result)); err != nil {
		return nil, err
	}
	if handle == 0 {
		return nil, errors.New("open memory engine returned an invalid handle")
	}
	return &MemoryEngine{handle: handle, api: native}, nil
}

// GetInto copies a present value into dst without allocating a native result.
// On ErrMemoryBufferTooSmall, found is true and needed is the required length.
func (e *MemoryEngine) GetInto(key, dst []byte) (needed int, found bool, err error) {
	if e == nil {
		return 0, false, errMemoryEngineClosed
	}
	if memoryBytesOverlap(key, dst) {
		return 0, false, errors.New("memory GET key overlaps the output buffer")
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.handle == 0 {
		return 0, false, errMemoryEngineClosed
	}
	var nativeNeeded uintptr
	result, _, _ := e.api.getInto.Call(
		e.handle, uintptr(memoryBytesPointer(key)), uintptr(len(key)),
		uintptr(memoryBytesPointer(dst)), uintptr(len(dst)),
		uintptr(unsafe.Pointer(&nativeNeeded)),
	)
	runtime.KeepAlive(key)
	runtime.KeepAlive(dst)
	status := int32(result)
	if status == 1 {
		return 0, false, nil
	}
	if status != 0 && status != 2 {
		return 0, false, memoryStatusError("get", status)
	}
	needed, err = memorySizeToInt(nativeNeeded)
	if err != nil {
		return 0, true, err
	}
	if status == 2 {
		return needed, true, ErrMemoryBufferTooSmall
	}
	if needed > len(dst) {
		return needed, true, errors.New("get memory engine returned an invalid output length")
	}
	return needed, true, nil
}

// GetManyInto copies all present values into dst and writes one span per key.
// spans must have at least len(keys) elements. Buffer-short results report the
// required total size; callers should retry before consuming any output.
func (e *MemoryEngine) GetManyInto(keys [][]byte, dst []byte, spans []MemorySpan) (needed int, err error) {
	if e == nil {
		return 0, errMemoryEngineClosed
	}
	if len(spans) < len(keys) {
		return 0, errors.New("memory MGET requires one output span per key")
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.handle == 0 {
		return 0, errMemoryEngineClosed
	}
	nativeKeys := make([]nativeMemorySlice, len(keys))
	for i, key := range keys {
		if memoryBytesOverlap(key, dst) {
			return 0, errors.New("memory MGET key overlaps the output buffer")
		}
		nativeKeys[i] = nativeMemorySlice{Data: memoryBytesPointer(key), Len: uintptr(len(key))}
	}
	var nativeNeeded uintptr
	result, _, _ := e.api.mgetInto.Call(
		e.handle, uintptr(unsafe.Pointer(unsafe.SliceData(nativeKeys))), uintptr(len(keys)),
		uintptr(memoryBytesPointer(dst)), uintptr(len(dst)),
		uintptr(unsafe.Pointer(unsafe.SliceData(spans))),
		uintptr(unsafe.Pointer(&nativeNeeded)),
	)
	runtime.KeepAlive(keys)
	runtime.KeepAlive(nativeKeys)
	runtime.KeepAlive(dst)
	runtime.KeepAlive(spans)
	status := int32(result)
	if status != 0 && status != 2 {
		return 0, memoryStatusError("mget", status)
	}
	needed, err = memorySizeToInt(nativeNeeded)
	if err != nil {
		return 0, err
	}
	if status == 2 {
		return needed, ErrMemoryBufferTooSmall
	}
	if needed > len(dst) {
		return needed, errors.New("mget memory engine returned an invalid output length")
	}
	return needed, nil
}

func (e *MemoryEngine) Put(key, value []byte) error {
	if e == nil {
		return errMemoryEngineClosed
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.handle == 0 {
		return errMemoryEngineClosed
	}
	result, _, _ := e.api.put.Call(
		e.handle, uintptr(memoryBytesPointer(key)), uintptr(len(key)),
		uintptr(memoryBytesPointer(value)), uintptr(len(value)),
	)
	runtime.KeepAlive(key)
	runtime.KeepAlive(value)
	return memoryStatusError("put", int32(result))
}

func (e *MemoryEngine) Delete(key []byte) (bool, error) {
	if e == nil {
		return false, errMemoryEngineClosed
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.handle == 0 {
		return false, errMemoryEngineClosed
	}
	result, _, _ := e.api.delete.Call(e.handle, uintptr(memoryBytesPointer(key)), uintptr(len(key)))
	runtime.KeepAlive(key)
	status := int32(result)
	if status == 1 {
		return false, nil
	}
	return status == 0, memoryStatusError("delete", status)
}

func (e *MemoryEngine) MSet(pairs []MemoryPair) error {
	if e == nil {
		return errMemoryEngineClosed
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.handle == 0 {
		return errMemoryEngineClosed
	}
	nativePairs := make([]nativeMemoryPair, len(pairs))
	for i, pair := range pairs {
		nativePairs[i] = nativeMemoryPair{
			Key:   nativeMemorySlice{Data: memoryBytesPointer(pair.Key), Len: uintptr(len(pair.Key))},
			Value: nativeMemorySlice{Data: memoryBytesPointer(pair.Value), Len: uintptr(len(pair.Value))},
		}
	}
	result, _, _ := e.api.mset.Call(
		e.handle, uintptr(unsafe.Pointer(unsafe.SliceData(nativePairs))), uintptr(len(pairs)),
	)
	runtime.KeepAlive(pairs)
	runtime.KeepAlive(nativePairs)
	return memoryStatusError("mset", int32(result))
}

func (e *MemoryEngine) Stats() (MemoryStats, error) {
	if e == nil {
		return MemoryStats{}, errMemoryEngineClosed
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.handle == 0 {
		return MemoryStats{}, errMemoryEngineClosed
	}
	var stats MemoryStats
	result, _, _ := e.api.stats.Call(e.handle, uintptr(unsafe.Pointer(&stats)))
	if err := memoryStatusError("stats", int32(result)); err != nil {
		return MemoryStats{}, err
	}
	return stats, nil
}

// Close is idempotent. The handle is retired even if native close reports an
// error, because retrying a native close could access an already released handle.
func (e *MemoryEngine) Close() error {
	if e == nil {
		return nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.handle == 0 {
		return nil
	}
	handle := e.handle
	e.handle = 0
	result, _, _ := e.api.close.Call(handle)
	return memoryStatusError("close", int32(result))
}

func ensureMemoryAPI() (*memoryDLLAPI, error) {
	if err := ensureDLL(); err != nil {
		return nil, err
	}
	memoryAPIMu.Lock()
	defer memoryAPIMu.Unlock()
	if memoryAPI != nil {
		return memoryAPI, nil
	}
	find := func(suffix string) (*nativeProc, error) {
		name := "kerniva_memory_" + suffix
		address, err := findSymbol(api.dll, name)
		if err != nil {
			return nil, fmt.Errorf("Rust memory ABI is unavailable: find %s: %w", name, err)
		}
		return &nativeProc{address: address}, nil
	}
	version, err := find("abi_version")
	if err != nil {
		return nil, err
	}
	result, _, _ := version.Call()
	if uint32(result) != 1 {
		return nil, fmt.Errorf("unsupported Rust memory ABI version %d (expected 1)", uint32(result))
	}
	loaded := &memoryDLLAPI{}
	for _, symbol := range []struct {
		name   string
		target **nativeProc
	}{
		{"open", &loaded.open}, {"close", &loaded.close},
		{"get_into", &loaded.getInto}, {"put", &loaded.put},
		{"delete", &loaded.delete}, {"mset", &loaded.mset},
		{"mget_into", &loaded.mgetInto}, {"stats", &loaded.stats},
	} {
		proc, err := find(symbol.name)
		if err != nil {
			return nil, err
		}
		*symbol.target = proc
	}
	memoryAPI = loaded
	return loaded, nil
}

func memoryBytesPointer(value []byte) unsafe.Pointer {
	return unsafe.Pointer(unsafe.SliceData(value))
}

func memoryBytesOverlap(first, second []byte) bool {
	if len(first) == 0 || len(second) == 0 {
		return false
	}
	a := uintptr(memoryBytesPointer(first))
	b := uintptr(memoryBytesPointer(second))
	if a < b {
		return b-a < uintptr(len(first))
	}
	return a-b < uintptr(len(second))
}

func memorySizeToInt(size uintptr) (int, error) {
	if size > uintptr(^uint(0)>>1) {
		return 0, errors.New("memory output length exceeds the Go slice size")
	}
	return int(size), nil
}

func memoryStatusError(operation string, status int32) error {
	switch status {
	case 0:
		return nil
	case 2:
		return fmt.Errorf("%s memory engine: %w", operation, ErrMemoryBufferTooSmall)
	case 3:
		return fmt.Errorf("%s memory engine: %w", operation, ErrMemoryPayloadLimit)
	case -1:
		return fmt.Errorf("%s memory engine: invalid argument or handle", operation)
	case -2:
		return fmt.Errorf("%s memory engine: native internal error", operation)
	default:
		return fmt.Errorf("%s memory engine: unexpected native status %d", operation, status)
	}
}
