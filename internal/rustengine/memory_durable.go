package rustengine

import (
	"errors"
	"runtime"
	"unsafe"
)

type MemoryWalStats struct {
	LogBytes       uint64
	Commits        uint64
	FailedCommits  uint64
	SyncAttempts   uint64
	SyncElapsedNS  uint64
	RequiresReopen uint64
}

// OpenDurableMemory opens a Rust store that syncs writes before acknowledging
// them. The given directory belongs exclusively to this store while it is open.
func OpenDurableMemory(directory string, shards int, maxPayloadBytes *uint64, readDuringSync bool) (*MemoryEngine, error) {
	limited, limit, err := nativeMemoryOptions(shards, maxPayloadBytes)
	if err != nil {
		return nil, err
	}
	native, err := ensureMemoryAPI()
	if err != nil {
		return nil, err
	}
	open, err := findMemorySymbol("open_sync")
	if err != nil {
		return nil, err
	}
	walStats, err := findMemorySymbol("wal_stats")
	if err != nil {
		return nil, err
	}

	path := []byte(directory)
	var mode, handle uintptr
	if readDuringSync {
		mode = 1
	}
	result, _, _ := open.Call(
		uintptr(memoryBytesPointer(path)), uintptr(len(path)), uintptr(shards),
		limited, limit, mode, uintptr(unsafe.Pointer(&handle)),
	)
	runtime.KeepAlive(path)
	if err := memoryStatusError("open sync", int32(result)); err != nil {
		return nil, err
	}
	if handle == 0 {
		return nil, errors.New("open sync memory engine returned an invalid handle")
	}
	return &MemoryEngine{handle: handle, api: native, walStats: walStats}, nil
}

func (e *MemoryEngine) WalStats() (MemoryWalStats, error) {
	var stats MemoryWalStats
	if e == nil {
		return stats, errMemoryEngineClosed
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.handle == 0 {
		return stats, errMemoryEngineClosed
	}
	if e.walStats == nil {
		return stats, ErrMemoryUnsupported
	}
	result, _, _ := e.walStats.Call(e.handle, uintptr(unsafe.Pointer(&stats)))
	return stats, memoryStatusError("WAL stats", int32(result))
}
