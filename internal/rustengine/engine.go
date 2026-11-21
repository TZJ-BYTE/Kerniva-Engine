package rustengine

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"unsafe"

	"github.com/ebitengine/purego"
)

type SyncPolicy uint32

const (
	SyncNone SyncPolicy = iota
	SyncFlush
	SyncData
)

type Options struct {
	DataDir            string
	SegmentSizeBytes   uint64
	CheckpointAfterOps uint64
	SyncPolicy         SyncPolicy
}

type Stats struct {
	Keys            uint64
	ActiveSegmentID uint64
	SegmentCount    uint64
	CachedReaders   uint64
	PinnedSegments  uint64
	PendingReclaims uint64
}

type kernivaBuf struct {
	Ptr unsafe.Pointer
	Len uintptr
}

type kernivaStats struct {
	Keys            uint64
	ActiveSegmentID uint64
	SegmentCount    uint64
	CachedReaders   uint64
	PinnedSegments  uint64
	PendingReclaims uint64
}

type WriteOp struct {
	Key    []byte
	Value  []byte
	Delete bool
}

type nativeWriteOp struct {
	Kind     uint32
	KeyPtr   unsafe.Pointer
	KeyLen   uintptr
	ValuePtr unsafe.Pointer
	ValueLen uintptr
}

type nativeProc struct{ address uintptr }

//go:uintptrescapes
func (p *nativeProc) Call(args ...uintptr) (uintptr, uintptr, error) {
	r1, r2, _ := purego.SyscallN(p.address, args...)
	return r1, r2, nil
}

type dllAPI struct {
	dll                    uintptr
	bufFree                *nativeProc
	engineOpen             *nativeProc
	engineClose            *nativeProc
	enginePut              *nativeProc
	engineGet              *nativeProc
	engineDelete           *nativeProc
	engineClear            *nativeProc
	engineWriteBatch       *nativeProc
	engineSync             *nativeProc
	engineCheckpoint       *nativeProc
	engineCompact          *nativeProc
	engineMaybeCompact     *nativeProc
	engineMaybeCheckpoint  *nativeProc
	engineShouldCheckpoint *nativeProc
	engineStats            *nativeProc
	engineIterOpen         *nativeProc
	engineIterNext         *nativeProc
	engineIterClose        *nativeProc
}

type Engine struct {
	mu     sync.RWMutex
	handle uintptr
}

var (
	loadMu sync.Mutex
	api    *dllAPI
)

func Open(opts Options) (*Engine, error) {
	if err := ensureDLL(); err != nil {
		return nil, err
	}
	if opts.DataDir == "" {
		return nil, errors.New("rust engine data dir is required")
	}
	if opts.SegmentSizeBytes == 0 {
		opts.SegmentSizeBytes = 64 << 20
	}
	if opts.CheckpointAfterOps == 0 {
		opts.CheckpointAfterOps = 1024
	}

	pathBytes := append([]byte(opts.DataDir), 0)
	var errBuf kernivaBuf
	handle, _, _ := api.engineOpen.Call(
		uintptr(unsafe.Pointer(&pathBytes[0])),
		uintptr(opts.SegmentSizeBytes),
		uintptr(opts.CheckpointAfterOps),
		uintptr(opts.SyncPolicy),
		uintptr(unsafe.Pointer(&errBuf)),
	)
	if handle == 0 {
		return nil, consumeErr("open rust engine", errBuf)
	}

	return &Engine{
		handle: handle,
	}, nil
}

func (e *Engine) Put(key, value []byte) error {
	if e == nil {
		return errors.New("rust engine is closed")
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.handle == 0 {
		return errors.New("rust engine is closed")
	}
	defer runtime.KeepAlive(key)
	defer runtime.KeepAlive(value)
	var errBuf kernivaBuf
	rc, _, _ := api.enginePut.Call(
		e.handle,
		uintptr(unsafe.Pointer(unsafe.SliceData(key))),
		uintptr(len(key)),
		uintptr(unsafe.Pointer(unsafe.SliceData(value))),
		uintptr(len(value)),
		uintptr(unsafe.Pointer(&errBuf)),
	)
	if int32(rc) != 0 {
		return consumeErr("put rust engine", errBuf)
	}
	return nil
}

func (e *Engine) Get(key []byte) ([]byte, bool, error) {
	if e == nil {
		return nil, false, errors.New("rust engine is closed")
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.handle == 0 {
		return nil, false, errors.New("rust engine is closed")
	}
	defer runtime.KeepAlive(key)
	var valueBuf kernivaBuf
	var errBuf kernivaBuf
	rc, _, _ := api.engineGet.Call(
		e.handle,
		uintptr(unsafe.Pointer(unsafe.SliceData(key))),
		uintptr(len(key)),
		uintptr(unsafe.Pointer(&valueBuf)),
		uintptr(unsafe.Pointer(&errBuf)),
	)
	switch int32(rc) {
	case 1:
		defer freeBuf(valueBuf)
		value := append([]byte(nil), unsafe.Slice((*byte)(valueBuf.Ptr), int(valueBuf.Len))...)
		return value, true, nil
	case 0:
		return nil, false, nil
	default:
		return nil, false, consumeErr("get rust engine", errBuf)
	}
}

func (e *Engine) Delete(key []byte) error {
	if e == nil {
		return errors.New("rust engine is closed")
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.handle == 0 {
		return errors.New("rust engine is closed")
	}
	defer runtime.KeepAlive(key)
	var errBuf kernivaBuf
	rc, _, _ := api.engineDelete.Call(
		e.handle,
		uintptr(unsafe.Pointer(unsafe.SliceData(key))),
		uintptr(len(key)),
		uintptr(unsafe.Pointer(&errBuf)),
	)
	if int32(rc) != 0 {
		return consumeErr("delete rust engine", errBuf)
	}
	return nil
}

func (e *Engine) WriteBatch(operations []WriteOp) error {
	if e == nil {
		return errors.New("rust engine is closed")
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.handle == 0 {
		return errors.New("rust engine is closed")
	}
	return e.writeBatchLocked(operations)
}

func (e *Engine) writeBatchLocked(operations []WriteOp) error {
	if len(operations) == 0 {
		return nil
	}
	native := make([]nativeWriteOp, len(operations))
	for i, operation := range operations {
		kind := uint32(1)
		if operation.Delete {
			kind = 2
		}
		native[i] = nativeWriteOp{
			Kind:     kind,
			KeyPtr:   unsafe.Pointer(unsafe.SliceData(operation.Key)),
			KeyLen:   uintptr(len(operation.Key)),
			ValuePtr: unsafe.Pointer(unsafe.SliceData(operation.Value)),
			ValueLen: uintptr(len(operation.Value)),
		}
	}
	var errBuf kernivaBuf
	rc, _, _ := api.engineWriteBatch.Call(e.handle, uintptr(unsafe.Pointer(&native[0])), uintptr(len(native)), uintptr(unsafe.Pointer(&errBuf)))
	runtime.KeepAlive(operations)
	runtime.KeepAlive(native)
	if int32(rc) != 0 {
		return consumeErr("write batch rust engine", errBuf)
	}
	return nil
}

func (e *Engine) CompactUntilStable(maxRounds int) (uint64, error) {
	if e == nil {
		return 0, errors.New("rust engine is closed")
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.handle == 0 {
		return 0, errors.New("rust engine is closed")
	}
	var rewritten uint64
	var errBuf kernivaBuf
	rc, _, _ := api.engineCompact.Call(
		e.handle,
		uintptr(maxRounds),
		uintptr(unsafe.Pointer(&rewritten)),
		uintptr(unsafe.Pointer(&errBuf)),
	)
	if int32(rc) != 0 {
		return 0, consumeErr("compact rust engine", errBuf)
	}
	return rewritten, nil
}

func (e *Engine) MaybeCompact() (uint64, error) {
	if e == nil {
		return 0, errors.New("rust engine is closed")
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.handle == 0 {
		return 0, errors.New("rust engine is closed")
	}
	var rewritten uint64
	var errBuf kernivaBuf
	rc, _, _ := api.engineMaybeCompact.Call(e.handle, uintptr(unsafe.Pointer(&rewritten)), uintptr(unsafe.Pointer(&errBuf)))
	if int32(rc) != 0 {
		return 0, consumeErr("maybe compact rust engine", errBuf)
	}
	return rewritten, nil
}

func (e *Engine) MaybeCheckpoint() error {
	if e == nil {
		return errors.New("rust engine is closed")
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.handle == 0 {
		return errors.New("rust engine is closed")
	}
	var errBuf kernivaBuf
	rc, _, _ := api.engineMaybeCheckpoint.Call(
		e.handle,
		uintptr(unsafe.Pointer(&errBuf)),
	)
	if int32(rc) != 0 {
		return consumeErr("maybe_checkpoint rust engine", errBuf)
	}
	return nil
}

func (e *Engine) Sync() error {
	if e == nil {
		return errors.New("rust engine is closed")
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.handle == 0 {
		return errors.New("rust engine is closed")
	}
	var errBuf kernivaBuf
	rc, _, _ := api.engineSync.Call(e.handle, uintptr(unsafe.Pointer(&errBuf)))
	if int32(rc) != 0 {
		return consumeErr("sync rust engine", errBuf)
	}
	return nil
}

func (e *Engine) Checkpoint() error {
	if e == nil {
		return errors.New("rust engine is closed")
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.handle == 0 {
		return errors.New("rust engine is closed")
	}
	var errBuf kernivaBuf
	rc, _, _ := api.engineCheckpoint.Call(e.handle, uintptr(unsafe.Pointer(&errBuf)))
	if int32(rc) != 0 {
		return consumeErr("checkpoint rust engine", errBuf)
	}
	return nil
}

func (e *Engine) ShouldCheckpoint() bool {
	if e == nil {
		return false
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.handle == 0 {
		return false
	}
	rc, _, _ := api.engineShouldCheckpoint.Call(e.handle)
	return int32(rc) != 0
}

func (e *Engine) Stats() (Stats, error) {
	if e == nil {
		return Stats{}, errors.New("rust engine is closed")
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.handle == 0 {
		return Stats{}, errors.New("rust engine is closed")
	}
	var out kernivaStats
	var errBuf kernivaBuf
	rc, _, _ := api.engineStats.Call(
		e.handle,
		uintptr(unsafe.Pointer(&out)),
		uintptr(unsafe.Pointer(&errBuf)),
	)
	if int32(rc) != 0 {
		return Stats{}, consumeErr("stats rust engine", errBuf)
	}
	return Stats{
		Keys:            out.Keys,
		ActiveSegmentID: out.ActiveSegmentID,
		SegmentCount:    out.SegmentCount,
		CachedReaders:   out.CachedReaders,
		PinnedSegments:  out.PinnedSegments,
		PendingReclaims: out.PendingReclaims,
	}, nil
}

func (e *Engine) LoadAllKeys() (map[string][]byte, error) {
	if e == nil {
		return nil, errors.New("rust engine is closed")
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.handle == 0 {
		return nil, errors.New("rust engine is closed")
	}
	return e.loadAllKeysLocked()
}

func (e *Engine) loadAllKeysLocked() (map[string][]byte, error) {
	var errBuf kernivaBuf
	iterHandle, _, _ := api.engineIterOpen.Call(
		e.handle,
		uintptr(unsafe.Pointer(&errBuf)),
	)
	if iterHandle == 0 {
		return nil, consumeErr("open rust iterator", errBuf)
	}
	defer api.engineIterClose.Call(iterHandle)

	out := make(map[string][]byte)
	for {
		var keyBuf kernivaBuf
		var valueBuf kernivaBuf
		var nextErr kernivaBuf
		rc, _, _ := api.engineIterNext.Call(
			iterHandle,
			uintptr(unsafe.Pointer(&keyBuf)),
			uintptr(unsafe.Pointer(&valueBuf)),
			uintptr(unsafe.Pointer(&nextErr)),
		)
		switch int32(rc) {
		case 1:
			key := string(append([]byte(nil), unsafe.Slice((*byte)(keyBuf.Ptr), int(keyBuf.Len))...))
			value := append([]byte(nil), unsafe.Slice((*byte)(valueBuf.Ptr), int(valueBuf.Len))...)
			freeBuf(keyBuf)
			freeBuf(valueBuf)
			out[key] = value
		case 0:
			return out, nil
		default:
			freeBuf(keyBuf)
			freeBuf(valueBuf)
			return nil, consumeErr("iterate rust engine", nextErr)
		}
	}
}

func (e *Engine) Reset() error {
	if e == nil {
		return nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.handle == 0 {
		return errors.New("rust engine is closed")
	}
	var errBuf kernivaBuf
	rc, _, _ := api.engineClear.Call(e.handle, uintptr(unsafe.Pointer(&errBuf)))
	if int32(rc) != 0 {
		return consumeErr("clear rust engine", errBuf)
	}
	return nil
}

func (e *Engine) Close() error {
	if e == nil {
		return nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.closeLocked()
}

func (e *Engine) closeLocked() error {
	if e.handle == 0 {
		return nil
	}
	var errBuf kernivaBuf
	rc, _, _ := api.engineClose.Call(e.handle, uintptr(unsafe.Pointer(&errBuf)))
	e.handle = 0
	if int32(rc) != 0 {
		return consumeErr("close rust engine", errBuf)
	}
	return nil
}

func ensureDLL() error {
	loadMu.Lock()
	defer loadMu.Unlock()
	if api != nil {
		return nil
	}
	filename := "libkerniva_engine.so"
	switch runtime.GOOS {
	case "windows":
		filename = "kerniva_engine.dll"
	case "darwin":
		filename = "libkerniva_engine.dylib"
	}
	var candidates []string
	if explicit := os.Getenv("KERNIVA_ENGINE_LIBRARY"); explicit != "" {
		candidates = []string{explicit}
	} else {
		executable, err := os.Executable()
		if err == nil {
			candidates = append(candidates, filepath.Join(filepath.Dir(executable), filename))
		}
		current, err := os.Getwd()
		if err == nil {
			for dir := current; ; dir = filepath.Dir(dir) {
				candidates = append(candidates, filepath.Join(dir, "bin", filename), filepath.Join(dir, "kerniva_engine", "target", "release", filename))
				if filepath.Dir(dir) == dir {
					break
				}
			}
		}
	}
	for _, candidate := range candidates {
		if _, err := os.Stat(candidate); err != nil {
			continue
		}
		loaded, err := loadDLL(candidate)
		if err != nil {
			return err
		}
		api = loaded
		return nil
	}
	return fmt.Errorf("prebuilt Kerniva Engine library %s not found, run the build script or set KERNIVA_ENGINE_LIBRARY", filename)
}

func loadDLL(path string) (*dllAPI, error) {
	dll, err := openLibrary(path)
	if err != nil {
		return nil, fmt.Errorf("load rust engine dll: %w", err)
	}

	loaded := false
	defer func() {
		if !loaded {
			closeLibrary(dll)
		}
	}()
	find := func(name string) (*nativeProc, error) {
		address, procErr := findSymbol(dll, name)
		if procErr != nil {
			return nil, fmt.Errorf("find proc %s: %w", name, procErr)
		}
		return &nativeProc{address: address}, nil
	}

	bufFree, err := find("kerniva_buf_free")
	if err != nil {
		return nil, err
	}
	engineOpen, err := find("kerniva_engine_open")
	if err != nil {
		return nil, err
	}
	engineClose, err := find("kerniva_engine_close")
	if err != nil {
		return nil, err
	}
	enginePut, err := find("kerniva_engine_put")
	if err != nil {
		return nil, err
	}
	engineGet, err := find("kerniva_engine_get")
	if err != nil {
		return nil, err
	}
	engineDelete, err := find("kerniva_engine_delete")
	if err != nil {
		return nil, err
	}
	engineClear, err := find("kerniva_engine_clear")
	if err != nil {
		return nil, err
	}
	engineCompact, err := find("kerniva_engine_compact_until_stable")
	if err != nil {
		return nil, err
	}
	engineMaybeCompact, err := find("kerniva_engine_maybe_compact")
	if err != nil {
		return nil, err
	}
	engineMaybeCheckpoint, err := find("kerniva_engine_maybe_checkpoint")
	if err != nil {
		return nil, err
	}
	engineShouldCheckpoint, err := find("kerniva_engine_should_checkpoint")
	if err != nil {
		return nil, err
	}
	engineStats, err := find("kerniva_engine_stats")
	if err != nil {
		return nil, err
	}
	engineIterOpen, err := find("kerniva_engine_iter_open")
	if err != nil {
		return nil, err
	}
	engineIterNext, err := find("kerniva_engine_iter_next")
	if err != nil {
		return nil, err
	}
	engineIterClose, err := find("kerniva_engine_iter_close")
	if err != nil {
		return nil, err
	}

	engineWriteBatch, err := find("kerniva_engine_write_batch")
	if err != nil {
		return nil, err
	}
	engineSync, err := find("kerniva_engine_sync")
	if err != nil {
		return nil, err
	}
	engineCheckpoint, err := find("kerniva_engine_checkpoint")
	if err != nil {
		return nil, err
	}
	loaded = true
	return &dllAPI{
		dll:                    dll,
		bufFree:                bufFree,
		engineOpen:             engineOpen,
		engineClose:            engineClose,
		enginePut:              enginePut,
		engineGet:              engineGet,
		engineDelete:           engineDelete,
		engineClear:            engineClear,
		engineWriteBatch:       engineWriteBatch,
		engineSync:             engineSync,
		engineCheckpoint:       engineCheckpoint,
		engineCompact:          engineCompact,
		engineMaybeCompact:     engineMaybeCompact,
		engineMaybeCheckpoint:  engineMaybeCheckpoint,
		engineShouldCheckpoint: engineShouldCheckpoint,
		engineStats:            engineStats,
		engineIterOpen:         engineIterOpen,
		engineIterNext:         engineIterNext,
		engineIterClose:        engineIterClose,
	}, nil
}

func freeBuf(buf kernivaBuf) {
	if buf.Ptr == nil || buf.Len == 0 {
		return
	}
	api.bufFree.Call(uintptr(buf.Ptr), uintptr(buf.Len))
}

func consumeErr(prefix string, buf kernivaBuf) error {
	if buf.Ptr == nil || buf.Len == 0 {
		return errors.New(prefix)
	}
	defer freeBuf(buf)
	msg := string(unsafe.Slice((*byte)(buf.Ptr), int(buf.Len)))
	return fmt.Errorf("%s: %s", prefix, msg)
}
