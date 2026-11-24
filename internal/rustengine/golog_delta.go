package rustengine

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"runtime"
	"strings"
	"sync"
	"unsafe"
)

// GoLogRange identifies an inclusive range in a sealed Go-log segment.
type GoLogRange struct {
	First uint64
	Last  uint64
}

type goLogDeltaAPI struct {
	open  *nativeProc
	next  *nativeProc
	close *nativeProc
}

// ErrGoLogDeltaUnavailable indicates that the loaded library predates delta recovery.
var ErrGoLogDeltaUnavailable = errors.New("Rust Go-log delta recovery ABI is unavailable")

var (
	goLogDeltaAPIMu sync.Mutex
	goLogDeltaProcs *goLogDeltaAPI
)

// RecoverGoLogDelta reads the supplied sealed ranges after a sequence number.
// The callback receives independent Go-owned copies that it may retain or
// modify. A nil callback validates the stream without copying individual rows.
// Callers must stage changes until recovery succeeds, then clear existing state
// if reset is true before applying the recovered changes.
func RecoverGoLogDelta(directory string, after uint64, ranges []GoLogRange, restore func(key, value []byte, deleted bool) error) (reset bool, err error) {
	return RecoverGoLogDeltaWithHandleObserver(directory, after, ranges, restore, nil)
}

// RecoverGoLogDeltaWithHandleObserver calls handleReady after opening the native
// iterator and before reading its first block. Native decoding can continue
// during iteration.
func RecoverGoLogDeltaWithHandleObserver(directory string, after uint64, ranges []GoLogRange, restore func(key, value []byte, deleted bool) error, handleReady func()) (reset bool, err error) {
	if directory == "" || strings.ContainsRune(directory, 0) {
		return false, errors.New("invalid Go-log delta recovery directory")
	}
	if unsafe.Sizeof(uintptr(0)) < 8 {
		return false, errors.New("Go-log delta recovery requires a 64-bit process")
	}
	procs, err := ensureGoLogDeltaAPI()
	if err != nil {
		return false, err
	}

	path := append([]byte(directory), 0)
	var firstRange *GoLogRange
	if len(ranges) > 0 {
		firstRange = &ranges[0]
	}
	var nativeReset uint8
	var nativeErr kernivaBuf
	handle, _, _ := procs.open.Call(
		uintptr(unsafe.Pointer(&path[0])),
		uintptr(after),
		uintptr(unsafe.Pointer(firstRange)),
		uintptr(len(ranges)),
		uintptr(unsafe.Pointer(&nativeReset)),
		uintptr(unsafe.Pointer(&nativeErr)),
	)
	runtime.KeepAlive(path)
	runtime.KeepAlive(ranges)
	if handle == 0 {
		return false, consumeErr("open Rust Go-log delta recovery", nativeErr)
	}
	defer procs.close.Call(handle)
	if nativeErr.Ptr != nil || nativeErr.Len != 0 {
		return false, consumeErr("open Rust Go-log delta recovery", nativeErr)
	}
	if nativeReset > 1 {
		return false, errors.New("invalid Go-log delta reset flag")
	}
	if handleReady != nil {
		handleReady()
	}

	for {
		var block, nativeErr kernivaBuf
		status, _, _ := procs.next.Call(
			handle,
			uintptr(unsafe.Pointer(&block)),
			uintptr(unsafe.Pointer(&nativeErr)),
		)
		if int32(status) == -1 || nativeErr.Ptr != nil || nativeErr.Len != 0 {
			freeBuf(block)
			return false, consumeErr("iterate Rust Go-log delta recovery", nativeErr)
		}
		switch int32(status) {
		case 1:
			if err := restoreNativeGoLogDeltaBlock(block, restore); err != nil {
				return false, err
			}
		case 0:
			if block.Ptr != nil || block.Len != 0 {
				freeBuf(block)
				return false, errors.New("unexpected Go-log delta block at EOF")
			}
			return nativeReset == 1, nil
		default:
			freeBuf(block)
			return false, fmt.Errorf("unexpected Rust Go-log delta status %d", int32(status))
		}
	}
}

func restoreNativeGoLogDeltaBlock(block kernivaBuf, restore func(key, value []byte, deleted bool) error) error {
	defer freeBuf(block)
	if block.Ptr == nil || block.Len > math.MaxInt {
		return errors.New("invalid Go-log delta buffer")
	}
	return restoreGoLogDeltaBlock(unsafe.Slice((*byte)(block.Ptr), int(block.Len)), restore)
}

func restoreGoLogDeltaBlock(block []byte, restore func(key, value []byte, deleted bool) error) error {
	const headerSize = 9
	if len(block) == 0 {
		return errors.New("empty Go-log delta block")
	}
	for len(block) > 0 {
		if len(block) < headerSize {
			return errors.New("truncated Go-log delta record header")
		}
		kind := block[0]
		keySize := uint64(binary.LittleEndian.Uint32(block[1:5]))
		valueSize := uint64(binary.LittleEndian.Uint32(block[5:9]))
		block = block[headerSize:]
		if keySize+valueSize > uint64(len(block)) {
			return errors.New("truncated Go-log delta record body")
		}
		if kind != 1 && kind != 2 {
			return fmt.Errorf("invalid Go-log delta record kind %d", kind)
		}
		deleted := kind == 2
		if deleted && valueSize != 0 {
			return errors.New("Go-log delta delete record has a value")
		}
		keyEnd := int(keySize)
		valueEnd := int(keySize + valueSize)
		if restore != nil {
			key := bytes.Clone(block[:keyEnd])
			var value []byte
			if !deleted {
				value = bytes.Clone(block[keyEnd:valueEnd])
			}
			if err := restore(key, value, deleted); err != nil {
				return fmt.Errorf("restore Go-log delta entry: %w", err)
			}
		}
		block = block[valueEnd:]
	}
	return nil
}

func ensureGoLogDeltaAPI() (*goLogDeltaAPI, error) {
	if err := ensureDLL(); err != nil {
		return nil, err
	}
	goLogDeltaAPIMu.Lock()
	defer goLogDeltaAPIMu.Unlock()
	if goLogDeltaProcs != nil {
		return goLogDeltaProcs, nil
	}

	loaded := &goLogDeltaAPI{}
	for _, symbol := range []struct {
		name   string
		target **nativeProc
	}{
		{"kerniva_golog_delta_open", &loaded.open},
		{"kerniva_golog_delta_next", &loaded.next},
		{"kerniva_golog_delta_close", &loaded.close},
	} {
		address, err := findSymbol(api.dll, symbol.name)
		if err != nil {
			return nil, fmt.Errorf("%w: find %s: %w", ErrGoLogDeltaUnavailable, symbol.name, err)
		}
		*symbol.target = &nativeProc{address: address}
	}
	goLogDeltaProcs = loaded
	return loaded, nil
}
