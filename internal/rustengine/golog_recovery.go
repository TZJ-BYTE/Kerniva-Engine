package rustengine

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"unsafe"
)

// GoLogRecovery identifies the last complete commit and the valid active-file
// prefix. A zero length means the active header is absent or incomplete.
type GoLogRecovery struct {
	LastSequence uint64
	ActiveEnd    uint64
}

type goLogRecoveryAPI struct {
	open  *nativeProc
	next  *nativeProc
	close *nativeProc
}

var (
	goLogRecoveryAPIMu sync.Mutex
	goLogRecoveryProcs *goLogRecoveryAPI
)

// RecoverGoLog replays the published snapshot and log before the caller begins
// serving requests. The callback receives Go-owned copies and may retain them.
func RecoverGoLog(directory string, restore func(key, value []byte) error) (GoLogRecovery, error) {
	return recoverGoLog(directory, restore, nil, restoreOwnedRecoveryRow)
}

// RecoverGoLogBorrowed replays through a synchronous callback. The key and value
// are read-only and valid only until the callback returns. The callback must copy
// any bytes it needs to retain. handleReady runs before the first row is read.
func RecoverGoLogBorrowed(directory string, restore func(key, value []byte) error, handleReady func()) (GoLogRecovery, error) {
	return recoverGoLog(directory, restore, handleReady, restoreBorrowedRecoveryRow)
}

func recoverGoLog(directory string, restore func(key, value []byte) error, handleReady func(), restoreRow func(key, value kernivaBuf, restore func([]byte, []byte) error) error) (GoLogRecovery, error) {
	if directory == "" || strings.ContainsRune(directory, 0) {
		return GoLogRecovery{}, errors.New("invalid Go log recovery directory")
	}
	procs, err := ensureGoLogRecoveryAPI()
	if err != nil {
		return GoLogRecovery{}, err
	}

	path := append([]byte(directory), 0)
	var result GoLogRecovery
	var nativeErr kernivaBuf
	handle, _, _ := procs.open.Call(
		uintptr(unsafe.Pointer(&path[0])),
		uintptr(unsafe.Pointer(&result.LastSequence)),
		uintptr(unsafe.Pointer(&result.ActiveEnd)),
		uintptr(unsafe.Pointer(&nativeErr)),
	)
	if handle == 0 {
		return GoLogRecovery{}, consumeErr("open Rust Go-log recovery", nativeErr)
	}
	defer procs.close.Call(handle)
	if handleReady != nil {
		handleReady()
	}
	if restore == nil {
		return result, nil
	}

	for {
		var key, value, nativeErr kernivaBuf
		status, _, _ := procs.next.Call(
			handle,
			uintptr(unsafe.Pointer(&key)),
			uintptr(unsafe.Pointer(&value)),
			uintptr(unsafe.Pointer(&nativeErr)),
		)
		switch int32(status) {
		case 1:
			if err := restoreRow(key, value, restore); err != nil {
				return GoLogRecovery{}, err
			}
		case 0:
			return result, nil
		default:
			freeBuf(key)
			freeBuf(value)
			return GoLogRecovery{}, consumeErr("iterate Rust Go-log recovery", nativeErr)
		}
	}
}

func restoreOwnedRecoveryRow(key, value kernivaBuf, restore func([]byte, []byte) error) error {
	keyCopy, valueCopy, err := copyRecoveryRow(key, value)
	if err != nil {
		return err
	}
	if err := restore(keyCopy, valueCopy); err != nil {
		return fmt.Errorf("restore Go-log entry: %w", err)
	}
	return nil
}

func restoreBorrowedRecoveryRow(key, value kernivaBuf, restore func([]byte, []byte) error) error {
	defer freeBuf(key)
	defer freeBuf(value)
	keyBytes, err := recoveryBufferBytes(key)
	if err != nil {
		return err
	}
	valueBytes, err := recoveryBufferBytes(value)
	if err != nil {
		return err
	}
	if err := restore(keyBytes, valueBytes); err != nil {
		return fmt.Errorf("restore Go-log entry: %w", err)
	}
	return nil
}

func copyRecoveryRow(key, value kernivaBuf) ([]byte, []byte, error) {
	defer freeBuf(key)
	defer freeBuf(value)
	copies := [2][]byte{}
	for i, row := range [...]kernivaBuf{key, value} {
		data, err := recoveryBufferBytes(row)
		if err != nil {
			return nil, nil, err
		}
		copies[i] = append([]byte(nil), data...)
	}
	return copies[0], copies[1], nil
}

func recoveryBufferBytes(buffer kernivaBuf) ([]byte, error) {
	if buffer.Len == 0 {
		return nil, nil
	}
	if buffer.Ptr == nil || buffer.Len > math.MaxInt {
		return nil, errors.New("invalid Go-log recovery buffer")
	}
	return unsafe.Slice((*byte)(buffer.Ptr), int(buffer.Len)), nil
}

func ensureGoLogRecoveryAPI() (*goLogRecoveryAPI, error) {
	if err := ensureDLL(); err != nil {
		return nil, err
	}
	goLogRecoveryAPIMu.Lock()
	defer goLogRecoveryAPIMu.Unlock()
	if goLogRecoveryProcs != nil {
		return goLogRecoveryProcs, nil
	}

	loaded := &goLogRecoveryAPI{}
	for _, symbol := range []struct {
		name   string
		target **nativeProc
	}{
		{"kerniva_golog_recover", &loaded.open},
		{"kerniva_golog_recovery_next", &loaded.next},
		{"kerniva_golog_recovery_close", &loaded.close},
	} {
		address, err := findSymbol(api.dll, symbol.name)
		if err != nil {
			return nil, fmt.Errorf("Go-log recovery ABI is unavailable: find %s: %w", symbol.name, err)
		}
		*symbol.target = &nativeProc{address: address}
	}
	goLogRecoveryProcs = loaded
	return loaded, nil
}
