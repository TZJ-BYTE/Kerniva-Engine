package rustengine_test

import (
	"bytes"
	"errors"
	"os"
	"testing"

	"github.com/TZJ-BYTE/Kerniva/internal/golog"
	"github.com/TZJ-BYTE/Kerniva/internal/rustengine"
)

func TestRecoverGoLogBorrowedCallbackLifetime(t *testing.T) {
	if os.Getenv("KERNIVA_ENGINE_LIBRARY") == "" {
		t.Skip("set KERNIVA_ENGINE_LIBRARY to the built recovery library")
	}
	dir := t.TempDir()
	writer, err := golog.Open(dir, golog.Options{Sync: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { writer.Close() })
	key, value := []byte{0, 255}, bytes.Repeat([]byte{0, 255, 127}, 4096)
	if err := writer.Put(key, value); err != nil {
		t.Fatal(err)
	}
	if err := writer.Put([]byte("empty"), nil); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	stop := errors.New("stop recovery")
	calls := 0
	_, err = rustengine.RecoverGoLogBorrowed(dir, func(_, _ []byte) error {
		calls++
		return stop
	}, nil)
	if !errors.Is(err, stop) || calls != 1 {
		t.Fatalf("callback error = %v after %d calls", err, calls)
	}
	func() {
		defer func() {
			if got := recover(); got != stop {
				t.Fatalf("callback panic = %v", got)
			}
		}()
		rustengine.RecoverGoLogBorrowed(dir, func(_, _ []byte) error { panic(stop) }, nil)
	}()

	ready := false
	borrowed := make(map[string][]byte)
	result, err := rustengine.RecoverGoLogBorrowed(dir, func(k, v []byte) error {
		if !ready || len(k) != cap(k) || len(v) != cap(v) {
			t.Error("borrowed row came before handle readiness or extended beyond its buffers")
		}
		borrowed[string(k)] = bytes.Clone(v)
		return nil
	}, func() { ready = true })
	if err != nil || result.LastSequence != 2 || result.ActiveEnd != 0 || len(borrowed) != 2 ||
		!bytes.Equal(borrowed[string(key)], value) || borrowed["empty"] != nil {
		t.Fatalf("borrowed recovery returned %+v, %d rows, error %v", result, len(borrowed), err)
	}

	var ownedKey, ownedValue []byte
	_, err = rustengine.RecoverGoLog(dir, func(k, v []byte) error {
		if bytes.Equal(k, key) {
			ownedKey, ownedValue = k, v
		}
		return nil
	})
	if err != nil || !bytes.Equal(ownedKey, key) || !bytes.Equal(ownedValue, value) {
		t.Fatalf("owned recovery did not retain the row: %v", err)
	}
	ownedValue[0] = 42
	if borrowed[string(key)][0] != 0 {
		t.Fatal("owned row and earlier result share mutable storage")
	}
}
