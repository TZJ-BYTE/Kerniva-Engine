package golog

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/TZJ-BYTE/Kerniva/internal/rustengine"
)

type commitFile struct {
	activeFile
	syncBefore    func(int) error
	partialWrite  func(int) bool
	writes, syncs int
}

var errInjectedCommit = errors.New("injected commit I/O failure")

func (file *commitFile) WriteAt(p []byte, offset int64) (int, error) {
	file.writes++
	if file.partialWrite != nil && file.partialWrite(file.writes) {
		written, err := file.activeFile.WriteAt(p[:len(p)/2], offset)
		return written, errors.Join(err, errInjectedCommit)
	}
	return file.activeFile.WriteAt(p, offset)
}

func (file *commitFile) Sync() error {
	file.syncs++
	if err := file.syncBefore(file.syncs); err != nil {
		return err
	}
	return file.activeFile.Sync()
}

// Stop the worker inside the first request's sync so the following requests
// are all in the FIFO before the worker tries to gather them.
func holdFirstSync(t *testing.T, w *Writer, failSync func(int) error, partialWrite func(int) bool) (<-chan struct{}, func()) {
	t.Helper()
	entered := make(chan struct{})
	resume := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(resume) }) }
	t.Cleanup(release)
	w.mu.Lock()
	w.active = &commitFile{activeFile: w.active, partialWrite: partialWrite, syncBefore: func(n int) error {
		if n == 1 {
			close(entered)
			<-resume
		}
		if failSync != nil {
			return failSync(n)
		}
		return nil
	}}
	w.mu.Unlock()
	return entered, release
}

func putAsync(w *Writer, value string) <-chan error {
	result := make(chan error, 1)
	go func() { result <- w.Put([]byte("k"), []byte(value)) }()
	return result
}

func awaitPut(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Go-log write completion")
		return nil
	}
}

func awaitSync(t *testing.T, entered <-chan struct{}) {
	t.Helper()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first write did not enter sync")
	}
}

func waitWriteQueue(t *testing.T, w *Writer, count int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for len(w.writes.requests) != count {
		if time.Now().After(deadline) {
			t.Fatalf("expected %d queued writes, got %d", count, len(w.writes.requests))
		}
		time.Sleep(time.Millisecond)
	}
}

func testSegment(t *testing.T, sequence uint64, values ...string) []byte {
	t.Helper()
	data := []byte(logMagic)
	for index, value := range values {
		frame, err := encodeFrame(nil, sequence+uint64(index), []rustengine.WriteOp{{Key: []byte("k"), Value: []byte(value)}}, false)
		if err != nil {
			t.Fatal(err)
		}
		data = append(data, frame...)
	}
	return data
}

func TestCommitQueueSharesSyncWithinSegment(t *testing.T) {
	directory := t.TempDir()
	firstSegment := testSegment(t, 1, "0001", "0002")
	lastSegment := testSegment(t, 3, "0003", "0004")
	w := openTestWriter(t, directory, Options{SegmentBytes: uint64(len(firstSegment)), Sync: true, GroupMax: 4})
	entered, resume := holdFirstSync(t, w, nil, nil)
	results := []<-chan error{putAsync(w, "0001")}
	awaitSync(t, entered)
	for _, value := range []string{"0002", "0003", "0004"} {
		results = append(results, putAsync(w, value))
		waitWriteQueue(t, w, len(results)-1)
	}
	closed := make(chan error, 1)
	go func() { closed <- w.Close() }()
	resume()
	for _, result := range results {
		if err := awaitPut(t, result); err != nil {
			t.Fatal(err)
		}
	}
	if err := awaitPut(t, closed); err != nil {
		t.Fatal(err)
	}
	stats := w.Stats()
	if stats.LastSequence != 4 || stats.SyncCount != 3 || stats.GroupBatches != 1 || stats.GroupRequests != 2 || stats.SealedSegments != 2 {
		t.Fatalf("unexpected stats after grouped rotation: %+v", stats)
	}
	for _, segment := range []struct {
		first, last uint64
		expected    []byte
	}{{1, 2, firstSegment}, {3, 4, lastSegment}} {
		path := filepath.Join(directory, fmt.Sprintf("log-%020d-%020d.seg", segment.first, segment.last))
		if got := requireFile(t, path); !bytes.Equal(got, segment.expected) {
			t.Fatalf("segment %s has unexpected bytes: %x", path, got)
		}
	}
	if err := w.Put([]byte("k"), []byte("late")); !errors.Is(err, errWriterClosed) {
		t.Fatalf("write after close = %v", err)
	}
	t.Run("RustRecovery", func(t *testing.T) {
		if os.Getenv("KERNIVA_ENGINE_LIBRARY") == "" {
			t.Skip("Run the project test script to provide the native Rust library")
		}
		var values []string
		reopened, err := Open(directory, Options{Sync: true, GroupMax: 4}, func(key, value []byte) error {
			values = append(values, string(key)+"="+string(value))
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = reopened.Close() })
		if len(values) != 1 || values[0] != "k=0004" || reopened.Stats().LastSequence != 4 {
			t.Fatalf("unexpected recovered values or sequence: %v, %+v", values, reopened.Stats())
		}
	})
}

func TestCommitQueueRollsBackFailedGroup(t *testing.T) {
	for _, tc := range []struct {
		name      string
		short     bool
		failClose bool
	}{
		{name: "partial append", short: true},
		{name: "failed sync"},
		{name: "failed rollback", failClose: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			w := openTestWriter(t, dir, Options{Sync: true, GroupMax: 4})
			entered, resume := holdFirstSync(t, w, func(syncs int) error {
				if !tc.short && syncs == 2 || tc.failClose && syncs == 3 {
					return errInjectedCommit
				}
				return nil
			}, func(writes int) bool { return tc.short && writes == 2 })
			first := putAsync(w, "first")
			awaitSync(t, entered)
			second := putAsync(w, "second")
			waitWriteQueue(t, w, 1)
			third := putAsync(w, "third")
			waitWriteQueue(t, w, 2)
			resume()
			if err := awaitPut(t, first); err != nil {
				t.Fatal(err)
			}
			for _, result := range []<-chan error{second, third} {
				if err := awaitPut(t, result); !errors.Is(err, errInjectedCommit) {
					t.Fatalf("failed group write result = %v", err)
				}
			}
			stats := w.Stats()
			if stats.LastSequence != 1 || stats.SyncCount != 1 || stats.GroupBatches != 0 || stats.GroupRequests != 0 || (stats.Fault != "") != tc.failClose {
				t.Fatalf("failed group advanced committed stats: %+v", stats)
			}
			if got := requireFile(t, filepath.Join(dir, activeName)); !bytes.Equal(got, testSegment(t, 1, "first")) {
				t.Fatalf("failed group changed the committed file prefix: %x", got)
			}
			err := w.Put([]byte("k"), []byte("retry"))
			if tc.failClose {
				if !errors.Is(err, errInjectedCommit) || err.Error() != stats.Fault {
					t.Fatalf("poisoned writer accepted or changed a failed commit: %v", err)
				}
			} else if err != nil || w.Stats().LastSequence != 2 {
				t.Fatalf("writer could not commit after successful rollback: %v, %+v", err, w.Stats())
			}
		})
	}
}
