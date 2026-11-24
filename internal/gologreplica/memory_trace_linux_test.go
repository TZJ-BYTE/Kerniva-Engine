//go:build linux

package gologreplica

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestMemoryTraceSnapshotDeltaAndStartup(t *testing.T) {
	maintainer := requireNativeTools(t)
	path := filepath.Join(t.TempDir(), "memory.jsonl")
	t.Setenv(memoryTraceEnv, path)
	primary := t.TempDir()
	advanceWorkerSource(t, primary, 0, "snapshot value")
	advanceWorkerSource(t, primary, 1, "second snapshot value")
	if output, err := exec.CommandContext(t.Context(), maintainer, "--data-dir", primary, "--once").CombinedOutput(); err != nil {
		t.Fatalf("compact source: %v\n%s", err, output)
	}
	handler, err := NewSource(primary)
	check(t, err)
	upstream := httptest.NewServer(handler)
	defer upstream.Close()
	root := t.TempDir()
	follower, err := OpenFollower(root, upstream.URL, []int{0, 1})
	check(t, err)
	t.Cleanup(func() { follower.Close() })
	poll(t, follower)
	advanceWorkerSource(t, primary, 0, "delta value")
	poll(t, follower)
	check(t, unix.Kill(os.Getpid(), unix.SIGUSR1))
	for deadline := time.Now().Add(3 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		follower.trace.mu.Lock()
		data, err := os.ReadFile(path)
		follower.trace.mu.Unlock()
		check(t, err)
		if bytes.Contains(data, []byte(`"stage":"sample"`)) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no memory trace sample from SIGUSR1")
		}
	}
	check(t, follower.Close())
	upstream.Close()
	reopened, err := OpenFollower(root, upstream.URL, []int{0, 1})
	check(t, err)
	t.Cleanup(func() { reopened.Close() })
	expectGet(t, reopened, 0, "key", http.StatusOK, "delta value")
	check(t, reopened.Close())

	file, err := os.Open(path)
	check(t, err)
	defer file.Close()
	decoder := json.NewDecoder(file)
	type group struct {
		db   int
		kind string
	}
	stages := make(map[group][]string)
	var previousTime int64
	for {
		var event memoryTraceEvent
		err := decoder.Decode(&event)
		if errors.Is(err, io.EOF) {
			break
		}
		check(t, err)
		if event.Time.IsZero() || event.MonotonicNS <= 0 || event.MonotonicNS < previousTime || event.HeapAllocBytes == 0 || event.TotalAllocBytes < event.HeapAllocBytes {
			t.Fatalf("invalid memory sample: %+v", event)
		}
		previousTime = event.MonotonicNS
		if event.DB >= 0 && (!validGeneration(event.Generation) || event.Sequence == 0) {
			t.Fatalf("invalid generation metadata: %+v", event)
		}
		if event.Stage == "rust_handle_ready" {
			mode := "full"
			if event.Kind == "delta" {
				mode = "delta"
			}
			if event.Recovery != mode {
				t.Fatalf("invalid recovery kind: %+v", event)
			}
		}
		key := group{event.DB, event.Kind}
		stages[key] = append(stages[key], event.Stage)
	}
	install := []string{"install_start", "download_complete", "rust_recovery_start", "rust_handle_ready", "rust_rows_restored", "go_view_ready", "current_published", "read_view_switched", "old_generations_cleaned"}
	startup := []string{"rust_recovery_start", "rust_handle_ready", "rust_rows_restored", "go_view_ready", "old_generations_cleaned", "read_view_switched"}
	for key, want := range map[group][]string{
		{0, "snapshot"}: install, {1, "snapshot"}: install, {0, "delta"}: install,
		{0, "startup"}: startup, {1, "startup"}: startup, {-1, "process"}: {"sample"},
	} {
		if got := stages[key]; !slices.Equal(got, want) {
			t.Errorf("stages for %+v = %v, want %v", key, got, want)
		}
	}
}
