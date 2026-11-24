//go:build linux

package gologreplica

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestRetainedSegmentsSourceSelection(t *testing.T) {
	primary := t.TempDir()
	directory := filepath.Join(primary, dbName(0))
	check(t, os.Mkdir(directory, 0700))
	check(t, os.WriteFile(filepath.Join(directory, "CURRENT"), []byte("5\n"), 0600))
	snapshot := snapshotName(5)
	check(t, os.WriteFile(filepath.Join(directory, snapshot), make([]byte, 32), 0600))
	first := fmt.Sprintf("log-%020d-%020d.seg", 2, 3)
	second := fmt.Sprintf("log-%020d-%020d.seg", 4, 5)
	tail := fmt.Sprintf("log-%020d-%020d.seg", 6, 7)
	overlap := fmt.Sprintf("log-%020d-%020d.seg", 5, 7)
	for _, name := range []string{first, second, tail} {
		check(t, os.WriteFile(filepath.Join(directory, name), make([]byte, 32), 0600))
	}
	handler, err := NewSource(primary)
	check(t, err)

	assertPull := func(query string, code int, cutoff uint64, last uint64, names ...string) {
		t.Helper()
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/pull?db=0&"+query, nil))
		if response.Code != code {
			t.Fatalf("pull %s: status %d, want %d: %s", query, response.Code, code, response.Body)
		}
		if code != http.StatusOK {
			return
		}
		info, err := readManifest(response.Body)
		check(t, err)
		var files []string
		for _, entry := range info.Files {
			files = append(files, entry.Name)
		}
		if info.Snapshot != cutoff || info.Last != last || !slices.Equal(files, names) ||
			info.Available == nil || info.Available.Sequence != 5 {
			t.Fatalf("pull %s: unexpected manifest %+v", query, info)
		}
	}
	assertPull("after=1", http.StatusOK, 0, 7, first, second, tail)
	assertPull("after=3", http.StatusOK, 0, 7, second, tail)
	assertPull("after=5", http.StatusOK, 0, 7, tail)
	assertPull("after=7", http.StatusOK, 0, 7)
	assertPull("after=2", http.StatusOK, 5, 7, snapshot, tail)
	assertPull("after=0", http.StatusOK, 5, 7, snapshot, tail)
	assertPull("after=1&snapshot=1", http.StatusOK, 5, 7, snapshot, tail)
	assertPull("after=1&rebase=1", http.StatusOK, 5, 7, snapshot, tail)
	assertPull("after=7&rebase=5", http.StatusOK, 5, 7, snapshot)
	assertPull("after=5&snapshot=1", http.StatusConflict, 0, 0)
	assertPull("after=1&snapshot=0", http.StatusBadRequest, 0, 0)
	assertPull("after=1&snapshot=1&rebase=1", http.StatusBadRequest, 0, 0)

	check(t, os.Remove(filepath.Join(directory, tail)))
	assertPull("after=1", http.StatusOK, 0, 5, first, second)
	check(t, os.WriteFile(filepath.Join(directory, overlap), make([]byte, 32), 0600))
	assertPull("after=1", http.StatusOK, 5, 7, snapshot, overlap)
	check(t, os.Rename(filepath.Join(directory, overlap), filepath.Join(directory, tail)))
	check(t, os.Remove(filepath.Join(directory, second)))
	assertPull("after=1", http.StatusOK, 5, 7, snapshot, tail)
	check(t, os.WriteFile(filepath.Join(directory, second), make([]byte, 32), 0600))
	for _, name := range []string{first, second} {
		path := filepath.Join(directory, name)
		check(t, os.Truncate(path, sealedLogHeaderBytes))
		assertPull("after=1", http.StatusOK, 5, 7, snapshot, tail)
		check(t, os.Truncate(path, 32))
	}
	check(t, os.Remove(filepath.Join(directory, second)))
	check(t, os.Mkdir(filepath.Join(directory, second), 0700))
	assertPull("after=1", http.StatusOK, 5, 7, snapshot, tail)
	check(t, os.Remove(filepath.Join(directory, second)))
	check(t, os.WriteFile(filepath.Join(directory, second), make([]byte, 32), 0600))
	check(t, os.Truncate(filepath.Join(directory, tail), sealedLogHeaderBytes))
	assertPull("after=1", http.StatusInternalServerError, 0, 0)
	assertPull("after=5", http.StatusInternalServerError, 0, 0)
	check(t, os.Truncate(filepath.Join(directory, tail), 32))
	check(t, os.Truncate(filepath.Join(directory, first), batchTargetBytes))
	// An independently enforced response budget may end an otherwise
	// complete bridge before the checkpoint. The next pull can resume it.
	s := &source{root: primary}
	s.id, err = sourceID(primary)
	check(t, err)
	capture, err := s.capture(t.Context(), 0, 1, 0, false, false)
	check(t, err)
	defer capture.close()
	if capture.info.Snapshot != 0 || capture.info.Last != 3 || len(capture.info.Files) != 1 {
		t.Fatalf("bounded bridge unexpectedly fell back to the snapshot: %+v", capture.info)
	}
	assertPull("after=3", http.StatusOK, 0, 7, second, tail)
}

func TestRetainedReplicaRecoversCorruptBridgeWithSnapshot(t *testing.T) {
	maintainer := requireNativeTools(t)
	primary := t.TempDir()
	directory := filepath.Join(primary, dbName(0))
	writer := openWriter(t, directory)
	put(t, writer, "key", "old", 0)
	check(t, writer.Close())

	handler, err := NewSource(primary)
	check(t, err)
	var mode atomic.Int32
	var forced atomic.Int32
	requested := make(chan struct{}, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if query := r.URL.Query(); query.Get("snapshot") == "1" {
			forced.Add(1)
			switch mode.Load() {
			case 1:
				select {
				case requested <- struct{}{}:
				default:
				}
				<-r.Context().Done()
				return
			case 2:
				w.WriteHeader(http.StatusOK)
				return
			}
		}
		handler.ServeHTTP(w, r)
	}))
	defer upstream.Close()

	goodRoot := t.TempDir()
	good, err := OpenFollower(goodRoot, upstream.URL, []int{0})
	check(t, err)
	defer good.Close()
	poll(t, good)
	bad, err := OpenFollowerWithOptions(t.TempDir(), upstream.URL, []int{0}, FollowerOptions{
		PullIdleTimeout: DefaultTransferIdleTimeout, SnapshotConcurrency: 1,
	})
	check(t, err)
	defer bad.Close()
	poll(t, bad)
	db := bad.byNumber[0]
	old := db.result.Load().view
	pointer := filepath.Join(db.directory, "CURRENT")
	oldCurrent, err := os.ReadFile(pointer)
	check(t, err)

	writer = openWriter(t, directory)
	put(t, writer, "key", "in snapshot", 0)
	check(t, writer.Close())
	covered := filepath.Join(directory, fmt.Sprintf("log-%020d-%020d.seg", 2, 2))
	saved, err := os.ReadFile(covered)
	check(t, err)
	cmd := exec.CommandContext(t.Context(), maintainer, "--data-dir", primary, "--once")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("publish source snapshot: %v\n%s", err, output)
	}
	// Simulate the optional retained file so this test can run with the
	// default maintainer as well as the retention-enabled one.
	check(t, os.WriteFile(covered, saved, 0600))

	writer = openWriter(t, directory)
	put(t, writer, "after", "tail", 0)
	check(t, writer.Close())
	poll(t, good)
	if got := reportStatus(t, good, 0); got.Sequence != 3 || got.Snapshot != 0 || got.Bootstraps != 0 {
		t.Fatalf("intact bridge did not install incrementally: %+v", got)
	}
	check(t, good.Close())
	reopened, err := OpenFollower(goodRoot, upstream.URL, []int{0})
	check(t, err)
	defer reopened.Close()
	expectGet(t, reopened, 0, "key", http.StatusOK, "in snapshot")
	expectGet(t, reopened, 0, "after", http.StatusOK, "tail")

	assertPrevious := func() {
		t.Helper()
		current, err := os.ReadFile(pointer)
		check(t, err)
		if status := db.result.Load(); status.view != old || status.status.Sequence != 1 ||
			!bytes.Equal(current, oldCurrent) {
			t.Fatalf("failed staged transfer changed the installed view: %+v", status.status)
		}
		expectGet(t, bad, 0, "key", http.StatusOK, "old")
		expectGet(t, bad, 0, "after", http.StatusNotFound, "")
	}

	// A failure linking the last installed generation is storage work, not
	// incoming record validation, even when the export contains a bridge.
	prior := filepath.Join(db.directory, old.Generation, fmt.Sprintf("log-%020d-%020d.seg", 1, 1))
	missing := filepath.Join(t.TempDir(), "previous.seg")
	check(t, os.Rename(prior, missing))
	err = bad.Poll(t.Context())
	check(t, os.Rename(missing, prior))
	if err == nil || forced.Load() != 0 {
		t.Fatalf("local staging failure unexpectedly retried: %v (%d force requests)", err, forced.Load())
	}
	assertPrevious()

	// Damage a covered byte after the independently valid snapshot is on disk.
	saved[len(saved)-1] ^= 0xff
	check(t, os.WriteFile(covered, saved, 0600))
	mode.Store(1)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- bad.Poll(ctx) }()
	select {
	case <-requested:
	case err := <-done:
		t.Fatalf("invalid retained data did not request a snapshot: %v", err)
	case <-ctx.Done():
		t.Fatal("timed out waiting for replacement snapshot request")
	}
	assertPrevious()
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) || !errors.Is(err, errRetainedDeltaRecovery) {
		t.Fatalf("expected canceled replacement with original failure, got %v", err)
	}
	assertPrevious()

	mode.Store(2)
	if err := bad.Poll(t.Context()); err == nil || !strings.Contains(err.Error(), "read replication manifest") {
		t.Fatalf("empty replacement changed error handling: %v", err)
	}
	assertPrevious()
	if got := forced.Load(); got != 2 {
		t.Fatalf("failed polls made %d replacement requests, want 2", got)
	}
	if len(bad.snapshotSlots) != 0 {
		t.Fatal("failed replacement kept a snapshot permit")
	}

	mode.Store(0)
	poll(t, bad)
	if got := reportStatus(t, bad, 0); got.Sequence != 3 || got.Snapshot != 2 ||
		got.Bootstraps != 1 || got.LastError != "" || forced.Load() != 3 {
		t.Fatalf("replacement snapshot did not publish once: status %+v, force requests %d", got, forced.Load())
	}
	expectGet(t, bad, 0, "key", http.StatusOK, "in snapshot")
	expectGet(t, bad, 0, "after", http.StatusOK, "tail")

	writer = openWriter(t, directory)
	put(t, writer, "after", "corrupt ordinary tail", 0)
	check(t, writer.Close())
	ordinary := filepath.Join(directory, fmt.Sprintf("log-%020d-%020d.seg", 4, 4))
	raw, err := os.ReadFile(ordinary)
	check(t, err)
	raw[len(raw)-1] ^= 0xff
	check(t, os.WriteFile(ordinary, raw, 0600))
	if err := bad.Poll(t.Context()); err == nil || forced.Load() != 3 {
		t.Fatalf("ordinary tail validation unexpectedly retried: %v (%d force requests)", err, forced.Load())
	}
	expectGet(t, bad, 0, "after", http.StatusOK, "tail")
}
