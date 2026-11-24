//go:build linux

package gologreplica

import (
	"bytes"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestOnlineRebaseKeepsSequenceAndReusesTail(t *testing.T) {
	maintainer := requireNativeTools(t)
	primary := t.TempDir()
	directory := filepath.Join(primary, dbName(0))
	writer := openWriter(t, directory)
	for range 12 {
		put(t, writer, "key", strings.Repeat("x", 128), 0)
	}
	check(t, writer.Close())
	handler, err := NewSource(primary)
	check(t, err)
	var attempts atomic.Int32
	var deferred atomic.Int32
	var injectChecksumError atomic.Bool
	var lastRebase atomic.Pointer[manifest]
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("rebase") == "" {
			handler.ServeHTTP(w, r)
			return
		}
		if r.URL.Query().Get(deferSnapshotQuery) == "" {
			attempts.Add(1)
		}
		recorded := httptest.NewRecorder()
		handler.ServeHTTP(recorded, r)
		if recorded.Code == http.StatusNoContent {
			deferred.Add(1)
			w.Header().Set(snapshotRequiredHeader, recorded.Header().Get(snapshotRequiredHeader))
		}
		data := recorded.Body.Bytes()
		info, err := readManifest(bytes.NewReader(data))
		if err == nil && info.Rebase {
			lastRebase.Store(&info)
			if injectChecksumError.Load() {
				data[len(data)-1] ^= 0xff
			}
		}
		w.WriteHeader(recorded.Code)
		w.Write(data)
	}))
	defer upstream.Close()
	root := t.TempDir()
	follower, err := OpenFollowerWithOptions(root, upstream.URL, []int{0}, FollowerOptions{
		PullIdleTimeout: DefaultTransferIdleTimeout, SnapshotConcurrency: 1,
	})
	check(t, err)
	db := follower.byNumber[0]
	db.minimumRebase = 1
	defer func() { follower.Close() }()
	capped, err := OpenFollower(t.TempDir(), upstream.URL, []int{0})
	check(t, err)
	defer capped.Close()
	poll(t, follower)
	poll(t, capped)

	compact := func() {
		t.Helper()
		output, err := exec.CommandContext(t.Context(), maintainer, "--data-dir", primary, "--once").CombinedOutput()
		if err != nil {
			t.Fatalf("compact source: %v\n%s", err, output)
		}
	}
	seal := func(key, value string) {
		t.Helper()
		writer := openWriter(t, directory)
		put(t, writer, key, value, 0)
		check(t, writer.Close())
	}
	compact()
	seal("tail", "first")
	poll(t, follower)
	poll(t, capped)
	poll(t, capped)
	if attempts.Load() != 0 || capped.byNumber[0].requestedSnapshot != 0 {
		t.Fatal("default size floor requested a small snapshot")
	}
	before := db.result.Load()
	if before.status.Sequence != 13 || db.requestedSnapshot != 12 {
		t.Fatalf("online rebase candidate was not scheduled: %+v", before.status)
	}
	generation := filepath.Join(db.directory, before.view.Generation)
	injectChecksumError.Store(true)
	if err := follower.Poll(t.Context()); err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("corrupted snapshot accepted: %v", err)
	}
	if current := db.result.Load(); current.view != before.view || current.status.Sequence != 13 || current.status.Rebases != 0 {
		t.Fatalf("corrupted rebase replaced published view: %+v", current.status)
	}
	if _, err := os.Stat(generation); err != nil {
		t.Fatalf("prior published generation was removed after corruption: %v", err)
	}
	expectGet(t, follower, 0, "tail", http.StatusOK, "first")

	injectChecksumError.Store(false)
	seal("tail", "second")
	poll(t, follower)
	if attempts.Load() != 1 || deferred.Load() != 1 || db.result.Load().status.Sequence != 14 {
		t.Fatal("snapshot retry delay blocked ordinary sealed-log progress")
	}
	compact()
	seal("still-needed", "third")
	db.retryRebaseAt = time.Time{}
	poll(t, follower)
	if got := reportStatus(t, follower, 0); got.Sequence != 15 || got.Snapshot != 0 ||
		got.Rebases != 0 || attempts.Load() != 1 || deferred.Load() != 1 || db.requestedSnapshot != 14 {
		t.Fatalf("stale request did not continue with latest hint and suffix: %+v", got)
	}
	generation = filepath.Join(db.directory, db.result.Load().view.Generation)
	const suffix = "log-00000000000000000015-00000000000000000015.seg"
	beforeSuffix, err := os.Stat(filepath.Join(generation, suffix))
	check(t, err)

	db.retryRebaseAt = time.Time{}
	poll(t, follower)
	rebased := reportStatus(t, follower, 0)
	if rebased.Sequence != 15 || rebased.Snapshot != 14 || rebased.Rebases != 1 ||
		rebased.Bootstraps != 0 || attempts.Load() != 2 || deferred.Load() != 2 {
		t.Fatalf("online rebase regressed or advanced its cursor: %+v", rebased)
	}
	info := lastRebase.Load()
	if info.Snapshot != 14 || info.Last != 15 || len(info.Files) != 1 || info.Files[0].Name != snapshotName(14) {
		t.Fatalf("online rebase retransmitted local suffix: %+v", info)
	}
	updatedGeneration := filepath.Join(db.directory, db.result.Load().view.Generation)
	afterSuffix, err := os.Stat(filepath.Join(updatedGeneration, suffix))
	check(t, err)
	if !os.SameFile(beforeSuffix, afterSuffix) {
		t.Fatal("online rebase did not reuse the existing suffix inode")
	}
	if _, err := os.Stat(generation); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("old generation was not reclaimed: %v", err)
	}
	prefix := filepath.Join(updatedGeneration, "log-00000000000000000001-00000000000000000012.seg")
	if _, err := os.Stat(prefix); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("covered log prefix was retained: %v", err)
	}
	poll(t, follower)
	if attempts.Load() != 2 || deferred.Load() != 2 {
		t.Fatal("same checkpoint was requested again")
	}

	check(t, follower.Close())
	follower, err = OpenFollower(root, upstream.URL, []int{0})
	check(t, err)
	if got := reportStatus(t, follower, 0); got.Sequence != 15 || got.Snapshot != 14 ||
		got.Source != rebased.Source {
		t.Fatalf("restart lost new snapshot and suffix: %+v", got)
	}
	expectGet(t, follower, 0, "key", http.StatusOK, strings.Repeat("x", 128))
	expectGet(t, follower, 0, "tail", http.StatusOK, "second")
	expectGet(t, follower, 0, "still-needed", http.StatusOK, "third")
}
func TestOnlineRebaseRequiresMeaningfulCoveredBytes(t *testing.T) {
	db := &replicaDB{directory: t.TempDir(), minimumRebase: 1}
	current := &view{installed: installed{Generation: "generation-100", Sequence: 2}}
	directory := filepath.Join(db.directory, current.Generation)
	check(t, os.Mkdir(directory, 0700))
	for name, size := range map[string]int{
		"log-00000000000000000001-00000000000000000001.seg": 64,
		"log-00000000000000000002-00000000000000000002.seg": 1024,
	} {
		check(t, os.WriteFile(filepath.Join(directory, name), make([]byte, size), 0600))
	}
	db.considerSnapshot(current, &snapshotHint{Sequence: 1, Size: 33})
	if db.requestedSnapshot != 0 {
		t.Fatal("rebase was requested using bytes outside the covered prefix")
	}
	db.considerSnapshot(current, &snapshotHint{Sequence: 2, Size: 128})
	if db.requestedSnapshot != 2 {
		t.Fatal("rebase was not requested even though covered prefix is large enough")
	}
}
