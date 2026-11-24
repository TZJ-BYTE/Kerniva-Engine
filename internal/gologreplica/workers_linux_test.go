//go:build linux

package gologreplica

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const followerTestTimeout = 10 * time.Second

func advanceWorkerSource(t *testing.T, root string, db int, value string) {
	t.Helper()
	writer := openWriter(t, filepath.Join(root, dbName(db)))
	put(t, writer, "key", value, 0)
	check(t, writer.Close())
}

func waitWorkerStatus(t *testing.T, follower *Follower, db int, accept func(Status) bool) Status {
	t.Helper()
	timer := time.NewTimer(followerTestTimeout)
	defer timer.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		status := reportStatus(t, follower, db)
		if accept(status) {
			return status
		}
		select {
		case <-timer.C:
			t.Fatalf("waiting for DB %d: last status %+v", db, status)
		case <-ticker.C:
		}
	}
}

func waitWorkerSignal(t *testing.T, signal <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(followerTestTimeout):
		t.Fatalf("timed out waiting for %s", label)
	}
}

func workerCurrent(t *testing.T, root string, db int) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(root, "follower-v1", dbName(db), "CURRENT"))
	check(t, err)
	return data
}

func expectWorkerCurrent(t *testing.T, data []byte, sequence uint64) {
	t.Helper()
	var current installed
	check(t, json.Unmarshal(data, &current))
	if current.Sequence != sequence {
		t.Fatalf("CURRENT = %+v, want sequence %d", current, sequence)
	}
}

func TestFollowerRunIsolatesDatabasesAndClosesInFlight(t *testing.T) {
	requireNativeTools(t)
	primary := t.TempDir()
	advanceWorkerSource(t, primary, 0, "old DB0")
	advanceWorkerSource(t, primary, 1, "old DB1")
	source, err := NewSource(primary)
	check(t, err)

	var mode atomic.Int32
	firstStarted := make(chan struct{}, 1)
	releaseFirst := make(chan struct{})
	closeStarted := make(chan struct{}, 1)
	closeCanceled := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		action := mode.Load()
		if action == 0 || r.URL.Query().Get("db") != "0" {
			source.ServeHTTP(w, r)
			return
		}
		capture := httptest.NewRecorder()
		source.ServeHTTP(capture, r)
		data := capture.Body.Bytes()
		if capture.Code != http.StatusOK || len(data) < 2 {
			http.Error(w, "failed to capture staged pull", http.StatusInternalServerError)
			return
		}

		// Keep one byte back so the follower must wait for the rest of the
		// sealed segment and cannot publish until the handler is released.
		w.Header().Set("Content-Length", fmt.Sprint(len(data)))
		if _, err := w.Write(data[:len(data)-1]); err != nil {
			return
		}
		w.(http.Flusher).Flush()
		switch action {
		case 1:
			select {
			case firstStarted <- struct{}{}:
			default:
			}
			select {
			case <-releaseFirst:
			case <-r.Context().Done():
			}
		case 2:
			select {
			case closeStarted <- struct{}{}:
			default:
			}
			<-r.Context().Done()
			select {
			case closeCanceled <- struct{}{}:
			default:
			}
		}
	}))
	defer server.Close()

	root := t.TempDir()
	follower, err := OpenFollower(root, server.URL, []int{0, 1})
	check(t, err)
	defer follower.Close()
	poll(t, follower)
	db0Current := workerCurrent(t, root, 0)
	db1Current := workerCurrent(t, root, 1)
	advanceWorkerSource(t, primary, 0, "unpublished DB0")
	mode.Store(1)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	runDone := make(chan error, 1)
	go func() { runDone <- follower.Run(ctx, 5*time.Millisecond) }()
	waitWorkerSignal(t, firstStarted, "DB 0 partial transfer")

	for i, value := range []string{"DB1 second write", "DB1 third write"} {
		advanceWorkerSource(t, primary, 1, value)
		want := uint64(i + 2)
		waitWorkerStatus(t, follower, 1, func(s Status) bool {
			return s.Ready && s.Sequence == want && s.LastError == ""
		})
		expectGet(t, follower, 1, "key", http.StatusOK, value)
		expectGet(t, follower, 0, "key", http.StatusOK, "old DB0")
		if bytes.Equal(workerCurrent(t, root, 1), db1Current) {
			t.Fatalf("DB 1 sequence %d was visible without advancing CURRENT", want)
		}
		db1Current = workerCurrent(t, root, 1)
		expectWorkerCurrent(t, db1Current, want)
	}

	close(releaseFirst)
	waitWorkerStatus(t, follower, 0, func(s Status) bool {
		return s.Sequence == 1 && s.LastError != ""
	})
	expectGet(t, follower, 0, "key", http.StatusOK, "old DB0")
	if !bytes.Equal(workerCurrent(t, root, 0), db0Current) {
		t.Fatal("truncated DB 0 transfer changed CURRENT")
	}

	mode.Store(2)
	waitWorkerSignal(t, closeStarted, "DB 0 transfer held for Close")
	advanceWorkerSource(t, primary, 1, "DB1 fourth write")
	waitWorkerStatus(t, follower, 1, func(s Status) bool {
		return s.Ready && s.Sequence == 4 && s.LastError == ""
	})
	expectGet(t, follower, 1, "key", http.StatusOK, "DB1 fourth write")

	closed := make(chan error, 1)
	go func() { closed <- follower.Close() }()
	select {
	case err := <-closed:
		check(t, err)
	case <-time.After(followerTestTimeout):
		t.Fatal("Close did not cancel and drain the active transfer")
	}
	waitWorkerSignal(t, closeCanceled, "cancellation at the DB 0 handler")
	select {
	case err := <-runDone:
		check(t, err)
	case <-time.After(followerTestTimeout):
		t.Fatal("Run did not return after Close")
	}
	reopened, err := OpenFollower(root, server.URL, []int{0, 1})
	check(t, err)
	defer reopened.Close()
	for db, want := range []uint64{1, 4} {
		if status := reportStatus(t, reopened, db); !status.Ready || status.Sequence != want {
			t.Fatalf("reopened DB %d: %+v, want sequence %d", db, status, want)
		}
	}
	expectGet(t, reopened, 0, "key", http.StatusOK, "old DB0")
	expectGet(t, reopened, 1, "key", http.StatusOK, "DB1 fourth write")
}

func TestFollowerRunPinsFirstSourceAcrossConcurrentDatabases(t *testing.T) {
	requireNativeTools(t)
	for _, mixed := range []bool{false, true} {
		t.Run(fmt.Sprintf("mixed=%t", mixed), func(t *testing.T) {
			firstRoot, secondRoot := t.TempDir(), t.TempDir()
			advanceWorkerSource(t, firstRoot, 0, "source zero")
			advanceWorkerSource(t, firstRoot, 1, "source one")
			advanceWorkerSource(t, secondRoot, 1, "other source one")
			first, err := NewSource(firstRoot)
			check(t, err)
			second, err := NewSource(secondRoot)
			check(t, err)
			firstID, err := sourceID(firstRoot)
			check(t, err)
			secondID, err := sourceID(secondRoot)
			check(t, err)

			started := make(chan string, 2)
			release := make(chan struct{})
			var requests [2]atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				db := r.URL.Query().Get("db")
				index := 0
				if db == "1" {
					index = 1
				}
				if requests[index].Add(1) == 1 {
					started <- db
					select {
					case <-release:
					case <-r.Context().Done():
						return
					}
				}
				if mixed && db == "1" {
					second.ServeHTTP(w, r)
					return
				}
				first.ServeHTTP(w, r)
			}))
			defer server.Close()
			root := t.TempDir()
			follower, err := OpenFollower(root, server.URL, []int{0, 1})
			check(t, err)
			defer follower.Close()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- follower.Run(ctx, 5*time.Millisecond) }()

			seen := make(map[string]bool)
			for range 2 {
				select {
				case db := <-started:
					seen[db] = true
				case <-time.After(followerTestTimeout):
					t.Fatalf("both initial requests must be in flight before either supplies a source ID: %v", seen)
				}
			}
			if !seen["0"] || !seen["1"] {
				t.Fatalf("unexpected initial requests: %v", seen)
			}
			close(release)

			var statuses [2]Status
			for db := range statuses {
				statuses[db] = waitWorkerStatus(t, follower, db, func(s Status) bool {
					return s.Ready || s.LastError != ""
				})
			}
			check(t, follower.Close())
			select {
			case err := <-done:
				check(t, err)
			case <-time.After(followerTestTimeout):
				t.Fatal("Run did not stop")
			}
			pinned, err := os.ReadFile(filepath.Join(root, "follower-v1", "UPSTREAM"))
			check(t, err)
			sourceForDB := [2]string{firstID, firstID}
			if mixed {
				sourceForDB[1] = secondID
				if string(pinned) != firstID && string(pinned) != secondID {
					t.Fatalf("unexpected pinned source %q", pinned)
				}
			} else if string(pinned) != firstID {
				t.Fatalf("pinned source = %q, want %q", pinned, firstID)
			}

			reopened, err := OpenFollower(root, server.URL, []int{0, 1})
			check(t, err)
			defer reopened.Close()
			for db, source := range sourceForDB {
				status := statuses[db]
				if source != string(pinned) {
					if status.Ready || !strings.Contains(status.LastError, "identity changed") {
						t.Fatalf("DB %d accepted mixed source: %+v", db, status)
					}
					expectGet(t, reopened, db, "key", http.StatusServiceUnavailable, "")
					continue
				}
				if !status.Ready || status.Source != source || status.Sequence != 1 || status.LastError != "" {
					t.Fatalf("DB %d did not install pinned source: %+v", db, status)
				}
				reloaded := reportStatus(t, reopened, db)
				if !reloaded.Ready || reloaded.Source != source || reloaded.Sequence != 1 {
					t.Fatalf("DB %d did not recover pinned source: %+v", db, reloaded)
				}
				value := "source zero"
				if db == 1 {
					value = "source one"
					if source == secondID {
						value = "other source one"
					}
				}
				expectGet(t, reopened, db, "key", http.StatusOK, value)
			}
		})
	}
}

func TestFollowerRunSerializesWithSinglePoll(t *testing.T) {
	requireNativeTools(t)
	primary := t.TempDir()
	advanceWorkerSource(t, primary, 0, "installed by Run")
	source, err := NewSource(primary)
	check(t, err)
	var requests atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			close(started)
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		}
		source.ServeHTTP(w, r)
	}))
	defer server.Close()

	follower, err := OpenFollower(t.TempDir(), server.URL, []int{0})
	check(t, err)
	defer follower.Close()
	runCtx, stopRun := context.WithCancel(t.Context())
	defer stopRun()
	runDone := make(chan error, 1)
	go func() { runDone <- follower.Run(runCtx, time.Hour) }()
	waitWorkerSignal(t, started, "Run's DB 0 pull")

	pollCtx, stopPoll := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer stopPoll()
	pollDone := make(chan error, 1)
	go func() { pollDone <- follower.Poll(pollCtx) }()
	select {
	case err := <-pollDone:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Poll waiting for the DB gate returned %v, want deadline exceeded", err)
		}
	case <-time.After(followerTestTimeout):
		t.Fatal("Poll did not honor its deadline while Run's pull was blocked")
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("Poll issued an overlapping pull: got %d requests, want 1", got)
	}

	close(release)
	waitWorkerStatus(t, follower, 0, func(s Status) bool {
		return s.Ready && s.Sequence == 1 && s.LastError == ""
	})
	expectGet(t, follower, 0, "key", http.StatusOK, "installed by Run")
	stopRun()
	select {
	case err := <-runDone:
		check(t, err)
	case <-time.After(followerTestTimeout):
		t.Fatal("Run did not return after cancellation")
	}
}
