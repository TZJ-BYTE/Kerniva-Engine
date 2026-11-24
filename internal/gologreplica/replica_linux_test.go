//go:build linux

package gologreplica

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/TZJ-BYTE/Kerniva/internal/datastruct"
	"github.com/TZJ-BYTE/Kerniva/internal/golog"
	"golang.org/x/sys/unix"
)

func TestClosedFollowerCannotPollReopenedRoot(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()
	root := t.TempDir()
	first, err := OpenFollower(root, server.URL, []int{0})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { first.Close() })
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := OpenFollower(root, server.URL, []int{0})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { second.Close() })
	if err := first.Close(); err != nil {
		t.Fatalf("repeated close: %v", err)
	}
	if err := first.Poll(context.Background()); err == nil || requests.Load() != 0 {
		t.Fatalf("poll after close: err=%v, requests=%d", err, requests.Load())
	}
}

func TestFollowersPreservePublishedDataAcrossFaultsAndCompaction(t *testing.T) {
	maintainer := requireNativeTools(t)
	primary := t.TempDir()
	db := filepath.Join(primary, dbName(0))
	otherDB := filepath.Join(primary, dbName(1))
	writer := openWriter(t, db)
	put(t, writer, "key", "old", 0)
	put(t, writer, "deleted", "present", 0)
	otherWriter := openWriter(t, otherDB)
	put(t, otherWriter, "key", "other database", 0)
	check(t, otherWriter.Close())

	sourceHandler, err := NewSource(primary)
	check(t, err)
	var fault atomic.Uint32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mode := fault.Load()
		if mode == 0 || r.URL.Query().Get("db") != "0" {
			sourceHandler.ServeHTTP(w, r)
			return
		}
		recorded := httptest.NewRecorder()
		sourceHandler.ServeHTTP(recorded, r)
		data := recorded.Body.Bytes()
		if recorded.Code != http.StatusOK || len(data) < 2 {
			http.Error(w, "fault injection could not capture batch", http.StatusInternalServerError)
			return
		}
		if mode == 1 {
			data = data[:len(data)-1]
		} else {
			data[len(data)-1] ^= 0xff
		}
		w.Write(data)
	}))
	defer server.Close()
	firstRoot, secondRoot := t.TempDir(), t.TempDir()
	first, err := OpenFollower(firstRoot, server.URL, []int{0, 1})
	check(t, err)
	defer first.Close()
	second, err := OpenFollower(secondRoot, server.URL, []int{0})
	check(t, err)

	poll(t, first)
	if report := reportStatus(t, first, 0); !report.Ready || report.Sequence != 0 {
		t.Fatalf("active file became visible to replica: %+v", report)
	}
	expectGet(t, first, 0, "key", http.StatusNotFound, "")
	expectGet(t, first, 1, "key", http.StatusOK, "other database")
	check(t, writer.Close())
	poll(t, first)
	poll(t, second)
	if report := reportStatus(t, first, 0); report.Sequence != 2 || report.Bootstraps != 0 {
		t.Fatalf("initial log-only replication: %+v", report)
	}
	check(t, second.Close())

	writer = openWriter(t, db)
	put(t, writer, "key", "new", 0)
	check(t, writer.Delete([]byte("deleted")))
	put(t, writer, "old-ttl", "expired", time.Now().Add(-time.Second).UnixMilli())
	check(t, writer.Close())
	for _, mode := range []uint32{1, 2} {
		fault.Store(mode)
		if err := first.Poll(t.Context()); err == nil {
			t.Fatalf("fault %d accepted", mode)
		}
		status := reportStatus(t, first, 0)
		if status.Sequence != 2 || status.Installs != 2 || status.LastError == "" {
			t.Fatalf("fault %d changed replica cursor: %+v", mode, status)
		}
		if mode == 2 && !strings.Contains(status.LastError, "checksum") {
			t.Fatalf("expected Rust checksum validation, got %+v", status)
		}
		expectGet(t, first, 0, "key", http.StatusOK, "old")
	}
	fault.Store(0)
	poll(t, first)
	expectGet(t, first, 0, "key", http.StatusOK, "new")
	expectGet(t, first, 0, "deleted", http.StatusNotFound, "")
	expectGet(t, first, 0, "old-ttl", http.StatusNotFound, "")
	advanced := reportStatus(t, first, 0)
	if advanced.Sequence != 5 || advanced.Installs != 3 || advanced.LastError != "" {
		t.Fatalf("incremental recovery: %+v", advanced)
	}
	poll(t, first)
	if unchanged := reportStatus(t, first, 0); unchanged != advanced {
		t.Fatalf("empty poll installed duplicate data: before %+v, after %+v", advanced, unchanged)
	}

	if output, err := exec.CommandContext(t.Context(), maintainer, "--data-dir", primary, "--once").CombinedOutput(); err != nil {
		t.Fatalf("compact source: %v\n%s", err, output)
	}
	if _, err := os.Stat(filepath.Join(db, "log-00000000000000000001-00000000000000000002.seg")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("compaction retained first log: %v", err)
	}
	writer = openWriter(t, db)
	put(t, writer, "suffix", "after snapshot", 0)
	check(t, writer.Close())

	second, err = OpenFollower(secondRoot, server.URL, []int{0})
	check(t, err)
	expectGet(t, second, 0, "key", http.StatusOK, "old")
	poll(t, second)
	caughtUp := reportStatus(t, second, 0)
	if caughtUp.Sequence != 6 || caughtUp.Snapshot != 5 || caughtUp.Bootstraps != 1 || caughtUp.Source != advanced.Source {
		t.Fatalf("snapshot fallback with suffix: %+v", caughtUp)
	}
	expectGet(t, second, 0, "key", http.StatusOK, "new")
	expectGet(t, second, 0, "suffix", http.StatusOK, "after snapshot")
	check(t, second.Close())

	restartedSource, err := NewSource(primary)
	check(t, err)
	restarted := httptest.NewServer(restartedSource)
	defer restarted.Close()
	second, err = OpenFollower(secondRoot, restarted.URL, []int{0})
	check(t, err)
	expectGet(t, second, 0, "suffix", http.StatusOK, "after snapshot")
	poll(t, second)
	check(t, second.Close())

	changedSource, err := NewSource(t.TempDir())
	check(t, err)
	changed := httptest.NewServer(changedSource)
	defer changed.Close()
	second, err = OpenFollower(secondRoot, changed.URL, []int{0})
	check(t, err)
	defer second.Close()
	if err := second.Poll(t.Context()); err == nil || !strings.Contains(err.Error(), "identity changed") {
		t.Fatalf("wrong source was not rejected: %v", err)
	}
	expectGet(t, second, 0, "suffix", http.StatusOK, "after snapshot")
	check(t, second.Close())

	pointer := filepath.Join(secondRoot, "follower-v1", dbName(0), "CURRENT")
	data, err := os.ReadFile(pointer)
	check(t, err)
	check(t, os.WriteFile(pointer, append(data, []byte("{}")...), 0600))
	rejected, err := OpenFollower(secondRoot, changed.URL, []int{0})
	if err == nil {
		rejected.Close()
		t.Fatal("replica CURRENT accepted a trailing JSON document")
	}
}

func TestExportDescriptorSurvivesRustReclaim(t *testing.T) {
	maintainer := requireNativeTools(t)
	primary := t.TempDir()
	db := filepath.Join(primary, dbName(0))
	writer := openWriter(t, db)
	put(t, writer, "key", "value", 0)
	check(t, writer.Close())
	handler, err := NewSource(primary)
	check(t, err)
	s := &source{root: primary}
	s.id, err = sourceID(primary)
	check(t, err)
	export, err := s.capture(t.Context(), 0, 0, 0, false, false)
	check(t, err)
	defer export.close()
	if export.info.Last != 1 || len(export.files) != 1 {
		t.Fatalf("unexpected immutable export: %+v", export.info)
	}

	hold, err := lockFile(t.Context(), filepath.Join(db, ".recovery.lock"), unix.LOCK_SH, false)
	check(t, err)
	defer hold.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, maintainer, "--data-dir", primary, "--once")
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	check(t, cmd.Start())
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		t.Fatalf("maintainer did not wait for the Go-held recovery lock: %v\n%s", err, &output)
	case <-time.After(150 * time.Millisecond):
	}
	snapshot := filepath.Join(db, snapshotName(1))
	if _, err := os.Stat(snapshot); err != nil {
		t.Fatalf("maintainer did not reach snapshot publication while lock held: %v", err)
	}
	if _, err := os.Stat(filepath.Join(db, "CURRENT")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("CURRENT became visible before lock release: %v", err)
	}
	check(t, hold.Close())
	if err := <-done; err != nil {
		t.Fatalf("compact source: %v\n%s", err, &output)
	}
	if _, err := os.Stat(export.files[0].Name()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("maintainer retained opened log name: %v", err)
	}
	data, err := io.ReadAll(export.files[0])
	check(t, err)
	if int64(len(data)) != export.info.Files[0].Size || !bytes.HasPrefix(data, []byte("KGLLOG01")) {
		t.Fatal("open descriptor did not preserve reclaimed log bytes")
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	follower, err := OpenFollower(t.TempDir(), server.URL, []int{0})
	check(t, err)
	defer follower.Close()
	poll(t, follower)
	if report := reportStatus(t, follower, 0); report.Snapshot != 1 || report.Sequence != 1 {
		t.Fatalf("fallback after reclaim: %+v", report)
	}
}

func TestManifestRejectsInvalidNamesAndSequenceGaps(t *testing.T) {
	for _, name := range []string{"../CURRENT", "active.log", "log-1-2.seg", "log-00000000000000000002-00000000000000000002.seg"} {
		info := manifest{
			Version: 1, Source: "42127788badfeedd42127788badfeedd",
			Last: 2, Files: []fileEntry{{Name: name, Size: 40}},
		}
		if err := info.validate(0, 0); err == nil {
			t.Fatalf("accepted invalid initial file %q", name)
		}
	}
}

func TestPendingGenerationWaitsForDirectorySync(t *testing.T) {
	parent := t.TempDir()
	db := &replicaDB{number: 0, directory: filepath.Join(parent, "db_0")}
	prior := &view{
		installed: installed{Generation: "generation-111", Sequence: 1},
		values:    map[string]*datastruct.DataValue{"k": {Value: "old"}},
	}
	next := &view{
		installed: installed{Generation: "generation-222", Sequence: 2},
		changes:   map[string]*datastruct.DataValue{"k": {Value: "new"}},
	}
	for _, state := range []*view{prior, next} {
		check(t, os.MkdirAll(filepath.Join(db.directory, state.Generation), 0700))
	}
	db.result.Store(&report{view: prior, status: statusFor(prior)})
	db.pending = next // CURRENT was renamed, but syncing the directory failed.
	follower := &Follower{databases: []*replicaDB{db}, byNumber: map[int]*replicaDB{0: db}}
	unavailable := filepath.Join(parent, "unavailable")
	check(t, os.Rename(db.directory, unavailable))
	if err := follower.Poll(t.Context()); err == nil {
		t.Fatal("unavailable publication directory was accepted")
	}
	if current := db.result.Load(); current.view != prior || current.status.Sequence != 1 || db.pending != next {
		t.Fatalf("failed sync advanced current view: %+v", current.status)
	}
	expectGet(t, follower, 0, "k", http.StatusOK, "old")
	for _, state := range []*view{prior, next} {
		_, err := os.Stat(filepath.Join(unavailable, state.Generation))
		check(t, err)
	}
	check(t, os.Rename(unavailable, db.directory))
	poll(t, follower)
	if current := db.result.Load(); current.view != next || current.status.Sequence != 2 || current.status.LastError != "" || db.pending != nil {
		t.Fatalf("successful retry did not publish verified view: %+v", current.status)
	}
	expectGet(t, follower, 0, "k", http.StatusOK, "new")
	if _, err := os.Stat(filepath.Join(db.directory, prior.Generation)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("old generation was not removed after durable publication: %v", err)
	}
}

func TestManifestRejectsTrailingDocument(t *testing.T) {
	content := []byte(`{"version":1,"source":"42127788badfeedd42127788badfeedd","db":0,"after":0,"last":0}`)
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(content)))
	m, err := readManifest(bytes.NewReader(append(header[:], content...)))
	check(t, err)
	check(t, m.validate(0, 0))

	content = append(content, []byte(`{}`)...)
	binary.BigEndian.PutUint32(header[:], uint32(len(content)))
	if _, err := readManifest(bytes.NewReader(append(header[:], content...))); err == nil {
		t.Fatal("manifest accepted a trailing JSON document")
	}
}

func requireNativeTools(t *testing.T) string {
	t.Helper()
	maintainer := os.Getenv("KERNIVA_MAINTAINER")
	if maintainer == "" || os.Getenv("KERNIVA_ENGINE_LIBRARY") == "" {
		t.Skip("set KERNIVA_ENGINE_LIBRARY and KERNIVA_MAINTAINER to built Linux artifacts")
	}
	return maintainer
}

func openWriter(t *testing.T, directory string) *golog.Writer {
	t.Helper()
	writer, err := golog.Open(directory, golog.Options{Sync: true, MaxAge: time.Hour}, nil)
	check(t, err)
	t.Cleanup(func() { writer.Close() })
	return writer
}

func put(t *testing.T, writer *golog.Writer, key, value string, expiry int64) {
	t.Helper()
	data, err := (&datastruct.DataValue{
		Value: &datastruct.String{Data: value}, ExpireTime: expiry,
	}).Serialize()
	check(t, err)
	check(t, writer.Put([]byte(key), data))
}

func check(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func poll(t *testing.T, follower *Follower) {
	t.Helper()
	check(t, follower.Poll(t.Context()))
}

func reportStatus(t *testing.T, follower *Follower, db int) Status {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/status?db=%d", db), nil)
	w := httptest.NewRecorder()
	follower.Handler().ServeHTTP(w, r)
	var status Status
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status %d: %s", w.Code, w.Body.String())
	}
	check(t, json.Unmarshal(w.Body.Bytes(), &status))
	return status
}

func expectGet(t *testing.T, follower *Follower, db int, key string, code int, expected string) {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/get?db=%d&key=%s", db, key), nil)
	w := httptest.NewRecorder()
	follower.Handler().ServeHTTP(w, r)
	if w.Code != code || code == http.StatusOK && w.Body.String() != expected {
		t.Fatalf("get db %d key %q: status %d body %q, expected status %d value %q", db, key, w.Code, w.Body.String(), code, expected)
	}
}
