//go:build linux

package gologreplica

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/TZJ-BYTE/Kerniva/internal/datastruct"
	"github.com/TZJ-BYTE/Kerniva/internal/rustengine"
)

func TestIncrementalReplicaAppliesVerifiedChanges(t *testing.T) {
	requireNativeTools(t)
	primary := t.TempDir()
	directory := filepath.Join(primary, dbName(0))
	writer := openWriter(t, directory)
	check(t, writer.WriteBatch([]rustengine.WriteOp{
		stringChange(t, "a", "old", 0), stringChange(t, "b", "old", 0),
		stringChange(t, "deleted", "old", 0), stringChange(t, "expired", "old", 0),
		stringChange(t, "untouched", "retained", 0),
	}))
	check(t, writer.Close())
	other := openWriter(t, filepath.Join(primary, dbName(1)))
	put(t, other, "a", "another database", 0)
	check(t, other.Close())
	handler, err := NewSource(primary)
	check(t, err)
	source := httptest.NewServer(handler)
	defer source.Close()
	root := t.TempDir()
	follower, err := OpenFollower(root, source.URL, []int{0, 1})
	check(t, err)
	defer func() { follower.Close() }()
	poll(t, follower)
	db := follower.byNumber[0]
	initial := db.result.Load().view
	retained := initial.values["untouched"]
	initialMap := reflect.ValueOf(initial.values).Pointer()

	// The sealed records have valid CRCs and sequence numbers. A value that
	// Go cannot decode must still leave the whole staged update invisible.
	writer = openWriter(t, directory)
	check(t, writer.WriteBatch([]rustengine.WriteOp{
		stringChange(t, "a", "new", 0), stringChange(t, "b", "new", 0),
		{Key: []byte("deleted"), Delete: true},
		stringChange(t, "expired", "past deadline", time.Now().Add(-time.Second).UnixMilli()),
		{Key: []byte("z-bad"), Value: []byte{0x03}},
	}))
	check(t, writer.Close())
	if err := follower.Poll(t.Context()); err == nil {
		t.Fatal("accepted an undecodable value")
	}
	if report := db.result.Load(); report.view != initial || report.status.Sequence != 1 || report.status.LastError == "" {
		t.Fatalf("decode error changed the published view: %+v", report.status)
	}
	expectGet(t, follower, 0, "a", http.StatusOK, "old")
	expectGet(t, follower, 0, "deleted", http.StatusOK, "old")
	expectGet(t, follower, 0, "expired", http.StatusOK, "old")

	writer = openWriter(t, directory)
	check(t, writer.Delete([]byte("z-bad")))
	put(t, writer, "a", "overwritten", 0)
	check(t, writer.Close())

	priorSegment := filepath.Join(db.directory, initial.Generation, "log-00000000000000000001-00000000000000000001.seg")
	missing := filepath.Join(t.TempDir(), "missing.seg")
	check(t, os.Rename(priorSegment, missing))
	gapError := follower.Poll(t.Context())
	check(t, os.Rename(missing, priorSegment))
	if gapError == nil || db.result.Load().view != initial {
		t.Fatal("advanced with a missing retained segment")
	}

	// Read the cursor and two keys under the same lock to detect any partially
	// applied update while new requests run alongside the poll.
	started := make(chan struct{})
	stopReading := make(chan struct{})
	reader := make(chan error, 1)
	go func() {
		close(started)
		for {
			db.viewMu.RLock()
			report := db.result.Load()
			left, right := report.view.values["a"], report.view.values["b"]
			db.viewMu.RUnlock()
			sequence := report.status.Sequence
			if left == nil || right == nil {
				reader <- fmt.Errorf("cursor %d lost a key during the update", sequence)
				return
			}
			a, aOK := left.Value.(*datastruct.String)
			b, bOK := right.Value.(*datastruct.String)
			if !aOK || !bOK || !(sequence == 1 && a.Data == "old" && b.Data == "old" ||
				sequence == 4 && a.Data == "overwritten" && b.Data == "new") {
				reader <- fmt.Errorf("cursor %d exposed a partial update", sequence)
				return
			}
			select {
			case <-stopReading:
				reader <- nil
				return
			default:
			}
		}
	}()
	<-started
	err = follower.Poll(t.Context())
	close(stopReading)
	check(t, <-reader)
	check(t, err)
	advanced := db.result.Load()
	if advanced.status.Sequence != 4 || advanced.status.LastError != "" || advanced.view.changes != nil {
		t.Fatalf("valid delta did not advance to its sealed head: %+v", advanced.status)
	}
	if reflect.ValueOf(advanced.view.values).Pointer() != initialMap || advanced.view.values["untouched"] != retained {
		t.Fatal("ordinary advance rebuilt the existing map")
	}
	expectGet(t, follower, 0, "a", http.StatusOK, "overwritten")
	expectGet(t, follower, 0, "b", http.StatusOK, "new")
	expectGet(t, follower, 0, "deleted", http.StatusNotFound, "")
	expectGet(t, follower, 0, "expired", http.StatusNotFound, "")
	expectGet(t, follower, 0, "z-bad", http.StatusNotFound, "")
	expectGet(t, follower, 1, "a", http.StatusOK, "another database")

	writer = openWriter(t, directory)
	put(t, writer, "discard-before-reset", "old", 0)
	check(t, writer.Reset())
	put(t, writer, "after-reset", "present", 0)
	check(t, writer.Close())
	poll(t, follower)
	state := db.result.Load()
	if state.status.Sequence != 7 || reflect.ValueOf(state.view.values).Pointer() == initialMap {
		t.Fatalf("reset did not replace the map: %+v", state.status)
	}
	expectGet(t, follower, 0, "a", http.StatusNotFound, "")
	expectGet(t, follower, 0, "discard-before-reset", http.StatusNotFound, "")
	expectGet(t, follower, 0, "after-reset", http.StatusOK, "present")
	expectGet(t, follower, 1, "a", http.StatusOK, "another database")

	check(t, follower.Close())
	follower, err = OpenFollower(root, source.URL, []int{0, 1})
	check(t, err)
	if got := reportStatus(t, follower, 0); got.Sequence != 7 {
		t.Fatalf("full recovery disagreed with incrementally applied head: %+v", got)
	}
	expectGet(t, follower, 0, "after-reset", http.StatusOK, "present")
	expectGet(t, follower, 0, "a", http.StatusNotFound, "")
	expectGet(t, follower, 1, "a", http.StatusOK, "another database")
}

func stringChange(t *testing.T, key, value string, expiresAt int64) rustengine.WriteOp {
	t.Helper()
	raw, err := (&datastruct.DataValue{Value: &datastruct.String{Data: value}, ExpireTime: expiresAt}).Serialize()
	check(t, err)
	return rustengine.WriteOp{Key: []byte(key), Value: raw}
}
