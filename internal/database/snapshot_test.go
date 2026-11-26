package database

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/TZJ-BYTE/Kerniva/internal/datastruct"
)

func requireSnapshotSuccess(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func TestStandaloneSnapshotRoundTrip(t *testing.T) {
	dir := t.TempDir()
	dbs := []*Database{NewDatabase(0), NewDatabase(1), NewDatabase(2)}
	first, second := dbs[0], dbs[1]
	binaryKey := []byte{'k', 0, 255}
	binaryValue := []byte{0, 255, 128}
	requireSnapshotSuccess(t, first.SetStringBytes(binaryKey, binaryValue))
	requireSnapshotSuccess(t, second.SetStringBytes(binaryKey, []byte("second db")))
	_, err := first.HSet("hash", "field", "value")
	requireSnapshotSuccess(t, err)
	_, err = first.RPush("list", []string{"a", "b", "a"})
	requireSnapshotSuccess(t, err)
	_, err = first.SAdd("set", []string{"a", "b"})
	requireSnapshotSuccess(t, err)
	_, err = first.ZAdd("zset", []datastruct.ZSetMember{{Member: "a", Score: 1.25}, {Member: "b", Score: -2}})
	requireSnapshotSuccess(t, err)
	_, err = first.XAdd("stream", "100-1", []string{"field", "value"}, -1, false)
	requireSnapshotSuccess(t, err)
	requireSnapshotSuccess(t, first.XGroupCreate("stream", "group", "0", false))
	_, err = first.XReadGroup("group", "owner", "stream", ">", 1, 0, false)
	requireSnapshotSuccess(t, err)

	expireAt := time.Now().Add(time.Hour).UnixMilli()
	ok, err := first.PExpireAt("hash", expireAt)
	requireSnapshotSuccess(t, err)
	if !ok {
		t.Fatal("could not set hash expiry")
	}
	requireSnapshotSuccess(t, first.SetStringBytes([]byte("deleted"), []byte("v")))
	requireSnapshotSuccess(t, first.SetStringBytes([]byte("expired"), []byte("v")))
	requireSnapshotSuccess(t, SaveSnapshot(dir, dbs))

	if !first.Delete("deleted") {
		t.Fatal("could not delete seeded key")
	}
	ok, err = first.PExpireAt("expired", time.Now().Add(-time.Second).UnixMilli())
	requireSnapshotSuccess(t, err)
	if !ok {
		t.Fatal("could not expire seeded key")
	}
	requireSnapshotSuccess(t, SaveSnapshot(dir, dbs))

	generations, err := snapshotGenerations(dir)
	requireSnapshotSuccess(t, err)
	if !slices.Equal(generations, []uint64{1, 2}) {
		t.Fatalf("generations %v, want current and preceding", generations)
	}
	requireSnapshotSuccess(t, os.WriteFile(filepath.Join(dir, ".snapshot-crash.tmp"), []byte("incomplete"), 0o600))

	restored := []*Database{NewDatabase(0), NewDatabase(1), NewDatabase(2)}
	requireSnapshotSuccess(t, LoadSnapshot(dir, restored))
	for _, key := range []string{string(binaryKey), "hash", "list", "set", "zset", "stream"} {
		expected, found := first.Get(key)
		if !found {
			t.Fatalf("source key %q missing", key)
		}
		actual, found := restored[0].Get(key)
		if !found || actual.ExpireTime != expected.ExpireTime || !reflect.DeepEqual(expected.Value, actual.Value) {
			t.Fatalf("key %q restored as %#v, want %#v", key, actual, expected)
		}
	}
	if restored[0].Exists("deleted") || restored[0].Exists("expired") || restored[1].Exists("hash") || restored[2].Exists(string(binaryKey)) {
		t.Fatal("unexpected key restored")
	}
	if actual, ok, err := restored[1].GetStringBytesCopy(binaryKey); err != nil || !ok || !bytes.Equal(actual, []byte("second db")) {
		t.Fatalf("second db returned %q, %t, %v", actual, ok, err)
	}
	if actual, ok := restored[0].GetExpireTime("hash"); !ok || actual != expireAt {
		t.Fatalf("expiry is %d, %t, want %d", actual, ok, expireAt)
	}

	requireSnapshotSuccess(t, first.Clear())
	requireSnapshotSuccess(t, second.Clear())
	requireSnapshotSuccess(t, SaveSnapshot(dir, dbs))
	generations, err = snapshotGenerations(dir)
	requireSnapshotSuccess(t, err)
	if !slices.Equal(generations, []uint64{2, 3}) {
		t.Fatalf("after third save generations = %v", generations)
	}
	empty := []*Database{NewDatabase(0), NewDatabase(1), NewDatabase(2)}
	requireSnapshotSuccess(t, LoadSnapshot(dir, empty))
	if empty[0].Exists("hash") || empty[1].Exists(string(binaryKey)) {
		t.Fatal("clear did not survive snapshot restore")
	}
}

func TestSnapshotCorruptionDoesNotRestoreOlderGenerationOrPartialState(t *testing.T) {
	dir := t.TempDir()
	dbs := []*Database{NewDatabase(0), NewDatabase(1)}
	requireSnapshotSuccess(t, dbs[0].SetStringBytes([]byte("deleted"), []byte("old")))
	requireSnapshotSuccess(t, SaveSnapshot(dir, dbs))
	if !dbs[0].Delete("deleted") {
		t.Fatal("could not delete seeded key")
	}
	requireSnapshotSuccess(t, dbs[1].SetStringBytes([]byte("keep"), []byte("current")))
	requireSnapshotSuccess(t, SaveSnapshot(dir, dbs))
	path := snapshotPath(dir, 2)
	content, err := os.ReadFile(path)
	requireSnapshotSuccess(t, err)
	content[len(content)-1] ^= 0xff
	requireSnapshotSuccess(t, os.WriteFile(path, content, 0o600))
	restored := []*Database{NewDatabase(0), NewDatabase(1)}
	if err := LoadSnapshot(dir, restored); err == nil {
		t.Fatal("restored corrupt snapshot")
	}
	if restored[0].Exists("deleted") || restored[1].Exists("keep") {
		t.Fatal("loaded an older generation or a partial image")
	}
}

func TestSnapshotUsesAbsoluteExpiryOnRestore(t *testing.T) {
	dir := t.TempDir()
	expireAt := time.Now().Add(time.Minute).UnixMilli()
	value := &datastruct.DataValue{
		Value:      &datastruct.BytesString{Data: []byte("ttl")},
		ExpireTime: expireAt,
	}
	encoded, err := value.Serialize()
	requireSnapshotSuccess(t, err)
	// The previously written value expires before the interrupted process
	// can restart. Inject an already elapsed timestamp to avoid sleeping.
	value.ExpireTime = time.Now().Add(-time.Minute).UnixMilli()
	expired, err := value.Serialize()
	requireSnapshotSuccess(t, err)
	image := snapshotImage{{{key: "future", value: encoded}, {key: "elapsed", value: expired}}}
	requireSnapshotSuccess(t, publishSnapshot(dir, 1, image))
	db := NewDatabase(0)
	requireSnapshotSuccess(t, LoadSnapshot(dir, []*Database{db}))
	if db.Exists("elapsed") {
		t.Fatal("restored a value that expired after capture")
	}
	if actual, ok := db.GetExpireTime("future"); !ok || actual != expireAt {
		t.Fatalf("expiry is %d, %t, want %d", actual, ok, expireAt)
	}
}

func TestConcurrentSnapshotPublication(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data", "snapshot")
	dbs := []*Database{NewDatabase(0)}
	requireSnapshotSuccess(t, dbs[0].SetStringBytes([]byte("key"), []byte("value")))
	const savers = 4
	results := make(chan error, savers)
	var done sync.WaitGroup
	for range savers {
		done.Go(func() { results <- SaveSnapshot(dir, dbs) })
	}
	done.Wait()
	close(results)
	for err := range results {
		requireSnapshotSuccess(t, err)
	}
	generations, err := snapshotGenerations(dir)
	requireSnapshotSuccess(t, err)
	if !slices.Equal(generations, []uint64{savers - 1, savers}) {
		t.Fatalf("concurrent saves published generations %v", generations)
	}
	requireSnapshotSuccess(t, dbs[0].Close())
	if err := SaveSnapshot(dir, dbs); err == nil {
		t.Fatal("save unexpectedly captured a closed database")
	}
	restored := NewDatabase(0)
	requireSnapshotSuccess(t, LoadSnapshot(dir, []*Database{restored}))
	if actual, found, err := restored.GetStringCopy([]byte("key")); err != nil || !found || actual != "value" {
		t.Fatalf("key after failed save = %q, %t, %v", actual, found, err)
	}
}
