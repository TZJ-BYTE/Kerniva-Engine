package database

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
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
	// Build an old flat KVSNAP01 file without the snapshot writer.
	legacy := bytes.NewBufferString("KVSNAP01")
	requireSnapshotSuccess(t, binary.Write(legacy, binary.LittleEndian, [2]uint64{1, 2}))
	for _, entry := range []snapshotEntry{{key: "future", value: encoded}, {key: "elapsed", value: expired}} {
		requireSnapshotSuccess(t, binary.Write(legacy, binary.LittleEndian, [2]uint64{uint64(len(entry.key)), uint64(len(entry.value))}))
		legacy.WriteString(entry.key)
		legacy.Write(entry.value)
	}
	checksum := sha256.Sum256(legacy.Bytes())
	legacy.Write(checksum[:])
	requireSnapshotSuccess(t, os.WriteFile(snapshotPath(dir, 1), legacy.Bytes(), 0o600))
	db := NewDatabase(0)
	requireSnapshotSuccess(t, LoadSnapshot(dir, []*Database{db}))
	if db.Exists("elapsed") {
		t.Fatal("restored a value that expired after capture")
	}
	if actual, ok := db.GetExpireTime("future"); !ok || actual != expireAt {
		t.Fatalf("expiry is %d, %t, want %d", actual, ok, expireAt)
	}
}

func TestSnapshotChunksPreserveFlatFile(t *testing.T) {
	dir := t.TempDir()
	dbs := []*Database{NewDatabase(0), NewDatabase(1), NewDatabase(2)}
	counts := []int{2200, 33, 0}
	capacities := [][]int{{32, 64, 128, 256, 512, 1024, 1024}, {32, 64}, nil}
	expires := time.Now().Add(time.Hour).UnixMilli()
	expected := make([]map[string][]byte, len(dbs))
	for dbIndex, db := range dbs {
		expected[dbIndex] = make(map[string][]byte, counts[dbIndex])
		for index := range counts[dbIndex] {
			key := fmt.Sprintf("key\x00%04d", index)
			value := []byte{byte(index), byte(index >> 8), byte(dbIndex)}
			requireSnapshotSuccess(t, db.SetStringBytes([]byte(key), value))
			if index%32 == 0 {
				ok, err := db.PExpireAt(key, expires)
				requireSnapshotSuccess(t, err)
				if !ok {
					t.Fatalf("DB %d key %q expiry was not set", dbIndex, key)
				}
			}
			entry, ok := db.Get(key)
			if !ok {
				t.Fatalf("DB %d key %q is missing", dbIndex, key)
			}
			encoded, err := entry.Serialize()
			requireSnapshotSuccess(t, err)
			expected[dbIndex][key] = encoded
		}
		// Keep an expired entry resident to exercise filtering at capture.
		requireSnapshotSuccess(t, db.SetStringBytes([]byte("expired"), []byte("unused")))
		db.getShard("expired").data["expired"].ExpireTime = time.Now().Add(-time.Minute).UnixMilli()
	}

	image, err := captureSnapshot(dbs, false)
	requireSnapshotSuccess(t, err)
	for i, captured := range image {
		var got []int
		for _, chunk := range captured.chunks {
			got = append(got, cap(chunk))
		}
		if captured.count != uint64(counts[i]) || !slices.Equal(got, capacities[i]) {
			t.Fatalf("DB %d captured %d entries with chunk capacities %v, want %d and %v", i, captured.count, got, counts[i], capacities[i])
		}
	}
	requireSnapshotSuccess(t, SaveSnapshot(dir, dbs))
	raw, err := os.ReadFile(snapshotPath(dir, 1))
	requireSnapshotSuccess(t, err)
	verifyFlatSnapshot(t, raw, expected)

	restored := []*Database{NewDatabase(0), NewDatabase(1), NewDatabase(2)}
	requireSnapshotSuccess(t, LoadSnapshot(dir, restored))
	for i, db := range restored {
		if db.Exists("expired") {
			t.Fatalf("DB %d restored the expired key", i)
		}
		for key, want := range expected[i] {
			entry, ok := db.Get(key)
			if !ok {
				t.Fatalf("DB %d is missing restored key %q", i, key)
			}
			got, err := entry.Serialize()
			requireSnapshotSuccess(t, err)
			if !bytes.Equal(got, want) {
				t.Fatalf("DB %d key %q restored bytes differ", i, key)
			}
		}
	}
}

func TestSnapshotLoadsFixedLegacyFile(t *testing.T) {
	// Two databases, a single binary key/value in DB 0 and an empty DB 1.
	const legacy = "4b56534e415030310200000000000000010000000000000008000000000000001900000000000000" +
		"6c656761637900ff030000000000000000010000000000000002030000000080ff0000000000000000" +
		"c757c356fa37da12ecc814325622e526b840f35fab6f7c97df86dd74e31a86db"
	raw, err := hex.DecodeString(legacy)
	requireSnapshotSuccess(t, err)
	dir := t.TempDir()
	requireSnapshotSuccess(t, os.WriteFile(snapshotPath(dir, 1), raw, 0o600))
	dbs := []*Database{NewDatabase(0), NewDatabase(1)}
	requireSnapshotSuccess(t, LoadSnapshot(dir, dbs))
	key := []byte("legacy\x00\xff")
	value, found, err := dbs[0].GetStringBytesCopy(key)
	requireSnapshotSuccess(t, err)
	if !found || !bytes.Equal(value, []byte{0, 128, 255}) || dbs[1].Exists(string(key)) {
		t.Fatalf("legacy key restored as %v, found %t", value, found)
	}
}

// Read the old flat file layout independently of the production decoder.
func verifyFlatSnapshot(t *testing.T, raw []byte, expected []map[string][]byte) {
	t.Helper()
	if len(raw) < 16+sha256.Size || string(raw[:8]) != "KVSNAP01" {
		t.Fatal("invalid flat snapshot header")
	}
	body := raw[:len(raw)-sha256.Size]
	checksum := sha256.Sum256(body)
	if !bytes.Equal(checksum[:], raw[len(body):]) {
		t.Fatal("flat snapshot checksum differs")
	}
	if count := binary.LittleEndian.Uint64(body[8:16]); count != uint64(len(expected)) {
		t.Fatalf("flat database count is %d, want %d", count, len(expected))
	}
	reader := bytes.NewReader(body[16:])
	for i, want := range expected {
		var count uint64
		requireSnapshotSuccess(t, binary.Read(reader, binary.LittleEndian, &count))
		if count != uint64(len(want)) {
			t.Fatalf("flat DB %d has %d entries, want %d", i, count, len(want))
		}
		seen := make(map[string]bool, len(want))
		for range count {
			var lengths [2]uint64
			requireSnapshotSuccess(t, binary.Read(reader, binary.LittleEndian, &lengths))
			remaining := uint64(reader.Len())
			if lengths[0] > remaining || lengths[1] > remaining-lengths[0] {
				t.Fatalf("flat DB %d entry does not fit", i)
			}
			keyBytes, value := make([]byte, lengths[0]), make([]byte, lengths[1])
			_, err := io.ReadFull(reader, keyBytes)
			requireSnapshotSuccess(t, err)
			_, err = io.ReadFull(reader, value)
			requireSnapshotSuccess(t, err)
			key := string(keyBytes)
			if expectedValue, ok := want[key]; !ok || seen[key] || !bytes.Equal(expectedValue, value) {
				t.Fatalf("flat DB %d has wrong or duplicate entry for key %q", i, key)
			}
			seen[key] = true
		}
	}
	if reader.Len() != 0 {
		t.Fatalf("flat file contains %d trailing bytes", reader.Len())
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
