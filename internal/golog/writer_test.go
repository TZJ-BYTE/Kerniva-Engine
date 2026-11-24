package golog

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/TZJ-BYTE/Kerniva/internal/rustengine"
)

func emptyRecovery(string, func([]byte, []byte) error) (rustengine.GoLogRecovery, error) {
	return rustengine.GoLogRecovery{}, nil
}

func openTestWriter(t *testing.T, directory string, opts Options) *Writer {
	t.Helper()
	w, err := openWithRecovery(directory, opts, nil, emptyRecovery)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = w.Close()
	})
	return w
}

func requireFile(t *testing.T, path string) []byte {
	t.Helper()
	value, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func TestFrameEncoding(t *testing.T) {
	ops := []rustengine.WriteOp{
		{Key: []byte{'a', 0}, Value: []byte{42}},
		{Key: []byte("b"), Value: []byte("ignored"), Delete: true},
	}
	frame, err := encodeFrame(nil, 1, ops, false)
	if err != nil {
		t.Fatal(err)
	}
	expected := []byte{
		1, 0, 0, 0, 0, 0, 0, 0, // sequence
		2, 0, 0, 0, // operation count
		1, 2, 0, 0, 0, 1, 0, 0, 0, 'a', 0, 42, // put
		2, 1, 0, 0, 0, 0, 0, 0, 0, 'b', // delete
	}
	if got := frame[8:]; !bytes.Equal(got, expected) {
		t.Fatalf("payload = %x, expected %x", got, expected)
	}
	length := binary.LittleEndian.Uint32(frame[:4])
	crc := binary.LittleEndian.Uint32(frame[4:8])
	if int(length) != len(expected) || crc != crc32.ChecksumIEEE(expected) {
		t.Fatalf("wrong frame header: size=%d crc=%08x", length, crc)
	}
	reset, err := encodeFrame(frame, 2, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(reset) != 8+12+9 || reset[20] != 3 {
		t.Fatalf("invalid reset frame %x", reset)
	}
}

func TestWriterExcludesSecondOwnerAndSealsBySize(t *testing.T) {
	directory := t.TempDir()
	w := openTestWriter(t, directory, Options{SegmentBytes: 50, Sync: true})
	second, err := openWithRecovery(directory, Options{}, nil, emptyRecovery)
	if err == nil {
		_ = second.Close()
		t.Fatal("second writer acquired the same directory")
	}
	if err := w.Put([]byte("key"), []byte("first")); err != nil {
		t.Fatal(err)
	}
	if err := w.Put([]byte("key"), []byte("second")); err != nil {
		t.Fatal(err)
	}
	firstFrame := requireFile(t, filepath.Join(directory, "log-00000000000000000001-00000000000000000001.seg"))
	active := requireFile(t, filepath.Join(directory, activeName))
	if !bytes.HasPrefix(firstFrame, []byte(logMagic)) || !bytes.HasSuffix(firstFrame, []byte("first")) || !bytes.HasSuffix(active, []byte("second")) {
		t.Fatalf("invalid sealed/active data: %x / %x", firstFrame, active)
	}
	stats := w.Stats()
	if stats.LastSequence != 2 || stats.SyncCount != 2 || stats.ActiveBytes != uint64(len(active)) || stats.SealedSegments != 1 || stats.SealedBytes != uint64(len(firstFrame)) {
		t.Fatalf("unexpected writer stats: %+v", stats)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	sealedLast := requireFile(t, filepath.Join(directory, "log-00000000000000000002-00000000000000000002.seg"))
	if !bytes.Equal(sealedLast, active) {
		t.Fatal("graceful close changed the last active segment")
	}
	if err := w.Reset(); err == nil {
		t.Fatal("closed writer accepted reset")
	}
}

func TestReopenTruncatesUnconfirmedTail(t *testing.T) {
	directory := t.TempDir()
	first, err := encodeFrame(nil, 1, []rustengine.WriteOp{{Key: []byte("key"), Value: []byte("original")}}, false)
	if err != nil {
		t.Fatal(err)
	}
	activePath := filepath.Join(directory, activeName)
	prefix := append([]byte(logMagic), first...)
	if err := os.WriteFile(activePath, append(bytes.Clone(prefix), []byte("unconfirmed-tail")...), 0o600); err != nil {
		t.Fatal(err)
	}
	recovered := func(string, func([]byte, []byte) error) (rustengine.GoLogRecovery, error) {
		return rustengine.GoLogRecovery{LastSequence: 1, ActiveEnd: uint64(len(prefix))}, nil
	}
	reopened, err := openWithRecovery(directory, Options{}, nil, recovered)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	if got := requireFile(t, activePath); !bytes.Equal(got, prefix) {
		t.Fatalf("active prefix was changed: %x", got)
	}
	if err := reopened.Delete([]byte("key")); err != nil {
		t.Fatal(err)
	}
	newFrame := requireFile(t, activePath)[len(prefix):]
	if sequence := binary.LittleEndian.Uint64(newFrame[8:16]); sequence != 2 {
		t.Fatalf("reopened writer used sequence %d", sequence)
	}
	if newFrame[20] != 2 {
		t.Fatalf("reopened writer used record type %d", newFrame[20])
	}
}

func TestAgeSealsWithoutAdditionalWrites(t *testing.T) {
	directory := t.TempDir()
	w := openTestWriter(t, directory, Options{MaxAge: 20 * time.Millisecond})
	if err := w.Reset(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		stats := w.Stats()
		if stats.Fault != "" {
			t.Fatalf("age sealing failed: %s", stats.Fault)
		}
		if stats.SealedSegments == 1 && stats.ActiveBytes == uint64(len(logMagic)) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("age worker did not seal: %+v", stats)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestAppendFailurePoisonsWriterWhenRollbackFails(t *testing.T) {
	w := openTestWriter(t, t.TempDir(), Options{Sync: true})
	if err := w.Put([]byte("first"), []byte("complete")); err != nil {
		t.Fatal(err)
	}
	if err := w.active.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Put([]byte("second"), []byte("rejected")); err == nil {
		t.Fatal("invalid active descriptor accepted a write")
	}
	fail := w.Stats()
	if fail.LastSequence != 1 || fail.SyncCount != 1 || fail.Fault == "" {
		t.Fatalf("writer advanced sequence on failure: %+v", fail)
	}
	if err := w.Reset(); err == nil || err.Error() != fail.Fault {
		t.Fatalf("writer did not preserve the original poison error: %v", err)
	}
}

func TestPartialActiveHeaderRepairRequiresMagicPrefix(t *testing.T) {
	for _, tc := range []struct {
		name   string
		header string
		repair bool
	}{
		{name: "empty", repair: true},
		{name: "magic prefix", header: "KGLL", repair: true},
		{name: "foreign header", header: "wrong"},
		{name: "complete header", header: logMagic},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, activeName)
			if err := os.WriteFile(path, []byte(tc.header), 0o600); err != nil {
				t.Fatal(err)
			}
			w, err := openWithRecovery(dir, Options{}, nil, emptyRecovery)
			if !tc.repair {
				if err == nil {
					_ = w.Close()
					t.Fatal("conflicting file was overwritten")
				}
				if got := requireFile(t, path); string(got) != tc.header {
					t.Fatalf("conflicting file changed: %x", got)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			if got := requireFile(t, path); string(got) != logMagic {
				t.Fatalf("active file was not repaired: %x", got)
			}
		})
	}
}
