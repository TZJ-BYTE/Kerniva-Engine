package rustengine_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/TZJ-BYTE/Kerniva/internal/golog"
	"github.com/TZJ-BYTE/Kerniva/internal/rustengine"
)

func TestRecoverGoLogDeltaFromSealedGoLog(t *testing.T) {
	dir := t.TempDir()
	writeSegment := func(write func(*golog.Writer)) {
		t.Helper()
		writer, err := golog.Open(dir, golog.Options{Sync: true}, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := writer.Close(); err != nil {
				t.Fatal(err)
			}
		}()
		write(writer)
	}
	mustWrite := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	largeKey := []byte{0, 255}
	largeValue := bytes.Repeat([]byte{0, 255}, 1<<19)
	writeSegment(func(writer *golog.Writer) {
		mustWrite(writer.Put([]byte("changed"), []byte("before")))
		mustWrite(writer.Put([]byte("deleted"), []byte("before")))
	})
	writeSegment(func(writer *golog.Writer) {
		mustWrite(writer.Put([]byte("changed"), []byte("intermediate")))
		mustWrite(writer.Put([]byte("changed"), []byte("after")))
		mustWrite(writer.Delete([]byte("deleted")))
		mustWrite(writer.Put(largeKey, largeValue))
	})
	writeSegment(func(writer *golog.Writer) {
		mustWrite(writer.Reset())
		mustWrite(writer.Put([]byte("survives-reset"), nil))
	})

	type change struct {
		value   []byte
		deleted bool
	}
	check := func(ranges []rustengine.GoLogRange, wantReset bool, want map[string]change) {
		t.Helper()
		got := make(map[string]change)
		reset, err := rustengine.RecoverGoLogDelta(dir, 2, ranges, func(key, value []byte, deleted bool) error {
			if _, exists := got[string(key)]; exists {
				t.Fatalf("duplicate change for key %q", key)
			}
			got[string(key)] = change{value, deleted}
			return nil
		})
		if err != nil || reset != wantReset || len(got) != len(want) {
			t.Fatalf("got reset=%t, %d changes, err=%v, want reset=%t and %d changes", reset, len(got), err, wantReset, len(want))
		}
		for key, expected := range want {
			actual, found := got[key]
			if !found || actual.deleted != expected.deleted || !bytes.Equal(actual.value, expected.value) {
				t.Errorf("invalid change for key %q", key)
			}
		}
	}
	second := rustengine.GoLogRange{First: 3, Last: 6}
	third := rustengine.GoLogRange{First: 7, Last: 8}
	check([]rustengine.GoLogRange{second}, false, map[string]change{
		"changed":        {value: []byte("after")},
		"deleted":        {deleted: true},
		string(largeKey): {value: largeValue},
	})
	check([]rustengine.GoLogRange{second, third}, true, map[string]change{"survives-reset": {}})

	wantErr := errors.New("cancel staged changes")
	if reset, err := rustengine.RecoverGoLogDelta(dir, 2, []rustengine.GoLogRange{second, third}, func(_, _ []byte, _ bool) error {
		return wantErr
	}); reset || !errors.Is(err, wantErr) {
		t.Fatalf("got reset=%t err=%v after callback failure, want reset=false and %v", reset, err, wantErr)
	}
	if _, err := rustengine.RecoverGoLogDelta(dir, 2, []rustengine.GoLogRange{third}, nil); err == nil {
		t.Fatal("recovered a noncontiguous range")
	}
}
