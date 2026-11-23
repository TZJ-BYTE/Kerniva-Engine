package rustengine

import (
	"bytes"
	"errors"
	"sync"
	"testing"
)

func TestMemoryEngineBinaryValuesAndCallerBuffer(t *testing.T) {
	engine, err := OpenMemory(8, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	key := []byte{0, 255}
	value := []byte{0, '\r', '\n', 255}
	if err := engine.Put(key, value); err != nil {
		t.Fatal(err)
	}
	needed, found, err := engine.GetInto(key, make([]byte, 1))
	if !errors.Is(err, ErrMemoryBufferTooSmall) || !found || needed != len(value) {
		t.Fatalf("short read: needed=%d found=%v error=%v", needed, found, err)
	}
	out := make([]byte, needed)
	needed, found, err = engine.GetInto(key, out)
	if err != nil || !found || !bytes.Equal(out[:needed], value) {
		t.Fatalf("binary read: needed=%d found=%v value=%v error=%v", needed, found, out, err)
	}
	overlapping := make([]byte, len(value)+1)
	copy(overlapping, key)
	if _, _, err := engine.GetInto(overlapping[:len(key)], overlapping[1:]); err == nil {
		t.Fatal("GET accepted overlapping key and output")
	}
	if _, err := engine.GetManyInto([][]byte{overlapping[:len(key)]}, overlapping[1:], make([]MemorySpan, 1)); err == nil {
		t.Fatal("MGET accepted overlapping key and output")
	}
	if err := engine.Put(nil, nil); err != nil {
		t.Fatal(err)
	}
	if n, found, err := engine.GetInto(nil, nil); err != nil || !found || n != 0 {
		t.Fatalf("empty key/value: needed=%d found=%v error=%v", n, found, err)
	}
	if n, found, err := engine.GetInto([]byte("missing"), nil); err != nil || found || n != 0 {
		t.Fatalf("missing key: needed=%d found=%v error=%v", n, found, err)
	}
	if deleted, err := engine.Delete(key); err != nil || !deleted {
		t.Fatalf("delete present: deleted=%v error=%v", deleted, err)
	}
	if deleted, err := engine.Delete(key); err != nil || deleted {
		t.Fatalf("delete missing: deleted=%v error=%v", deleted, err)
	}
	stats, err := engine.Stats()
	if err != nil || stats.Keys != 1 || stats.PayloadBytes != 0 || stats.Shards != 8 {
		t.Fatalf("stats after deletion: %+v error=%v", stats, err)
	}
}

func TestMemoryEngineBatchBudgetAndMGet(t *testing.T) {
	limit := uint64(8)
	engine, err := OpenMemory(4, &limit)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	if err := engine.Put([]byte("a"), []byte("old")); err != nil {
		t.Fatal(err)
	}
	err = engine.MSet([]MemoryPair{
		{Key: []byte("a"), Value: []byte("new")},
		{Key: []byte("b"), Value: []byte("too large")},
	})
	if !errors.Is(err, ErrMemoryPayloadLimit) {
		t.Fatalf("over-budget batch: %v", err)
	}
	out := make([]byte, 8)
	n, found, err := engine.GetInto([]byte("a"), out)
	if err != nil || !found || string(out[:n]) != "old" {
		t.Fatalf("rejected batch changed old value: %q found=%v error=%v", out[:n], found, err)
	}
	if _, found, err := engine.GetInto([]byte("b"), out); err != nil || found {
		t.Fatalf("rejected batch published a prefix: found=%v error=%v", found, err)
	}
	if err := engine.MSet([]MemoryPair{
		{Key: []byte("a"), Value: []byte("x")},
		{Key: []byte("b"), Value: []byte("yz")},
	}); err != nil {
		t.Fatal(err)
	}
	keys := [][]byte{[]byte("a"), []byte("missing"), []byte("b"), []byte("a")}
	spans := make([]MemorySpan, len(keys))
	if _, err := engine.GetManyInto(keys, out, spans[:1]); err == nil {
		t.Fatal("accepted insufficient span storage")
	}
	needed, err := engine.GetManyInto(keys, nil, spans)
	if !errors.Is(err, ErrMemoryBufferTooSmall) || needed != 4 {
		t.Fatalf("short MGET: needed=%d error=%v", needed, err)
	}
	needed, err = engine.GetManyInto(keys, out, spans)
	if err != nil || needed != 4 {
		t.Fatalf("MGET: needed=%d error=%v", needed, err)
	}
	for i, expected := range []string{"x", "", "yz", "x"} {
		span := spans[i]
		if i == 1 {
			if span.Found != 0 {
				t.Fatalf("missing span: %+v", span)
			}
			continue
		}
		if span.Found != 1 || span.Offset > uintptr(needed) || span.Len > uintptr(needed)-span.Offset {
			t.Fatalf("invalid span %d: %+v", i, span)
		}
		if actual := string(out[span.Offset : span.Offset+span.Len]); actual != expected {
			t.Fatalf("span %d: got %q want %q", i, actual, expected)
		}
	}
	stats, err := engine.Stats()
	if err != nil || stats.Keys != 2 || stats.PayloadBytes != 5 {
		t.Fatalf("batch stats: %+v error=%v", stats, err)
	}
}

func TestMemoryEngineZeroBudget(t *testing.T) {
	limit := uint64(0)
	engine, err := OpenMemory(1, &limit)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	if err := engine.Put(nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := engine.Put([]byte("key"), nil); !errors.Is(err, ErrMemoryPayloadLimit) {
		t.Fatalf("zero budget accepted payload: %v", err)
	}
}

func TestMemoryEngineCloseConcurrentWithCalls(t *testing.T) {
	engine, err := OpenMemory(8, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Put([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	var workers sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			dst := make([]byte, 16)
			for attempt := 0; attempt < 200; attempt++ {
				_, _, err := engine.GetInto([]byte("key"), dst)
				if errors.Is(err, errMemoryEngineClosed) {
					return
				}
				if err != nil {
					t.Errorf("concurrent GET: %v", err)
					return
				}
				if err := engine.Put([]byte("key"), []byte("value")); err != nil {
					if !errors.Is(err, errMemoryEngineClosed) {
						t.Errorf("concurrent PUT: %v", err)
					}
					return
				}
			}
		}()
	}
	close(start)
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	workers.Wait()
	if err := engine.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	if _, _, err := engine.GetInto(nil, nil); !errors.Is(err, errMemoryEngineClosed) {
		t.Fatalf("read after close: %v", err)
	}
	if _, err := engine.Stats(); !errors.Is(err, errMemoryEngineClosed) {
		t.Fatalf("stats after close: %v", err)
	}
}
