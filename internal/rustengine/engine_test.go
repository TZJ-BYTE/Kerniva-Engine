package rustengine

import "testing"

func TestEngineRoundTrip(t *testing.T) {
	dir := t.TempDir()
	engine, err := Open(Options{
		DataDir:            dir,
		SegmentSizeBytes:   4 * 1024,
		CheckpointAfterOps: 8,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() {
		if closeErr := engine.Close(); closeErr != nil {
			t.Fatalf("close: %v", closeErr)
		}
	}()

	if err := engine.Put([]byte("alpha"), []byte("one")); err != nil {
		t.Fatalf("put: %v", err)
	}

	value, found, err := engine.Get([]byte("alpha"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !found {
		t.Fatalf("expected key alpha")
	}
	if string(value) != "one" {
		t.Fatalf("expected value one, got %q", string(value))
	}

	all, err := engine.LoadAllKeys()
	if err != nil {
		t.Fatalf("load all: %v", err)
	}
	if string(all["alpha"]) != "one" {
		t.Fatalf("expected alpha in snapshot export")
	}

	if err := engine.Delete([]byte("alpha")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, found, err := engine.Get([]byte("alpha")); err != nil {
		t.Fatalf("get after delete: %v", err)
	} else if found {
		t.Fatalf("expected key alpha to be deleted")
	}
}

func TestEngineBatchSurvivesReopenAndReset(t *testing.T) {
	opts := Options{DataDir: t.TempDir(), SegmentSizeBytes: 4096, CheckpointAfterOps: 100, SyncPolicy: SyncData}
	engine, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Put([]byte("removed"), []byte("old")); err != nil {
		t.Fatal(err)
	}
	if err := engine.WriteBatch([]WriteOp{
		{Key: []byte("alpha"), Value: []byte("one")},
		{Key: []byte("beta"), Value: []byte("two")},
		{Key: []byte("removed"), Delete: true},
	}); err != nil {
		t.Fatal(err)
	}
	if err := engine.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := engine.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.MaybeCompact(); err != nil {
		t.Fatal(err)
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	engine, err = Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	values, err := engine.LoadAllKeys()
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 2 || string(values["alpha"]) != "one" || string(values["beta"]) != "two" {
		t.Fatalf("batch recovery %#v", values)
	}
	if err := engine.Reset(); err != nil {
		t.Fatal(err)
	}
	values, err = engine.LoadAllKeys()
	if err != nil || len(values) != 0 {
		t.Fatalf("reset %#v, %v", values, err)
	}
}
