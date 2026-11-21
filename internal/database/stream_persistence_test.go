package database

import "testing"

func TestLazyLoadStreamCommandsAfterRestart(t *testing.T) {
	dir := t.TempDir()
	cfg := &DatabaseConfig{
		Type:              Persistent,
		DataDir:           dir,
		Options:           DefaultEngineOptions(),
		ColdStartStrategy: "lazy_load",
		WriteMode:         "strong",
	}

	db1, err := NewDatabaseWithConfig(0, cfg)
	if err != nil {
		t.Fatalf("open db1: %v", err)
	}
	if id, err := db1.XAdd("stream", "1000-0", []string{"f1", "v1"}, -1, false); err != nil || id != "1000-0" {
		t.Fatalf("seed xadd 1: id=%q err=%v", id, err)
	}
	if id, err := db1.XAdd("stream", "1001-0", []string{"f2", "v2"}, -1, false); err != nil || id != "1001-0" {
		t.Fatalf("seed xadd 2: id=%q err=%v", id, err)
	}
	if id, err := db1.XAdd("stream", "1002-0", []string{"f3", "v3"}, -1, false); err != nil || id != "1002-0" {
		t.Fatalf("seed xadd 3: id=%q err=%v", id, err)
	}
	if err := db1.Close(); err != nil {
		t.Fatalf("close db1: %v", err)
	}

	db2, err := NewDatabaseWithConfig(0, cfg)
	if err != nil {
		t.Fatalf("open db2: %v", err)
	}
	if typ := db2.TypeOf("stream"); typ != "stream" {
		t.Fatalf("unexpected type after reopen: %s", typ)
	}
	if n, err := db2.XLen("stream"); err != nil || n != 3 {
		t.Fatalf("xlen after reopen: n=%d err=%v", n, err)
	}
	entries, err := db2.XRange("stream", "-", "+", 0, false)
	if err != nil || len(entries) != 3 || entries[0].ID != "1000-0" || entries[2].ID != "1002-0" {
		t.Fatalf("xrange after reopen: entries=%v err=%v", entries, err)
	}
	reversed, err := db2.XRange("stream", "+", "-", 1, true)
	if err != nil || len(reversed) != 1 || reversed[0].ID != "1002-0" {
		t.Fatalf("xrevrange after reopen: entries=%v err=%v", reversed, err)
	}
	if removed, err := db2.XDel("stream", []string{"1000-0"}); err != nil || removed != 1 {
		t.Fatalf("xdel after reopen: removed=%d err=%v", removed, err)
	}
	if trimmed, err := db2.XTrim("stream", 1); err != nil || trimmed != 1 {
		t.Fatalf("xtrim after reopen: trimmed=%d err=%v", trimmed, err)
	}
	if err := db2.Close(); err != nil {
		t.Fatalf("close db2: %v", err)
	}

	db3, err := NewDatabaseWithConfig(0, cfg)
	if err != nil {
		t.Fatalf("open db3: %v", err)
	}
	defer db3.Close()

	if n, err := db3.XLen("stream"); err != nil || n != 1 {
		t.Fatalf("xlen after second reopen: n=%d err=%v", n, err)
	}
	entries, err = db3.XRange("stream", "-", "+", 0, false)
	if err != nil || len(entries) != 1 || entries[0].ID != "1002-0" {
		t.Fatalf("persisted stream mismatch: entries=%v err=%v", entries, err)
	}
	if id, err := db3.XAdd("stream", "1002-*", []string{"f4", "v4"}, -1, false); err != nil || id != "1002-1" {
		t.Fatalf("xadd after second reopen: id=%q err=%v", id, err)
	}
}
