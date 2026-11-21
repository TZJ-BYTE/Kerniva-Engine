package database

import (
	"testing"

	"github.com/TZJ-BYTE/Kerniva/internal/datastruct"
)

func TestColdStartStrategy_LoadAllVersusNoLoad(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultEngineOptions()

	db1, err := NewDatabaseWithConfig(0, &DatabaseConfig{
		Type:              Persistent,
		DataDir:           dir,
		Options:           opts,
		ColdStartStrategy: "no_load",
		WriteMode:         "strong",
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db1.Set("k1", &datastruct.DataValue{Value: &datastruct.String{Data: "v1"}}); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := db1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	db2, err := NewDatabaseWithConfig(0, &DatabaseConfig{
		Type:              Persistent,
		DataDir:           dir,
		Options:           opts,
		ColdStartStrategy: "load_all",
		WriteMode:         "strong",
	})
	if err != nil {
		t.Fatalf("reopen(load_all): %v", err)
	}
	stats2 := db2.GetStats()
	if stats2["memory_keys"].(int) == 0 {
		t.Fatalf("expected preload keys on load_all")
	}
	_ = db2.Close()

	db3, err := NewDatabaseWithConfig(0, &DatabaseConfig{
		Type:              Persistent,
		DataDir:           dir,
		Options:           opts,
		ColdStartStrategy: "no_load",
		WriteMode:         "strong",
	})
	if err != nil {
		t.Fatalf("reopen(no_load): %v", err)
	}
	stats3 := db3.GetStats()
	if stats3["memory_keys"].(int) != 0 {
		t.Fatalf("expected no preload keys on no_load")
	}
	_ = db3.Close()
}
