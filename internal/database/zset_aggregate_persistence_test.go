package database

import (
	"reflect"
	"testing"

	"github.com/TZJ-BYTE/Kerniva/internal/datastruct"
)

func TestLazyLoadZSetAggregateCommandsAfterRestart(t *testing.T) {
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
	if _, err := db1.ZAdd("z1", []datastruct.ZSetMember{
		{Member: "a", Score: 1},
		{Member: "b", Score: 2},
		{Member: "c", Score: 3},
	}); err != nil {
		t.Fatalf("seed z1: %v", err)
	}
	if _, err := db1.ZAdd("z2", []datastruct.ZSetMember{
		{Member: "b", Score: 4},
		{Member: "c", Score: 1},
		{Member: "d", Score: 2},
	}); err != nil {
		t.Fatalf("seed z2: %v", err)
	}
	if _, err := db1.ZAdd("z3", []datastruct.ZSetMember{
		{Member: "b", Score: 1},
		{Member: "c", Score: 5},
		{Member: "e", Score: 6},
	}); err != nil {
		t.Fatalf("seed z3: %v", err)
	}
	if err := db1.Close(); err != nil {
		t.Fatalf("close db1: %v", err)
	}

	db2, err := NewDatabaseWithConfig(0, cfg)
	if err != nil {
		t.Fatalf("open db2: %v", err)
	}

	unionMembers, err := db2.ZUnion([]string{"z1", "z2"}, []float64{1, 1}, "SUM")
	if err != nil {
		t.Fatalf("lazy zunion: %v", err)
	}
	assertZSetMembers(t, unionMembers, []datastruct.ZSetMember{
		{Member: "a", Score: 1},
		{Member: "d", Score: 2},
		{Member: "c", Score: 4},
		{Member: "b", Score: 6},
	})

	interMembers, err := db2.ZInter([]string{"z1", "z2", "z3"}, []float64{1, 2, 3}, "MAX")
	if err != nil {
		t.Fatalf("lazy zinter: %v", err)
	}
	assertZSetMembers(t, interMembers, []datastruct.ZSetMember{
		{Member: "b", Score: 8},
		{Member: "c", Score: 15},
	})

	diffMembers, err := db2.ZDiff([]string{"z1", "z2"})
	if err != nil {
		t.Fatalf("lazy zdiff: %v", err)
	}
	assertZSetMembers(t, diffMembers, []datastruct.ZSetMember{
		{Member: "a", Score: 1},
	})

	if n, err := db2.ZUnionStore("zu:dst", []string{"z1", "z2"}, []float64{2, 1}, "MIN"); err != nil || n != 4 {
		t.Fatalf("lazy zunionstore: n=%d err=%v", n, err)
	}
	if n, err := db2.ZInterStore("zi:dst", []string{"z1", "z2"}, []float64{2, 1}, "MIN"); err != nil || n != 2 {
		t.Fatalf("lazy zinterstore: n=%d err=%v", n, err)
	}
	if n, err := db2.ZDiffStore("zd:dst", []string{"z1", "z2"}); err != nil || n != 1 {
		t.Fatalf("lazy zdiffstore: n=%d err=%v", n, err)
	}
	if err := db2.Close(); err != nil {
		t.Fatalf("close db2: %v", err)
	}

	db3, err := NewDatabaseWithConfig(0, cfg)
	if err != nil {
		t.Fatalf("open db3: %v", err)
	}
	defer db3.Close()

	unionStored, err := db3.ZRange("zu:dst", 0, -1, false, true)
	if err != nil {
		t.Fatalf("reload zunionstore: %v", err)
	}
	assertZSetPairs(t, unionStored, []string{"c", "1", "a", "2", "d", "2", "b", "4"})

	interStored, err := db3.ZRange("zi:dst", 0, -1, false, true)
	if err != nil {
		t.Fatalf("reload zinterstore: %v", err)
	}
	assertZSetPairs(t, interStored, []string{"c", "1", "b", "4"})

	diffStored, err := db3.ZRange("zd:dst", 0, -1, false, true)
	if err != nil {
		t.Fatalf("reload zdiffstore: %v", err)
	}
	assertZSetPairs(t, diffStored, []string{"a", "1"})
}

func assertZSetMembers(t *testing.T, got, expected []datastruct.ZSetMember) {
	t.Helper()
	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("unexpected zset members=%v expected=%v", got, expected)
	}
}

func assertZSetPairs(t *testing.T, got, expected []string) {
	t.Helper()
	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("unexpected zset range=%v expected=%v", got, expected)
	}
}
