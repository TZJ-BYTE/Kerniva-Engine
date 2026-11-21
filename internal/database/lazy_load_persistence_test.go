package database

import (
	"sort"
	"testing"
	"time"

	"github.com/TZJ-BYTE/Kerniva/internal/datastruct"
)

func TestLazyLoadStringMetaAndMutationCommands(t *testing.T) {
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

	ok, err := db1.SetStringBytesWithOptions([]byte("alpha"), []byte("v1"), 60_000, false, false, false)
	if err != nil || !ok {
		t.Fatalf("seed alpha: ok=%v err=%v", ok, err)
	}
	if err := db1.SetStringBytes([]byte("beta"), []byte("v2")); err != nil {
		t.Fatalf("seed beta: %v", err)
	}
	if err := db1.Close(); err != nil {
		t.Fatalf("close db1: %v", err)
	}

	db2, err := NewDatabaseWithConfig(0, cfg)
	if err != nil {
		t.Fatalf("open db2: %v", err)
	}
	defer db2.Close()

	if !db2.Exists("alpha") {
		t.Fatalf("expected alpha to exist after reopen")
	}
	if expireAt, exists := db2.GetExpireTime("alpha"); !exists || expireAt <= time.Now().UnixMilli() {
		t.Fatalf("expected alpha ttl to survive reopen, exists=%v expireAt=%d", exists, expireAt)
	}

	ok, err = db2.SetStringBytesWithOptions([]byte("alpha"), []byte("nx-blocked"), 0, false, true, false)
	if err != nil {
		t.Fatalf("set alpha nx: %v", err)
	}
	if ok {
		t.Fatalf("expected NX to reject persisted key")
	}

	ok, err = db2.SetStringBytesWithOptions([]byte("alpha"), []byte("v1-updated"), 0, true, false, true)
	if err != nil || !ok {
		t.Fatalf("set alpha xx keepttl: ok=%v err=%v", ok, err)
	}

	if persisted, err := db2.Persist("alpha"); err != nil || !persisted {
		t.Fatalf("persist alpha: persisted=%v err=%v", persisted, err)
	}
	if expireAt, exists := db2.GetExpireTime("alpha"); !exists || expireAt != 0 {
		t.Fatalf("expected alpha ttl removed, exists=%v expireAt=%d", exists, expireAt)
	}

	if renamed, err := db2.Rename("beta", "gamma", false); err != nil || !renamed {
		t.Fatalf("rename beta->gamma: renamed=%v err=%v", renamed, err)
	}
	if _, found := db2.Get("gamma"); !found {
		t.Fatalf("expected renamed key gamma to exist")
	}

	if deleted, err := db2.DeleteWithError("gamma"); err != nil || !deleted {
		t.Fatalf("delete gamma: deleted=%v err=%v", deleted, err)
	}
	if db2.Exists("gamma") {
		t.Fatalf("expected gamma deleted from reopened database")
	}
}

func TestLazyLoadCollectionsAndKeyEnumeration(t *testing.T) {
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

	if _, err := db1.HSet("hash", "f1", "v1"); err != nil {
		t.Fatalf("seed hash: %v", err)
	}
	if _, err := db1.LPush("list", []string{"a", "b"}); err != nil {
		t.Fatalf("seed list: %v", err)
	}
	if _, err := db1.SAdd("set", []string{"x", "y"}); err != nil {
		t.Fatalf("seed set: %v", err)
	}
	if _, err := db1.SAdd("set3", []string{"x", "z"}); err != nil {
		t.Fatalf("seed set3: %v", err)
	}
	if _, err := db1.ZAdd("zset", []datastruct.ZSetMember{
		{Member: "m1", Score: 1},
		{Member: "m2", Score: 2},
	}); err != nil {
		t.Fatalf("seed zset: %v", err)
	}
	if _, err := db1.LPush("single", []string{"only"}); err != nil {
		t.Fatalf("seed single list: %v", err)
	}
	if err := db1.SetStringBytes([]byte("plain"), []byte("value")); err != nil {
		t.Fatalf("seed plain: %v", err)
	}
	if err := db1.Close(); err != nil {
		t.Fatalf("close db1: %v", err)
	}

	db2, err := NewDatabaseWithConfig(0, cfg)
	if err != nil {
		t.Fatalf("open db2: %v", err)
	}
	defer db2.Close()

	if got, ok, err := db2.HGet("hash", "f1"); err != nil || !ok || got != "v1" {
		t.Fatalf("lazy hget: got=%q ok=%v err=%v", got, ok, err)
	}
	if n, err := db2.LLen("list"); err != nil || n != 2 {
		t.Fatalf("lazy llen: n=%d err=%v", n, err)
	}
	if n, err := db2.LPush("list", []string{"c"}); err != nil || n != 3 {
		t.Fatalf("lazy lpush existing list: n=%d err=%v", n, err)
	}
	if ok, err := db2.SIsMember("set", "x"); err != nil || !ok {
		t.Fatalf("lazy sismember: ok=%v err=%v", ok, err)
	}
	if cursor, members, err := db2.SScan("set", 0, "x", 10); err != nil || cursor != 0 || len(members) != 1 || members[0] != "x" {
		t.Fatalf("lazy sscan: cursor=%d members=%v err=%v", cursor, members, err)
	}
	if score, ok, err := db2.ZScore("zset", "m2"); err != nil || !ok || score != 2 {
		t.Fatalf("lazy zscore: score=%v ok=%v err=%v", score, ok, err)
	}
	if scores, err := db2.ZMScore("zset", []string{"m1", "missing"}); err != nil || scores[0] == nil || *scores[0] != 1 || scores[1] != nil {
		t.Fatalf("lazy zmscore: scores=%v err=%v", scores, err)
	}
	if rank, ok, err := db2.ZRank("zset", "m1", false); err != nil || !ok || rank != 0 {
		t.Fatalf("lazy zrank: rank=%d ok=%v err=%v", rank, ok, err)
	}
	if member, score, ok, err := db2.ZRandMember("zset"); err != nil || !ok || (member != "m1" && member != "m2") || (score != 1 && score != 2) {
		t.Fatalf("lazy zrandmember: member=%q score=%v ok=%v err=%v", member, score, ok, err)
	}
	if n, err := db2.SInterStore("set:dst", []string{"set", "set3"}); err != nil || n != 1 {
		t.Fatalf("lazy sinterstore: n=%d err=%v", n, err)
	}
	if card, err := db2.SCard("set:dst"); err != nil || card != 1 {
		t.Fatalf("lazy scard set:dst: card=%d err=%v", card, err)
	}
	if moved, err := db2.SMove("set", "set2", "x"); err != nil || !moved {
		t.Fatalf("lazy smove: moved=%v err=%v", moved, err)
	}
	if card, err := db2.SCard("set2"); err != nil || card != 1 {
		t.Fatalf("lazy scard set2: card=%d err=%v", card, err)
	}
	if removed, err := db2.ZRemRangeByRank("zset", 0, 0); err != nil || removed != 1 {
		t.Fatalf("lazy zremrangebyrank: removed=%d err=%v", removed, err)
	}
	if v, ok, err := db2.LPop("single"); err != nil || !ok || v != "only" {
		t.Fatalf("lazy lpop single: v=%q ok=%v err=%v", v, ok, err)
	}
	if db2.Exists("single") {
		t.Fatalf("expected single list key removed after last pop")
	}

	keys := db2.Keys()
	sort.Strings(keys)
	expected := []string{"hash", "list", "plain", "set", "set2", "set3", "set:dst", "zset"}
	for i := range expected {
		if i >= len(keys) || keys[i] != expected[i] {
			t.Fatalf("keys mismatch: got=%v want=%v", keys, expected)
		}
	}
	if size := db2.Size(); size != 8 {
		t.Fatalf("expected size 8 after reopen, got %d", size)
	}
}

func TestLazyLoadListExtensionCommandsAfterRestart(t *testing.T) {
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
	if _, err := db1.RPush("movesrc", []string{"a", "b", "a", "c", "a"}); err != nil {
		t.Fatalf("seed movesrc: %v", err)
	}
	if err := db1.Close(); err != nil {
		t.Fatalf("close db1: %v", err)
	}

	db2, err := NewDatabaseWithConfig(0, cfg)
	if err != nil {
		t.Fatalf("open db2: %v", err)
	}

	if n, err := db2.LPushX("movesrc", []string{"z"}); err != nil || n != 6 {
		t.Fatalf("lazy lpushx: n=%d err=%v", n, err)
	}
	if n, err := db2.RPushX("missing", []string{"x"}); err != nil || n != 0 {
		t.Fatalf("lazy rpushx missing: n=%d err=%v", n, err)
	}
	if positions, err := db2.LPos("movesrc", "a", 2, 0, 0); err != nil || len(positions) != 2 || positions[0] != 3 || positions[1] != 5 {
		t.Fatalf("lazy lpos: positions=%v err=%v", positions, err)
	}
	if moved, ok, err := db2.LMove("movesrc", "movedst", false, true); err != nil || !ok || moved != "a" {
		t.Fatalf("lazy lmove: moved=%q ok=%v err=%v", moved, ok, err)
	}
	if removed, err := db2.LRem("movesrc", -1, "a"); err != nil || removed != 1 {
		t.Fatalf("lazy lrem: removed=%d err=%v", removed, err)
	}
	if err := db2.Close(); err != nil {
		t.Fatalf("close db2: %v", err)
	}

	db3, err := NewDatabaseWithConfig(0, cfg)
	if err != nil {
		t.Fatalf("open db3: %v", err)
	}
	defer db3.Close()

	if values, err := db3.LRange("movesrc", 0, -1); err != nil || len(values) != 4 || values[0] != "z" || values[1] != "a" || values[2] != "b" || values[3] != "c" {
		t.Fatalf("persisted movesrc mismatch: values=%v err=%v", values, err)
	}
	if moved, ok, err := db3.LIndex("movedst", 0); err != nil || !ok || moved != "a" {
		t.Fatalf("persisted movedst mismatch: moved=%q ok=%v err=%v", moved, ok, err)
	}
}

func TestLazyLoadSetStoreCommandsAfterRestart(t *testing.T) {
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
	if _, err := db1.SAdd("set1", []string{"a", "b"}); err != nil {
		t.Fatalf("seed set1: %v", err)
	}
	if _, err := db1.SAdd("set2", []string{"b", "c"}); err != nil {
		t.Fatalf("seed set2: %v", err)
	}
	if _, err := db1.SAdd("set:gone", []string{"stale"}); err != nil {
		t.Fatalf("seed set:gone: %v", err)
	}
	if err := db1.Close(); err != nil {
		t.Fatalf("close db1: %v", err)
	}

	db2, err := NewDatabaseWithConfig(0, cfg)
	if err != nil {
		t.Fatalf("open db2: %v", err)
	}
	if n, err := db2.SUnionStore("set:union", []string{"set1", "set2"}); err != nil || n != 3 {
		t.Fatalf("lazy sunionstore: n=%d err=%v", n, err)
	}
	if n, err := db2.SDiffStore("set:diff", []string{"set1", "set2"}); err != nil || n != 1 {
		t.Fatalf("lazy sdiffstore: n=%d err=%v", n, err)
	}
	if n, err := db2.SDiffStore("set:gone", []string{"set1", "set1"}); err != nil || n != 0 {
		t.Fatalf("lazy sdiffstore empty: n=%d err=%v", n, err)
	}
	if card, err := db2.SInterCard([]string{"set1", "set2"}, 0); err != nil || card != 1 {
		t.Fatalf("lazy sintercard: card=%d err=%v", card, err)
	}
	if card, err := db2.SInterCard([]string{"set1", "set2"}, 1); err != nil || card != 1 {
		t.Fatalf("lazy sintercard limit: card=%d err=%v", card, err)
	}
	if err := db2.Close(); err != nil {
		t.Fatalf("close db2: %v", err)
	}

	db3, err := NewDatabaseWithConfig(0, cfg)
	if err != nil {
		t.Fatalf("open db3: %v", err)
	}
	defer db3.Close()

	if members, err := db3.SMembers("set:union"); err != nil || len(members) != 3 || members[0] != "a" || members[1] != "b" || members[2] != "c" {
		t.Fatalf("persisted set:union mismatch: members=%v err=%v", members, err)
	}
	if members, err := db3.SMembers("set:diff"); err != nil || len(members) != 1 || members[0] != "a" {
		t.Fatalf("persisted set:diff mismatch: members=%v err=%v", members, err)
	}
	if db3.Exists("set:gone") {
		t.Fatalf("expected set:gone deleted after empty store result")
	}
}
