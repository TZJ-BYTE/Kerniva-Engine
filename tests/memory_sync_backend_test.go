package tests

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/TZJ-BYTE/Kerniva/config"
)

func TestRustMemorySyncBackendAcknowledgedBatchesSurviveRestart(t *testing.T) {
	for _, readDuringSync := range []bool{false, true} {
		t.Run("read-during-sync="+strconv.FormatBool(readDuringSync), func(t *testing.T) {
			cfg := rustMemoryNetworkConfig("std", 1<<20)
			cfg.StorageBackend = config.StorageRustMemorySync
			cfg.PersistenceEnabled = true
			cfg.PersistenceDurability = "sync"
			cfg.PersistenceWriteMode = "strong"
			cfg.RustMemoryReadDuringSync = readDuringSync
			cfg.DataDir = t.TempDir()

			// A previous legacy database must remain separate and untouched.
			legacyDirectory := filepath.Join(cfg.DataDir, "db_0")
			if err := os.MkdirAll(legacyDirectory, 0o750); err != nil {
				t.Fatal(err)
			}
			marker := filepath.Join(legacyDirectory, "legacy-sentinel")
			if err := os.WriteFile(marker, []byte("legacy-data"), 0o600); err != nil {
				t.Fatal(err)
			}
			address, stop := startPersistentTestServer(t, cfg)
			client := newMemoryNetworkClient(t, address)
			info := memoryMustReply(t, client, "INFO")
			for key, want := range map[string]string{
				"storage_backend": "rust-memory-sync", "mode": "memory-sync",
				"persistence_enabled": "true", "memory_abi_version": "1",
				"db_id": "0", "db_count": "2",
				"memory_read_during_sync": strconv.FormatBool(readDuringSync),
			} {
				if got := memoryInfoField(info.Text, key); got != want {
					t.Fatalf("INFO %s = %q, want %q", key, got, want)
				}
			}
			memoryExpectReply(t, memoryMustReply(t, client, "PING"), '+', "PONG")
			binaryKey, binaryValue := "\x00key\xff", "\xffvalue\r\n\x00"
			memoryExpectReply(t, memoryMustReply(t, client, "MSET",
				"left", "zero", "right", "zero", "guard", "original",
				binaryKey, binaryValue, "", "", "erased", "temporary"), '+', "OK")
			memoryExpectReply(t, memoryMustReply(t, client, "DEL", "erased"), ':', "1")
			memoryExpectReply(t, memoryMustReply(t, client, "SELECT", "1"), '+', "OK")
			memoryExpectMissing(t, memoryMustReply(t, client, "GET", "left"))
			memoryExpectReply(t, memoryMustReply(t, client, "MSET", "left", "one", "right", "one"), '+', "OK")
			memoryExpectReply(t, memoryMustReply(t, client, "SELECT", "0"), '+', "OK")
			for version := 1; version <= 4; version++ {
				value := strconv.Itoa(version)
				memoryExpectReply(t, memoryMustReply(t, client, "MSET", "left", value, "right", value), '+', "OK")
			}
			memoryExpectReply(t, memoryMustReply(t, client, "SET", "ordinary", "acknowledged"), '+', "OK")
			info = memoryMustReply(t, client, "INFO")
			for key, want := range map[string]string{
				"wal_commits": "7", "wal_sync_attempts": "7", "wal_failed_commits": "0",
				"wal_requires_reopen": "0",
			} {
				if got := memoryInfoField(info.Text, key); got != want {
					t.Fatalf("INFO %s = %q, want %q", key, got, want)
				}
			}
			for _, args := range [][]string{
				{"SET", "guard", "changed", "NX"}, {"SET", "guard", "changed", "PX", "1000"},
				{"DEL", "guard", "left"}, {"INCR", "guard"}, {"INCRBY", "guard", "1"},
				{"EXPIRE", "guard", "1"}, {"PTTL", "guard"}, {"EXISTS", "guard"},
				{"HSET", "guard", "field", "changed"}, {"MULTI"}, {"FLUSHALL"},
			} {
				reply := memoryMustReply(t, client, args...)
				if reply.Type != '-' || !strings.Contains(reply.Text, "rust-memory-sync") {
					t.Fatalf("unsupported synchronous command %q = %+v", args, reply)
				}
				memoryExpectReply(t, memoryMustReply(t, client, "GET", "guard"), '$', "original")
			}
			stop()

			address, stop = startPersistentTestServer(t, cfg)
			recovered := newMemoryNetworkClient(t, address)
			reply := memoryMustReply(t, recovered, "MGET", "left", "right", binaryKey, "", "erased", "ordinary")
			if reply.Type != '*' || len(reply.Items) != 6 {
				t.Fatalf("recovered MGET = %+v", reply)
			}
			for index, expected := range []string{"4", "4", binaryValue, "", "", "acknowledged"} {
				if index == 4 {
					memoryExpectMissing(t, reply.Items[index])
				} else {
					memoryExpectReply(t, reply.Items[index], '$', expected)
				}
			}
			memoryExpectReply(t, memoryMustReply(t, recovered, "SELECT", "1"), '+', "OK")
			reply = memoryMustReply(t, recovered, "MGET", "left", "right", "guard")
			if reply.Type != '*' || len(reply.Items) != 3 {
				t.Fatalf("recovered database one MGET = %+v", reply)
			}
			memoryExpectReply(t, reply.Items[0], '$', "one")
			memoryExpectReply(t, reply.Items[1], '$', "one")
			memoryExpectMissing(t, reply.Items[2])
			stop()

			for _, name := range []string{"db_0", "db_1"} {
				path := filepath.Join(cfg.DataDir, "rust-memory-v1", name)
				if info, err := os.Stat(path); err != nil || !info.IsDir() {
					t.Fatalf("synchronous database directory %s = %v", path, err)
				}
			}
			if contents, err := os.ReadFile(marker); err != nil || string(contents) != "legacy-data" {
				t.Fatalf("legacy path changed: %q, error=%v", contents, err)
			}
		})
	}
}

func TestRustMemorySyncBackendConfiguration(t *testing.T) {
	t.Setenv("KERNIVA_RUST_MEMORY_READ_DURING_SYNC", "true")
	if !config.DefaultConfig().RustMemoryReadDuringSync {
		t.Fatal("read-during-sync environment override was ignored")
	}
	cfg := rustMemoryNetworkConfig("std", 1<<20)
	cfg.StorageBackend = config.StorageRustMemorySync
	cfg.PersistenceEnabled = true
	cfg.PersistenceDurability = "sync"
	cfg.PersistenceWriteMode = "strong"
	cfg.DataDir = t.TempDir()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("valid synchronous configuration: %v", err)
	}
	for _, example := range []struct {
		name string
		edit func(*config.Config)
	}{
		{"disabled-persistence", func(c *config.Config) { c.PersistenceEnabled = false }},
		{"unsynchronized", func(c *config.Config) { c.PersistenceDurability = "none" }},
		{"weak-write", func(c *config.Config) { c.PersistenceWriteMode = "weak" }},
		{"eviction", func(c *config.Config) { c.MaxMemoryPolicy = "allkeys-lru" }},
		{"missing-directory", func(c *config.Config) { c.DataDir = "" }},
		{"invalid-shards", func(c *config.Config) { c.RustMemoryShards = 3 }},
	} {
		t.Run(example.name, func(t *testing.T) {
			invalid := *cfg
			example.edit(&invalid)
			if err := invalid.Validate(); err == nil {
				t.Fatal("accepted configuration incompatible with synchronous persistence")
			}
		})
	}
}
