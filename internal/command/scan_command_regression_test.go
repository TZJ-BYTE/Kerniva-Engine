package command_test

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/TZJ-BYTE/Kerniva/config"
	"github.com/TZJ-BYTE/Kerniva/internal/command"
	"github.com/TZJ-BYTE/Kerniva/internal/database"
	"github.com/TZJ-BYTE/Kerniva/internal/protocol"
)

func TestScanStaticKeyspaceAcrossPages(t *testing.T) {
	for _, backend := range []string{"go-memory", "go-log"} {
		t.Run(backend, func(t *testing.T) {
			cfg := database.DefaultDatabaseConfig()
			if backend == "go-log" {
				cfg.Type = database.Persistent
				cfg.StorageBackend = config.StorageGoLog
				cfg.DataDir = t.TempDir()
				cfg.ColdStartStrategy = "load_all"
				cfg.MaxMemoryPolicy = "noeviction"
			}
			db, err := database.NewDatabaseWithConfig(0, cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { db.Close() })

			expected := make(map[string]bool)
			const keysPerPrefix = 512
			for i := range keysPerPrefix {
				for _, prefix := range []string{"match", "other"} {
					key := fmt.Sprintf("%s:%03d", prefix, i)
					if err := db.SetStringBytes([]byte(key), []byte("value")); err != nil {
						t.Fatalf("seed %q: %v", key, err)
					}
					if prefix == "match" {
						expected[key] = true
					}
				}
			}

			scanner := &command.ScanCommand{}
			const count = 3
			for round := range 4 {
				seen := make(map[string]bool, len(expected))
				duplicates := 0
				cursor := "0"
				for range 2*keysPerPrefix + 1 {
					response := scanner.Execute(db, [][]byte{[]byte(cursor), []byte("MATCH"), []byte("match:*"), []byte("COUNT"), []byte(strconv.Itoa(count))})
					if response.Type != protocol.Array {
						t.Fatalf("SCAN %s: %#v", cursor, response)
					}
					fields := response.Value.([]*protocol.Response)
					keys := fields[1].Value.([]string)
					if len(keys) > count {
						t.Fatalf("SCAN %s returned %d keys, COUNT was %d", cursor, len(keys), count)
					}
					for _, key := range keys {
						if !expected[key] {
							t.Fatalf("round %d, SCAN %s returned unexpected key %q", round, cursor, key)
						}
						if seen[key] {
							duplicates++
						}
						seen[key] = true
					}
					cursor = fields[0].Value.(string)
					if cursor == "0" {
						break
					}
				}
				if cursor != "0" || len(seen) != len(expected) || duplicates != 0 {
					t.Fatalf("round %d ended at cursor %s with %d missing keys and %d duplicate keys", round, cursor, len(expected)-len(seen), duplicates)
				}
			}
		})
	}
}
