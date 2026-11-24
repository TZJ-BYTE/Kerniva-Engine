package tests

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
)

func TestGoLogGroupedTCPRecovery(t *testing.T) {
	for _, network := range []string{"std", "gnet"} {
		t.Run(network, func(t *testing.T) {
			cfg := goLogIntegrationConfig(t, network)
			cfg.GoLogGroupMax = 8
			address, crash := startGoLogCrashFixture(t, cfg)
			info := newPersistentTestClient(t, address)
			const clients, keysPerClient = 24, 12
			key := func(client, sequence int) string {
				return fmt.Sprintf("group:%d:%d", client, sequence)
			}
			for client := range clients {
				for sequence := range keysPerClient {
					if reply := info("SET", key(client, sequence), "seeded"); reply != "OK" {
						t.Fatalf("seed %s = %#v", key(client, sequence), reply)
					}
				}
			}
			fields := []string{"log_group_max", "last_sequence", "sync_count", "log_group_batches", "log_group_requests"}
			before := goLogUnsignedInfo(t, info("INFO"), fields...)
			start := make(chan struct{})
			var writers sync.WaitGroup
			for client := range clients {
				command := newPersistentTestClient(t, address)
				writers.Go(func() {
					<-start
					for sequence := range keysPerClient {
						if reply := command("SET", key(client, sequence), "committed"); reply != "OK" {
							t.Errorf("SET %s = %#v", key(client, sequence), reply)
							return
						}
					}
				})
			}
			close(start)
			writers.Wait()
			if t.Failed() {
				return
			}
			stats := goLogUnsignedInfo(t, info("INFO"), fields...)
			commits := uint64(clients * keysPerClient)
			shared := stats["log_group_batches"] - before["log_group_batches"]
			grouped := stats["log_group_requests"] - before["log_group_requests"]
			syncs := stats["sync_count"] - before["sync_count"]
			if stats["log_group_max"] != 8 || stats["last_sequence"]-before["last_sequence"] != commits || shared == 0 ||
				grouped < 2*shared || syncs+grouped-shared != commits {
				t.Fatalf("unexpected Go-log group accounting: before %v, after %v", before, stats)
			}
			crash()

			address, _ = startPersistentTestServer(t, cfg)
			reopened := newPersistentTestClient(t, address)
			for client := range clients {
				for sequence := range keysPerClient {
					if reply := reopened("GET", key(client, sequence)); reply != "committed" {
						t.Fatalf("GET recovered %s = %#v", key(client, sequence), reply)
					}
				}
			}
		})
	}
}

func goLogUnsignedInfo(t *testing.T, reply any, names ...string) map[string]uint64 {
	t.Helper()
	body, ok := reply.(string)
	if !ok {
		t.Fatalf("INFO returned %#v", reply)
	}
	fields := make(map[string]string)
	for line := range strings.SplitSeq(body, "\n") {
		if name, value, ok := strings.Cut(line, ":"); ok {
			fields[name] = strings.TrimSpace(value)
		}
	}
	stats := make(map[string]uint64, len(names))
	for _, name := range names {
		value, err := strconv.ParseUint(fields[name], 10, 64)
		if err != nil {
			t.Fatalf("INFO field %s = %q, expected an unsigned integer", name, fields[name])
		}
		stats[name] = value
	}
	return stats
}
