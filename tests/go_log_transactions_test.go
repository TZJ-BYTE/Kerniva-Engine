package tests

import (
	"strings"
	"testing"
)

func TestGoLogTransactionsAndBudgetRecovery(t *testing.T) {
	for _, network := range []string{"std", "gnet"} {
		t.Run(network, func(t *testing.T) {
			cfg := goLogIntegrationConfig(t, network)
			cfg.MaxMemory = 2048
			address, crash := startGoLogCrashFixture(t, cfg)
			client := newPersistentTestClient(t, address)
			peer := newPersistentTestClient(t, address)
			requireReply := func(reply, want any) {
				t.Helper()
				if reply != want {
					t.Fatalf("reply = %#v, want %#v", reply, want)
				}
			}
			requireError := func(reply any, fragment string) {
				t.Helper()
				err, ok := reply.(error)
				if !ok || !strings.Contains(err.Error(), fragment) {
					t.Fatalf("reply = %#v, want error containing %q", reply, fragment)
				}
			}

			requireReply(client("SET", "watched", "initial"), "OK")
			requireReply(client("WATCH", "watched"), "OK")
			requireReply(client("MULTI"), "OK")
			requireReply(client("SET", "aborted", "orphan"), "QUEUED")
			requireReply(peer("SET", "watched", "peer"), "OK")
			requireReply(client("EXEC"), nil)
			requireReply(peer("GET", "aborted"), nil)

			requireReply(client("MULTI"), "OK")
			requireReply(client("MSET", "left", "before", "right", "before"), "QUEUED")
			requireReply(client("HSET", "watched", "entry", "wrong-type"), "QUEUED")
			requireReply(client("SET", "after-error", "committed"), "QUEUED")
			result, ok := client("EXEC").([]interface{})
			if !ok || len(result) != 3 {
				t.Fatalf("EXEC returned %#v", result)
			}
			requireReply(result[0], "OK")
			requireError(result[1], "WRONGTYPE")
			requireReply(result[2], "OK")

			requireError(client("MSET", "left", "would-change", "oversized", strings.Repeat("x", 2048)), "OOM")
			requireReply(peer("GET", "left"), "before")
			requireReply(peer("GET", "oversized"), nil)
			crash()

			address, stop := startPersistentTestServer(t, cfg)
			defer stop()
			reopened := newPersistentTestClient(t, address)
			if values := reopened("MGET", "left", "right", "watched", "after-error"); !equalRESPArray(values, "before", "before", "peer", "committed") {
				t.Fatalf("unexpected recovered keys: %#v", values)
			}
			requireReply(reopened("GET", "aborted"), nil)
			requireReply(reopened("GET", "oversized"), nil)
		})
	}
}
