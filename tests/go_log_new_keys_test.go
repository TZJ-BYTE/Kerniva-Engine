package tests

import (
	"fmt"
	"sync"
	"testing"
)

func TestGoLogNewKeysGroupAndRecoverOverTCP(t *testing.T) {
	for _, network := range []string{"std", "gnet"} {
		t.Run(network, func(t *testing.T) {
			cfg := goLogIntegrationConfig(t, network)
			cfg.GoLogGroupMax = 8
			address, crash := startGoLogCrashFixture(t, cfg)
			observer := newPersistentTestClient(t, address)
			const clients, keysPerClient = 24, 8
			key := func(client, sequence int) string {
				return fmt.Sprintf("new:%d:%d", client, sequence)
			}
			for _, command := range [][]string{{"WATCH", key(0, 0)}, {"MULTI"}} {
				if reply := observer(command...); reply != "OK" {
					t.Fatalf("%s = %#v", command[0], reply)
				}
			}
			if reply := observer("SET", "aborted", "unexpected"); reply != "QUEUED" {
				t.Fatalf("queued SET = %#v", reply)
			}

			start := make(chan struct{})
			var writers sync.WaitGroup
			for client := range clients {
				command := newPersistentTestClient(t, address)
				writers.Go(func() {
					<-start
					for sequence := range keysPerClient {
						name := key(client, sequence)
						if reply := command("SET", name, name); reply != "OK" {
							t.Errorf("SET %s = %#v", name, reply)
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
			if reply := observer("EXEC"); reply != nil {
				t.Fatalf("EXEC after creating a watched key = %#v", reply)
			}
			stats := goLogUnsignedInfo(t, observer("INFO"), "last_sequence", "sync_count", "log_group_batches", "log_group_requests")
			const commits = clients * keysPerClient
			shared, grouped, syncs := stats["log_group_batches"], stats["log_group_requests"], stats["sync_count"]
			if stats["last_sequence"] != commits || shared == 0 || grouped < 2*shared || syncs+grouped-shared != commits {
				t.Fatalf("new keys did not share log syncs: %v", stats)
			}
			crash()

			address, _ = startPersistentTestServer(t, cfg)
			reopened := newPersistentTestClient(t, address)
			for client := range clients {
				for sequence := range keysPerClient {
					name := key(client, sequence)
					if reply := reopened("GET", name); reply != name {
						t.Fatalf("recovered GET %s = %#v", name, reply)
					}
				}
			}
			if reply := reopened("GET", "aborted"); reply != nil {
				t.Fatalf("recovered aborted SET = %#v", reply)
			}
		})
	}
}
