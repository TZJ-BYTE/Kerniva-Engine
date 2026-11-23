package tests

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/TZJ-BYTE/Kerniva/config"
	"github.com/TZJ-BYTE/Kerniva/internal/respclient"
)

// Exercise the existing Go listeners and RESP framing with Rust as the only
// owner of values. These tests require the optional memory ABI in the library.
func TestRustMemoryBackendNetwork(t *testing.T) {
	for _, networkType := range []string{"std", "gnet"} {
		t.Run(networkType, func(t *testing.T) {
			cfg := rustMemoryNetworkConfig(networkType, 1<<20)
			cfg.DataDir = filepath.Join(t.TempDir(), "unused-memory-data")
			address, stop := startPersistentTestServer(t, cfg)
			client := newMemoryNetworkClient(t, address)

			info := memoryMustReply(t, client, "INFO")
			if info.Type != '$' || info.Null {
				t.Fatalf("INFO = %+v", info)
			}
			for key, want := range map[string]string{
				"storage_backend": "rust-memory", "mode": "memory",
				"persistence_enabled": "false", "memory_abi_version": "1",
				"db_id": "0", "db_count": "2", "memory_shards": "4",
			} {
				if got := memoryInfoField(info.Text, key); got != want {
					t.Fatalf("INFO %s = %q, want %q", key, got, want)
				}
			}

			binaryKey := "\x00key\r\n\xff"
			binaryValue := "\xff\x00value\r\n"
			memoryExpectReply(t, memoryMustReply(t, client, "SET", binaryKey, binaryValue), '+', "OK")
			memoryExpectReply(t, memoryMustReply(t, client, "GET", binaryKey), '$', binaryValue)
			memoryExpectReply(t, memoryMustReply(t, client, "SET", "", ""), '+', "OK")
			memoryExpectReply(t, memoryMustReply(t, client, "GET", ""), '$', "")
			memoryExpectMissing(t, memoryMustReply(t, client, "GET", "missing"))

			mget := memoryMustReply(t, client, "MGET", binaryKey, "missing", "", binaryKey)
			if mget.Type != '*' || len(mget.Items) != 4 {
				t.Fatalf("MGET = %+v", mget)
			}
			memoryExpectReply(t, mget.Items[0], '$', binaryValue)
			memoryExpectMissing(t, mget.Items[1])
			memoryExpectReply(t, mget.Items[2], '$', "")
			memoryExpectReply(t, mget.Items[3], '$', binaryValue)

			// The first reply exceeds the reusable read buffer and the connection
			// output batch. Subsequent replies must still preserve their bytes.
			largeValue := strings.Repeat("\x00\xff\r\n", 18000)
			memoryExpectReply(t, memoryMustReply(t, client, "SET", "large", largeValue), '+', "OK")
			pipeline := append(respclient.Request("GET", "large"), respclient.Request("GET", binaryKey)...)
			pipeline = append(pipeline, respclient.Request("GET", "missing")...)
			pipeline = append(pipeline, respclient.Request("PING")...)
			if err := client.conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
				t.Fatal(err)
			}
			writeAll(t, client.conn, pipeline)
			for index, expected := range []string{largeValue, binaryValue, "", "PONG"} {
				reply, err := respclient.ReadReply(client.reader)
				if err != nil {
					t.Fatalf("pipeline reply %d: %v", index, err)
				}
				switch index {
				case 2:
					memoryExpectMissing(t, reply)
				case 3:
					memoryExpectReply(t, reply, '+', expected)
				default:
					memoryExpectReply(t, reply, '$', expected)
				}
			}
			mget = memoryMustReply(t, client, "MGET", "large", binaryKey, "large")
			if mget.Type != '*' || len(mget.Items) != 3 {
				t.Fatalf("large MGET = %+v", mget)
			}
			memoryExpectReply(t, mget.Items[0], '$', largeValue)
			memoryExpectReply(t, mget.Items[1], '$', binaryValue)
			memoryExpectReply(t, mget.Items[2], '$', largeValue)

			memoryExpectReply(t, memoryMustReply(t, client, "SELECT", "1"), '+', "OK")
			memoryExpectMissing(t, memoryMustReply(t, client, "GET", binaryKey))
			memoryExpectReply(t, memoryMustReply(t, client, "SET", binaryKey, "database-one"), '+', "OK")
			if selected := memoryInfoField(memoryMustReply(t, client, "INFO").Text, "db_id"); selected != "1" {
				t.Fatalf("selected database INFO id = %q", selected)
			}
			other := newMemoryNetworkClient(t, address)
			memoryExpectReply(t, memoryMustReply(t, other, "GET", binaryKey), '$', binaryValue)
			for _, invalid := range []string{"-1", "2", "not-an-index"} {
				reply := memoryMustReply(t, client, "SELECT", invalid)
				if reply.Type != '-' {
					t.Fatalf("SELECT %q unexpectedly accepted: %+v", invalid, reply)
				}
				memoryExpectReply(t, memoryMustReply(t, client, "GET", binaryKey), '$', "database-one")
			}

			memoryExpectReply(t, memoryMustReply(t, client, "SET", "guard", "original"), '+', "OK")
			for _, args := range [][]string{
				{"SET", "guard", "changed", "NX"},
				{"SET", "guard", "changed", "EX", "1"},
				{"SET", "guard", "changed", "GET"},
				{"MSET", "guard", "changed", "unpaired"},
				{"DEL", "guard", binaryKey},
				{"HSET", "guard", "field", "changed"},
				{"MULTI"},
				{"WATCH", "guard"},
				{"EXPIRE", "guard", "1"},
			} {
				if reply := memoryMustReply(t, client, args...); reply.Type != '-' {
					t.Fatalf("unsupported/invalid %s was accepted: %+v", args[0], reply)
				}
				memoryExpectReply(t, memoryMustReply(t, client, "GET", "guard"), '$', "original")
			}
			memoryExpectReply(t, memoryMustReply(t, client, "DEL", "guard"), ':', "1")
			memoryExpectReply(t, memoryMustReply(t, client, "DEL", "guard"), ':', "0")
			memoryExpectMissing(t, memoryMustReply(t, client, "GET", "guard"))
			memoryExpectReply(t, memoryMustReply(t, client, "QUIT"), '+', "OK")

			stop()
			if _, err := os.Stat(cfg.DataDir); !os.IsNotExist(err) {
				t.Fatalf("pure-memory backend created/accessed its unused data directory: %v", err)
			}
		})
	}
}

func TestRustMemoryBackendAtomicBudgetRejection(t *testing.T) {
	for _, networkType := range []string{"std", "gnet"} {
		t.Run(networkType, func(t *testing.T) {
			address, _ := startPersistentTestServer(t, rustMemoryNetworkConfig(networkType, 8))
			client := newMemoryNetworkClient(t, address)
			memoryExpectReply(t, memoryMustReply(t, client, "SET", "a", "old"), '+', "OK")
			reply := memoryMustReply(t, client, "MSET", "a", "new", "b", "too-large")
			if reply.Type != '-' || !strings.HasPrefix(reply.Text, "OOM ") {
				t.Fatalf("over-budget MSET = %+v", reply)
			}
			memoryExpectReply(t, memoryMustReply(t, client, "GET", "a"), '$', "old")
			memoryExpectMissing(t, memoryMustReply(t, client, "GET", "b"))
			info := memoryMustReply(t, client, "INFO")
			if got := memoryInfoField(info.Text, "used_memory_bytes"); got != "4" {
				t.Fatalf("rejected batch changed resident budget to %q", got)
			}
			if got := memoryInfoField(info.Text, "memory_keys"); got != "1" {
				t.Fatalf("rejected batch published keys: %q", got)
			}
			memoryExpectReply(t, memoryMustReply(t, client, "MSET", "a", "x", "b", "yz"), '+', "OK")
			memoryExpectReply(t, memoryMustReply(t, client, "DEL", "a"), ':', "1")
			memoryExpectReply(t, memoryMustReply(t, client, "SET", "c", "1234"), '+', "OK")
			if reply := memoryMustReply(t, client, "SET", "d", ""); reply.Type != '-' || !strings.HasPrefix(reply.Text, "OOM ") {
				t.Fatalf("write beyond exactly full budget = %+v", reply)
			}
			memoryExpectReply(t, memoryMustReply(t, client, "GET", "c"), '$', "1234")
			memoryExpectReply(t, memoryMustReply(t, client, "DEL", "b"), ':', "1")
			memoryExpectReply(t, memoryMustReply(t, client, "SET", "d", ""), '+', "OK")
			memoryExpectReply(t, memoryMustReply(t, client, "GET", "d"), '$', "")
		})
	}
}

func TestRustMemoryBackendConcurrentBatchVisibility(t *testing.T) {
	for _, networkType := range []string{"std", "gnet"} {
		t.Run(networkType, func(t *testing.T) {
			address, _ := startPersistentTestServer(t, rustMemoryNetworkConfig(networkType, 1<<20))
			writer := newMemoryNetworkClient(t, address)
			reader := newMemoryNetworkClient(t, address)
			memoryExpectReply(t, memoryMustReply(t, writer, "MSET", "left", "0", "right", "0"), '+', "OK")
			begin := make(chan struct{})
			results := make(chan error, 2)
			go func() {
				<-begin
				for version := 1; version <= 128; version++ {
					value := strconv.Itoa(version)
					reply, err := writer.exchange("MSET", "left", value, "right", value)
					if err != nil {
						results <- err
						return
					}
					if reply.Type != '+' || reply.Text != "OK" {
						results <- fmt.Errorf("concurrent MSET: %+v", reply)
						return
					}
				}
				results <- nil
			}()
			go func() {
				<-begin
				for read := 0; read < 256; read++ {
					reply, err := reader.exchange("MGET", "left", "right")
					if err != nil {
						results <- err
						return
					}
					if reply.Type != '*' || len(reply.Items) != 2 ||
						reply.Items[0].Type != '$' || reply.Items[1].Type != '$' ||
						reply.Items[0].Null || reply.Items[1].Null ||
						reply.Items[0].Text != reply.Items[1].Text {
						results <- fmt.Errorf("MGET observed partial MSET: %+v", reply)
						return
					}
				}
				results <- nil
			}()
			close(begin)
			for worker := 0; worker < 2; worker++ {
				if err := <-results; err != nil {
					t.Error(err)
				}
			}
		})
	}
}

func rustMemoryNetworkConfig(networkType string, budget int64) *config.Config {
	cfg := config.DefaultConfig()
	cfg.NetworkType = networkType
	cfg.StorageBackend = config.StorageRustMemory
	cfg.RustMemoryShards = 4
	cfg.PersistenceEnabled = false
	cfg.MaxMemoryPolicy = "noeviction"
	cfg.MaxMemory = budget
	cfg.DBCount = 2
	return cfg
}

type memoryNetworkClient struct {
	conn   net.Conn
	reader *bufio.Reader
}

func newMemoryNetworkClient(t *testing.T, address string) *memoryNetworkClient {
	t.Helper()
	conn, err := net.DialTimeout("tcp", address, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return &memoryNetworkClient{conn: conn, reader: bufio.NewReader(conn)}
}

func (client *memoryNetworkClient) exchange(args ...string) (respclient.Reply, error) {
	if err := client.conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return respclient.Reply{}, err
	}
	if err := respclient.WriteRequest(client.conn, args...); err != nil {
		return respclient.Reply{}, err
	}
	return respclient.ReadReply(client.reader)
}

func memoryMustReply(t *testing.T, client *memoryNetworkClient, args ...string) respclient.Reply {
	t.Helper()
	reply, err := client.exchange(args...)
	if err != nil {
		t.Fatalf("%s exchange: %v", args[0], err)
	}
	return reply
}

func memoryExpectReply(t *testing.T, reply respclient.Reply, kind byte, text string) {
	t.Helper()
	if reply.Type != kind || reply.Null || reply.Text != text {
		t.Fatalf("reply type=%q null=%v bytes=%d, want type=%q bytes=%d", reply.Type, reply.Null, len(reply.Text), kind, len(text))
	}
}

func memoryExpectMissing(t *testing.T, reply respclient.Reply) {
	t.Helper()
	if reply.Type != '$' || !reply.Null {
		t.Fatalf("missing value reply = %+v", reply)
	}
}

func memoryInfoField(info, key string) string {
	for _, line := range strings.Split(info, "\r\n") {
		if value, ok := strings.CutPrefix(line, key+":"); ok {
			return value
		}
	}
	return ""
}
