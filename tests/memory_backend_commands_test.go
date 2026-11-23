package tests

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestRustMemoryBackendSetConditionsAndExpiration(t *testing.T) {
	for _, networkType := range []string{"std", "gnet"} {
		t.Run(networkType, func(t *testing.T) {
			address, _ := startPersistentTestServer(t, rustMemoryNetworkConfig(networkType, 1<<20))
			client := newMemoryNetworkClient(t, address)
			memoryExpectReply(t, memoryMustReply(t, client, "TTL", "missing"), ':', "-2")
			memoryExpectReply(t, memoryMustReply(t, client, "PTTL", "missing"), ':', "-2")
			memoryExpectReply(t, memoryMustReply(t, client, "PEXPIRE", "missing", "1000"), ':', "0")
			memoryExpectMissing(t, memoryMustReply(t, client, "SET", "missing", "value", "XX"))
			memoryExpectMissing(t, memoryMustReply(t, client, "GET", "missing"))

			memoryExpectReply(t, memoryMustReply(t, client, "SET", "key", "first", "nx", "px", "5000"), '+', "OK")
			memoryExpectMissing(t, memoryMustReply(t, client, "SET", "key", "second", "NX"))
			memoryExpectReply(t, memoryMustReply(t, client, "GET", "key"), '$', "first")
			if ttl := memoryIntegerReply(t, client, "PTTL", "key"); ttl <= 0 || ttl > 5000 {
				t.Fatalf("failed NX changed TTL: %d", ttl)
			}
			memoryExpectReply(t, memoryMustReply(t, client, "SET", "key", "second", "XX"), '+', "OK")
			memoryExpectReply(t, memoryMustReply(t, client, "TTL", "key"), ':', "-1")
			memoryExpectReply(t, memoryMustReply(t, client, "PTTL", "key"), ':', "-1")

			for _, options := range [][]string{
				{"NX", "XX"}, {"XX", "NX"}, {"EX", "0"}, {"PX", "-1"},
				{"EX", "9223372036854775807"}, {"PX", "9223372036854775807"},
				{"PX", "01"}, {"PX", "+1"}, {"PX", "1.5"}, {"PX", ""},
				{"EX"}, {"EX", "1", "PX", "1"}, {"GET"}, {"KEEPTTL"},
			} {
				args := append([]string{"SET", "key", "changed"}, options...)
				if reply := memoryMustReply(t, client, args...); reply.Type != '-' {
					t.Fatalf("invalid SET %q accepted: %+v", options, reply)
				}
				memoryExpectReply(t, memoryMustReply(t, client, "GET", "key"), '$', "second")
				memoryExpectReply(t, memoryMustReply(t, client, "PTTL", "key"), ':', "-1")
			}

			// Redis rounds TTL to the nearest second. Derive a lower bound
			// from elapsed wall time so scheduler pauses cannot make this flaky.
			started := time.Now()
			memoryExpectReply(t, memoryMustReply(t, client, "SET", "rounded", "value", "PX", "2800"), '+', "OK")
			seconds := memoryIntegerReply(t, client, "TTL", "rounded")
			elapsed := time.Since(started).Milliseconds() + 2
			if elapsed < 2800 {
				minimum := max(int64(0), 2800-elapsed)
				if seconds < (minimum+500)/1000 || seconds > 3 {
					t.Fatalf("TTL did not round milliseconds: ttl=%d elapsed=%dms", seconds, elapsed)
				}
			}
			memoryExpectReply(t, memoryMustReply(t, client, "EXPIRE", "key", "5"), ':', "1")
			if ttl := memoryIntegerReply(t, client, "PTTL", "key"); ttl <= 0 || ttl > 5000 {
				t.Fatalf("EXPIRE milliseconds = %d", ttl)
			}
			for _, args := range [][]string{
				{"EXPIRE", "key", "9223372036854775807"},
				{"EXPIRE", "key", "-9223372036854775808"},
				{"PEXPIRE", "key", "9223372036854775807"},
				{"PEXPIRE", "key", "100", "NX"},
			} {
				if reply := memoryMustReply(t, client, args...); reply.Type != '-' {
					t.Fatalf("unsupported/out-of-range %q accepted: %+v", args, reply)
				}
				memoryExpectReply(t, memoryMustReply(t, client, "GET", "key"), '$', "second")
			}
			memoryExpectReply(t, memoryMustReply(t, client, "PEXPIRE", "key", "0"), ':', "1")
			memoryExpectMissing(t, memoryMustReply(t, client, "GET", "key"))
			memoryExpectReply(t, memoryMustReply(t, client, "EXPIRE", "key", "-1"), ':', "0")
			memoryExpectReply(t, memoryMustReply(t, client, "SET", "key", "value"), '+', "OK")
			memoryExpectReply(t, memoryMustReply(t, client, "PEXPIRE", "key", "-1"), ':', "1")
			memoryExpectReply(t, memoryMustReply(t, client, "PTTL", "key"), ':', "-2")

			memoryExpectReply(t, memoryMustReply(t, client, "SET", "short", "old", "PX", "30"), '+', "OK")
			deadline := time.Now().Add(2 * time.Second)
			for {
				reply := memoryMustReply(t, client, "GET", "short")
				if reply.Null {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("short-lived key did not expire")
				}
				time.Sleep(5 * time.Millisecond)
			}
			memoryExpectReply(t, memoryMustReply(t, client, "EXISTS", "short", "short"), ':', "0")
			memoryExpectReply(t, memoryMustReply(t, client, "SET", "short", "new", "NX", "EX", "5"), '+', "OK")
			memoryExpectReply(t, memoryMustReply(t, client, "GET", "short"), '$', "new")
		})
	}
}

func TestRustMemoryBackendIntegerUpdates(t *testing.T) {
	for _, networkType := range []string{"std", "gnet"} {
		t.Run(networkType, func(t *testing.T) {
			address, _ := startPersistentTestServer(t, rustMemoryNetworkConfig(networkType, 1<<20))
			client := newMemoryNetworkClient(t, address)
			memoryExpectReply(t, memoryMustReply(t, client, "INCR", "counter"), ':', "1")
			memoryExpectReply(t, memoryMustReply(t, client, "INCRBY", "counter", "41"), ':', "42")
			memoryExpectReply(t, memoryMustReply(t, client, "DECR", "counter"), ':', "41")
			memoryExpectReply(t, memoryMustReply(t, client, "INCRBY", "counter", "-42"), ':', "-1")
			for _, invalid := range []string{"01", "+1", "-0", " 1", "", "1.5", "9223372036854775808"} {
				if reply := memoryMustReply(t, client, "INCRBY", "counter", invalid); reply.Type != '-' {
					t.Fatalf("INCRBY invalid integer %q accepted: %+v", invalid, reply)
				}
				memoryExpectReply(t, memoryMustReply(t, client, "GET", "counter"), '$', "-1")
			}
			for _, example := range []struct{ value, command string }{
				{"9223372036854775807", "INCR"},
				{"-9223372036854775808", "DECR"},
				{"not-integer", "INCR"}, {"", "INCR"},
				{"01", "INCR"}, {"+1", "INCR"}, {"-0", "INCR"},
			} {
				memoryExpectReply(t, memoryMustReply(t, client, "SET", "counter", example.value), '+', "OK")
				if reply := memoryMustReply(t, client, example.command, "counter"); reply.Type != '-' {
					t.Fatalf("%s invalid/overflow value %q accepted: %+v", example.command, example.value, reply)
				}
				memoryExpectReply(t, memoryMustReply(t, client, "GET", "counter"), '$', example.value)
			}
			memoryExpectReply(t, memoryMustReply(t, client, "INCRBY", "minimum", "-9223372036854775808"), ':', "-9223372036854775808")
			memoryExpectReply(t, memoryMustReply(t, client, "SET", "counter", "9", "PX", "5000"), '+', "OK")
			memoryExpectReply(t, memoryMustReply(t, client, "INCR", "counter"), ':', "10")
			if ttl := memoryIntegerReply(t, client, "PTTL", "counter"); ttl <= 0 || ttl > 5000 {
				t.Fatalf("INCR lost the existing TTL: %d", ttl)
			}

			const workers, increments = 4, 32
			clients := make([]*memoryNetworkClient, workers)
			for index := range clients {
				clients[index] = newMemoryNetworkClient(t, address)
			}
			results := make(chan error, workers)
			begin := make(chan struct{})
			for _, connection := range clients {
				go func(connection *memoryNetworkClient) {
					<-begin
					for update := 0; update < increments; update++ {
						reply, err := connection.exchange("INCR", "shared")
						if err != nil {
							results <- err
							return
						}
						if reply.Type != ':' {
							results <- fmt.Errorf("concurrent INCR: %+v", reply)
							return
						}
					}
					results <- nil
				}(connection)
			}
			close(begin)
			for worker := 0; worker < workers; worker++ {
				if err := <-results; err != nil {
					t.Error(err)
				}
			}
			memoryExpectReply(t, memoryMustReply(t, client, "GET", "shared"), '$', strconv.Itoa(workers*increments))
		})
	}
}

func TestRustMemoryBackendExistsDeleteAndConditionalBudget(t *testing.T) {
	for _, networkType := range []string{"std", "gnet"} {
		t.Run(networkType, func(t *testing.T) {
			address, _ := startPersistentTestServer(t, rustMemoryNetworkConfig(networkType, 8))
			client := newMemoryNetworkClient(t, address)
			memoryExpectReply(t, memoryMustReply(t, client, "MSET", "a", "old", "b", "old"), '+', "OK")
			memoryExpectReply(t, memoryMustReply(t, client, "EXISTS", "a", "a", "missing", "b"), ':', "3")
			memoryExpectMissing(t, memoryMustReply(t, client, "SET", "a", strings.Repeat("x", 1024), "NX"))
			memoryExpectMissing(t, memoryMustReply(t, client, "SET", "z", strings.Repeat("x", 1024), "XX"))
			memoryExpectReply(t, memoryMustReply(t, client, "GET", "a"), '$', "old")
			memoryExpectMissing(t, memoryMustReply(t, client, "GET", "z"))
			memoryExpectReply(t, memoryMustReply(t, client, "DEL", "a", "missing", "a", "b", "b"), ':', "2")
			memoryExpectReply(t, memoryMustReply(t, client, "EXISTS", "a", "b"), ':', "0")
			memoryExpectReply(t, memoryMustReply(t, client, "DEL", "a", "b"), ':', "0")
			memoryExpectReply(t, memoryMustReply(t, client, "SET", "a", "1234567", "NX"), '+', "OK")
			if reply := memoryMustReply(t, client, "SET", "a", "12345678", "XX"); reply.Type != '-' || !strings.HasPrefix(reply.Text, "OOM ") {
				t.Fatalf("conditional budget overflow = %+v", reply)
			}
			memoryExpectReply(t, memoryMustReply(t, client, "GET", "a"), '$', "1234567")
		})
	}
}

func memoryIntegerReply(t *testing.T, client *memoryNetworkClient, args ...string) int64 {
	t.Helper()
	reply := memoryMustReply(t, client, args...)
	if reply.Type != ':' || reply.Null {
		t.Fatalf("%s returned non-integer reply: %+v", args[0], reply)
	}
	value, err := strconv.ParseInt(reply.Text, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return value
}
