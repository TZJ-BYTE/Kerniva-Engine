package tests

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/TZJ-BYTE/Kerniva/config"
)

func TestGoLogSnapshotAndCrashRecovery(t *testing.T) {
	maintainer := os.Getenv("KERNIVA_MAINTAINER")
	if maintainer == "" {
		t.Skip("set KERNIVA_MAINTAINER or run the project test script")
	}
	for _, network := range []string{"std", "gnet"} {
		t.Run(network, func(t *testing.T) {
			cfg := goLogIntegrationConfig(t, network)
			cfg.DBCount = 2
			cfg.GoLogSegmentAgeSeconds = 1

			address, crash := startGoLogCrashFixture(t, cfg)
			request := newPersistentTestClient(t, address)
			expect := func(result, want any) {
				t.Helper()
				if result != want {
					t.Fatalf("reply = %#v, want %#v", result, want)
				}
			}
			info, ok := request("INFO").(string)
			if !ok || !strings.Contains(info, "storage_backend:go-log\n") {
				t.Fatalf("unexpected backend INFO: %v", info)
			}
			expect(request("SET", "changed", "old"), "OK")
			expect(request("SET", "removed", "old"), "OK")
			expect(request("SET", "short-lived", "old", "PX", "250"), "OK")
			expect(request("RPUSH", "queue", "one", "two"), int64(2))
			expect(request("HSET", "hash", "field", "value"), int64(1))
			expect(request("SELECT", "1"), "OK")
			expect(request("SET", "changed", "other-db"), "OK")
			expect(request("SELECT", "0"), "OK")

			padding := strings.Repeat("p", 256)
			for i := range 42 {
				expect(request("SET", fmt.Sprintf("pad:%d", i), padding), "OK")
			}
			dataRoot := filepath.Join(cfg.DataDir, "go-log-v1")
			maintain := exec.Command(maintainer, "--data-dir", dataRoot, "--once")
			if data, err := maintain.CombinedOutput(); err != nil {
				t.Fatalf("maintainer: %v\n%s", err, data)
			}
			if _, err := os.Stat(filepath.Join(dataRoot, "db_0", "CURRENT")); err != nil {
				t.Fatalf("snapshot was not published: %v", err)
			}

			expect(request("SET", "changed", "new"), "OK")
			expect(request("DEL", "removed"), int64(1))
			expect(request("SET", "with-ttl", "live", "PX", "30000"), "OK")
			expect(request("MSET", "left", "new", "right", "new"), "OK")
			expect(request("GET", "changed"), "new")
			expect(request("GET", "removed"), nil)
			crash()

			address, stop := startPersistentTestServer(t, cfg)
			defer stop()
			reopened := newPersistentTestClient(t, address)
			expect(reopened("GET", "changed"), "new")
			expect(reopened("GET", "removed"), nil)
			expect(reopened("GET", "with-ttl"), "live")
			expect(reopened("LPOP", "queue"), "one")
			expect(reopened("HGET", "hash", "field"), "value")
			if got := reopened("MGET", "left", "right"); !equalRESPArray(got, "new", "new") {
				t.Fatalf("recovered batch = %#v", got)
			}
			for i := range 42 {
				expect(reopened("GET", fmt.Sprintf("pad:%d", i)), padding)
			}
			time.Sleep(255 * time.Millisecond)
			expect(reopened("GET", "short-lived"), nil)
			expect(reopened("SELECT", "1"), "OK")
			expect(reopened("GET", "changed"), "other-db")
			expect(reopened("FLUSHDB"), "OK")
			expect(reopened("SELECT", "0"), "OK")
			expect(reopened("GET", "changed"), "new")
			stop()

			address, stop = startPersistentTestServer(t, cfg)
			defer stop()
			reopened = newPersistentTestClient(t, address)
			expect(reopened("SELECT", "1"), "OK")
			expect(reopened("GET", "changed"), nil)
			expect(reopened("SELECT", "0"), "OK")
			expect(reopened("GET", "changed"), "new")
		})
	}
}

func TestGoLogConcurrentExpiryAndCrashRecovery(t *testing.T) {
	for _, network := range []string{"std", "gnet"} {
		t.Run(network, func(t *testing.T) {
			cfg := goLogIntegrationConfig(t, network)
			address, crash := startGoLogCrashFixture(t, cfg)
			request := newPersistentTestClient(t, address)
			const writers, keysPerWriter, witnesses = 4, 8, 16
			const refreshedKeys = writers * keysPerWriter
			key := func(i int) string { return fmt.Sprintf("renew:%d", i) }
			witness := func(i int) string { return fmt.Sprintf("expire:%d", i) }
			for i := range refreshedKeys {
				if reply := request("SET", key(i), "stale", "PXAT", "1"); reply != "OK" {
					t.Fatalf("seed expired %s = %v", key(i), reply)
				}
			}
			for i := range witnesses {
				if reply := request("SET", witness(i), "stale", "PXAT", "1"); reply != "OK" {
					t.Fatalf("seed expiry witness = %v", reply)
				}
			}

			deadline := time.Now().Add(2 * time.Minute).UnixMilli()
			absolute := strconv.FormatInt(deadline, 10)
			start := make(chan struct{})
			var finished sync.WaitGroup
			for worker := range writers {
				writer := newPersistentTestClient(t, address)
				reader := newPersistentTestClient(t, address)
				finished.Go(func() {
					<-start
					for i := worker; i < refreshedKeys; i += writers {
						if reply := writer("SET", key(i), "fresh", "PXAT", absolute); reply != "OK" {
							t.Errorf("refresh %s = %v", key(i), reply)
							return
						}
					}
				})
				finished.Go(func() {
					<-start
					for i := worker; i < refreshedKeys; i += writers {
						if reply := reader("GET", key(i)); reply != nil && reply != "fresh" {
							t.Errorf("concurrent GET %s returned expired data: %v", key(i), reply)
							return
						}
					}
				})
			}
			close(start)
			finished.Wait()

			// Witnesses are never read online. Their removal establishes that the
			// background sampler ran, even if readers removed the first versions.
			for limit := time.Now().Add(4 * time.Second); ; time.Sleep(20 * time.Millisecond) {
				if count := goLogMemoryKeyCount(t, request("INFO")); count < refreshedKeys+witnesses {
					break
				}
				if time.Now().After(limit) {
					t.Fatal("expiry cleaner did not reclaim any witness")
				}
			}
			verify := func(client func(...string) interface{}) {
				t.Helper()
				for i := range refreshedKeys {
					if reply := client("GET", key(i)); reply != "fresh" {
						t.Fatalf("last acknowledged value of %s = %v", key(i), reply)
					}
					if reply := client("PEXPIRETIME", key(i)); reply != deadline {
						t.Fatalf("absolute TTL of %s = %v, want %d", key(i), reply, deadline)
					}
				}
			}
			verify(request)
			crash()
			address, _ = startPersistentTestServer(t, cfg)
			recovered := newPersistentTestClient(t, address)
			verify(recovered)
			for i := range witnesses {
				if reply := recovered("GET", witness(i)); reply != nil {
					t.Fatalf("expired witness %s returned after recovery: %v", witness(i), reply)
				}
			}
		})
	}
}

func TestGoLogSealFailureRollsBackUnacknowledgedBatch(t *testing.T) {
	for _, network := range []string{"std", "gnet"} {
		t.Run(network, func(t *testing.T) {
			cfg := goLogIntegrationConfig(t, network)
			address, crash := startGoLogCrashFixture(t, cfg)
			request := newPersistentTestClient(t, address)
			if reply := request("MSET", "left", "old", "right", "old"); reply != "OK" {
				t.Fatalf("seed MSET = %v", reply)
			}
			// Force publication to fail at the first rollover without touching
			// the active file containing the acknowledged commit.
			blocker := filepath.Join(cfg.DataDir, "go-log-v1", "db_0", "log-00000000000000000001-00000000000000000001.seg")
			if err := os.Mkdir(blocker, 0o700); err != nil {
				t.Fatal(err)
			}
			if reply := request("MSET", "left", strings.Repeat("x", 4096), "right", "new"); !goLogErrorContains(reply, "Go-log publication would replace") {
				t.Fatalf("blocked rollover MSET = %v", reply)
			}
			if reply := request("SET", "after-fault", "new"); !goLogErrorContains(reply, "Go-log publication would replace") {
				t.Fatalf("SET after rollover fault = %v", reply)
			}
			if reply := request("MGET", "left", "right", "after-fault"); !equalRESPArray(reply, "old", "old", nil) {
				t.Fatalf("unacknowledged batch visible in memory: %v", reply)
			}
			crash()
			if err := os.Remove(blocker); err != nil {
				t.Fatal(err)
			}
			address, _ = startPersistentTestServer(t, cfg)
			recovered := newPersistentTestClient(t, address)
			if reply := recovered("MGET", "left", "right", "after-fault"); !equalRESPArray(reply, "old", "old", nil) {
				t.Fatalf("unacknowledged batch replayed: %v", reply)
			}
			if reply := recovered("MSET", "left", "restored", "right", "restored"); reply != "OK" {
				t.Fatalf("write after successful recovery = %v", reply)
			}
		})
	}
}

func goLogIntegrationConfig(t *testing.T, network string) *config.Config {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.StorageBackend = config.StorageGoLog
	cfg.NetworkType = network
	cfg.DataDir = t.TempDir()
	cfg.DBCount = 1
	cfg.ColdStartStrategy = "load_all"
	cfg.PersistenceEnabled = true
	cfg.PersistenceWriteMode = "strong"
	cfg.PersistenceDurability = "sync"
	cfg.MaxMemoryPolicy = "noeviction"
	cfg.EngineSegmentSize = 4096
	cfg.GoLogSegmentAgeSeconds = 60
	return cfg
}

func goLogMemoryKeyCount(t *testing.T, reply interface{}) int {
	t.Helper()
	if info, ok := reply.(string); ok {
		for line := range strings.SplitSeq(info, "\n") {
			if value, ok := strings.CutPrefix(line, "memory_keys:"); ok {
				count, err := strconv.Atoi(value)
				if err == nil {
					return count
				}
			}
		}
	}
	t.Fatalf("missing memory_keys from INFO: %v", reply)
	return 0
}

func goLogErrorContains(reply interface{}, substring string) bool {
	err, ok := reply.(error)
	return ok && strings.Contains(err.Error(), substring)
}

func startGoLogCrashFixture(t *testing.T, cfg *config.Config) (string, func()) {
	t.Helper()
	return startGoLogFixture(t, cfg, "TestPersistentCrashServerFixture")
}

func startGoLogFixture(t *testing.T, cfg *config.Config, fixture string) (string, func()) {
	t.Helper()
	reservation, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := reservation.Addr().(*net.TCPAddr).Port
	if err := reservation.Close(); err != nil {
		t.Fatal(err)
	}
	copy := *cfg
	copy.Host, copy.Port = "127.0.0.1", port
	encoded, err := json.Marshal(&copy)
	if err != nil {
		t.Fatal(err)
	}
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	log, err := os.Create(filepath.Join(t.TempDir(), "go-log-crash-server.log"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	child := exec.Command(self, "-test.run=^"+fixture+"$", "-test.timeout=30s")
	child.Env = append(os.Environ(), "KERNIVA_THESIS_CRASH_CONFIG="+string(encoded))
	child.Stdout, child.Stderr = log, log
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	crash := func() {
		if child.ProcessState != nil {
			return
		}
		if err := child.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			t.Error(err)
		}
		_ = child.Wait()
	}
	t.Cleanup(crash)
	address := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	for deadline := time.Now().Add(8 * time.Second); time.Now().Before(deadline); {
		if conn, err := net.DialTimeout("tcp", address, 100*time.Millisecond); err == nil {
			_ = conn.Close()
			return address, crash
		}
		time.Sleep(20 * time.Millisecond)
	}
	crash()
	data, _ := os.ReadFile(log.Name())
	t.Fatalf("golog crash fixture failed to start: %s", data)
	return "", nil
}
