package tests

import (
	"bytes"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

func TestGoLogShortAppendRollsBack(t *testing.T) {
	for _, network := range []string{"std", "gnet"} {
		t.Run(network, func(t *testing.T) {
			cfg := goLogIntegrationConfig(t, network)
			address, crash := startGoLogFixture(t, cfg, "TestGoLogFileLimitFixture")
			request := newPersistentTestClient(t, address)
			if reply := request("MSET", "left", "old", "right", "old"); reply != "OK" {
				t.Fatalf("seed MSET = %v", reply)
			}
			activePath := filepath.Join(cfg.DataDir, "go-log-v1", "db_0", "active.log")
			before, err := os.ReadFile(activePath)
			if err != nil {
				t.Fatal(err)
			}
			// The batch crosses the file limit but stays below the rollover
			// threshold, forcing a partial write against the existing file.
			if reply := request("MSET", "left", strings.Repeat("x", 1500), "right", "rejected"); !goLogErrorContains(reply, "append Go-log commit") {
				t.Fatalf("oversized MSET = %v", reply)
			}
			after, err := os.ReadFile(activePath)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(before, after) {
				t.Fatalf("active log changed after rejected MSET (%d bytes before, %d after)", len(before), len(after))
			}
			if reply := request("MGET", "left", "right"); !equalRESPArray(reply, "old", "old") {
				t.Fatalf("rejected batch visible in memory: %v", reply)
			}
			if reply := request("MSET", "left", "committed", "right", "committed"); reply != "OK" {
				t.Fatalf("small write after failed append = %v", reply)
			}
			crash()
			address, _ = startPersistentTestServer(t, cfg)
			recovered := newPersistentTestClient(t, address)
			if reply := recovered("MGET", "left", "right"); !equalRESPArray(reply, "committed", "committed") {
				t.Fatalf("MGET after recovery = %v", reply)
			}
		})
	}
}

func TestGoLogFileLimitFixture(t *testing.T) {
	if os.Getenv("KERNIVA_THESIS_CRASH_CONFIG") == "" {
		return
	}
	signal.Ignore(syscall.SIGXFSZ)
	if err := syscall.Setrlimit(syscall.RLIMIT_FSIZE, &syscall.Rlimit{Cur: 1024, Max: 1024}); err != nil {
		t.Fatal(err)
	}
	TestPersistentCrashServerFixture(t)
}
