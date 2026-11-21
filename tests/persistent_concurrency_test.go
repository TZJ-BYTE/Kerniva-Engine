package tests

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/TZJ-BYTE/Kerniva/config"
	"github.com/TZJ-BYTE/Kerniva/internal/protocol"
	"github.com/TZJ-BYTE/Kerniva/internal/server"
)

// Exercise the public network entry points with durable fast-path writes and
// exclusive compound commands, then reopen the same data through cold reads.
func TestPersistentConcurrentCommandsAndRestart(t *testing.T) {
	for _, networkType := range []string{"std", "gnet"} {
		t.Run(networkType, func(t *testing.T) {
			cfg := config.DefaultConfig()
			cfg.NetworkType = networkType
			cfg.DBCount = 1
			cfg.DataDir = t.TempDir()
			cfg.PersistenceEnabled = true
			cfg.PersistenceDurability = "sync"
			cfg.ColdStartStrategy = "lazy_load"
			cfg.MaxMemoryPolicy = "noeviction"
			cfg.MaxMemory = 16 << 20
			cfg.EngineSegmentSize = 4096
			cfg.EngineCheckpointAfterOps = 16

			address, stop := startPersistentTestServer(t, cfg)
			request := newPersistentTestClient(t, address)
			if got := request("MSET", "pair:left", "0", "pair:right", "0"); got != "OK" {
				t.Fatalf("seed MSET = %#v", got)
			}

			watcher := newPersistentTestClient(t, address)
			for _, args := range [][]string{{"WATCH", "watched"}, {"SET", "watched", "committed"}} {
				client := watcher
				if args[0] == "SET" {
					client = request
				}
				if got := client(args...); got != "OK" {
					t.Fatalf("%s = %#v", args[0], got)
				}
			}
			if got := watcher("MULTI"); got != "OK" {
				t.Fatalf("MULTI = %#v", got)
			}
			if got := watcher("GET", "watched"); got != "QUEUED" {
				t.Fatalf("queued GET = %#v", got)
			}
			if got := watcher("EXEC"); got != nil {
				t.Fatalf("WATCH conflict EXEC = %#v", got)
			}

			const workers, updates = 4, 40
			var ready, finished sync.WaitGroup
			ready.Add(workers + 1)
			finished.Add(workers + 1)
			begin := make(chan struct{})
			for worker := 0; worker < workers; worker++ {
				client := newPersistentTestClient(t, address)
				go func(worker int) {
					defer finished.Done()
					ready.Done()
					<-begin
					key := fmt.Sprintf("writer:%d", worker)
					for update := 0; update < updates; update++ {
						value := strconv.Itoa(update)
						if got := client("SET", key, value); got != "OK" {
							t.Errorf("durable SET %s = %#v", key, got)
							return
						}
						if got := client("GET", key); got != value {
							t.Errorf("read after acknowledgement %s = %#v, want %q", key, got, value)
							return
						}
					}
				}(worker)
			}
			reader := newPersistentTestClient(t, address)
			go func() {
				defer finished.Done()
				ready.Done()
				<-begin
				for read := 0; read < updates*3; read++ {
					got := reader("MGET", "pair:left", "pair:right")
					pair, ok := got.([]interface{})
					if !ok || len(pair) != 2 || pair[0] != pair[1] {
						t.Errorf("compound update exposed partial pair = %#v", got)
						return
					}
				}
			}()
			ready.Wait()
			close(begin)
			defer finished.Wait()
			for update := 1; update <= updates; update++ {
				value := strconv.Itoa(update)
				switch update % 3 {
				case 0:
					if got := request("MSET", "pair:left", value, "pair:right", value); got != "OK" {
						t.Fatalf("MSET = %#v", got)
					}
				case 1:
					script := `redis.call("SET", KEYS[1], ARGV[1]); redis.call("SET", KEYS[2], ARGV[1]); return 1`
					if got := request("EVAL", script, "2", "pair:left", "pair:right", value); got != int64(1) {
						t.Fatalf("EVAL = %#v", got)
					}
				case 2:
					if got := request("MULTI"); got != "OK" {
						t.Fatalf("MULTI = %#v", got)
					}
					for _, key := range []string{"pair:left", "pair:right"} {
						if got := request("SET", key, value); got != "QUEUED" {
							t.Fatalf("queued SET = %#v", got)
						}
					}
					if got := request("EXEC"); !equalRESPArray(got, "OK", "OK") {
						t.Fatalf("EXEC = %#v", got)
					}
				}
			}
			finished.Wait()
			stop()

			address, stop = startPersistentTestServer(t, cfg)
			defer stop()
			recovered := newPersistentTestClient(t, address)
			for worker := 0; worker < workers; worker++ {
				key := fmt.Sprintf("writer:%d", worker)
				if got := recovered("GET", key); got != strconv.Itoa(updates-1) {
					t.Fatalf("recovered acknowledged key %s = %#v", key, got)
				}
			}
			if got := recovered("MGET", "pair:left", "pair:right"); !equalRESPArray(got, strconv.Itoa(updates), strconv.Itoa(updates)) {
				t.Fatalf("recovered compound pair = %#v", got)
			}
		})
	}
}

func TestPersistentAcknowledgementsSurviveProcessTermination(t *testing.T) {
	for _, networkType := range []string{"std", "gnet"} {
		t.Run(networkType, func(t *testing.T) {
			cfg := config.DefaultConfig()
			cfg.NetworkType = networkType
			cfg.Host = "127.0.0.1"
			cfg.DBCount = 1
			cfg.DataDir = t.TempDir()
			cfg.PersistenceEnabled = true
			cfg.PersistenceDurability = "sync"
			cfg.MaxMemoryPolicy = "noeviction"
			cfg.ColdStartStrategy = "lazy_load"
			reserved, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			cfg.Port = reserved.Addr().(*net.TCPAddr).Port
			_ = reserved.Close()
			encoded, err := json.Marshal(cfg)
			if err != nil {
				t.Fatal(err)
			}
			executable, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			log, err := os.Create(filepath.Join(t.TempDir(), "crash-server.log"))
			if err != nil {
				t.Fatal(err)
			}
			defer log.Close()
			child := exec.Command(executable, "-test.run=^TestPersistentCrashServerFixture$", "-test.timeout=30s")
			child.Env = append(os.Environ(), "KERNIVA_THESIS_CRASH_CONFIG="+string(encoded))
			child.Stdout, child.Stderr = log, log
			if err := child.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if child.ProcessState == nil {
					_ = child.Process.Kill()
					_ = child.Wait()
				}
			})
			address := net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port))
			deadline := time.Now().Add(5 * time.Second)
			ready := false
			for time.Now().Before(deadline) {
				conn, err := net.DialTimeout("tcp", address, 50*time.Millisecond)
				if err == nil {
					_ = conn.Close()
					ready = true
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			if !ready {
				t.Fatal("crash fixture failed to start")
			}
			request := newPersistentTestClient(t, address)
			for version := 0; version < 32; version++ {
				value := strconv.Itoa(version)
				if got := request("MSET", "left", value, "right", value); got != "OK" {
					t.Fatalf("acknowledged batch = %#v", got)
				}
				if got := request("SET", "fast:key", "fast-"+value); got != "OK" {
					t.Fatalf("acknowledged fast SET = %#v", got)
				}
			}
			if err := child.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			_ = child.Wait()
			address, stop := startPersistentTestServer(t, cfg)
			defer stop()
			recovered := newPersistentTestClient(t, address)
			if got := recovered("MGET", "left", "right"); !equalRESPArray(got, "31", "31") {
				t.Fatalf("recovered acknowledged values = %#v", got)
			}
			if got := recovered("GET", "fast:key"); got != "fast-31" {
				t.Fatalf("recovered acknowledged fast SET = %#v", got)
			}
		})
	}
}

func TestPersistentCrashServerFixture(t *testing.T) {
	encoded := os.Getenv("KERNIVA_THESIS_CRASH_CONFIG")
	if encoded == "" {
		return
	}
	var cfg config.Config
	if err := json.Unmarshal([]byte(encoded), &cfg); err != nil {
		t.Fatal(err)
	}
	if err := server.NewServer(&cfg).Start(); err != nil {
		t.Fatal(err)
	}
}

func newPersistentTestClient(t *testing.T, address string) func(...string) interface{} {
	t.Helper()
	conn, err := net.DialTimeout("tcp", address, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	var buffer bytes.Buffer
	return func(args ...string) interface{} {
		t.Helper()
		if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
			t.Fatal(err)
		}
		payload := protocol.MakeArray(args)
		encoded := protocol.EncodeResponse(payload)
		protocol.ReleaseResponse(payload)
		writeAll(t, conn, encoded)
		return readResp(t, conn, &buffer)
	}
}

func startPersistentTestServer(t *testing.T, cfg *config.Config) (string, func()) {
	t.Helper()
	reserved, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Host, cfg.Port = "127.0.0.1", reserved.Addr().(*net.TCPAddr).Port
	_ = reserved.Close()
	srv := server.NewServer(cfg)
	started := make(chan error, 1)
	go func() { started <- srv.Start() }()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			if err := srv.Stop(); err != nil {
				t.Error(err)
			}
			select {
			case err := <-started:
				if err != nil {
					t.Error(err)
				}
			case <-time.After(5 * time.Second):
				t.Error("persistent server did not stop")
			}
		})
	}
	t.Cleanup(stop)
	address := net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port))
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", address, 50*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return address, stop
		}
		select {
		case err := <-started:
			t.Fatalf("persistent server exited before accepting connections, %v", err)
		default:
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("persistent server did not accept connections")
	return "", stop
}
