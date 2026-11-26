package server

import (
	"bytes"
	"encoding/json"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/TZJ-BYTE/Kerniva/config"
	"github.com/TZJ-BYTE/Kerniva/internal/writetrace"
)

func TestWriteTraceJoinsTCPOrdinalAndCommit(t *testing.T) {
	if os.Getenv("KERNIVA_ENGINE_LIBRARY") == "" {
		t.Skip("Run the project test script to provide the native Rust library")
	}
	for _, tc := range []struct {
		name, network string
		group         int
	}{{"std/direct", "std", 1}, {"std/queued", "std", 4}, {"gnet/direct", "gnet", 1}, {"gnet/queued", "gnet", 4}} {
		t.Run(tc.name, func(t *testing.T) {
			tracePath := filepath.Join(t.TempDir(), "trace.jsonl")
			trace, err := writetrace.Open(tracePath)
			if err != nil {
				t.Fatal(err)
			}
			traceClosed := false
			t.Cleanup(func() {
				if !traceClosed {
					_ = trace.Close()
				}
			})
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			address := listener.Addr().String()
			cfg := config.DefaultConfig()
			cfg.Port = listener.Addr().(*net.TCPAddr).Port
			_ = listener.Close()
			cfg.Host, cfg.NetworkType, cfg.StorageBackend = "127.0.0.1", tc.network, config.StorageGoLog
			cfg.PersistenceEnabled, cfg.PersistenceDurability, cfg.PersistenceWriteMode = true, "sync", "strong"
			cfg.ColdStartStrategy, cfg.MaxMemoryPolicy = "load_all", "noeviction"
			cfg.DBCount, cfg.GoLogGroupMax, cfg.DataDir = 1, tc.group, t.TempDir()
			srv := NewServer(cfg)
			done := make(chan struct{})
			var startErr error
			go func() {
				defer close(done)
				startErr = srv.Start()
			}()
			t.Cleanup(func() {
				_ = srv.Stop()
				select {
				case <-done:
				case <-time.After(2 * time.Second):
					t.Error("server did not stop")
				}
			})
			var connection net.Conn
			deadline := time.Now().Add(2 * time.Second)
			for connection == nil {
				connection, err = net.DialTimeout("tcp", address, 50*time.Millisecond)
				if err == nil {
					break
				}
				select {
				case <-done:
					t.Fatalf("server stopped before accepting connections: %v", startErr)
				default:
				}
				if time.Now().After(deadline) {
					t.Fatal(err)
				}
				time.Sleep(10 * time.Millisecond)
			}
			t.Cleanup(func() { _ = connection.Close() })
			_ = connection.SetDeadline(time.Now().Add(3 * time.Second))
			const pipeline = "*1\r\n$4\r\nPING\r\n*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n*3\r\n$3\r\nset\r\n$14\r\ntrace-test-key\r\n$16\r\ntrace-test-value\r\n"
			if _, err := io.WriteString(connection, pipeline); err != nil {
				t.Fatal(err)
			}
			const reply = "+PONG\r\n+OK\r\n+OK\r\n"
			received := make([]byte, len(reply))
			if _, err := io.ReadFull(connection, received); err != nil || string(received) != reply {
				t.Fatalf("replies = %q, %v", received, err)
			}
			port := connection.LocalAddr().(*net.TCPAddr).Port
			_ = connection.Close()
			if err := srv.Stop(); err != nil {
				t.Fatal(err)
			}
			traceClosed = true
			if err := trace.Close(); err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(tracePath)
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(data, []byte("trace-test-")) {
				t.Fatal("trace contains request arguments")
			}
			lines := bytes.Split(bytes.TrimSpace(data), []byte("\n"))
			if len(lines) != 4 {
				t.Fatalf("expected header, SELECT, SET and summary, got %s", data)
			}
			var observer, row writetrace.Request
			if err := json.Unmarshal(lines[1], &observer); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(lines[2], &row); err != nil {
				t.Fatal(err)
			}
			if observer.Port != port || observer.Ordinal != 2 || observer.Command != "SELECT" || observer.ExecutionLock < observer.Dispatch || observer.Batch.ID != 0 || !observer.SendOK {
				t.Fatalf("observer trace = %+v", observer)
			}
			if row.Port != port || row.Ordinal != 3 || row.Command != "SET" || row.Batch.Size != 1 || row.Batch.Sequence != 1 || row.Batch.Failed || !row.SendOK || (row.Batch.Dequeue != 0) != (tc.group > 1) {
				t.Fatalf("SET trace = %+v", row)
			}
			phases := []int64{row.Parsed, row.Dispatch, row.ExecutionLock, row.KeyLock, row.Queue, row.Batch.Locked, row.Batch.Append, row.Batch.Write, row.Batch.Written, row.Batch.Sync, row.Batch.Synced, row.Batch.Release, row.Result, row.Encode, row.Send, row.Sent}
			for i := 1; i < len(phases); i++ {
				if phases[i-1] == 0 || phases[i-1] > phases[i] {
					t.Fatalf("unordered SET phases: %v", phases)
				}
			}
		})
	}
}
