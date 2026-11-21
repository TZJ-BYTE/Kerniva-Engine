package server

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/TZJ-BYTE/Kerniva/config"
)

func TestStdBlockedFullPipelineCancelsOnDisconnect(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.PersistenceEnabled = false
	srv := NewStdServer(cfg).(*StdServer)
	t.Cleanup(func() { _ = srv.Stop() })
	serverConn, clientConn := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.handleConnection(serverConn)
	}()
	payload := "*3\r\n$5\r\nBLPOP\r\n$7\r\nmissing\r\n$1\r\n0\r\n" + strings.Repeat("*1\r\n$4\r\nPING\r\n", 256)
	_ = clientConn.SetWriteDeadline(time.Now().Add(time.Second))
	_, _ = clientConn.Write([]byte(payload))
	_ = clientConn.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("full pipeline retained a blocked connection after disconnect")
	}
}

func TestNetworkPipelineRefillsAndBlockingClientsRemainIndependent(t *testing.T) {
	for _, networkType := range []string{"std", "gnet"} {
		t.Run(networkType, func(t *testing.T) {
			reserved, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			port := reserved.Addr().(*net.TCPAddr).Port
			_ = reserved.Close()
			cfg := config.DefaultConfig()
			cfg.PersistenceEnabled = false
			cfg.DBCount = 1
			cfg.Host, cfg.Port, cfg.NetworkType = "127.0.0.1", port, networkType
			srv := NewServer(cfg)
			started := make(chan error, 1)
			go func() { started <- srv.Start() }()
			t.Cleanup(func() {
				if err := srv.Stop(); err != nil {
					t.Error(err)
				}
				select {
				case err := <-started:
					if err != nil {
						t.Error(err)
					}
				case <-time.After(2 * time.Second):
					t.Error("server did not stop")
				}
			})
			addr := fmt.Sprintf("127.0.0.1:%d", port)
			dial := func() net.Conn {
				t.Helper()
				deadline := time.Now().Add(2 * time.Second)
				for {
					conn, err := net.DialTimeout("tcp", addr, 50*time.Millisecond)
					if err == nil {
						t.Cleanup(func() { _ = conn.Close() })
						_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
						return conn
					}
					if time.Now().After(deadline) {
						t.Fatal(err)
					}
					time.Sleep(10 * time.Millisecond)
				}
			}
			for i := 0; i < 2; i++ {
				blocked := dial()
				if _, err := io.WriteString(blocked, "*3\r\n$5\r\nBLPOP\r\n$7\r\nmissing\r\n$1\r\n0\r\n"); err != nil {
					t.Fatal(err)
				}
			}
			conn := dial()
			if _, err := io.WriteString(conn, strings.Repeat("*1\r\n$4\r\nPING\r\n", 512)); err != nil {
				t.Fatal(err)
			}
			got := make([]byte, 512*len("+PONG\r\n"))
			if n, err := io.ReadFull(conn, got); err != nil {
				t.Fatalf("read %d of %d response bytes: %v", n, len(got), err)
			}
			if !bytes.Equal(got, bytes.Repeat([]byte("+PONG\r\n"), 512)) {
				t.Fatal("pipeline replies were reordered or lost")
			}
			if _, err := io.WriteString(conn, "*1\r\n$4\r\nQUIT\r\n"); err != nil {
				t.Fatal(err)
			}
			quit := make([]byte, len("+OK\r\n"))
			if _, err := io.ReadFull(conn, quit); err != nil || string(quit) != "+OK\r\n" {
				t.Fatalf("QUIT reply=%q err=%v", quit, err)
			}
		})
	}
}
