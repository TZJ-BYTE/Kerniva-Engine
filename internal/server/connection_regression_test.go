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
	t.Cleanup(func() { _ = clientConn.Close() })
	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.handleConnection(serverConn)
	}()
	_ = clientConn.SetDeadline(time.Now().Add(time.Second))
	if _, err := io.WriteString(clientConn, "*2\r\n$5\r\nWATCH\r\n$7\r\nwatched\r\n"); err != nil {
		t.Fatal(err)
	}
	reply := make([]byte, len("+OK\r\n"))
	if _, err := io.ReadFull(clientConn, reply); err != nil || string(reply) != "+OK\r\n" {
		t.Fatalf("WATCH replied %q, %v", reply, err)
	}
	db := srv.dbManager.GetDefaultDB()
	if err := db.SetStringBytes([]byte("watched"), []byte("changed")); err != nil {
		t.Fatal(err)
	}
	if version := db.KeyVersion("watched"); version != 1 {
		t.Fatalf("active WATCH version = %d, want 1", version)
	}

	payload := "*3\r\n$5\r\nBLPOP\r\n$7\r\nmissing\r\n$1\r\n0\r\n" + strings.Repeat("*1\r\n$4\r\nPING\r\n", 256)
	_ = clientConn.SetWriteDeadline(time.Now().Add(time.Second))
	_, _ = clientConn.Write([]byte(payload))
	_ = clientConn.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("full pipeline retained a blocked connection after disconnect")
	}
	if version := db.KeyVersion("watched"); version != 0 {
		t.Fatalf("disconnect retained WATCH version %d", version)
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
			if err := conn.(*net.TCPConn).SetReadBuffer(1024); err != nil {
				t.Fatal(err)
			}
			var requests, replies strings.Builder
			for i := range 512 {
				message := fmt.Sprintf("reply-%d", i)
				if i == 256 {
					// Force the response to outgrow the connection's 512-byte buffer.
					message = strings.Repeat("large-reply-", 512)
				}
				fmt.Fprintf(&requests, "*2\r\n$4\r\nECHO\r\n$%d\r\n%s\r\n", len(message), message)
				fmt.Fprintf(&replies, "$%d\r\n%s\r\n", len(message), message)
			}
			if _, err := io.WriteString(conn, requests.String()); err != nil {
				t.Fatal(err)
			}
			got := make([]byte, replies.Len())
			for offset := 0; offset < len(got); {
				end := min(offset+257, len(got))
				if n, err := io.ReadFull(conn, got[offset:end]); err != nil {
					t.Fatalf("read %d of %d response bytes: %v", offset+n, len(got), err)
				}
				offset = end
			}
			if !bytes.Equal(got, []byte(replies.String())) {
				t.Fatal("pipeline replies were reordered or lost")
			}
			const blockingPipeline = "*2\r\n$4\r\nECHO\r\n$6\r\nbefore\r\n" +
				"*3\r\n$5\r\nBLPOP\r\n$7\r\nrelease\r\n$1\r\n0\r\n" +
				"*2\r\n$4\r\nECHO\r\n$5\r\nafter\r\n"
			if _, err := io.WriteString(conn, blockingPipeline); err != nil {
				t.Fatal(err)
			}
			expect := func(conn net.Conn, want string) {
				t.Helper()
				got := make([]byte, len(want))
				if _, err := io.ReadFull(conn, got); err != nil || string(got) != want {
					t.Fatalf("reply = %q, error = %v, want %q", got, err, want)
				}
			}
			// The client must receive this reply while BLPOP is still blocked.
			expect(conn, "$6\r\nbefore\r\n")
			peer := dial()
			if _, err := io.WriteString(peer, "*3\r\n$5\r\nLPUSH\r\n$7\r\nrelease\r\n$5\r\nvalue\r\n"); err != nil {
				t.Fatal(err)
			}
			expect(peer, ":1\r\n")
			expect(conn, "*2\r\n$7\r\nrelease\r\n$5\r\nvalue\r\n$5\r\nafter\r\n")
			if _, err := io.WriteString(conn, "*1\r\n$4\r\nQUIT\r\n"); err != nil {
				t.Fatal(err)
			}
			expect(conn, "+OK\r\n")
		})
	}
}
