package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/TZJ-BYTE/Kerniva/internal/respclient"
)

func TestSplitCommandQuotesEscapesAndEmptyArguments(t *testing.T) {
	for _, test := range []struct {
		input string
		want  []string
	}{
		{`SET "中文 键" ""`, []string{"SET", "中文 键", ""}},
		{`SET 'key name' 'it\'s \\ ok'`, []string{"SET", "key name", "it's \\ ok"}},
		{`SET key "a\n\r\t\x00\"b"`, []string{"SET", "key", "a\n\r\t\x00\"b"}},
		{`SET spaced\ key one"two"'三'`, []string{"SET", "spaced key", "onetwo三"}},
		{" \t\r\n", nil},
	} {
		got, err := splitCommand(test.input)
		if err != nil || !reflect.DeepEqual(got, test.want) {
			t.Fatalf("split %q = %#v, %v; want %#v", test.input, got, err, test.want)
		}
	}
	for _, input := range []string{`SET "open`, `SET key \`, `SET key \x0`, `SET key \xgg`} {
		if _, err := splitCommand(input); err == nil {
			t.Fatalf("accepted incomplete command %q", input)
		}
	}
}

func TestInteractFragmentedEXECAndFinalLineWithoutNewline(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() { client.Close(); server.Close() })
	if err := client.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	commands := [][]string{{"SET", "中文 键", ""}, {"MULTI"}, {"GET", "中文 键"}, {"EXEC"}, {"PING"}}
	replies := []string{"+OK\r\n", "+OK\r\n", "+QUEUED\r\n", "*2\r\n$0\r\n\r\n*2\r\n$6\r\n中文\r\n$-1\r\n", "+PONG\r\n"}
	done := make(chan error, 1)
	go func() {
		reader := bufio.NewReader(server)
		for index, want := range commands {
			request, err := respclient.ReadReply(reader)
			if err != nil {
				done <- err
				return
			}
			got := make([]string, 0, len(request.Items))
			for _, arg := range request.Items {
				got = append(got, arg.Text)
			}
			if !reflect.DeepEqual(got, want) {
				done <- fmt.Errorf("received %#v, expected %#v", got, want)
				return
			}
			for _, fragment := range []byte(replies[index]) {
				if _, err := server.Write([]byte{fragment}); err != nil {
					done <- err
					return
				}
			}
		}
		done <- nil
	}()
	var output bytes.Buffer
	if err := interact(client, strings.NewReader("SET \"中文 键\" \"\"\nMULTI\nGET \"中文 键\"\nEXEC\nPING"), &output); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), `["", ["中文", (nil)]]`) || !strings.Contains(output.String(), "PONG") {
		t.Fatalf("EXEC or next reply lost: %s", output.String())
	}
}

type bufferedConn struct {
	io.Reader
	bytes.Buffer
}

func (c *bufferedConn) Read(p []byte) (int, error)       { return c.Reader.Read(p) }
func (c *bufferedConn) Close() error                     { return nil }
func (c *bufferedConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (c *bufferedConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (c *bufferedConn) SetDeadline(time.Time) error      { return nil }
func (c *bufferedConn) SetReadDeadline(time.Time) error  { return nil }
func (c *bufferedConn) SetWriteDeadline(time.Time) error { return nil }

func TestInteractKeepsConnectionReaderAcrossCommands(t *testing.T) {
	connection := &bufferedConn{Reader: strings.NewReader("$6\r\n中文\r\n$0\r\n\r\n+PONG\r\n")}
	var output bytes.Buffer
	if err := interact(connection, strings.NewReader("GET key\nGET empty\nPING\n"), &output); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), `"中文"`) || !strings.Contains(output.String(), `""`) || !strings.Contains(output.String(), "PONG") {
		t.Fatalf("buffered continuous replies lost: %s", output.String())
	}
}
