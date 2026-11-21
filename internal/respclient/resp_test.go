package respclient

import (
	"bufio"
	"bytes"
	"io"
	"strings"
	"testing"
)

type fragmentedReader struct{ io.Reader }

func (r fragmentedReader) Read(payload []byte) (int, error) {
	return r.Reader.Read(payload[:min(len(payload), 2)])
}

func TestReadReplyFragmentedBulkAndNestedEXEC(t *testing.T) {
	wire := "*4\r\n+OK\r\n$6\r\n中文\r\n*3\r\n$0\r\n\r\n$-1\r\n:123\r\n-ERR example\r\n+PONG\r\n"
	for _, fragmented := range []bool{false, true} {
		var source io.Reader = strings.NewReader(wire)
		if fragmented {
			source = fragmentedReader{source}
		}
		reader := bufio.NewReader(source)
		reply, err := ReadReply(reader)
		if err != nil {
			t.Fatal(err)
		}
		if len(reply.Items) != 4 || reply.Items[1].Text != "中文" || len(reply.Items[2].Items) != 3 {
			t.Fatalf("lost nested reply: %#v", reply)
		}
		nested := reply.Items[2].Items
		if nested[0].Null || nested[0].Text != "" || !nested[1].Null || nested[2].Text != "123" {
			t.Fatalf("empty, null or integer corrupted: %#v", nested)
		}
		next, err := ReadReply(reader)
		if err != nil || next.Text != "PONG" {
			t.Fatalf("next reply consumed or lost: reply=%#v err=%v", next, err)
		}
	}
}

func TestReadReplyRejectsMalformedFrames(t *testing.T) {
	for _, wire := range []string{
		"+OK\n", "$-2\r\n", "$wat\r\n", "$536870913\r\n", "$2\r\na\r\n",
		"$2\r\nabxx", "*1048577\r\n", ":invalid\r\n", "?what\r\n",
		strings.Repeat("*1\r\n", 130) + "+OK\r\n",
	} {
		if reply, err := ReadReply(bufio.NewReader(strings.NewReader(wire))); err == nil {
			t.Fatalf("accepted malformed frame %q as %#v", wire, reply)
		}
	}
}

type partialWriter struct{ bytes.Buffer }

func (w *partialWriter) Write(payload []byte) (int, error) {
	return w.Buffer.Write(payload[:min(len(payload), 3)])
}

func TestWriteRequestUTF8EmptyAndPartialWrites(t *testing.T) {
	var writer partialWriter
	if err := WriteRequest(&writer, "SET", "中文", ""); err != nil {
		t.Fatal(err)
	}
	if got, want := writer.String(), "*3\r\n$3\r\nSET\r\n$6\r\n中文\r\n$0\r\n\r\n"; got != want {
		t.Fatalf("request=%q want=%q", got, want)
	}
}
