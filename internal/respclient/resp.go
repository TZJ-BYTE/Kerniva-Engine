// Package respclient implements the RESP2 client framing used by the CLI and demo.
package respclient

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// Reply preserves null values, empty strings and nested transaction replies.
type Reply struct {
	Type  byte
	Text  string
	Items []Reply
	Null  bool
}

// Request encodes byte lengths, including UTF-8 and empty arguments.
func Request(arguments ...string) []byte {
	request := append([]byte{'*'}, strconv.Itoa(len(arguments))...)
	request = append(request, '\r', '\n')
	for _, argument := range arguments {
		request = append(request, '$')
		request = strconv.AppendInt(request, int64(len(argument)), 10)
		request = append(request, '\r', '\n')
		request = append(request, argument...)
		request = append(request, '\r', '\n')
	}
	return request
}

// WriteRequest handles partial writes without losing the rest of a command.
func WriteRequest(writer io.Writer, arguments ...string) error {
	for remaining := Request(arguments...); len(remaining) > 0; {
		written, err := writer.Write(remaining)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		remaining = remaining[written:]
	}
	return nil
}

// ReadReply consumes exactly one reply. Reuse reader for the whole connection.
func ReadReply(reader *bufio.Reader) (Reply, error) {
	return readReply(reader, 0)
}

func readReply(reader *bufio.Reader, depth int) (Reply, error) {
	if depth > 128 {
		return Reply{}, fmt.Errorf("RESP reply nesting exceeds 128 levels")
	}
	line, err := reader.ReadString('\n')
	if err != nil {
		return Reply{}, err
	}
	if len(line) < 3 || !strings.HasSuffix(line, "\r\n") {
		return Reply{}, fmt.Errorf("invalid RESP reply header %q", line)
	}
	reply := Reply{Type: line[0], Text: line[1 : len(line)-2]}
	switch reply.Type {
	case '+', '-':
		return reply, nil
	case ':':
		if _, err := strconv.ParseInt(reply.Text, 10, 64); err != nil {
			return Reply{}, fmt.Errorf("invalid RESP integer: %w", err)
		}
		return reply, nil
	case '$', '*':
		length, err := strconv.ParseInt(reply.Text, 10, 64)
		if err != nil || length < -1 {
			return Reply{}, fmt.Errorf("invalid RESP length %q", reply.Text)
		}
		reply.Text = ""
		if length == -1 {
			reply.Null = true
			return reply, nil
		}
		if reply.Type == '$' {
			if length > 512<<20 {
				return Reply{}, fmt.Errorf("RESP bulk reply exceeds 512 MiB")
			}
			payload := make([]byte, int(length)+2)
			if _, err := io.ReadFull(reader, payload); err != nil {
				return Reply{}, err
			}
			if payload[length] != '\r' || payload[length+1] != '\n' {
				return Reply{}, fmt.Errorf("invalid RESP bulk terminator")
			}
			reply.Text = string(payload[:length])
			return reply, nil
		}
		if length > 1<<20 {
			return Reply{}, fmt.Errorf("RESP array reply exceeds 1048576 elements")
		}
		reply.Items = make([]Reply, 0, min(int(length), 1024))
		for index := int64(0); index < length; index++ {
			item, err := readReply(reader, depth+1)
			if err != nil {
				return Reply{}, err
			}
			reply.Items = append(reply.Items, item)
		}
		return reply, nil
	default:
		return Reply{}, fmt.Errorf("unsupported RESP reply type %q", reply.Type)
	}
}

// String renders typed replies while distinguishing a missing value from "".
func (r Reply) String() string {
	if r.Null {
		return "(nil)"
	}
	switch r.Type {
	case '-':
		return "(error) " + r.Text
	case ':':
		return "(integer) " + r.Text
	case '$':
		return strconv.Quote(r.Text)
	case '*':
		items := make([]string, len(r.Items))
		for index, item := range r.Items {
			items[index] = item.String()
		}
		return "[" + strings.Join(items, ", ") + "]"
	default:
		return r.Text
	}
}
