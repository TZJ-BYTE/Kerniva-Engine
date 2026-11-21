package protocol

import (
	"bytes"
	"testing"
)

func TestParserEmptyBulkAndContinuousRequests(t *testing.T) {
	parser := NewParser(bytes.NewBufferString("*3\r\n$3\r\nSET\r\n$6\r\n中文\r\n$0\r\n\r\n*2\r\n$3\r\nGET\r\n$0\r\n\r\n"))
	request, err := parser.ParseRequest()
	if err != nil {
		t.Fatal(err)
	}
	if request.Cmd != "SET" || len(request.Args) != 2 || string(request.Args[0]) != "中文" || len(request.Args[1]) != 0 {
		t.Fatalf("empty argument corrupted: %#v", request)
	}
	ReleaseRequest(request)
	next, err := parser.ParseRequest()
	if err != nil {
		t.Fatal(err)
	}
	defer ReleaseRequest(next)
	if next.Cmd != "GET" || len(next.Args) != 1 || len(next.Args[0]) != 0 {
		t.Fatalf("next empty argument corrupted: %#v", next)
	}
}
