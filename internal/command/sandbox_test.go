package command_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/TZJ-BYTE/Kerniva/internal/command"
	"github.com/TZJ-BYTE/Kerniva/internal/database"
	"github.com/TZJ-BYTE/Kerniva/internal/protocol"
)

func TestLuaSandboxRejectsHostLibrariesAndResourceAbuse(t *testing.T) {
	command.InitDefaultCommands()
	db := database.NewDatabase(0)
	defer db.Close()
	resp := command.DefaultRegistry.Execute(db, "EVAL", [][]byte{[]byte(`return {type(os),type(io),type(package),type(debug),type(dofile),type(loadfile),type(require)}`), []byte("0")})
	if resp.Type != protocol.Array {
		t.Fatalf("library inspection=%#v", resp)
	}
	for _, item := range resp.Value.([]*protocol.Response) {
		if item.Value != "nil" {
			t.Fatalf("host entry exposed=%#v", item)
		}
	}
	protocol.ReleaseResponse(resp)
	for _, source := range []string{
		`while true do end`,
		`return string.rep("x", 1000000000)`,
		`return string.format("%999999999s", "x")`,
		`local a={} a[1]=a return a`,
	} {
		started := time.Now()
		resp := command.DefaultRegistry.Execute(db, "EVAL", [][]byte{[]byte(source), []byte("0")})
		if resp.Type != protocol.Error {
			t.Fatalf("unbounded script accepted=%#v", resp)
		}
		if time.Since(started) > 2*time.Second {
			t.Fatalf("resource cancellation took too long")
		}
		protocol.ReleaseResponse(resp)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	resp = command.DefaultRegistry.ExecuteContext(ctx, db, "EVAL", [][]byte{[]byte(`return 1`), []byte("0")})
	if resp.Type != protocol.Error || !strings.Contains(resp.Error.Error(), "canceled") {
		t.Fatalf("canceled script=%#v", resp)
	}
	protocol.ReleaseResponse(resp)
}

func TestCommandMetadataReportsWritesAndExtractsKeys(t *testing.T) {
	command.InitDefaultCommands()
	db := database.NewDatabase(0)
	defer db.Close()
	info := command.DefaultRegistry.Execute(db, "COMMAND", [][]byte{[]byte("INFO"), []byte("SET")})
	row := info.Value.([]*protocol.Response)[0].Value.([]*protocol.Response)
	if row[1].Value != int64(-3) || row[2].Value.([]string)[0] != "write" {
		t.Fatalf("SET metadata=%#v", row)
	}
	keys := command.DefaultRegistry.Execute(db, "COMMAND", [][]byte{[]byte("GETKEYS"), []byte("MSET"), []byte("a"), []byte("1"), []byte("b"), []byte("2")})
	if got := keys.Value.([]string); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("MSET keys=%#v", got)
	}
}
