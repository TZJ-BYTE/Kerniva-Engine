package command_test

import (
	"testing"

	"github.com/TZJ-BYTE/Kerniva/config"
	"github.com/TZJ-BYTE/Kerniva/internal/command"
	"github.com/TZJ-BYTE/Kerniva/internal/database"
	"github.com/TZJ-BYTE/Kerniva/internal/protocol"
)

func TestStreamCommands(t *testing.T) {
	command.InitDefaultCommands()
	cfg := config.DefaultConfig()
	cfg.DataDir = t.TempDir()
	manager := database.NewDBManager(cfg)
	defer manager.Close()
	db := manager.GetDefaultDB()

	nomkResp := command.DefaultRegistry.Execute(db, "XADD", [][]byte{
		[]byte("missing:stream"),
		[]byte("NOMKSTREAM"),
		[]byte("*"),
		[]byte("field"),
		[]byte("value"),
	})
	if nomkResp.Type != '$' || nomkResp.Value != nil {
		t.Fatalf("unexpected xadd nomkstream resp=%#v", nomkResp)
	}

	first := command.DefaultRegistry.Execute(db, "XADD", [][]byte{
		[]byte("stream"),
		[]byte("1000-0"),
		[]byte("field1"),
		[]byte("value1"),
		[]byte("field2"),
		[]byte("value2"),
	})
	if first.Type != '$' || first.Value == nil {
		t.Fatalf("unexpected first xadd resp=%#v", first)
	}
	firstID := first.Value.(string)

	second := command.DefaultRegistry.Execute(db, "XADD", [][]byte{
		[]byte("stream"),
		[]byte("MAXLEN"),
		[]byte("2"),
		[]byte("1001-0"),
		[]byte("field3"),
		[]byte("value3"),
	})
	if second.Type != '$' || second.Value == nil {
		t.Fatalf("unexpected second xadd resp=%#v", second)
	}
	secondID := second.Value.(string)

	assertRespInt(t, command.DefaultRegistry.Execute(db, "XLEN", [][]byte{[]byte("stream")}), 2)
	assertRespString(t, (&command.TypeCommand{}).Execute(db, [][]byte{[]byte("stream")}), '+', "stream")

	xrangeResp := command.DefaultRegistry.Execute(db, "XRANGE", [][]byte{
		[]byte("stream"),
		[]byte("-"),
		[]byte("+"),
	})
	assertStreamEntryIDs(t, xrangeResp, []string{firstID, secondID})
	assertStreamEntryFields(t, xrangeResp.Value.([]*protocol.Response)[0], []string{"field1", "value1", "field2", "value2"})

	xrangeCountResp := command.DefaultRegistry.Execute(db, "XRANGE", [][]byte{
		[]byte("stream"),
		[]byte("-"),
		[]byte("+"),
		[]byte("COUNT"),
		[]byte("1"),
	})
	assertStreamEntryIDs(t, xrangeCountResp, []string{firstID})

	xrevrangeResp := command.DefaultRegistry.Execute(db, "XREVRANGE", [][]byte{
		[]byte("stream"),
		[]byte("+"),
		[]byte("-"),
		[]byte("COUNT"),
		[]byte("1"),
	})
	assertStreamEntryIDs(t, xrevrangeResp, []string{secondID})

	assertRespInt(t, command.DefaultRegistry.Execute(db, "XDEL", [][]byte{
		[]byte("stream"),
		[]byte(firstID),
	}), 1)
	assertRespInt(t, command.DefaultRegistry.Execute(db, "XLEN", [][]byte{[]byte("stream")}), 1)

	assertRespInt(t, command.DefaultRegistry.Execute(db, "XTRIM", [][]byte{
		[]byte("stream"),
		[]byte("MAXLEN"),
		[]byte("0"),
	}), 1)
	assertRespInt(t, command.DefaultRegistry.Execute(db, "XLEN", [][]byte{[]byte("stream")}), 0)
	assertRespString(t, (&command.TypeCommand{}).Execute(db, [][]byte{[]byte("stream")}), '+', "stream")

	explicitResp := command.DefaultRegistry.Execute(db, "XADD", [][]byte{
		[]byte("stream"),
		[]byte("1002-0"),
		[]byte("f"),
		[]byte("v"),
	})
	assertRespString(t, explicitResp, '$', "1002-0")

	badResp := command.DefaultRegistry.Execute(db, "XADD", [][]byte{
		[]byte("stream"),
		[]byte("1001-0"),
		[]byte("f"),
		[]byte("v"),
	})
	if badResp.Type != '-' {
		t.Fatalf("expected xadd smaller id to fail, got %#v", badResp)
	}
}

func assertStreamEntryIDs(t *testing.T, resp *protocol.Response, expected []string) {
	t.Helper()
	if resp.Type != '*' {
		t.Fatalf("unexpected stream resp=%#v", resp)
	}
	items := resp.Value.([]*protocol.Response)
	if len(items) != len(expected) {
		t.Fatalf("unexpected stream item count=%d expected=%d", len(items), len(expected))
	}
	for i := range items {
		entryItems := items[i].Value.([]*protocol.Response)
		if len(entryItems) != 2 || entryItems[0].Type != '$' || entryItems[0].Value != expected[i] {
			t.Fatalf("unexpected stream entry payload=%#v", items[i].Value)
		}
	}
}

func assertStreamEntryFields(t *testing.T, resp *protocol.Response, expected []string) {
	t.Helper()
	if resp.Type != '*' {
		t.Fatalf("unexpected stream entry resp=%#v", resp)
	}
	items := resp.Value.([]*protocol.Response)
	if len(items) != 2 || items[1].Type != '*' {
		t.Fatalf("unexpected stream entry payload=%#v", resp.Value)
	}
	fields := items[1].Value.([]string)
	if len(fields) != len(expected) {
		t.Fatalf("unexpected stream field count=%d expected=%d", len(fields), len(expected))
	}
	for i := range fields {
		if fields[i] != expected[i] {
			t.Fatalf("unexpected stream fields=%v expected=%v", fields, expected)
		}
	}
}
