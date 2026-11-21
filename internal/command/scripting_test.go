package command_test

import (
	"crypto/sha1"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/TZJ-BYTE/Kerniva/internal/command"
	"github.com/TZJ-BYTE/Kerniva/internal/database"
	"github.com/TZJ-BYTE/Kerniva/internal/protocol"
)

func TestEvalAndEvalShaCompatibility(t *testing.T) {
	command.InitDefaultCommands()
	db := newCommandTestDB(t)
	flushScriptCache(t, db)

	assertRespString(t, command.DefaultRegistry.Execute(db, "SET", [][]byte{
		[]byte("lua:key"),
		[]byte("value"),
	}), '+', "OK")

	getResp := command.DefaultRegistry.Execute(db, "EVAL", [][]byte{
		[]byte(`return redis.call("GET", KEYS[1])`),
		[]byte("1"),
		[]byte("lua:key"),
	})
	assertRespString(t, getResp, '$', "value")

	setResp := command.DefaultRegistry.Execute(db, "EVAL", [][]byte{
		[]byte(`return redis.call("SET", KEYS[1], ARGV[1])`),
		[]byte("1"),
		[]byte("lua:key"),
		[]byte("next"),
	})
	assertRespString(t, setResp, '+', "OK")
	assertRespString(t, command.DefaultRegistry.Execute(db, "GET", [][]byte{
		[]byte("lua:key"),
	}), '$', "next")

	script := `return {KEYS[1], ARGV[1], redis.call("GET", KEYS[1]), 7}`
	sha := sha1Hex(script)
	arrayResp := command.DefaultRegistry.Execute(db, "EVAL", [][]byte{
		[]byte(script),
		[]byte("1"),
		[]byte("lua:key"),
		[]byte("arg-1"),
	})
	if arrayResp.Type != '*' {
		t.Fatalf("expected array reply, got %#v", arrayResp)
	}
	items := arrayResp.Value.([]*protocol.Response)
	if len(items) != 4 || items[0].Value != "lua:key" || items[1].Value != "arg-1" || items[2].Value != "next" || items[3].Value.(int64) != 7 {
		t.Fatalf("unexpected eval array payload=%#v", arrayResp.Value)
	}

	existsResp := command.DefaultRegistry.Execute(db, "SCRIPT", [][]byte{
		[]byte("EXISTS"),
		[]byte(sha),
		[]byte(strings.Repeat("0", 40)),
	})
	if existsResp.Type != '*' {
		t.Fatalf("expected exists array reply, got %#v", existsResp)
	}
	existsItems := existsResp.Value.([]*protocol.Response)
	if len(existsItems) != 2 || existsItems[0].Value.(int64) != 1 || existsItems[1].Value.(int64) != 0 {
		t.Fatalf("unexpected script exists payload=%#v", existsResp.Value)
	}

	evalShaResp := command.DefaultRegistry.Execute(db, "EVALSHA", [][]byte{
		[]byte(sha),
		[]byte("1"),
		[]byte("lua:key"),
		[]byte("arg-2"),
	})
	if evalShaResp.Type != '*' {
		t.Fatalf("expected evalsha array reply, got %#v", evalShaResp)
	}
	evalShaItems := evalShaResp.Value.([]*protocol.Response)
	if len(evalShaItems) != 4 || evalShaItems[1].Value != "arg-2" {
		t.Fatalf("unexpected evalsha payload=%#v", evalShaResp.Value)
	}
}

func TestScriptFlushAndNoScript(t *testing.T) {
	command.InitDefaultCommands()
	db := newCommandTestDB(t)
	flushScriptCache(t, db)

	script := `return redis.call("PING")`
	sha := sha1Hex(script)

	assertRespString(t, command.DefaultRegistry.Execute(db, "EVAL", [][]byte{
		[]byte(script),
		[]byte("0"),
	}), '+', "PONG")

	existsBefore := command.DefaultRegistry.Execute(db, "SCRIPT", [][]byte{
		[]byte("EXISTS"),
		[]byte(sha),
	})
	itemsBefore := existsBefore.Value.([]*protocol.Response)
	if len(itemsBefore) != 1 || itemsBefore[0].Value.(int64) != 1 {
		t.Fatalf("expected cached script before flush, got %#v", existsBefore.Value)
	}

	assertRespString(t, command.DefaultRegistry.Execute(db, "SCRIPT", [][]byte{
		[]byte("FLUSH"),
	}), '+', "OK")

	existsAfter := command.DefaultRegistry.Execute(db, "SCRIPT", [][]byte{
		[]byte("EXISTS"),
		[]byte(sha),
	})
	itemsAfter := existsAfter.Value.([]*protocol.Response)
	if len(itemsAfter) != 1 || itemsAfter[0].Value.(int64) != 0 {
		t.Fatalf("expected cache miss after flush, got %#v", existsAfter.Value)
	}

	noScriptResp := command.DefaultRegistry.Execute(db, "EVALSHA", [][]byte{
		[]byte(sha),
		[]byte("0"),
	})
	if noScriptResp.Type != '-' || noScriptResp.Error == nil || !strings.Contains(noScriptResp.Error.Error(), "NOSCRIPT") {
		t.Fatalf("expected NOSCRIPT error, got %#v", noScriptResp)
	}
}

func TestScriptLoadAndHelp(t *testing.T) {
	command.InitDefaultCommands()
	db := newCommandTestDB(t)
	flushScriptCache(t, db)

	script := `return ARGV[1]`
	sha := sha1Hex(script)

	loadResp := command.DefaultRegistry.Execute(db, "SCRIPT", [][]byte{
		[]byte("LOAD"),
		[]byte(script),
	})
	assertRespString(t, loadResp, '$', sha)

	existsResp := command.DefaultRegistry.Execute(db, "SCRIPT", [][]byte{
		[]byte("EXISTS"),
		[]byte(sha),
	})
	items := existsResp.Value.([]*protocol.Response)
	if len(items) != 1 || items[0].Value.(int64) != 1 {
		t.Fatalf("expected loaded script in cache, got %#v", existsResp.Value)
	}

	evalShaResp := command.DefaultRegistry.Execute(db, "EVALSHA", [][]byte{
		[]byte(sha),
		[]byte("0"),
		[]byte("loaded"),
	})
	assertRespString(t, evalShaResp, '$', "loaded")

	helpResp := command.DefaultRegistry.Execute(db, "SCRIPT", [][]byte{
		[]byte("HELP"),
	})
	if helpResp.Type != '*' {
		t.Fatalf("expected help array reply, got %#v", helpResp)
	}
	helpItems := helpResp.Value.([]string)
	if len(helpItems) < 5 || !strings.Contains(strings.Join(helpItems, "\n"), "LOAD <script>") || !strings.Contains(strings.Join(helpItems, "\n"), "HELP --") {
		t.Fatalf("unexpected script help payload=%#v", helpResp.Value)
	}
}

func TestEvalSupportsProtectedCallAndArgumentValidation(t *testing.T) {
	command.InitDefaultCommands()
	db := newCommandTestDB(t)
	flushScriptCache(t, db)

	errTableResp := command.DefaultRegistry.Execute(db, "EVAL", [][]byte{
		[]byte(`return redis.pcall("NONEXISTENT")`),
		[]byte("0"),
	})
	if errTableResp.Type != '-' || errTableResp.Error == nil || !strings.Contains(errTableResp.Error.Error(), "unknown command") {
		t.Fatalf("expected pcall error table to become error reply, got %#v", errTableResp)
	}

	wrongArity := command.DefaultRegistry.Execute(db, "EVAL", [][]byte{
		[]byte(`return 1`),
		[]byte("2"),
		[]byte("only-one-key"),
	})
	if wrongArity.Type != '-' || wrongArity.Error == nil || !strings.Contains(wrongArity.Error.Error(), "Number of keys") {
		t.Fatalf("expected key count validation error, got %#v", wrongArity)
	}
}

func TestScriptUnknownSubcommandSuggestsHelp(t *testing.T) {
	command.InitDefaultCommands()
	db := newCommandTestDB(t)
	flushScriptCache(t, db)

	resp := command.DefaultRegistry.Execute(db, "SCRIPT", [][]byte{
		[]byte("NOPE"),
	})
	if resp.Type != '-' || resp.Error == nil || !strings.Contains(resp.Error.Error(), "Try SCRIPT HELP.") {
		t.Fatalf("expected help hint for unknown script subcommand, got %#v", resp)
	}
}

func flushScriptCache(t *testing.T, db *database.Database) {
	t.Helper()
	resp := command.DefaultRegistry.Execute(db, "SCRIPT", [][]byte{
		[]byte("FLUSH"),
	})
	if resp.Type != '+' || resp.Value != "OK" {
		t.Fatalf("failed to flush script cache: %#v", resp)
	}
}

func sha1Hex(source string) string {
	sum := sha1.Sum([]byte(source))
	return hex.EncodeToString(sum[:])
}
