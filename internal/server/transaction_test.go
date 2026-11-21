package server

import (
	"testing"

	"github.com/TZJ-BYTE/Kerniva/config"
	"github.com/TZJ-BYTE/Kerniva/internal/command"
	"github.com/TZJ-BYTE/Kerniva/internal/database"
)

func TestDispatchCopyAndMove(t *testing.T) {
	command.InitDefaultCommands()

	cfg := config.DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.DBCount = 4
	manager := database.NewDBManager(cfg)
	defer manager.Close()

	ctx := NewConnectionContext(manager.GetDefaultDB(), nil, nil)

	var out []byte
	out, _ = dispatchCommand(out, ctx, manager, command.DefaultRegistry, "SET", [][]byte{[]byte("copy:src"), []byte("value")})
	if string(out) != "+OK\r\n" {
		t.Fatalf("SET resp=%q", string(out))
	}

	out = out[:0]
	out, _ = dispatchCommand(out, ctx, manager, command.DefaultRegistry, "COPY", [][]byte{
		[]byte("copy:src"),
		[]byte("copy:dst"),
	})
	if string(out) != ":1\r\n" {
		t.Fatalf("COPY same db resp=%q", string(out))
	}
	if got, ok, err := ctx.DB.GetStringCopy([]byte("copy:dst")); err != nil || !ok || got != "value" {
		t.Fatalf("copied value=%q ok=%v err=%v", got, ok, err)
	}

	out = out[:0]
	out, _ = dispatchCommand(out, ctx, manager, command.DefaultRegistry, "COPY", [][]byte{
		[]byte("copy:src"),
		[]byte("copy:remote"),
		[]byte("DB"),
		[]byte("1"),
	})
	if string(out) != ":1\r\n" {
		t.Fatalf("COPY remote db resp=%q", string(out))
	}
	remoteDB, err := manager.GetDBByIndex(1)
	if err != nil {
		t.Fatalf("GetDBByIndex: %v", err)
	}
	if got, ok, err := remoteDB.GetStringCopy([]byte("copy:remote")); err != nil || !ok || got != "value" {
		t.Fatalf("remote copied value=%q ok=%v err=%v", got, ok, err)
	}

	out = out[:0]
	out, _ = dispatchCommand(out, ctx, manager, command.DefaultRegistry, "MOVE", [][]byte{
		[]byte("copy:src"),
		[]byte("2"),
	})
	if string(out) != ":1\r\n" {
		t.Fatalf("MOVE resp=%q", string(out))
	}
	if ctx.DB.Exists("copy:src") {
		t.Fatalf("expected source key removed after MOVE")
	}
	movedDB, err := manager.GetDBByIndex(2)
	if err != nil {
		t.Fatalf("GetDBByIndex moved: %v", err)
	}
	if got, ok, err := movedDB.GetStringCopy([]byte("copy:src")); err != nil || !ok || got != "value" {
		t.Fatalf("moved value=%q ok=%v err=%v", got, ok, err)
	}
}
