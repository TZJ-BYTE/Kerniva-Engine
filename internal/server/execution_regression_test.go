package server

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/TZJ-BYTE/Kerniva/config"
	"github.com/TZJ-BYTE/Kerniva/internal/command"
	"github.com/TZJ-BYTE/Kerniva/internal/database"
	"github.com/TZJ-BYTE/Kerniva/internal/protocol"
)

func newExecutionTestManager(t *testing.T) *database.DBManager {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.PersistenceEnabled = false
	cfg.DBCount = 2
	manager := database.NewDBManager(cfg)
	t.Cleanup(func() { _ = manager.Close() })
	command.InitDefaultCommands()
	return manager
}

func dispatchTestCommand(ctx *ConnectionContext, manager *database.DBManager, name string, args ...string) string {
	raw := make([][]byte, len(args))
	for i, arg := range args {
		raw[i] = []byte(arg)
	}
	out, _ := dispatchCommand(nil, ctx, manager, command.DefaultRegistry, name, raw)
	return string(out)
}

func TestWatchIgnoresUnrelatedWrites(t *testing.T) {
	manager := newExecutionTestManager(t)
	ctx := NewConnectionContext(manager.GetDefaultDB(), nil, nil)
	peer := NewConnectionContext(manager.GetDefaultDB(), nil, nil)
	dispatchTestCommand(ctx, manager, "WATCH", "watched")
	dispatchTestCommand(peer, manager, "SET", "unrelated", "value")
	dispatchTestCommand(ctx, manager, "MULTI")
	dispatchTestCommand(ctx, manager, "SET", "watched", "value")
	if got := dispatchTestCommand(ctx, manager, "EXEC"); got != "*1\r\n+OK\r\n" {
		t.Fatalf("EXEC after unrelated write=%q", got)
	}
	dispatchTestCommand(ctx, manager, "WATCH", "watched")
	dispatchTestCommand(peer, manager, "SET", "watched", "changed")
	dispatchTestCommand(ctx, manager, "WATCH", "watched")
	dispatchTestCommand(ctx, manager, "MULTI")
	if got := dispatchTestCommand(ctx, manager, "EXEC"); got != "*-1\r\n" {
		t.Fatalf("repeated WATCH must preserve conflict=%q", got)
	}
}

func TestCopyAndMoveAreQueuedUntilExec(t *testing.T) {
	for _, name := range []string{"COPY", "MOVE"} {
		t.Run(name, func(t *testing.T) {
			manager := newExecutionTestManager(t)
			ctx := NewConnectionContext(manager.GetDefaultDB(), nil, nil)
			dispatchTestCommand(ctx, manager, "SET", "source", "value")
			dispatchTestCommand(ctx, manager, "MULTI")
			args := []string{"source", "destination"}
			if name == "MOVE" {
				args[1] = "1"
			}
			if got := dispatchTestCommand(ctx, manager, name, args...); got != "+QUEUED\r\n" {
				t.Fatalf("%s=%q", name, got)
			}
			dispatchTestCommand(ctx, manager, "DISCARD")
			other, _ := manager.GetDBByIndex(1)
			if !ctx.DB.Exists("source") || ctx.DB.Exists("destination") || other.Exists("source") {
				t.Fatal("discarded command modified data")
			}
		})
	}
}

func TestTransactionArityErrorAbortsQueue(t *testing.T) {
	manager := newExecutionTestManager(t)
	ctx := NewConnectionContext(manager.GetDefaultDB(), nil, nil)
	dispatchTestCommand(ctx, manager, "MULTI")
	if got := dispatchTestCommand(ctx, manager, "GET"); !strings.HasPrefix(got, "-ERR") {
		t.Fatalf("invalid GET=%q", got)
	}
	if got := dispatchTestCommand(ctx, manager, "EXEC"); !strings.HasPrefix(got, "-EXECABORT") {
		t.Fatalf("EXEC=%q", got)
	}
}

type executionBarrierCommand struct{ entered, resume chan struct{} }

func (c executionBarrierCommand) Execute(_ *database.Database, _ [][]byte) *protocol.Response {
	c.entered <- struct{}{}
	<-c.resume
	return protocol.MakeSimpleString("OK")
}

func TestExecExcludesConcurrentFastPathReads(t *testing.T) {
	manager := newExecutionTestManager(t)
	ctx := NewConnectionContext(manager.GetDefaultDB(), nil, nil)
	peer := NewConnectionContext(manager.GetDefaultDB(), nil, nil)
	barrier := executionBarrierCommand{make(chan struct{}, 1), make(chan struct{})}
	var release sync.Once
	defer release.Do(func() { close(barrier.resume) })
	registry := command.NewCommandRegistry()
	registry.Register("SET", &command.SetCommand{})
	registry.Register("GET", &command.GetCommand{})
	registry.Register("TEST-EXEC-BARRIER", barrier)
	execute := func(connection *ConnectionContext, name string, args ...string) string {
		raw := make([][]byte, len(args))
		for i, arg := range args {
			raw[i] = []byte(arg)
		}
		out, _ := dispatchCommand(nil, connection, manager, registry, name, raw)
		return string(out)
	}
	execute(ctx, "MULTI")
	execute(ctx, "SET", "key", "intermediate")
	execute(ctx, "TEST-EXEC-BARRIER")
	execute(ctx, "SET", "key", "final")
	finished := make(chan string, 1)
	go func() { finished <- execute(ctx, "EXEC") }()
	select {
	case <-barrier.entered:
	case <-time.After(time.Second):
		t.Fatal("EXEC did not enter barrier")
	}
	read := make(chan string, 1)
	go func() { read <- execute(peer, "GET", "key") }()
	select {
	case got := <-read:
		t.Fatalf("read entered active transaction=%q", got)
	case <-time.After(20 * time.Millisecond):
	}
	release.Do(func() { close(barrier.resume) })
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("EXEC did not finish")
	}
	select {
	case got := <-read:
		if got != "$5\r\nfinal\r\n" {
			t.Fatalf("GET=%q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("reader did not resume")
	}
}

func TestStreamReadsUseNotificationsAndCorrectBlockModes(t *testing.T) {
	manager := newExecutionTestManager(t)
	ctx := NewConnectionContext(manager.GetDefaultDB(), nil, nil)
	args := [][]byte{[]byte("STREAMS"), []byte("stream"), []byte("0-0")}
	resp := executeXReadBlocking(ctx, args)
	if resp.Type != protocol.Array || resp.Value != nil {
		t.Fatalf("nonblocking empty XREAD=%#v", resp)
	}
	protocol.ReleaseResponse(resp)
	plan, err := makeBlockingPlan(ctx, "XREAD", [][]byte{[]byte("BLOCK"), []byte("0"), []byte("STREAMS"), []byte("stream"), []byte("$")})
	if err != nil || !plan.wait {
		t.Fatalf("BLOCK 0 plan=%#v err=%v", plan, err)
	}
	if _, err := ctx.DB.XAdd("stream", "1-0", []string{"f", "v"}, -1, false); err != nil {
		t.Fatal(err)
	}
	resp, err = plan.try()
	if err != nil || resp == nil {
		t.Fatalf("$ cursor skipped future message, resp=%#v err=%v", resp, err)
	}
	protocol.ReleaseResponse(resp)
	blocked, cancel := context.WithCancel(context.Background())
	waiting := newConnectionContext(blocked, ctx.DB, nil, nil)
	defer waiting.Cancel()
	done := make(chan *protocol.Response, 1)
	go func() {
		done <- executeXReadBlocking(waiting, [][]byte{[]byte("BLOCK"), []byte("0"), []byte("STREAMS"), []byte("missing"), []byte("0-0")})
	}()
	cancel()
	select {
	case result := <-done:
		protocol.ReleaseResponse(result)
	case <-time.After(time.Second):
		t.Fatal("cancel did not release blocked reader")
	}
}

func TestClientConnectionMetadata(t *testing.T) {
	manager := newExecutionTestManager(t)
	ctx := NewConnectionContext(manager.GetDefaultDB(), nil, nil)
	if got := dispatchTestCommand(ctx, manager, "CLIENT", "SETNAME", "test-client"); got != "+OK\r\n" {
		t.Fatal(got)
	}
	if got := dispatchTestCommand(ctx, manager, "CLIENT", "GETNAME"); got != "$11\r\ntest-client\r\n" {
		t.Fatal(got)
	}
	if got := dispatchTestCommand(ctx, manager, "CLIENT", "TRACKING", "ON"); !strings.HasPrefix(got, "-ERR") {
		t.Fatalf("unsupported CLIENT TRACKING=%q", got)
	}
}
