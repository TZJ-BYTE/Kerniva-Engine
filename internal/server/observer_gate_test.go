package server

import (
	"strings"
	"testing"
	"time"

	"github.com/TZJ-BYTE/Kerniva/internal/command"
	"github.com/TZJ-BYTE/Kerniva/internal/database"
)

func TestObserverCommandsCanRunAlongsideOrdinaryWrite(t *testing.T) {
	manager := newExecutionTestManager(t)
	connection := NewConnectionContext(manager.GetDefaultDB(), nil, nil)
	// Ordinary SET holds the shared execution gate through durable commit.
	database.LockSharedExecution()
	finished := make(chan string, 1)
	go func() {
		if reply := dispatchTestCommand(connection, manager, "SELECT", "1"); reply != "+OK\r\n" {
			finished <- reply
			return
		}
		finished <- dispatchTestCommand(connection, manager, "INFO")
	}()
	select {
	case reply := <-finished:
		database.UnlockSharedExecution()
		if !strings.Contains(reply, "\ndb_id:1\n") {
			t.Fatalf("observer did not report the selected database: %q", reply)
		}
	case <-time.After(time.Second):
		database.UnlockSharedExecution()
		<-finished
		t.Fatal("observer waited for an unrelated ordinary write")
	}
}

func TestSlowInfoAllowsIndependentGet(t *testing.T) {
	manager := newExecutionTestManager(t)
	observer := NewConnectionContext(manager.GetDefaultDB(), nil, nil)
	peer := NewConnectionContext(manager.GetDefaultDB(), nil, nil)
	dispatchTestCommand(peer, manager, "SELECT", "1")
	dispatchTestCommand(peer, manager, "SET", "witness", "value")

	// Hold the INFO handler at a deterministic barrier, as can happen
	// when GetStats waits on the durable writer for another database.
	barrier := executionBarrierCommand{make(chan struct{}, 1), make(chan struct{})}
	registry := command.NewCommandRegistry()
	registry.Register("INFO", barrier)
	infoDone := make(chan struct{})
	go func() {
		dispatchCommand(nil, observer, manager, registry, "INFO", nil)
		close(infoDone)
	}()
	defer func() {
		close(barrier.resume)
		<-infoDone
	}()
	<-barrier.entered
	read := make(chan string, 1)
	go func() { read <- dispatchTestCommand(peer, manager, "GET", "witness") }()
	select {
	case reply := <-read:
		if reply != "$5\r\nvalue\r\n" {
			t.Fatalf("GET returned %q", reply)
		}
	case <-time.After(time.Second):
		t.Fatal("DB 1 GET waited for DB 0 INFO")
	}
}
