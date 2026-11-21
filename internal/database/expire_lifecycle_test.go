package database

import (
	"sync"
	"testing"
	"time"
)

func TestExpireCleanerConcurrentRestart(t *testing.T) {
	db := NewDatabase(0)
	t.Cleanup(func() { _ = db.Close() })
	var workers sync.WaitGroup
	for worker := 0; worker < 12; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for iteration := 0; iteration < 100; iteration++ {
				db.StartExpireCleaner()
				db.StopExpireCleaner()
			}
		}()
	}
	finished := make(chan struct{})
	go func() {
		workers.Wait()
		close(finished)
	}()
	waitPersistentSignal(t, finished)
	db.StopExpireCleaner()
	db.expireMu.Lock()
	running := db.expireStop != nil
	db.expireMu.Unlock()
	if running {
		t.Fatal("expiration worker survived final stop")
	}
}

func TestExpireCleanerConcurrentStartStopAndClose(t *testing.T) {
	db := NewDatabase(0)
	t.Cleanup(func() { _ = db.Close() })
	begin := make(chan struct{})
	var workers sync.WaitGroup
	for worker := 0; worker < 12; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-begin
			for iteration := 0; iteration < 100; iteration++ {
				db.StartExpireCleaner()
				db.StopExpireCleaner()
			}
		}()
	}
	workers.Add(1)
	go func() {
		defer workers.Done()
		<-begin
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()
	close(begin)
	finished := make(chan struct{})
	go func() {
		workers.Wait()
		close(finished)
	}()
	waitPersistentSignal(t, finished)
	db.StartExpireCleaner()
	db.expireMu.Lock()
	closed, running := db.expireClosed, db.expireStop != nil
	db.expireMu.Unlock()
	if !closed || running {
		t.Fatal("closed database restarted its expiration worker")
	}
}

func TestExpireCleanerStopWhileExecutionGateIsExclusive(t *testing.T) {
	db := NewDatabase(0)
	t.Cleanup(func() { _ = db.Close() })
	db.StartExpireCleaner()
	LockExecution()
	// Let at least one periodic scan attempt to enter the exclusive gate.
	time.Sleep(250 * time.Millisecond)
	stopped := make(chan struct{})
	go func() {
		db.StopExpireCleaner()
		close(stopped)
	}()
	select {
	case <-stopped:
		UnlockExecution()
	case <-time.After(2 * time.Second):
		UnlockExecution()
		waitPersistentSignal(t, stopped)
		t.Fatal("expiration shutdown waited for the foreground execution gate")
	}
}
