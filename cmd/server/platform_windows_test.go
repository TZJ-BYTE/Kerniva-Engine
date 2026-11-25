package main

import (
	"errors"
	"testing"
	"time"

	"golang.org/x/sys/windows/svc"
)

func TestWindowsServiceReportsProgressUntilReadyAndStopped(t *testing.T) {
	requests := make(chan svc.ChangeRequest, 1)
	states := make(chan svc.Status, 1)
	stop := make(chan struct{})
	ready := make(chan struct{})
	stopped := make(chan error, 1)
	ticks := make(chan time.Time, 1)
	done := make(chan error, 1)
	go func() {
		done <- monitorWindowsService(
			svc.Status{State: svc.StartPending, CheckPoint: 1, WaitHint: 30000},
			requests, states, stop, ready, stopped, ticks,
		)
	}()

	got := func(want svc.State, checkpoint uint32) {
		t.Helper()
		select {
		case state := <-states:
			if state.State != want || state.CheckPoint != checkpoint {
				t.Fatalf("status = %+v, want state %v and checkpoint %d", state, want, checkpoint)
			}
			if checkpoint > 0 && state.WaitHint != 30000 {
				t.Fatalf("pending wait hint = %d, want 30000", state.WaitHint)
			}
		case <-time.After(time.Second):
			t.Fatalf("missing status %v checkpoint %d", want, checkpoint)
		}
	}
	for checkpoint := uint32(2); checkpoint <= 8; checkpoint++ {
		ticks <- time.Now()
		got(svc.StartPending, checkpoint)
	}
	close(ready)
	got(svc.Running, 0)
	requests <- svc.ChangeRequest{Cmd: svc.Stop}
	got(svc.StopPending, 1)
	select {
	case <-stop:
	case <-time.After(time.Second):
		t.Fatal("missing shutdown signal")
	}
	for checkpoint := uint32(2); checkpoint <= 8; checkpoint++ {
		ticks <- time.Now()
		got(svc.StopPending, checkpoint)
	}
	requests <- svc.ChangeRequest{Cmd: svc.Interrogate}
	got(svc.StopPending, 8)

	want := errors.New("service write failed")
	stopped <- want
	select {
	case err := <-done:
		if !errors.Is(err, want) {
			t.Fatalf("shutdown error = %v, want %v", err, want)
		}
	case <-time.After(time.Second):
		t.Fatal("service monitor did not exit")
	}
}
