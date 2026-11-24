//go:build linux

package gologreplica

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync"
	"time"
)

const DefaultTransferIdleTimeout = 30 * time.Second

var errPullIdle = errors.New("replication pull idle timeout")

const (
	deferSnapshotQuery     = "defer_snapshot"
	snapshotRequiredHeader = "Kerniva-Snapshot-Required"
)

var errSnapshotRequired = errors.New("replication pull requires a snapshot")

// A pull with steady progress may take longer than its idle limit.
// One timer follows progress without creating a timer per response read.
type pullProgress struct {
	mu       sync.Mutex
	timeout  time.Duration
	deadline time.Time
	timer    *time.Timer
	cancel   context.CancelCauseFunc
	stopped  bool
}

func watchPullProgress(ctx context.Context, timeout time.Duration) (context.Context, *pullProgress) {
	ctx, cancel := context.WithCancelCause(ctx)
	progress := &pullProgress{timeout: timeout, deadline: time.Now().Add(timeout), cancel: cancel}
	progress.mu.Lock()
	progress.timer = time.AfterFunc(timeout, progress.expire)
	progress.mu.Unlock()
	return ctx, progress
}

func (p *pullProgress) received() {
	p.mu.Lock()
	p.deadline = time.Now().Add(p.timeout)
	p.mu.Unlock()
}

func (p *pullProgress) expire() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped {
		return
	}
	if remaining := time.Until(p.deadline); remaining > 0 {
		p.timer.Reset(remaining)
		return
	}
	p.cancel(errPullIdle)
}

func (p *pullProgress) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stopped = true
	p.timer.Stop()
	p.cancel(nil)
}

type pullReader struct {
	ctx      context.Context
	body     io.Reader
	progress *pullProgress
}

func (r pullReader) Read(buffer []byte) (int, error) {
	n, err := r.body.Read(buffer)
	if n > 0 {
		r.progress.received()
	}
	if err != nil && errors.Is(context.Cause(r.ctx), errPullIdle) {
		err = errPullIdle
	}
	return n, err
}

type pullResponseWriter struct {
	body       io.Writer
	controller *http.ResponseController
}

func (w pullResponseWriter) Write(buffer []byte) (int, error) {
	err := w.controller.SetWriteDeadline(time.Now().Add(DefaultTransferIdleTimeout))
	if err != nil && !errors.Is(err, http.ErrNotSupported) {
		return 0, err
	}
	return w.body.Write(buffer)
}
