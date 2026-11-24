package golog

import (
	"errors"
	"sync"

	"github.com/TZJ-BYTE/Kerniva/internal/rustengine"
)

const (
	groupMaximum       = 16
	groupMaxBytes      = 1 << 20
	writeQueueCapacity = 64
)

var errWriterClosed = errors.New("Go-log writer is closed")

type writeRequest struct {
	operations []rustengine.WriteOp
	reset      bool
	result     chan error
}

type writeQueue struct {
	mu       sync.RWMutex
	requests chan *writeRequest
	stopped  chan struct{}
}

func newWriteQueue() *writeQueue {
	return &writeQueue{requests: make(chan *writeRequest, writeQueueCapacity), stopped: make(chan struct{})}
}

// A synchronous caller keeps its inputs alive across the worker's append and
// holds the queue read lock so Close cannot stop the worker before it replies.
func (w *Writer) enqueueWrite(operations []rustengine.WriteOp, reset bool) error {
	queue := w.writes
	queue.mu.RLock()
	defer queue.mu.RUnlock()
	if w.closed {
		return errWriterClosed
	}
	request := &writeRequest{operations: operations, reset: reset, result: make(chan error, 1)}
	queue.requests <- request
	return <-request.result
}

func (w *Writer) runWriteQueue() {
	queue := w.writes
	defer close(queue.stopped)
	var batch [groupMaximum]*writeRequest
	for request := range queue.requests {
		batch[0] = request
		n := 1
	drain:
		for n < w.options.GroupMax {
			select {
			case next, ok := <-queue.requests:
				if !ok {
					break drain
				}
				batch[n] = next
				n++
			default:
				break drain
			}
		}
		w.commitQueue(batch[:n])
		clear(batch[:n])
	}
}

func (w *Writer) commitQueue(batch []*writeRequest) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for len(batch) != 0 {
		first := batch[0]
		done, err := w.appendLocked(first.operations, first.reset, batch[1:])
		for _, request := range batch[:done] {
			request.result <- err
		}
		batch = batch[done:]
	}
}
