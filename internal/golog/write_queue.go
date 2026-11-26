package golog

import (
	"errors"
	"sync"

	"github.com/TZJ-BYTE/Kerniva/internal/rustengine"
	"github.com/TZJ-BYTE/Kerniva/internal/writetrace"
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
	trace      *writetrace.Request
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
func (w *Writer) enqueueWrite(operations []rustengine.WriteOp, reset bool, row *writetrace.Request) error {
	queue := w.writes
	if row != nil {
		row.Queue = row.Stamp()
	}
	queue.mu.RLock()
	defer queue.mu.RUnlock()
	if w.closed {
		return errWriterClosed
	}
	request := &writeRequest{operations: operations, reset: reset, result: make(chan error, 1), trace: row}
	queue.requests <- request
	err := <-request.result
	if row != nil {
		row.Result = row.Stamp()
	}
	return err
}

func (w *Writer) runWriteQueue() {
	queue := w.writes
	defer close(queue.stopped)
	var batch [groupMaximum]*writeRequest
	for request := range queue.requests {
		batch[0] = request
		tracked := request.trace
		n := 1
	drain:
		for n < w.options.GroupMax {
			select {
			case next, ok := <-queue.requests:
				if !ok {
					break drain
				}
				batch[n] = next
				if next.trace != nil {
					tracked = next.trace
				}
				n++
			default:
				break drain
			}
		}
		w.commitQueue(batch[:n], tracked)
		clear(batch[:n])
	}
}

func (w *Writer) commitQueue(batch []*writeRequest, tracked *writetrace.Request) {
	var drained, locked int64
	if tracked != nil {
		drained = tracked.Stamp()
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if tracked != nil {
		locked = tracked.Stamp()
	}
	for len(batch) != 0 {
		first := batch[0]
		var phases writetrace.Batch
		var trace *writetrace.Batch
		if tracked != nil {
			phases = tracked.NewBatch()
			phases.Dequeue, phases.Locked = drained, locked
			trace = &phases
		}
		done, err := w.appendLocked(first.operations, first.reset, batch[1:], trace)
		if trace != nil {
			phases.Failed = err != nil
			phases.Release = trace.Stamp()
		}
		for index, request := range batch[:done] {
			if request.trace != nil {
				request.trace.Batch = phases
				if phases.Size != 0 {
					request.trace.Batch.Sequence += uint64(index)
				}
			}
			request.result <- err
		}
		batch = batch[done:]
	}
}
