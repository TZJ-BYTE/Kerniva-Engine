package rustengine

import (
	"errors"
	"runtime"
	"sync/atomic"
	"unsafe"
)

const (
	memoryPutQueueCapacity = 64
	memoryPutGroupMaximum  = 8
	memoryPutQueueKeyLimit = 128
	memoryPutQueueValLimit = 64
)

type memoryPutDirect func(key, value []byte) error

// false,nil promises no write occurred. Every other result is final.
type memoryPutGroup func(pairs []MemoryPair) (merged bool, err error)

type memoryPutRequest struct {
	key, value []byte
	result     chan error
}

// One queue belongs to one live MemoryEngine. Put holds the engine lifecycle
// RLock until put returns; Close obtains its write lock before calling close
// once, then frees the handle. Worker hooks MUST NOT acquire that lifecycle
// lock or call MemoryEngine.Put: a pending writer would deadlock the worker.
type memoryPutQueue struct {
	requests chan *memoryPutRequest
	stopped  chan struct{}
	maxBatch int
	direct   memoryPutDirect
	group    memoryPutGroup

	groupBatches  atomic.Uint64
	groupRequests atomic.Uint64
}

func newMemoryPutQueue(maxBatch int, direct memoryPutDirect, group memoryPutGroup) (*memoryPutQueue, error) {
	if maxBatch < 2 || maxBatch > memoryPutGroupMaximum || direct == nil || group == nil {
		return nil, errors.New("memory put queue requires a group limit of 2..8 and both hooks")
	}
	q := &memoryPutQueue{
		requests: make(chan *memoryPutRequest, memoryPutQueueCapacity),
		stopped:  make(chan struct{}),
		maxBatch: maxBatch,
		direct:   direct,
		group:    group,
	}
	go q.run()
	return q, nil
}

// Resolve the optional symbol only for explicitly enabled grouping, after
// opening the handle and before publishing its MemoryEngine. Disabled/max=1
// mode must retain the original direct path without creating a queue.
func newNativeMemoryPutQueue(maxBatch int, handle uintptr, direct memoryPutDirect) (*memoryPutQueue, error) {
	group, err := findMemorySymbol("try_group_puts")
	if err != nil {
		return nil, err
	}
	return newMemoryPutQueue(maxBatch, direct, func(pairs []MemoryPair) (bool, error) {
		return callMemoryPutGroup(group, handle, pairs)
	})
}

// Input slices remain borrowed until put returns. A full channel blocks the
// caller; larger puts retain the original direct native path.
func (q *memoryPutQueue) put(key, value []byte) error {
	if len(key) > memoryPutQueueKeyLimit || len(value) > memoryPutQueueValLimit {
		return q.direct(key, value)
	}
	request := &memoryPutRequest{key: key, value: value, result: make(chan error, 1)}
	q.requests <- request
	return <-request.result
}

// The owner has already waited for every Put by acquiring its lifecycle write
// lock. There are no senders or pending puts, so stop the worker before freeing
// its handle. Only the owning MemoryEngine.Close calls this, exactly once.
func (q *memoryPutQueue) close() {
	close(q.requests)
	<-q.stopped
}

// These independent loads describe a consistent pair at quiescent endpoints.
func (q *memoryPutQueue) groupStats() (batches, requests uint64) {
	return q.groupBatches.Load(), q.groupRequests.Load()
}

func (q *memoryPutQueue) run() {
	defer close(q.stopped)
	var batch [memoryPutGroupMaximum]*memoryPutRequest
	var pairs [memoryPutGroupMaximum]MemoryPair
	for first := range q.requests {
		batch[0] = first
		count := 1
	drain:
		for count < q.maxBatch {
			select {
			case next := <-q.requests:
				batch[count] = next
				count++
			default:
				// No timer, minimum size, or wait for another request.
				break drain
			}
		}

		var merged bool
		var groupErr error
		if count > 1 {
			for index, request := range batch[:count] {
				pairs[index] = MemoryPair{Key: request.key, Value: request.value}
			}
			merged, groupErr = q.group(pairs[:count])
			clear(pairs[:count])
			if merged && groupErr == nil {
				q.groupRequests.Add(uint64(count))
				q.groupBatches.Add(1)
			}
		}
		for index, request := range batch[:count] {
			err := groupErr
			if !merged && groupErr == nil {
				// Includes lone requests. Only false,nil permits fallback.
				err = q.direct(request.key, request.value)
			}
			batch[index] = nil
			request.key, request.value = nil, nil
			request.result <- err
		}
	}
}

// Synchronous ABI call; the owner keeps handle open. Rust cannot retain inputs.
func callMemoryPutGroup(proc *nativeProc, handle uintptr, pairs []MemoryPair) (bool, error) {
	nativePairs := make([]nativeMemoryPair, len(pairs))
	for index, pair := range pairs {
		nativePairs[index] = nativeMemoryPair{
			Key:   nativeMemorySlice{Data: memoryBytesPointer(pair.Key), Len: uintptr(len(pair.Key))},
			Value: nativeMemorySlice{Data: memoryBytesPointer(pair.Value), Len: uintptr(len(pair.Value))},
		}
	}
	result, _, _ := proc.Call(
		handle,
		uintptr(unsafe.Pointer(unsafe.SliceData(nativePairs))),
		uintptr(len(nativePairs)),
	)
	runtime.KeepAlive(nativePairs)
	runtime.KeepAlive(pairs)
	switch status := int32(result); status {
	case 0:
		return true, nil
	case 1:
		return false, nil
	default:
		return false, memoryStatusError("group put", status)
	}
}
