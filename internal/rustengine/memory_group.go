package rustengine

import "errors"

type MemoryWriteGroupStats struct {
	MaxBatch int
	Batches  uint64
	Requests uint64
}

// EnableWriteGrouping configures one synchronous store before it is published
// to clients. Calls submit only ordinary small writes to its worker.
func (e *MemoryEngine) EnableWriteGrouping(maxBatch int) error {
	if e == nil {
		return errMemoryEngineClosed
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.handle == 0 {
		return errMemoryEngineClosed
	}
	if e.walStats == nil {
		return ErrMemoryUnsupported
	}
	if e.putQueue != nil {
		return errors.New("memory write grouping is already enabled")
	}
	queue, err := newNativeMemoryPutQueue(maxBatch, e.handle, e.putNativePrepared)
	if err != nil {
		return err
	}
	e.putQueue = queue
	return nil
}

func (e *MemoryEngine) WriteGroupStats() MemoryWriteGroupStats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.putQueue == nil {
		return MemoryWriteGroupStats{MaxBatch: 1}
	}
	batches, requests := e.putQueue.groupStats()
	return MemoryWriteGroupStats{MaxBatch: e.putQueue.maxBatch, Batches: batches, Requests: requests}
}
