package rustengine

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestMemoryPutQueueImmediateCappedFIFO(t *testing.T) {
	var direct []string
	var groups [][]string
	owner, first, release := gatedMemoryPutQueue(t, 4,
		func(key, value []byte) error { direct = append(direct, string(key)); return nil },
		func(pairs []MemoryPair) (bool, error) {
			var keys []string
			for _, pair := range pairs {
				keys = append(keys, string(pair.Key))
			}
			groups = append(groups, keys)
			return true, nil
		})
	var pending []<-chan error
	for index := 0; index < 5; index++ {
		pending = append(pending, memoryQueuePutAsync(owner, fmt.Sprintf("k%d", index), "v"))
		waitMemoryQueuePending(t, owner.queue, index+1)
	}
	release()
	memoryQueueResult(t, first, nil)
	for _, result := range pending {
		memoryQueueResult(t, result, nil)
	}
	queue := owner.queue
	owner.close()
	if batches, requests := queue.groupStats(); batches != 1 || requests != 4 {
		t.Fatalf("successful group stats=%d/%d, want 1/4", batches, requests)
	}
	if fmt.Sprint(groups) != "[[k0 k1 k2 k3]]" || fmt.Sprint(direct) != "[seed k4]" {
		t.Fatalf("groups=%v direct=%v", groups, direct)
	}
}

func TestMemoryPutQueueFallbackAndFinalError(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(fmt.Sprintf("group-error=%t", failed), func(t *testing.T) {
			var direct []string
			groupCalls := 0
			owner, first, release := gatedMemoryPutQueue(t, 8,
				func(key, value []byte) error {
					direct = append(direct, string(key))
					if string(key) == "b" {
						return ErrMemoryPayloadLimit
					}
					return nil
				},
				func(pairs []MemoryPair) (bool, error) {
					groupCalls++
					if failed {
						return false, ErrMemoryDurability
					}
					return false, nil
				})
			var pending []<-chan error
			for index, key := range []string{"a", "b", "c"} {
				pending = append(pending, memoryQueuePutAsync(owner, key, "v"))
				waitMemoryQueuePending(t, owner.queue, index+1)
			}
			release()
			memoryQueueResult(t, first, nil)
			for index, result := range pending {
				var expected error
				if failed {
					expected = ErrMemoryDurability
				} else if index == 1 {
					expected = ErrMemoryPayloadLimit
				}
				memoryQueueResult(t, result, expected)
			}
			queue := owner.queue
			owner.close()
			want := "[seed a b c]"
			if failed {
				want = "[seed]"
			}
			if groupCalls != 1 || fmt.Sprint(direct) != want {
				t.Fatalf("group calls=%d direct=%v, want %s", groupCalls, direct, want)
			}
			if batches, requests := queue.groupStats(); batches != 0 || requests != 0 {
				t.Fatalf("fallback/error counted as success: %d/%d", batches, requests)
			}
		})
	}
}

func TestMemoryPutQueueOwnerCloseWaitsForPuts(t *testing.T) {
	owner, first, release := gatedMemoryPutQueue(t, 8,
		func(key, value []byte) error { return nil },
		func(pairs []MemoryPair) (bool, error) { return true, nil })
	var pending []<-chan error
	for index := 0; index < 3; index++ {
		pending = append(pending, memoryQueuePutAsync(owner, "same", fmt.Sprint(index)))
		waitMemoryQueuePending(t, owner.queue, index+1)
	}
	queue := owner.queue
	closed := make(chan struct{})
	go func() { owner.close(); close(closed) }()
	deadline := time.Now().Add(2 * time.Second)
	for owner.mu.TryRLock() {
		owner.mu.RUnlock()
		if time.Now().After(deadline) {
			t.Fatal("Close never requested the lifecycle write lock")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case <-closed:
		t.Fatal("owner closed while a Put was in flight")
	default:
	}
	release()
	memoryQueueResult(t, first, nil)
	for _, result := range pending {
		memoryQueueResult(t, result, nil)
	}
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("worker deadlocked behind lifecycle write lock")
	}
	if batches, requests := queue.groupStats(); batches != 1 || requests != 3 {
		t.Fatalf("successful group stats=%d/%d, want 1/3", batches, requests)
	}
}

func TestMemoryPutQueueLargerPutsBypass(t *testing.T) {
	owner, first, release := gatedMemoryPutQueue(t, 8,
		func(key, value []byte) error { return nil },
		func(pairs []MemoryPair) (bool, error) { return true, nil })
	memoryQueueResult(t, memoryQueuePutAsync(owner, strings.Repeat("k", 129), "v"), nil)
	memoryQueueResult(t, memoryQueuePutAsync(owner, "k", strings.Repeat("v", 65)), nil)
	boundary := memoryQueuePutAsync(owner, strings.Repeat("k", 128), strings.Repeat("v", 64))
	waitMemoryQueuePending(t, owner.queue, 1)
	release()
	memoryQueueResult(t, first, nil)
	memoryQueueResult(t, boundary, nil)
}

// Models MemoryEngine's existing lifecycle contract, not queue-owned locking.
type memoryQueueOwner struct {
	mu    sync.RWMutex
	queue *memoryPutQueue
}

func (owner *memoryQueueOwner) put(key, value []byte) error {
	owner.mu.RLock()
	defer owner.mu.RUnlock()
	return owner.queue.put(key, value)
}

func (owner *memoryQueueOwner) close() {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if owner.queue != nil {
		owner.queue.close()
		owner.queue = nil
	}
}

func gatedMemoryPutQueue(t *testing.T, maximum int, direct memoryPutDirect, group memoryPutGroup) (*memoryQueueOwner, <-chan error, func()) {
	t.Helper()
	entered, unblock := make(chan struct{}), make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(unblock) }) }
	queue, err := newMemoryPutQueue(maximum, func(key, value []byte) error {
		if string(key) == "seed" {
			close(entered)
			<-unblock
		}
		return direct(key, value)
	}, group)
	if err != nil {
		t.Fatal(err)
	}
	owner := &memoryQueueOwner{queue: queue}
	t.Cleanup(func() { release(); owner.close() })
	first := memoryQueuePutAsync(owner, "seed", "v")
	select {
	case <-entered: // A lone request executes without waiting for companions.
	case <-time.After(2 * time.Second):
		t.Fatal("first request waited for a group")
	}
	return owner, first, release
}

func memoryQueuePutAsync(owner *memoryQueueOwner, key, value string) <-chan error {
	result := make(chan error, 1)
	go func() { result <- owner.put([]byte(key), []byte(value)) }()
	return result
}

func waitMemoryQueuePending(t *testing.T, queue *memoryPutQueue, count int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for len(queue.requests) != count {
		if time.Now().After(deadline) {
			t.Fatalf("pending=%d, want %d", len(queue.requests), count)
		}
		time.Sleep(time.Millisecond)
	}
}

func memoryQueueResult(t *testing.T, result <-chan error, expected error) {
	t.Helper()
	select {
	case err := <-result:
		if !errors.Is(err, expected) {
			t.Fatalf("put result=%v, want %v", err, expected)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("put did not return")
	}
}
