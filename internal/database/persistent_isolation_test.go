package database

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/TZJ-BYTE/Kerniva/internal/datastruct"
	"github.com/TZJ-BYTE/Kerniva/internal/rustengine"
)

type persistentWriteBarrier struct {
	entered chan struct{}
	release chan struct{}
	calls   atomic.Int64
	once    sync.Once
}

func blockPersistentKey(t *testing.T, db *Database, key string, commitErr error) *persistentWriteBarrier {
	t.Helper()
	barrier := &persistentWriteBarrier{entered: make(chan struct{}), release: make(chan struct{})}
	writeBatch := db.writeBatch
	db.writeBatch = func(operations []rustengine.WriteOp) error {
		if len(operations) == 1 && string(operations[0].Key) == key {
			if barrier.calls.Add(1) == 1 {
				close(barrier.entered)
			}
			<-barrier.release
			if commitErr != nil {
				return commitErr
			}
		}
		return writeBatch(operations)
	}
	t.Cleanup(barrier.unblock)
	return barrier
}

func (barrier *persistentWriteBarrier) unblock() {
	barrier.once.Do(func() { close(barrier.release) })
}

func waitPersistentSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for persistent operation")
	}
}

func waitPersistentError(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for persistent result")
		return nil
	}
}

func differentPersistentShard(key string) string {
	for i := 0; ; i++ {
		candidate := fmt.Sprintf("independent-%d", i)
		if getShardIndex(candidate) != getShardIndex(key) {
			return candidate
		}
	}
}

func samePersistentShard(key string) string {
	for i := 0; ; i++ {
		candidate := fmt.Sprintf("same-shard-%d", i)
		if candidate != key && getShardIndex(candidate) == getShardIndex(key) {
			return candidate
		}
	}
}

func TestPersistentCommitAllowsSameShardOtherKey(t *testing.T) {
	db, _ := persistentTestDB(t, 1<<20)
	key := "writing"
	other := samePersistentShard(key)
	for _, seed := range []string{key, other} {
		if err := db.SetStringBytes([]byte(seed), []byte("old")); err != nil {
			t.Fatal(err)
		}
	}
	barrier := blockPersistentKey(t, db, key, nil)
	write := make(chan error, 1)
	go func() { write <- db.SetStringBytes([]byte(key), []byte("committed")) }()
	waitPersistentSignal(t, barrier.entered)
	read := make(chan error, 1)
	go func() {
		value, found, err := db.GetStringCopy([]byte(other))
		if err == nil && (!found || value != "old") {
			err = fmt.Errorf("same-shard unrelated read = %q, %v", value, found)
		}
		read <- err
	}()
	if err := waitPersistentError(t, read); err != nil {
		t.Fatal(err)
	}
	concurrent := make(chan error, 1)
	go func() { concurrent <- db.SetStringBytes([]byte(other), []byte("independent")) }()
	if err := waitPersistentError(t, concurrent); err != nil {
		t.Fatal(err)
	}
	barrier.unblock()
	if err := waitPersistentError(t, write); err != nil {
		t.Fatal(err)
	}
	requireString(t, db, key, "committed")
	requireString(t, db, other, "independent")
	if db.pendingMemory.Load() != 0 {
		t.Fatal("same-shard commits retained pending memory")
	}
}

func TestPersistentCommitAllowsHotReadAndExpiryScan(t *testing.T) {
	db, _ := persistentTestDB(t, 1<<20)
	key := "writing"
	other := differentPersistentShard(key)
	for _, seed := range []string{key, other} {
		if err := db.SetStringBytes([]byte(seed), []byte("old")); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Set("expired", &datastruct.DataValue{
		Value: &datastruct.String{Data: "expired"}, ExpireTime: time.Now().Add(-time.Second).UnixMilli(),
	}); err != nil {
		t.Fatal(err)
	}
	version := watchKeyForTest(t, db, key)
	changed := db.ChangeChannel()
	barrier := blockPersistentKey(t, db, key, nil)
	first := make(chan error, 1)
	go func() { first <- db.SetStringBytes([]byte(key), []byte("first")) }()
	waitPersistentSignal(t, barrier.entered)
	if db.pendingMemory.Load() <= 0 || atomic.LoadInt64(&db.usedMemory) > db.maxMemory {
		t.Fatal("pending write was not admitted within the memory budget")
	}
	if db.KeyVersion(key) != version {
		t.Fatal("pending write changed WATCH version")
	}
	select {
	case <-changed:
		t.Fatal("pending write notified readers before commit")
	default:
	}

	read := make(chan error, 1)
	go func() {
		value, found, err := db.GetStringCopy([]byte(other))
		if err == nil && (!found || value != "old") {
			err = fmt.Errorf("hot read returned %q, found=%v", value, found)
		}
		read <- err
	}()
	if err := waitPersistentError(t, read); err != nil {
		t.Fatal(err)
	}
	scanned := make(chan struct{})
	go func() {
		db.expireSample(time.Now().UnixMilli(), 8192)
		close(scanned)
	}()
	waitPersistentSignal(t, scanned)
	// A scan that queued an exclusive lifecycle lock would now block this GET.
	if err := waitHotPersistentRead(db, other); err != nil {
		t.Fatal(err)
	}
	sameRead := make(chan error, 1)
	go func() {
		value, found, err := db.GetStringCopy([]byte(key))
		if err == nil && (!found || (value != "first" && value != "second")) {
			err = fmt.Errorf("same-key read returned %q, found=%v", value, found)
		}
		sameRead <- err
	}()
	select {
	case err := <-sameRead:
		t.Fatalf("same-key read passed an uncommitted write: %v", err)
	case <-time.After(30 * time.Millisecond):
	}

	second := make(chan error, 1)
	go func() { second <- db.SetStringBytes([]byte(key), []byte("second")) }()
	select {
	case err := <-second:
		t.Fatalf("same-key write completed before its predecessor: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	if barrier.calls.Load() != 1 {
		t.Fatal("same-key commits entered storage out of order")
	}
	barrier.unblock()
	if err := waitPersistentError(t, first); err != nil {
		t.Fatal(err)
	}
	if err := waitPersistentError(t, second); err != nil {
		t.Fatal(err)
	}
	if err := waitPersistentError(t, sameRead); err != nil {
		t.Fatal(err)
	}
	requireString(t, db, key, "second")
	if db.KeyVersion(key) != version+2 || db.pendingMemory.Load() != 0 {
		t.Fatal("successful writes left pending state or incorrect WATCH versions")
	}
	waitPersistentSignal(t, changed)
	db.expireSample(time.Now().UnixMilli(), 8192)
	if _, exists := db.shards[getShardIndex("expired")].data["expired"]; exists {
		t.Fatal("expiration made no progress after commit")
	}
}

func waitHotPersistentRead(db *Database, key string) error {
	result := make(chan error, 1)
	go func() {
		_, found, err := db.GetStringBytesCopy([]byte(key))
		if err == nil && !found {
			err = errors.New("hot read lost its value")
		}
		result <- err
	}()
	select {
	case err := <-result:
		return err
	case <-time.After(5 * time.Second):
		return errors.New("hot read waited for an unrelated persistent commit")
	}
}

func TestPersistentFailedPendingWritePreservesConcurrentCommit(t *testing.T) {
	db, cfg := persistentTestDB(t, 1<<20)
	key := "failing"
	other := differentPersistentShard(key)
	for _, seed := range []string{key, other} {
		if err := db.SetStringBytes([]byte(seed), []byte("old")); err != nil {
			t.Fatal(err)
		}
	}
	version := watchKeyForTest(t, db, key)
	otherVersion := watchKeyForTest(t, db, other)
	changed := db.ChangeChannel()
	forced := errors.New("injected persistent commit failure")
	barrier := blockPersistentKey(t, db, key, forced)
	write := make(chan error, 1)
	go func() { write <- db.SetStringBytes([]byte(key), []byte("uncommitted")) }()
	waitPersistentSignal(t, barrier.entered)
	concurrent := make(chan error, 1)
	go func() { concurrent <- db.SetStringBytes([]byte(other), []byte("concurrent committed value")) }()
	if err := waitPersistentError(t, concurrent); err != nil {
		t.Fatal(err)
	}
	barrier.unblock()
	if err := waitPersistentError(t, write); !errors.Is(err, forced) {
		t.Fatalf("commit failure = %v", err)
	}
	requireString(t, db, key, "old")
	requireString(t, db, other, "concurrent committed value")
	if db.KeyVersion(key) != version || db.KeyVersion(other) != otherVersion+1 {
		t.Fatal("failure rolled back an unrelated WATCH version")
	}
	if db.pendingMemory.Load() != 0 {
		t.Fatal("failed write leaked its memory reservation")
	}
	var resident int64
	for i := range db.shards {
		for k, value := range db.shards[i].data {
			resident += int64(len(k)) + value.ApproximateSize()
		}
	}
	if used := atomic.LoadInt64(&db.usedMemory); used != resident || used > db.maxMemory {
		t.Fatalf("memory accounting=%d, resident=%d", used, resident)
	}
	waitPersistentSignal(t, changed)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewDatabaseWithConfig(0, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	requireString(t, reopened, key, "old")
	requireString(t, reopened, other, "concurrent committed value")
}

func TestPersistentFailedCommitWakesSameKeyReadersAndWriter(t *testing.T) {
	db, _ := persistentTestDB(t, 1<<20)
	key := "key"
	if err := db.SetStringBytes([]byte(key), []byte("old")); err != nil {
		t.Fatal(err)
	}
	used := atomic.LoadInt64(&db.usedMemory)
	version := watchKeyForTest(t, db, key)
	// Admit one private value at a time. The waiting writer must reserve its
	// budget after the failed writer releases its reservation.
	value := []byte("committed")
	db.operationMu.Lock()
	db.maxMemory = used + int64(len(key)+len(value)+24) + 128
	db.operationMu.Unlock()
	forced := errors.New("injected first commit failure")
	barrier := &persistentWriteBarrier{entered: make(chan struct{}), release: make(chan struct{})}
	writeBatch := db.writeBatch
	db.writeBatch = func(operations []rustengine.WriteOp) error {
		if len(operations) == 1 && string(operations[0].Key) == key {
			if barrier.calls.Add(1) == 1 {
				close(barrier.entered)
				<-barrier.release
				return forced
			}
			if db.mutation != nil || db.pendingMemory.Load() == 0 {
				return errors.New("waiting writer lost the pending-write fast path")
			}
		}
		return writeBatch(operations)
	}
	t.Cleanup(barrier.unblock)
	first := make(chan error, 1)
	go func() { first <- db.SetStringBytes([]byte(key), []byte("discarded")) }()
	waitPersistentSignal(t, barrier.entered)
	readers := []func() error{
		func() error {
			value, found, err := db.GetStringCopy([]byte(key))
			if err == nil && (!found || (value != "old" && value != "committed")) {
				err = fmt.Errorf("same-key string read = %q, %v", value, found)
			}
			return err
		},
		func() error {
			value, found, err := db.GetStringBytesCopy([]byte(key))
			if err == nil && (!found || (string(value) != "old" && string(value) != "committed")) {
				err = fmt.Errorf("same-key bytes read = %q, %v", value, found)
			}
			return err
		},
		func() error {
			if value, found := db.Get(key); !found || value == nil {
				return errors.New("same-key Get lost its value")
			}
			return nil
		},
		func() error {
			if value, found := db.GetBytes([]byte(key)); !found || value == nil {
				return errors.New("same-key GetBytes lost its value")
			}
			return nil
		},
	}
	results := make([]<-chan error, 0, len(readers)+1)
	for _, read := range readers {
		result := make(chan error, 1)
		go func() { result <- read() }()
		results = append(results, result)
	}
	second := make(chan error, 1)
	go func() { second <- db.SetStringBytes([]byte(key), value) }()
	results = append(results, second)
	for _, result := range results {
		select {
		case err := <-result:
			t.Fatalf("same-key operation passed a pending commit: %v", err)
		case <-time.After(10 * time.Millisecond):
		}
	}
	barrier.unblock()
	if err := waitPersistentError(t, first); !errors.Is(err, forced) {
		t.Fatalf("first commit failure = %v", err)
	}
	for _, result := range results {
		if err := waitPersistentError(t, result); err != nil {
			t.Fatal(err)
		}
	}
	requireString(t, db, key, "committed")
	wantMemory := used + int64(len(value)-len("old"))
	if db.KeyVersion(key) != version+1 || db.pendingMemory.Load() != 0 || atomic.LoadInt64(&db.usedMemory) != wantMemory {
		t.Fatal("failed commit left pending state, an incorrect WATCH version or memory budget")
	}
	if len(db.pendingStringWrites[getShardIndex(key)]) != 0 {
		t.Fatal("same-key completion retained a pending entry")
	}
}

func TestPersistentFailedPendingWriteDoesNotNotifyOrConsumeMemory(t *testing.T) {
	db, _ := persistentTestDB(t, 1<<20)
	if err := db.SetStringBytes([]byte("key"), []byte("old")); err != nil {
		t.Fatal(err)
	}
	used := atomic.LoadInt64(&db.usedMemory)
	version := watchKeyForTest(t, db, "key")
	changed := db.ChangeChannel()
	if err := db.rustEngine.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.SetStringBytes([]byte("key"), []byte("must remain private")); err == nil {
		t.Fatal("closed engine accepted pending write")
	}
	requireString(t, db, "key", "old")
	if db.KeyVersion("key") != version || atomic.LoadInt64(&db.usedMemory) != used || db.pendingMemory.Load() != 0 {
		t.Fatal("failed commit changed published state or leaked pending memory")
	}
	select {
	case <-changed:
		t.Fatal("failed commit emitted a change notification")
	default:
	}
}

func TestPersistentLifecycleWaitsForPendingCommit(t *testing.T) {
	for _, action := range []string{"clear", "close"} {
		t.Run(action, func(t *testing.T) {
			db, cfg := persistentTestDB(t, 1<<20)
			if err := db.SetStringBytes([]byte("key"), []byte("old")); err != nil {
				t.Fatal(err)
			}
			barrier := blockPersistentKey(t, db, "key", nil)
			write := make(chan error, 1)
			go func() { write <- db.SetStringBytes([]byte("key"), []byte("committed")) }()
			waitPersistentSignal(t, barrier.entered)
			stopped := make(chan error, 1)
			go func() {
				if action == "clear" {
					stopped <- db.Clear()
				} else {
					stopped <- db.Close()
				}
			}()
			select {
			case err := <-stopped:
				t.Fatalf("%s passed a pending commit: %v", action, err)
			case <-time.After(30 * time.Millisecond):
			}
			barrier.unblock()
			if err := waitPersistentError(t, write); err != nil {
				t.Fatal(err)
			}
			if err := waitPersistentError(t, stopped); err != nil {
				t.Fatal(err)
			}
			if db.pendingMemory.Load() != 0 {
				t.Fatal("lifecycle operation left pending memory")
			}
			if action == "clear" {
				if db.Size() != 0 || atomic.LoadInt64(&db.usedMemory) != 0 {
					t.Fatal("clear retained a pending value")
				}
			} else {
				if _, _, err := db.GetStringBytesCopy([]byte("key")); !errors.Is(err, errDatabaseClosed) {
					t.Fatalf("closed database GET = %v", err)
				}
				reopened, err := NewDatabaseWithConfig(0, cfg)
				if err != nil {
					t.Fatal(err)
				}
				defer reopened.Close()
				requireString(t, reopened, "key", "committed")
			}
		})
	}
}

func TestPersistentColdExpiredTypeAndBudgetFallback(t *testing.T) {
	db, cfg := persistentTestDB(t, 96)
	if err := db.SetStringBytes([]byte("cold"), []byte("on disk")); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewDatabaseWithConfig(0, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	requireString(t, reopened, "cold", "on disk")
	if err := reopened.SetStringBytes([]byte("cold"), []byte("budget fallback")); err != nil {
		t.Fatal(err)
	}
	if err := reopened.Set("expired", &datastruct.DataValue{
		Value: &datastruct.String{Data: "old"}, ExpireTime: time.Now().Add(-time.Second).UnixMilli(),
	}); err != nil {
		t.Fatal(err)
	}
	if err := reopened.SetStringBytes([]byte("expired"), []byte("fresh")); err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.LPush("list", []string{"item"}); err != nil {
		t.Fatal(err)
	}
	if err := reopened.SetStringBytes([]byte("list"), []byte("string")); err != nil {
		t.Fatal(err)
	}
	for key, value := range map[string]string{"cold": "budget fallback", "expired": "fresh", "list": "string"} {
		requireString(t, reopened, key, value)
	}
	if reopened.pendingMemory.Load() != 0 || atomic.LoadInt64(&reopened.usedMemory) > reopened.maxMemory {
		t.Fatal("fallback leaked pending state or exceeded cache budget")
	}
}

func TestPersistentTTLReadRetainsExclusiveFallbackAcrossDeadline(t *testing.T) {
	db, _ := persistentTestDB(t, 1<<20)
	if _, err := db.SetStringBytesWithOptions([]byte("ttl"), []byte("old"), 60_000, false, false, false); err != nil {
		t.Fatal(err)
	}
	parallel := db.beginKeyOperation("ttl", false)
	if parallel {
		db.endKeyOperation("ttl", false, parallel)
		t.Fatal("persistent TTL key entered shared lookup")
	}
	// Move the deadline past while holding the entry-point lock. The normal
	// getter must remove the expired value under this exclusive ownership.
	db.shards[getShardIndex("ttl")].data["ttl"].ExpireTime = time.Now().Add(-time.Second).UnixMilli()
	_, found, err := db.getStringBytesCopyLocked([]byte("ttl"))
	db.endKeyOperation("ttl", false, parallel)
	if err != nil || found {
		t.Fatalf("expired lookup = found %v, %v", found, err)
	}
	var workers sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for iteration := 0; iteration < 40; iteration++ {
				if err := db.SetStringBytes([]byte("ttl"), []byte("new")); err != nil {
					t.Error(err)
					return
				}
				value, found, err := db.GetStringCopy([]byte("ttl"))
				if err != nil || !found || value != "new" {
					t.Errorf("concurrent TTL replacement = %q, %v, %v", value, found, err)
					return
				}
			}
		}()
	}
	workers.Wait()
	requireString(t, db, "ttl", "new")
	if db.pendingMemory.Load() != 0 {
		t.Fatal("TTL replacement leaked pending state")
	}
}

func TestPersistentColdFallbackQueuesAndBlocksLaterHotRead(t *testing.T) {
	db, _ := persistentTestDB(t, 1<<20)
	key := "pending"
	other := differentPersistentShard(key)
	for _, seed := range []string{key, other, "cold"} {
		if err := db.SetStringBytes([]byte(seed), []byte("old")); err != nil {
			t.Fatal(err)
		}
	}
	// Unload one decoded value while keeping the durable record available.
	db.operationMu.Lock()
	coldShard := db.getShard("cold")
	coldShard.lock.Lock()
	coldValue := coldShard.data["cold"]
	delete(coldShard.data, "cold")
	db.updateMemoryUsage(-int64(len("cold")) - coldValue.ApproximateSize())
	coldShard.lock.Unlock()
	db.operationMu.Unlock()

	barrier := blockPersistentKey(t, db, key, nil)
	write := make(chan error, 1)
	go func() { write <- db.SetStringBytes([]byte(key), []byte("new")) }()
	waitPersistentSignal(t, barrier.entered)
	coldRead := make(chan error, 1)
	go func() { coldRead <- waitHotPersistentRead(db, "cold") }()
	// A held shared lock alone permits TryRLock. Failure proves that the
	// cold lookup has queued its exclusive fallback before the next GET.
	deadline := time.Now().Add(5 * time.Second)
	for db.operationMu.TryRLock() {
		db.operationMu.RUnlock()
		if time.Now().After(deadline) {
			t.Fatal("cold lookup did not queue its exclusive fallback")
		}
		runtime.Gosched()
	}
	hotRead := make(chan error, 1)
	started := make(chan struct{})
	go func() {
		close(started)
		hotRead <- waitHotPersistentRead(db, other)
	}()
	waitPersistentSignal(t, started)
	select {
	case err := <-hotRead:
		t.Fatalf("later hot read bypassed queued exclusive fallback: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	barrier.unblock()
	for _, result := range []<-chan error{write, coldRead, hotRead} {
		if err := waitPersistentError(t, result); err != nil {
			t.Fatal(err)
		}
	}
	requireString(t, db, key, "new")
	if db.pendingMemory.Load() != 0 {
		t.Fatal("exclusive fallback retained pending write memory")
	}
}
