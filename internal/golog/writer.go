// Package golog appends Go-owned commit records and seals them for Rust
// maintenance. Rust recovery runs once before a Writer is returned.
package golog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/TZJ-BYTE/Kerniva/internal/rustengine"
	"github.com/TZJ-BYTE/Kerniva/internal/writetrace"
)

const (
	activeName        = "active.log"
	writerLock        = ".writer.lock"
	logMagic          = "KGLLOG01"
	frameHeaderBytes  = 8
	batchHeaderBytes  = 12
	recordHeaderBytes = 9
	maxPayloadBytes   = 256 << 20

	putRecord    = 1
	deleteRecord = 2
	resetRecord  = 3
)

type Options struct {
	SegmentBytes uint64
	MaxAge       time.Duration
	Sync         bool
	GroupMax     int
}

type Stats struct {
	LastSequence   uint64
	ActiveBytes    uint64
	SealedSegments uint64
	SealedBytes    uint64
	SyncCount      uint64
	GroupBatches   uint64
	GroupRequests  uint64
	Fault          string
}

type activeFile interface {
	io.WriterAt
	Sync() error
	Truncate(int64) error
	Close() error
}

type Writer struct {
	directory     string
	lock          *os.File
	options       Options
	mu            sync.Mutex
	active        activeFile
	activeEnd     uint64
	firstSequence uint64
	lastSequence  uint64
	firstWriteAt  time.Time
	syncCount     uint64
	groupBatches  uint64
	groupRequests uint64
	scratch       []byte
	fault         error
	closed        bool
	stop          chan struct{}
	done          chan struct{}
	closeOnce     sync.Once
	closeErr      error
	writes        *writeQueue
}

type recoverFunc func(string, func([]byte, []byte) error) (rustengine.GoLogRecovery, error)

func Open(directory string, options Options, restore func(key, value []byte) error) (*Writer, error) {
	return openWithRecovery(directory, options, restore, rustengine.RecoverGoLog)
}

func openWithRecovery(directory string, options Options, restore func(key, value []byte) error, recover recoverFunc) (*Writer, error) {
	if directory == "" {
		return nil, errors.New("Go-log directory is required")
	}
	if options.SegmentBytes == 0 {
		options.SegmentBytes = 4 << 20
	}
	if options.SegmentBytes > math.MaxInt64 || options.MaxAge < 0 {
		return nil, errors.New("invalid Go-log segment size or maximum age")
	}
	if options.GroupMax < 0 || options.GroupMax > groupMaximum || options.GroupMax > 1 && !options.Sync {
		return nil, fmt.Errorf("Go-log group maximum must be from 0 to %d and requires sync to exceed 1", groupMaximum)
	}
	if options.MaxAge == 0 {
		options.MaxAge = 30 * time.Second
	}
	directory, err := filepath.Abs(directory)
	if err != nil {
		return nil, err
	}
	if err := makeDirectoryDurable(directory); err != nil {
		return nil, fmt.Errorf("prepare Go-log directory: %w", err)
	}
	lock, err := LockWriterDirectory(directory)
	if err != nil {
		return nil, err
	}
	recovered, err := recover(directory, restore)
	if err != nil {
		return nil, errors.Join(err, lock.Close())
	}
	w := &Writer{
		directory:    directory,
		lock:         lock,
		options:      options,
		lastSequence: recovered.LastSequence,
		stop:         make(chan struct{}),
		done:         make(chan struct{}),
	}
	if err := w.openActive(recovered.ActiveEnd); err != nil {
		return nil, errors.Join(err, lock.Close())
	}
	if options.GroupMax > 1 {
		w.writes = newWriteQueue()
		go w.runWriteQueue()
	}
	// Rotate on age even if further writes stop arriving.
	go w.sealAgedSegments()
	return w, nil
}

func makeDirectoryDurable(directory string) error {
	info, err := os.Stat(directory)
	switch {
	case err == nil && info.IsDir():
		return nil
	case err == nil:
		return fmt.Errorf("%s is not a directory", directory)
	case !errors.Is(err, os.ErrNotExist):
		return err
	}
	parent := filepath.Dir(directory)
	if parent == directory {
		return err
	}
	if err := makeDirectoryDurable(parent); err != nil {
		return err
	}
	if err := os.Mkdir(directory, 0o750); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	return syncDirectory(parent)
}

func (w *Writer) openActive(validEnd uint64) error {
	if validEnd == 0 {
		if err := discardIncompleteHeader(w.directory); err != nil {
			return fmt.Errorf("repair Go-log active header: %w", err)
		}
		file, err := createActive(w.directory)
		if err != nil {
			return fmt.Errorf("create Go-log active segment: %w", err)
		}
		w.active = file
		w.activeEnd = uint64(len(logMagic))
		return nil
	}
	if validEnd < uint64(len(logMagic)) || validEnd > math.MaxInt64 {
		return fmt.Errorf("invalid recovered Go-log active length %d", validEnd)
	}
	active, err := os.OpenFile(filepath.Join(w.directory, activeName), os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("open recovered Go-log active segment: %w", err)
	}
	info, err := active.Stat()
	if err != nil {
		return errors.Join(err, active.Close())
	}
	if info.Size() < int64(validEnd) {
		return errors.Join(errors.New("Go-log active segment became shorter after recovery"), active.Close())
	}
	if info.Size() > int64(validEnd) {
		if err := truncateAndSync(active, validEnd); err != nil {
			return errors.Join(fmt.Errorf("truncate Go-log active tail: %w", err), active.Close())
		}
	}
	if validEnd > uint64(len(logMagic)) {
		if validEnd < uint64(len(logMagic)+frameHeaderBytes+batchHeaderBytes+recordHeaderBytes) {
			return errors.Join(errors.New("incomplete first recovered Go-log frame"), active.Close())
		}
		var sequence [8]byte
		if _, err := active.ReadAt(sequence[:], int64(len(logMagic)+frameHeaderBytes)); err != nil {
			return errors.Join(fmt.Errorf("read active segment first sequence: %w", err), active.Close())
		}
		first := binary.LittleEndian.Uint64(sequence[:])
		if first == 0 || first > w.lastSequence {
			return errors.Join(errors.New("invalid recovered Go-log sequence limits"), active.Close())
		}
		w.firstSequence = first
		w.firstWriteAt = info.ModTime()
	}
	w.active = active
	w.activeEnd = validEnd
	return nil
}

func discardIncompleteHeader(directory string) error {
	path := filepath.Join(directory, activeName)
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() >= int64(len(logMagic)) {
		return errors.New("active file conflicts with empty recovered header")
	}
	prefix, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if !strings.HasPrefix(logMagic, string(prefix)) {
		return errors.New("active file has invalid partial magic")
	}
	return os.Remove(path)
}

func createActive(directory string) (*os.File, error) {
	destination := filepath.Join(directory, activeName)
	f, err := os.CreateTemp(directory, ".active-*.tmp")
	if err != nil {
		return nil, err
	}
	defer os.Remove(f.Name())
	_, writeErr := f.WriteString(logMagic)
	err = errors.Join(writeErr, f.Sync(), f.Close())
	if err != nil {
		return nil, err
	}
	if err := publishNewFile(f.Name(), destination); err != nil {
		return nil, err
	}
	return os.OpenFile(destination, os.O_RDWR, 0)
}

func publishNewFile(source, destination string) error {
	if _, err := os.Lstat(destination); !errors.Is(err, os.ErrNotExist) {
		if err == nil {
			return fmt.Errorf("Go-log publication would replace %s", destination)
		}
		return err
	}
	if err := publishFile(source, destination); err != nil {
		return err
	}
	return syncDirectory(filepath.Dir(destination))
}

func truncateAndSync(file activeFile, size uint64) error {
	if err := file.Truncate(int64(size)); err != nil {
		return err
	}
	return file.Sync()
}

func (w *Writer) WriteBatch(operations []rustengine.WriteOp) error {
	return w.append(operations, false, nil)
}

func (w *Writer) WriteBatchTraced(operations []rustengine.WriteOp, row *writetrace.Request) error {
	return w.append(operations, false, row)
}

func (w *Writer) Put(key, value []byte) error {
	return w.WriteBatch([]rustengine.WriteOp{{Key: key, Value: value}})
}

func (w *Writer) Delete(key []byte) error {
	return w.WriteBatch([]rustengine.WriteOp{{Key: key, Delete: true}})
}

func (w *Writer) Reset() error {
	return w.append(nil, true, nil)
}

// Seal publishes the active segment durably while retaining the writer lock.
func (w *Writer) Seal() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return errWriterClosed
	}
	if w.fault != nil || w.firstSequence == 0 {
		return w.fault
	}
	return w.sealLocked()
}

func (w *Writer) append(operations []rustengine.WriteOp, reset bool, row *writetrace.Request) error {
	if w.writes != nil {
		return w.enqueueWrite(operations, reset, row)
	}

	if row != nil {
		row.Queue = row.Stamp()
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	var trace *writetrace.Batch
	if row != nil {
		row.Batch = row.NewBatch()
		row.Batch.Locked = row.Batch.Append
		trace = &row.Batch
	}
	_, err := w.appendLocked(operations, reset, nil, trace)
	if trace != nil {
		trace.Failed = err != nil
		trace.Release = trace.Stamp()
		row.Result = row.Stamp()
	}
	return err
}

// appendLocked takes one request and as many complete following requests as
// fit in the same sync. The caller retains the suffix for the next append.
func (w *Writer) appendLocked(operations []rustengine.WriteOp, reset bool, following []*writeRequest, trace *writetrace.Batch) (int, error) {
	if w.closed {
		return len(following) + 1, errWriterClosed
	}
	if w.fault != nil {
		return len(following) + 1, w.fault
	}
	if len(operations) == 0 && !reset {
		return 1, nil
	}
	if w.lastSequence == math.MaxUint64 {
		return 1, errors.New("Go-log commit sequence exhausted")
	}
	sequence := w.lastSequence + 1
	frame, err := encodeFrame(w.scratch, sequence, operations, reset)
	if err != nil {
		return 1, err
	}
	if w.firstSequence != 0 && w.activeEnd+uint64(len(frame)) > w.options.SegmentBytes {
		if err := w.sealLocked(); err != nil {
			return 1, err
		}
	}
	count := 1
	for _, request := range following {
		if sequence == math.MaxUint64 || len(frame) >= groupMaxBytes {
			break
		}
		payload, err := batchPayloadSize(request.operations, request.reset)
		nextSize := len(frame) + frameHeaderBytes + payload
		if err != nil || nextSize > groupMaxBytes || w.activeEnd+uint64(nextSize) > w.options.SegmentBytes {
			break
		}
		sequence++
		frame = appendFrame(frame, sequence, request.operations, request.reset, payload)
		count++
	}
	if cap(frame) <= groupMaxBytes {
		w.scratch = frame
	} else {
		// Large batches should not become a permanent per-database allocation.
		w.scratch = nil
	}
	return count, w.writeFramesLocked(frame, sequence, count, trace)
}

func (w *Writer) writeFramesLocked(frames []byte, lastSequence uint64, count int, trace *writetrace.Batch) error {
	if w.activeEnd > math.MaxInt64-uint64(len(frames)) {
		return errors.New("Go-log active offset exhausted")
	}
	if trace != nil {
		trace.Size, trace.Bytes = count, len(frames)
		trace.Sequence = lastSequence - uint64(count-1)
		trace.Write = trace.Stamp()
	}
	n, err := w.active.WriteAt(frames, int64(w.activeEnd))
	if trace != nil {
		trace.Written = trace.Stamp()
	}
	if err == nil && n != len(frames) {
		err = io.ErrShortWrite
	}
	if err != nil {
		return w.rollbackLocked(fmt.Errorf("append Go-log commit: %w", err))
	}
	if w.options.Sync {
		if trace != nil {
			trace.Sync = trace.Stamp()
		}
		err := w.active.Sync()
		if trace != nil {
			trace.Synced = trace.Stamp()
		}
		if err != nil {
			return w.rollbackLocked(fmt.Errorf("sync Go-log commit: %w", err))
		}
		w.syncCount++
		if count > 1 {
			w.groupBatches++
			w.groupRequests += uint64(count)
		}
	}
	if w.firstSequence == 0 {
		w.firstSequence = lastSequence - uint64(count-1)
		w.firstWriteAt = time.Now()
	}
	w.lastSequence = lastSequence
	w.activeEnd += uint64(len(frames))
	return nil
}

func (w *Writer) rollbackLocked(err error) error {
	if rollback := truncateAndSync(w.active, w.activeEnd); rollback != nil {
		w.fault = errors.Join(err, fmt.Errorf("restore Go-log active prefix: %w", rollback))
		return w.fault
	}
	return err
}

func (w *Writer) sealLocked() error {
	if err := w.sealSegment(true); err != nil {
		w.fault = fmt.Errorf("seal Go-log segment: %w", err)
		return w.fault
	}
	return nil
}

func (w *Writer) sealSegment(reopen bool) error {
	if err := w.active.Sync(); err != nil {
		return err
	}
	err := w.active.Close()
	w.active = nil
	if err != nil {
		return err
	}
	name := fmt.Sprintf("log-%020d-%020d.seg", w.firstSequence, w.lastSequence)
	if err := publishNewFile(filepath.Join(w.directory, activeName), filepath.Join(w.directory, name)); err != nil {
		return err
	}
	w.activeEnd = 0
	w.firstSequence = 0
	w.firstWriteAt = time.Time{}
	if !reopen {
		return nil
	}
	active, err := createActive(w.directory)
	if err != nil {
		return err
	}
	w.active = active
	w.activeEnd = uint64(len(logMagic))
	return nil
}

func (w *Writer) sealAgedSegments() {
	defer close(w.done)
	interval := min(max(w.options.MaxAge/2, time.Millisecond), time.Second)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-w.stop:
			return
		case <-ticker.C:
			w.mu.Lock()
			if !w.closed && w.fault == nil && w.firstSequence != 0 && time.Since(w.firstWriteAt) >= w.options.MaxAge {
				_ = w.sealLocked()
			}
			w.mu.Unlock()
		}
	}
}

// Stats counts successful commit sync calls during this Writer's lifetime.
// Sealed segment counts are sampled separately from ongoing maintenance.
func (w *Writer) Stats() Stats {
	w.mu.Lock()
	stats := Stats{
		LastSequence: w.lastSequence, ActiveBytes: w.activeEnd, SyncCount: w.syncCount,
		GroupBatches: w.groupBatches, GroupRequests: w.groupRequests,
	}
	if w.fault != nil {
		stats.Fault = w.fault.Error()
	}
	w.mu.Unlock()

	entries, _ := os.ReadDir(w.directory)
	for _, entry := range entries {
		if entry.IsDir() || len(entry.Name()) != 49 || !strings.HasPrefix(entry.Name(), "log-") || !strings.HasSuffix(entry.Name(), ".seg") {
			continue
		}
		if info, err := entry.Info(); err == nil {
			stats.SealedSegments++
			stats.SealedBytes += uint64(info.Size())
		}
	}
	return stats
}

func (w *Writer) Close() error {
	w.closeOnce.Do(func() {
		if w.writes != nil {
			// Every sender keeps the read lock until the worker replies.
			w.writes.mu.Lock()
			defer w.writes.mu.Unlock()
			close(w.writes.requests)
			<-w.writes.stopped
		}
		w.mu.Lock()
		w.closed = true
		w.mu.Unlock()
		close(w.stop)
		<-w.done

		w.mu.Lock()
		defer w.mu.Unlock()
		err := w.fault
		if err == nil && w.firstSequence != 0 {
			err = w.sealSegment(false)
		}
		if w.active != nil {
			err = errors.Join(err, w.active.Close())
		}
		w.closeErr = errors.Join(err, w.lock.Close())
	})
	return w.closeErr
}

func encodeFrame(scratch []byte, sequence uint64, operations []rustengine.WriteOp, reset bool) ([]byte, error) {
	payload, err := batchPayloadSize(operations, reset)
	if err != nil {
		return nil, err
	}
	return appendFrame(scratch[:0], sequence, operations, reset, payload), nil
}

func batchPayloadSize(operations []rustengine.WriteOp, reset bool) (int, error) {
	payloadSize := batchHeaderBytes
	count := len(operations)
	if reset {
		if count != 0 {
			return 0, errors.New("reset must be the only Go-log record")
		}
		count = 1
		payloadSize += recordHeaderBytes
	}
	if count == 0 || count > (maxPayloadBytes-batchHeaderBytes)/recordHeaderBytes {
		return 0, errors.New("invalid Go-log batch operation count")
	}
	for _, operation := range operations {
		valueSize := len(operation.Value)
		if operation.Delete {
			valueSize = 0
		}
		if len(operation.Key) > maxPayloadBytes || valueSize > maxPayloadBytes {
			return 0, errors.New("Go-log batch exceeds the maximum payload size")
		}
		size := recordHeaderBytes + len(operation.Key) + valueSize
		if size > maxPayloadBytes-payloadSize {
			return 0, errors.New("Go-log batch exceeds the maximum payload size")
		}
		payloadSize += size
	}

	return payloadSize, nil
}

func appendFrame(frames []byte, sequence uint64, operations []rustengine.WriteOp, reset bool, payloadSize int) []byte {
	start := len(frames)
	needed := start + frameHeaderBytes + payloadSize
	if cap(frames) < needed {
		grown := make([]byte, start, max(needed, min(2*cap(frames), groupMaxBytes)))
		copy(grown, frames)
		frames = grown
	}
	frames = binary.LittleEndian.AppendUint32(frames, uint32(payloadSize))
	frames = binary.LittleEndian.AppendUint32(frames, 0)
	frames = binary.LittleEndian.AppendUint64(frames, sequence)
	if reset {
		frames = binary.LittleEndian.AppendUint32(frames, 1)
		frames = appendRecord(frames, resetRecord, nil, nil)
	} else {
		frames = binary.LittleEndian.AppendUint32(frames, uint32(len(operations)))
	}
	for _, operation := range operations {
		if operation.Delete {
			frames = appendRecord(frames, deleteRecord, operation.Key, nil)
		} else {
			frames = appendRecord(frames, putRecord, operation.Key, operation.Value)
		}
	}
	frame := frames[start:]
	binary.LittleEndian.PutUint32(frame[4:8], crc32.ChecksumIEEE(frame[frameHeaderBytes:]))
	return frames
}

func appendRecord(frame []byte, kind byte, key, value []byte) []byte {
	frame = append(frame, kind)
	frame = binary.LittleEndian.AppendUint32(frame, uint32(len(key)))
	frame = binary.LittleEndian.AppendUint32(frame, uint32(len(value)))
	frame = append(frame, key...)
	return append(frame, value...)
}
