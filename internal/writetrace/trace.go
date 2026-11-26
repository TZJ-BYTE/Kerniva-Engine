// Package writetrace records bounded timing data for the primary's plain SET path.
package writetrace

import (
	"bufio"
	"encoding/json"
	"errors"
	"os"
	"sync/atomic"
	"time"
)

const capacity = 262144

var active atomic.Pointer[Recorder]

type Recorder struct {
	origin  time.Time
	file    *os.File
	rows    []Request
	next    atomic.Uint64
	batchID atomic.Uint64
}

type Request struct {
	recorder      *Recorder
	Port          int    `json:"local_port"`
	Ordinal       uint64 `json:"conn_ordinal"`
	Command       string `json:"command"`
	Parsed        int64  `json:"parsed_ns"`
	Dispatch      int64  `json:"dispatch_ns"`
	ExecutionLock int64  `json:"execution_locked_ns,omitempty"`
	KeyLock       int64  `json:"key_locked_ns,omitempty"`
	Queue         int64  `json:"queue_ns,omitempty"`
	Result        int64  `json:"result_ns,omitempty"`
	Encode        int64  `json:"response_ready_ns,omitempty"`
	Send          int64  `json:"response_start_ns,omitempty"`
	Sent          int64  `json:"response_end_ns,omitempty"`
	SendOK        bool   `json:"response_ok"`
	Batch         Batch  `json:"batch"`
}

type Batch struct {
	recorder *Recorder
	ID       uint64 `json:"id,omitempty"`
	Size     int    `json:"size,omitempty"`
	Bytes    int    `json:"bytes,omitempty"`
	Sequence uint64 `json:"sequence,omitempty"`
	Dequeue  int64  `json:"dequeue_ns,omitempty"`
	Locked   int64  `json:"locked_ns,omitempty"`
	Append   int64  `json:"append_ns,omitempty"`
	Write    int64  `json:"write_start_ns,omitempty"`
	Written  int64  `json:"write_end_ns,omitempty"`
	Sync     int64  `json:"sync_start_ns,omitempty"`
	Synced   int64  `json:"sync_end_ns,omitempty"`
	Release  int64  `json:"release_ns,omitempty"`
	Failed   bool   `json:"failed,omitempty"`
}

// Open runs before the primary starts accepting connections. All observation
// data stays in memory until Close, after the server has joined its workers.
func Open(path string) (*Recorder, error) {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return nil, err
	}
	r := &Recorder{origin: time.Now(), file: file, rows: make([]Request, capacity)}
	active.Store(r)
	return r, nil
}

func Active() *Recorder { return active.Load() }

func (r *Recorder) Stamp() int64 { return time.Since(r.origin).Nanoseconds() }

func (r *Recorder) Begin(port int, ordinal uint64, command string, parsed int64) *Request {
	i := r.next.Add(1) - 1
	if i >= uint64(len(r.rows)) {
		return nil
	}
	row := &r.rows[i]
	*row = Request{recorder: r, Port: port, Ordinal: ordinal, Command: command, Parsed: parsed, Dispatch: r.Stamp()}
	return row
}

func (r *Request) Stamp() int64 { return r.recorder.Stamp() }

func (r *Request) NewBatch() Batch { return r.recorder.NewBatch() }

func (r *Recorder) NewBatch() Batch {
	return Batch{recorder: r, ID: r.batchID.Add(1), Append: r.Stamp()}
}

func (b *Batch) Stamp() int64 { return b.recorder.Stamp() }

func (r *Recorder) Close() (err error) {
	active.CompareAndSwap(r, nil)
	defer func() { err = errors.Join(err, r.file.Close()) }()
	output := bufio.NewWriterSize(r.file, 128<<10)
	encoder := json.NewEncoder(output)
	if err := encoder.Encode(map[string]any{"type": "meta", "schema_version": 1, "origin_unix_ns": r.origin.UnixNano(), "capacity": len(r.rows), "connection_identity": "client TCP port and 1-based parsed command ordinal"}); err != nil {
		return err
	}
	seen := r.next.Load()
	kept := min(seen, uint64(len(r.rows)))
	for i := uint64(0); i < kept; i++ {
		if err := encoder.Encode(&r.rows[i]); err != nil {
			return err
		}
	}
	if err := encoder.Encode(map[string]any{"type": "summary", "seen": seen, "kept": kept, "drops": seen - kept}); err != nil {
		return err
	}
	return output.Flush()
}
