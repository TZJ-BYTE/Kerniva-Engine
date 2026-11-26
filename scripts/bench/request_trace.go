package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"os"
	"time"
)

const maxTracePerWorker = 4096
const traceSlowThreshold = 100 * time.Millisecond

type requestTiming struct {
	Worker      int           `json:"worker"`
	Index       int64         `json:"index"`
	Op          string        `json:"op"`
	Planned     time.Duration `json:"planned_ns"`
	Dequeued    time.Duration `json:"dequeued_ns"`
	SendStart   time.Duration `json:"send_start_ns"`
	Completed   time.Duration `json:"completed_ns"`
	Outcome     string        `json:"outcome"`
	Uniform     bool          `json:"uniform"`
	LocalPort   int           `json:"local_port,omitempty"`
	ConnOrdinal uint64        `json:"conn_ordinal,omitempty"`
}

type traceCoverage struct {
	Offered     int64 `json:"offered"`
	Uniform     int64 `json:"uniform"`
	Slow        int64 `json:"slow"`
	KeptUniform int64 `json:"kept_uniform"`
	KeptSlow    int64 `json:"kept_slow"`
	Kept        int64 `json:"kept"`
	Drops       int64 `json:"drops"`
}

type workerTrace struct {
	coverage    [][2]traceCoverage
	rows        []requestTiming
	candidates  int64
	drops       int64
	localPort   int
	connOrdinal uint64
}

func (t *workerTrace) add(index int64, write bool, planned, dequeued, sendStart, completed time.Duration, outcome string, o *mixedOptions) {
	// Distinct hash domain from the operation and key selection streams.
	uniform := mixHash(uint64(index)^o.seed^0xd81b54a32d192ed3)%o.traceStride == 0
	opIndex := 0
	if write {
		opIndex = 1
	}
	counts := &t.coverage[int(planned/time.Second)][opIndex]
	counts.Offered++
	slow := completed-planned >= traceSlowThreshold
	if uniform {
		counts.Uniform++
	}
	if slow {
		counts.Slow++
	}
	if !uniform && !slow {
		return
	}
	t.candidates++
	if len(t.rows) == cap(t.rows) {
		counts.Drops++
		t.drops++
		return
	}
	counts.Kept++
	if slow {
		counts.KeptSlow++
	}
	if uniform {
		counts.KeptUniform++
	}
	op := "GET"
	if write {
		op = "SET"
	}
	row := requestTiming{Index: index, Op: op, Planned: planned, Dequeued: dequeued, SendStart: sendStart, Completed: completed, Outcome: outcome, Uniform: uniform}
	if sendStart >= 0 {
		row.LocalPort, row.ConnOrdinal = t.localPort, t.connOrdinal
	}
	t.rows = append(t.rows, row)
}

func writeRequestTrace(path, started string, total int64, o *mixedOptions, traces []workerTrace) (err error) {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, file.Close()) }()
	buffer := bufio.NewWriter(file)
	encoder := json.NewEncoder(buffer)
	meta := map[string]any{"type": "meta", "schema_version": 1, "started_utc": started, "offered": total, "clients": len(traces), "rate": o.rate, "seed": o.seed, "stride": o.traceStride, "slow_threshold_ns": traceSlowThreshold.Nanoseconds(), "cap_per_worker": maxTracePerWorker, "time_origin": "client measurement monotonic epoch", "send_start": "immediately before mixedExchange", "unsent_sentinel": -1, "connection_identity": "local TCP port and 1-based send ordinal, restarted on reconnect"}
	if err := encoder.Encode(meta); err != nil {
		return err
	}
	var candidates, kept, drops int64
	for worker, trace := range traces {
		candidates += trace.candidates
		drops += trace.drops
		kept += int64(len(trace.rows))
		for _, row := range trace.rows {
			row.Worker = worker
			if err := encoder.Encode(row); err != nil {
				return err
			}
		}
	}
	coverage := make([][][2]traceCoverage, len(traces))
	for i := range traces {
		coverage[i] = traces[i].coverage
	}
	if err := encoder.Encode(map[string]any{"by_worker_second_get_set": coverage, "type": "summary", "offered": total, "candidates": candidates, "kept": kept, "drops": drops}); err != nil {
		return err
	}
	return buffer.Flush()
}
