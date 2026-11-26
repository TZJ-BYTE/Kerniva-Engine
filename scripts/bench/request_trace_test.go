package main

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestTraceSamplingCoverage(t *testing.T) {
	o := testMixedOptions()
	o.traceStride = 16
	o.readPercent = 95
	for worker := 0; worker < 64; worker++ {
		trace := workerTrace{rows: make([]requestTiming, 0, 4096), coverage: make([][2]traceCoverage, 1)}
		for index := int64(worker); index < 180000; index += 64 {
			write, _ := selectOperation(index, o)
			trace.add(index, write, 0, 1, 2, 3, "success", o)
		}
		for _, c := range trace.coverage[0] {
			if c.Uniform == 0 || c.KeptUniform != c.Kept || c.Slow != 0 || c.Drops != 0 {
				t.Fatalf("worker %d %+v", worker, c)
			}
		}
	}
	trace := workerTrace{rows: make([]requestTiming, 0, 1), coverage: make([][2]traceCoverage, 1)}
	for i := int64(0); i < 2; i++ {
		trace.add(i, true, 0, 0, 0, traceSlowThreshold, "success", o)
	}
	if c := trace.coverage[0][1]; c.Offered != 2 || c.Slow != 2 || c.KeptSlow != 1 || c.Drops != 1 || trace.candidates != 2 {
		t.Fatalf("slow record coverage %+v", c)
	}
}

func TestMeasuredRequestTrace(t *testing.T) {
	o := testMixedOptions()
	o.rate = 300
	o.tracePath = filepath.Join(t.TempDir(), "requests.jsonl")
	o.traceStride = 1
	o.readPercent = 50
	address := fakeMixedServer(t, 0, false)
	res, err := measureRate(address, 2, o.duration, o)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(o.tracePath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	rows := uint64(0)
	summary := false
	if !scanner.Scan() {
		t.Fatal("trace header missing")
	}
	var header struct {
		Type    string
		Started string `json:"started_utc"`
	}
	if err := json.Unmarshal(scanner.Bytes(), &header); err != nil || header.Type != "meta" || header.Started != res.result.StartedUTC {
		t.Fatalf("bad header %+v %v", header, err)
	}
	for scanner.Scan() {
		var kind struct{ Type string }
		if err := json.Unmarshal(scanner.Bytes(), &kind); err != nil {
			t.Fatal(err)
		}
		if kind.Type == "summary" {
			var totals struct{ Offered, Kept, Drops uint64 }
			if err := json.Unmarshal(scanner.Bytes(), &totals); err != nil || totals.Offered != rows || totals.Kept != rows || totals.Drops != 0 {
				t.Fatalf("bad totals %+v %v", totals, err)
			}
			summary = true
			continue
		}
		var row requestTiming
		if err := json.Unmarshal(scanner.Bytes(), &row); err != nil {
			t.Fatal(err)
		}
		if summary || row.Index%2 != int64(row.Worker) || row.Planned != scheduledOffset(row.Index, o.rate) || row.Planned > row.Dequeued || row.Dequeued > row.SendStart || row.SendStart > row.Completed || !row.Uniform || row.Outcome != "success" || row.LocalPort == 0 || row.ConnOrdinal != uint64(row.Index/2+1) {
			t.Fatalf("invalid timing %+v", row)
		}
		rows++
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if !summary || rows != res.result.Succeeded || rows == 0 {
		t.Fatalf("incomplete trace %d", rows)
	}
}

func TestUnsentTrace(t *testing.T) {
	o := testMixedOptions()
	o.traceStride = 1
	trace := workerTrace{rows: make([]requestTiming, 0, 1), coverage: make([][2]traceCoverage, 1)}
	trace.add(0, false, 0, 200*time.Millisecond, -1, 200*time.Millisecond, "queue_timeout", o)
	if row := trace.rows[0]; row.SendStart != -1 || row.Outcome != "queue_timeout" || row.Completed != row.Dequeued {
		t.Fatal(row)
	}
}
