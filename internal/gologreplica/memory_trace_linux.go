//go:build linux

package gologreplica

import (
	"bytes"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

const memoryTraceEnv = "KERNIVA_GOLOG_MEMORY_TRACE"

type memoryTrace struct {
	file    *os.File
	mu      sync.Mutex
	failed  bool
	signals chan os.Signal
	stop    chan struct{}
	done    chan struct{}
}

type recoveryTrace struct {
	trace      *memoryTrace
	db         int
	generation string
	kind       string
	sequence   uint64
	recovery   string
}

type memoryTraceEvent struct {
	Time              time.Time `json:"time"`
	MonotonicNS       int64     `json:"monotonic_ns"`
	DB                int       `json:"db"`
	Generation        string    `json:"generation"`
	Kind              string    `json:"kind"`
	Sequence          uint64    `json:"sequence"`
	Recovery          string    `json:"recovery,omitempty"`
	Stage             string    `json:"stage"`
	HeapAllocBytes    uint64    `json:"heap_alloc_bytes"`
	HeapInuseBytes    uint64    `json:"heap_inuse_bytes"`
	HeapSysBytes      uint64    `json:"heap_sys_bytes"`
	HeapReleasedBytes uint64    `json:"heap_released_bytes"`
	TotalAllocBytes   uint64    `json:"total_alloc_bytes"`
	HeapObjects       uint64    `json:"heap_objects"`
	NumGC             uint32    `json:"num_gc"`
	RSSBytes          uint64    `json:"rss_bytes,omitempty"`
}

func openMemoryTrace() *memoryTrace {
	path := os.Getenv(memoryTraceEnv)
	if path == "" {
		return nil
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		log.Printf("open replica memory trace: %v", err)
		return nil
	}
	trace := &memoryTrace{
		file:    file,
		signals: make(chan os.Signal, 1),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
	signal.Notify(trace.signals, unix.SIGUSR1)
	go trace.sampleOnSignal()
	return trace
}

func (trace *memoryTrace) sampleOnSignal() {
	defer close(trace.done)
	sample := recoveryTrace{trace: trace, db: -1, kind: "process"}
	for {
		select {
		case <-trace.signals:
			sample.record("sample")
		case <-trace.stop:
			return
		}
	}
}

func (trace *memoryTrace) close() {
	if trace == nil {
		return
	}
	signal.Stop(trace.signals)
	close(trace.stop)
	<-trace.done
	if err := trace.file.Close(); err != nil {
		log.Printf("close replica memory trace: %v", err)
	}
}

func (event recoveryTrace) handleObserver() func() {
	if event.trace == nil {
		return nil
	}
	return func() { event.record("rust_handle_ready") }
}

func (event recoveryTrace) record(stage string) {
	trace := event.trace
	if trace == nil {
		return
	}
	trace.mu.Lock()
	defer trace.mu.Unlock()
	if trace.failed {
		return
	}

	now := time.Now().UTC()
	var monotonic unix.Timespec
	_ = unix.ClockGettime(unix.CLOCK_MONOTONIC, &monotonic)
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	rss := currentRSSBytes()
	if err := json.NewEncoder(trace.file).Encode(memoryTraceEvent{
		Time: now, MonotonicNS: monotonic.Nano(), DB: event.db,
		Generation: event.generation, Kind: event.kind, Sequence: event.sequence,
		Recovery: event.recovery, Stage: stage, HeapAllocBytes: memory.HeapAlloc,
		HeapInuseBytes: memory.HeapInuse, HeapSysBytes: memory.HeapSys,
		HeapReleasedBytes: memory.HeapReleased, TotalAllocBytes: memory.TotalAlloc,
		HeapObjects: memory.HeapObjects, NumGC: memory.NumGC, RSSBytes: rss,
	}); err != nil {
		trace.failed = true
		log.Printf("write replica memory trace: %v", err)
	}
}

func currentRSSBytes() uint64 {
	data, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0
	}
	fields := bytes.Fields(data)
	if len(fields) < 2 {
		return 0
	}
	pages, err := strconv.ParseUint(string(fields[1]), 10, 64)
	if err != nil {
		return 0
	}
	return pages * uint64(os.Getpagesize())
}
