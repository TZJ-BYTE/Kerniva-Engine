package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

type mixedOptions struct {
	mode             string
	rate             int64
	duration         time.Duration
	warmup           time.Duration
	timeout          time.Duration
	keys             uint64
	readPercent      int
	hotPercent       int
	hotKeys          uint64
	queue            int64
	seed             uint64
	prefill          bool
	insertWrites     bool
	insertWindow     int
	missingReads     bool
	preloadBatch     int
	reconnectBackoff time.Duration
	tracePath        string
	traceStride      uint64
	histogramPath    string
	readyFile        string
	startFile        string
}

func mixedFlags() *mixedOptions {
	o := &mixedOptions{}
	flag.StringVar(&o.mode, "mode", "simple", "simple or mixed fixed-rate workload")
	flag.Int64Var(&o.rate, "rate", 10000, "total planned mixed requests per second")
	flag.DurationVar(&o.duration, "duration", 30*time.Second, "mixed measured offer window")
	flag.DurationVar(&o.warmup, "warmup", 3*time.Second, "mixed warmup before measured window")
	flag.DurationVar(&o.timeout, "timeout", 2*time.Second, "mixed deadline measured from planned send time")
	flag.Uint64Var(&o.keys, "keys", 100000, "mixed working-set key count, 1 selects single-key contention")
	flag.IntVar(&o.readPercent, "read-percent", 95, "mixed GET percentage")
	flag.IntVar(&o.hotPercent, "hot-percent", 0, "percentage of requests selecting the hot subset")
	flag.Uint64Var(&o.hotKeys, "hot-keys", 1000, "number of keys in the hot subset")
	flag.Int64Var(&o.queue, "queue-per-client", 4096, "virtual pending requests per connection, 0 uses only the deadline bound")
	flag.Uint64Var(&o.seed, "seed", 42, "deterministic operation and key seed")
	flag.BoolVar(&o.prefill, "prefill", true, "preload the mixed working set using verified MSET batches")
	flag.BoolVar(&o.insertWrites, "insert-writes", false, "send SET to a distinct new key for every scheduled write")
	flag.BoolVar(&o.missingReads, "missing-reads", false, "send GET to keys that were never written")
	flag.IntVar(&o.preloadBatch, "preload-batch", 256, "keys per unmeasured preload MSET")
	flag.DurationVar(&o.reconnectBackoff, "reconnect-backoff", time.Second, "minimum per-connection delay after failure before another dial")
	flag.StringVar(&o.histogramPath, "histogram-out", "", "optional sparse histogram JSON file")
	flag.StringVar(&o.readyFile, "ready-file", "", "optional ready marker after preload and warmup")
	flag.StringVar(&o.startFile, "start-file", "", "optional start marker written by the measurement controller")
	flag.StringVar(&o.tracePath, "request-trace-out", "", "optional measured request timing JSONL")
	flag.Uint64Var(&o.traceStride, "request-trace-stride", 16, "include one in N requests plus all requests at least 100ms when tracing")
	return o
}

func (o *mixedOptions) validate() error {
	if o.tracePath != "" && o.traceStride == 0 {
		return fmt.Errorf("request-trace-stride must be positive")
	}
	if o.rate <= 0 || o.duration <= 0 || o.timeout <= 0 || o.warmup < 0 {
		return fmt.Errorf("rate, duration and timeout must be positive, warmup must be nonnegative")
	}
	if o.keys == 0 || o.readPercent < 0 || o.readPercent > 100 || o.hotPercent < 0 || o.hotPercent > 100 {
		return fmt.Errorf("keys must be positive and percentages must be between 0 and 100")
	}
	if o.hotPercent > 0 && (o.hotKeys == 0 || o.hotKeys > o.keys) {
		return fmt.Errorf("hot-keys must be within the working set")
	}
	if o.queue < 0 || o.preloadBatch <= 0 || o.reconnectBackoff < 0 {
		return fmt.Errorf("queue and reconnect-backoff must be nonnegative and preload-batch must be positive")
	}
	if (o.readyFile == "") != (o.startFile == "") {
		return fmt.Errorf("ready-file and start-file must be supplied together")
	}
	if o.rate > math.MaxInt64/int64(time.Second) {
		return fmt.Errorf("rate exceeds addressable nanosecond schedule")
	}
	_, err := plannedRequests(o.duration, o.rate)
	return err
}

type operationCounts struct {
	Offered           uint64 `json:"offered"`
	Sent              uint64 `json:"sent"`
	Succeeded         uint64 `json:"succeeded"`
	SucceededInWindow uint64 `json:"succeeded_in_window"`
	SentInWindow      uint64 `json:"sent_in_window"`
	QueueDrops        uint64 `json:"queue_drops"`
	QueueTimeouts     uint64 `json:"queue_timeouts"`
	RequestTimeouts   uint64 `json:"request_timeouts"`
	NetworkErrors     uint64 `json:"network_errors"`
	ReplyErrors       uint64 `json:"reply_errors"`
	ReconnectAttempts uint64 `json:"reconnect_attempts"`
}

func (c *operationCounts) add(other operationCounts) {
	c.Offered += other.Offered
	c.Sent += other.Sent
	c.Succeeded += other.Succeeded
	c.SucceededInWindow += other.SucceededInWindow
	c.SentInWindow += other.SentInWindow
	c.QueueDrops += other.QueueDrops
	c.QueueTimeouts += other.QueueTimeouts
	c.RequestTimeouts += other.RequestTimeouts
	c.NetworkErrors += other.NetworkErrors
	c.ReplyErrors += other.ReplyErrors
	c.ReconnectAttempts += other.ReconnectAttempts
}

func (c operationCounts) failures() uint64 {
	return c.QueueDrops + c.QueueTimeouts + c.RequestTimeouts + c.NetworkErrors + c.ReplyErrors
}

type operationMeasurement struct {
	operationCounts
	Failures         uint64         `json:"failures"`
	SuccessLatency   latencySummary `json:"success_latency"`
	ObservedLatency  latencySummary `json:"observed_latency"`
	ClientQueueDelay latencySummary `json:"client_queue_delay"`
}

type operationAccumulator struct {
	counts   operationCounts
	success  latencyHistogram
	observed latencyHistogram
	queue    latencyHistogram
}

func (a *operationAccumulator) measurement() operationMeasurement {
	return operationMeasurement{operationCounts: a.counts, Failures: a.counts.failures(), SuccessLatency: a.success.summary(), ObservedLatency: a.observed.summary(), ClientQueueDelay: a.queue.summary()}
}

type operationSnapshot struct {
	Counts   operationCounts   `json:"counts"`
	Success  histogramSnapshot `json:"success"`
	Observed histogramSnapshot `json:"observed"`
	Queue    histogramSnapshot `json:"client_queue"`
}

func (a *operationAccumulator) snapshot() operationSnapshot {
	return operationSnapshot{Counts: a.counts, Success: a.success.snapshot(), Observed: a.observed.snapshot(), Queue: a.queue.snapshot()}
}

func (a *operationAccumulator) merge(snapshot operationSnapshot) {
	a.counts.add(snapshot.Counts)
	a.success.merge(snapshot.Success)
	a.observed.merge(snapshot.Observed)
	a.queue.merge(snapshot.Queue)
}

type windowSnapshot struct {
	Second     int
	Get        operationSnapshot
	Set        operationSnapshot
	FirstError string
}

type mixedWindow struct {
	Second int                  `json:"planned_second"`
	Get    operationMeasurement `json:"get"`
	Set    operationMeasurement `json:"set"`
}

type mixedResult struct {
	SchemaVersion           int                  `json:"schema_version"`
	Mode                    string               `json:"mode"`
	Network                 string               `json:"network"`
	Durability              string               `json:"durability"`
	StartedUTC              string               `json:"started_utc"`
	Clients                 int                  `json:"clients"`
	Keys                    uint64               `json:"keys"`
	ValueBytes              int                  `json:"value_bytes"`
	ReadPercent             int                  `json:"read_percent"`
	HotPercent              int                  `json:"hot_percent"`
	HotKeys                 uint64               `json:"hot_keys"`
	InsertWrites            bool                 `json:"insert_writes,omitempty"`
	MissingReads            bool                 `json:"missing_reads,omitempty"`
	Seed                    uint64               `json:"seed"`
	TargetRate              int64                `json:"target_rate"`
	DurationSeconds         float64              `json:"duration_seconds"`
	ElapsedSeconds          float64              `json:"elapsed_seconds"`
	DrainSeconds            float64              `json:"drain_seconds"`
	ActualRate              float64              `json:"actual_success_rate"`
	ActualSendRate          float64              `json:"actual_send_rate"`
	WindowSuccessRate       float64              `json:"window_success_rate"`
	WindowSendRate          float64              `json:"window_send_rate"`
	TimeoutSeconds          float64              `json:"timeout_seconds"`
	QueuePerClient          int64                `json:"queue_per_client"`
	ReconnectBackoffSeconds float64              `json:"reconnect_backoff_seconds"`
	Offered                 uint64               `json:"offered"`
	Sent                    uint64               `json:"sent"`
	Succeeded               uint64               `json:"succeeded"`
	Failures                uint64               `json:"failures"`
	LatencyOrigin           string               `json:"latency_origin"`
	WindowBasis             string               `json:"window_basis"`
	HistogramRelative       float64              `json:"histogram_max_relative_error"`
	Get                     operationMeasurement `json:"get"`
	Set                     operationMeasurement `json:"set"`
	Windows                 []mixedWindow        `json:"windows"`
	ErrorExamples           []string             `json:"error_examples,omitempty"`
	ClientNumGCDelta        uint32               `json:"client_num_gc_delta"`
	ClientPauseNSDelta      uint64               `json:"client_gc_pause_ns_delta"`
	ClientHeapAllocBytes    uint64               `json:"client_heap_alloc_bytes"`
}

type rateOutput struct {
	result mixedResult
	get    operationSnapshot
	set    operationSnapshot
}

func plannedRequests(duration time.Duration, rate int64) (int64, error) {
	seconds, fraction := int64(duration/time.Second), int64(duration%time.Second)
	if rate <= 0 || seconds > math.MaxInt64/rate || fraction > math.MaxInt64/rate {
		return 0, fmt.Errorf("planned request count overflows int64")
	}
	whole, extra := seconds*rate, fraction*rate/int64(time.Second)
	if whole > math.MaxInt64-extra || whole+extra == 0 {
		return 0, fmt.Errorf("duration and rate must plan at least one addressable request")
	}
	return whole + extra, nil
}

func scheduledOffset(index, rate int64) time.Duration {
	return time.Duration(index/rate)*time.Second + time.Duration(index%rate)*time.Second/time.Duration(rate)
}

func mixHash(value uint64) uint64 {
	value += 0x9e3779b97f4a7c15
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	return value ^ (value >> 31)
}

func selectOperation(index int64, o *mixedOptions) (write bool, key uint64) {
	choice := mixHash(uint64(index) ^ o.seed)
	write = choice%100 >= uint64(o.readPercent)
	keyChoice := mixHash(choice)
	if o.hotPercent > 0 {
		if mixHash(keyChoice)%100 < uint64(o.hotPercent) {
			return write, keyChoice % o.hotKeys
		}
		if o.keys > o.hotKeys {
			return write, o.hotKeys + keyChoice%(o.keys-o.hotKeys)
		}
	}
	return write, keyChoice % o.keys
}

func keyValue(key uint64, seed uint64, value []byte) {
	binary.LittleEndian.PutUint64(value[:8], key)
	for i := 8; i < 32; i += 8 {
		binary.LittleEndian.PutUint64(value[i:i+8], mixHash(key^seed^uint64(i)))
	}
}

func appendKey(dst []byte, key uint64) []byte {
	dst = append(dst, "kerniva:thesis:"...)
	return strconv.AppendUint(dst, key, 10)
}

func appendInsertKey(dst []byte, window int, index int64) []byte {
	dst = append(dst, "kerniva:thesis:new:"...)
	dst = strconv.AppendInt(dst, int64(window), 10)
	dst = append(dst, ':')
	return strconv.AppendInt(dst, index, 10)
}

func appendBulk(dst, value []byte) []byte {
	dst = append(dst, '$')
	dst = strconv.AppendInt(dst, int64(len(value)), 10)
	dst = append(dst, '\r', '\n')
	dst = append(dst, value...)
	return append(dst, '\r', '\n')
}

func mixedRequest(dst, key, value []byte, write bool) []byte {
	dst = dst[:0]
	if write {
		dst = append(dst, "*3\r\n$3\r\nSET\r\n"...)
	} else {
		dst = append(dst, "*2\r\n$3\r\nGET\r\n"...)
	}
	dst = appendBulk(dst, key)
	if write {
		dst = appendBulk(dst, value)
	}
	return dst
}

type unexpectedReply struct{ message string }

func (e *unexpectedReply) Error() string { return e.message }

func mixedExchange(conn net.Conn, reader *bufio.Reader, request, expected, payload []byte, write bool) error {
	for remaining := request; len(remaining) > 0; {
		written, err := conn.Write(remaining)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		remaining = remaining[written:]
	}
	header, err := reader.ReadSlice('\n')
	if err != nil {
		return err
	}
	if write {
		if !bytes.Equal(header, []byte("+OK\r\n")) {
			return &unexpectedReply{message: fmt.Sprintf("unexpected SET reply %q", header)}
		}
		return nil
	}
	if expected == nil {
		if !bytes.Equal(header, []byte("$-1\r\n")) {
			return &unexpectedReply{message: fmt.Sprintf("expected missing GET reply, got %q", header)}
		}
		return nil
	}
	if !bytes.Equal(header, []byte("$32\r\n")) {
		return &unexpectedReply{message: fmt.Sprintf("unexpected GET reply %q", header)}
	}
	if _, err := io.ReadFull(reader, payload[:34]); err != nil {
		return err
	}
	if !bytes.Equal(payload[:32], expected) || payload[32] != '\r' || payload[33] != '\n' {
		return &unexpectedReply{message: "GET payload mismatch"}
	}
	return nil
}

func preloadMixed(address string, o *mixedOptions) error {
	conn, err := net.DialTimeout("tcp", address, 5*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)
	request := make([]byte, 0, o.preloadBatch*96)
	key := make([]byte, 0, 48)
	var value [32]byte
	var payload [34]byte
	for first := uint64(0); first < o.keys; {
		count := min(uint64(o.preloadBatch), o.keys-first)
		request = append(request[:0], '*')
		request = strconv.AppendUint(request, 1+count*2, 10)
		request = append(request, "\r\n$4\r\nMSET\r\n"...)
		for index := first; index < first+count; index++ {
			key = appendKey(key[:0], index)
			keyValue(index, o.seed, value[:])
			request = appendBulk(request, key)
			request = appendBulk(request, value[:])
		}
		if err := conn.SetDeadline(time.Now().Add(30 * time.Second)); err != nil {
			return err
		}
		if err := mixedExchange(conn, reader, request, value[:], payload[:], true); err != nil {
			return fmt.Errorf("prefill at key %d: %w", first, err)
		}
		first += count
	}
	return nil
}

func runMixed(address, concurrency, network, durability string, o *mixedOptions) error {
	if err := o.validate(); err != nil {
		return err
	}
	if o.prefill {
		if err := preloadMixed(address, o); err != nil {
			return err
		}
	}
	clientCounts := strings.Split(concurrency, ",")
	if len(clientCounts) != 1 && o.readyFile != "" {
		return fmt.Errorf("controller handshake requires one client count")
	}
	for run, raw := range clientCounts {
		clients, err := strconv.Atoi(raw)
		if err != nil || clients <= 0 {
			return fmt.Errorf("invalid clients %q", raw)
		}
		window := *o
		if window.tracePath != "" && len(clientCounts) > 1 {
			window.tracePath = fmt.Sprintf("%s.clients-%d.jsonl", window.tracePath, clients)
		}
		window.insertWindow = 2*run + 1
		if window.warmup > 0 {
			warmupOptions := window
			warmupOptions.tracePath = ""
			warmupOptions.insertWindow--
			warmupOptions.readyFile, warmupOptions.startFile = "", ""
			if _, err := measureRate(address, clients, window.warmup, &warmupOptions); err != nil {
				return fmt.Errorf("warmup: %w", err)
			}
		}
		output, err := measureRate(address, clients, window.duration, &window)
		if err != nil {
			return err
		}
		output.result.Network, output.result.Durability = network, durability
		if o.histogramPath != "" {
			path := o.histogramPath
			if len(clientCounts) > 1 {
				path = fmt.Sprintf("%s.clients-%d.json", path, clients)
			}
			file, err := os.Create(path)
			if err != nil {
				return err
			}
			encoded := struct {
				SchemaVersion int               `json:"schema_version"`
				RelativeError float64           `json:"max_relative_error"`
				Get           operationSnapshot `json:"get"`
				Set           operationSnapshot `json:"set"`
			}{2, 1.0 / 256.0, output.get, output.set}
			encodeErr := json.NewEncoder(file).Encode(encoded)
			closeErr := file.Close()
			if encodeErr != nil {
				return encodeErr
			}
			if closeErr != nil {
				return closeErr
			}
		}
		if err := json.NewEncoder(os.Stdout).Encode(output.result); err != nil {
			return err
		}
	}
	return nil
}

func measureRate(address string, clients int, duration time.Duration, o *mixedOptions) (rateOutput, error) {
	total, err := plannedRequests(duration, o.rate)
	if err != nil {
		return rateOutput{}, err
	}
	connections := make([]net.Conn, clients)
	for index := range connections {
		connections[index], err = net.DialTimeout("tcp", address, 5*time.Second)
		if err != nil {
			for _, conn := range connections[:index] {
				conn.Close()
			}
			return rateOutput{}, err
		}
	}
	events := make(chan windowSnapshot, clients)
	collected := make(chan rateOutput, 1)
	go collectWindows(events, collected)
	var ready, done sync.WaitGroup
	ready.Add(clients)
	done.Add(clients)
	start := make(chan struct{})
	var epoch int64 = -1
	var traces []workerTrace
	if o.tracePath != "" {
		traces = make([]workerTrace, clients)
		for i := range traces {
			capacity := min((total-1)/int64(clients)+1, maxTracePerWorker)
			traces[i].coverage = make([][2]traceCoverage, 1+scheduledOffset(total-1, o.rate)/time.Second)
			traces[i].rows = make([]requestTiming, 0, capacity)
		}
	}
	for worker := 0; worker < clients; worker++ {
		go func(worker int) {
			defer done.Done()
			var trace *workerTrace
			if traces != nil {
				trace = &traces[worker]
			}
			runRateWorker(trace, worker, clients, total, duration, address, o, connections[worker], &ready, start, &epoch, events)
		}(worker)
	}
	ready.Wait()
	runtime.GC()
	if o.readyFile != "" {
		if err := os.WriteFile(o.readyFile, []byte("ready\n"), 0600); err != nil {
			close(start)
			done.Wait()
			close(events)
			<-collected
			return rateOutput{}, err
		}
		deadline := time.Now().Add(30 * time.Second)
		for {
			if _, err := os.Stat(o.startFile); err == nil {
				break
			}
			if time.Now().After(deadline) {
				close(start)
				done.Wait()
				close(events)
				<-collected
				return rateOutput{}, fmt.Errorf("controller start marker timed out")
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	started := time.Now().UTC().Format(time.RFC3339Nano)
	epoch = clockNow()
	close(start)
	done.Wait()
	for remaining := duration - clockElapsed(epoch); remaining > 0; remaining = duration - clockElapsed(epoch) {
		time.Sleep(remaining)
	}
	elapsed := clockElapsed(epoch)
	close(events)
	output := <-collected
	runtime.ReadMemStats(&after)
	r := &output.result
	r.SchemaVersion, r.Mode, r.StartedUTC = 2, "mixed", started
	r.Clients, r.Keys, r.ValueBytes = clients, o.keys, 32
	r.ReadPercent, r.HotPercent, r.HotKeys, r.Seed = o.readPercent, o.hotPercent, o.hotKeys, o.seed
	r.InsertWrites = o.insertWrites
	r.MissingReads = o.missingReads
	r.TargetRate, r.DurationSeconds, r.ElapsedSeconds = o.rate, duration.Seconds(), elapsed.Seconds()
	r.DrainSeconds = max(0, elapsed.Seconds()-duration.Seconds())
	r.TimeoutSeconds, r.QueuePerClient = o.timeout.Seconds(), o.queue
	r.ReconnectBackoffSeconds = o.reconnectBackoff.Seconds()
	r.Offered, r.Sent, r.Succeeded = r.Get.Offered+r.Set.Offered, r.Get.Sent+r.Set.Sent, r.Get.Succeeded+r.Set.Succeeded
	r.Failures = r.Get.Failures + r.Set.Failures
	r.ActualRate, r.ActualSendRate = float64(r.Succeeded)/elapsed.Seconds(), float64(r.Sent)/elapsed.Seconds()
	r.WindowSuccessRate = float64(r.Get.SucceededInWindow+r.Set.SucceededInWindow) / duration.Seconds()
	r.WindowSendRate = float64(r.Get.SentInWindow+r.Set.SentInWindow) / duration.Seconds()
	r.LatencyOrigin = "planned_send_to_full_reply_or_failure_including_client_queue"
	r.WindowBasis, r.HistogramRelative = "planned_send_second", 1.0/256.0
	r.ClientNumGCDelta, r.ClientPauseNSDelta = after.NumGC-before.NumGC, after.PauseTotalNs-before.PauseTotalNs
	r.ClientHeapAllocBytes = after.HeapAlloc
	if r.Offered != uint64(total) || r.Succeeded+r.Failures != r.Offered {
		return rateOutput{}, fmt.Errorf("measurement accounting mismatch: planned=%d offered=%d success=%d failures=%d", total, r.Offered, r.Succeeded, r.Failures)
	}
	if traces != nil {
		if err := writeRequestTrace(o.tracePath, started, total, o, traces); err != nil {
			return rateOutput{}, err
		}
	}
	return output, nil
}

func runRateWorker(trace *workerTrace, worker, clients int, total int64, duration time.Duration, address string, o *mixedOptions, conn net.Conn, ready *sync.WaitGroup, start <-chan struct{}, epoch *int64, events chan<- windowSnapshot) {
	defer func() {
		if conn != nil {
			conn.Close()
		}
	}()
	reader := bufio.NewReader(conn)
	get, set := new(operationAccumulator), new(operationAccumulator)
	request, key := make([]byte, 0, 160), make([]byte, 0, 48)
	var expected [32]byte
	var payload [34]byte
	second := -1
	firstError := ""
	var nextDialAt time.Duration
	flush := func() {
		if second >= 0 {
			events <- windowSnapshot{Second: second, Get: get.snapshot(), Set: set.snapshot(), FirstError: firstError}
			*get, *set = operationAccumulator{}, operationAccumulator{}
			firstError = ""
		}
	}
	ready.Done()
	<-start
	if *epoch < 0 {
		return
	}
	for index := int64(worker); index < total; index += int64(clients) {
		planned := scheduledOffset(index, o.rate)
		if nextSecond := int(planned / time.Second); nextSecond != second {
			flush()
			second = nextSecond
		}
		for remaining := planned - clockElapsed(*epoch); remaining > 0; remaining = planned - clockElapsed(*epoch) {
			time.Sleep(remaining)
		}
		write, keyIndex := selectOperation(index, o)
		operation := get
		if write {
			operation = set
		}
		operation.counts.Offered++
		if conn == nil {
			resumeAt := min(nextDialAt, planned+o.timeout)
			for remaining := resumeAt - clockElapsed(*epoch); remaining > 0; remaining = resumeAt - clockElapsed(*epoch) {
				time.Sleep(remaining)
			}
		}
		queueDelay := max(clockElapsed(*epoch)-planned, 0)
		operation.queue.record(queueDelay)
		if queueDelay >= o.timeout {
			operation.counts.QueueTimeouts++
			operation.observed.record(o.timeout)
			if trace != nil {
				trace.add(index, write, planned, planned+queueDelay, -1, planned+queueDelay, "queue_timeout", o)
			}
			continue
		}
		if o.queue > 0 && float64(queueDelay)*float64(o.rate)/float64(time.Second)/float64(clients) >= float64(o.queue) {
			operation.counts.QueueDrops++
			operation.observed.record(queueDelay)
			if trace != nil {
				trace.add(index, write, planned, planned+queueDelay, -1, planned+queueDelay, "queue_drop", o)
			}
			continue
		}
		replyValue := expected[:]
		if write && o.insertWrites {
			key = appendInsertKey(key[:0], o.insertWindow, index)
			keyIndex = uint64(index)
		} else {
			key = appendKey(key[:0], keyIndex)
			if !write && o.missingReads {
				key = append(key, ":absent"...)
				replyValue = nil
			}
		}
		keyValue(keyIndex, o.seed, expected[:])
		request = mixedRequest(request, key, expected[:], write)
		remaining := o.timeout - (clockElapsed(*epoch) - planned)
		var err error
		if conn == nil {
			operation.counts.ReconnectAttempts++
			conn, err = net.DialTimeout("tcp", address, max(remaining, time.Nanosecond))
			if err == nil {
				reader.Reset(conn)
			}
		}
		if err == nil {
			remaining = o.timeout - (clockElapsed(*epoch) - planned)
			if remaining <= 0 {
				err = os.ErrDeadlineExceeded
			} else {
				err = conn.SetDeadline(time.Now().Add(remaining))
			}
		}
		sendStart := time.Duration(-1)
		if err == nil {
			operation.counts.Sent++
			sendStart = clockElapsed(*epoch)
			if sendStart < duration {
				operation.counts.SentInWindow++
			}
			err = mixedExchange(conn, reader, request, replyValue, payload[:], write)
		}
		latency := clockElapsed(*epoch) - planned
		operation.observed.record(latency)
		if err == nil {
			operation.counts.Succeeded++
			operation.success.record(latency)
			if clockElapsed(*epoch) < duration {
				operation.counts.SucceededInWindow++
			}
			if trace != nil {
				trace.add(index, write, planned, planned+queueDelay, sendStart, planned+latency, "success", o)
			}
			continue
		}
		var replyError *unexpectedReply
		var networkError net.Error
		outcome := "network_error"
		switch {
		case errors.As(err, &replyError):
			outcome = "reply_error"
			operation.counts.ReplyErrors++
		case errors.As(err, &networkError) && networkError.Timeout():
			outcome = "request_timeout"
			operation.counts.RequestTimeouts++
		default:
			operation.counts.NetworkErrors++
		}
		if trace != nil {
			trace.add(index, write, planned, planned+queueDelay, sendStart, planned+latency, outcome, o)
		}
		if firstError == "" {
			firstError = err.Error()
		}
		if conn != nil {
			conn.Close()
			conn = nil
		}
		nextDialAt = clockElapsed(*epoch) + o.reconnectBackoff
	}
	flush()
}

func collectWindows(events <-chan windowSnapshot, finished chan<- rateOutput) {
	type accumulators struct{ get, set *operationAccumulator }
	windows := make(map[int]accumulators)
	get, set := new(operationAccumulator), new(operationAccumulator)
	examples := make(map[string]bool)
	var output rateOutput
	for event := range events {
		window, ok := windows[event.Second]
		if !ok {
			window = accumulators{get: new(operationAccumulator), set: new(operationAccumulator)}
			windows[event.Second] = window
		}
		window.get.merge(event.Get)
		window.set.merge(event.Set)
		get.merge(event.Get)
		set.merge(event.Set)
		if event.FirstError != "" && len(examples) < 8 && !examples[event.FirstError] {
			examples[event.FirstError] = true
			output.result.ErrorExamples = append(output.result.ErrorExamples, event.FirstError)
		}
	}
	for second := 0; second < len(windows); second++ {
		window := windows[second]
		output.result.Windows = append(output.result.Windows, mixedWindow{Second: second, Get: window.get.measurement(), Set: window.set.measurement()})
	}
	output.result.Get, output.result.Set = get.measurement(), set.measurement()
	output.get, output.set = get.snapshot(), set.snapshot()
	finished <- output
}
