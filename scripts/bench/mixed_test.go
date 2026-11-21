package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func fakeMixedServer(t *testing.T, delay time.Duration, corrupt bool, closeFirst ...bool) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var connections sync.Map
	var closedFirst atomic.Bool
	t.Cleanup(func() {
		listener.Close()
		connections.Range(func(key, _ any) bool {
			key.(net.Conn).Close()
			return true
		})
	})
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			if len(closeFirst) > 0 && closeFirst[0] && closedFirst.CompareAndSwap(false, true) {
				conn.Close()
				continue
			}
			connections.Store(conn, true)
			go func() {
				defer conn.Close()
				defer connections.Delete(conn)
				reader := bufio.NewReader(conn)
				for {
					args, err := readTestRequest(reader)
					if err != nil {
						return
					}
					time.Sleep(delay)
					if string(args[0]) == "SET" {
						if _, err := conn.Write([]byte("+OK\r\n")); err != nil {
							return
						}
						continue
					}
					key, err := strconv.ParseUint(strings.TrimPrefix(string(args[1]), "kerniva:thesis:"), 10, 64)
					if err != nil {
						return
					}
					var value [32]byte
					keyValue(key, 42, value[:])
					if corrupt {
						value[0] ^= 1
					}
					reply := append([]byte("$32\r\n"), value[:]...)
					reply = append(reply, '\r', '\n')
					if _, err := conn.Write(reply); err != nil {
						return
					}
				}
			}()
		}
	}()
	return listener.Addr().String()
}

func readTestRequest(reader *bufio.Reader) ([][]byte, error) {
	header, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	count, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(header, "*")))
	if err != nil || count < 2 {
		return nil, fmt.Errorf("bad request header %q", header)
	}
	args := make([][]byte, count)
	for index := range args {
		header, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		length, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(header, "$")))
		if err != nil || length < 0 {
			return nil, fmt.Errorf("bad bulk header %q", header)
		}
		payload := make([]byte, length+2)
		if _, err := io.ReadFull(reader, payload); err != nil {
			return nil, err
		}
		args[index] = payload[:length]
	}
	return args, nil
}

func testMixedOptions() *mixedOptions {
	return &mixedOptions{rate: 1000, duration: 60 * time.Millisecond, timeout: time.Second, keys: 100, readPercent: 100, seed: 42, preloadBatch: 256}
}

func TestMixedPlannedCountAndQueueLatency(t *testing.T) {
	address := fakeMixedServer(t, 3*time.Millisecond, false)
	o := testMixedOptions()
	output, err := measureRate(address, 1, o.duration, o)
	if err != nil {
		t.Fatal(err)
	}
	r := output.result
	if r.Offered != 60 || r.Succeeded != 60 || r.Failures != 0 || r.Get.SuccessLatency.Count != 60 {
		t.Fatalf("lost planned denominator: offered=%d success=%d failures=%d", r.Offered, r.Succeeded, r.Failures)
	}
	if r.Get.SuccessLatency.P99Us < 20000 || r.Get.ClientQueueDelay.P99Us < 10000 {
		t.Fatalf("queue time missing: latency=%+v queue=%+v", r.Get.SuccessLatency, r.Get.ClientQueueDelay)
	}
	if r.ActualSendRate >= float64(o.rate) || r.DrainSeconds <= 0 {
		t.Fatalf("slow replies changed planned load: rate=%f drain=%f", r.ActualSendRate, r.DrainSeconds)
	}
}

func TestMixedTimeoutIncludesQueueAndKeepsFailures(t *testing.T) {
	address := fakeMixedServer(t, 100*time.Millisecond, false)
	o := testMixedOptions()
	o.duration, o.timeout = 50*time.Millisecond, 20*time.Millisecond
	output, err := measureRate(address, 1, o.duration, o)
	if err != nil {
		t.Fatal(err)
	}
	r := output.result
	if r.Offered != 50 || r.Succeeded+r.Failures != 50 || r.Succeeded != 0 {
		t.Fatalf("failed requests lost: offered=%d success=%d failures=%d", r.Offered, r.Succeeded, r.Failures)
	}
	if r.Get.RequestTimeouts == 0 || r.Get.ObservedLatency.Count != 50 || r.Get.ClientQueueDelay.P99Us < 10000 || r.ElapsedSeconds > .3 {
		t.Fatalf("queue deadline missing: %+v", r.Get)
	}
}

func TestMixedBoundedVirtualQueue(t *testing.T) {
	address := fakeMixedServer(t, 3*time.Millisecond, false)
	o := testMixedOptions()
	o.queue = 1
	output, err := measureRate(address, 1, o.duration, o)
	if err != nil {
		t.Fatal(err)
	}
	r := output.result
	if r.Offered != 60 || r.Succeeded+r.Failures != 60 || r.Get.QueueDrops == 0 {
		t.Fatalf("bounded queue lost accounting: %+v", r.Get)
	}
}

func TestMixedWrongPayloadIsFailure(t *testing.T) {
	address := fakeMixedServer(t, 0, true)
	o := testMixedOptions()
	o.rate, o.duration = 100, 50*time.Millisecond
	output, err := measureRate(address, 1, o.duration, o)
	if err != nil {
		t.Fatal(err)
	}
	r := output.result
	if r.Offered != 5 || r.Get.ReplyErrors != 5 || r.Succeeded != 0 || r.Failures != 5 {
		t.Fatalf("corrupt GET was accepted: %+v", r.Get)
	}
}

func TestMixedLowRateKeepsFullOfferWindow(t *testing.T) {
	address := fakeMixedServer(t, 0, false)
	o := testMixedOptions()
	o.rate, o.duration = 10, 100*time.Millisecond
	output, err := measureRate(address, 1, o.duration, o)
	if err != nil {
		t.Fatal(err)
	}
	r := output.result
	if r.Offered != 1 || r.Succeeded != 1 || r.ElapsedSeconds < .1 || r.ActualRate > 10 {
		t.Fatalf("incomplete denominator window: %+v", r)
	}
}

func TestMixedReconnectBackoffBoundsFailedDials(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	go func() {
		connection, err := listener.Accept()
		if err == nil {
			listener.Close()
			connection.Close()
		}
	}()
	o := testMixedOptions()
	o.rate, o.duration, o.timeout = 500, 200*time.Millisecond, 20*time.Millisecond
	o.reconnectBackoff = 50 * time.Millisecond
	output, err := measureRate(listener.Addr().String(), 1, o.duration, o)
	if err != nil {
		t.Fatal(err)
	}
	r := output.result
	if r.Offered != 100 || r.Succeeded != 0 || r.Failures != 100 || r.Sent > r.Offered {
		t.Fatalf("reconnect lost planned denominator: %+v", r.Get)
	}
	if r.Get.ReconnectAttempts < 2 || r.Get.ReconnectAttempts > 5 || r.Get.QueueTimeouts == 0 {
		t.Fatalf("failed reconnects escaped backoff: %+v", r.Get)
	}
	if r.Get.ClientQueueDelay.P99Us < 19000 || r.ElapsedSeconds < .2 || r.ElapsedSeconds > .4 {
		t.Fatalf("backoff time or offer window missing: %+v", r)
	}
}

func TestMixedReconnectBackoffRecoversAndIncludesQueue(t *testing.T) {
	address := fakeMixedServer(t, 0, false, true)
	o := testMixedOptions()
	o.rate, o.duration, o.timeout = 100, 200*time.Millisecond, 80*time.Millisecond
	o.reconnectBackoff = 50 * time.Millisecond
	output, err := measureRate(address, 1, o.duration, o)
	if err != nil {
		t.Fatal(err)
	}
	r := output.result
	if r.Offered != 20 || r.Succeeded != 19 || r.Failures != 1 || r.Get.ReconnectAttempts != 1 {
		t.Fatalf("failed to recover after bounded reconnect: %+v", r.Get)
	}
	if r.Get.SuccessLatency.P99Us < 30000 || r.Get.ClientQueueDelay.P99Us < 30000 {
		t.Fatalf("reconnect wait escaped queue latency: %+v", r.Get)
	}
}

func TestMixedSeedAndHotSubset(t *testing.T) {
	o := testMixedOptions()
	o.readPercent, o.hotPercent, o.hotKeys = 80, 100, 3
	copy := *o
	reads, writes := 0, 0
	for index := int64(0); index < 1000; index++ {
		write, key := selectOperation(index, o)
		otherWrite, otherKey := selectOperation(index, &copy)
		if write != otherWrite || key != otherKey || key >= 3 {
			t.Fatalf("nonreproducible trace at %d", index)
		}
		if write {
			writes++
		} else {
			reads++
		}
	}
	if reads < 750 || reads > 850 || writes == 0 {
		t.Fatalf("unexpected operation mix %d/%d", reads, writes)
	}
}

func TestHistogramRoundTripAndBounds(t *testing.T) {
	var h, restored latencyHistogram
	for _, ns := range []int64{0, 20, 511, 512, 1000, 1000000, 1000000000, 1<<62 + 3} {
		h.record(time.Duration(ns))
		upper := histogramUpper(histogramIndex(ns))
		if upper < ns || (ns >= 512 && float64(upper-ns)/float64(ns) > 1.0/256.0) {
			t.Fatalf("histogram bound ns=%d upper=%d", ns, upper)
		}
	}
	restored.merge(h.snapshot())
	if h.summary() != restored.summary() || h.count != 8 {
		t.Fatalf("histogram snapshot lost samples")
	}
}
