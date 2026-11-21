package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type result struct {
	Operation  string  `json:"operation"`
	Network    string  `json:"network"`
	Durability string  `json:"durability"`
	Clients    int     `json:"clients"`
	Requests   int     `json:"requests"`
	ValueBytes int     `json:"value_bytes"`
	OpsPerSec  float64 `json:"ops_per_sec"`
	P50us      float64 `json:"p50_us"`
	P95us      float64 `json:"p95_us"`
	P99us      float64 `json:"p99_us"`
}

func main() {
	address := flag.String("addr", "127.0.0.1:16379", "server address")
	concurrency := flag.String("clients", "1,16,64", "comma-separated connection counts")
	requests := flag.Int("requests", 10000, "total measured operations per command and concurrency")
	network := flag.String("network", "unspecified", "network backend label")
	durability := flag.String("durability", "unspecified", "memory, none, flush, or sync label")
	mixed := mixedFlags()
	flag.Parse()
	if mixed.mode == "mixed" {
		if err := runMixed(*address, *concurrency, *network, *durability, mixed); err != nil {
			fail(err)
		}
		return
	}
	if mixed.mode != "simple" {
		fail(fmt.Errorf("unknown mode %q", mixed.mode))
	}
	if *requests <= 0 {
		fail(fmt.Errorf("requests must be positive"))
	}
	for _, raw := range strings.Split(*concurrency, ",") {
		clients, err := strconv.Atoi(raw)
		if err != nil || clients <= 0 {
			fail(fmt.Errorf("invalid clients %q", raw))
		}
		for _, operation := range []string{"SET", "GET"} {
			measurement, err := measure(*address, operation, clients, *requests)
			if err != nil {
				fail(err)
			}
			measurement.Network = *network
			measurement.Durability = *durability
			if err := json.NewEncoder(os.Stdout).Encode(measurement); err != nil {
				fail(err)
			}
		}
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

func measure(address, operation string, clients, requests int) (result, error) {
	value := bytes.Repeat([]byte("v"), 32)
	latencies := make([]time.Duration, requests)
	var next atomic.Int64
	var ready, done sync.WaitGroup
	ready.Add(clients)
	done.Add(clients)
	start := make(chan struct{})
	errors := make(chan error, clients)
	for client := 0; client < clients; client++ {
		go func(client int) {
			defer done.Done()
			conn, err := net.DialTimeout("tcp", address, 5*time.Second)
			if err != nil {
				errors <- err
				ready.Done()
				return
			}
			defer conn.Close()
			if err := conn.SetDeadline(time.Now().Add(5 * time.Minute)); err != nil {
				errors <- err
				ready.Done()
				return
			}
			reader := bufio.NewReader(conn)
			key := fmt.Sprintf("kerniva:bench:%d", client)
			set := []byte(fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$32\r\n%s\r\n", len(key), key, value))
			get := []byte(fmt.Sprintf("*2\r\n$3\r\nGET\r\n$%d\r\n%s\r\n", len(key), key))
			request := set
			if operation == "GET" {
				request = get
			}
			if err := exchange(conn, reader, set, "SET", value); err != nil {
				errors <- err
				ready.Done()
				return
			}
			for warmup := 0; warmup < 20; warmup++ {
				if err := exchange(conn, reader, request, operation, value); err != nil {
					errors <- err
					ready.Done()
					return
				}
			}
			ready.Done()
			<-start
			for {
				index := int(next.Add(1) - 1)
				if index >= requests {
					return
				}
				began := clockNow()
				if err := exchange(conn, reader, request, operation, value); err != nil {
					errors <- err
					return
				}
				latencies[index] = clockElapsed(began)
			}
		}(client)
	}
	ready.Wait()
	began := clockNow()
	close(start)
	done.Wait()
	elapsed := clockElapsed(began)
	close(errors)
	for err := range errors {
		return result{}, err
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	percentile := func(percent int) float64 {
		index := (requests*percent+99)/100 - 1
		return float64(latencies[index]) / float64(time.Microsecond)
	}
	return result{Operation: operation, Clients: clients, Requests: requests, ValueBytes: len(value), OpsPerSec: float64(requests) / elapsed.Seconds(), P50us: percentile(50), P95us: percentile(95), P99us: percentile(99)}, nil
}

func exchange(conn net.Conn, reader *bufio.Reader, request []byte, operation string, expected []byte) error {
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
	header, err := reader.ReadString('\n')
	if err != nil {
		return err
	}
	if operation == "SET" {
		if header != "+OK\r\n" {
			return fmt.Errorf("unexpected SET reply %q", header)
		}
		return nil
	}
	if header != "$32\r\n" {
		return fmt.Errorf("unexpected GET reply %q", header)
	}
	var payload [34]byte
	if _, err := io.ReadFull(reader, payload[:]); err != nil {
		return err
	}
	if !bytes.Equal(payload[:32], expected) || string(payload[32:]) != "\r\n" {
		return fmt.Errorf("GET value mismatch")
	}
	return nil
}
