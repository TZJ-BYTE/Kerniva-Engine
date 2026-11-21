package tests

import (
	"bytes"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/TZJ-BYTE/Kerniva/config"
	"github.com/TZJ-BYTE/Kerniva/internal/protocol"
	"github.com/TZJ-BYTE/Kerniva/internal/server"
)

func TestGnetServer(t *testing.T) {
	// 随机端口
	port := 16380
	cfg := &config.Config{
		Host:        "127.0.0.1",
		Port:        port,
		NetworkType: "gnet",
		DBCount:     16,
	}

	srv := server.NewServer(cfg)

	go func() {
		if err := srv.Start(); err != nil {
			// 如果 gnet 启动失败（可能是端口占用或环境问题），记录日志但不 fail
			// 因为测试环境可能受限
			t.Logf("Server start error: %v", err)
		}
	}()

	// 等待启动
	time.Sleep(1 * time.Second)
	defer srv.Stop()

	// 连接
	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	// 发送 PING
	writeAll(t, conn, []byte("*1\r\n$4\r\nPING\r\n"))

	var rbuf bytes.Buffer
	v := readResp(t, conn, &rbuf)
	if s, ok := v.(string); !ok || s != "PONG" {
		t.Fatalf("PING resp=%#v", v)
	}

	// 测试 SELECT
	writeAll(t, conn, []byte("*2\r\n$6\r\nSELECT\r\n$1\r\n1\r\n"))
	v = readResp(t, conn, &rbuf)
	if s, ok := v.(string); !ok || s != "OK" {
		t.Fatalf("SELECT resp=%#v", v)
	}

	// SET k1 hello
	writeAll(t, conn, []byte("*3\r\n$3\r\nSET\r\n$2\r\nk1\r\n$5\r\nhello\r\n"))
	v = readResp(t, conn, &rbuf)
	if s, ok := v.(string); !ok || s != "OK" {
		t.Fatalf("SET resp=%#v", v)
	}

	// MGET k1 k2(missing) => ["hello", nil]
	writeAll(t, conn, []byte("*3\r\n$4\r\nMGET\r\n$2\r\nk1\r\n$2\r\nk2\r\n"))
	v = readResp(t, conn, &rbuf)
	arr, ok := v.([]interface{})
	if !ok || len(arr) != 2 || arr[0] != "hello" || arr[1] != nil {
		t.Fatalf("MGET resp=%#v", v)
	}

	// HSET h f1 v1
	writeAll(t, conn, []byte("*4\r\n$4\r\nHSET\r\n$1\r\nh\r\n$2\r\nf1\r\n$2\r\nv1\r\n"))
	v = readResp(t, conn, &rbuf)
	if n, ok := v.(int64); !ok || n != 1 {
		t.Fatalf("HSET resp=%#v", v)
	}

	// HMGET h f1 f2(missing) => ["v1", nil]
	writeAll(t, conn, []byte("*4\r\n$5\r\nHMGET\r\n$1\r\nh\r\n$2\r\nf1\r\n$2\r\nf2\r\n"))
	v = readResp(t, conn, &rbuf)
	arr, ok = v.([]interface{})
	if !ok || len(arr) != 2 || arr[0] != "v1" || arr[1] != nil {
		t.Fatalf("HMGET resp=%#v", v)
	}

	// SET with options should not be intercepted by fastpath
	writeAll(t, conn, []byte("*5\r\n$3\r\nSET\r\n$2\r\nk2\r\n$5\r\nhello\r\n$2\r\nEX\r\n$1\r\n1\r\n"))
	v = readResp(t, conn, &rbuf)
	if s, ok := v.(string); !ok || s != "OK" {
		t.Fatalf("SET EX resp=%#v", v)
	}

	// FLUSHALL clears across dbs
	writeAll(t, conn, []byte("*1\r\n$8\r\nFLUSHALL\r\n"))
	v = readResp(t, conn, &rbuf)
	if s, ok := v.(string); !ok || s != "OK" {
		t.Fatalf("FLUSHALL resp=%#v", v)
	}

	// After FLUSHALL, key should be nil
	writeAll(t, conn, []byte("*2\r\n$3\r\nGET\r\n$2\r\nk1\r\n"))
	v = readResp(t, conn, &rbuf)
	if v != nil {
		t.Fatalf("GET after flushall resp=%#v", v)
	}
}

func TestGnetServerTransactions(t *testing.T) {
	port := 16381
	cfg := &config.Config{
		Host:        "127.0.0.1",
		Port:        port,
		NetworkType: "gnet",
		DBCount:     16,
	}

	srv := server.NewServer(cfg)
	go func() {
		if err := srv.Start(); err != nil {
			t.Logf("Server start error: %v", err)
		}
	}()

	time.Sleep(1 * time.Second)
	defer srv.Stop()

	conn1, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("Failed to connect conn1: %v", err)
	}
	defer conn1.Close()

	conn2, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("Failed to connect conn2: %v", err)
	}
	defer conn2.Close()

	var rbuf1 bytes.Buffer
	var rbuf2 bytes.Buffer

	writeAll(t, conn1, []byte("*1\r\n$5\r\nMULTI\r\n"))
	if s := readResp(t, conn1, &rbuf1); s != "OK" {
		t.Fatalf("MULTI resp=%#v", s)
	}

	writeAll(t, conn1, []byte("*3\r\n$3\r\nSET\r\n$2\r\ntx\r\n$1\r\n1\r\n"))
	if s := readResp(t, conn1, &rbuf1); s != "QUEUED" {
		t.Fatalf("SET queued resp=%#v", s)
	}

	writeAll(t, conn1, []byte("*2\r\n$4\r\nINCR\r\n$2\r\ntx\r\n"))
	if s := readResp(t, conn1, &rbuf1); s != "QUEUED" {
		t.Fatalf("INCR queued resp=%#v", s)
	}

	writeAll(t, conn1, []byte("*1\r\n$4\r\nEXEC\r\n"))
	v := readResp(t, conn1, &rbuf1)
	arr, ok := v.([]interface{})
	if !ok || len(arr) != 2 || arr[0] != "OK" || arr[1] != int64(2) {
		t.Fatalf("EXEC resp=%#v", v)
	}

	writeAll(t, conn1, []byte("*2\r\n$3\r\nGET\r\n$2\r\ntx\r\n"))
	if s := readResp(t, conn1, &rbuf1); s != "2" {
		t.Fatalf("GET tx resp=%#v", s)
	}

	writeAll(t, conn1, []byte("*2\r\n$5\r\nWATCH\r\n$7\r\nwatched\r\n"))
	if s := readResp(t, conn1, &rbuf1); s != "OK" {
		t.Fatalf("WATCH resp=%#v", s)
	}

	writeAll(t, conn2, []byte("*3\r\n$3\r\nSET\r\n$7\r\nwatched\r\n$1\r\nx\r\n"))
	if s := readResp(t, conn2, &rbuf2); s != "OK" {
		t.Fatalf("SET watched resp=%#v", s)
	}

	writeAll(t, conn1, []byte("*1\r\n$5\r\nMULTI\r\n"))
	if s := readResp(t, conn1, &rbuf1); s != "OK" {
		t.Fatalf("MULTI after watch resp=%#v", s)
	}

	writeAll(t, conn1, []byte("*2\r\n$3\r\nGET\r\n$7\r\nwatched\r\n"))
	if s := readResp(t, conn1, &rbuf1); s != "QUEUED" {
		t.Fatalf("GET queued resp=%#v", s)
	}

	writeAll(t, conn1, []byte("*1\r\n$4\r\nEXEC\r\n"))
	if v := readResp(t, conn1, &rbuf1); v != nil {
		t.Fatalf("EXEC with dirty watch resp=%#v", v)
	}

	writeAll(t, conn1, []byte("*1\r\n$7\r\nDISCARD\r\n"))
	if errResp, ok := readResp(t, conn1, &rbuf1).(error); !ok || errResp.Error() != "ERR DISCARD without MULTI" {
		t.Fatalf("DISCARD without MULTI resp=%#v", errResp)
	}
}

func TestGnetServerBlockingListCommands(t *testing.T) {
	port := 16382
	cfg := &config.Config{
		Host:        "127.0.0.1",
		Port:        port,
		NetworkType: "gnet",
		DBCount:     16,
	}

	srv := server.NewServer(cfg)
	go func() {
		if err := srv.Start(); err != nil {
			t.Logf("Server start error: %v", err)
		}
	}()

	time.Sleep(1 * time.Second)
	defer srv.Stop()

	conn1, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("Failed to connect conn1: %v", err)
	}
	defer conn1.Close()

	conn2, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("Failed to connect conn2: %v", err)
	}
	defer conn2.Close()

	var rbuf1 bytes.Buffer
	var rbuf2 bytes.Buffer
	producerDone := make(chan struct{})

	go func() {
		defer close(producerDone)
		time.Sleep(150 * time.Millisecond)
		writeAll(t, conn2, []byte("*3\r\n$5\r\nRPUSH\r\n$5\r\njobs1\r\n$2\r\nj1\r\n"))
		_ = readResp(t, conn2, &rbuf2)
	}()

	writeAll(t, conn1, []byte("*3\r\n$5\r\nBLPOP\r\n$5\r\njobs1\r\n$1\r\n1\r\n"))
	v := readResp(t, conn1, &rbuf1)
	arr, ok := v.([]interface{})
	if !ok || len(arr) != 2 || arr[0] != "jobs1" || arr[1] != "j1" {
		t.Fatalf("BLPOP resp=%#v", v)
	}
	<-producerDone
	producerDone = make(chan struct{})

	go func() {
		defer close(producerDone)
		time.Sleep(150 * time.Millisecond)
		writeAll(t, conn2, []byte("*3\r\n$5\r\nLPUSH\r\n$5\r\njobs2\r\n$2\r\nj2\r\n"))
		_ = readResp(t, conn2, &rbuf2)
	}()

	writeAll(t, conn1, []byte("*3\r\n$5\r\nBRPOP\r\n$5\r\njobs2\r\n$1\r\n1\r\n"))
	v = readResp(t, conn1, &rbuf1)
	arr, ok = v.([]interface{})
	if !ok || len(arr) != 2 || arr[0] != "jobs2" || arr[1] != "j2" {
		t.Fatalf("BRPOP resp=%#v", v)
	}
	<-producerDone
	producerDone = make(chan struct{})

	go func() {
		defer close(producerDone)
		time.Sleep(150 * time.Millisecond)
		writeAll(t, conn2, []byte("*3\r\n$5\r\nRPUSH\r\n$3\r\nsrc\r\n$2\r\nj3\r\n"))
		_ = readResp(t, conn2, &rbuf2)
	}()

	writeAll(t, conn1, []byte("*4\r\n$10\r\nBRPOPLPUSH\r\n$3\r\nsrc\r\n$4\r\ndest\r\n$1\r\n1\r\n"))
	v = readResp(t, conn1, &rbuf1)
	if s, ok := v.(string); !ok || s != "j3" {
		t.Fatalf("BRPOPLPUSH resp=%#v", v)
	}
	<-producerDone
	producerDone = make(chan struct{})

	go func() {
		defer close(producerDone)
		time.Sleep(150 * time.Millisecond)
		writeAll(t, conn2, []byte("*3\r\n$5\r\nRPUSH\r\n$4\r\nsrc2\r\n$2\r\nj4\r\n"))
		_ = readResp(t, conn2, &rbuf2)
	}()

	writeAll(t, conn1, []byte("*6\r\n$6\r\nBLMOVE\r\n$4\r\nsrc2\r\n$11\r\nprocessing2\r\n$5\r\nRIGHT\r\n$4\r\nLEFT\r\n$1\r\n1\r\n"))
	v = readResp(t, conn1, &rbuf1)
	if s, ok := v.(string); !ok || s != "j4" {
		t.Fatalf("BLMOVE resp=%#v", v)
	}
	<-producerDone

	writeAll(t, conn1, []byte("*4\r\n$6\r\nLRANGE\r\n$11\r\nprocessing2\r\n$1\r\n0\r\n$2\r\n-1\r\n"))
	if v := readResp(t, conn1, &rbuf1); !equalRESPArray(v, "j4") {
		t.Fatalf("BLMOVE destination resp=%#v", v)
	}

	producerDone = make(chan struct{})
	go func() {
		defer close(producerDone)
		time.Sleep(150 * time.Millisecond)
		writeAll(t, conn2, []byte("*5\r\n$5\r\nRPUSH\r\n$2\r\nq1\r\n$2\r\nm1\r\n$2\r\nm2\r\n$2\r\nm3\r\n"))
		_ = readResp(t, conn2, &rbuf2)
	}()

	writeAll(t, conn1, []byte("*8\r\n$6\r\nBLMPOP\r\n$1\r\n1\r\n$1\r\n2\r\n$2\r\nq0\r\n$2\r\nq1\r\n$4\r\nLEFT\r\n$5\r\nCOUNT\r\n$1\r\n2\r\n"))
	v = readResp(t, conn1, &rbuf1)
	arr, ok = v.([]interface{})
	if !ok || len(arr) != 2 || arr[0] != "q1" {
		t.Fatalf("BLMPOP resp=%#v", v)
	}
	popped, ok := arr[1].([]interface{})
	if !ok || len(popped) != 2 || popped[0] != "m1" || popped[1] != "m2" {
		t.Fatalf("BLMPOP popped=%#v", arr[1])
	}
	<-producerDone

	writeAll(t, conn1, []byte("*2\r\n$3\r\nGET\r\n$4\r\ndest\r\n"))
	if errResp, ok := readResp(t, conn1, &rbuf1).(error); !ok || errResp == nil {
		t.Fatalf("expected WRONGTYPE for dest GET, got %#v", errResp)
	}

	writeAll(t, conn1, []byte("*3\r\n$5\r\nBLPOP\r\n$7\r\nmissing\r\n$3\r\n0.1\r\n"))
	if v := readResp(t, conn1, &rbuf1); v != nil {
		t.Fatalf("BLPOP timeout resp=%#v", v)
	}
}

func TestGnetServerPubSub(t *testing.T) {
	port := 16383
	cfg := &config.Config{
		Host:        "127.0.0.1",
		Port:        port,
		NetworkType: "gnet",
		DBCount:     16,
	}

	srv := server.NewServer(cfg)
	go func() {
		if err := srv.Start(); err != nil {
			t.Logf("Server start error: %v", err)
		}
	}()

	time.Sleep(1 * time.Second)
	defer srv.Stop()

	subscriber, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("Failed to connect subscriber: %v", err)
	}
	defer subscriber.Close()

	publisher, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("Failed to connect publisher: %v", err)
	}
	defer publisher.Close()

	var subBuf bytes.Buffer
	var pubBuf bytes.Buffer

	writeAll(t, subscriber, []byte("*2\r\n$9\r\nSUBSCRIBE\r\n$4\r\nnews\r\n"))
	if v := readResp(t, subscriber, &subBuf); !equalRESPArray(v, "subscribe", "news", int64(1)) {
		t.Fatalf("SUBSCRIBE resp=%#v", v)
	}

	writeAll(t, publisher, []byte("*3\r\n$7\r\nPUBLISH\r\n$4\r\nnews\r\n$5\r\nhello\r\n"))
	if v := readResp(t, publisher, &pubBuf); v != int64(1) {
		t.Fatalf("PUBLISH resp=%#v", v)
	}
	if v := readResp(t, subscriber, &subBuf); !equalRESPArray(v, "message", "news", "hello") {
		t.Fatalf("message resp=%#v", v)
	}

	writeAll(t, subscriber, []byte("*2\r\n$11\r\nUNSUBSCRIBE\r\n$4\r\nnews\r\n"))
	if v := readResp(t, subscriber, &subBuf); !equalRESPArray(v, "unsubscribe", "news", int64(0)) {
		t.Fatalf("UNSUBSCRIBE resp=%#v", v)
	}

	writeAll(t, publisher, []byte("*3\r\n$7\r\nPUBLISH\r\n$4\r\nnews\r\n$5\r\nlater\r\n"))
	if v := readResp(t, publisher, &pubBuf); v != int64(0) {
		t.Fatalf("PUBLISH after unsubscribe resp=%#v", v)
	}

	writeAll(t, subscriber, []byte("*2\r\n$9\r\nSUBSCRIBE\r\n$4\r\nnews\r\n"))
	if v := readResp(t, subscriber, &subBuf); !equalRESPArray(v, "subscribe", "news", int64(1)) {
		t.Fatalf("SUBSCRIBE again resp=%#v", v)
	}

	_ = subscriber.Close()

	deadline := time.Now().Add(2 * time.Second)
	for {
		writeAll(t, publisher, []byte("*3\r\n$7\r\nPUBLISH\r\n$4\r\nnews\r\n$5\r\nfinal\r\n"))
		if v := readResp(t, publisher, &pubBuf); v == int64(0) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected closed subscriber to be cleaned up")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestGnetServerPubSubPatternsAndIntrospection(t *testing.T) {
	port := 16384
	cfg := &config.Config{
		Host:        "127.0.0.1",
		Port:        port,
		NetworkType: "gnet",
		DBCount:     16,
	}

	srv := server.NewServer(cfg)
	go func() {
		if err := srv.Start(); err != nil {
			t.Logf("Server start error: %v", err)
		}
	}()

	time.Sleep(1 * time.Second)
	defer srv.Stop()

	sub1, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("Failed to connect sub1: %v", err)
	}
	defer sub1.Close()

	sub2, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("Failed to connect sub2: %v", err)
	}
	defer sub2.Close()

	publisher, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("Failed to connect publisher: %v", err)
	}
	defer publisher.Close()

	var subBuf1 bytes.Buffer
	var subBuf2 bytes.Buffer
	var pubBuf bytes.Buffer

	writeAll(t, sub1, []byte("*2\r\n$9\r\nSUBSCRIBE\r\n$4\r\nnews\r\n"))
	if v := readResp(t, sub1, &subBuf1); !equalRESPArray(v, "subscribe", "news", int64(1)) {
		t.Fatalf("sub1 SUBSCRIBE resp=%#v", v)
	}

	writeAll(t, sub1, []byte("*2\r\n$10\r\nPSUBSCRIBE\r\n$2\r\nn*\r\n"))
	if v := readResp(t, sub1, &subBuf1); !equalRESPArray(v, "psubscribe", "n*", int64(2)) {
		t.Fatalf("sub1 PSUBSCRIBE resp=%#v", v)
	}

	writeAll(t, sub2, []byte("*2\r\n$9\r\nSUBSCRIBE\r\n$4\r\nnews\r\n"))
	if v := readResp(t, sub2, &subBuf2); !equalRESPArray(v, "subscribe", "news", int64(1)) {
		t.Fatalf("sub2 SUBSCRIBE resp=%#v", v)
	}

	writeAll(t, publisher, []byte("*2\r\n$6\r\nPUBSUB\r\n$8\r\nCHANNELS\r\n"))
	if v := readResp(t, publisher, &pubBuf); !equalRESPArray(v, "news") {
		t.Fatalf("PUBSUB CHANNELS resp=%#v", v)
	}

	writeAll(t, publisher, []byte("*3\r\n$6\r\nPUBSUB\r\n$8\r\nCHANNELS\r\n$2\r\nn*\r\n"))
	if v := readResp(t, publisher, &pubBuf); !equalRESPArray(v, "news") {
		t.Fatalf("PUBSUB CHANNELS n* resp=%#v", v)
	}

	writeAll(t, publisher, []byte("*4\r\n$6\r\nPUBSUB\r\n$6\r\nNUMSUB\r\n$4\r\nnews\r\n$7\r\nmissing\r\n"))
	if v := readResp(t, publisher, &pubBuf); !equalRESPArray(v, "news", int64(2), "missing", int64(0)) {
		t.Fatalf("PUBSUB NUMSUB resp=%#v", v)
	}

	writeAll(t, publisher, []byte("*3\r\n$7\r\nPUBLISH\r\n$4\r\nnews\r\n$5\r\nhello\r\n"))
	if v := readResp(t, publisher, &pubBuf); v != int64(2) {
		t.Fatalf("PUBLISH resp=%#v", v)
	}

	v1 := readResp(t, sub1, &subBuf1)
	v2 := readResp(t, sub1, &subBuf1)
	if !matchesPubSubSet(v1, v2,
		[]interface{}{"message", "news", "hello"},
		[]interface{}{"pmessage", "n*", "news", "hello"},
	) {
		t.Fatalf("sub1 publish messages resp1=%#v resp2=%#v", v1, v2)
	}

	if v := readResp(t, sub2, &subBuf2); !equalRESPArray(v, "message", "news", "hello") {
		t.Fatalf("sub2 message resp=%#v", v)
	}

	writeAll(t, sub1, []byte("*2\r\n$12\r\nPUNSUBSCRIBE\r\n$2\r\nn*\r\n"))
	if v := readResp(t, sub1, &subBuf1); !equalRESPArray(v, "punsubscribe", "n*", int64(1)) {
		t.Fatalf("sub1 PUNSUBSCRIBE resp=%#v", v)
	}

	writeAll(t, publisher, []byte("*4\r\n$6\r\nPUBSUB\r\n$6\r\nNUMSUB\r\n$4\r\nnews\r\n$7\r\nmissing\r\n"))
	if v := readResp(t, publisher, &pubBuf); !equalRESPArray(v, "news", int64(2), "missing", int64(0)) {
		t.Fatalf("PUBSUB NUMSUB after punsubscribe resp=%#v", v)
	}

	writeAll(t, sub2, []byte("*2\r\n$11\r\nUNSUBSCRIBE\r\n$4\r\nnews\r\n"))
	if v := readResp(t, sub2, &subBuf2); !equalRESPArray(v, "unsubscribe", "news", int64(0)) {
		t.Fatalf("sub2 UNSUBSCRIBE resp=%#v", v)
	}

	writeAll(t, publisher, []byte("*3\r\n$6\r\nPUBSUB\r\n$6\r\nNUMSUB\r\n$4\r\nnews\r\n"))
	if v := readResp(t, publisher, &pubBuf); !equalRESPArray(v, "news", int64(1)) {
		t.Fatalf("PUBSUB NUMSUB single resp=%#v", v)
	}

	writeAll(t, sub1, []byte("*2\r\n$11\r\nUNSUBSCRIBE\r\n$4\r\nnews\r\n"))
	if v := readResp(t, sub1, &subBuf1); !equalRESPArray(v, "unsubscribe", "news", int64(0)) {
		t.Fatalf("sub1 UNSUBSCRIBE resp=%#v", v)
	}

	writeAll(t, publisher, []byte("*2\r\n$6\r\nPUBSUB\r\n$8\r\nCHANNELS\r\n"))
	if v := readResp(t, publisher, &pubBuf); !equalRESPArray(v) {
		t.Fatalf("PUBSUB CHANNELS empty resp=%#v", v)
	}
}

func writeAll(t *testing.T, conn net.Conn, p []byte) {
	t.Helper()
	for len(p) > 0 {
		n, err := conn.Write(p)
		if err != nil {
			t.Fatalf("Write error: %v", err)
		}
		p = p[n:]
	}
}

func readExpect(t *testing.T, conn net.Conn, rbuf *bytes.Buffer, expected []byte) {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))

	tmp := make([]byte, 4096)
	for rbuf.Len() < len(expected) {
		n, err := conn.Read(tmp)
		if err != nil {
			t.Fatalf("Read error: %v", err)
		}
		rbuf.Write(tmp[:n])
	}
	got := rbuf.Bytes()[:len(expected)]
	if !bytes.Equal(got, expected) {
		t.Fatalf("Expected %q, got %q", string(expected), string(rbuf.Bytes()))
	}
	rbuf.Next(len(expected))
}

func readResp(t *testing.T, conn net.Conn, rbuf *bytes.Buffer) interface{} {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))

	tmp := make([]byte, 4096)
	for {
		v, n, ok, err := parseResp(rbuf.Bytes())
		if err != nil {
			t.Fatalf("parse err: %v", err)
		}
		if ok {
			rbuf.Next(n)
			return v
		}
		nr, err := conn.Read(tmp)
		if err != nil {
			t.Fatalf("Read error: %v", err)
		}
		rbuf.Write(tmp[:nr])
	}
}

func parseResp(b []byte) (interface{}, int, bool, error) {
	if len(b) < 1 {
		return nil, 0, false, nil
	}
	switch b[0] {
	case '+', '-', ':', '$', '*':
	default:
		return nil, 0, false, nil
	}

	readLine := func(off int) ([]byte, int, bool) {
		i := bytes.Index(b[off:], []byte("\r\n"))
		if i < 0 {
			return nil, 0, false
		}
		line := b[off : off+i]
		return line, off + i + 2, true
	}

	switch b[0] {
	case '+':
		line, next, ok := readLine(1)
		if !ok {
			return nil, 0, false, nil
		}
		return string(line), next, true, nil
	case '-':
		line, next, ok := readLine(1)
		if !ok {
			return nil, 0, false, nil
		}
		return fmt.Errorf("%s", string(line)), next, true, nil
	case ':':
		line, next, ok := readLine(1)
		if !ok {
			return nil, 0, false, nil
		}
		n, err := protocol.ParseInt(line)
		if err != nil {
			return nil, 0, false, err
		}
		return int64(n), next, true, nil
	case '$':
		line, next, ok := readLine(1)
		if !ok {
			return nil, 0, false, nil
		}
		n, err := protocol.ParseInt(line)
		if err != nil {
			return nil, 0, false, err
		}
		if n == -1 {
			return nil, next, true, nil
		}
		if n < 0 {
			return nil, 0, false, fmt.Errorf("invalid bulk len")
		}
		end := next + n + 2
		if end > len(b) {
			return nil, 0, false, nil
		}
		if b[next+n] != '\r' || b[next+n+1] != '\n' {
			return nil, 0, false, fmt.Errorf("invalid bulk trailer")
		}
		return string(b[next : next+n]), end, true, nil
	case '*':
		line, next, ok := readLine(1)
		if !ok {
			return nil, 0, false, nil
		}
		n, err := protocol.ParseInt(line)
		if err != nil {
			return nil, 0, false, err
		}
		if n == -1 {
			return nil, next, true, nil
		}
		if n < 0 {
			return nil, 0, false, fmt.Errorf("invalid array len")
		}
		out := make([]interface{}, 0, n)
		off := next
		for i := 0; i < n; i++ {
			v, nn, ok, err := parseResp(b[off:])
			if err != nil {
				return nil, 0, false, err
			}
			if !ok {
				return nil, 0, false, nil
			}
			out = append(out, v)
			off += nn
		}
		return out, off, true, nil
	default:
		return nil, 0, false, nil
	}
}

func equalRESPArray(v interface{}, expected ...interface{}) bool {
	arr, ok := v.([]interface{})
	if !ok || len(arr) != len(expected) {
		return false
	}
	for i := range expected {
		if arr[i] != expected[i] {
			return false
		}
	}
	return true
}

func matchesPubSubSet(v1 interface{}, v2 interface{}, expected1 []interface{}, expected2 []interface{}) bool {
	return (equalRESPArray(v1, expected1...) && equalRESPArray(v2, expected2...)) ||
		(equalRESPArray(v1, expected2...) && equalRESPArray(v2, expected1...))
}
