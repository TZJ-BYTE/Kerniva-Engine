package main

import (
	"context"
	"embed"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
	"time"
)

//go:embed web/*
var assets embed.FS

func main() {
	listen := flag.String("listen", "127.0.0.1:18080", "loopback HTTP address")
	database := flag.String("addr", "127.0.0.1:16379", "loopback Kerniva or Redis address")
	bench := flag.String("bench", defaultBench(), "existing scripts/bench executable")
	results := flag.String("results-dir", "bench-results", "root for benchmark history and demo windows")
	flag.Parse()
	if err := serve(*listen, *database, *bench, *results); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func defaultBench() string {
	name := "kerniva-bench"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	return filepath.Join("bin", name)
}

func loopbackAddress(address string) (string, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return "", err
	}
	if host == "localhost" {
		host = "127.0.0.1"
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return "", fmt.Errorf("address %q must use a loopback IP or localhost", address)
	}
	return net.JoinHostPort(host, port), nil
}

func serve(listen, database, benchmark, results string) error {
	listen, err := loopbackAddress(listen)
	if err != nil {
		return err
	}
	database, err = loopbackAddress(database)
	if err != nil {
		return err
	}
	benchmark, err = filepath.Abs(benchmark)
	if err != nil {
		return err
	}
	results, err = filepath.Abs(results)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(results, 0700); err != nil {
		return err
	}
	root, err := os.OpenRoot(results)
	if err != nil {
		return err
	}
	defer root.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	app := &application{database: database, benchmark: benchmark, history: root, lifetime: ctx}
	web, err := fs.Sub(assets, "web")
	if err != nil {
		return err
	}
	server := &http.Server{
		Handler: app.handler(web), ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second,
	}
	listener, err := net.Listen("tcp", listen)
	if err != nil {
		return err
	}
	serverResult := make(chan error, 1)
	go func() { serverResult <- server.Serve(listener) }()
	fmt.Printf("Kerniva demo http://%s\nDatabase %s\nHistory %s\nLoad starts only when requested in the page. Ctrl+C exits.\n", listener.Addr(), database, results)
	select {
	case <-ctx.Done():
		app.stopLoad()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		server.Shutdown(shutdown)
		cancel()
		err = <-serverResult
	case err = <-serverResult:
	}
	stop()
	app.stopLoad()
	app.work.Wait()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
