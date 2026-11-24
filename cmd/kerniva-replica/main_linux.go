//go:build linux

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/TZJ-BYTE/Kerniva/internal/gologreplica"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, os.Args[1:]); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, args []string) error {
	if len(args) == 0 || (args[0] != "serve" && args[0] != "follow" && args[0] != "rotate-source-id") {
		return errors.New("usage: kerniva-replica {serve|follow|rotate-source-id} --help")
	}
	mode := args[0]
	flags := flag.NewFlagSet(mode, flag.ContinueOnError)
	root := flags.String("root", "", "go-log-v1 directory for serve or rotation, independent data directory for follow")
	var listen, upstream, databases string
	if mode != "rotate-source-id" {
		flags.StringVar(&listen, "listen", "127.0.0.1:18379", "loopback HTTP listening address")
	}
	var interval, pullIdle time.Duration
	var snapshotConcurrency int
	if mode == "follow" {
		flags.StringVar(&upstream, "source", "", "source HTTP URL")
		flags.StringVar(&databases, "dbs", "0", "comma-separated database numbers")
		flags.DurationVar(&interval, "interval", time.Second, "delay between pulls for each database")
		flags.DurationVar(&pullIdle, "pull-idle-timeout", gologreplica.DefaultTransferIdleTimeout, "maximum time with no response or new bytes during a pull")
		flags.IntVar(&snapshotConcurrency, "snapshot-concurrency", 0, "maximum concurrent snapshot pulls across databases (0 is unlimited)")
	}
	if err := flags.Parse(args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if *root == "" || flags.NArg() != 0 {
		return errors.New("--root is required")
	}
	if mode == "rotate-source-id" {
		id, err := gologreplica.RotateSourceID(*root)
		if err != nil {
			return err
		}
		fmt.Println(id)
		return nil
	}
	host, _, err := net.SplitHostPort(listen)
	if err != nil || !net.ParseIP(host).IsLoopback() {
		return errors.New("--listen must be a numeric loopback address and port")
	}
	if mode == "serve" {
		handler, lease, err := gologreplica.OpenLeasedSource(*root)
		if err != nil {
			return err
		}
		defer lease.Close()
		return serve(ctx, listen, handler)
	}
	if interval <= 0 {
		return errors.New("--interval must be positive")
	}
	var numbers []int
	for _, db := range strings.Split(databases, ",") {
		number, err := strconv.Atoi(db)
		if err != nil || number < 0 || strconv.Itoa(number) != db {
			return fmt.Errorf("invalid database number %q", db)
		}
		numbers = append(numbers, number)
	}
	follower, err := gologreplica.OpenFollowerWithOptions(*root, upstream, numbers, gologreplica.FollowerOptions{
		PullIdleTimeout: pullIdle, SnapshotConcurrency: snapshotConcurrency,
	})
	if err != nil {
		return err
	}
	defer follower.Close()
	pollCtx, stopPolling := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- follower.Run(pollCtx, interval)
	}()
	err = serve(ctx, listen, follower.Handler())
	stopPolling()
	return errors.Join(err, <-done)
}

func serve(ctx context.Context, addr string, handler http.Handler) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	server := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       30 * time.Second,
	}
	programDone, shutdownDone := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(shutdownDone)
		select {
		case <-ctx.Done():
			shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := server.Shutdown(shutdown); err != nil {
				server.Close()
			}
		case <-programDone:
		}
	}()
	log.Printf("listening on %s", listener.Addr())
	err = server.Serve(listener)
	close(programDone)
	<-shutdownDone
	if !errors.Is(err, http.ErrServerClosed) {
		server.Close()
		return err
	}
	return nil
}
