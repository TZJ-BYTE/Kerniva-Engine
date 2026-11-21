// Run from the repository with go run ./kerniva_engine/examples/go [data-dir].
// Build the native library first with the platform's build script.
package main

import (
	"fmt"
	"os"

	"github.com/TZJ-BYTE/Kerniva/internal/rustengine"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() (err error) {
	dataDir := "./demo_data"
	if len(os.Args) > 1 {
		dataDir = os.Args[1]
	}
	engine, err := rustengine.Open(rustengine.Options{
		DataDir:            dataDir,
		SegmentSizeBytes:   4 << 20,
		CheckpointAfterOps: 1024,
		SyncPolicy:         rustengine.SyncData,
	})
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := engine.Close(); err == nil {
			err = closeErr
		}
	}()
	if err := engine.WriteBatch([]rustengine.WriteOp{
		{Key: []byte("hello"), Value: []byte("world")},
		{Key: []byte("foo"), Value: []byte("bar")},
	}); err != nil {
		return err
	}
	value, found, err := engine.Get([]byte("hello"))
	if err != nil {
		return err
	}
	if !found || string(value) != "world" {
		return fmt.Errorf("unexpected value for hello: %q, found=%t", value, found)
	}
	fmt.Printf("hello = %s\n", value)
	if err := engine.Delete([]byte("foo")); err != nil {
		return err
	}
	if _, err := engine.MaybeCompact(); err != nil {
		return err
	}
	if err := engine.Checkpoint(); err != nil {
		return err
	}
	stats, err := engine.Stats()
	if err != nil {
		return err
	}
	fmt.Printf("keys=%d segments=%d\n", stats.Keys, stats.SegmentCount)
	return nil
}
