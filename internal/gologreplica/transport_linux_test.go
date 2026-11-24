//go:build linux

package gologreplica

import (
	"context"
	"errors"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

func TestPullProgressUsesIdleTime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const timeout = time.Second
		ctx, progress := watchPullProgress(t.Context(), timeout)
		defer progress.close()
		reader := pullReader{ctx: ctx, body: strings.NewReader("abc"), progress: progress}
		var chunk [1]byte
		for _, want := range []byte("abc") {
			time.Sleep(3 * timeout / 4)
			if err := ctx.Err(); err != nil {
				t.Fatalf("idle deadline interrupted a progressing pull: %v", err)
			}
			if n, err := reader.Read(chunk[:]); err != nil || n != 1 || chunk[0] != want {
				t.Fatalf("Read = %d, %v, %q, want byte %q", n, err, chunk[0], want)
			}
		}
		time.Sleep(3 * timeout / 4)
		if err := ctx.Err(); err != nil {
			t.Fatalf("idle deadline after the last byte was shortened: %v", err)
		}
		time.Sleep(timeout / 4)
		<-ctx.Done()
		if err := context.Cause(ctx); !errors.Is(err, errPullIdle) {
			t.Fatalf("expired pull cause = %v, want %v", err, errPullIdle)
		}
	})
}

func TestPullProgressTimesOutBeforeHeaders(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, progress := watchPullProgress(t.Context(), time.Second)
		defer progress.close()
		<-ctx.Done()
		if err := context.Cause(ctx); !errors.Is(err, errPullIdle) {
			t.Fatalf("no-response pull cause = %v, want %v", err, errPullIdle)
		}
	})
}
