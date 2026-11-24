//go:build linux

package gologreplica

import (
	"context"
	"encoding/binary"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestSnapshotQueueLeavesDeltaWorkerAndHTTPIdleTimeoutIndependent(t *testing.T) {
	maintainer := requireNativeTools(t)
	primary := t.TempDir()
	advanceWorkerSource(t, primary, 0, "snapshot value")
	if output, err := exec.CommandContext(t.Context(), maintainer, "--data-dir", primary, "--once").CombinedOutput(); err != nil {
		t.Fatalf("compact source: %v\n%s", err, output)
	}
	advanceWorkerSource(t, primary, 1, "delta value")
	source, err := NewSource(primary)
	check(t, err)

	for _, tc := range []struct {
		name        string
		legacy      bool
		closeQueued bool
	}{
		{name: "deferred snapshot"},
		{name: "legacy closed response", legacy: true},
		{name: "close while queued", closeQueued: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			probeDone := make(chan struct{}, 1)
			fullRequest := make(chan struct{}, 1)
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				query := r.URL.Query()
				if query.Get("db") != "0" || query.Get("after") != "0" {
					source.ServeHTTP(w, r)
					return
				}
				if query.Get(deferSnapshotQuery) != "1" {
					select {
					case fullRequest <- struct{}{}:
					default:
					}
					source.ServeHTTP(w, r)
					return
				}
				if tc.legacy {
					query.Del(deferSnapshotQuery)
					r.URL.RawQuery = query.Encode()
				}
				captured := httptest.NewRecorder()
				source.ServeHTTP(captured, r)
				if tc.legacy {
					data := captured.Body.Bytes()
					if captured.Code != http.StatusOK || len(data) < 4 {
						t.Errorf("legacy probe returned HTTP %d with %d bytes", captured.Code, len(data))
						return
					}
					end := 4 + int(binary.BigEndian.Uint32(data[:4]))
					w.Header().Set("Content-Length", fmt.Sprint(len(data)))
					w.Write(data[:end])
					w.(http.Flusher).Flush()
					// A receiver holding this body while waiting for a permit
					// would keep this request open until its idle timer fired.
					<-r.Context().Done()
				} else {
					if captured.Code != http.StatusNoContent || captured.Body.Len() != 0 || captured.Header().Get(snapshotRequiredHeader) != "1" {
						t.Errorf("unexpected deferred response: HTTP %d, %v, %d bytes", captured.Code, captured.Header(), captured.Body.Len())
					}
					w.Header().Set(snapshotRequiredHeader, captured.Header().Get(snapshotRequiredHeader))
					w.WriteHeader(captured.Code)
				}
				select {
				case probeDone <- struct{}{}:
				default:
				}
			}))
			defer upstream.Close()
			root := t.TempDir()
			const idle = time.Second
			follower, err := OpenFollowerWithOptions(root, upstream.URL, []int{0, 1}, FollowerOptions{
				PullIdleTimeout: idle, SnapshotConcurrency: 1,
			})
			check(t, err)
			defer follower.Close()
			follower.snapshotSlots <- struct{}{}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			before := time.Now()
			go func() { done <- follower.Run(ctx, time.Hour) }()
			waitWorkerSignal(t, probeDone, "completed snapshot probe")
			if tc.legacy && time.Since(before) >= idle {
				t.Fatal("legacy body remained open until its idle timeout")
			}
			waitWorkerStatus(t, follower, 1, func(s Status) bool { return s.Ready && s.Sequence == 1 })
			expectGet(t, follower, 1, "key", http.StatusOK, "delta value")
			if _, err := os.Stat(filepath.Join(root, "follower-v1", dbName(0), "CURRENT")); !os.IsNotExist(err) {
				t.Fatalf("snapshot became published while queued: %v", err)
			}
			select {
			case <-fullRequest:
				t.Fatal("full snapshot request bypassed the occupied permit")
			default:
			}
			if tc.closeQueued {
				closed := make(chan struct{})
				go func() { follower.Close(); close(closed) }()
				waitWorkerSignal(t, closed, "follower close with an occupied permit")
				check(t, <-done)
				return
			}
			// Keep the queue occupied past the pull timeout. The authorized
			// request must still get its own full idle allowance.
			time.Sleep(idle + 25*time.Millisecond)
			<-follower.snapshotSlots
			waitWorkerSignal(t, fullRequest, "authorized snapshot request")
			waitWorkerStatus(t, follower, 0, func(s Status) bool { return s.Ready && s.Bootstraps == 1 })
			expectGet(t, follower, 0, "key", http.StatusOK, "snapshot value")
			if len(follower.snapshotSlots) != 0 {
				t.Fatal("published snapshot kept its permit")
			}
			cancel()
			check(t, <-done)
		})
	}
}
