//go:build linux

package gologreplica

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type defaultPullRoundTripper struct{}

func (defaultPullRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errCustomDefaultPull
}

var errCustomDefaultPull = errors.New("custom default pull transport")

func TestFollowerClonesPullTransportDefaults(t *testing.T) {
	original := http.DefaultTransport
	t.Cleanup(func() { http.DefaultTransport = original })
	for _, test := range []struct {
		name       string
		dbs        []int
		configured int
		want       int
	}{
		{"one database", []int{0}, 0, http.DefaultMaxIdleConnsPerHost},
		{"many databases", []int{0, 1, 2, 3}, 0, 4},
		{"explicit positive cap", []int{0, 1, 2, 3}, 1, 1},
		{"explicit negative cap", []int{0, 1, 2, 3}, -1, -1},
	} {
		t.Run(test.name, func(t *testing.T) {
			defaults := &http.Transport{
				MaxIdleConnsPerHost: test.configured,
				MaxIdleConns:        27,
				MaxConnsPerHost:     9,
				IdleConnTimeout:     13 * time.Second,
				DisableKeepAlives:   true,
			}
			http.DefaultTransport = defaults
			follower, err := OpenFollower(t.TempDir(), "http://127.0.0.1", test.dbs)
			check(t, err)
			defer follower.Close()
			pool, ok := follower.client.Transport.(*http.Transport)
			if !ok || pool == defaults {
				t.Fatalf("follower transport = %T %p, want its own *http.Transport", follower.client.Transport,
					follower.client.Transport)
			}
			if pool.MaxIdleConnsPerHost != test.want || defaults.MaxIdleConnsPerHost != test.configured {
				t.Fatalf("idle caps = (%d, %d), want (%d, %d)", pool.MaxIdleConnsPerHost,
					defaults.MaxIdleConnsPerHost, test.want, test.configured)
			}
			if pool.MaxIdleConns != defaults.MaxIdleConns || pool.MaxConnsPerHost != defaults.MaxConnsPerHost ||
				pool.IdleConnTimeout != defaults.IdleConnTimeout || !pool.DisableKeepAlives {
				t.Fatalf("follower transport changed configured limits: %+v", pool)
			}
		})
	}

	t.Run("custom default round tripper", func(t *testing.T) {
		http.DefaultTransport = defaultPullRoundTripper{}
		follower, err := OpenFollower(t.TempDir(), "http://127.0.0.1", []int{0})
		check(t, err)
		defer follower.Close()
		if follower.client.Transport != nil {
			t.Fatalf("fallback client transport = %T, want the default", follower.client.Transport)
		}
		if err := follower.Poll(t.Context()); !errors.Is(err, errCustomDefaultPull) {
			t.Fatalf("poll error = %v, want the custom default error", err)
		}
	})
}

func TestFollowersReuseIndependentPullPools(t *testing.T) {
	type pullWave struct {
		arrived chan string
		release chan struct{}
	}
	var wave atomic.Pointer[pullWave]
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		current := wave.Load()
		select {
		case current.arrived <- r.RemoteAddr:
		case <-r.Context().Done():
			return
		}
		select {
		case <-current.release:
		case <-r.Context().Done():
			return
		}
		http.Error(w, "test pull", http.StatusConflict)
	}))
	defer server.Close()
	first, err := OpenFollower(t.TempDir(), server.URL, []int{0, 1, 2, 3})
	check(t, err)
	defer first.Close()
	second, err := OpenFollower(t.TempDir(), server.URL, []int{0})
	check(t, err)
	defer second.Close()

	pull := func(follower *Follower) []string {
		t.Helper()
		workers := len(follower.databases)
		current := &pullWave{arrived: make(chan string, workers), release: make(chan struct{})}
		wave.Store(current)
		release := sync.OnceFunc(func() { close(current.release) })
		defer release()
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		results := make(chan error, workers)
		for _, db := range follower.databases {
			go func() { results <- follower.pollDatabase(ctx, db) }()
		}
		peers := make([]string, 0, workers)
		for range workers {
			select {
			case peer := <-current.arrived:
				peers = append(peers, peer)
			case <-ctx.Done():
				t.Fatalf("only %d of %d concurrent pulls reached source", len(peers), workers)
			}
		}
		release()
		for range workers {
			select {
			case err := <-results:
				if err == nil || err.Error() != "source HTTP 409: test pull" {
					t.Fatalf("poll returned %v, want the source response", err)
				}
			case <-ctx.Done():
				t.Fatal("a pull did not consume the source response")
			}
		}
		slices.Sort(peers)
		return peers
	}

	firstPeers := pull(first)
	secondPeers := pull(second)
	if slices.Contains(firstPeers, secondPeers[0]) {
		t.Fatalf("separate followers shared connection %s", secondPeers[0])
	}
	if reused := pull(first); !slices.Equal(reused, firstPeers) {
		t.Fatalf("concurrent DB pulls used %v, want existing connections %v", reused, firstPeers)
	}
	check(t, first.Close())
	if reused := pull(second); !slices.Equal(reused, secondPeers) {
		t.Fatalf("closing another follower discarded the connection %v, now using %v", secondPeers, reused)
	}
}
