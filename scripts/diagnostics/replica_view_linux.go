//go:build linux

// Run on a private copy of a published follower directory. No polling is started.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http/httptest"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/TZJ-BYTE/Kerniva/internal/gologreplica"
)

func sample(stage string) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	rss := map[string]uint64{}
	data, err := os.ReadFile("/proc/self/smaps_rollup")
	if err != nil {
		log.Fatal(err)
	}
	for _, line := range strings.Split(string(data), "\n") {
		parts := strings.Fields(line)
		if len(parts) != 3 {
			continue
		}
		switch parts[0] {
		case "Rss:", "Anonymous:", "Pss_File:", "Swap:":
			n, err := strconv.ParseUint(parts[1], 10, 64)
			if err != nil {
				log.Fatal(err)
			}
			rss[strings.TrimSuffix(parts[0], ":")] = n * 1024
		}
	}
	err = json.NewEncoder(os.Stdout).Encode(map[string]any{
		"stage": stage, "time": time.Now().UTC(), "rollup_bytes": rss,
		"heap_alloc_bytes": m.HeapAlloc, "heap_inuse_bytes": m.HeapInuse,
		"heap_sys_bytes": m.HeapSys, "heap_released_bytes": m.HeapReleased,
		"total_alloc_bytes": m.TotalAlloc, "num_gc": m.NumGC,
	})
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	if len(os.Args) != 2 {
		log.Fatal("expected a private copy of a complete follower root")
	}
	dbs := make([]int, 16)
	for i := range dbs {
		dbs[i] = i
	}
	sample("before_open")
	f, err := gologreplica.OpenFollower(os.Args[1], "http://127.0.0.1:1", dbs)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	sample("view_opened")
	runtime.GC()
	sample("after_forced_gc")
	debug.FreeOSMemory()
	sample("after_returning_go_pages")
	handler := f.Handler()
	for db := range dbs {
		key, want := "anchor", "new"
		if db == 15 {
			key, want = "capacity:marker", "fixed-link-snapshot-capacity-marker-4"
		}
		target := fmt.Sprintf("/get?db=%d&key=%s", db, key)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest("GET", target, nil))
		if response.Code != 200 || !bytes.Equal(response.Body.Bytes(), []byte(want)) {
			log.Fatalf("DB %d read failed: HTTP %d", db, response.Code)
		}
	}
	runtime.KeepAlive(f)
}
