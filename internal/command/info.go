package command

import (
	"fmt"
	"runtime"
	"sort"
	"strings"

	"github.com/TZJ-BYTE/Kerniva/internal/database"
	"github.com/TZJ-BYTE/Kerniva/internal/protocol"
)

type InfoCommand struct{}

func (c *InfoCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	stats := db.GetStats()
	mgr := getDBManager()
	var mgrStats map[string]interface{}
	if mgr != nil {
		mgrStats = mgr.GetStats()
	}
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	var b strings.Builder
	writeSection(&b, "server")
	writeKV(&b, "go_version", runtime.Version())
	writeKV(&b, "go_os_arch", runtime.GOOS+"/"+runtime.GOARCH)
	writeKV(&b, "cpu_num", fmt.Sprintf("%d", runtime.NumCPU()))

	writeSection(&b, "database")
	writeKV(&b, "db_id", fmt.Sprintf("%v", stats["db_id"]))
	writeKV(&b, "mode", fmt.Sprintf("%v", stats["mode"]))
	writeKV(&b, "memory_keys", fmt.Sprintf("%v", stats["memory_keys"]))
	writeKV(&b, "used_memory_bytes", fmt.Sprintf("%v", stats["used_memory_bytes"]))
	writeKV(&b, "max_memory_bytes", fmt.Sprintf("%v", stats["max_memory_bytes"]))
	writeKV(&b, "max_memory_policy", fmt.Sprintf("%v", stats["max_memory_policy"]))

	if mgrStats != nil {
		writeSection(&b, "db_manager")
		writeKV(&b, "db_count", fmt.Sprintf("%v", mgrStats["db_count"]))
		writeKV(&b, "persistence_enabled", fmt.Sprintf("%v", mgrStats["persistence_enabled"]))
		if v, ok := mgrStats["data_dir"]; ok {
			writeKV(&b, "data_dir", fmt.Sprintf("%v", v))
		}
		if dbs, ok := mgrStats["databases"].([]map[string]interface{}); ok {
			writeSection(&b, "databases")
			for _, s := range dbs {
				id, _ := s["db_id"]
				prefix := fmt.Sprintf("db_%v_", id)
				writeKV(&b, prefix+"mode", fmt.Sprintf("%v", s["mode"]))
				writeKV(&b, prefix+"memory_keys", fmt.Sprintf("%v", s["memory_keys"]))
				writeKV(&b, prefix+"used_memory_bytes", fmt.Sprintf("%v", s["used_memory_bytes"]))
			}
		}
	}

	writeSection(&b, "runtime")
	writeKV(&b, "heap_alloc_bytes", fmt.Sprintf("%d", ms.HeapAlloc))
	writeKV(&b, "heap_inuse_bytes", fmt.Sprintf("%d", ms.HeapInuse))
	writeKV(&b, "heap_objects", fmt.Sprintf("%d", ms.HeapObjects))
	writeKV(&b, "num_gc", fmt.Sprintf("%d", ms.NumGC))
	writeKV(&b, "gc_pause_total_ns", fmt.Sprintf("%d", ms.PauseTotalNs))
	if ms.NumGC > 0 {
		last := ms.PauseNs[(ms.NumGC-1)%uint32(len(ms.PauseNs))]
		writeKV(&b, "gc_last_pause_ns", fmt.Sprintf("%d", last))
	}

	if enabled, ok := stats["persistence_enabled"].(bool); ok && enabled {
		writeSection(&b, "persistence")
		persistence, _ := stats["persistence"].(map[string]interface{})
		keys := sortedKeys(persistence)
		for _, k := range keys {
			writeKV(&b, k, fmt.Sprintf("%v", persistence[k]))
		}
	}

	return protocol.MakeBulkString(b.String())
}

func writeSection(b *strings.Builder, name string) {
	b.WriteString("# ")
	b.WriteString(name)
	b.WriteString("\n")
}

func writeKV(b *strings.Builder, k string, v string) {
	b.WriteString(k)
	b.WriteString(":")
	b.WriteString(v)
	b.WriteString("\n")
}

func sortedKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
