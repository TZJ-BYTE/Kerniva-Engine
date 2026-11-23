package server

import (
	"errors"
	"fmt"
	"math"
	"runtime"
	"strconv"
	"strings"

	"github.com/TZJ-BYTE/Kerniva/internal/database"
	"github.com/TZJ-BYTE/Kerniva/internal/protocol"
	"github.com/TZJ-BYTE/Kerniva/internal/rustengine"
)

const (
	memorySmallReadBuffer = 256
	memoryKeepReadBuffer  = 64 << 10
	memoryReadAttempts    = 3
)

func dispatchMemoryCommand(dst []byte, ctx *ConnectionContext, manager *database.DBManager, cmd string, args [][]byte) ([]byte, bool) {
	switch cmd {
	case "PING":
		switch len(args) {
		case 0:
			return protocol.AppendSimpleString(dst, "PONG"), false
		case 1:
			return protocol.AppendBulkBytes(dst, args[0]), false
		}
		return memoryArityError(dst, cmd), false
	case "QUIT":
		if len(args) != 0 {
			return memoryArityError(dst, cmd), false
		}
		return protocol.AppendSimpleString(dst, "OK"), true
	case "SELECT":
		if len(args) != 1 {
			return memoryArityError(dst, cmd), false
		}
		index, err := protocol.ParseInt(args[0])
		if err != nil {
			return protocol.AppendErrorString(dst, "ERR invalid DB index"), false
		}
		if _, err := manager.GetMemoryDBByIndex(index); err != nil {
			return protocol.AppendErrorString(dst, "ERR DB index is out of range"), false
		}
		ctx.memoryDBIndex = index
		return protocol.AppendSimpleString(dst, "OK"), false
	}

	db, err := manager.GetMemoryDBByIndex(ctx.memoryDBIndex)
	if err != nil {
		return memoryCommandError(dst, err), false
	}
	switch cmd {
	case "GET":
		if len(args) != 1 {
			return memoryArityError(dst, cmd), false
		}
		return appendMemoryGet(dst, ctx, db, args[0]), false
	case "SET":
		if len(args) < 2 {
			return memoryArityError(dst, cmd), false
		}
		if len(args) > 2 {
			return appendMemorySetOptions(dst, db, args), false
		}
		if err := db.Put(args[0], args[1]); err != nil {
			return memoryCommandError(dst, err), false
		}
		return protocol.AppendSimpleString(dst, "OK"), false
	case "MGET":
		if len(args) == 0 {
			return memoryArityError(dst, cmd), false
		}
		return appendMemoryMGet(dst, ctx, db, args), false
	case "MSET":
		if len(args) == 0 || len(args)%2 != 0 {
			return memoryArityError(dst, cmd), false
		}
		pairs := make([]rustengine.MemoryPair, len(args)/2)
		for i := range pairs {
			pairs[i] = rustengine.MemoryPair{Key: args[i*2], Value: args[i*2+1]}
		}
		if err := db.MSet(pairs); err != nil {
			return memoryCommandError(dst, err), false
		}
		return protocol.AppendSimpleString(dst, "OK"), false
	case "DEL", "EXISTS":
		if len(args) == 0 {
			return memoryArityError(dst, cmd), false
		}
		var count int64
		var err error
		if cmd == "DEL" {
			if len(args) == 1 {
				var removed bool
				removed, err = db.Delete(args[0])
				if removed {
					count = 1
				}
			} else {
				count, err = db.DeleteMany(args)
			}
		} else {
			count, err = db.CountExisting(args)
		}
		if err != nil {
			return memoryCommandError(dst, err), false
		}
		return protocol.AppendInteger(dst, count), false
	case "TTL", "PTTL":
		if len(args) != 1 {
			return memoryArityError(dst, cmd), false
		}
		ttl, err := db.PTTL(args[0])
		if err != nil {
			return memoryCommandError(dst, err), false
		}
		if cmd == "TTL" && ttl >= 0 {
			// Redis 8.0.5 expire.c rounds relative TTL to the nearest second.
			// Split the arithmetic so a large millisecond value cannot overflow.
			seconds := ttl / 1000
			if ttl%1000 >= 500 {
				seconds++
			}
			ttl = seconds
		}
		return protocol.AppendInteger(dst, ttl), false
	case "EXPIRE", "PEXPIRE":
		if len(args) < 2 {
			return memoryArityError(dst, cmd), false
		}
		if len(args) > 2 {
			return protocol.AppendErrorString(dst, "ERR EXPIRE options are unavailable with the rust-memory backend"), false
		}
		ttl, err := memoryParseInteger(args[1])
		if err != nil {
			return memoryIntegerArgumentError(dst), false
		}
		if cmd == "EXPIRE" {
			if ttl > math.MaxInt64/1000 || ttl < math.MinInt64/1000 {
				return protocol.AppendErrorString(dst, "ERR invalid expire time in 'expire' command"), false
			}
			ttl *= 1000
		}
		changed, err := db.Expire(args[0], ttl)
		if err != nil {
			return memoryCommandError(dst, err), false
		}
		if changed {
			return protocol.AppendInteger(dst, 1), false
		}
		return protocol.AppendInteger(dst, 0), false
	case "INCR", "INCRBY", "DECR":
		expected := 1
		if cmd == "INCRBY" {
			expected = 2
		}
		if len(args) != expected {
			return memoryArityError(dst, cmd), false
		}
		by := int64(1)
		if cmd == "DECR" {
			by = -1
		} else if cmd == "INCRBY" {
			by, err = memoryParseInteger(args[1])
			if err != nil {
				return memoryIntegerArgumentError(dst), false
			}
		}
		value, err := db.Increment(args[0], by)
		if err != nil {
			return memoryCommandError(dst, err), false
		}
		return protocol.AppendInteger(dst, value), false
	case "INFO":
		if len(args) > 1 {
			return memoryArityError(dst, cmd), false
		}
		stats, err := db.Stats()
		if err != nil {
			return memoryCommandError(dst, err), false
		}
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		info := fmt.Sprintf("# Database\r\nstorage_backend:rust-memory\r\nmode:memory\r\npersistence_enabled:false\r\nmemory_abi_version:1\r\ndb_id:%d\r\ndb_count:%d\r\nmemory_keys:%d\r\nused_memory_bytes:%d\r\nmemory_shards:%d\r\n# Runtime\r\nheap_alloc_bytes:%d\r\nnum_gc:%d\r\ngc_pause_total_ns:%d\r\n",
			ctx.memoryDBIndex, manager.DBCount(), stats.Keys, stats.PayloadBytes, stats.Shards, mem.HeapAlloc, mem.NumGC, mem.PauseTotalNs)
		return protocol.AppendBulkString(dst, info), false
	default:
		return protocol.AppendErrorString(dst, "ERR command is unavailable with the rust-memory backend"), false
	}
}

func appendMemorySetOptions(dst []byte, db *rustengine.MemoryEngine, args [][]byte) []byte {
	condition := rustengine.MemorySetAny
	var ttl uint64
	hasTTL := false
	for index := 2; index < len(args); index++ {
		option := strings.ToUpper(string(args[index]))
		switch option {
		case "NX":
			if condition == rustengine.MemorySetXX {
				return protocol.AppendErrorString(dst, "ERR syntax error")
			}
			condition = rustengine.MemorySetNX
		case "XX":
			if condition == rustengine.MemorySetNX {
				return protocol.AppendErrorString(dst, "ERR syntax error")
			}
			condition = rustengine.MemorySetXX
		case "EX", "PX":
			if hasTTL || index+1 >= len(args) {
				return protocol.AppendErrorString(dst, "ERR syntax error")
			}
			milliseconds, err := memoryParseInteger(args[index+1])
			if err != nil {
				return memoryIntegerArgumentError(dst)
			}
			if milliseconds <= 0 {
				return protocol.AppendErrorString(dst, "ERR invalid expire time in 'set' command")
			}
			if option == "EX" {
				if milliseconds > math.MaxInt64/1000 {
					return protocol.AppendErrorString(dst, "ERR invalid expire time in 'set' command")
				}
				milliseconds *= 1000
			}
			ttl = uint64(milliseconds)
			hasTTL = true
			index++
		default:
			return protocol.AppendErrorString(dst, "ERR SET option is unavailable with the rust-memory backend")
		}
	}
	changed, err := db.PutOptions(args[0], args[1], condition, ttl)
	if err != nil {
		return memoryCommandError(dst, err)
	}
	if !changed {
		return protocol.AppendNull(dst)
	}
	return protocol.AppendSimpleString(dst, "OK")
}

// Redis integer arguments use the canonical signed decimal representation.
// strconv.ParseInt alone also accepts plus signs and redundant leading zeroes.
func memoryParseInteger(value []byte) (int64, error) {
	if len(value) == 0 || value[0] == '+' ||
		(value[0] == '0' && len(value) > 1) ||
		(value[0] == '-' && (len(value) == 1 || value[1] == '0')) {
		return 0, strconv.ErrSyntax
	}
	return strconv.ParseInt(string(value), 10, 64)
}

func memoryIntegerArgumentError(dst []byte) []byte {
	return protocol.AppendErrorString(dst, "ERR value is not an integer or out of range")
}

func (ctx *ConnectionContext) memoryBuffer(size int) []byte {
	if len(ctx.memoryReadBuf) >= size {
		return ctx.memoryReadBuf
	}
	buf := make([]byte, size)
	if size <= memoryKeepReadBuffer {
		ctx.memoryReadBuf = buf
	}
	return buf
}

func appendMemoryGet(dst []byte, ctx *ConnectionContext, db *rustengine.MemoryEngine, key []byte) []byte {
	buf := ctx.memoryBuffer(memorySmallReadBuffer)
	for attempt := 0; attempt < memoryReadAttempts; attempt++ {
		needed, found, err := db.GetInto(key, buf)
		if errors.Is(err, rustengine.ErrMemoryBufferTooSmall) {
			if needed > maxConnectionBufferedBytes {
				return protocol.AppendErrorString(dst, "ERR rust-memory response exceeds the connection buffer limit")
			}
			buf = ctx.memoryBuffer(needed)
			continue
		}
		if err != nil {
			return memoryCommandError(dst, err)
		}
		if !found {
			return protocol.AppendNull(dst)
		}
		return protocol.AppendBulkBytes(dst, buf[:needed])
	}
	return protocol.AppendErrorString(dst, "ERR rust-memory value changed size during GET")
}

func appendMemoryMGet(dst []byte, ctx *ConnectionContext, db *rustengine.MemoryEngine, keys [][]byte) []byte {
	initial := min(memoryKeepReadBuffer, max(memorySmallReadBuffer, 32*len(keys)))
	buf := ctx.memoryBuffer(initial)
	spans := make([]rustengine.MemorySpan, len(keys))
	for attempt := 0; attempt < memoryReadAttempts; attempt++ {
		needed, err := db.GetManyInto(keys, buf, spans)
		if errors.Is(err, rustengine.ErrMemoryBufferTooSmall) {
			if needed > maxConnectionBufferedBytes {
				return protocol.AppendErrorString(dst, "ERR rust-memory response exceeds the connection buffer limit")
			}
			buf = ctx.memoryBuffer(needed)
			continue
		}
		if err != nil {
			return memoryCommandError(dst, err)
		}
		dst = append(dst, protocol.Array)
		dst = strconv.AppendInt(dst, int64(len(spans)), 10)
		dst = append(dst, '\r', '\n')
		for _, span := range spans {
			if span.Found == 0 {
				dst = protocol.AppendNull(dst)
			} else {
				dst = protocol.AppendBulkBytes(dst, buf[span.Offset:span.Offset+span.Len])
			}
		}
		return dst
	}
	return protocol.AppendErrorString(dst, "ERR rust-memory values changed size during MGET")
}

func memoryArityError(dst []byte, cmd string) []byte {
	return protocol.AppendErrorString(dst, "ERR wrong number of arguments for '"+strings.ToLower(cmd)+"' command")
}

func memoryCommandError(dst []byte, err error) []byte {
	if errors.Is(err, rustengine.ErrMemoryExpiration) {
		return protocol.AppendErrorString(dst, "ERR invalid expire time")
	}
	if errors.Is(err, rustengine.ErrMemoryInteger) {
		return memoryIntegerArgumentError(dst)
	}
	if errors.Is(err, rustengine.ErrMemoryPayloadLimit) {
		return protocol.AppendErrorString(dst, "OOM command not allowed when used memory > 'maxmemory'.")
	}
	return protocol.AppendErrorString(dst, "ERR "+err.Error())
}
