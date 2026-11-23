package server

import (
	"errors"
	"fmt"
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
			return protocol.AppendErrorString(dst, "ERR SET options are unavailable with the rust-memory backend"), false
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
	case "DEL":
		if len(args) == 0 {
			return memoryArityError(dst, cmd), false
		}
		if len(args) > 1 {
			return protocol.AppendErrorString(dst, "ERR rust-memory DEL currently accepts one key"), false
		}
		removed, err := db.Delete(args[0])
		if err != nil {
			return memoryCommandError(dst, err), false
		}
		if removed {
			return protocol.AppendInteger(dst, 1), false
		}
		return protocol.AppendInteger(dst, 0), false
	case "INFO":
		if len(args) > 1 {
			return memoryArityError(dst, cmd), false
		}
		stats, err := db.Stats()
		if err != nil {
			return memoryCommandError(dst, err), false
		}
		info := fmt.Sprintf("# Database\r\nstorage_backend:rust-memory\r\nmode:memory\r\npersistence_enabled:false\r\nmemory_abi_version:1\r\ndb_id:%d\r\ndb_count:%d\r\nmemory_keys:%d\r\nused_memory_bytes:%d\r\nmemory_shards:%d\r\n", ctx.memoryDBIndex, manager.DBCount(), stats.Keys, stats.PayloadBytes, stats.Shards)
		return protocol.AppendBulkString(dst, info), false
	default:
		return protocol.AppendErrorString(dst, "ERR command '"+strings.ToLower(cmd)+"' is unavailable with the rust-memory backend"), false
	}
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
	if errors.Is(err, rustengine.ErrMemoryPayloadLimit) {
		return protocol.AppendErrorString(dst, "OOM command not allowed when used memory > 'maxmemory'.")
	}
	return protocol.AppendErrorString(dst, "ERR "+err.Error())
}
