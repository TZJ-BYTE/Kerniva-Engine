package command

import (
	"errors"
	"sort"
	"strconv"
	"strings"
	"time"

	"path"

	"github.com/TZJ-BYTE/Kerniva/internal/database"
	"github.com/TZJ-BYTE/Kerniva/internal/protocol"
)

// SetCommand SET 命令
type SetCommand struct{}

func (c *SetCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) < 2 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'set' command"))
	}

	nx := false
	xx := false
	get := false
	var ttlMs int64
	var expireAtMs int64
	keepTTL := false

	for i := 2; i < len(args); i++ {
		opt := args[i]
		if len(opt) == 0 {
			return protocol.MakeError(errors.New("ERR syntax error"))
		}
		switch {
		case equalsIgnoreCase(opt, []byte("NX")):
			if xx {
				return protocol.MakeError(errors.New("ERR syntax error"))
			}
			nx = true
		case equalsIgnoreCase(opt, []byte("XX")):
			if nx {
				return protocol.MakeError(errors.New("ERR syntax error"))
			}
			xx = true
		case equalsIgnoreCase(opt, []byte("KEEPTTL")):
			if ttlMs != 0 || expireAtMs != 0 {
				return protocol.MakeError(errors.New("ERR syntax error"))
			}
			keepTTL = true
		case equalsIgnoreCase(opt, []byte("GET")):
			get = true
		case equalsIgnoreCase(opt, []byte("EX")):
			if ttlMs != 0 || expireAtMs != 0 || keepTTL {
				return protocol.MakeError(errors.New("ERR syntax error"))
			}
			if i+1 >= len(args) {
				return protocol.MakeError(errors.New("ERR syntax error"))
			}
			n, err := protocol.ParseInt(args[i+1])
			if err != nil || n <= 0 {
				return protocol.MakeError(errors.New("ERR value is not an integer or out of range"))
			}
			ttlMs = int64(n) * 1000
			i++
		case equalsIgnoreCase(opt, []byte("PX")):
			if ttlMs != 0 || expireAtMs != 0 || keepTTL {
				return protocol.MakeError(errors.New("ERR syntax error"))
			}
			if i+1 >= len(args) {
				return protocol.MakeError(errors.New("ERR syntax error"))
			}
			n, err := protocol.ParseInt(args[i+1])
			if err != nil || n <= 0 {
				return protocol.MakeError(errors.New("ERR value is not an integer or out of range"))
			}
			ttlMs = int64(n)
			i++
		case equalsIgnoreCase(opt, []byte("EXAT")):
			if ttlMs != 0 || expireAtMs != 0 || keepTTL {
				return protocol.MakeError(errors.New("ERR syntax error"))
			}
			if i+1 >= len(args) {
				return protocol.MakeError(errors.New("ERR syntax error"))
			}
			n, err := protocol.ParseInt(args[i+1])
			if err != nil || n <= 0 {
				return protocol.MakeError(errors.New("ERR value is not an integer or out of range"))
			}
			expireAtMs = int64(n) * 1000
			i++
		case equalsIgnoreCase(opt, []byte("PXAT")):
			if ttlMs != 0 || expireAtMs != 0 || keepTTL {
				return protocol.MakeError(errors.New("ERR syntax error"))
			}
			if i+1 >= len(args) {
				return protocol.MakeError(errors.New("ERR syntax error"))
			}
			n, err := protocol.ParseInt(args[i+1])
			if err != nil || n <= 0 {
				return protocol.MakeError(errors.New("ERR value is not an integer or out of range"))
			}
			expireAtMs = int64(n)
			i++
		default:
			return protocol.MakeError(errors.New("ERR syntax error"))
		}
	}

	oldValue := ""
	hadOldValue := false
	if get {
		var err error
		oldValue, hadOldValue, err = db.GetStringCopy(args[0])
		if err != nil {
			return protocol.MakeError(err)
		}
	}

	set, err := db.SetStringBytesWithExpiryOptions(args[0], args[1], ttlMs, expireAtMs, keepTTL, nx, xx)
	if err != nil {
		return protocol.MakeError(err)
	}
	if get {
		if hadOldValue {
			return protocol.MakeBulkString(oldValue)
		}
		return protocol.MakeNull()
	}
	if !set {
		return protocol.MakeNull()
	}
	return protocol.MakeSimpleString("OK")
}

// GetCommand GET 命令
type GetCommand struct{}

func (c *GetCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 1 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'get' command"))
	}

	s, ok, err := db.GetStringCopy(args[0])
	if err != nil {
		return protocol.MakeError(err)
	}
	if !ok {
		return protocol.MakeNull()
	}
	return protocol.MakeBulkString(s)
}

// GetDelCommand GETDEL 命令
type GetDelCommand struct{}

func (c *GetDelCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 1 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'getdel' command"))
	}

	s, ok, err := db.GetStringCopy(args[0])
	if err != nil {
		return protocol.MakeError(err)
	}
	if !ok {
		return protocol.MakeNull()
	}
	if _, err := db.DeleteWithError(argString(args, 0)); err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeBulkString(s)
}

// GetExCommand GETEX 命令
type GetExCommand struct{}

func (c *GetExCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) < 1 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'getex' command"))
	}
	value, ok, err := db.GetStringCopy(args[0])
	if err != nil {
		return protocol.MakeError(err)
	}
	if !ok {
		return protocol.MakeNull()
	}

	if len(args) > 1 {
		if len(args) != 3 && len(args) != 2 {
			return protocol.MakeError(errors.New("ERR syntax error"))
		}
		switch {
		case len(args) == 2 && equalsIgnoreCase(args[1], []byte("PERSIST")):
			_, err := db.Persist(argString(args, 0))
			if err != nil {
				return protocol.MakeError(err)
			}
		case len(args) == 3 && equalsIgnoreCase(args[1], []byte("EX")):
			n, err := protocol.ParseInt(args[2])
			if err != nil || n <= 0 {
				return protocol.MakeError(errors.New("ERR value is not an integer or out of range"))
			}
			db.Expire(argString(args, 0), int64(n)*1000)
		case len(args) == 3 && equalsIgnoreCase(args[1], []byte("PX")):
			n, err := protocol.ParseInt(args[2])
			if err != nil || n <= 0 {
				return protocol.MakeError(errors.New("ERR value is not an integer or out of range"))
			}
			db.Expire(argString(args, 0), int64(n))
		case len(args) == 3 && equalsIgnoreCase(args[1], []byte("EXAT")):
			n, err := protocol.ParseInt(args[2])
			if err != nil || n <= 0 {
				return protocol.MakeError(errors.New("ERR value is not an integer or out of range"))
			}
			_, err = db.PExpireAt(argString(args, 0), int64(n)*1000)
			if err != nil {
				return protocol.MakeError(err)
			}
		case len(args) == 3 && equalsIgnoreCase(args[1], []byte("PXAT")):
			n, err := protocol.ParseInt(args[2])
			if err != nil || n <= 0 {
				return protocol.MakeError(errors.New("ERR value is not an integer or out of range"))
			}
			_, err = db.PExpireAt(argString(args, 0), int64(n))
			if err != nil {
				return protocol.MakeError(err)
			}
		default:
			return protocol.MakeError(errors.New("ERR syntax error"))
		}
	}

	return protocol.MakeBulkString(value)
}

// DelCommand DEL 命令
type DelCommand struct{}

func (c *DelCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) == 0 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'del' command"))
	}

	keys := make([]string, len(args))
	for i := range args {
		keys[i] = argString(args, i)
	}
	count, err := db.DeleteManyWithError(keys)
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeInteger(count)
}

// ExistsCommand EXISTS 命令
type ExistsCommand struct{}

func (c *ExistsCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) == 0 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'exists' command"))
	}

	count := 0
	for i := range args {
		if db.Exists(argString(args, i)) {
			count++
		}
	}

	return protocol.MakeInteger(int64(count))
}

// ExpireCommand EXPIRE 命令
type ExpireCommand struct{}

func (c *ExpireCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) < 2 || len(args) > 3 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'expire' command"))
	}

	key := argString(args, 0)
	ttl, err := protocol.ParseInt(args[1])
	if err != nil {
		return protocol.MakeError(errors.New("ERR value is not an integer or out of range"))
	}

	option, err := parseExpireOption(args[2:])
	if err != nil {
		return protocol.MakeError(err)
	}
	success, err := db.ExpireWithOption(key, int64(ttl)*1000, option)
	if err != nil {
		return protocol.MakeError(err)
	}
	if success {
		return protocol.MakeInteger(1)
	}
	return protocol.MakeInteger(0)
}

// ExpireTimeCommand EXPIRETIME 命令
type ExpireTimeCommand struct{}

func (c *ExpireTimeCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 1 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'expiretime' command"))
	}

	expireAt, exists := db.GetExpireTime(argString(args, 0))
	if !exists {
		return protocol.MakeInteger(-2)
	}
	if expireAt == 0 {
		return protocol.MakeInteger(-1)
	}
	return protocol.MakeInteger(expireAt / 1000)
}

// PExpireCommand PEXPIRE 命令
type PExpireCommand struct{}

func (c *PExpireCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) < 2 || len(args) > 3 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'pexpire' command"))
	}

	ttl, err := protocol.ParseInt(args[1])
	if err != nil {
		return protocol.MakeError(errors.New("ERR value is not an integer or out of range"))
	}

	option, err := parseExpireOption(args[2:])
	if err != nil {
		return protocol.MakeError(err)
	}
	success, err := db.ExpireWithOption(argString(args, 0), int64(ttl), option)
	if err != nil {
		return protocol.MakeError(err)
	}
	if success {
		return protocol.MakeInteger(1)
	}
	return protocol.MakeInteger(0)
}

// PExpireTimeCommand PEXPIRETIME 命令
type PExpireTimeCommand struct{}

func (c *PExpireTimeCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 1 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'pexpiretime' command"))
	}

	expireAt, exists := db.GetExpireTime(argString(args, 0))
	if !exists {
		return protocol.MakeInteger(-2)
	}
	if expireAt == 0 {
		return protocol.MakeInteger(-1)
	}
	return protocol.MakeInteger(expireAt)
}

// ExpireAtCommand EXPIREAT 命令
type ExpireAtCommand struct{}

func (c *ExpireAtCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) < 2 || len(args) > 3 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'expireat' command"))
	}

	ts, err := protocol.ParseInt(args[1])
	if err != nil {
		return protocol.MakeError(errors.New("ERR value is not an integer or out of range"))
	}

	option, err := parseExpireOption(args[2:])
	if err != nil {
		return protocol.MakeError(err)
	}
	success, err := db.PExpireAtWithOption(argString(args, 0), int64(ts)*1000, option)
	if err != nil {
		return protocol.MakeError(err)
	}
	if success {
		return protocol.MakeInteger(1)
	}
	return protocol.MakeInteger(0)
}

// PExpireAtCommand PEXPIREAT 命令
type PExpireAtCommand struct{}

func (c *PExpireAtCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) < 2 || len(args) > 3 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'pexpireat' command"))
	}

	ts, err := protocol.ParseInt(args[1])
	if err != nil {
		return protocol.MakeError(errors.New("ERR value is not an integer or out of range"))
	}

	option, err := parseExpireOption(args[2:])
	if err != nil {
		return protocol.MakeError(err)
	}
	success, err := db.PExpireAtWithOption(argString(args, 0), int64(ts), option)
	if err != nil {
		return protocol.MakeError(err)
	}
	if success {
		return protocol.MakeInteger(1)
	}
	return protocol.MakeInteger(0)
}

func parseExpireOption(args [][]byte) (string, error) {
	if len(args) == 0 {
		return "", nil
	}
	if len(args) != 1 {
		return "", errors.New("ERR syntax error")
	}
	switch {
	case equalsIgnoreCase(args[0], []byte("NX")):
		return "NX", nil
	case equalsIgnoreCase(args[0], []byte("XX")):
		return "XX", nil
	case equalsIgnoreCase(args[0], []byte("GT")):
		return "GT", nil
	case equalsIgnoreCase(args[0], []byte("LT")):
		return "LT", nil
	default:
		return "", errors.New("ERR syntax error")
	}
}

// PersistCommand PERSIST 命令
type PersistCommand struct{}

func (c *PersistCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 1 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'persist' command"))
	}

	ok, err := db.Persist(argString(args, 0))
	if err != nil {
		return protocol.MakeError(err)
	}
	if ok {
		return protocol.MakeInteger(1)
	}
	return protocol.MakeInteger(0)
}

// TypeCommand TYPE 命令
type TypeCommand struct{}

func (c *TypeCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 1 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'type' command"))
	}
	return protocol.MakeSimpleString(db.TypeOf(argString(args, 0)))
}

// KeysCommand KEYS 命令
type KeysCommand struct{}

func (c *KeysCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 1 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'keys' command"))
	}

	pattern := argString(args, 0)
	keys := db.Keys()

	var result []string
	if pattern == "*" {
		result = keys
	} else {
		result = make([]string, 0, len(keys))
		for _, k := range keys {
			ok, err := path.Match(pattern, k)
			if err != nil {
				return protocol.MakeError(errors.New("ERR invalid pattern"))
			}
			if ok {
				result = append(result, k)
			}
		}
	}

	return protocol.MakeArray(result)
}

// RandomKeyCommand RANDOMKEY 命令
type RandomKeyCommand struct{}

func (c *RandomKeyCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 0 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'randomkey' command"))
	}

	key, ok := db.RandomKey()
	if !ok {
		return protocol.MakeNull()
	}
	return protocol.MakeBulkString(key)
}

// TouchCommand TOUCH 命令
type TouchCommand struct{}

func (c *TouchCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) == 0 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'touch' command"))
	}

	var touched int64
	for i := range args {
		ok, err := db.Touch(argString(args, i))
		if err != nil {
			return protocol.MakeError(err)
		}
		if ok {
			touched++
		}
	}
	return protocol.MakeInteger(touched)
}

// UnlinkCommand UNLINK 命令
type UnlinkCommand struct{}

func (c *UnlinkCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) == 0 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'unlink' command"))
	}

	keys := make([]string, len(args))
	for i := range args {
		keys[i] = argString(args, i)
	}
	removed, err := db.DeleteManyWithError(keys)
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeInteger(removed)
}

// DumpCommand DUMP 命令
type DumpCommand struct{}

func (c *DumpCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 1 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'dump' command"))
	}

	data, ok, err := db.DumpValue(argString(args, 0))
	if err != nil {
		return protocol.MakeError(err)
	}
	if !ok {
		return protocol.MakeNull()
	}
	return protocol.MakeBulkString(string(data))
}

// RestoreCommand RESTORE 命令
type RestoreCommand struct{}

func (c *RestoreCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) < 3 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'restore' command"))
	}

	key := argString(args, 0)
	ttl, err := protocol.ParseInt(args[1])
	if err != nil || ttl < 0 {
		return protocol.MakeError(errors.New("ERR value is not an integer or out of range"))
	}

	replace := false
	absTTL := false
	for i := 3; i < len(args); i++ {
		switch {
		case equalsIgnoreCase(args[i], []byte("REPLACE")):
			replace = true
		case equalsIgnoreCase(args[i], []byte("ABSTTL")):
			absTTL = true
		case equalsIgnoreCase(args[i], []byte("IDLETIME")), equalsIgnoreCase(args[i], []byte("FREQ")):
			if i+1 >= len(args) {
				return protocol.MakeError(errors.New("ERR syntax error"))
			}
			i++
		default:
			return protocol.MakeError(errors.New("ERR syntax error"))
		}
	}

	restoreTTL := int64(ttl)
	if absTTL {
		if ttl == 0 {
			restoreTTL = 0
		} else {
			restoreTTL = int64(ttl) - time.Now().UnixMilli()
			if restoreTTL <= 0 {
				return protocol.MakeError(errors.New("ERR invalid expire time in 'restore' command"))
			}
		}
	}

	if err := db.RestoreSerializedValue(key, args[2], restoreTTL, replace); err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeSimpleString("OK")
}

// ScanCommand SCAN 命令
type ScanCommand struct{}

func (c *ScanCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) < 1 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'scan' command"))
	}

	cursor, err := protocol.ParseInt(args[0])
	if err != nil || cursor < 0 {
		return protocol.MakeError(errors.New("ERR invalid cursor"))
	}

	pattern := "*"
	count := 10
	for i := 1; i < len(args); i++ {
		switch {
		case equalsIgnoreCase(args[i], []byte("MATCH")):
			if i+1 >= len(args) {
				return protocol.MakeError(errors.New("ERR syntax error"))
			}
			pattern = argString(args, i+1)
			i++
		case equalsIgnoreCase(args[i], []byte("COUNT")):
			if i+1 >= len(args) {
				return protocol.MakeError(errors.New("ERR syntax error"))
			}
			n, err := protocol.ParseInt(args[i+1])
			if err != nil || n < 0 {
				return protocol.MakeError(errors.New("ERR value is not an integer or out of range"))
			}
			if n > 0 {
				count = n
			}
			i++
		default:
			return protocol.MakeError(errors.New("ERR syntax error"))
		}
	}

	keys := db.Keys()
	// The cursor is an offset, so each page needs the same order for an unchanged keyspace.
	sort.Strings(keys)
	result := make([]string, 0, count)
	idx := cursor
	for idx < len(keys) && len(result) < count {
		key := keys[idx]
		if pattern == "*" {
			result = append(result, key)
		} else {
			matched, err := path.Match(pattern, key)
			if err != nil {
				return protocol.MakeError(errors.New("ERR invalid pattern"))
			}
			if matched {
				result = append(result, key)
			}
		}
		idx++
	}

	nextCursor := "0"
	if idx < len(keys) {
		nextCursor = protocol.BytesToString(strconv.AppendInt(nil, int64(idx), 10))
	}

	return protocol.MakeArrayResponses([]*protocol.Response{
		protocol.MakeBulkString(nextCursor),
		protocol.MakeArray(result),
	})
}

// GetRangeCommand GETRANGE 命令
type GetRangeCommand struct{}

func (c *GetRangeCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 3 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'getrange' command"))
	}
	start, err := protocol.ParseInt(args[1])
	if err != nil {
		return protocol.MakeError(errors.New("ERR value is not an integer or out of range"))
	}
	end, err := protocol.ParseInt(args[2])
	if err != nil {
		return protocol.MakeError(errors.New("ERR value is not an integer or out of range"))
	}
	value, err := db.GetRangeStringBytes(args[0], start, end)
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeBulkString(value)
}

// SetRangeCommand SETRANGE 命令
type SetRangeCommand struct{}

func (c *SetRangeCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 3 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'setrange' command"))
	}
	offset, err := protocol.ParseInt(args[1])
	if err != nil || offset < 0 {
		return protocol.MakeError(errors.New("ERR value is not an integer or out of range"))
	}
	n, err := db.SetRangeStringBytes(args[0], offset, args[2])
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeInteger(n)
}

// FlushDBCommand FLUSHDB 命令
type FlushDBCommand struct{}

func (c *FlushDBCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 0 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'flushdb' command"))
	}
	if err := db.Clear(); err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeSimpleString("OK")
}

// DBSizeCommand DBSIZE 命令
type DBSizeCommand struct{}

func (c *DBSizeCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 0 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'dbsize' command"))
	}
	size := db.Size()
	return protocol.MakeInteger(int64(size))
}

// StrLenCommand STRLEN 命令
type StrLenCommand struct{}

func (c *StrLenCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 1 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'strlen' command"))
	}
	n, err := db.StrLen(args[0])
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeInteger(n)
}

// PingCommand PING 命令
type PingCommand struct{}

func (c *PingCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) > 1 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'ping' command"))
	}

	if len(args) == 0 {
		return protocol.MakeSimpleString("PONG")
	}

	return protocol.MakeBulkString(argString(args, 0))
}

// EchoCommand ECHO 命令
type EchoCommand struct{}

func (c *EchoCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 1 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'echo' command"))
	}
	return protocol.MakeBulkString(argString(args, 0))
}

// TtlCommand TTL 命令
type TtlCommand struct{}

func (c *TtlCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 1 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'ttl' command"))
	}

	key := argString(args, 0)
	expireAt, exists := db.GetExpireTime(key)
	if !exists {
		return protocol.MakeInteger(-2) // key 不存在
	}

	if expireAt == 0 {
		return protocol.MakeInteger(-1) // 永不过期
	}

	// 计算剩余时间（秒）
	now := time.Now().UnixMilli()
	remaining := (expireAt - now) / 1000
	if remaining <= 0 {
		return protocol.MakeInteger(-2) // 已过期
	}

	return protocol.MakeInteger(remaining)
}

// PttlCommand PTTL 命令
type PttlCommand struct{}

func (c *PttlCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 1 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'pttl' command"))
	}

	key := argString(args, 0)
	expireAt, exists := db.GetExpireTime(key)
	if !exists {
		return protocol.MakeInteger(-2) // key 不存在
	}

	if expireAt == 0 {
		return protocol.MakeInteger(-1) // 永不过期
	}

	// 计算剩余时间（毫秒）
	remaining := expireAt - time.Now().UnixMilli()
	if remaining <= 0 {
		return protocol.MakeInteger(-2) // 已过期
	}

	return protocol.MakeInteger(remaining)
}

// IncrCommand INCR 命令
type IncrCommand struct{}

func (c *IncrCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 1 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'incr' command"))
	}

	newValue, err := db.IncrStringBytes(args[0])
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeInteger(newValue)
}

// IncrByCommand INCRBY 命令
type IncrByCommand struct{}

func (c *IncrByCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 2 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'incrby' command"))
	}
	delta, err := protocol.ParseInt(args[1])
	if err != nil {
		return protocol.MakeError(errors.New("ERR value is not an integer or out of range"))
	}
	newValue, err := db.IncrByStringBytes(args[0], int64(delta))
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeInteger(newValue)
}

// DecrCommand DECR 命令
type DecrCommand struct{}

func (c *DecrCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 1 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'decr' command"))
	}

	newValue, err := db.DecrStringBytes(args[0])
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeInteger(newValue)
}

// DecrByCommand DECRBY 命令
type DecrByCommand struct{}

func (c *DecrByCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 2 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'decrby' command"))
	}
	delta, err := protocol.ParseInt(args[1])
	if err != nil {
		return protocol.MakeError(errors.New("ERR value is not an integer or out of range"))
	}
	newValue, err := db.DecrByStringBytes(args[0], int64(delta))
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeInteger(newValue)
}

// AppendCommand APPEND 命令
type AppendCommand struct{}

func (c *AppendCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 2 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'append' command"))
	}
	n, err := db.AppendStringBytes(args[0], args[1])
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeInteger(n)
}

// IncrByFloatCommand INCRBYFLOAT 命令
type IncrByFloatCommand struct{}

func (c *IncrByFloatCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 2 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'incrbyfloat' command"))
	}
	delta, err := strconv.ParseFloat(argString(args, 1), 64)
	if err != nil {
		return protocol.MakeError(errors.New("ERR value is not a valid float"))
	}
	out, err := db.IncrByFloatStringBytes(args[0], delta)
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeBulkString(out)
}

// MsetCommand MSET 命令
type MsetCommand struct{}

func (c *MsetCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) < 2 || len(args)%2 != 0 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'mset' command"))
	}

	if err := db.MSetStringBytes(args); err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeSimpleString("OK")
}

// MsetNxCommand MSETNX 命令
type MsetNxCommand struct{}

func (c *MsetNxCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) < 2 || len(args)%2 != 0 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'msetnx' command"))
	}
	ok, err := db.MSetNXStringBytes(args)
	if err != nil {
		return protocol.MakeError(err)
	}
	if ok {
		return protocol.MakeInteger(1)
	}
	return protocol.MakeInteger(0)
}

// SetNxCommand SETNX 命令
type SetNxCommand struct{}

func (c *SetNxCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 2 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'setnx' command"))
	}

	ok, err := db.SetStringBytesWithOptions(args[0], args[1], 0, false, true, false)
	if err != nil {
		return protocol.MakeError(err)
	}
	if ok {
		return protocol.MakeInteger(1)
	}
	return protocol.MakeInteger(0)
}

// SetExCommand SETEX 命令
type SetExCommand struct{}

func (c *SetExCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 3 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'setex' command"))
	}
	ttl, err := protocol.ParseInt(args[1])
	if err != nil || ttl <= 0 {
		return protocol.MakeError(errors.New("ERR value is not an integer or out of range"))
	}
	ok, err := db.SetStringBytesWithOptions(args[0], args[2], int64(ttl)*1000, false, false, false)
	if err != nil {
		return protocol.MakeError(err)
	}
	if !ok {
		return protocol.MakeNull()
	}
	return protocol.MakeSimpleString("OK")
}

// PSetExCommand PSETEX 命令
type PSetExCommand struct{}

func (c *PSetExCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 3 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'psetex' command"))
	}
	ttl, err := protocol.ParseInt(args[1])
	if err != nil || ttl <= 0 {
		return protocol.MakeError(errors.New("ERR value is not an integer or out of range"))
	}
	ok, err := db.SetStringBytesWithOptions(args[0], args[2], int64(ttl), false, false, false)
	if err != nil {
		return protocol.MakeError(err)
	}
	if !ok {
		return protocol.MakeNull()
	}
	return protocol.MakeSimpleString("OK")
}

// MgetCommand MGET 命令
type MgetCommand struct{}

func (c *MgetCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) == 0 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'mget' command"))
	}

	values, ok, err := db.MGetStringCopies(args)
	if err != nil {
		return protocol.MakeError(err)
	}

	resp := make([]*protocol.Response, len(values))
	for i := range values {
		if !ok[i] {
			resp[i] = protocol.MakeNull()
			continue
		}
		resp[i] = protocol.MakeBulkString(values[i])
	}
	return protocol.MakeArrayResponses(resp)
}

// GetBitCommand GETBIT 命令
type GetBitCommand struct{}

func (c *GetBitCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 2 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'getbit' command"))
	}
	offset, err := strconv.ParseInt(argString(args, 1), 10, 64)
	if err != nil || offset < 0 {
		return protocol.MakeError(errors.New("ERR bit offset is not an integer or out of range"))
	}
	out, err := db.GetBitStringBytes(args[0], offset)
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeInteger(out)
}

// SetBitCommand SETBIT 命令
type SetBitCommand struct{}

func (c *SetBitCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 3 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'setbit' command"))
	}
	offset, err := strconv.ParseInt(argString(args, 1), 10, 64)
	if err != nil || offset < 0 {
		return protocol.MakeError(errors.New("ERR bit offset is not an integer or out of range"))
	}
	bit, err := protocol.ParseInt(args[2])
	if err != nil || (bit != 0 && bit != 1) {
		return protocol.MakeError(errors.New("ERR bit is not an integer or out of range"))
	}
	out, err := db.SetBitStringBytes(args[0], offset, bit)
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeInteger(out)
}

// BitCountCommand BITCOUNT 命令
type BitCountCommand struct{}

func (c *BitCountCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 1 && len(args) != 3 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'bitcount' command"))
	}
	var startPtr *int
	var endPtr *int
	if len(args) == 3 {
		start, err := protocol.ParseInt(args[1])
		if err != nil {
			return protocol.MakeError(errors.New("ERR value is not an integer or out of range"))
		}
		end, err := protocol.ParseInt(args[2])
		if err != nil {
			return protocol.MakeError(errors.New("ERR value is not an integer or out of range"))
		}
		startPtr = &start
		endPtr = &end
	}
	out, err := db.BitCountStringBytes(args[0], startPtr, endPtr)
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeInteger(out)
}

// BitOpCommand BITOP 命令
type BitOpCommand struct{}

func (c *BitOpCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) < 3 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'bitop' command"))
	}

	op := argString(args, 0)
	out, err := db.BitOpStringBytes(strings.ToUpper(op), args[1], args[2:])
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeInteger(out)
}

// BitPosCommand BITPOS 命令
type BitPosCommand struct{}

func (c *BitPosCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) < 2 || len(args) > 5 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'bitpos' command"))
	}

	bit, err := protocol.ParseInt(args[1])
	if err != nil || (bit != 0 && bit != 1) {
		return protocol.MakeError(errors.New("ERR bit is not an integer or out of range"))
	}

	var start *int64
	var end *int64
	mode := "BYTE"
	if len(args) >= 3 {
		value, err := strconv.ParseInt(argString(args, 2), 10, 64)
		if err != nil {
			return protocol.MakeError(errors.New("ERR value is not an integer or out of range"))
		}
		start = &value
	}
	if len(args) >= 4 {
		value, err := strconv.ParseInt(argString(args, 3), 10, 64)
		if err != nil {
			return protocol.MakeError(errors.New("ERR value is not an integer or out of range"))
		}
		end = &value
	}
	if len(args) == 5 {
		switch {
		case equalsIgnoreCase(args[4], []byte("BYTE")):
			mode = "BYTE"
		case equalsIgnoreCase(args[4], []byte("BIT")):
			mode = "BIT"
		default:
			return protocol.MakeError(errors.New("ERR syntax error"))
		}
	}

	out, err := db.BitPosStringBytes(args[0], bit, start, end, mode)
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeInteger(out)
}

// GetSetCommand GETSET 命令
type GetSetCommand struct{}

func (c *GetSetCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 2 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'getset' command"))
	}

	oldValue, ok, err := db.GetStringCopy(args[0])
	if err != nil {
		return protocol.MakeError(err)
	}
	if err := db.SetStringBytes(args[0], args[1]); err != nil {
		return protocol.MakeError(err)
	}
	if !ok {
		return protocol.MakeNull()
	}
	return protocol.MakeBulkString(oldValue)
}

// RenameCommand RENAME 命令
type RenameCommand struct{}

func (c *RenameCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 2 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'rename' command"))
	}

	oldKey := argString(args, 0)
	newKey := argString(args, 1)
	_, err := db.Rename(oldKey, newKey, false)
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeSimpleString("OK")
}

// RenamenxCommand RENAMENX 命令（只有当新 key 不存在时才重命名）
type RenamenxCommand struct{}

func (c *RenamenxCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 2 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'renamenx' command"))
	}

	oldKey := argString(args, 0)
	newKey := argString(args, 1)
	ok, err := db.Rename(oldKey, newKey, true)
	if err != nil {
		return protocol.MakeError(err)
	}
	if !ok {
		return protocol.MakeInteger(0)
	}
	return protocol.MakeInteger(1)
}
