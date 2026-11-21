package command

import (
	"errors"
	"strconv"

	"github.com/TZJ-BYTE/Kerniva/internal/database"
	"github.com/TZJ-BYTE/Kerniva/internal/protocol"
)

// HSetCommand HSET 命令
type HSetCommand struct{}

func (c *HSetCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) < 3 || len(args)%2 == 0 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'hset' command"))
	}

	key := argString(args, 0)
	fields := make([]string, 0, (len(args)-1)/2)
	values := make([]string, 0, (len(args)-1)/2)
	for i := 1; i < len(args); i += 2 {
		fields = append(fields, argString(args, i))
		values = append(values, argString(args, i+1))
	}
	created, err := db.HSetMany(key, fields, values)
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeInteger(created)
}

// HGetCommand HGET 命令
type HGetCommand struct{}

func (c *HGetCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 2 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'hget' command"))
	}

	key := argString(args, 0)
	field := argString(args, 1)
	v, ok, err := db.HGet(key, field)
	if err != nil {
		return protocol.MakeError(err)
	}
	if !ok {
		return protocol.MakeNull()
	}
	return protocol.MakeBulkString(v)
}

// HMSetCommand HMSET 命令
type HMSetCommand struct{}

func (c *HMSetCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) < 3 || (len(args)-1)%2 != 0 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'hmset' command"))
	}

	key := argString(args, 0)
	fields := make([]string, 0, (len(args)-1)/2)
	values := make([]string, 0, (len(args)-1)/2)
	for i := 1; i < len(args); i += 2 {
		fields = append(fields, argString(args, i))
		values = append(values, argString(args, i+1))
	}
	if err := db.HMSet(key, fields, values); err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeSimpleString("OK")
}

// HMGetCommand HMGET 命令
type HMGetCommand struct{}

func (c *HMGetCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) < 2 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'hmget' command"))
	}

	key := argString(args, 0)
	fields := make([]string, 0, len(args)-1)
	for i := 1; i < len(args); i++ {
		fields = append(fields, argString(args, i))
	}
	result, ok, err := db.HMGetWithExists(key, fields)
	if err != nil {
		return protocol.MakeError(err)
	}
	resp := make([]*protocol.Response, len(result))
	for i := range result {
		if !ok[i] {
			resp[i] = protocol.MakeNull()
			continue
		}
		resp[i] = protocol.MakeBulkString(result[i])
	}
	return protocol.MakeArrayResponses(resp)
}

// HDelCommand HDEL 命令
type HDelCommand struct{}

func (c *HDelCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) < 2 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'hdel' command"))
	}

	key := argString(args, 0)
	fields := make([]string, 0, len(args)-1)
	for i := 1; i < len(args); i++ {
		fields = append(fields, argString(args, i))
	}
	n, err := db.HDel(key, fields)
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeInteger(n)
}

// HLenCommand HLEN 命令
type HLenCommand struct{}

func (c *HLenCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 1 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'hlen' command"))
	}

	key := argString(args, 0)
	n, err := db.HLen(key)
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeInteger(n)
}

// HExistsCommand HEXISTS 命令
type HExistsCommand struct{}

func (c *HExistsCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 2 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'hexists' command"))
	}

	key := argString(args, 0)
	field := argString(args, 1)
	ok, err := db.HExists(key, field)
	if err != nil {
		return protocol.MakeError(err)
	}
	if ok {
		return protocol.MakeInteger(1)
	}
	return protocol.MakeInteger(0)
}

// HSetNXCommand HSETNX 命令
type HSetNXCommand struct{}

func (c *HSetNXCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 3 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'hsetnx' command"))
	}

	out, err := db.HSetNX(argString(args, 0), argString(args, 1), argString(args, 2))
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeInteger(out)
}

// HStrLenCommand HSTRLEN 命令
type HStrLenCommand struct{}

func (c *HStrLenCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 2 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'hstrlen' command"))
	}

	out, err := db.HStrLen(argString(args, 0), argString(args, 1))
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeInteger(out)
}

// HKeysCommand HKEYS 命令
type HKeysCommand struct{}

func (c *HKeysCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 1 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'hkeys' command"))
	}

	key := argString(args, 0)
	keys, err := db.HKeys(key)
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeArray(keys)
}

// HValsCommand HVALS 命令
type HValsCommand struct{}

func (c *HValsCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 1 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'hvals' command"))
	}

	key := argString(args, 0)
	values, err := db.HVals(key)
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeArray(values)
}

// HGetAllCommand HGETALL 命令
type HGetAllCommand struct{}

func (c *HGetAllCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 1 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'hgetall' command"))
	}

	key := argString(args, 0)
	result, err := db.HGetAll(key)
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeArray(result)
}

// HScanCommand HSCAN 命令
type HScanCommand struct{}

func (c *HScanCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) < 2 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'hscan' command"))
	}

	cursor, err := protocol.ParseInt(args[1])
	if err != nil || cursor < 0 {
		return protocol.MakeError(errors.New("ERR invalid cursor"))
	}

	pattern := "*"
	count := 10
	for i := 2; i < len(args); i++ {
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

	nextCursor, items, err := db.HScan(argString(args, 0), cursor, pattern, count)
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeArrayResponses([]*protocol.Response{
		protocol.MakeBulkString(strconv.FormatInt(int64(nextCursor), 10)),
		protocol.MakeArray(items),
	})
}

// HRandFieldCommand HRANDFIELD 命令
type HRandFieldCommand struct{}

func (c *HRandFieldCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) < 1 || len(args) > 3 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'hrandfield' command"))
	}

	key := argString(args, 0)
	if len(args) == 1 {
		field, ok, err := db.HRandField(key)
		if err != nil {
			return protocol.MakeError(err)
		}
		if !ok {
			return protocol.MakeNull()
		}
		return protocol.MakeBulkString(field)
	}

	count, err := protocol.ParseInt(args[1])
	if err != nil {
		return protocol.MakeError(errors.New("ERR value is not an integer or out of range"))
	}

	withValues := false
	if len(args) == 3 {
		if !equalsIgnoreCase(args[2], []byte("WITHVALUES")) {
			return protocol.MakeError(errors.New("ERR syntax error"))
		}
		withValues = true
	}

	items, err := db.HRandFields(key, count, withValues)
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeArray(items)
}

// HIncrByCommand HINCRBY 命令
type HIncrByCommand struct{}

func (c *HIncrByCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 3 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'hincrby' command"))
	}

	key := argString(args, 0)
	field := argString(args, 1)
	delta, err := strconv.ParseInt(argString(args, 2), 10, 64)
	if err != nil {
		return protocol.MakeError(errors.New("ERR value is not an integer or out of range"))
	}
	out, err := db.HIncrBy(key, field, delta)
	if err != nil {
		return protocol.MakeError(errors.New(err.Error()))
	}
	return protocol.MakeInteger(out)
}

// HIncrByFloatCommand HINCRBYFLOAT 命令
type HIncrByFloatCommand struct{}

func (c *HIncrByFloatCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 3 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'hincrbyfloat' command"))
	}

	key := argString(args, 0)
	field := argString(args, 1)
	delta, err := strconv.ParseFloat(argString(args, 2), 64)
	if err != nil {
		return protocol.MakeError(errors.New("ERR value is not a float or out of range"))
	}
	out, err := db.HIncrByFloat(key, field, delta)
	if err != nil {
		return protocol.MakeError(errors.New(err.Error()))
	}
	return protocol.MakeBulkString(strconv.FormatFloat(out, 'f', -1, 64))
}
