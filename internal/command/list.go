package command

import (
	"errors"
	"strings"

	"github.com/TZJ-BYTE/Kerniva/internal/database"
	"github.com/TZJ-BYTE/Kerniva/internal/protocol"
)

// LPushCommand LPUSH 命令
type LPushCommand struct{}

func (c *LPushCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) < 2 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'lpush' command"))
	}

	key := argString(args, 0)
	vals := make([]string, 0, len(args)-1)
	for i := 1; i < len(args); i++ {
		vals = append(vals, argString(args, i))
	}
	n, err := db.LPush(key, vals)
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeInteger(int64(n))
}

// LPushXCommand LPUSHX 命令
type LPushXCommand struct{}

func (c *LPushXCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) < 2 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'lpushx' command"))
	}

	key := argString(args, 0)
	vals := make([]string, 0, len(args)-1)
	for i := 1; i < len(args); i++ {
		vals = append(vals, argString(args, i))
	}
	n, err := db.LPushX(key, vals)
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeInteger(int64(n))
}

// RPushCommand RPUSH 命令
type RPushCommand struct{}

func (c *RPushCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) < 2 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'rpush' command"))
	}

	key := argString(args, 0)
	vals := make([]string, 0, len(args)-1)
	for i := 1; i < len(args); i++ {
		vals = append(vals, argString(args, i))
	}
	n, err := db.RPush(key, vals)
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeInteger(int64(n))
}

// RPushXCommand RPUSHX 命令
type RPushXCommand struct{}

func (c *RPushXCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) < 2 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'rpushx' command"))
	}

	key := argString(args, 0)
	vals := make([]string, 0, len(args)-1)
	for i := 1; i < len(args); i++ {
		vals = append(vals, argString(args, i))
	}
	n, err := db.RPushX(key, vals)
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeInteger(int64(n))
}

// LPopCommand LPOP 命令
type LPopCommand struct{}

func (c *LPopCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 1 && len(args) != 2 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'lpop' command"))
	}

	key := argString(args, 0)
	if len(args) == 2 {
		count, err := parseStrictIntArg(args[1])
		if err != nil {
			return protocol.MakeError(err)
		}
		if count < 0 {
			return protocol.MakeError(errors.New("ERR value is out of range, must be positive"))
		}
		values, ok, err := db.LPopCount(key, count)
		if err != nil {
			return protocol.MakeError(err)
		}
		if !ok {
			return protocol.MakeNull()
		}
		return protocol.MakeArray(values)
	}

	val, ok, err := db.LPop(key)
	if err != nil {
		return protocol.MakeError(err)
	}
	if !ok {
		return protocol.MakeNull()
	}
	return protocol.MakeBulkString(val)
}

// RPopCommand RPOP 命令
type RPopCommand struct{}

func (c *RPopCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 1 && len(args) != 2 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'rpop' command"))
	}

	key := argString(args, 0)
	if len(args) == 2 {
		count, err := parseStrictIntArg(args[1])
		if err != nil {
			return protocol.MakeError(err)
		}
		if count < 0 {
			return protocol.MakeError(errors.New("ERR value is out of range, must be positive"))
		}
		values, ok, err := db.RPopCount(key, count)
		if err != nil {
			return protocol.MakeError(err)
		}
		if !ok {
			return protocol.MakeNull()
		}
		return protocol.MakeArray(values)
	}

	val, ok, err := db.RPop(key)
	if err != nil {
		return protocol.MakeError(err)
	}
	if !ok {
		return protocol.MakeNull()
	}
	return protocol.MakeBulkString(val)
}

// LLenCommand LLEN 命令
type LLenCommand struct{}

func (c *LLenCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 1 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'llen' command"))
	}

	key := argString(args, 0)
	n, err := db.LLen(key)
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeInteger(int64(n))
}

// LRangeCommand LRANGE 命令
type LRangeCommand struct{}

func (c *LRangeCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 3 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'lrange' command"))
	}

	key := argString(args, 0)
	start := parseIntBytes(args[1])
	stop := parseIntBytes(args[2])
	result, err := db.LRange(key, start, stop)
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeArray(result)
}

// LIndexCommand LINDEX 命令
type LIndexCommand struct{}

func (c *LIndexCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 2 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'lindex' command"))
	}
	idx := parseIntBytes(args[1])
	value, ok, err := db.LIndex(argString(args, 0), idx)
	if err != nil {
		return protocol.MakeError(err)
	}
	if !ok {
		return protocol.MakeNull()
	}
	return protocol.MakeBulkString(value)
}

// LSetCommand LSET 命令
type LSetCommand struct{}

func (c *LSetCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 3 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'lset' command"))
	}
	idx := parseIntBytes(args[1])
	if err := db.LSet(argString(args, 0), idx, argString(args, 2)); err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeSimpleString("OK")
}

// LTrimCommand LTRIM 命令
type LTrimCommand struct{}

func (c *LTrimCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 3 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'ltrim' command"))
	}
	start := parseIntBytes(args[1])
	stop := parseIntBytes(args[2])
	if err := db.LTrim(argString(args, 0), start, stop); err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeSimpleString("OK")
}

// RPopLPushCommand RPOPLPUSH 命令
type RPopLPushCommand struct{}

func (c *RPopLPushCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 2 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'rpoplpush' command"))
	}
	value, ok, err := db.RPopLPush(argString(args, 0), argString(args, 1))
	if err != nil {
		return protocol.MakeError(err)
	}
	if !ok {
		return protocol.MakeNull()
	}
	return protocol.MakeBulkString(value)
}

// LMoveCommand LMOVE 命令
type LMoveCommand struct{}

func (c *LMoveCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 4 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'lmove' command"))
	}

	fromLeft, err := parseListSide(args[2])
	if err != nil {
		return protocol.MakeError(err)
	}
	toLeft, err := parseListSide(args[3])
	if err != nil {
		return protocol.MakeError(err)
	}

	value, ok, err := db.LMove(argString(args, 0), argString(args, 1), fromLeft, toLeft)
	if err != nil {
		return protocol.MakeError(err)
	}
	if !ok {
		return protocol.MakeNull()
	}
	return protocol.MakeBulkString(value)
}

// LMPopCommand LMPOP 命令
type LMPopCommand struct{}

func (c *LMPopCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	keys, fromLeft, count, err := parseListMPopArgs("lmpop", args)
	if err != nil {
		return protocol.MakeError(err)
	}

	key, values, ok, err := db.LMPop(keys, fromLeft, count)
	if err != nil {
		return protocol.MakeError(err)
	}
	if !ok {
		return protocol.MakeNull()
	}
	return makeListKeyArrayResponse(key, values)
}

// LPosCommand LPOS 命令
type LPosCommand struct{}

func (c *LPosCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) < 2 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'lpos' command"))
	}

	rank := 1
	count := -1
	maxLen := 0
	countSpecified := false

	for i := 2; i < len(args); i++ {
		switch strings.ToUpper(argString(args, i)) {
		case "RANK":
			if i+1 >= len(args) {
				return protocol.MakeError(errors.New("ERR syntax error"))
			}
			n, err := parseStrictIntArg(args[i+1])
			if err != nil {
				return protocol.MakeError(err)
			}
			if n == 0 {
				return protocol.MakeError(errors.New("ERR RANK can't be zero"))
			}
			rank = n
			i++
		case "COUNT":
			if i+1 >= len(args) {
				return protocol.MakeError(errors.New("ERR syntax error"))
			}
			n, err := parseStrictIntArg(args[i+1])
			if err != nil {
				return protocol.MakeError(err)
			}
			if n < 0 {
				return protocol.MakeError(errors.New("ERR COUNT can't be negative"))
			}
			count = n
			countSpecified = true
			i++
		case "MAXLEN":
			if i+1 >= len(args) {
				return protocol.MakeError(errors.New("ERR syntax error"))
			}
			n, err := parseStrictIntArg(args[i+1])
			if err != nil {
				return protocol.MakeError(err)
			}
			if n < 0 {
				return protocol.MakeError(errors.New("ERR MAXLEN can't be negative"))
			}
			maxLen = n
			i++
		default:
			return protocol.MakeError(errors.New("ERR syntax error"))
		}
	}

	positions, err := db.LPos(argString(args, 0), argString(args, 1), rank, count, maxLen)
	if err != nil {
		return protocol.MakeError(err)
	}

	if countSpecified {
		items := make([]*protocol.Response, 0, len(positions))
		for _, pos := range positions {
			items = append(items, protocol.MakeInteger(int64(pos)))
		}
		return protocol.MakeArrayResponses(items)
	}
	if len(positions) == 0 {
		return protocol.MakeNull()
	}
	return protocol.MakeInteger(int64(positions[0]))
}

// LRemCommand LREM 命令
type LRemCommand struct{}

func (c *LRemCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 3 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'lrem' command"))
	}

	count, err := parseStrictIntArg(args[1])
	if err != nil {
		return protocol.MakeError(err)
	}
	removed, err := db.LRem(argString(args, 0), count, argString(args, 2))
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeInteger(int64(removed))
}

func parseListSide(b []byte) (bool, error) {
	switch strings.ToUpper(argString([][]byte{b}, 0)) {
	case "LEFT":
		return true, nil
	case "RIGHT":
		return false, nil
	default:
		return false, errors.New("ERR syntax error")
	}
}

func parseStrictIntArg(b []byte) (int, error) {
	n, err := protocol.ParseInt(b)
	if err != nil {
		return 0, errors.New("ERR value is not an integer or out of range")
	}
	return n, nil
}

func parseIntBytes(b []byte) int {
	n, _ := protocol.ParseInt(b)
	return n
}

func parseListMPopArgs(commandName string, args [][]byte) ([]string, bool, int, error) {
	if len(args) < 3 {
		return nil, false, 0, errors.New("ERR wrong number of arguments for '" + commandName + "' command")
	}

	numKeys, err := parseStrictIntArg(args[0])
	if err != nil {
		return nil, false, 0, err
	}
	if numKeys <= 0 {
		return nil, false, 0, errors.New("ERR numkeys should be greater than 0")
	}
	if len(args) < numKeys+2 {
		return nil, false, 0, errors.New("ERR wrong number of arguments for '" + commandName + "' command")
	}

	keys := make([]string, 0, numKeys)
	for i := 0; i < numKeys; i++ {
		keys = append(keys, argString(args, i+1))
	}

	fromLeft, err := parseListSide(args[numKeys+1])
	if err != nil {
		return nil, false, 0, err
	}

	count := 1
	if len(args) == numKeys+2 {
		return keys, fromLeft, count, nil
	}
	if len(args) != numKeys+4 {
		return nil, false, 0, errors.New("ERR syntax error")
	}
	if strings.ToUpper(argString(args, numKeys+2)) != "COUNT" {
		return nil, false, 0, errors.New("ERR syntax error")
	}

	count, err = parseStrictIntArg(args[numKeys+3])
	if err != nil {
		return nil, false, 0, err
	}
	if count <= 0 {
		return nil, false, 0, errors.New("ERR count should be greater than 0")
	}
	return keys, fromLeft, count, nil
}

func makeListKeyArrayResponse(key string, values []string) *protocol.Response {
	return protocol.MakeArrayResponses([]*protocol.Response{
		protocol.MakeBulkString(key),
		protocol.MakeArray(values),
	})
}
