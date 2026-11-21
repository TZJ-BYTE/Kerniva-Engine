package command

import (
	"errors"
	"strconv"

	"github.com/TZJ-BYTE/Kerniva/internal/database"
	"github.com/TZJ-BYTE/Kerniva/internal/protocol"
)

type SAddCommand struct{}

func (c *SAddCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) < 2 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'sadd' command"))
	}
	members := make([]string, 0, len(args)-1)
	for i := 1; i < len(args); i++ {
		members = append(members, argString(args, i))
	}
	n, err := db.SAdd(argString(args, 0), members)
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeInteger(n)
}

type SRemCommand struct{}

func (c *SRemCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) < 2 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'srem' command"))
	}
	members := make([]string, 0, len(args)-1)
	for i := 1; i < len(args); i++ {
		members = append(members, argString(args, i))
	}
	n, err := db.SRem(argString(args, 0), members)
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeInteger(n)
}

type SMembersCommand struct{}

func (c *SMembersCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 1 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'smembers' command"))
	}
	members, err := db.SMembers(argString(args, 0))
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeArray(members)
}

type SIsMemberCommand struct{}

func (c *SIsMemberCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 2 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'sismember' command"))
	}
	ok, err := db.SIsMember(argString(args, 0), argString(args, 1))
	if err != nil {
		return protocol.MakeError(err)
	}
	if ok {
		return protocol.MakeInteger(1)
	}
	return protocol.MakeInteger(0)
}

type SCardCommand struct{}

func (c *SCardCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 1 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'scard' command"))
	}
	n, err := db.SCard(argString(args, 0))
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeInteger(n)
}

type SPopCommand struct{}

func (c *SPopCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 1 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'spop' command"))
	}
	member, ok, err := db.SPop(argString(args, 0))
	if err != nil {
		return protocol.MakeError(err)
	}
	if !ok {
		return protocol.MakeNull()
	}
	return protocol.MakeBulkString(member)
}

type SRandMemberCommand struct{}

func (c *SRandMemberCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 1 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'srandmember' command"))
	}
	member, ok, err := db.SRandMember(argString(args, 0))
	if err != nil {
		return protocol.MakeError(err)
	}
	if !ok {
		return protocol.MakeNull()
	}
	return protocol.MakeBulkString(member)
}

type SMoveCommand struct{}

func (c *SMoveCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 3 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'smove' command"))
	}
	moved, err := db.SMove(argString(args, 0), argString(args, 1), argString(args, 2))
	if err != nil {
		return protocol.MakeError(err)
	}
	if moved {
		return protocol.MakeInteger(1)
	}
	return protocol.MakeInteger(0)
}

type SScanCommand struct{}

func (c *SScanCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) < 2 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'sscan' command"))
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

	nextCursor, values, err := db.SScan(argString(args, 0), cursor, pattern, count)
	if err != nil {
		if err.Error() == "syntax error in pattern" {
			return protocol.MakeError(errors.New("ERR invalid pattern"))
		}
		return protocol.MakeError(err)
	}
	return protocol.MakeArrayResponses([]*protocol.Response{
		protocol.MakeBulkString(strconv.FormatInt(int64(nextCursor), 10)),
		protocol.MakeArray(values),
	})
}

type SInterCommand struct{}

func (c *SInterCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) < 1 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'sinter' command"))
	}
	keys := make([]string, len(args))
	for i := range args {
		keys[i] = argString(args, i)
	}
	values, err := db.SInter(keys)
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeArray(values)
}

type SInterStoreCommand struct{}

func (c *SInterStoreCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) < 2 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'sinterstore' command"))
	}
	keys := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		keys[i-1] = argString(args, i)
	}
	n, err := db.SInterStore(argString(args, 0), keys)
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeInteger(n)
}

type SInterCardCommand struct{}

func (c *SInterCardCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) < 2 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'sintercard' command"))
	}

	numKeys, err := protocol.ParseInt(args[0])
	if err != nil || numKeys < 1 {
		return protocol.MakeError(errors.New("ERR numkeys should be greater than 0"))
	}
	if len(args) < numKeys+1 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'sintercard' command"))
	}

	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = argString(args, i+1)
	}

	limit := int64(0)
	if len(args) > numKeys+1 {
		if len(args) != numKeys+3 || !equalsIgnoreCase(args[numKeys+1], []byte("LIMIT")) {
			return protocol.MakeError(errors.New("ERR syntax error"))
		}
		parsedLimit, err := protocol.ParseInt(args[numKeys+2])
		if err != nil || parsedLimit < 0 {
			return protocol.MakeError(errors.New("ERR limit is out of range"))
		}
		limit = int64(parsedLimit)
	}

	n, err := db.SInterCard(keys, limit)
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeInteger(n)
}

type SUnionCommand struct{}

func (c *SUnionCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) < 1 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'sunion' command"))
	}
	keys := make([]string, len(args))
	for i := range args {
		keys[i] = argString(args, i)
	}
	values, err := db.SUnion(keys)
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeArray(values)
}

type SUnionStoreCommand struct{}

func (c *SUnionStoreCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) < 2 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'sunionstore' command"))
	}
	keys := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		keys[i-1] = argString(args, i)
	}
	n, err := db.SUnionStore(argString(args, 0), keys)
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeInteger(n)
}

type SDiffCommand struct{}

func (c *SDiffCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) < 1 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'sdiff' command"))
	}
	keys := make([]string, len(args))
	for i := range args {
		keys[i] = argString(args, i)
	}
	values, err := db.SDiff(keys)
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeArray(values)
}

type SDiffStoreCommand struct{}

func (c *SDiffStoreCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) < 2 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'sdiffstore' command"))
	}
	keys := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		keys[i-1] = argString(args, i)
	}
	n, err := db.SDiffStore(argString(args, 0), keys)
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeInteger(n)
}

type SMIsMemberCommand struct{}

func (c *SMIsMemberCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) < 2 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'smismember' command"))
	}
	members := make([]string, 0, len(args)-1)
	for i := 1; i < len(args); i++ {
		members = append(members, argString(args, i))
	}
	results, err := db.SMIsMember(argString(args, 0), members)
	if err != nil {
		return protocol.MakeError(err)
	}
	items := make([]*protocol.Response, 0, len(results))
	for _, result := range results {
		items = append(items, protocol.MakeInteger(result))
	}
	return protocol.MakeArrayResponses(items)
}
