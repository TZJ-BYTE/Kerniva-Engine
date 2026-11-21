package command

import (
	"errors"
	"strconv"
	"strings"

	"github.com/TZJ-BYTE/Kerniva/internal/database"
	"github.com/TZJ-BYTE/Kerniva/internal/datastruct"
	"github.com/TZJ-BYTE/Kerniva/internal/protocol"
)

type ZAddCommand struct{}

func (c *ZAddCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) < 3 || (len(args)-1)%2 != 0 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'zadd' command"))
	}
	entries := make([]datastruct.ZSetMember, 0, (len(args)-1)/2)
	for i := 1; i < len(args); i += 2 {
		score, err := strconv.ParseFloat(argString(args, i), 64)
		if err != nil {
			return protocol.MakeError(errors.New("ERR value is not a valid float"))
		}
		entries = append(entries, datastruct.ZSetMember{
			Member: argString(args, i+1),
			Score:  score,
		})
	}
	n, err := db.ZAdd(argString(args, 0), entries)
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeInteger(n)
}

type ZRemCommand struct{}

func (c *ZRemCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) < 2 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'zrem' command"))
	}
	members := make([]string, 0, len(args)-1)
	for i := 1; i < len(args); i++ {
		members = append(members, argString(args, i))
	}
	n, err := db.ZRem(argString(args, 0), members)
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeInteger(n)
}

type ZCardCommand struct{}

func (c *ZCardCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 1 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'zcard' command"))
	}
	n, err := db.ZCard(argString(args, 0))
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeInteger(n)
}

type ZScoreCommand struct{}

func (c *ZScoreCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 2 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'zscore' command"))
	}
	score, ok, err := db.ZScore(argString(args, 0), argString(args, 1))
	if err != nil {
		return protocol.MakeError(err)
	}
	if !ok {
		return protocol.MakeNull()
	}
	return protocol.MakeBulkString(strconv.FormatFloat(score, 'f', -1, 64))
}

type ZMScoreCommand struct{}

func (c *ZMScoreCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) < 2 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'zmscore' command"))
	}
	members := make([]string, 0, len(args)-1)
	for i := 1; i < len(args); i++ {
		members = append(members, argString(args, i))
	}
	scores, err := db.ZMScore(argString(args, 0), members)
	if err != nil {
		return protocol.MakeError(err)
	}
	items := make([]*protocol.Response, 0, len(scores))
	for _, score := range scores {
		if score == nil {
			items = append(items, protocol.MakeNull())
			continue
		}
		items = append(items, protocol.MakeBulkString(strconv.FormatFloat(*score, 'f', -1, 64)))
	}
	return protocol.MakeArrayResponses(items)
}

type ZIncrByCommand struct{}

func (c *ZIncrByCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 3 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'zincrby' command"))
	}
	delta, err := strconv.ParseFloat(argString(args, 1), 64)
	if err != nil {
		return protocol.MakeError(errors.New("ERR value is not a valid float"))
	}
	score, err := db.ZIncrBy(argString(args, 0), argString(args, 2), delta)
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeBulkString(strconv.FormatFloat(score, 'f', -1, 64))
}

type ZRangeCommand struct{}

func (c *ZRangeCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	return executeZRange(db, args, false)
}

type ZRevRangeCommand struct{}

func (c *ZRevRangeCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	return executeZRange(db, args, true)
}

func executeZRange(db *database.Database, args [][]byte, reverse bool) *protocol.Response {
	name := "zrange"
	if reverse {
		name = "zrevrange"
	}
	if len(args) < 3 || len(args) > 4 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for '" + name + "' command"))
	}
	start, err := protocol.ParseInt(args[1])
	if err != nil {
		return protocol.MakeError(errors.New("ERR value is not an integer or out of range"))
	}
	stop, err := protocol.ParseInt(args[2])
	if err != nil {
		return protocol.MakeError(errors.New("ERR value is not an integer or out of range"))
	}
	withScores := false
	if len(args) == 4 {
		if !equalsIgnoreCase(args[3], []byte("WITHSCORES")) {
			return protocol.MakeError(errors.New("ERR syntax error"))
		}
		withScores = true
	}
	values, err := db.ZRange(argString(args, 0), start, stop, reverse, withScores)
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeArray(values)
}

type ZCountCommand struct{}

func (c *ZCountCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 3 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'zcount' command"))
	}
	min, err := database.ParseZCountBound(argString(args, 1))
	if err != nil {
		return protocol.MakeError(errors.New("ERR min or max is not a float"))
	}
	max, err := database.ParseZCountBound(argString(args, 2))
	if err != nil {
		return protocol.MakeError(errors.New("ERR min or max is not a float"))
	}
	n, err := db.ZCount(argString(args, 0), min, max)
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeInteger(n)
}

type ZRankCommand struct{}

func (c *ZRankCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	return executeZRank(db, args, false)
}

type ZRevRankCommand struct{}

func (c *ZRevRankCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	return executeZRank(db, args, true)
}

func executeZRank(db *database.Database, args [][]byte, reverse bool) *protocol.Response {
	name := "zrank"
	if reverse {
		name = "zrevrank"
	}
	if len(args) != 2 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for '" + name + "' command"))
	}
	rank, ok, err := db.ZRank(argString(args, 0), argString(args, 1), reverse)
	if err != nil {
		return protocol.MakeError(err)
	}
	if !ok {
		return protocol.MakeNull()
	}
	return protocol.MakeInteger(rank)
}

type ZRangeByScoreCommand struct{}

func (c *ZRangeByScoreCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	return executeZRangeByScore(db, args, false)
}

type ZRevRangeByScoreCommand struct{}

func (c *ZRevRangeByScoreCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	return executeZRangeByScore(db, args, true)
}

func executeZRangeByScore(db *database.Database, args [][]byte, reverse bool) *protocol.Response {
	name := "zrangebyscore"
	if reverse {
		name = "zrevrangebyscore"
	}
	if len(args) < 3 || len(args) > 4 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for '" + name + "' command"))
	}
	min, err := database.ParseZCountBound(argString(args, 1))
	if err != nil {
		return protocol.MakeError(errors.New("ERR min or max is not a float"))
	}
	max, err := database.ParseZCountBound(argString(args, 2))
	if err != nil {
		return protocol.MakeError(errors.New("ERR min or max is not a float"))
	}
	withScores := false
	if len(args) == 4 {
		if !equalsIgnoreCase(args[3], []byte("WITHSCORES")) {
			return protocol.MakeError(errors.New("ERR syntax error"))
		}
		withScores = true
	}
	values, err := db.ZRangeByScore(argString(args, 0), min, max, withScores, reverse)
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeArray(values)
}

type ZRemRangeByRankCommand struct{}

func (c *ZRemRangeByRankCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 3 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'zremrangebyrank' command"))
	}
	start, err := protocol.ParseInt(args[1])
	if err != nil {
		return protocol.MakeError(errors.New("ERR value is not an integer or out of range"))
	}
	stop, err := protocol.ParseInt(args[2])
	if err != nil {
		return protocol.MakeError(errors.New("ERR value is not an integer or out of range"))
	}
	n, err := db.ZRemRangeByRank(argString(args, 0), start, stop)
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeInteger(n)
}

type ZRandMemberCommand struct{}

func (c *ZRandMemberCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) < 1 || len(args) > 3 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'zrandmember' command"))
	}

	if len(args) == 1 {
		member, _, ok, err := db.ZRandMember(argString(args, 0))
		if err != nil {
			return protocol.MakeError(err)
		}
		if !ok {
			return protocol.MakeNull()
		}
		return protocol.MakeBulkString(member)
	}

	count, err := protocol.ParseInt(args[1])
	if err != nil {
		return protocol.MakeError(errors.New("ERR value is not an integer or out of range"))
	}
	withScores := false
	if len(args) == 3 {
		if !equalsIgnoreCase(args[2], []byte("WITHSCORES")) {
			return protocol.MakeError(errors.New("ERR syntax error"))
		}
		withScores = true
	}

	values, err := db.ZRandMembers(argString(args, 0), count, withScores)
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeArray(values)
}

type ZRemRangeByScoreCommand struct{}

func (c *ZRemRangeByScoreCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 3 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'zremrangebyscore' command"))
	}
	min, err := database.ParseZCountBound(argString(args, 1))
	if err != nil {
		return protocol.MakeError(errors.New("ERR min or max is not a float"))
	}
	max, err := database.ParseZCountBound(argString(args, 2))
	if err != nil {
		return protocol.MakeError(errors.New("ERR min or max is not a float"))
	}
	n, err := db.ZRemRangeByScore(argString(args, 0), min, max)
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeInteger(n)
}

type ZPopMinCommand struct{}

func (c *ZPopMinCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 1 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'zpopmin' command"))
	}
	values, err := db.ZPop(argString(args, 0), false)
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeArray(values)
}

type ZPopMaxCommand struct{}

func (c *ZPopMaxCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 1 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'zpopmax' command"))
	}
	values, err := db.ZPop(argString(args, 0), true)
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeArray(values)
}

type ZScanCommand struct{}

func (c *ZScanCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) < 2 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'zscan' command"))
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
	nextCursor, values, err := db.ZScan(argString(args, 0), cursor, pattern, count)
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeArrayResponses([]*protocol.Response{
		protocol.MakeBulkString(strconv.Itoa(nextCursor)),
		protocol.MakeArray(values),
	})
}

type ZUnionCommand struct{}

func (c *ZUnionCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	keys, weights, aggregate, withScores, errResp := parseZSetAggregateSpec(args, 0, "zunion", true, true)
	if errResp != nil {
		return errResp
	}
	values, err := db.ZUnion(keys, weights, aggregate)
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeArray(zsetOpResponseValues(values, withScores))
}

type ZUnionStoreCommand struct{}

func (c *ZUnionStoreCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) < 3 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'zunionstore' command"))
	}
	keys, weights, aggregate, _, errResp := parseZSetAggregateSpec(args, 1, "zunionstore", true, false)
	if errResp != nil {
		return errResp
	}
	n, err := db.ZUnionStore(argString(args, 0), keys, weights, aggregate)
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeInteger(n)
}

type ZInterCommand struct{}

func (c *ZInterCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	keys, weights, aggregate, withScores, errResp := parseZSetAggregateSpec(args, 0, "zinter", true, true)
	if errResp != nil {
		return errResp
	}
	values, err := db.ZInter(keys, weights, aggregate)
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeArray(zsetOpResponseValues(values, withScores))
}

type ZInterStoreCommand struct{}

func (c *ZInterStoreCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) < 3 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'zinterstore' command"))
	}
	keys, weights, aggregate, _, errResp := parseZSetAggregateSpec(args, 1, "zinterstore", true, false)
	if errResp != nil {
		return errResp
	}
	n, err := db.ZInterStore(argString(args, 0), keys, weights, aggregate)
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeInteger(n)
}

type ZDiffCommand struct{}

func (c *ZDiffCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	keys, _, _, withScores, errResp := parseZSetAggregateSpec(args, 0, "zdiff", false, true)
	if errResp != nil {
		return errResp
	}
	values, err := db.ZDiff(keys)
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeArray(zsetOpResponseValues(values, withScores))
}

type ZDiffStoreCommand struct{}

func (c *ZDiffStoreCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) < 3 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'zdiffstore' command"))
	}
	keys, _, _, _, errResp := parseZSetAggregateSpec(args, 1, "zdiffstore", false, false)
	if errResp != nil {
		return errResp
	}
	n, err := db.ZDiffStore(argString(args, 0), keys)
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeInteger(n)
}

func parseZSetAggregateSpec(args [][]byte, offset int, commandName string, allowAggregate bool, allowWithScores bool) ([]string, []float64, string, bool, *protocol.Response) {
	if len(args) <= offset {
		return nil, nil, "", false, protocol.MakeError(errors.New("ERR wrong number of arguments for '" + commandName + "' command"))
	}
	numKeys, err := protocol.ParseInt(args[offset])
	if err != nil || numKeys < 1 {
		return nil, nil, "", false, protocol.MakeError(errors.New("ERR numkeys should be greater than 0"))
	}
	if len(args) < offset+1+numKeys {
		return nil, nil, "", false, protocol.MakeError(errors.New("ERR wrong number of arguments for '" + commandName + "' command"))
	}

	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = argString(args, offset+1+i)
	}

	weights := make([]float64, numKeys)
	for i := range weights {
		weights[i] = 1
	}
	aggregate := "SUM"
	withScores := false

	seenWeights := false
	seenAggregate := false
	idx := offset + 1 + numKeys
	for idx < len(args) {
		switch {
		case equalsIgnoreCase(args[idx], []byte("WEIGHTS")):
			if !allowAggregate || seenWeights || idx+numKeys >= len(args) {
				return nil, nil, "", false, protocol.MakeError(errors.New("ERR syntax error"))
			}
			for i := 0; i < numKeys; i++ {
				weight, err := strconv.ParseFloat(argString(args, idx+1+i), 64)
				if err != nil {
					return nil, nil, "", false, protocol.MakeError(errors.New("ERR weight value is not a float"))
				}
				weights[i] = weight
			}
			seenWeights = true
			idx += numKeys + 1
		case equalsIgnoreCase(args[idx], []byte("AGGREGATE")):
			if !allowAggregate || seenAggregate || idx+1 >= len(args) {
				return nil, nil, "", false, protocol.MakeError(errors.New("ERR syntax error"))
			}
			aggregate = strings.ToUpper(argString(args, idx+1))
			switch aggregate {
			case "SUM", "MIN", "MAX":
			default:
				return nil, nil, "", false, protocol.MakeError(errors.New("ERR syntax error"))
			}
			seenAggregate = true
			idx += 2
		case equalsIgnoreCase(args[idx], []byte("WITHSCORES")):
			if !allowWithScores || withScores {
				return nil, nil, "", false, protocol.MakeError(errors.New("ERR syntax error"))
			}
			withScores = true
			idx++
		default:
			return nil, nil, "", false, protocol.MakeError(errors.New("ERR syntax error"))
		}
	}

	return keys, weights, aggregate, withScores, nil
}

func zsetOpResponseValues(items []datastruct.ZSetMember, withScores bool) []string {
	if len(items) == 0 {
		return []string{}
	}
	if !withScores {
		out := make([]string, 0, len(items))
		for _, item := range items {
			out = append(out, item.Member)
		}
		return out
	}
	out := make([]string, 0, len(items)*2)
	for _, item := range items {
		out = append(out, item.Member, strconv.FormatFloat(item.Score, 'f', -1, 64))
	}
	return out
}
