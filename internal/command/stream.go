package command

import (
	"errors"
	"strconv"
	"strings"

	"github.com/TZJ-BYTE/Kerniva/internal/database"
	"github.com/TZJ-BYTE/Kerniva/internal/datastruct"
	"github.com/TZJ-BYTE/Kerniva/internal/protocol"
)

type XAddCommand struct{}

func (c *XAddCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) < 4 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'xadd' command"))
	}

	key := argString(args, 0)
	noMkStream := false
	maxLen := -1
	i := 1
	for i < len(args)-1 {
		switch strings.ToUpper(argString(args, i)) {
		case "NOMKSTREAM":
			noMkStream = true
			i++
		case "MAXLEN":
			if i+1 >= len(args)-1 {
				return protocol.MakeError(errors.New("ERR syntax error"))
			}
			i++
			if i < len(args)-1 && (equalsIgnoreCase(args[i], []byte("=")) || equalsIgnoreCase(args[i], []byte("~"))) {
				i++
			}
			if i >= len(args)-1 {
				return protocol.MakeError(errors.New("ERR syntax error"))
			}
			n, err := parseStrictIntArg(args[i])
			if err != nil || n < 0 {
				return protocol.MakeError(errors.New("ERR value is not an integer or out of range"))
			}
			maxLen = n
			i++
		default:
			goto parsedOptions
		}
	}

parsedOptions:
	if i >= len(args) {
		return protocol.MakeError(errors.New("ERR syntax error"))
	}
	if len(args)-i < 3 || (len(args)-i-1)%2 != 0 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'xadd' command"))
	}

	id, err := db.XAdd(key, argString(args, i), byteArgsToStrings(args[i+1:]), maxLen, noMkStream)
	if err != nil {
		return protocol.MakeError(err)
	}
	if id == "" {
		return protocol.MakeNull()
	}
	return protocol.MakeBulkString(id)
}

type XLenCommand struct{}

func (c *XLenCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 1 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'xlen' command"))
	}
	n, err := db.XLen(argString(args, 0))
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeInteger(n)
}

type XRangeCommand struct{}

func (c *XRangeCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	return executeXRange(db, args, false)
}

type XRevRangeCommand struct{}

func (c *XRevRangeCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	return executeXRange(db, args, true)
}

func executeXRange(db *database.Database, args [][]byte, reverse bool) *protocol.Response {
	name := "xrange"
	if reverse {
		name = "xrevrange"
	}
	if len(args) != 3 && len(args) != 5 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for '" + name + "' command"))
	}

	count := 0
	if len(args) == 5 {
		if !equalsIgnoreCase(args[3], []byte("COUNT")) {
			return protocol.MakeError(errors.New("ERR syntax error"))
		}
		n, err := parseStrictIntArg(args[4])
		if err != nil || n < 0 {
			return protocol.MakeError(errors.New("ERR value is not an integer or out of range"))
		}
		count = n
	}

	start := argString(args, 1)
	end := argString(args, 2)
	entries, err := db.XRange(argString(args, 0), start, end, count, reverse)
	if err != nil {
		return protocol.MakeError(err)
	}
	return makeStreamEntriesResponse(entries)
}

type XDelCommand struct{}

func (c *XDelCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) < 2 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'xdel' command"))
	}
	n, err := db.XDel(argString(args, 0), byteArgsToStrings(args[1:]))
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeInteger(n)
}

type XTrimCommand struct{}

func (c *XTrimCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) < 3 || len(args) > 4 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'xtrim' command"))
	}
	if !equalsIgnoreCase(args[1], []byte("MAXLEN")) {
		return protocol.MakeError(errors.New("ERR syntax error"))
	}
	index := 2
	if len(args) == 4 {
		if !equalsIgnoreCase(args[2], []byte("=")) && !equalsIgnoreCase(args[2], []byte("~")) {
			return protocol.MakeError(errors.New("ERR syntax error"))
		}
		index = 3
	}
	n, err := parseStrictIntArg(args[index])
	if err != nil || n < 0 {
		return protocol.MakeError(errors.New("ERR value is not an integer or out of range"))
	}
	removed, err := db.XTrim(argString(args, 0), n)
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeInteger(removed)
}

func byteArgsToStrings(args [][]byte) []string {
	out := make([]string, len(args))
	for i := range args {
		out[i] = argString(args, i)
	}
	return out
}

func makeStreamEntriesResponse(entries []datastruct.StreamEntry) *protocol.Response {
	items := make([]*protocol.Response, 0, len(entries))
	for i := range entries {
		entry := entries[i]
		items = append(items, protocol.MakeArrayResponses([]*protocol.Response{
			protocol.MakeBulkString(entry.ID),
			protocol.MakeArray(entry.Fields),
		}))
	}
	return protocol.MakeArrayResponses(items)
}

// XReadCommand XREAD 命令
type XReadCommand struct{}

func (c *XReadCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	count, blockMs, keys, ids, err := parseXReadArgs(args)
	if err != nil {
		return protocol.MakeError(err)
	}
	_ = blockMs // Blocking is handled in server layer via XReadBlockingResult

	results, err := db.XRead(keys, ids, count)
	if err != nil {
		return protocol.MakeError(err)
	}
	if len(results) == 0 {
		return protocol.MakeNull()
	}
	return makeXReadResponse(results)
}

// XReadBlockingResult 从 DB 层非阻塞读取（供 server 层轮询）
type XReadBlockingResult struct {
	Count   int
	Keys    []string
	IDs     []string
	BlockMs int64
}

func parseXReadArgs(args [][]byte) (count int, blockMs int64, keys []string, ids []string, err error) {
	count = -1
	blockMs = -1
	i := 0
	for i < len(args) {
		switch strings.ToUpper(argString(args, i)) {
		case "COUNT":
			if i+1 >= len(args) {
				return 0, 0, nil, nil, errors.New("ERR syntax error")
			}
			i++
			n, parseErr := strconv.Atoi(argString(args, i))
			if parseErr != nil || n <= 0 {
				return 0, 0, nil, nil, errors.New("ERR value is not an integer or out of range")
			}
			count = n
			i++
		case "BLOCK":
			if i+1 >= len(args) {
				return 0, 0, nil, nil, errors.New("ERR syntax error")
			}
			i++
			n, parseErr := strconv.ParseInt(argString(args, i), 10, 64)
			if parseErr != nil || n < 0 {
				return 0, 0, nil, nil, errors.New("ERR timeout is not a float or out of range")
			}
			blockMs = n
			i++
		default:
			goto parsedOptions
		}
	}

parsedOptions:
	if i >= len(args) || strings.ToUpper(argString(args, i)) != "STREAMS" {
		return 0, 0, nil, nil, errors.New("ERR syntax error")
	}
	i++
	remaining := len(args) - i
	if remaining == 0 || remaining%2 != 0 {
		return 0, 0, nil, nil, errors.New("ERR Unbalanced XREAD list of streams: for each stream key an ID or '$' must be specified")
	}
	n := remaining / 2
	keys = make([]string, 0, n)
	ids = make([]string, 0, n)
	for j := 0; j < n; j++ {
		keys = append(keys, argString(args, i+j))
	}
	for j := 0; j < n; j++ {
		id := argString(args, i+n+j)
		if id == "$" {
			id = "0-0"
		}
		ids = append(ids, id)
	}
	return count, blockMs, keys, ids, nil
}

func makeXReadResponse(results []database.XReadResult) *protocol.Response {
	items := make([]*protocol.Response, 0, len(results))
	for _, r := range results {
		entryItems := make([]*protocol.Response, 0, len(r.Entries))
		for _, entry := range r.Entries {
			fields := make([]*protocol.Response, 0, len(entry.Fields)+2)
			fields = append(fields, protocol.MakeBulkString(entry.ID))
			fieldVals := make([]*protocol.Response, 0, len(entry.Fields))
			for _, f := range entry.Fields {
				fieldVals = append(fieldVals, protocol.MakeBulkString(f))
			}
			fields = append(fields, protocol.MakeArrayResponses(fieldVals))
			entryItems = append(entryItems, protocol.MakeArrayResponses(fields))
		}
		streamItem := protocol.MakeArrayResponses([]*protocol.Response{
			protocol.MakeBulkString(r.Key),
			protocol.MakeArrayResponses(entryItems),
		})
		items = append(items, streamItem)
	}
	return protocol.MakeArrayResponses(items)
}

// XGroupCommand XGROUP 命令
type XGroupCommand struct{}

func (c *XGroupCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) < 1 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'xgroup' command"))
	}

	sub := strings.ToUpper(argString(args, 0))
	switch sub {
	case "CREATE":
		if len(args) < 4 || len(args) > 6 {
			return protocol.MakeError(errors.New("ERR wrong number of arguments for 'xgroup|create' command"))
		}
		key := argString(args, 1)
		groupName := argString(args, 2)
		startID := argString(args, 3)
		mkStream := false
		for i := 4; i < len(args); i++ {
			switch strings.ToUpper(argString(args, i)) {
			case "MKSTREAM":
				mkStream = true
			default:
				// Ignore ENTRIESREAD for now
			}
		}
		if err := db.XGroupCreate(key, groupName, startID, mkStream); err != nil {
			return protocol.MakeError(err)
		}
		return protocol.MakeSimpleString("OK")

	case "DESTROY":
		if len(args) != 3 {
			return protocol.MakeError(errors.New("ERR wrong number of arguments for 'xgroup|destroy' command"))
		}
		key := argString(args, 1)
		groupName := argString(args, 2)
		n, err := db.XGroupDestroy(key, groupName)
		if err != nil {
			return protocol.MakeError(err)
		}
		return protocol.MakeInteger(n)

	case "SETID":
		if len(args) != 4 {
			return protocol.MakeError(errors.New("ERR wrong number of arguments for 'xgroup|setid' command"))
		}
		key := argString(args, 1)
		groupName := argString(args, 2)
		lastID := argString(args, 3)
		if err := db.XGroupSetID(key, groupName, lastID); err != nil {
			return protocol.MakeError(err)
		}
		return protocol.MakeSimpleString("OK")

	default:
		return protocol.MakeError(errors.New("ERR unknown subcommand '" + sub + "'. Try XGROUP HELP."))
	}
}

// XReadGroupCommand XREADGROUP 命令
type XReadGroupCommand struct{}

func (c *XReadGroupCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	groupName, consumerName, count, blockMs, key, id, noAck, err := parseXReadGroupArgs(args)
	if err != nil {
		return protocol.MakeError(err)
	}
	_ = blockMs

	entries, err := db.XReadGroup(groupName, consumerName, key, id, count, blockMs, noAck)
	if err != nil {
		return protocol.MakeError(err)
	}
	if entries == nil || len(entries) == 0 {
		return protocol.MakeNull()
	}
	return makeXReadGroupResponse(key, entries)
}

func parseXReadGroupArgs(args [][]byte) (groupName string, consumerName string, count int, blockMs int64, key string, id string, noAck bool, err error) {
	if len(args) < 5 {
		return "", "", 0, 0, "", "", false, errors.New("ERR wrong number of arguments for 'xreadgroup' command")
	}

	i := 0
	// GROUP <group> <consumer>
	if strings.ToUpper(argString(args, i)) != "GROUP" {
		return "", "", 0, 0, "", "", false, errors.New("ERR syntax error")
	}
	i++
	if i >= len(args) {
		return "", "", 0, 0, "", "", false, errors.New("ERR syntax error")
	}
	groupName = argString(args, i)
	i++
	if i >= len(args) {
		return "", "", 0, 0, "", "", false, errors.New("ERR syntax error")
	}
	consumerName = argString(args, i)
	i++

	count = -1
	blockMs = -1
	noAck = false

	for i < len(args) {
		switch strings.ToUpper(argString(args, i)) {
		case "COUNT":
			if i+1 >= len(args) {
				return "", "", 0, 0, "", "", false, errors.New("ERR syntax error")
			}
			i++
			n, parseErr := strconv.Atoi(argString(args, i))
			if parseErr != nil || n <= 0 {
				return "", "", 0, 0, "", "", false, errors.New("ERR value is not an integer or out of range")
			}
			count = n
			i++
		case "BLOCK":
			if i+1 >= len(args) {
				return "", "", 0, 0, "", "", false, errors.New("ERR syntax error")
			}
			i++
			n, parseErr := strconv.ParseInt(argString(args, i), 10, 64)
			if parseErr != nil || n < 0 {
				return "", "", 0, 0, "", "", false, errors.New("ERR timeout is not a float or out of range")
			}
			blockMs = n
			i++
		case "NOACK":
			noAck = true
			i++
		case "STREAMS":
			goto parsedStreams
		default:
			return "", "", 0, 0, "", "", false, errors.New("ERR syntax error")
		}
	}

parsedStreams:
	if i >= len(args) || strings.ToUpper(argString(args, i)) != "STREAMS" {
		return "", "", 0, 0, "", "", false, errors.New("ERR syntax error")
	}
	i++
	remaining := len(args) - i
	if remaining != 2 {
		return "", "", 0, 0, "", "", false, errors.New("ERR Unbalanced XREADGROUP list of streams: for each stream key an ID or '>' must be specified")
	}
	key = argString(args, i)
	id = argString(args, i+1)

	return groupName, consumerName, count, blockMs, key, id, noAck, nil
}

func makeXReadGroupResponse(key string, entries []datastruct.StreamEntry) *protocol.Response {
	entryItems := make([]*protocol.Response, 0, len(entries))
	for _, entry := range entries {
		fields := make([]*protocol.Response, 0, len(entry.Fields)+2)
		fields = append(fields, protocol.MakeBulkString(entry.ID))
		fieldVals := make([]*protocol.Response, 0, len(entry.Fields))
		for _, f := range entry.Fields {
			fieldVals = append(fieldVals, protocol.MakeBulkString(f))
		}
		if entry.Fields == nil {
			fields = append(fields, protocol.MakeNullArray())
		} else {
			fields = append(fields, protocol.MakeArrayResponses(fieldVals))
		}
		entryItems = append(entryItems, protocol.MakeArrayResponses(fields))
	}
	return protocol.MakeArrayResponses([]*protocol.Response{protocol.MakeArrayResponses([]*protocol.Response{
		protocol.MakeBulkString(key),
		protocol.MakeArrayResponses(entryItems),
	})})
}

// XAckCommand XACK 命令
type XAckCommand struct{}

func (c *XAckCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) < 3 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'xack' command"))
	}
	key := argString(args, 0)
	groupName := argString(args, 1)
	ids := make([]string, 0, len(args)-2)
	for i := 2; i < len(args); i++ {
		ids = append(ids, argString(args, i))
	}
	n, err := db.XAck(key, groupName, ids)
	if err != nil {
		return protocol.MakeError(err)
	}
	return protocol.MakeInteger(n)
}

// XPendingCommand XPENDING 命令
type XPendingCommand struct{}

func (c *XPendingCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) < 2 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'xpending' command"))
	}
	key := argString(args, 0)
	groupName := argString(args, 1)

	result, err := db.XPending(key, groupName, "", "", 0, "")
	if err != nil {
		return protocol.MakeError(err)
	}
	if result == nil {
		return protocol.MakeNull()
	}

	switch v := result.(type) {
	case database.XPendingSummary:
		consumerItems := make([]*protocol.Response, 0, len(v.ConsumerMap)*2)
		for cName, count := range v.ConsumerMap {
			consumerItems = append(consumerItems, protocol.MakeArrayResponses([]*protocol.Response{
				protocol.MakeBulkString(cName),
				protocol.MakeBulkString("0"),
			}))
			_ = count
		}
		// Actually Redis XPENDING returns [total, min_id, max_id, [[consumer, count], ...]]
		detailItems := make([]*protocol.Response, 0, len(v.ConsumerMap))
		for cName, count := range v.ConsumerMap {
			detailItems = append(detailItems, protocol.MakeArrayResponses([]*protocol.Response{
				protocol.MakeBulkString(cName),
				protocol.MakeBulkString(strconv.FormatInt(count, 10)),
			}))
		}
		return protocol.MakeArrayResponses([]*protocol.Response{
			protocol.MakeInteger(v.Total),
			protocol.MakeBulkString(v.StartID),
			protocol.MakeBulkString(v.EndID),
			protocol.MakeArrayResponses(detailItems),
		})
	case []database.XPendingDetail:
		items := make([]*protocol.Response, 0, len(v))
		for _, d := range v {
			items = append(items, protocol.MakeArrayResponses([]*protocol.Response{
				protocol.MakeBulkString(d.ID),
				protocol.MakeBulkString(d.ConsumerName),
				protocol.MakeInteger(d.IdleMs),
				protocol.MakeInteger(d.DeliveryCount),
			}))
		}
		return protocol.MakeArrayResponses(items)
	default:
		return protocol.MakeNull()
	}
}
