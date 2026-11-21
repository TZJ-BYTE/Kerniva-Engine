package server

import (
	"errors"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/TZJ-BYTE/Kerniva/internal/database"
	"github.com/TZJ-BYTE/Kerniva/internal/datastruct"
	"github.com/TZJ-BYTE/Kerniva/internal/protocol"
)

type blockingPlan struct {
	wait      bool
	timeout   time.Duration
	nullArray bool
	try       func() (*protocol.Response, error)
}

func appendBlockingListResponse(dst []byte, ctx *ConnectionContext, cmd string, args [][]byte) []byte {
	resp := executeBlockingCommand(ctx, cmd, args, false, false)
	dst = protocol.AppendResponse(dst, resp)
	protocol.ReleaseResponse(resp)
	return dst
}

// The change channel is captured under the same execution gate as the empty
// read. A writer cannot slip between checking the data and registering a wait.
func executeBlockingCommand(ctx *ConnectionContext, cmd string, args [][]byte, nonBlocking, executionLocked bool) *protocol.Response {
	var plan *blockingPlan
	var timeout <-chan time.Time
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for {
		if !executionLocked {
			database.LockExecution()
		}
		if ctx.requestContext().Err() != nil {
			if !executionLocked {
				database.UnlockExecution()
			}
			return protocol.MakeError(ctx.requestContext().Err())
		}
		if plan == nil {
			parsed, err := makeBlockingPlan(ctx, cmd, args)
			if err != nil {
				if !executionLocked {
					database.UnlockExecution()
				}
				return protocol.MakeError(err)
			}
			plan = parsed
			if plan.timeout > 0 {
				timer = time.NewTimer(plan.timeout)
				timeout = timer.C
			}
		}
		changed := ctx.DB.ChangeChannel()
		resp, err := plan.try()
		if !executionLocked {
			database.UnlockExecution()
		}
		if err != nil {
			return protocol.MakeError(err)
		}
		if resp != nil {
			return resp
		}
		empty := func() *protocol.Response {
			if plan.nullArray {
				return protocol.MakeNullArray()
			}
			return protocol.MakeNull()
		}
		if nonBlocking || !plan.wait {
			return empty()
		}
		ctx.waiting.Store(true)
		select {
		case ctx.waitStarted <- struct{}{}:
		default:
		}
		select {
		case <-changed:
		case <-timeout:
			ctx.waiting.Store(false)
			return empty()
		case <-ctx.requestContext().Done():
			ctx.waiting.Store(false)
			return protocol.MakeError(ctx.requestContext().Err())
		}
		ctx.waiting.Store(false)
	}
}

func makeBlockingPlan(ctx *ConnectionContext, cmd string, args [][]byte) (*blockingPlan, error) {
	plan := &blockingPlan{wait: true}
	switch cmd {
	case "BLPOP", "BRPOP":
		if len(args) < 2 {
			return nil, errors.New("ERR wrong number of arguments for '" + strings.ToLower(cmd) + "' command")
		}
		timeout, err := parseBlockTimeout(args[len(args)-1])
		if err != nil {
			return nil, err
		}
		plan.timeout, plan.nullArray = timeout, true
		keys := make([]string, len(args)-1)
		for i := range keys {
			keys[i] = string(args[i])
		}
		plan.try = func() (*protocol.Response, error) {
			for _, key := range keys {
				var value string
				var ok bool
				var err error
				if cmd == "BRPOP" {
					value, ok, err = ctx.DB.RPop(key)
				} else {
					value, ok, err = ctx.DB.LPop(key)
				}
				if err != nil {
					return nil, err
				}
				if ok {
					return protocol.MakeArray([]string{key, value}), nil
				}
			}
			return nil, nil
		}
	case "BRPOPLPUSH", "BLMOVE":
		required := 3
		if cmd == "BLMOVE" {
			required = 5
		}
		if len(args) != required {
			return nil, errors.New("ERR wrong number of arguments for '" + strings.ToLower(cmd) + "' command")
		}
		timeout, err := parseBlockTimeout(args[len(args)-1])
		if err != nil {
			return nil, err
		}
		plan.timeout = timeout
		fromLeft, toLeft := false, true
		if cmd == "BLMOVE" {
			fromLeft, err = parseBlockingListSide(args[2])
			if err != nil {
				return nil, err
			}
			toLeft, err = parseBlockingListSide(args[3])
			if err != nil {
				return nil, err
			}
		}
		source, destination := string(args[0]), string(args[1])
		plan.try = func() (*protocol.Response, error) {
			value, ok, err := ctx.DB.LMove(source, destination, fromLeft, toLeft)
			if err != nil || !ok {
				return nil, err
			}
			return protocol.MakeBulkString(value), nil
		}
	case "BLMPOP":
		if len(args) < 4 {
			return nil, errors.New("ERR wrong number of arguments for 'blmpop' command")
		}
		timeout, err := parseBlockTimeout(args[0])
		if err != nil {
			return nil, err
		}
		keys, fromLeft, count, err := parseBlockingListMPopArgs("blmpop", args[1:])
		if err != nil {
			return nil, err
		}
		plan.timeout, plan.nullArray = timeout, true
		plan.try = func() (*protocol.Response, error) {
			key, values, ok, err := ctx.DB.LMPop(keys, fromLeft, count)
			if err != nil || !ok {
				return nil, err
			}
			return makeBlockingListKeyArrayResponse(key, values), nil
		}
	case "XREAD":
		count, blockMs, keys, ids, err := parseXReadBlockingArgs(args)
		if err != nil {
			return nil, err
		}
		plan.wait, plan.nullArray = blockMs >= 0, true
		if blockMs > 0 {
			plan.timeout = time.Duration(blockMs) * time.Millisecond
		}
		// Resolve "$" once, at registration, so new entries remain visible.
		for i, id := range ids {
			if id != "$" {
				continue
			}
			entries, err := ctx.DB.XRange(keys[i], "+", "-", 1, true)
			if err != nil {
				return nil, err
			}
			ids[i] = "0-0"
			if len(entries) > 0 {
				ids[i] = entries[0].ID
			}
		}
		plan.try = func() (*protocol.Response, error) {
			result, err := ctx.DB.XRead(keys, ids, count)
			if err != nil || len(result) == 0 {
				return nil, err
			}
			return makeXReadBlockingResponse(result), nil
		}
	case "XREADGROUP":
		group, consumer, count, blockMs, key, id, noAck, err := parseXReadGroupBlockingArgs(args)
		if err != nil {
			return nil, err
		}
		plan.wait, plan.nullArray = blockMs >= 0 && id == ">", true
		if blockMs > 0 {
			plan.timeout = time.Duration(blockMs) * time.Millisecond
		}
		plan.try = func() (*protocol.Response, error) {
			entries, err := ctx.DB.XReadGroup(group, consumer, key, id, count, 0, noAck)
			if err != nil || len(entries) == 0 {
				return nil, err
			}
			return makeXReadGroupBlockingResponse(key, entries), nil
		}
	default:
		return nil, errors.New("ERR unknown blocking command")
	}
	return plan, nil
}

func executeQueuedBlockingCommand(ctx *ConnectionContext, queued QueuedCommand) *protocol.Response {
	return executeBlockingCommand(ctx, queued.Cmd, queued.Args, true, true)
}

func executeXReadBlocking(ctx *ConnectionContext, args [][]byte) *protocol.Response {
	return executeBlockingCommand(ctx, "XREAD", args, false, false)
}

func executeXReadGroupBlocking(ctx *ConnectionContext, args [][]byte) *protocol.Response {
	return executeBlockingCommand(ctx, "XREADGROUP", args, false, false)
}
func parseBlockTimeout(arg []byte) (time.Duration, error) {
	value, err := strconv.ParseFloat(string(arg), 64)
	if err != nil || math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || value >= float64(math.MaxInt64)/float64(time.Second) {
		return 0, errors.New("ERR timeout is not a float or out of range")
	}
	if value == 0 {
		return 0, nil
	}
	return time.Duration(value * float64(time.Second)), nil
}

func isBlockingListCommand(cmd string) bool {
	switch cmd {
	case "BLPOP", "BRPOP", "BRPOPLPUSH", "BLMOVE", "BLMPOP", "XREAD", "XREADGROUP":
		return true
	default:
		return false
	}
}

func parseBlockingListSide(b []byte) (bool, error) {
	switch strings.ToUpper(string(b)) {
	case "LEFT":
		return true, nil
	case "RIGHT":
		return false, nil
	default:
		return false, errors.New("ERR syntax error")
	}
}

func parseBlockingListMPopArgs(commandName string, args [][]byte) ([]string, bool, int, error) {
	if len(args) < 3 {
		return nil, false, 0, errors.New("ERR wrong number of arguments for '" + commandName + "' command")
	}

	numKeys, err := protocol.ParseInt(args[0])
	if err != nil {
		return nil, false, 0, errors.New("ERR value is not an integer or out of range")
	}
	if numKeys <= 0 {
		return nil, false, 0, errors.New("ERR numkeys should be greater than 0")
	}
	if len(args) < numKeys+2 {
		return nil, false, 0, errors.New("ERR wrong number of arguments for '" + commandName + "' command")
	}

	keys := make([]string, 0, numKeys)
	for i := 0; i < numKeys; i++ {
		keys = append(keys, string(args[i+1]))
	}

	fromLeft, err := parseBlockingListSide(args[numKeys+1])
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
	if strings.ToUpper(string(args[numKeys+2])) != "COUNT" {
		return nil, false, 0, errors.New("ERR syntax error")
	}

	count, err = protocol.ParseInt(args[numKeys+3])
	if err != nil {
		return nil, false, 0, errors.New("ERR value is not an integer or out of range")
	}
	if count <= 0 {
		return nil, false, 0, errors.New("ERR count should be greater than 0")
	}
	return keys, fromLeft, count, nil
}

func makeBlockingListKeyArrayResponse(key string, values []string) *protocol.Response {
	return protocol.MakeArrayResponses([]*protocol.Response{
		protocol.MakeBulkString(key),
		protocol.MakeArray(values),
	})
}

func parseXReadBlockingArgs(args [][]byte) (count int, blockMs int64, keys []string, ids []string, err error) {
	count = -1
	blockMs = -1
	i := 0
	for i < len(args) {
		switch strings.ToUpper(string(args[i])) {
		case "COUNT":
			if i+1 >= len(args) {
				return 0, 0, nil, nil, errors.New("ERR syntax error")
			}
			i++
			n, parseErr := strconv.Atoi(string(args[i]))
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
			n, parseErr := strconv.ParseInt(string(args[i]), 10, 64)
			if parseErr != nil || n < 0 || n > math.MaxInt64/int64(time.Millisecond) {
				return 0, 0, nil, nil, errors.New("ERR timeout is not a float or out of range")
			}
			blockMs = n
			i++
		default:
			goto parsedOptions
		}
	}

parsedOptions:
	if i >= len(args) || strings.ToUpper(string(args[i])) != "STREAMS" {
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
		key := string(args[i+j])
		keys = append(keys, key)
		id := string(args[i+n+j])
		ids = append(ids, id)
	}
	return count, blockMs, keys, ids, nil
}

func makeXReadBlockingResponse(results []database.XReadResult) *protocol.Response {
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

func parseXReadGroupBlockingArgs(args [][]byte) (groupName string, consumerName string, count int, blockMs int64, keyToRead string, id string, noAck bool, err error) {
	if len(args) < 5 {
		return "", "", 0, 0, "", "", false, errors.New("ERR wrong number of arguments for 'xreadgroup' command")
	}

	i := 0
	if strings.ToUpper(string(args[i])) != "GROUP" {
		return "", "", 0, 0, "", "", false, errors.New("ERR syntax error")
	}
	i++
	if i >= len(args) {
		return "", "", 0, 0, "", "", false, errors.New("ERR syntax error")
	}
	groupName = string(args[i])
	i++
	if i >= len(args) {
		return "", "", 0, 0, "", "", false, errors.New("ERR syntax error")
	}
	consumerName = string(args[i])
	i++

	count = -1
	blockMs = -1
	noAck = false

	for i < len(args) {
		switch strings.ToUpper(string(args[i])) {
		case "COUNT":
			if i+1 >= len(args) {
				return "", "", 0, 0, "", "", false, errors.New("ERR syntax error")
			}
			i++
			n, parseErr := strconv.Atoi(string(args[i]))
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
			n, parseErr := strconv.ParseInt(string(args[i]), 10, 64)
			if parseErr != nil || n < 0 || n > math.MaxInt64/int64(time.Millisecond) {
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
	if i >= len(args) || strings.ToUpper(string(args[i])) != "STREAMS" {
		return "", "", 0, 0, "", "", false, errors.New("ERR syntax error")
	}
	i++
	remaining := len(args) - i
	if remaining != 2 {
		return "", "", 0, 0, "", "", false, errors.New("ERR Unbalanced XREADGROUP list of streams: for each stream key an ID or '>' must be specified")
	}
	keyToRead = string(args[i])
	id = string(args[i+1])

	return groupName, consumerName, count, blockMs, keyToRead, id, noAck, nil
}

func makeXReadGroupBlockingResponse(key string, entries []datastruct.StreamEntry) *protocol.Response {
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
