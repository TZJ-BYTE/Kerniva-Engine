package server

import (
	"errors"
	"fmt"
	"strings"

	"github.com/TZJ-BYTE/Kerniva/internal/command"
	"github.com/TZJ-BYTE/Kerniva/internal/database"
	"github.com/TZJ-BYTE/Kerniva/internal/protocol"
)

func dispatchCommand(dst []byte, ctx *ConnectionContext, manager *database.DBManager, registry *command.CommandRegistry, cmd string, args [][]byte) ([]byte, bool) {
	if manager.UsesRustMemory() {
		if ctx.requestContext().Err() != nil {
			return dst, true
		}
		return dispatchMemoryCommand(dst, ctx, manager, cmd, args)
	}
	if isBlockingListCommand(cmd) && (ctx.Tx == nil || !ctx.Tx.InMulti) {
		ctx.pubsubMu.Lock()
		subscribed := ctx.pubsubSubscriptionCountLocked() > 0
		ctx.pubsubMu.Unlock()
		if subscribed {
			return protocol.AppendErrorString(dst, "ERR blocking commands are not allowed in subscribed mode"), false
		}
		return appendBlockingListResponse(dst, ctx, cmd, args), false
	}
	if (cmd == "GET" && len(args) == 1) || (cmd == "SET" && len(args) == 2) || (cmd == "INCR" && len(args) == 1) {
		database.LockSharedExecution()
		defer database.UnlockSharedExecution()
	} else {
		database.LockExecution()
		defer database.UnlockExecution()
	}
	if ctx.requestContext().Err() != nil {
		return dst, true
	}
	return dispatchCommandLocked(dst, ctx, manager, registry, cmd, args)
}

func dispatchCommandLocked(dst []byte, ctx *ConnectionContext, manager *database.DBManager, registry *command.CommandRegistry, cmd string, args [][]byte) ([]byte, bool) {
	ctx.pubsubMu.Lock()
	subscribed := ctx.pubsubSubscriptionCountLocked() > 0
	ctx.pubsubMu.Unlock()
	if subscribed {
		switch cmd {
		case "SUBSCRIBE", "PSUBSCRIBE", "UNSUBSCRIBE", "PUNSUBSCRIBE", "QUIT":
		case "PING":
			var resp *protocol.Response
			if len(args) > 1 {
				resp = protocol.MakeError(errors.New("ERR wrong number of arguments for 'ping' command"))
			} else {
				message := ""
				if len(args) == 1 {
					message = string(args[0])
				}
				resp = protocol.MakeArray([]string{"pong", message})
			}
			dst = protocol.AppendResponse(dst, resp)
			protocol.ReleaseResponse(resp)
			return dst, false
		default:
			return protocol.AppendErrorString(dst, "ERR Can't execute '"+strings.ToLower(cmd)+"': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT are allowed in this context"), false
		}
	}
	switch cmd {
	case "MULTI":
		resp := handleMulti(ctx, args)
		dst = protocol.AppendResponse(dst, resp)
		protocol.ReleaseResponse(resp)
		return dst, false
	case "DISCARD":
		resp := handleDiscard(ctx, args)
		dst = protocol.AppendResponse(dst, resp)
		protocol.ReleaseResponse(resp)
		return dst, false
	case "WATCH":
		resp := handleWatch(ctx, args)
		dst = protocol.AppendResponse(dst, resp)
		protocol.ReleaseResponse(resp)
		return dst, false
	case "UNWATCH":
		resp := handleUnwatch(ctx, args)
		dst = protocol.AppendResponse(dst, resp)
		protocol.ReleaseResponse(resp)
		return dst, false
	case "EXEC":
		dst = appendExecResponse(dst, ctx, manager, registry, args)
		return dst, false
	}

	if ctx.Tx != nil && ctx.Tx.InMulti {
		resp := queueCommand(ctx, registry, cmd, args)
		dst = protocol.AppendResponse(dst, resp)
		protocol.ReleaseResponse(resp)
		return dst, false
	}

	switch cmd {
	case "COPY":
		resp := makeCopyResponse(manager, ctx, args)
		dst = protocol.AppendResponse(dst, resp)
		protocol.ReleaseResponse(resp)
		return dst, false
	case "MOVE":
		resp := makeMoveResponse(manager, ctx, args)
		dst = protocol.AppendResponse(dst, resp)
		protocol.ReleaseResponse(resp)
		return dst, false
	case "CLIENT":
		resp := makeClientResponse(ctx, args)
		dst = protocol.AppendResponse(dst, resp)
		protocol.ReleaseResponse(resp)
		return dst, false
	case "HELLO":
		resp := makeHelloResponse(ctx, args)
		dst = protocol.AppendResponse(dst, resp)
		protocol.ReleaseResponse(resp)
		return dst, false
	case "SUBSCRIBE":
		return appendSubscribeResponse(dst, ctx, args), false
	case "PSUBSCRIBE":
		return appendPSubscribeResponse(dst, ctx, args), false
	case "UNSUBSCRIBE":
		return appendUnsubscribeResponse(dst, ctx, args), false
	case "PUNSUBSCRIBE":
		return appendPUnsubscribeResponse(dst, ctx, args), false
	case "PUBLISH":
		return appendPublishResponse(dst, ctx, args), false
	case "PUBSUB":
		return appendPubSubInspectResponse(dst, ctx, args), false
	}

	return executeAndAppend(dst, ctx, manager, registry, cmd, args, true)
}

func executeAndAppend(dst []byte, ctx *ConnectionContext, manager *database.DBManager, registry *command.CommandRegistry, cmd string, args [][]byte, allowFastPath bool) ([]byte, bool) {
	switch cmd {
	case "SELECT":
		resp := makeSelectResponse(manager, ctx, args)
		dst = protocol.AppendResponse(dst, resp)
		protocol.ReleaseResponse(resp)
		return dst, false
	case "QUIT":
		if len(args) != 0 {
			resp := protocol.MakeError(errors.New("ERR wrong number of arguments for 'quit' command"))
			dst = protocol.AppendResponse(dst, resp)
			protocol.ReleaseResponse(resp)
			return dst, false
		}
		if ctx.Tx != nil {
			ctx.Tx.ResetAll()
		}
		resp := protocol.MakeSimpleString("OK")
		dst = protocol.AppendResponse(dst, resp)
		protocol.ReleaseResponse(resp)
		return dst, true
	}

	if allowFastPath {
		if out, ok := fastPathExecute(dst, ctx.DB, cmd, args); ok {
			return out, false
		}
	}

	resp := registry.ExecuteContext(ctx.requestContext(), ctx.DB, cmd, args)
	dst = protocol.AppendResponse(dst, resp)
	protocol.ReleaseResponse(resp)
	return dst, false
}

func handleMulti(ctx *ConnectionContext, args [][]byte) *protocol.Response {
	if len(args) != 0 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'multi' command"))
	}
	if ctx.Tx == nil {
		ctx.Tx = &TransactionState{}
	}
	if ctx.Tx.InMulti {
		return protocol.MakeError(errors.New("ERR MULTI calls can not be nested"))
	}
	ctx.Tx.InMulti = true
	ctx.Tx.Queue = ctx.Tx.Queue[:0]
	ctx.Tx.QueueFailed = false
	return protocol.MakeSimpleString("OK")
}

func handleDiscard(ctx *ConnectionContext, args [][]byte) *protocol.Response {
	if len(args) != 0 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'discard' command"))
	}
	if ctx.Tx == nil || !ctx.Tx.InMulti {
		return protocol.MakeError(errors.New("ERR DISCARD without MULTI"))
	}
	ctx.Tx.ResetAll()
	return protocol.MakeSimpleString("OK")
}

func handleWatch(ctx *ConnectionContext, args [][]byte) *protocol.Response {
	if len(args) == 0 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'watch' command"))
	}
	if ctx.Tx == nil {
		ctx.Tx = &TransactionState{}
	}
	if ctx.Tx.InMulti {
		return protocol.MakeError(errors.New("ERR WATCH inside MULTI is not allowed"))
	}

	for _, rawKey := range args {
		key := string(rawKey)
		_ = ctx.DB.Exists(key)
		replaced := false
		for i := range ctx.Tx.Watched {
			if ctx.Tx.Watched[i].DB == ctx.DB && ctx.Tx.Watched[i].Key == key {
				replaced = true
				break
			}
		}
		if replaced {
			continue
		}
		ctx.Tx.Watched = append(ctx.Tx.Watched, WatchedKey{
			DB:      ctx.DB,
			Key:     key,
			Version: ctx.DB.WatchKeyVersion(key),
			Epoch:   ctx.DB.ModEpoch(),
		})
	}
	return protocol.MakeSimpleString("OK")
}

func handleUnwatch(ctx *ConnectionContext, args [][]byte) *protocol.Response {
	if len(args) != 0 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'unwatch' command"))
	}
	ctx.Tx.ResetWatches()
	return protocol.MakeSimpleString("OK")
}

func queueCommand(ctx *ConnectionContext, registry *command.CommandRegistry, cmd string, args [][]byte) *protocol.Response {
	switch cmd {
	case "MULTI":
		ctx.Tx.QueueFailed = true
		return protocol.MakeError(errors.New("ERR MULTI calls can not be nested"))
	case "WATCH":
		ctx.Tx.QueueFailed = true
		return protocol.MakeError(errors.New("ERR WATCH inside MULTI is not allowed"))
	case "SELECT", "QUIT":
		ctx.Tx.QueueFailed = true
		return protocol.MakeError(fmt.Errorf("ERR %s inside MULTI is not allowed", strings.ToUpper(cmd)))
	case "SUBSCRIBE", "PSUBSCRIBE", "UNSUBSCRIBE", "PUNSUBSCRIBE", "PUBLISH", "PUBSUB":
		ctx.Tx.QueueFailed = true
		return protocol.MakeError(fmt.Errorf("ERR %s inside MULTI is not allowed", strings.ToUpper(cmd)))
	}

	if _, ok := registry.Get(cmd); !ok {
		ctx.Tx.QueueFailed = true
		return protocol.MakeError(fmt.Errorf("ERR unknown command '%s'", strings.ToLower(cmd)))
	}
	if invalid := registry.Validate(cmd, args); invalid != nil {
		ctx.Tx.QueueFailed = true
		return invalid
	}
	commandBytes := len(cmd) + len(args)*24
	for _, arg := range args {
		commandBytes += len(arg)
	}
	if len(ctx.Tx.Queue) >= 10000 || ctx.Tx.QueueBytes+commandBytes > 16<<20 {
		ctx.Tx.QueueFailed = true
		return protocol.MakeError(errors.New("ERR transaction queue exceeds size limit"))
	}
	ctx.Tx.QueueBytes += commandBytes

	ctx.Tx.Queue = append(ctx.Tx.Queue, cloneQueuedCommand(ctx.DB, cmd, args))
	return protocol.MakeSimpleString("QUEUED")
}

func appendExecResponse(dst []byte, ctx *ConnectionContext, manager *database.DBManager, registry *command.CommandRegistry, args [][]byte) []byte {
	if len(args) != 0 {
		resp := protocol.MakeError(errors.New("ERR wrong number of arguments for 'exec' command"))
		dst = protocol.AppendResponse(dst, resp)
		protocol.ReleaseResponse(resp)
		return dst
	}
	if ctx.Tx == nil || !ctx.Tx.InMulti {
		resp := protocol.MakeError(errors.New("ERR EXEC without MULTI"))
		dst = protocol.AppendResponse(dst, resp)
		protocol.ReleaseResponse(resp)
		return dst
	}
	defer ctx.Tx.ResetAll()

	if ctx.Tx.QueueFailed {
		resp := protocol.MakeError(errors.New("EXECABORT Transaction discarded because of previous errors."))
		dst = protocol.AppendResponse(dst, resp)
		protocol.ReleaseResponse(resp)
		return dst
	}
	if watchedKeysDirty(ctx) {
		resp := protocol.MakeNullArray()
		dst = protocol.AppendResponse(dst, resp)
		protocol.ReleaseResponse(resp)
		return dst
	}

	items := make([]*protocol.Response, 0, len(ctx.Tx.Queue))
	for _, queued := range ctx.Tx.Queue {
		items = append(items, executeQueuedCommand(ctx, manager, registry, queued))
	}

	resp := protocol.MakeArrayResponses(items)
	dst = protocol.AppendResponse(dst, resp)
	for _, item := range items {
		protocol.ReleaseResponse(item)
	}
	protocol.ReleaseResponse(resp)
	return dst
}

func executeQueuedCommand(ctx *ConnectionContext, manager *database.DBManager, registry *command.CommandRegistry, queued QueuedCommand) *protocol.Response {
	switch queued.Cmd {
	case "SELECT":
		return protocol.MakeError(errors.New("ERR SELECT inside MULTI is not allowed"))
	case "QUIT":
		return protocol.MakeError(errors.New("ERR QUIT inside MULTI is not allowed"))
	case "COPY":
		return makeCopyResponse(manager, &ConnectionContext{DB: queued.DB}, queued.Args)
	case "MOVE":
		return makeMoveResponse(manager, &ConnectionContext{DB: queued.DB}, queued.Args)
	case "BLPOP", "BRPOP", "BRPOPLPUSH", "BLMOVE", "BLMPOP", "XREAD", "XREADGROUP":
		return executeQueuedBlockingCommand(&ConnectionContext{DB: queued.DB}, queued)
	case "CLIENT":
		return makeClientResponse(ctx, queued.Args)
	default:
		return registry.ExecuteContext(ctx.requestContext(), queued.DB, queued.Cmd, queued.Args)
	}
}

func watchedKeysDirty(ctx *ConnectionContext) bool {
	if ctx.Tx == nil {
		return false
	}
	for _, watched := range ctx.Tx.Watched {
		if watched.DB == nil {
			continue
		}
		_ = watched.DB.Exists(watched.Key)
		if watched.Epoch != watched.DB.ModEpoch() || watched.Version != watched.DB.KeyVersion(watched.Key) {
			return true
		}
	}
	return false
}

func makeSelectResponse(manager *database.DBManager, ctx *ConnectionContext, args [][]byte) *protocol.Response {
	if len(args) != 1 {
		return protocol.MakeError(fmt.Errorf("ERR wrong number of arguments for 'select' command"))
	}

	index, err := protocol.ParseInt(args[0])
	if err != nil {
		return protocol.MakeError(fmt.Errorf("ERR invalid DB index"))
	}

	newDB, err := manager.GetDBByIndex(index)
	if err != nil {
		return protocol.MakeError(fmt.Errorf("ERR DB index is out of range"))
	}

	ctx.DB = newDB
	return protocol.MakeSimpleString("OK")
}

func makeCopyResponse(manager *database.DBManager, ctx *ConnectionContext, args [][]byte) *protocol.Response {
	if len(args) < 2 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'copy' command"))
	}

	sourceKey := string(args[0])
	destKey := string(args[1])
	targetDB := ctx.DB
	replace := false

	for i := 2; i < len(args); i++ {
		switch {
		case strings.EqualFold(string(args[i]), "DB"):
			if i+1 >= len(args) {
				return protocol.MakeError(errors.New("ERR syntax error"))
			}
			index, err := protocol.ParseInt(args[i+1])
			if err != nil {
				return protocol.MakeError(errors.New("ERR DB index is out of range"))
			}
			db, err := manager.GetDBByIndex(index)
			if err != nil {
				return protocol.MakeError(errors.New("ERR DB index is out of range"))
			}
			targetDB = db
			i++
		case strings.EqualFold(string(args[i]), "REPLACE"):
			replace = true
		default:
			return protocol.MakeError(errors.New("ERR syntax error"))
		}
	}

	if targetDB == ctx.DB && sourceKey == destKey {
		return protocol.MakeError(errors.New("ERR source and destination objects are the same"))
	}

	ok, err := ctx.DB.CopyKeyTo(targetDB, sourceKey, destKey, replace)
	if err != nil {
		return protocol.MakeError(err)
	}
	if ok {
		return protocol.MakeInteger(1)
	}
	return protocol.MakeInteger(0)
}

func makeMoveResponse(manager *database.DBManager, ctx *ConnectionContext, args [][]byte) *protocol.Response {
	if len(args) != 2 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'move' command"))
	}

	index, err := protocol.ParseInt(args[1])
	if err != nil {
		return protocol.MakeError(errors.New("ERR DB index is out of range"))
	}
	targetDB, err := manager.GetDBByIndex(index)
	if err != nil {
		return protocol.MakeError(errors.New("ERR DB index is out of range"))
	}
	if targetDB == ctx.DB {
		return protocol.MakeError(errors.New("ERR source and destination objects are the same"))
	}

	key := string(args[0])
	ok, err := ctx.DB.CopyKeyTo(targetDB, key, key, false)
	if err != nil {
		return protocol.MakeError(err)
	}
	if !ok {
		return protocol.MakeInteger(0)
	}
	deleted, err := ctx.DB.DeleteWithError(key)
	if err != nil {
		if _, rollbackErr := targetDB.DeleteWithError(key); rollbackErr != nil {
			return protocol.MakeError(fmt.Errorf("ERR MOVE source deletion failed: %v, destination rollback failed: %v", err, rollbackErr))
		}
		return protocol.MakeError(err)
	}
	if !deleted {
		if _, err := targetDB.DeleteWithError(key); err != nil {
			return protocol.MakeError(err)
		}
		return protocol.MakeInteger(0)
	}
	return protocol.MakeInteger(1)
}

func appendSubscribeResponse(dst []byte, ctx *ConnectionContext, args [][]byte) []byte {
	if len(args) == 0 {
		resp := protocol.MakeError(errors.New("ERR wrong number of arguments for 'subscribe' command"))
		dst = protocol.AppendResponse(dst, resp)
		protocol.ReleaseResponse(resp)
		return dst
	}

	if ctx.pubsubHub == nil || ctx.pubsubWriter == nil {
		resp := protocol.MakeError(errPubSubWriterUnavailable)
		dst = protocol.AppendResponse(dst, resp)
		protocol.ReleaseResponse(resp)
		return dst
	}

	for _, ack := range ctx.pubsubHub.Subscribe(ctx, args) {
		dst = appendPubSubAck(dst, ack)
	}
	return dst
}

func appendUnsubscribeResponse(dst []byte, ctx *ConnectionContext, args [][]byte) []byte {
	if ctx.pubsubHub == nil || ctx.pubsubWriter == nil {
		resp := protocol.MakeError(errPubSubWriterUnavailable)
		dst = protocol.AppendResponse(dst, resp)
		protocol.ReleaseResponse(resp)
		return dst
	}

	for _, ack := range ctx.pubsubHub.Unsubscribe(ctx, args) {
		dst = appendPubSubAck(dst, ack)
	}
	return dst
}

func appendPSubscribeResponse(dst []byte, ctx *ConnectionContext, args [][]byte) []byte {
	if len(args) == 0 {
		resp := protocol.MakeError(errors.New("ERR wrong number of arguments for 'psubscribe' command"))
		dst = protocol.AppendResponse(dst, resp)
		protocol.ReleaseResponse(resp)
		return dst
	}

	if ctx.pubsubHub == nil || ctx.pubsubWriter == nil {
		resp := protocol.MakeError(errPubSubWriterUnavailable)
		dst = protocol.AppendResponse(dst, resp)
		protocol.ReleaseResponse(resp)
		return dst
	}

	for _, ack := range ctx.pubsubHub.PSubscribe(ctx, args) {
		dst = appendPubSubAck(dst, ack)
	}
	return dst
}

func appendPUnsubscribeResponse(dst []byte, ctx *ConnectionContext, args [][]byte) []byte {
	if ctx.pubsubHub == nil || ctx.pubsubWriter == nil {
		resp := protocol.MakeError(errPubSubWriterUnavailable)
		dst = protocol.AppendResponse(dst, resp)
		protocol.ReleaseResponse(resp)
		return dst
	}

	for _, ack := range ctx.pubsubHub.PUnsubscribe(ctx, args) {
		dst = appendPubSubAck(dst, ack)
	}
	return dst
}

func appendPublishResponse(dst []byte, ctx *ConnectionContext, args [][]byte) []byte {
	if len(args) != 2 {
		resp := protocol.MakeError(errors.New("ERR wrong number of arguments for 'publish' command"))
		dst = protocol.AppendResponse(dst, resp)
		protocol.ReleaseResponse(resp)
		return dst
	}

	var delivered int64
	if ctx.pubsubHub != nil {
		delivered = ctx.pubsubHub.Publish(string(args[0]), args[1])
	}
	resp := protocol.MakeInteger(delivered)
	dst = protocol.AppendResponse(dst, resp)
	protocol.ReleaseResponse(resp)
	return dst
}

func appendPubSubInspectResponse(dst []byte, ctx *ConnectionContext, args [][]byte) []byte {
	if len(args) == 0 {
		resp := protocol.MakeError(errors.New("ERR wrong number of arguments for 'pubsub' command"))
		dst = protocol.AppendResponse(dst, resp)
		protocol.ReleaseResponse(resp)
		return dst
	}
	if ctx.pubsubHub == nil {
		resp := protocol.MakeArrayResponses([]*protocol.Response{})
		dst = protocol.AppendResponse(dst, resp)
		protocol.ReleaseResponse(resp)
		return dst
	}

	subcommand := strings.ToUpper(string(args[0]))
	switch subcommand {
	case "CHANNELS":
		if len(args) > 2 {
			resp := protocol.MakeError(errors.New("ERR wrong number of arguments for 'pubsub|channels' command"))
			dst = protocol.AppendResponse(dst, resp)
			protocol.ReleaseResponse(resp)
			return dst
		}

		pattern := ""
		if len(args) == 2 {
			pattern = string(args[1])
		}

		channels, err := ctx.pubsubHub.Channels(pattern)
		if err != nil {
			resp := protocol.MakeError(errors.New("ERR invalid pattern"))
			dst = protocol.AppendResponse(dst, resp)
			protocol.ReleaseResponse(resp)
			return dst
		}

		items := make([]*protocol.Response, 0, len(channels))
		for _, channel := range channels {
			items = append(items, protocol.MakeBulkString(channel))
		}
		resp := protocol.MakeArrayResponses(items)
		dst = protocol.AppendResponse(dst, resp)
		for _, item := range items {
			protocol.ReleaseResponse(item)
		}
		protocol.ReleaseResponse(resp)
		return dst
	case "NUMSUB":
		if len(args) < 2 {
			resp := protocol.MakeError(errors.New("ERR wrong number of arguments for 'pubsub|numsub' command"))
			dst = protocol.AppendResponse(dst, resp)
			protocol.ReleaseResponse(resp)
			return dst
		}

		counts := ctx.pubsubHub.NumSub(args[1:])
		items := make([]*protocol.Response, 0, len(counts)*2)
		for i, count := range counts {
			items = append(items, protocol.MakeBulkString(string(args[i+1])))
			items = append(items, protocol.MakeInteger(count))
		}
		resp := protocol.MakeArrayResponses(items)
		dst = protocol.AppendResponse(dst, resp)
		for _, item := range items {
			protocol.ReleaseResponse(item)
		}
		protocol.ReleaseResponse(resp)
		return dst
	default:
		resp := protocol.MakeError(errors.New("ERR syntax error"))
		dst = protocol.AppendResponse(dst, resp)
		protocol.ReleaseResponse(resp)
		return dst
	}
}
