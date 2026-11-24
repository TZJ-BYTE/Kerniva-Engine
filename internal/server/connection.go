package server

import (
	"strings"

	"github.com/TZJ-BYTE/Kerniva/internal/command"
	"github.com/TZJ-BYTE/Kerniva/internal/database"
	"github.com/TZJ-BYTE/Kerniva/internal/protocol"
)

const maxConnectionBufferedBytes = 64 << 20

type connectionRequest struct {
	name  string
	args  [][]byte
	bytes int64
	err   error
}

// Parsing stays on the network reader. Each connection executes its copied
// requests in order so blocking commands never occupy a gnet event loop.
func (ctx *ConnectionContext) enqueueRequest(req *protocol.Request, parseErr error, wait bool) bool {
	queued := connectionRequest{err: parseErr}
	if req != nil {
		queued.name = strings.Clone(req.Cmd)
		queued.args = make([][]byte, len(req.Args))
		queued.bytes = int64(len(req.Cmd) + len(req.Args)*24)
		for i, arg := range req.Args {
			queued.bytes += int64(len(arg))
			if queued.bytes > maxConnectionBufferedBytes {
				return false
			}
			queued.args[i] = append([]byte(nil), arg...)
		}
	}
	if ctx.pendingBytes.Add(queued.bytes) > maxConnectionBufferedBytes {
		ctx.pendingBytes.Add(-queued.bytes)
		return false
	}
	if wait {
		for {
			// An indefinitely blocked command cannot drain a full pipeline. Close
			// that connection so the reader can cancel its wait and release it.
			if len(ctx.requests) == cap(ctx.requests) && ctx.waiting.Load() {
				ctx.pendingBytes.Add(-queued.bytes)
				return false
			}
			select {
			case ctx.requests <- queued:
				return true
			case <-ctx.requestContext().Done():
				ctx.pendingBytes.Add(-queued.bytes)
				return false
			case <-ctx.waitStarted:
			}
		}
	}
	select {
	case <-ctx.requestContext().Done():
	case ctx.requests <- queued:
		return true
	default:
	}
	ctx.pendingBytes.Add(-queued.bytes)
	return false
}

// A successful write has finished consuming its buffer before returning.
func runConnectionCommands(ctx *ConnectionContext, manager *database.DBManager, registry *command.CommandRegistry, write func([]byte) error, closeConnection func()) {
	defer closeConnection()
	defer func() { ctx.Tx.ResetAll() }()
	for {
		var req connectionRequest
		select {
		case <-ctx.requestContext().Done():
			return
		case req = <-ctx.requests:
		}
		// Any growth beyond the connection buffer is local to this batch.
		out := ctx.respBuf[:0]
		closeAfter := false
		for {
			if ctx.requestContext().Err() != nil {
				return
			}
			// Replies preceding a blocking command must reach the client before
			// that command registers an indefinite wait.
			if isBlockingListCommand(req.name) && len(out) > 0 {
				if write(out) != nil {
					return
				}
				out = ctx.respBuf[:0]
			}
			if req.err != nil {
				out = protocol.AppendErrorString(out, req.err.Error())
				closeAfter = true
			} else {
				out, closeAfter = dispatchCommand(out, ctx, manager, registry, req.name, req.args)
			}
			ctx.pendingBytes.Add(-req.bytes)
			if ctx.requestContext().Err() != nil {
				return
			}
			if closeAfter || len(out) >= 64<<10 {
				break
			}
			select {
			case req = <-ctx.requests:
				continue
			default:
			}
			break
		}
		if len(out) > 0 && write(out) != nil {
			return
		}
		if closeAfter {
			return
		}
	}
}
