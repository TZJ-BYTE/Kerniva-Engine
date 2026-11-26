package server

import (
	"net"
	"strings"

	"github.com/TZJ-BYTE/Kerniva/internal/command"
	"github.com/TZJ-BYTE/Kerniva/internal/database"
	"github.com/TZJ-BYTE/Kerniva/internal/protocol"
	"github.com/TZJ-BYTE/Kerniva/internal/writetrace"
)

const maxConnectionBufferedBytes = 64 << 20

type connectionRequest struct {
	name    string
	args    [][]byte
	bytes   int64
	err     error
	ordinal uint64
	parsed  int64
}

func (ctx *ConnectionContext) initWriteTrace(remote net.Addr) {
	if trace := writetrace.Active(); trace != nil {
		if tcp, ok := remote.(*net.TCPAddr); ok {
			ctx.trace = trace
			ctx.tracePort = tcp.Port
		}
	}
}

// Parsing stays on the network reader. Each connection executes its copied
// requests in order so blocking commands never occupy a gnet event loop.
func (ctx *ConnectionContext) enqueueRequest(req *protocol.Request, parseErr error, wait bool) bool {
	queued := connectionRequest{err: parseErr}
	if req != nil {
		if ctx.trace != nil {
			ctx.traceOrdinal++
			if (req.Cmd == "SET" && len(req.Args) == 2) || req.Cmd == "INFO" || (req.Cmd == "SELECT" && len(req.Args) == 1) {
				queued.ordinal = ctx.traceOrdinal
				queued.parsed = ctx.trace.Stamp()
			}
		}
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
	var tracedReplies []*writetrace.Request
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
				if writeConnectionReplies(write, out, tracedReplies) != nil {
					return
				}
				tracedReplies = tracedReplies[:0]
				out = ctx.respBuf[:0]
			}
			if req.err != nil {
				out = protocol.AppendErrorString(out, req.err.Error())
				closeAfter = true
			} else {
				if req.ordinal != 0 {
					ctx.traceRequest = ctx.trace.Begin(ctx.tracePort, req.ordinal, req.name, req.parsed)
				}
				out, closeAfter = dispatchCommand(out, ctx, manager, registry, req.name, req.args)
				if row := ctx.traceRequest; row != nil {
					row.Encode = row.Stamp()
					tracedReplies = append(tracedReplies, row)
					ctx.traceRequest = nil
				}
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
		if len(out) > 0 && writeConnectionReplies(write, out, tracedReplies) != nil {
			return
		}
		tracedReplies = tracedReplies[:0]
		if closeAfter {
			return
		}
	}
}

func writeConnectionReplies(write func([]byte) error, out []byte, rows []*writetrace.Request) error {
	if len(rows) == 0 {
		return write(out)
	}
	start := rows[0].Stamp()
	err := write(out)
	end := rows[0].Stamp()
	for _, row := range rows {
		row.Send, row.Sent, row.SendOK = start, end, err == nil
	}
	return err
}
