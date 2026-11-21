package server

import (
	"errors"
	"fmt"
	"strings"

	"github.com/TZJ-BYTE/Kerniva/internal/command"
	"github.com/TZJ-BYTE/Kerniva/internal/protocol"
)

func validClientName(name string) bool {
	return strings.IndexFunc(name, func(r rune) bool { return r <= ' ' || r == 127 }) < 0
}

func makeClientResponse(ctx *ConnectionContext, args [][]byte) *protocol.Response {
	if len(args) == 0 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'client' command"))
	}
	sub := strings.ToUpper(string(args[0]))
	switch sub {
	case "SETNAME":
		if len(args) != 2 {
			break
		}
		name := string(args[1])
		if !validClientName(name) {
			return protocol.MakeError(errors.New("ERR Client names cannot contain spaces, newlines or special characters"))
		}
		ctx.ClientName = name
		return protocol.MakeSimpleString("OK")
	case "GETNAME":
		if len(args) != 1 {
			break
		}
		if ctx.ClientName == "" {
			return protocol.MakeNull()
		}
		return protocol.MakeBulkString(ctx.ClientName)
	case "ID":
		if len(args) != 1 {
			break
		}
		return protocol.MakeInteger(ctx.ClientID)
	case "INFO":
		if len(args) != 1 {
			break
		}
		return protocol.MakeBulkString(fmt.Sprintf("id=%d name=%s db=%v lib-name=%s lib-ver=%s", ctx.ClientID, ctx.ClientName, ctx.DB.GetStats()["db_id"], ctx.LibraryName, ctx.LibraryVersion))
	case "SETINFO":
		resp := (&command.ClientCommand{}).Execute(ctx.DB, args)
		if resp.Type == protocol.Error {
			return resp
		}
		if strings.EqualFold(string(args[1]), "LIB-NAME") {
			ctx.LibraryName = string(args[2])
		} else {
			ctx.LibraryVersion = string(args[2])
		}
		return resp
	default:
		return (&command.ClientCommand{}).Execute(ctx.DB, args)
	}
	return protocol.MakeError(fmt.Errorf("ERR wrong number of arguments for 'client|%s' command", strings.ToLower(sub)))
}

func makeHelloResponse(ctx *ConnectionContext, args [][]byte) *protocol.Response {
	resp := (&command.HelloCommand{}).Execute(ctx.DB, args)
	if resp.Type == protocol.Error {
		return resp
	}
	for i := 1; i+1 < len(args); i++ {
		if !strings.EqualFold(string(args[i]), "SETNAME") {
			continue
		}
		name := string(args[i+1])
		if !validClientName(name) {
			protocol.ReleaseResponse(resp)
			return protocol.MakeError(errors.New("ERR Invalid client name"))
		}
		ctx.ClientName = name
		i++
	}
	items := resp.Value.([]*protocol.Response)
	protocol.ReleaseResponse(items[7])
	items[7] = protocol.MakeInteger(ctx.ClientID)
	return resp
}
