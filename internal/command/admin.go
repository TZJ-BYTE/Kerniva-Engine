package command

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/TZJ-BYTE/Kerniva/internal/database"
	"github.com/TZJ-BYTE/Kerniva/internal/protocol"
)

type TimeCommand struct{}

func (c *TimeCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 0 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'time' command"))
	}
	now := time.Now()
	return protocol.MakeArray([]string{
		strconv.FormatInt(now.Unix(), 10),
		strconv.FormatInt(int64(now.Nanosecond()/1000), 10),
	})
}

type QuitCommand struct{}

func (c *QuitCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 0 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'quit' command"))
	}
	return protocol.MakeSimpleString("OK")
}

type CommandCommand struct{}

func (c *CommandCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) == 0 {
		names := make([]string, 0, len(DefaultRegistry.Commands()))
		for name := range DefaultRegistry.Commands() {
			names = append(names, name)
		}
		sort.Strings(names)
		items := make([]*protocol.Response, 0, len(names))
		for _, name := range names {
			items = append(items, commandInfo(name))
		}
		return protocol.MakeArrayResponses(items)
	}

	sub := strings.ToUpper(argString(args, 0))
	switch sub {
	case "COUNT":
		if len(args) != 1 {
			return protocol.MakeError(errors.New("ERR wrong number of arguments for 'command|count' command"))
		}
		return protocol.MakeInteger(int64(len(DefaultRegistry.Commands())))
	case "INFO":
		resp := make([]*protocol.Response, 0, len(args)-1)
		for i := 1; i < len(args); i++ {
			cmdName := strings.ToUpper(argString(args, i))
			if _, ok := DefaultRegistry.Get(cmdName); ok {
				resp = append(resp, commandInfo(cmdName))
			} else {
				resp = append(resp, protocol.MakeNull())
			}
		}
		return protocol.MakeArrayResponses(resp)
	case "DOCS":
		return protocol.MakeError(errors.New("ERR COMMAND DOCS is not supported"))
	case "GETKEYS":
		return commandGetKeys(args[1:])
	default:
		return protocol.MakeError(errors.New("ERR syntax error"))
	}
}

type HelloCommand struct{}

func (c *HelloCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) > 0 {
		version, err := protocol.ParseInt(args[0])
		if err != nil || version != 2 {
			return protocol.MakeError(errors.New("NOPROTO unsupported protocol version"))
		}
		for i := 1; i < len(args); i++ {
			switch {
			case equalsIgnoreCase(args[i], []byte("AUTH")):
				if i+2 >= len(args) {
					return protocol.MakeError(errors.New("ERR syntax error"))
				}
				return protocol.MakeError(errors.New("ERR AUTH is not configured"))
			case equalsIgnoreCase(args[i], []byte("SETNAME")):
				if i+1 >= len(args) {
					return protocol.MakeError(errors.New("ERR syntax error"))
				}
				i++
			default:
				return protocol.MakeError(errors.New("ERR syntax error"))
			}
		}
	}

	return protocol.MakeArrayResponses([]*protocol.Response{
		protocol.MakeBulkString("server"), protocol.MakeBulkString("kerniva"),
		protocol.MakeBulkString("version"), protocol.MakeBulkString("development"),
		protocol.MakeBulkString("proto"), protocol.MakeInteger(2),
		protocol.MakeBulkString("id"), protocol.MakeInteger(0),
		protocol.MakeBulkString("mode"), protocol.MakeBulkString("standalone"),
		protocol.MakeBulkString("role"), protocol.MakeBulkString("master"),
		protocol.MakeBulkString("modules"), protocol.MakeArrayResponses([]*protocol.Response{}),
	})
}

type ClientCommand struct{}

func (c *ClientCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) == 0 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'client' command"))
	}
	sub := strings.ToUpper(argString(args, 0))
	switch sub {
	case "SETINFO":
		if len(args) != 3 || (!strings.EqualFold(argString(args, 1), "LIB-NAME") && !strings.EqualFold(argString(args, 1), "LIB-VER")) {
			return protocol.MakeError(errors.New("ERR wrong number of arguments or unsupported attribute for 'client|setinfo' command"))
		}
		return protocol.MakeSimpleString("OK")
	case "SETNAME", "GETNAME", "ID", "INFO":
		return protocol.MakeError(errors.New("ERR CLIENT " + sub + " requires connection state"))
	case "HELP":
		return protocol.MakeArray([]string{
			"CLIENT <subcommand> [args]",
		})
	default:
		return protocol.MakeError(fmt.Errorf("ERR unknown subcommand or wrong number of arguments for '%s'. Try CLIENT HELP.", strings.ToLower(sub)))
	}
}

func commandInfo(name string) *protocol.Response {
	spec, exists := commandSpecs[name]
	if !exists {
		return protocol.MakeNull()
	}
	arity := spec.minArgs + 1
	if spec.maxArgs != spec.minArgs {
		arity = -arity
	}
	flags := []string{}
	if spec.readonly {
		flags = append(flags, "readonly")
	} else if spec.firstKey > 0 {
		flags = append(flags, "write")
	}
	if spec.firstKey == 0 && strings.Contains(" EVAL EVALSHA ZINTER ZUNION ZDIFF ZINTERSTORE ZUNIONSTORE ZDIFFSTORE LMPOP BLMPOP SINTERCARD XREAD XREADGROUP ", " "+name+" ") {
		flags = append(flags, "movablekeys")
	}
	return protocol.MakeArrayResponses([]*protocol.Response{
		protocol.MakeBulkString(strings.ToLower(name)), protocol.MakeInteger(int64(arity)), protocol.MakeArray(flags),
		protocol.MakeInteger(int64(spec.firstKey)), protocol.MakeInteger(int64(spec.lastKey)), protocol.MakeInteger(int64(spec.keyStep)),
	})
}

func commandGetKeys(args [][]byte) *protocol.Response {
	if len(args) == 0 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'command|getkeys' command"))
	}
	name := strings.ToUpper(string(args[0]))
	if _, exists := DefaultRegistry.Get(name); !exists {
		return protocol.MakeError(errors.New("ERR Invalid command specified"))
	}
	if invalid := DefaultRegistry.Validate(name, args[1:]); invalid != nil {
		return invalid
	}
	spec := commandSpecs[name]
	first, last, step := spec.firstKey, spec.lastKey, spec.keyStep
	if last < 0 {
		last = len(args) + last
	}
	switch name {
	case "EVAL", "EVALSHA", "ZINTER", "ZUNION", "ZDIFF", "ZINTERSTORE", "ZUNIONSTORE", "ZDIFFSTORE", "LMPOP", "BLMPOP", "SINTERCARD":
		countIndex := 1
		if name == "EVAL" || name == "EVALSHA" || strings.HasSuffix(name, "STORE") || name == "BLMPOP" {
			countIndex = 2
		}
		count, err := strconv.Atoi(string(args[countIndex]))
		if err != nil || count < 0 || count > len(args)-countIndex-1 {
			return protocol.MakeError(errors.New("ERR Invalid arguments specified for command"))
		}
		keys := make([]string, 0, count+1)
		if strings.HasSuffix(name, "STORE") {
			keys = append(keys, string(args[1]))
		}
		for _, key := range args[countIndex+1 : countIndex+1+count] {
			keys = append(keys, string(key))
		}
		return protocol.MakeArray(keys)
	case "XREAD", "XREADGROUP":
		for i := 1; i < len(args); i++ {
			if !strings.EqualFold(string(args[i]), "STREAMS") {
				continue
			}
			remaining := len(args) - i - 1
			if remaining == 0 || remaining%2 != 0 {
				break
			}
			keys := make([]string, 0, remaining/2)
			for _, key := range args[i+1 : i+1+remaining/2] {
				keys = append(keys, string(key))
			}
			return protocol.MakeArray(keys)
		}
		return protocol.MakeError(errors.New("ERR Invalid arguments specified for command"))
	}
	keys := []string{}
	if first > 0 && step > 0 {
		for i := first; i <= last && i < len(args); i += step {
			keys = append(keys, string(args[i]))
		}
	}
	return protocol.MakeArray(keys)
}
