package command

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/TZJ-BYTE/Kerniva/internal/database"
	"github.com/TZJ-BYTE/Kerniva/internal/protocol"
	lua "github.com/yuin/gopher-lua"
)

const (
	maxScriptBytes         = 1 << 20
	maxScriptArgumentBytes = 8 << 20
	maxScriptResultBytes   = 8 << 20
	maxScriptCacheBytes    = 16 << 20
	scriptTimeout          = 250 * time.Millisecond
	maxScriptAllocations   = 64 << 20
	maxScriptInstructions  = 2000000
)

// GopherLua checks Done before every bytecode instruction. The allocation
// budget is conservative and includes concurrent process allocations.
type scriptBudgetContext struct {
	context.Context
	cancel    context.CancelCauseFunc
	remaining int
	allocated uint64
}

func newScriptBudgetContext(parent context.Context) *scriptBudgetContext {
	ctx, cancel := context.WithCancelCause(parent)
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return &scriptBudgetContext{Context: ctx, cancel: cancel, remaining: maxScriptInstructions, allocated: stats.TotalAlloc}
}

func (ctx *scriptBudgetContext) Done() <-chan struct{} {
	if ctx.Context.Err() == nil {
		ctx.remaining--
		if ctx.remaining <= 0 {
			ctx.cancel(errors.New("script instruction budget exceeded"))
		} else if ctx.remaining%4096 == 0 {
			var stats runtime.MemStats
			runtime.ReadMemStats(&stats)
			if stats.TotalAlloc-ctx.allocated > maxScriptAllocations {
				ctx.cancel(errors.New("script allocation budget exceeded"))
			}
		}
	}
	return ctx.Context.Done()
}

func (ctx *scriptBudgetContext) Err() error {
	if ctx.Context.Err() != nil {
		return context.Cause(ctx.Context)
	}
	return nil
}

var globalScriptCache = struct {
	mu      sync.RWMutex
	scripts map[string]string
	bytes   int
}{
	scripts: make(map[string]string),
}

type ScriptCommand struct{}
type EvalCommand struct{}
type EvalShaCommand struct{}

var scriptHelpText = []string{
	"SCRIPT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
	"EXISTS <sha1> [<sha1> ...] -- Return information about the existence of the scripts in the script cache.",
	"FLUSH -- Flush the Lua scripts cache.",
	"HELP -- Show helpful text about the different subcommands.",
	"LOAD <script> -- Load the script into the cache without executing it.",
}

func (c *ScriptCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) == 0 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'script' command"))
	}

	switch strings.ToUpper(argString(args, 0)) {
	case "EXISTS":
		if len(args) < 2 {
			return protocol.MakeError(errors.New("ERR wrong number of arguments for 'script|exists' command"))
		}
		items := make([]*protocol.Response, 0, len(args)-1)
		for _, rawSHA := range args[1:] {
			if scriptExists(strings.ToLower(protocol.BytesToString(rawSHA))) {
				items = append(items, protocol.MakeInteger(1))
			} else {
				items = append(items, protocol.MakeInteger(0))
			}
		}
		return protocol.MakeArrayResponses(items)
	case "FLUSH":
		if len(args) != 1 {
			return protocol.MakeError(errors.New("ERR wrong number of arguments for 'script|flush' command"))
		}
		flushScripts()
		return protocol.MakeSimpleString("OK")
	case "HELP":
		if len(args) != 1 {
			return protocol.MakeError(errors.New("ERR wrong number of arguments for 'script|help' command"))
		}
		return protocol.MakeArray(scriptHelpText)
	case "LOAD":
		if len(args) != 2 {
			return protocol.MakeError(errors.New("ERR wrong number of arguments for 'script|load' command"))
		}
		if len(args[1]) > maxScriptBytes {
			return protocol.MakeError(errors.New("ERR script exceeds size limit"))
		}
		return protocol.MakeBulkString(rememberScript(argStringCopy(args, 1)))
	default:
		return protocol.MakeError(fmt.Errorf("ERR unknown subcommand '%s'. Try SCRIPT HELP.", strings.ToLower(argString(args, 0))))
	}
}

func (c *EvalCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	return c.ExecuteContext(context.Background(), db, args)
}

func (c *EvalCommand) ExecuteContext(ctx context.Context, db *database.Database, args [][]byte) *protocol.Response {
	source, keys, argv, errResp := parseEvalArgs("eval", args)
	if errResp != nil {
		return errResp
	}
	rememberScript(source)
	return runLuaScriptContext(ctx, db, source, keys, argv)
}

func (c *EvalShaCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	return c.ExecuteContext(context.Background(), db, args)
}

func (c *EvalShaCommand) ExecuteContext(ctx context.Context, db *database.Database, args [][]byte) *protocol.Response {
	sha, keys, argv, errResp := parseEvalArgs("evalsha", args)
	if errResp != nil {
		return errResp
	}

	source, ok := lookupScript(strings.ToLower(sha))
	if !ok {
		return protocol.MakeError(errors.New("NOSCRIPT No matching script. Please use EVAL."))
	}
	return runLuaScriptContext(ctx, db, source, keys, argv)
}

func parseEvalArgs(name string, args [][]byte) (string, [][]byte, [][]byte, *protocol.Response) {
	if len(args) < 2 {
		return "", nil, nil, protocol.MakeError(fmt.Errorf("ERR wrong number of arguments for '%s' command", name))
	}
	if len(args[0]) > maxScriptBytes {
		return "", nil, nil, protocol.MakeError(errors.New("ERR script exceeds size limit"))
	}
	argumentBytes := 0
	for _, arg := range args[2:] {
		argumentBytes += len(arg)
		if argumentBytes > maxScriptArgumentBytes {
			return "", nil, nil, protocol.MakeError(errors.New("ERR script arguments exceed size limit"))
		}
	}

	numKeys, err := strconv.Atoi(argString(args, 1))
	if err != nil {
		return "", nil, nil, protocol.MakeError(errors.New("ERR value is not an integer or out of range"))
	}
	if numKeys < 0 {
		return "", nil, nil, protocol.MakeError(errors.New("ERR Number of keys can't be negative"))
	}
	if numKeys > len(args)-2 {
		return "", nil, nil, protocol.MakeError(errors.New("ERR Number of keys can't be greater than number of args"))
	}

	keys := cloneScriptArgs(args[2 : 2+numKeys])
	argv := cloneScriptArgs(args[2+numKeys:])
	return argStringCopy(args, 0), keys, argv, nil
}

func cloneScriptArgs(args [][]byte) [][]byte {
	out := make([][]byte, 0, len(args))
	for _, arg := range args {
		out = append(out, append([]byte(nil), arg...))
	}
	return out
}

func runLuaScript(db *database.Database, source string, keys [][]byte, argv [][]byte) *protocol.Response {
	return runLuaScriptContext(context.Background(), db, source, keys, argv)
}

func runLuaScriptContext(parent context.Context, db *database.Database, source string, keys [][]byte, argv [][]byte) *protocol.Response {
	ctx, cancel := context.WithTimeout(parent, scriptTimeout)
	defer cancel()
	L := lua.NewState(lua.Options{SkipOpenLibs: true, CallStackSize: 128, RegistrySize: 1024, RegistryMaxSize: 8192, RegistryGrowStep: 1024})
	defer L.Close()
	for _, lib := range []struct {
		name string
		open lua.LGFunction
	}{
		{lua.BaseLibName, lua.OpenBase}, {lua.TabLibName, lua.OpenTable},
		{lua.StringLibName, lua.OpenString}, {lua.MathLibName, lua.OpenMath},
	} {
		L.Push(L.NewFunction(lib.open))
		L.Push(lua.LString(lib.name))
		L.Call(1, 0)
	}
	for _, name := range []string{"dofile", "loadfile", "load", "loadstring", "require", "module", "collectgarbage", "print", "_printregs", "getfenv", "setfenv", "newproxy"} {
		L.SetGlobal(name, lua.LNil)
	}
	budgetContext := newScriptBudgetContext(ctx)
	defer budgetContext.cancel(context.Canceled)
	L.SetContext(budgetContext)
	stringLib := L.GetGlobal("string").(*lua.LTable)
	stringLib.RawSetString("format", L.NewFunction(func(L *lua.LState) int {
		format := L.CheckString(1)
		if len(format) > 8192 {
			L.RaiseError("script format exceeds size limit")
			return 0
		}
		inFormat, width, argumentBytes := false, 0, 0
		for _, r := range format {
			if r == '%' {
				inFormat = !inFormat
				width = 0
				continue
			}
			if !inFormat {
				continue
			}
			if r >= '0' && r <= '9' {
				width = width*10 + int(r-'0')
				if width > 4096 {
					L.RaiseError("script format width exceeds size limit")
					return 0
				}
			} else {
				width = 0
			}
			if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') {
				inFormat = false
			}
		}
		arguments := make([]interface{}, 0, L.GetTop()-1)
		for i := 2; i <= L.GetTop(); i++ {
			argumentBytes += len(L.Get(i).String())
			if argumentBytes > maxScriptResultBytes {
				L.RaiseError("script format arguments exceed size limit")
				return 0
			}
			arguments = append(arguments, L.Get(i))
		}
		result := fmt.Sprintf(format, arguments...)
		if len(result) > maxScriptResultBytes {
			L.RaiseError("script string exceeds size limit")
			return 0
		}
		L.Push(lua.LString(result))
		return 1
	}))
	stringLib.RawSetString("rep", L.NewFunction(func(L *lua.LState) int {
		value, count := L.CheckString(1), L.CheckInt(2)
		if count < 0 {
			count = 0
		}
		if len(value) > 0 && count > maxScriptResultBytes/len(value) {
			L.RaiseError("script string exceeds size limit")
			return 0
		}
		L.Push(lua.LString(strings.Repeat(value, count)))
		return 1
	}))
	L.GetGlobal("table").(*lua.LTable).RawSetString("concat", L.NewFunction(func(L *lua.LState) int {
		table := L.CheckTable(1)
		separator := L.OptString(2, "")
		start, end := L.OptInt(3, 1), L.OptInt(4, table.Len())
		if end < start {
			L.Push(lua.LString(""))
			return 1
		}
		if start < 1 || end-start >= maxScriptResultBytes/16 {
			L.RaiseError("script table exceeds size limit")
			return 0
		}
		var result strings.Builder
		for i := start; i <= end; i++ {
			value := table.RawGetInt(i)
			if value.Type() != lua.LTString && value.Type() != lua.LTNumber {
				L.RaiseError("invalid value in table.concat")
				return 0
			}
			text := value.String()
			separatorBytes := 0
			if i > start {
				separatorBytes = len(separator)
			}
			if len(text) > maxScriptResultBytes-result.Len()-separatorBytes {
				L.RaiseError("script string exceeds size limit")
				return 0
			}
			if i > start {
				result.WriteString(separator)
			}
			result.WriteString(text)
		}
		L.Push(lua.LString(result.String()))
		return 1
	}))

	L.SetGlobal("KEYS", makeLuaArgsTable(L, keys))
	L.SetGlobal("ARGV", makeLuaArgsTable(L, argv))
	L.SetGlobal("redis", buildRedisLuaAPI(L, db))

	fn, err := L.LoadString(source)
	if err != nil {
		return protocol.MakeError(fmt.Errorf("ERR Error compiling script: %v", err))
	}

	L.Push(fn)
	if err := L.PCall(0, lua.MultRet, nil); err != nil {
		return protocol.MakeError(fmt.Errorf("ERR Error running script: %v", err))
	}

	if L.GetTop() == 0 {
		return protocol.MakeNull()
	}
	budget := maxScriptResultBytes
	return luaValueToRedisResponseLimited(L.Get(1), 0, &budget, make(map[*lua.LTable]bool))
}

func makeLuaArgsTable(L *lua.LState, args [][]byte) *lua.LTable {
	tbl := L.NewTable()
	for i, arg := range args {
		tbl.RawSetInt(i+1, lua.LString(protocol.BytesToString(arg)))
	}
	return tbl
}

func buildRedisLuaAPI(L *lua.LState, db *database.Database) *lua.LTable {
	tbl := L.NewTable()
	tbl.RawSetString("call", L.NewFunction(makeRedisCall(db, false)))
	tbl.RawSetString("pcall", L.NewFunction(makeRedisCall(db, true)))
	tbl.RawSetString("sha1hex", L.NewFunction(func(L *lua.LState) int {
		L.Push(lua.LString(scriptSHA1(L.CheckString(1))))
		return 1
	}))
	return tbl
}

func makeRedisCall(db *database.Database, protected bool) lua.LGFunction {
	return func(L *lua.LState) int {
		if L.GetTop() == 0 {
			if protected {
				L.Push(luaErrorTable(L, "ERR redis.call must be called with a command name"))
				return 1
			}
			L.RaiseError("%s", "ERR redis.call must be called with a command name")
			return 0
		}

		cmdName := strings.ToUpper(L.CheckString(1))
		if scriptCommandDisallowed(cmdName) {
			errMsg := fmt.Sprintf("ERR Command '%s' is not allowed from script", strings.ToLower(cmdName))
			if protected {
				L.Push(luaErrorTable(L, errMsg))
				return 1
			}
			L.RaiseError("%s", errMsg)
			return 0
		}

		args := make([][]byte, 0, L.GetTop()-1)
		for i := 2; i <= L.GetTop(); i++ {
			arg, err := luaValueToCommandArg(L.Get(i))
			if err != nil {
				if protected {
					L.Push(luaErrorTable(L, err.Error()))
					return 1
				}
				L.RaiseError("%s", err.Error())
				return 0
			}
			args = append(args, arg)
		}

		resp := DefaultRegistry.Execute(db, cmdName, args)
		if resp == nil {
			if protected {
				L.Push(lua.LFalse)
				return 1
			}
			L.Push(lua.LFalse)
			return 1
		}
		defer protocol.ReleaseResponse(resp)

		if resp.Type == protocol.Error {
			errMsg := resp.Error.Error()
			if protected {
				L.Push(luaErrorTable(L, errMsg))
				return 1
			}
			L.RaiseError("%s", errMsg)
			return 0
		}

		L.Push(redisResponseToLuaValue(L, resp))
		return 1
	}
}

func scriptCommandDisallowed(name string) bool {
	switch name {
	case "EVAL", "EVALSHA", "SCRIPT",
		"MULTI", "EXEC", "DISCARD", "WATCH", "UNWATCH",
		"SUBSCRIBE", "PSUBSCRIBE", "UNSUBSCRIBE", "PUNSUBSCRIBE", "PUBLISH", "PUBSUB",
		"BLPOP", "BRPOP", "BRPOPLPUSH", "BLMOVE", "BLMPOP",
		"SELECT", "QUIT":
		return true
	default:
		return false
	}
}

func luaValueToCommandArg(value lua.LValue) ([]byte, error) {
	switch value.Type() {
	case lua.LTString:
		return []byte(value.String()), nil
	case lua.LTNumber:
		n := float64(value.(lua.LNumber))
		if math.Trunc(n) == n {
			return []byte(strconv.FormatInt(int64(n), 10)), nil
		}
		return []byte(strconv.FormatFloat(n, 'g', -1, 64)), nil
	case lua.LTBool:
		if lua.LVAsBool(value) {
			return []byte("1"), nil
		}
		return []byte("0"), nil
	default:
		return nil, errors.New("ERR Lua redis() command arguments must be strings, numbers or booleans")
	}
}

func redisResponseToLuaValue(L *lua.LState, resp *protocol.Response) lua.LValue {
	switch resp.Type {
	case protocol.SimpleString:
		tbl := L.NewTable()
		tbl.RawSetString("ok", lua.LString(resp.Value.(string)))
		return tbl
	case protocol.BulkString:
		if resp.Value == nil {
			return lua.LFalse
		}
		return lua.LString(resp.Value.(string))
	case protocol.Integer:
		return lua.LNumber(resp.Value.(int64))
	case protocol.Array:
		if resp.Value == nil {
			return lua.LFalse
		}
		tbl := L.NewTable()
		switch items := resp.Value.(type) {
		case []string:
			for i, item := range items {
				tbl.RawSetInt(i+1, lua.LString(item))
			}
		case []*protocol.Response:
			for i, item := range items {
				if item == nil {
					tbl.RawSetInt(i+1, lua.LFalse)
					continue
				}
				tbl.RawSetInt(i+1, redisResponseToLuaValue(L, item))
			}
		default:
			return lua.LFalse
		}
		return tbl
	default:
		return lua.LFalse
	}
}

func luaValueToRedisResponse(value lua.LValue) *protocol.Response {
	switch value.Type() {
	case lua.LTNil:
		return protocol.MakeNull()
	case lua.LTBool:
		if !lua.LVAsBool(value) {
			return protocol.MakeNull()
		}
		return protocol.MakeInteger(1)
	case lua.LTNumber:
		n := float64(value.(lua.LNumber))
		if math.Trunc(n) == n {
			return protocol.MakeInteger(int64(n))
		}
		return protocol.MakeBulkString(strconv.FormatFloat(n, 'g', -1, 64))
	case lua.LTString:
		return protocol.MakeBulkString(value.String())
	case lua.LTTable:
		tbl := value.(*lua.LTable)
		if okValue := tbl.RawGetString("ok"); okValue != lua.LNil {
			return protocol.MakeSimpleString(okValue.String())
		}
		if errValue := tbl.RawGetString("err"); errValue != lua.LNil {
			return protocol.MakeError(errors.New(errValue.String()))
		}

		items := make([]*protocol.Response, 0, tbl.Len())
		for i := 1; i <= tbl.Len(); i++ {
			items = append(items, luaValueToRedisResponse(tbl.RawGetInt(i)))
		}
		return protocol.MakeArrayResponses(items)
	default:
		return protocol.MakeNull()
	}
}

func luaValueToRedisResponseLimited(value lua.LValue, depth int, budget *int, seen map[*lua.LTable]bool) *protocol.Response {
	if err := validateLuaResult(value, depth, budget, seen); err != nil {
		return protocol.MakeError(err)
	}
	return luaValueToRedisResponse(value)
}

func validateLuaResult(value lua.LValue, depth int, budget *int, seen map[*lua.LTable]bool) error {
	*budget -= 16 + len(value.String())
	if depth > 64 || *budget < 0 {
		return errors.New("ERR script result exceeds size limit")
	}
	table, ok := value.(*lua.LTable)
	if !ok {
		return nil
	}
	if seen[table] {
		return errors.New("ERR script result contains a cycle")
	}
	seen[table] = true
	defer delete(seen, table)
	for _, name := range []string{"ok", "err"} {
		if status := table.RawGetString(name); status != lua.LNil {
			*budget -= len(status.String())
			if *budget < 0 {
				return errors.New("ERR script result exceeds size limit")
			}
			return nil
		}
	}
	for i := 1; i <= table.Len(); i++ {
		if err := validateLuaResult(table.RawGetInt(i), depth+1, budget, seen); err != nil {
			return err
		}
	}
	return nil
}

func luaErrorTable(L *lua.LState, errMsg string) *lua.LTable {
	tbl := L.NewTable()
	tbl.RawSetString("err", lua.LString(errMsg))
	return tbl
}

func scriptSHA1(source string) string {
	sum := sha1.Sum([]byte(source))
	return hex.EncodeToString(sum[:])
}

func rememberScript(source string) string {
	sha := scriptSHA1(source)
	globalScriptCache.mu.Lock()
	defer globalScriptCache.mu.Unlock()
	if _, exists := globalScriptCache.scripts[sha]; exists {
		return sha
	}
	if globalScriptCache.bytes+len(source) > maxScriptCacheBytes {
		clear(globalScriptCache.scripts)
		globalScriptCache.bytes = 0
	}
	globalScriptCache.scripts[sha] = source
	globalScriptCache.bytes += len(source)
	return sha
}

func lookupScript(sha string) (string, bool) {
	globalScriptCache.mu.RLock()
	source, ok := globalScriptCache.scripts[sha]
	globalScriptCache.mu.RUnlock()
	return source, ok
}

func scriptExists(sha string) bool {
	globalScriptCache.mu.RLock()
	_, ok := globalScriptCache.scripts[sha]
	globalScriptCache.mu.RUnlock()
	return ok
}

func flushScripts() {
	globalScriptCache.mu.Lock()
	clear(globalScriptCache.scripts)
	globalScriptCache.bytes = 0
	globalScriptCache.mu.Unlock()
}
