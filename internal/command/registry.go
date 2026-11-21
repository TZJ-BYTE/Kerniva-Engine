package command

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/TZJ-BYTE/Kerniva/internal/database"
	"github.com/TZJ-BYTE/Kerniva/internal/protocol"
)

// CommandRegistry 命令注册表
type CommandRegistry struct {
	mu       sync.RWMutex
	commands map[string]Command
}

// NewCommandRegistry 创建命令注册表
func NewCommandRegistry() *CommandRegistry {
	return &CommandRegistry{
		commands: make(map[string]Command),
	}
}

// Register 注册命令
func (r *CommandRegistry) Register(name string, cmd Command) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.commands[strings.ToUpper(name)] = cmd
}

// Get 获取命令
func (r *CommandRegistry) Get(name string) (Command, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	cmd, exists := r.commands[name]
	return cmd, exists
}

func (r *CommandRegistry) Commands() map[string]Command {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string]Command, len(r.commands))
	for name, cmd := range r.commands {
		out[name] = cmd
	}
	return out
}

// Execute 执行命令
func (r *CommandRegistry) Execute(db *database.Database, cmdName string, args [][]byte) *protocol.Response {
	return r.ExecuteContext(context.Background(), db, cmdName, args)
}

func (r *CommandRegistry) ExecuteContext(ctx context.Context, db *database.Database, cmdName string, args [][]byte) *protocol.Response {
	if ctx.Err() != nil {
		return protocol.MakeError(ctx.Err())
	}
	cmd, exists := r.Get(cmdName)
	if !exists {
		return protocol.MakeError(errors.New("ERR unknown command '" + cmdName + "'"))
	}
	if invalid := r.Validate(cmdName, args); invalid != nil {
		return invalid
	}
	if contextual, ok := cmd.(interface {
		ExecuteContext(context.Context, *database.Database, [][]byte) *protocol.Response
	}); ok {
		return contextual.ExecuteContext(ctx, db, args)
	}

	return cmd.Execute(db, args)
}

func (r *CommandRegistry) Validate(name string, args [][]byte) *protocol.Response {
	spec, ok := commandSpecs[name]
	if !ok {
		return nil
	}
	if len(args) < spec.minArgs || (spec.maxArgs >= 0 && len(args) > spec.maxArgs) {
		return protocol.MakeError(fmt.Errorf("ERR wrong number of arguments for '%s' command", strings.ToLower(name)))
	}
	if (name == "MSET" || name == "MSETNX") && len(args)%2 != 0 {
		return protocol.MakeError(fmt.Errorf("ERR wrong number of arguments for '%s' command", strings.ToLower(name)))
	}
	if (name == "HSET" || name == "HMSET") && len(args)%2 != 1 {
		return protocol.MakeError(fmt.Errorf("ERR wrong number of arguments for '%s' command", strings.ToLower(name)))
	}
	return nil
}

// DefaultRegistry 默认命令注册表
var DefaultRegistry = NewCommandRegistry()

// InitDefaultCommands 初始化默认命令
func InitDefaultCommands() {
	// 字符串命令
	DefaultRegistry.Register("SET", &SetCommand{})
	DefaultRegistry.Register("SETNX", &SetNxCommand{})
	DefaultRegistry.Register("SETEX", &SetExCommand{})
	DefaultRegistry.Register("PSETEX", &PSetExCommand{})
	DefaultRegistry.Register("GET", &GetCommand{})
	DefaultRegistry.Register("GETEX", &GetExCommand{})
	DefaultRegistry.Register("GETDEL", &GetDelCommand{})
	DefaultRegistry.Register("GETSET", &GetSetCommand{})
	DefaultRegistry.Register("GETRANGE", &GetRangeCommand{})
	DefaultRegistry.Register("SETRANGE", &SetRangeCommand{})
	DefaultRegistry.Register("DEL", &DelCommand{})
	DefaultRegistry.Register("EXISTS", &ExistsCommand{})
	DefaultRegistry.Register("EXPIRE", &ExpireCommand{})
	DefaultRegistry.Register("EXPIRETIME", &ExpireTimeCommand{})
	DefaultRegistry.Register("PEXPIRE", &PExpireCommand{})
	DefaultRegistry.Register("PEXPIRETIME", &PExpireTimeCommand{})
	DefaultRegistry.Register("EXPIREAT", &ExpireAtCommand{})
	DefaultRegistry.Register("PEXPIREAT", &PExpireAtCommand{})
	DefaultRegistry.Register("PERSIST", &PersistCommand{})
	DefaultRegistry.Register("TYPE", &TypeCommand{})
	DefaultRegistry.Register("KEYS", &KeysCommand{})
	DefaultRegistry.Register("RANDOMKEY", &RandomKeyCommand{})
	DefaultRegistry.Register("DUMP", &DumpCommand{})
	DefaultRegistry.Register("SCAN", &ScanCommand{})
	DefaultRegistry.Register("TOUCH", &TouchCommand{})
	DefaultRegistry.Register("UNLINK", &UnlinkCommand{})
	DefaultRegistry.Register("RESTORE", &RestoreCommand{})
	DefaultRegistry.Register("FLUSHDB", &FlushDBCommand{})
	DefaultRegistry.Register("FLUSHALL", &FlushAllCommand{})
	DefaultRegistry.Register("DBSIZE", &DBSizeCommand{})
	DefaultRegistry.Register("STRLEN", &StrLenCommand{})
	DefaultRegistry.Register("COPY", &CopyCommand{serverManagedCommand{name: "COPY"}})
	DefaultRegistry.Register("MOVE", &MoveCommand{serverManagedCommand{name: "MOVE"}})

	// 连接测试命令
	DefaultRegistry.Register("PING", &PingCommand{})
	DefaultRegistry.Register("ECHO", &EchoCommand{})
	DefaultRegistry.Register("INFO", &InfoCommand{})
	DefaultRegistry.Register("TIME", &TimeCommand{})
	DefaultRegistry.Register("QUIT", &QuitCommand{})
	DefaultRegistry.Register("MULTI", &MultiCommand{serverManagedCommand{name: "MULTI"}})
	DefaultRegistry.Register("EXEC", &ExecCommand{serverManagedCommand{name: "EXEC"}})
	DefaultRegistry.Register("DISCARD", &DiscardCommand{serverManagedCommand{name: "DISCARD"}})
	DefaultRegistry.Register("WATCH", &WatchCommand{serverManagedCommand{name: "WATCH"}})
	DefaultRegistry.Register("UNWATCH", &UnwatchCommand{serverManagedCommand{name: "UNWATCH"}})
	DefaultRegistry.Register("SUBSCRIBE", &SubscribeCommand{serverManagedCommand{name: "SUBSCRIBE"}})
	DefaultRegistry.Register("PSUBSCRIBE", &PSubscribeCommand{serverManagedCommand{name: "PSUBSCRIBE"}})
	DefaultRegistry.Register("UNSUBSCRIBE", &UnsubscribeCommand{serverManagedCommand{name: "UNSUBSCRIBE"}})
	DefaultRegistry.Register("PUNSUBSCRIBE", &PUnsubscribeCommand{serverManagedCommand{name: "PUNSUBSCRIBE"}})
	DefaultRegistry.Register("PUBLISH", &PublishCommand{serverManagedCommand{name: "PUBLISH"}})
	DefaultRegistry.Register("PUBSUB", &PubSubCommand{serverManagedCommand{name: "PUBSUB"}})
	DefaultRegistry.Register("HELLO", &HelloCommand{})
	DefaultRegistry.Register("CLIENT", &ClientCommand{})
	DefaultRegistry.Register("COMMAND", &CommandCommand{})
	DefaultRegistry.Register("EVAL", &EvalCommand{})
	DefaultRegistry.Register("EVALSHA", &EvalShaCommand{})
	DefaultRegistry.Register("SCRIPT", &ScriptCommand{})

	// 过期时间命令
	DefaultRegistry.Register("TTL", &TtlCommand{})
	DefaultRegistry.Register("PTTL", &PttlCommand{})

	// 原子增减命令
	DefaultRegistry.Register("INCR", &IncrCommand{})
	DefaultRegistry.Register("INCRBY", &IncrByCommand{})
	DefaultRegistry.Register("INCRBYFLOAT", &IncrByFloatCommand{})
	DefaultRegistry.Register("DECR", &DecrCommand{})
	DefaultRegistry.Register("DECRBY", &DecrByCommand{})
	DefaultRegistry.Register("APPEND", &AppendCommand{})
	DefaultRegistry.Register("GETBIT", &GetBitCommand{})
	DefaultRegistry.Register("SETBIT", &SetBitCommand{})
	DefaultRegistry.Register("BITCOUNT", &BitCountCommand{})
	DefaultRegistry.Register("BITOP", &BitOpCommand{})
	DefaultRegistry.Register("BITPOS", &BitPosCommand{})

	// 批量操作命令
	DefaultRegistry.Register("MSET", &MsetCommand{})
	DefaultRegistry.Register("MSETNX", &MsetNxCommand{})
	DefaultRegistry.Register("MGET", &MgetCommand{})

	// 重命名命令
	DefaultRegistry.Register("RENAME", &RenameCommand{})
	DefaultRegistry.Register("RENAMENX", &RenamenxCommand{})

	// 列表命令
	DefaultRegistry.Register("LPUSH", &LPushCommand{})
	DefaultRegistry.Register("LPUSHX", &LPushXCommand{})
	DefaultRegistry.Register("RPUSH", &RPushCommand{})
	DefaultRegistry.Register("RPUSHX", &RPushXCommand{})
	DefaultRegistry.Register("LPOP", &LPopCommand{})
	DefaultRegistry.Register("RPOP", &RPopCommand{})
	DefaultRegistry.Register("LLEN", &LLenCommand{})
	DefaultRegistry.Register("LRANGE", &LRangeCommand{})
	DefaultRegistry.Register("LINDEX", &LIndexCommand{})
	DefaultRegistry.Register("LPOS", &LPosCommand{})
	DefaultRegistry.Register("LREM", &LRemCommand{})
	DefaultRegistry.Register("LSET", &LSetCommand{})
	DefaultRegistry.Register("LTRIM", &LTrimCommand{})
	DefaultRegistry.Register("LMOVE", &LMoveCommand{})
	DefaultRegistry.Register("LMPOP", &LMPopCommand{})
	DefaultRegistry.Register("RPOPLPUSH", &RPopLPushCommand{})
	DefaultRegistry.Register("BLPOP", &BLPopCommand{serverManagedCommand{name: "BLPOP"}})
	DefaultRegistry.Register("BRPOP", &BRPopCommand{serverManagedCommand{name: "BRPOP"}})
	DefaultRegistry.Register("BRPOPLPUSH", &BRPopLPushCommand{serverManagedCommand{name: "BRPOPLPUSH"}})
	DefaultRegistry.Register("BLMOVE", &BLMoveCommand{serverManagedCommand{name: "BLMOVE"}})
	DefaultRegistry.Register("BLMPOP", &BLMPopCommand{serverManagedCommand{name: "BLMPOP"}})

	// 哈希命令
	DefaultRegistry.Register("HSET", &HSetCommand{})
	DefaultRegistry.Register("HGET", &HGetCommand{})
	DefaultRegistry.Register("HMSET", &HMSetCommand{})
	DefaultRegistry.Register("HMGET", &HMGetCommand{})
	DefaultRegistry.Register("HDEL", &HDelCommand{})
	DefaultRegistry.Register("HLEN", &HLenCommand{})
	DefaultRegistry.Register("HEXISTS", &HExistsCommand{})
	DefaultRegistry.Register("HSETNX", &HSetNXCommand{})
	DefaultRegistry.Register("HSTRLEN", &HStrLenCommand{})
	DefaultRegistry.Register("HKEYS", &HKeysCommand{})
	DefaultRegistry.Register("HVALS", &HValsCommand{})
	DefaultRegistry.Register("HGETALL", &HGetAllCommand{})
	DefaultRegistry.Register("HSCAN", &HScanCommand{})
	DefaultRegistry.Register("HRANDFIELD", &HRandFieldCommand{})
	DefaultRegistry.Register("HINCRBY", &HIncrByCommand{})
	DefaultRegistry.Register("HINCRBYFLOAT", &HIncrByFloatCommand{})

	// 集合命令
	DefaultRegistry.Register("SADD", &SAddCommand{})
	DefaultRegistry.Register("SREM", &SRemCommand{})
	DefaultRegistry.Register("SMEMBERS", &SMembersCommand{})
	DefaultRegistry.Register("SISMEMBER", &SIsMemberCommand{})
	DefaultRegistry.Register("SCARD", &SCardCommand{})
	DefaultRegistry.Register("SPOP", &SPopCommand{})
	DefaultRegistry.Register("SRANDMEMBER", &SRandMemberCommand{})
	DefaultRegistry.Register("SMOVE", &SMoveCommand{})
	DefaultRegistry.Register("SSCAN", &SScanCommand{})
	DefaultRegistry.Register("SINTER", &SInterCommand{})
	DefaultRegistry.Register("SINTERSTORE", &SInterStoreCommand{})
	DefaultRegistry.Register("SINTERCARD", &SInterCardCommand{})
	DefaultRegistry.Register("SUNION", &SUnionCommand{})
	DefaultRegistry.Register("SUNIONSTORE", &SUnionStoreCommand{})
	DefaultRegistry.Register("SDIFF", &SDiffCommand{})
	DefaultRegistry.Register("SDIFFSTORE", &SDiffStoreCommand{})
	DefaultRegistry.Register("SMISMEMBER", &SMIsMemberCommand{})

	// 有序集合命令
	DefaultRegistry.Register("ZADD", &ZAddCommand{})
	DefaultRegistry.Register("ZREM", &ZRemCommand{})
	DefaultRegistry.Register("ZCARD", &ZCardCommand{})
	DefaultRegistry.Register("ZSCORE", &ZScoreCommand{})
	DefaultRegistry.Register("ZMSCORE", &ZMScoreCommand{})
	DefaultRegistry.Register("ZINCRBY", &ZIncrByCommand{})
	DefaultRegistry.Register("ZRANGE", &ZRangeCommand{})
	DefaultRegistry.Register("ZREVRANGE", &ZRevRangeCommand{})
	DefaultRegistry.Register("ZCOUNT", &ZCountCommand{})
	DefaultRegistry.Register("ZDIFF", &ZDiffCommand{})
	DefaultRegistry.Register("ZDIFFSTORE", &ZDiffStoreCommand{})
	DefaultRegistry.Register("ZRANK", &ZRankCommand{})
	DefaultRegistry.Register("ZREVRANK", &ZRevRankCommand{})
	DefaultRegistry.Register("ZRANGEBYSCORE", &ZRangeByScoreCommand{})
	DefaultRegistry.Register("ZREVRANGEBYSCORE", &ZRevRangeByScoreCommand{})
	DefaultRegistry.Register("ZINTER", &ZInterCommand{})
	DefaultRegistry.Register("ZINTERSTORE", &ZInterStoreCommand{})
	DefaultRegistry.Register("ZSCAN", &ZScanCommand{})
	DefaultRegistry.Register("ZREMRANGEBYRANK", &ZRemRangeByRankCommand{})
	DefaultRegistry.Register("ZREMRANGEBYSCORE", &ZRemRangeByScoreCommand{})
	DefaultRegistry.Register("ZRANDMEMBER", &ZRandMemberCommand{})
	DefaultRegistry.Register("ZPOPMIN", &ZPopMinCommand{})
	DefaultRegistry.Register("ZPOPMAX", &ZPopMaxCommand{})
	DefaultRegistry.Register("ZUNION", &ZUnionCommand{})
	DefaultRegistry.Register("ZUNIONSTORE", &ZUnionStoreCommand{})

	// Stream 命令
	DefaultRegistry.Register("XADD", &XAddCommand{})
	DefaultRegistry.Register("XLEN", &XLenCommand{})
	DefaultRegistry.Register("XRANGE", &XRangeCommand{})
	DefaultRegistry.Register("XREVRANGE", &XRevRangeCommand{})
	DefaultRegistry.Register("XDEL", &XDelCommand{})
	DefaultRegistry.Register("XTRIM", &XTrimCommand{})
	DefaultRegistry.Register("XREAD", &XReadCommand{})
	DefaultRegistry.Register("XGROUP", &XGroupCommand{})
	DefaultRegistry.Register("XREADGROUP", &XReadGroupCommand{})
	DefaultRegistry.Register("XACK", &XAckCommand{})
	DefaultRegistry.Register("XPENDING", &XPendingCommand{})
}
