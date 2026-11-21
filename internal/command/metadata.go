package command

import "strings"

type commandSpec struct {
	minArgs  int
	maxArgs  int
	readonly bool
	firstKey int
	lastKey  int
	keyStep  int
}

var commandSpecs = makeCommandSpecs()

func makeCommandSpecs() map[string]commandSpec {
	commands := make(map[string]commandSpec)
	add := func(names string, minArgs, maxArgs int) {
		for _, name := range strings.Fields(names) {
			commands[name] = commandSpec{minArgs: minArgs, maxArgs: maxArgs, firstKey: 1, lastKey: 1, keyStep: 1}
		}
	}
	add("TIME QUIT MULTI EXEC DISCARD UNWATCH DBSIZE RANDOMKEY FLUSHDB FLUSHALL", 0, 0)
	add("PING INFO", 0, 1)
	add("GET GETDEL STRLEN TYPE TTL PTTL EXPIRETIME PEXPIRETIME PERSIST INCR DECR LLEN HLEN HKEYS HVALS HGETALL SMEMBERS SCARD ZCARD XLEN DUMP ECHO SELECT", 1, 1)
	add("GETSET APPEND INCRBY INCRBYFLOAT DECRBY GETBIT HEXISTS HGET HSTRLEN HINCRBY HINCRBYFLOAT SISMEMBER SMISMEMBER ZSCORE ZMSCORE ZRANK ZREVRANK XACK", 2, -1)
	add("GETSET APPEND INCRBY INCRBYFLOAT DECRBY GETBIT HEXISTS HGET HSTRLEN SISMEMBER ZSCORE", 2, 2)
	add("ZRANK ZREVRANK", 2, 3)
	add("GETRANGE SETRANGE SETBIT RENAME RENAMENX MOVE RPOPLPUSH", 2, 2)
	add("GETRANGE SETRANGE SETBIT", 3, 3)
	add("HSETNX HINCRBY HINCRBYFLOAT ZINCRBY", 3, 3)
	add("SETEX PSETEX", 3, 3)
	add("SETNX", 2, 2)
	add("SET", 2, -1)
	add("GETEX", 1, -1)
	add("EXPIRE PEXPIRE EXPIREAT PEXPIREAT", 2, -1)
	add("DEL EXISTS MGET TOUCH UNLINK WATCH SUBSCRIBE PSUBSCRIBE", 1, -1)
	add("KEYS", 1, 1)
	add("UNSUBSCRIBE PUNSUBSCRIBE COMMAND", 0, -1)
	add("MSET MSETNX", 2, -1)
	add("LPUSH LPUSHX RPUSH RPUSHX HDEL SADD SREM HMGET", 2, -1)
	add("LPOP RPOP SPOP SRANDMEMBER HRANDFIELD ZPOPMIN ZPOPMAX", 1, 2)
	add("LRANGE LTRIM LSET LREM ZCOUNT ZREMRANGEBYRANK ZREMRANGEBYSCORE", 3, 3)
	add("LINDEX", 2, 2)
	add("LPOS", 2, -1)
	add("LMOVE", 4, 4)
	add("LMPOP", 3, -1)
	add("BLPOP BRPOP", 2, -1)
	add("BRPOPLPUSH", 3, 3)
	add("BLMOVE", 5, 5)
	add("BLMPOP", 4, -1)
	add("HSET HMSET", 3, -1)
	add("SSCAN HSCAN ZSCAN SCAN", 1, -1)
	add("SSCAN HSCAN ZSCAN", 2, -1)
	add("SMOVE", 3, 3)
	add("SINTER SUNION SDIFF SINTERCARD", 1, -1)
	add("SINTERSTORE SUNIONSTORE SDIFFSTORE", 2, -1)
	add("ZADD", 3, -1)
	add("ZREM", 2, -1)
	add("ZRANGE ZREVRANGE ZRANGEBYSCORE ZREVRANGEBYSCORE", 3, -1)
	add("ZINTER ZUNION ZDIFF", 2, -1)
	add("ZINTERSTORE ZUNIONSTORE ZDIFFSTORE", 3, -1)
	add("ZRANDMEMBER", 1, 3)
	add("BITCOUNT", 1, 4)
	add("BITPOS", 2, 5)
	add("BITOP", 3, -1)
	add("COPY", 2, -1)
	add("RESTORE", 3, -1)
	add("PUBLISH", 2, 2)
	add("PUBSUB CLIENT SCRIPT", 1, -1)
	add("HELLO", 0, -1)
	add("EVAL EVALSHA", 2, -1)
	add("XADD", 4, -1)
	add("XRANGE XREVRANGE", 3, -1)
	add("XDEL", 2, -1)
	add("XTRIM", 3, -1)
	add("XREAD", 3, -1)
	add("XGROUP", 2, -1)
	add("XREADGROUP", 6, -1)
	add("XACK", 3, -1)
	add("XPENDING", 2, -1)
	for _, name := range strings.Fields("GET STRLEN GETRANGE GETBIT BITCOUNT BITPOS TYPE TTL PTTL EXPIRETIME PEXPIRETIME EXISTS KEYS RANDOMKEY DBSIZE DUMP SCAN MGET LLEN LRANGE LINDEX LPOS HGET HMGET HLEN HEXISTS HSTRLEN HKEYS HVALS HGETALL HSCAN HRANDFIELD SMEMBERS SISMEMBER SMISMEMBER SCARD SRANDMEMBER SSCAN SINTER SINTERCARD SUNION SDIFF ZCARD ZSCORE ZMSCORE ZRANGE ZREVRANGE ZCOUNT ZRANK ZREVRANK ZRANGEBYSCORE ZREVRANGEBYSCORE ZINTER ZUNION ZDIFF ZSCAN ZRANDMEMBER XLEN XRANGE XREVRANGE XREAD XPENDING") {
		spec := commands[name]
		spec.readonly = true
		commands[name] = spec
	}
	for _, name := range strings.Fields("PING ECHO TIME INFO QUIT MULTI EXEC DISCARD UNWATCH SELECT SUBSCRIBE PSUBSCRIBE UNSUBSCRIBE PUNSUBSCRIBE PUBLISH PUBSUB CLIENT HELLO COMMAND SCRIPT FLUSHDB FLUSHALL DBSIZE KEYS SCAN RANDOMKEY") {
		spec := commands[name]
		spec.firstKey, spec.lastKey, spec.keyStep = 0, 0, 0
		commands[name] = spec
	}
	for _, name := range strings.Fields("DEL EXISTS MGET TOUCH UNLINK WATCH SINTER SUNION SDIFF SINTERSTORE SUNIONSTORE SDIFFSTORE") {
		spec := commands[name]
		spec.lastKey = -1
		commands[name] = spec
	}
	for _, name := range []string{"MSET", "MSETNX"} {
		spec := commands[name]
		spec.lastKey, spec.keyStep = -1, 2
		commands[name] = spec
	}
	for _, name := range strings.Fields("RENAME RENAMENX COPY RPOPLPUSH BRPOPLPUSH LMOVE BLMOVE SMOVE") {
		spec := commands[name]
		spec.lastKey = 2
		commands[name] = spec
	}
	spec := commands["BITOP"]
	spec.firstKey, spec.lastKey = 2, -1
	commands["BITOP"] = spec
	spec = commands["XGROUP"]
	spec.firstKey, spec.lastKey = 2, 2
	commands["XGROUP"] = spec
	for _, name := range []string{"BLPOP", "BRPOP"} {
		spec := commands[name]
		spec.lastKey = -2
		commands[name] = spec
	}
	for _, name := range strings.Fields("EVAL EVALSHA ZINTER ZUNION ZDIFF ZINTERSTORE ZUNIONSTORE ZDIFFSTORE LMPOP BLMPOP SINTERCARD XREAD XREADGROUP") {
		spec := commands[name]
		spec.firstKey, spec.lastKey, spec.keyStep = 0, 0, 0
		commands[name] = spec
	}
	return commands
}
