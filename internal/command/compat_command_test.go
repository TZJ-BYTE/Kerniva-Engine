package command_test

import (
	"testing"
	"time"

	"github.com/TZJ-BYTE/Kerniva/config"
	"github.com/TZJ-BYTE/Kerniva/internal/command"
	"github.com/TZJ-BYTE/Kerniva/internal/database"
	"github.com/TZJ-BYTE/Kerniva/internal/protocol"
)

func TestCompatCommands(t *testing.T) {
	command.InitDefaultCommands()
	cfg := config.DefaultConfig()
	cfg.DataDir = t.TempDir()
	manager := database.NewDBManager(cfg)
	defer manager.Close()
	db := manager.GetDefaultDB()

	assertRespString(t, (&command.EchoCommand{}).Execute(db, [][]byte{[]byte("hello")}), '$', "hello")
	helloResp := (&command.HelloCommand{}).Execute(db, [][]byte{[]byte("2")})
	if helloResp.Type != '*' || len(helloResp.Value.([]*protocol.Response)) == 0 {
		t.Fatalf("unexpected hello resp=%#v", helloResp)
	}
	if unsupported := (&command.HelloCommand{}).Execute(db, [][]byte{[]byte("3")}); unsupported.Type != '-' {
		t.Fatalf("RESP3 must be explicitly rejected until supported, got %#v", unsupported)
	}
	assertRespString(t, (&command.ClientCommand{}).Execute(db, [][]byte{[]byte("SETINFO"), []byte("LIB-NAME"), []byte("kerniva-test")}), '+', "OK")
	assertRespInt(t, (&command.CommandCommand{}).Execute(db, [][]byte{[]byte("COUNT")}), int64(len(command.DefaultRegistry.Commands())))

	assertRespString(t, (&command.SetExCommand{}).Execute(db, [][]byte{
		[]byte("k"),
		[]byte("60"),
		[]byte("v"),
	}), '+', "OK")

	assertRespString(t, (&command.TypeCommand{}).Execute(db, [][]byte{[]byte("k")}), '+', "string")
	assertRespInt(t, (&command.StrLenCommand{}).Execute(db, [][]byte{[]byte("k")}), 1)

	assertRespInt(t, (&command.AppendCommand{}).Execute(db, [][]byte{[]byte("k"), []byte("tail")}), 5)
	assertRespString(t, (&command.GetCommand{}).Execute(db, [][]byte{[]byte("k")}), '$', "vtail")
	assertRespString(t, (&command.GetRangeCommand{}).Execute(db, [][]byte{[]byte("k"), []byte("1"), []byte("3")}), '$', "tai")
	assertRespInt(t, (&command.SetRangeCommand{}).Execute(db, [][]byte{[]byte("k"), []byte("5"), []byte("++")}), 7)
	assertRespString(t, (&command.GetCommand{}).Execute(db, [][]byte{[]byte("k")}), '$', "vtail++")
	assertRespString(t, (&command.IncrByFloatCommand{}).Execute(db, [][]byte{[]byte("float"), []byte("1.5")}), '$', "1.5")
	assertRespString(t, (&command.IncrByFloatCommand{}).Execute(db, [][]byte{[]byte("float"), []byte("0.5")}), '$', "2")
	assertRespInt(t, (&command.SetBitCommand{}).Execute(db, [][]byte{[]byte("bits"), []byte("7"), []byte("1")}), 0)
	assertRespInt(t, (&command.GetBitCommand{}).Execute(db, [][]byte{[]byte("bits"), []byte("7")}), 1)
	assertRespInt(t, (&command.BitCountCommand{}).Execute(db, [][]byte{[]byte("bits")}), 1)
	assertRespString(t, (&command.SetCommand{}).Execute(db, [][]byte{[]byte("bit:a"), {0x0f}}), '+', "OK")
	assertRespString(t, (&command.SetCommand{}).Execute(db, [][]byte{[]byte("bit:b"), {0xf0}}), '+', "OK")
	assertRespInt(t, (&command.BitOpCommand{}).Execute(db, [][]byte{[]byte("OR"), []byte("bit:or"), []byte("bit:a"), []byte("bit:b")}), 1)
	assertRespInt(t, (&command.BitOpCommand{}).Execute(db, [][]byte{[]byte("NOT"), []byte("bit:not"), []byte("bit:a")}), 1)
	bitOrResp := (&command.GetCommand{}).Execute(db, [][]byte{[]byte("bit:or")})
	if bitOrResp.Type != '$' || bitOrResp.Value != string([]byte{0xff}) {
		t.Fatalf("unexpected bitop or resp=%#v", bitOrResp)
	}
	assertRespInt(t, (&command.BitPosCommand{}).Execute(db, [][]byte{[]byte("bit:or"), []byte("1")}), 0)
	assertRespInt(t, (&command.BitPosCommand{}).Execute(db, [][]byte{[]byte("bit:or"), []byte("0")}), 8)
	assertRespInt(t, (&command.BitPosCommand{}).Execute(db, [][]byte{[]byte("bit:not"), []byte("1"), []byte("0"), []byte("7"), []byte("BIT")}), 0)

	assertRespInt(t, (&command.PExpireCommand{}).Execute(db, [][]byte{[]byte("k"), []byte("120000")}), 1)
	if expireAt, exists := db.GetExpireTime("k"); !exists || expireAt <= time.Now().UnixMilli() {
		t.Fatalf("expected pexpire to update ttl, exists=%v expireAt=%d", exists, expireAt)
	}
	pexpireResp := (&command.PExpireTimeCommand{}).Execute(db, [][]byte{[]byte("k")})
	if pexpireResp.Type != ':' || pexpireResp.Value.(int64) <= time.Now().UnixMilli() {
		t.Fatalf("pexpiretime resp=%#v", pexpireResp)
	}

	assertRespInt(t, (&command.PersistCommand{}).Execute(db, [][]byte{[]byte("k")}), 1)
	assertRespInt(t, (&command.ExpireTimeCommand{}).Execute(db, [][]byte{[]byte("k")}), -1)
	assertRespInt(t, (&command.ExpireCommand{}).Execute(db, [][]byte{[]byte("k"), []byte("60"), []byte("NX")}), 1)
	assertRespInt(t, (&command.ExpireCommand{}).Execute(db, [][]byte{[]byte("k"), []byte("120"), []byte("NX")}), 0)
	assertRespInt(t, (&command.PExpireCommand{}).Execute(db, [][]byte{[]byte("k"), []byte("120000"), []byte("GT")}), 1)
	assertRespInt(t, (&command.PExpireCommand{}).Execute(db, [][]byte{[]byte("k"), []byte("1000"), []byte("LT")}), 1)
	assertRespInt(t, (&command.PersistCommand{}).Execute(db, [][]byte{[]byte("k")}), 1)
	assertRespInt(t, (&command.PExpireCommand{}).Execute(db, [][]byte{[]byte("k"), []byte("1000"), []byte("GT")}), 0)
	assertRespInt(t, (&command.PExpireCommand{}).Execute(db, [][]byte{[]byte("k"), []byte("1000"), []byte("LT")}), 1)
	assertRespString(t, (&command.GetExCommand{}).Execute(db, [][]byte{[]byte("k"), []byte("EX"), []byte("60")}), '$', "vtail++")
	if expireAt, exists := db.GetExpireTime("k"); !exists || expireAt <= time.Now().UnixMilli() {
		t.Fatalf("expected getex to set expire, exists=%v expireAt=%d", exists, expireAt)
	}

	assertRespInt(t, (&command.SetNxCommand{}).Execute(db, [][]byte{[]byte("k"), []byte("blocked")}), 0)
	assertRespString(t, (&command.GetSetCommand{}).Execute(db, [][]byte{[]byte("k"), []byte("42")}), '$', "vtail++")
	assertRespInt(t, (&command.IncrByCommand{}).Execute(db, [][]byte{[]byte("k"), []byte("8")}), 50)
	assertRespInt(t, (&command.DecrByCommand{}).Execute(db, [][]byte{[]byte("k"), []byte("5")}), 45)

	assertRespString(t, (&command.PSetExCommand{}).Execute(db, [][]byte{
		[]byte("temp"),
		[]byte("5000"),
		[]byte("value"),
	}), '+', "OK")
	expireTimeResp := (&command.ExpireTimeCommand{}).Execute(db, [][]byte{[]byte("temp")})
	if expireTimeResp.Type != ':' || expireTimeResp.Value.(int64) <= time.Now().Unix() {
		t.Fatalf("unexpected expiretime resp=%#v", expireTimeResp)
	}

	assertRespString(t, (&command.GetDelCommand{}).Execute(db, [][]byte{[]byte("temp")}), '$', "value")
	nullResp := (&command.GetCommand{}).Execute(db, [][]byte{[]byte("temp")})
	if nullResp.Type != '$' || nullResp.Value != nil {
		t.Fatalf("expected nil bulk string, got %#v", nullResp)
	}

	assertRespString(t, (&command.SetCommand{}).Execute(db, [][]byte{[]byte("scan:1"), []byte("a")}), '+', "OK")
	assertRespString(t, (&command.SetCommand{}).Execute(db, [][]byte{[]byte("scan:2"), []byte("b")}), '+', "OK")
	assertRespInt(t, (&command.MsetNxCommand{}).Execute(db, [][]byte{[]byte("mx:1"), []byte("a"), []byte("mx:2"), []byte("b")}), 1)
	assertRespInt(t, (&command.MsetNxCommand{}).Execute(db, [][]byte{[]byte("mx:1"), []byte("x"), []byte("mx:3"), []byte("y")}), 0)
	scanResp := (&command.ScanCommand{}).Execute(db, [][]byte{
		[]byte("0"),
		[]byte("MATCH"),
		[]byte("scan:*"),
		[]byte("COUNT"),
		[]byte("100"),
	})
	if scanResp.Type != '*' {
		t.Fatalf("scan type=%c", scanResp.Type)
	}
	items := scanResp.Value.([]*protocol.Response)
	if len(items) != 2 || items[0].Type != '$' || items[1].Type != '*' {
		t.Fatalf("unexpected scan payload %#v", scanResp.Value)
	}

	assertRespInt(t, (&command.SAddCommand{}).Execute(db, [][]byte{[]byte("set"), []byte("b"), []byte("a")}), 2)
	assertRespInt(t, (&command.SAddCommand{}).Execute(db, [][]byte{[]byte("set3"), []byte("a"), []byte("c")}), 2)
	assertRespInt(t, (&command.SCardCommand{}).Execute(db, [][]byte{[]byte("set")}), 2)
	assertRespInt(t, (&command.SIsMemberCommand{}).Execute(db, [][]byte{[]byte("set"), []byte("a")}), 1)
	srandResp := (&command.SRandMemberCommand{}).Execute(db, [][]byte{[]byte("set")})
	if srandResp.Type != '$' {
		t.Fatalf("unexpected srandmember resp=%#v", srandResp)
	}

	smembersResp := (&command.SMembersCommand{}).Execute(db, [][]byte{[]byte("set")})
	if smembersResp.Type != '*' || len(smembersResp.Value.([]string)) != 2 {
		t.Fatalf("unexpected smembers resp=%#v", smembersResp)
	}
	if resp := (&command.SInterCommand{}).Execute(db, [][]byte{[]byte("set"), []byte("set3")}); resp.Type != '*' || len(resp.Value.([]string)) != 1 || resp.Value.([]string)[0] != "a" {
		t.Fatalf("unexpected sinter resp=%#v", resp)
	}
	assertRespInt(t, (&command.SInterStoreCommand{}).Execute(db, [][]byte{[]byte("set:dst"), []byte("set"), []byte("set3")}), 1)
	assertRespInt(t, (&command.SIsMemberCommand{}).Execute(db, [][]byte{[]byte("set:dst"), []byte("a")}), 1)
	assertRespInt(t, (&command.SInterCardCommand{}).Execute(db, [][]byte{[]byte("2"), []byte("set"), []byte("set3")}), 1)
	assertRespInt(t, (&command.SInterCardCommand{}).Execute(db, [][]byte{[]byte("2"), []byte("set"), []byte("set3"), []byte("LIMIT"), []byte("1")}), 1)
	if resp := (&command.SUnionCommand{}).Execute(db, [][]byte{[]byte("set"), []byte("set3")}); resp.Type != '*' || len(resp.Value.([]string)) != 3 {
		t.Fatalf("unexpected sunion resp=%#v", resp)
	}
	assertRespInt(t, (&command.SUnionStoreCommand{}).Execute(db, [][]byte{[]byte("set:union"), []byte("set"), []byte("set3")}), 3)
	if resp := (&command.SMembersCommand{}).Execute(db, [][]byte{[]byte("set:union")}); resp.Type != '*' || len(resp.Value.([]string)) != 3 {
		t.Fatalf("unexpected sunionstore resp=%#v", resp)
	}
	if resp := (&command.SDiffCommand{}).Execute(db, [][]byte{[]byte("set3"), []byte("set")}); resp.Type != '*' || len(resp.Value.([]string)) != 1 || resp.Value.([]string)[0] != "c" {
		t.Fatalf("unexpected sdiff resp=%#v", resp)
	}
	assertRespInt(t, (&command.SDiffStoreCommand{}).Execute(db, [][]byte{[]byte("set:diff"), []byte("set3"), []byte("set")}), 1)
	assertRespInt(t, (&command.SIsMemberCommand{}).Execute(db, [][]byte{[]byte("set:diff"), []byte("c")}), 1)
	assertRespInt(t, (&command.SAddCommand{}).Execute(db, [][]byte{[]byte("set:gone"), []byte("stale")}), 1)
	assertRespInt(t, (&command.SDiffStoreCommand{}).Execute(db, [][]byte{[]byte("set:gone"), []byte("set"), []byte("set")}), 0)
	goneResp := (&command.SMembersCommand{}).Execute(db, [][]byte{[]byte("set:gone")})
	if goneResp.Type != '*' || len(goneResp.Value.([]string)) != 0 {
		t.Fatalf("expected sdiffstore empty destination removal, got %#v", goneResp)
	}
	if resp := (&command.SMIsMemberCommand{}).Execute(db, [][]byte{[]byte("set"), []byte("a"), []byte("missing")}); resp.Type != '*' {
		t.Fatalf("unexpected smismember resp=%#v", resp)
	}
	sscanResp := (&command.SScanCommand{}).Execute(db, [][]byte{
		[]byte("set3"),
		[]byte("0"),
		[]byte("MATCH"),
		[]byte("a"),
		[]byte("COUNT"),
		[]byte("10"),
	})
	if sscanResp.Type != '*' {
		t.Fatalf("unexpected sscan resp=%#v", sscanResp)
	}
	sscanItems := sscanResp.Value.([]*protocol.Response)
	if len(sscanItems) != 2 || sscanItems[0].Value != "0" {
		t.Fatalf("unexpected sscan payload=%#v", sscanResp.Value)
	}
	sscanValues := sscanItems[1].Value.([]string)
	if len(sscanValues) != 1 || sscanValues[0] != "a" {
		t.Fatalf("unexpected sscan values=%v", sscanValues)
	}
	assertRespInt(t, (&command.SMoveCommand{}).Execute(db, [][]byte{[]byte("set"), []byte("set2"), []byte("a")}), 1)
	assertRespInt(t, (&command.SIsMemberCommand{}).Execute(db, [][]byte{[]byte("set2"), []byte("a")}), 1)
	spopResp := (&command.SPopCommand{}).Execute(db, [][]byte{[]byte("set2")})
	if spopResp.Type != '$' || spopResp.Value != "a" {
		t.Fatalf("unexpected spop resp=%#v", spopResp)
	}

	assertRespInt(t, (&command.ZAddCommand{}).Execute(db, [][]byte{
		[]byte("z"),
		[]byte("2"),
		[]byte("b"),
		[]byte("1"),
		[]byte("a"),
	}), 2)
	assertRespString(t, (&command.ZScoreCommand{}).Execute(db, [][]byte{[]byte("z"), []byte("a")}), '$', "1")
	assertRespString(t, (&command.ZIncrByCommand{}).Execute(db, [][]byte{[]byte("z"), []byte("2"), []byte("a")}), '$', "3")
	assertRespInt(t, (&command.ZCountCommand{}).Execute(db, [][]byte{[]byte("z"), []byte("-inf"), []byte("+inf")}), 2)
	zmscoreResp := (&command.ZMScoreCommand{}).Execute(db, [][]byte{[]byte("z"), []byte("a"), []byte("missing")})
	if zmscoreResp.Type != '*' {
		t.Fatalf("unexpected zmscore resp=%#v", zmscoreResp)
	}
	zmscoreItems := zmscoreResp.Value.([]*protocol.Response)
	if len(zmscoreItems) != 2 || zmscoreItems[0].Value != "3" || zmscoreItems[1].Value != nil {
		t.Fatalf("unexpected zmscore payload=%#v", zmscoreResp.Value)
	}

	zrangeResp := (&command.ZRangeCommand{}).Execute(db, [][]byte{
		[]byte("z"),
		[]byte("0"),
		[]byte("-1"),
		[]byte("WITHSCORES"),
	})
	if zrangeResp.Type != '*' {
		t.Fatalf("unexpected zrange resp=%#v", zrangeResp)
	}
	assertRespInt(t, (&command.ZRankCommand{}).Execute(db, [][]byte{[]byte("z"), []byte("b")}), 0)
	assertRespInt(t, (&command.ZRevRankCommand{}).Execute(db, [][]byte{[]byte("z"), []byte("a")}), 0)
	zrangeByScoreResp := (&command.ZRangeByScoreCommand{}).Execute(db, [][]byte{
		[]byte("z"),
		[]byte("2"),
		[]byte("+inf"),
		[]byte("WITHSCORES"),
	})
	if zrangeByScoreResp.Type != '*' || len(zrangeByScoreResp.Value.([]string)) != 4 {
		t.Fatalf("unexpected zrangebyscore resp=%#v", zrangeByScoreResp)
	}
	assertRespInt(t, (&command.ZAddCommand{}).Execute(db, [][]byte{[]byte("z2"), []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c")}), 3)
	assertRespInt(t, (&command.ZRemRangeByScoreCommand{}).Execute(db, [][]byte{[]byte("z2"), []byte("1"), []byte("2")}), 2)
	if resp := (&command.ZPopMaxCommand{}).Execute(db, [][]byte{[]byte("z2")}); resp.Type != '*' || len(resp.Value.([]string)) != 2 || resp.Value.([]string)[0] != "c" {
		t.Fatalf("unexpected zpopmax resp=%#v", resp)
	}
	zscanResp := (&command.ZScanCommand{}).Execute(db, [][]byte{[]byte("z"), []byte("0"), []byte("MATCH"), []byte("*"), []byte("COUNT"), []byte("10")})
	if zscanResp.Type != '*' {
		t.Fatalf("unexpected zscan resp=%#v", zscanResp)
	}
	zrangeValues := zrangeResp.Value.([]string)
	if len(zrangeValues) != 4 || zrangeValues[0] != "b" || zrangeValues[2] != "a" {
		t.Fatalf("unexpected zrange values=%v", zrangeValues)
	}

	zrevrangeResp := (&command.ZRevRangeCommand{}).Execute(db, [][]byte{
		[]byte("z"),
		[]byte("0"),
		[]byte("0"),
	})
	if zrevrangeResp.Type != '*' || len(zrevrangeResp.Value.([]string)) != 1 || zrevrangeResp.Value.([]string)[0] != "a" {
		t.Fatalf("unexpected zrevrange resp=%#v", zrevrangeResp)
	}
	zrandSingleResp := (&command.ZRandMemberCommand{}).Execute(db, [][]byte{[]byte("z")})
	if zrandSingleResp.Type != '$' {
		t.Fatalf("unexpected zrandmember single resp=%#v", zrandSingleResp)
	}
	zrandWithScoresResp := (&command.ZRandMemberCommand{}).Execute(db, [][]byte{[]byte("z"), []byte("2"), []byte("WITHSCORES")})
	if zrandWithScoresResp.Type != '*' || len(zrandWithScoresResp.Value.([]string)) != 4 {
		t.Fatalf("unexpected zrandmember withscores resp=%#v", zrandWithScoresResp)
	}
	assertRespInt(t, (&command.ZRemRangeByRankCommand{}).Execute(db, [][]byte{[]byte("z"), []byte("0"), []byte("0")}), 1)
	assertRespInt(t, (&command.ZCardCommand{}).Execute(db, [][]byte{[]byte("z")}), 1)

	for _, name := range []string{"LPUSHX", "RPUSHX", "LMOVE", "LMPOP", "LPOS", "LREM", "BLMOVE", "BLMPOP"} {
		if _, ok := command.DefaultRegistry.Get(name); !ok {
			t.Fatalf("expected %s registered", name)
		}
	}
	assertRespInt(t, (&command.RPushCommand{}).Execute(db, [][]byte{[]byte("count:list"), []byte("a"), []byte("b"), []byte("c"), []byte("d")}), 4)
	lpopCountResp := command.DefaultRegistry.Execute(db, "LPOP", [][]byte{[]byte("count:list"), []byte("2")})
	if lpopCountResp.Type != '*' {
		t.Fatalf("unexpected lpop count resp=%#v", lpopCountResp)
	}
	lpopCountValues := lpopCountResp.Value.([]string)
	if len(lpopCountValues) != 2 || lpopCountValues[0] != "a" || lpopCountValues[1] != "b" {
		t.Fatalf("unexpected lpop count values=%v", lpopCountValues)
	}
	rpopCountResp := command.DefaultRegistry.Execute(db, "RPOP", [][]byte{[]byte("count:list"), []byte("0")})
	if rpopCountResp.Type != '*' || len(rpopCountResp.Value.([]string)) != 0 {
		t.Fatalf("unexpected rpop zero-count resp=%#v", rpopCountResp)
	}
	rpopCountResp = command.DefaultRegistry.Execute(db, "RPOP", [][]byte{[]byte("count:list"), []byte("2")})
	if rpopCountResp.Type != '*' {
		t.Fatalf("unexpected rpop count resp=%#v", rpopCountResp)
	}
	rpopCountValues := rpopCountResp.Value.([]string)
	if len(rpopCountValues) != 2 || rpopCountValues[0] != "d" || rpopCountValues[1] != "c" {
		t.Fatalf("unexpected rpop count values=%v", rpopCountValues)
	}

	assertRespInt(t, (&command.RPushCommand{}).Execute(db, [][]byte{[]byte("lm:list2"), []byte("x"), []byte("y"), []byte("z")}), 3)
	lmpopResp := command.DefaultRegistry.Execute(db, "LMPOP", [][]byte{
		[]byte("2"),
		[]byte("lm:list1"),
		[]byte("lm:list2"),
		[]byte("RIGHT"),
		[]byte("COUNT"),
		[]byte("2"),
	})
	if lmpopResp.Type != '*' {
		t.Fatalf("unexpected lmpop resp=%#v", lmpopResp)
	}
	lmpopItems := lmpopResp.Value.([]*protocol.Response)
	if len(lmpopItems) != 2 || lmpopItems[0].Type != '$' || lmpopItems[0].Value != "lm:list2" || lmpopItems[1].Type != '*' {
		t.Fatalf("unexpected lmpop payload=%#v", lmpopResp.Value)
	}
	lmpopValues := lmpopItems[1].Value.([]string)
	if len(lmpopValues) != 2 || lmpopValues[0] != "z" || lmpopValues[1] != "y" {
		t.Fatalf("unexpected lmpop values=%v", lmpopValues)
	}

	assertRespInt(t, command.DefaultRegistry.Execute(db, "LPUSHX", [][]byte{[]byte("missing:list"), []byte("x")}), 0)
	assertRespInt(t, (&command.RPushCommand{}).Execute(db, [][]byte{[]byte("list"), []byte("x"), []byte("y"), []byte("z")}), 3)
	assertRespInt(t, command.DefaultRegistry.Execute(db, "LPUSHX", [][]byte{[]byte("list"), []byte("h1"), []byte("h2")}), 5)
	assertRespInt(t, command.DefaultRegistry.Execute(db, "RPUSHX", [][]byte{[]byte("list"), []byte("tail")}), 6)
	assertRespInt(t, command.DefaultRegistry.Execute(db, "LPOS", [][]byte{[]byte("list"), []byte("h1")}), 1)
	lposCountResp := command.DefaultRegistry.Execute(db, "LPOS", [][]byte{
		[]byte("list"),
		[]byte("tail"),
		[]byte("COUNT"),
		[]byte("0"),
	})
	if lposCountResp.Type != '*' {
		t.Fatalf("unexpected lpos count resp=%#v", lposCountResp)
	}
	lposCountItems := lposCountResp.Value.([]*protocol.Response)
	if len(lposCountItems) != 1 || lposCountItems[0].Type != ':' || lposCountItems[0].Value != int64(5) {
		t.Fatalf("unexpected lpos count items=%#v", lposCountItems)
	}
	lposRankResp := command.DefaultRegistry.Execute(db, "LPOS", [][]byte{
		[]byte("list"),
		[]byte("x"),
		[]byte("RANK"),
		[]byte("-1"),
	})
	if lposRankResp.Type != ':' || lposRankResp.Value != int64(2) {
		t.Fatalf("unexpected negative-rank lpos resp=%#v", lposRankResp)
	}
	assertRespString(t, (&command.LIndexCommand{}).Execute(db, [][]byte{[]byte("list"), []byte("-1")}), '$', "tail")
	assertRespString(t, (&command.LSetCommand{}).Execute(db, [][]byte{[]byte("list"), []byte("1"), []byte("yy")}), '+', "OK")
	assertRespString(t, (&command.LIndexCommand{}).Execute(db, [][]byte{[]byte("list"), []byte("1")}), '$', "yy")
	assertRespString(t, (&command.RPopLPushCommand{}).Execute(db, [][]byte{[]byte("list"), []byte("list2")}), '$', "tail")
	assertRespString(t, command.DefaultRegistry.Execute(db, "LMOVE", [][]byte{
		[]byte("list"),
		[]byte("list2"),
		[]byte("LEFT"),
		[]byte("RIGHT"),
	}), '$', "h2")
	assertRespInt(t, command.DefaultRegistry.Execute(db, "LREM", [][]byte{
		[]byte("list2"),
		[]byte("1"),
		[]byte("tail"),
	}), 1)
	list2LPosResp := command.DefaultRegistry.Execute(db, "LPOS", [][]byte{
		[]byte("list2"),
		[]byte("tail"),
		[]byte("COUNT"),
		[]byte("0"),
	})
	if list2LPosResp.Type != '*' || len(list2LPosResp.Value.([]*protocol.Response)) != 0 {
		t.Fatalf("unexpected empty lpos resp=%#v", list2LPosResp)
	}
	assertRespString(t, (&command.LTrimCommand{}).Execute(db, [][]byte{[]byte("list"), []byte("0"), []byte("0")}), '+', "OK")
	assertRespString(t, (&command.LIndexCommand{}).Execute(db, [][]byte{[]byte("list"), []byte("0")}), '$', "yy")

	assertRespInt(t, (&command.HSetCommand{}).Execute(db, [][]byte{[]byte("hash"), []byte("field"), []byte("hello"), []byte("field2"), []byte("world")}), 2)
	assertRespInt(t, (&command.HSetNXCommand{}).Execute(db, [][]byte{[]byte("hash"), []byte("field"), []byte("hello")}), 0)
	assertRespInt(t, (&command.HSetNXCommand{}).Execute(db, [][]byte{[]byte("hash"), []byte("field"), []byte("blocked")}), 0)
	assertRespInt(t, (&command.HStrLenCommand{}).Execute(db, [][]byte{[]byte("hash"), []byte("field")}), 5)
	hrandResp := (&command.HRandFieldCommand{}).Execute(db, [][]byte{[]byte("hash")})
	if hrandResp.Type != '$' || (hrandResp.Value != "field" && hrandResp.Value != "field2") {
		t.Fatalf("unexpected hrandfield single=%#v", hrandResp)
	}
	hrandWithValuesResp := (&command.HRandFieldCommand{}).Execute(db, [][]byte{[]byte("hash"), []byte("2"), []byte("WITHVALUES")})
	if hrandWithValuesResp.Type != '*' {
		t.Fatalf("unexpected hrandfield resp=%#v", hrandWithValuesResp)
	}
	hrandWithValues := hrandWithValuesResp.Value.([]string)
	if len(hrandWithValues) != 4 {
		t.Fatalf("unexpected hrandfield withvalues payload=%v", hrandWithValues)
	}
	for i := 0; i < len(hrandWithValues); i += 2 {
		switch hrandWithValues[i] {
		case "field":
			if hrandWithValues[i+1] != "hello" {
				t.Fatalf("unexpected hrandfield pair=%v", hrandWithValues)
			}
		case "field2":
			if hrandWithValues[i+1] != "world" {
				t.Fatalf("unexpected hrandfield pair=%v", hrandWithValues)
			}
		default:
			t.Fatalf("unexpected hrandfield pair=%v", hrandWithValues)
		}
	}
	hscanResp := (&command.HScanCommand{}).Execute(db, [][]byte{
		[]byte("hash"),
		[]byte("0"),
		[]byte("MATCH"),
		[]byte("field*"),
		[]byte("COUNT"),
		[]byte("1"),
	})
	if hscanResp.Type != '*' {
		t.Fatalf("unexpected hscan resp=%#v", hscanResp)
	}
	hscanItems := hscanResp.Value.([]*protocol.Response)
	if len(hscanItems) != 2 || hscanItems[0].Type != '$' || hscanItems[1].Type != '*' {
		t.Fatalf("unexpected hscan payload=%#v", hscanResp.Value)
	}
	if fields := hscanItems[1].Value.([]string); len(fields) != 2 || fields[0] != "field" || fields[1] != "hello" {
		t.Fatalf("unexpected hscan fields=%v", fields)
	}

	timeResp := (&command.TimeCommand{}).Execute(db, nil)
	if timeResp.Type != '*' || len(timeResp.Value.([]string)) != 2 {
		t.Fatalf("unexpected time resp=%#v", timeResp)
	}
}

func assertRespString(t *testing.T, resp *protocol.Response, typ byte, expected string) {
	t.Helper()
	if resp.Type != typ || resp.Value != expected {
		t.Fatalf("unexpected resp type=%c value=%#v expected type=%c value=%q", resp.Type, resp.Value, typ, expected)
	}
}

func assertRespInt(t *testing.T, resp *protocol.Response, expected int64) {
	t.Helper()
	if resp.Type != ':' || resp.Value != expected {
		t.Fatalf("unexpected integer resp=%#v expected=%d", resp, expected)
	}
}
