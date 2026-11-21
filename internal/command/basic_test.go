package command_test

import (
	"testing"

	"github.com/TZJ-BYTE/Kerniva/config"
	"github.com/TZJ-BYTE/Kerniva/internal/command"
	"github.com/TZJ-BYTE/Kerniva/internal/database"
	"github.com/TZJ-BYTE/Kerniva/internal/datastruct"
)

func TestSetAndGet(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.DataDir = t.TempDir()
	dbManager := database.NewDBManager(cfg)
	db := dbManager.GetDefaultDB()

	// 测试 SET
	setCmd := &command.SetCommand{}
	resp := setCmd.Execute(db, [][]byte{[]byte("name"), []byte("Alice")})
	if resp.Type != '+' {
		t.Errorf("SET 命令返回错误类型：%v", resp.Type)
	}

	// 测试 GET
	getCmd := &command.GetCommand{}
	resp = getCmd.Execute(db, [][]byte{[]byte("name")})
	if resp.Type != '$' {
		t.Errorf("GET 命令返回错误类型：%v", resp.Type)
	}
	if resp.Value != "Alice" {
		t.Errorf("GET 返回值错误，期望：Alice, 实际：%v", resp.Value)
	}
}

func TestDel(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.DataDir = t.TempDir()
	dbManager := database.NewDBManager(cfg)
	db := dbManager.GetDefaultDB()

	// 先设置一个值
	db.Set("testkey", &datastruct.DataValue{
		Value:      &datastruct.String{Data: "testvalue"},
		ExpireTime: 0,
	})

	// 测试 DEL
	delCmd := &command.DelCommand{}
	resp := delCmd.Execute(db, [][]byte{[]byte("testkey")})
	if resp.Type != ':' {
		t.Errorf("DEL 命令返回错误类型：%v", resp.Type)
	}
	if resp.Value != int64(1) {
		t.Errorf("DEL 返回值错误，期望：1, 实际：%v", resp.Value)
	}
}

func TestExists(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.DataDir = t.TempDir()
	dbManager := database.NewDBManager(cfg)
	db := dbManager.GetDefaultDB()

	// 先设置一个值
	db.Set("existkey", &datastruct.DataValue{
		Value:      &datastruct.String{Data: "existvalue"},
		ExpireTime: 0,
	})

	// 测试 EXISTS
	existsCmd := &command.ExistsCommand{}
	resp := existsCmd.Execute(db, [][]byte{[]byte("existkey")})
	if resp.Type != ':' {
		t.Errorf("EXISTS 命令返回错误类型：%v", resp.Type)
	}
	if resp.Value != int64(1) {
		t.Errorf("EXISTS 返回值错误，期望：1, 实际：%v", resp.Value)
	}
}

func TestLPushAndLPop(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.DataDir = t.TempDir()
	dbManager := database.NewDBManager(cfg)
	db := dbManager.GetDefaultDB()

	// 测试 LPUSH
	lpushCmd := &command.LPushCommand{}
	resp := lpushCmd.Execute(db, [][]byte{[]byte("mylist"), []byte("a"), []byte("b"), []byte("c")})
	if resp.Type != ':' {
		t.Errorf("LPUSH 命令返回错误类型：%v", resp.Type)
	}
	if resp.Value != int64(3) {
		t.Errorf("LPUSH 返回值错误，期望：3, 实际：%v", resp.Value)
	}

	// 测试 LPOP
	lpopCmd := &command.LPopCommand{}
	resp = lpopCmd.Execute(db, [][]byte{[]byte("mylist")})
	if resp.Type != '$' {
		t.Errorf("LPOP 命令返回错误类型：%v", resp.Type)
	}
	if resp.Value != "c" {
		t.Errorf("LPOP 返回值错误，期望：c, 实际：%v", resp.Value)
	}
}

func TestRandomKeyTouchAndUnlink(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.DataDir = t.TempDir()
	dbManager := database.NewDBManager(cfg)
	db := dbManager.GetDefaultDB()

	if err := db.Set("only", &datastruct.DataValue{
		Value:      &datastruct.String{Data: "value"},
		ExpireTime: 0,
	}); err != nil {
		t.Fatalf("set only: %v", err)
	}

	randomResp := (&command.RandomKeyCommand{}).Execute(db, nil)
	if randomResp.Type != '$' || randomResp.Value != "only" {
		t.Fatalf("RANDOMKEY 返回错误，resp=%#v", randomResp)
	}

	touchResp := (&command.TouchCommand{}).Execute(db, [][]byte{[]byte("only"), []byte("missing")})
	if touchResp.Type != ':' || touchResp.Value != int64(1) {
		t.Fatalf("TOUCH 返回错误，resp=%#v", touchResp)
	}

	unlinkResp := (&command.UnlinkCommand{}).Execute(db, [][]byte{[]byte("only"), []byte("missing")})
	if unlinkResp.Type != ':' || unlinkResp.Value != int64(1) {
		t.Fatalf("UNLINK 返回错误，resp=%#v", unlinkResp)
	}
	if db.Exists("only") {
		t.Fatalf("expected key deleted after UNLINK")
	}
}

func TestDumpAndRestore(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.DataDir = t.TempDir()
	dbManager := database.NewDBManager(cfg)
	db := dbManager.GetDefaultDB()

	setResp := (&command.SetCommand{}).Execute(db, [][]byte{[]byte("src"), []byte("value")})
	if setResp.Type != '+' {
		t.Fatalf("SET resp=%#v", setResp)
	}

	dumpResp := (&command.DumpCommand{}).Execute(db, [][]byte{[]byte("src")})
	if dumpResp.Type != '$' || dumpResp.Value == nil {
		t.Fatalf("DUMP resp=%#v", dumpResp)
	}

	restoreResp := (&command.RestoreCommand{}).Execute(db, [][]byte{
		[]byte("dst"),
		[]byte("0"),
		[]byte(dumpResp.Value.(string)),
	})
	if restoreResp.Type != '+' || restoreResp.Value != "OK" {
		t.Fatalf("RESTORE resp=%#v", restoreResp)
	}

	getResp := (&command.GetCommand{}).Execute(db, [][]byte{[]byte("dst")})
	if getResp.Type != '$' || getResp.Value != "value" {
		t.Fatalf("GET dst resp=%#v", getResp)
	}
}
