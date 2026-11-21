package command_test

import (
	"reflect"
	"testing"

	"github.com/TZJ-BYTE/Kerniva/config"
	"github.com/TZJ-BYTE/Kerniva/internal/command"
	"github.com/TZJ-BYTE/Kerniva/internal/database"
	"github.com/TZJ-BYTE/Kerniva/internal/protocol"
)

func TestZSetAggregateCommands(t *testing.T) {
	command.InitDefaultCommands()
	cfg := config.DefaultConfig()
	cfg.DataDir = t.TempDir()
	manager := database.NewDBManager(cfg)
	defer manager.Close()
	db := manager.GetDefaultDB()

	assertRespInt(t, command.DefaultRegistry.Execute(db, "ZADD", [][]byte{
		[]byte("z1"),
		[]byte("1"), []byte("a"),
		[]byte("2"), []byte("b"),
		[]byte("3"), []byte("c"),
	}), 3)
	assertRespInt(t, command.DefaultRegistry.Execute(db, "ZADD", [][]byte{
		[]byte("z2"),
		[]byte("4"), []byte("b"),
		[]byte("1"), []byte("c"),
		[]byte("2"), []byte("d"),
	}), 3)
	assertRespInt(t, command.DefaultRegistry.Execute(db, "ZADD", [][]byte{
		[]byte("z3"),
		[]byte("1"), []byte("b"),
		[]byte("5"), []byte("c"),
		[]byte("6"), []byte("e"),
	}), 3)

	assertRespArray(t, command.DefaultRegistry.Execute(db, "ZUNION", [][]byte{
		[]byte("2"),
		[]byte("z1"),
		[]byte("z2"),
		[]byte("WITHSCORES"),
	}), []string{"a", "1", "d", "2", "c", "4", "b", "6"})

	assertRespArray(t, command.DefaultRegistry.Execute(db, "ZINTER", [][]byte{
		[]byte("3"),
		[]byte("z1"),
		[]byte("z2"),
		[]byte("z3"),
		[]byte("WEIGHTS"),
		[]byte("1"),
		[]byte("2"),
		[]byte("3"),
		[]byte("AGGREGATE"),
		[]byte("MAX"),
		[]byte("WITHSCORES"),
	}), []string{"b", "8", "c", "15"})

	assertRespArray(t, command.DefaultRegistry.Execute(db, "ZDIFF", [][]byte{
		[]byte("2"),
		[]byte("z1"),
		[]byte("z2"),
		[]byte("WITHSCORES"),
	}), []string{"a", "1"})

	assertRespInt(t, command.DefaultRegistry.Execute(db, "ZUNIONSTORE", [][]byte{
		[]byte("zu:dst"),
		[]byte("2"),
		[]byte("z1"),
		[]byte("z2"),
		[]byte("WEIGHTS"),
		[]byte("2"),
		[]byte("1"),
		[]byte("AGGREGATE"),
		[]byte("MIN"),
	}), 4)
	assertRespArray(t, command.DefaultRegistry.Execute(db, "ZRANGE", [][]byte{
		[]byte("zu:dst"),
		[]byte("0"),
		[]byte("-1"),
		[]byte("WITHSCORES"),
	}), []string{"c", "1", "a", "2", "d", "2", "b", "4"})

	assertRespInt(t, command.DefaultRegistry.Execute(db, "ZINTERSTORE", [][]byte{
		[]byte("zi:dst"),
		[]byte("2"),
		[]byte("z1"),
		[]byte("z2"),
		[]byte("WEIGHTS"),
		[]byte("2"),
		[]byte("1"),
		[]byte("AGGREGATE"),
		[]byte("MIN"),
	}), 2)
	assertRespArray(t, command.DefaultRegistry.Execute(db, "ZRANGE", [][]byte{
		[]byte("zi:dst"),
		[]byte("0"),
		[]byte("-1"),
		[]byte("WITHSCORES"),
	}), []string{"c", "1", "b", "4"})

	assertRespInt(t, command.DefaultRegistry.Execute(db, "ZDIFFSTORE", [][]byte{
		[]byte("zd:dst"),
		[]byte("2"),
		[]byte("z1"),
		[]byte("z2"),
	}), 1)
	assertRespArray(t, command.DefaultRegistry.Execute(db, "ZRANGE", [][]byte{
		[]byte("zd:dst"),
		[]byte("0"),
		[]byte("-1"),
		[]byte("WITHSCORES"),
	}), []string{"a", "1"})

	assertRespInt(t, command.DefaultRegistry.Execute(db, "ZADD", [][]byte{
		[]byte("zd:gone"),
		[]byte("9"),
		[]byte("stale"),
	}), 1)
	assertRespInt(t, command.DefaultRegistry.Execute(db, "ZDIFFSTORE", [][]byte{
		[]byte("zd:gone"),
		[]byte("2"),
		[]byte("z1"),
		[]byte("z1"),
	}), 0)
	assertRespString(t, (&command.TypeCommand{}).Execute(db, [][]byte{[]byte("zd:gone")}), '+', "none")
}

func assertRespArray(t *testing.T, resp *protocol.Response, expected []string) {
	t.Helper()
	if resp.Type != '*' {
		t.Fatalf("unexpected array resp=%#v", resp)
	}
	got, ok := resp.Value.([]string)
	if !ok {
		t.Fatalf("unexpected array payload type=%T value=%#v", resp.Value, resp.Value)
	}
	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("unexpected array payload=%v expected=%v", got, expected)
	}
}
