package command_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/TZJ-BYTE/Kerniva/config"
	"github.com/TZJ-BYTE/Kerniva/internal/command"
	"github.com/TZJ-BYTE/Kerniva/internal/database"
)

func TestSetCommandSupportsGetExatPxatAndConditionalReturn(t *testing.T) {
	db := newCommandTestDB(t)

	assertRespString(t, (&command.SetExCommand{}).Execute(db, [][]byte{
		[]byte("k"),
		[]byte("60"),
		[]byte("old"),
	}), '+', "OK")

	assertRespString(t, (&command.SetCommand{}).Execute(db, [][]byte{
		[]byte("k"),
		[]byte("new"),
		[]byte("GET"),
	}), '$', "old")
	assertRespString(t, (&command.GetCommand{}).Execute(db, [][]byte{[]byte("k")}), '$', "new")
	if expireAt, exists := db.GetExpireTime("k"); !exists || expireAt != 0 {
		t.Fatalf("expected SET GET to clear ttl, exists=%v expireAt=%d", exists, expireAt)
	}

	nullResp := (&command.SetCommand{}).Execute(db, [][]byte{
		[]byte("fresh"),
		[]byte("value"),
		[]byte("GET"),
	})
	if nullResp.Type != '$' || nullResp.Value != nil {
		t.Fatalf("expected nil old value for missing key, got %#v", nullResp)
	}
	assertRespString(t, (&command.GetCommand{}).Execute(db, [][]byte{[]byte("fresh")}), '$', "value")

	assertRespString(t, (&command.SetCommand{}).Execute(db, [][]byte{
		[]byte("k"),
		[]byte("blocked"),
		[]byte("NX"),
		[]byte("GET"),
	}), '$', "new")
	assertRespString(t, (&command.GetCommand{}).Execute(db, [][]byte{[]byte("k")}), '$', "new")

	nullResp = (&command.SetCommand{}).Execute(db, [][]byte{
		[]byte("xx-miss"),
		[]byte("value"),
		[]byte("XX"),
		[]byte("GET"),
	})
	if nullResp.Type != '$' || nullResp.Value != nil {
		t.Fatalf("expected nil response for XX GET miss, got %#v", nullResp)
	}
	if db.Exists("xx-miss") {
		t.Fatalf("expected XX GET miss to keep key absent")
	}

	exat := time.Now().Unix() + 120
	assertRespString(t, (&command.SetCommand{}).Execute(db, [][]byte{
		[]byte("abs:sec"),
		[]byte("v1"),
		[]byte("EXAT"),
		[]byte(strconv.FormatInt(exat, 10)),
	}), '+', "OK")
	if expireAt, exists := db.GetExpireTime("abs:sec"); !exists || expireAt != exat*1000 {
		t.Fatalf("expected EXAT ttl=%d, exists=%v expireAt=%d", exat*1000, exists, expireAt)
	}

	pxat := time.Now().UnixMilli() + 120_000
	assertRespString(t, (&command.SetCommand{}).Execute(db, [][]byte{
		[]byte("abs:ms"),
		[]byte("v2"),
		[]byte("PXAT"),
		[]byte(strconv.FormatInt(pxat, 10)),
	}), '+', "OK")
	if expireAt, exists := db.GetExpireTime("abs:ms"); !exists || expireAt != pxat {
		t.Fatalf("expected PXAT ttl=%d, exists=%v expireAt=%d", pxat, exists, expireAt)
	}
}

func TestGetExPersistKeepsValueWithoutTTL(t *testing.T) {
	db := newCommandTestDB(t)

	assertRespString(t, (&command.SetCommand{}).Execute(db, [][]byte{
		[]byte("plain"),
		[]byte("value"),
	}), '+', "OK")

	assertRespString(t, (&command.GetExCommand{}).Execute(db, [][]byte{
		[]byte("plain"),
		[]byte("PERSIST"),
	}), '$', "value")
	if expireAt, exists := db.GetExpireTime("plain"); !exists || expireAt != 0 {
		t.Fatalf("expected GETEX PERSIST to keep key without ttl, exists=%v expireAt=%d", exists, expireAt)
	}

	pxat := time.Now().UnixMilli() + 90_000
	assertRespString(t, (&command.GetExCommand{}).Execute(db, [][]byte{
		[]byte("plain"),
		[]byte("PXAT"),
		[]byte(strconv.FormatInt(pxat, 10)),
	}), '$', "value")
	if expireAt, exists := db.GetExpireTime("plain"); !exists || expireAt != pxat {
		t.Fatalf("expected GETEX PXAT ttl=%d, exists=%v expireAt=%d", pxat, exists, expireAt)
	}
}

func TestGetSetAndSetRangeCompatibility(t *testing.T) {
	db := newCommandTestDB(t)

	assertRespString(t, (&command.SetExCommand{}).Execute(db, [][]byte{
		[]byte("ttl-key"),
		[]byte("60"),
		[]byte("before"),
	}), '+', "OK")
	assertRespString(t, (&command.GetSetCommand{}).Execute(db, [][]byte{
		[]byte("ttl-key"),
		[]byte("after"),
	}), '$', "before")
	assertRespString(t, (&command.GetCommand{}).Execute(db, [][]byte{[]byte("ttl-key")}), '$', "after")
	if expireAt, exists := db.GetExpireTime("ttl-key"); !exists || expireAt != 0 {
		t.Fatalf("expected GETSET to clear ttl, exists=%v expireAt=%d", exists, expireAt)
	}

	nullResp := (&command.GetSetCommand{}).Execute(db, [][]byte{
		[]byte("missing"),
		[]byte("created"),
	})
	if nullResp.Type != '$' || nullResp.Value != nil {
		t.Fatalf("expected nil old value for missing GETSET key, got %#v", nullResp)
	}
	assertRespString(t, (&command.GetCommand{}).Execute(db, [][]byte{[]byte("missing")}), '$', "created")

	assertRespInt(t, (&command.SetRangeCommand{}).Execute(db, [][]byte{
		[]byte("empty-range"),
		[]byte("0"),
		[]byte(""),
	}), 0)
	if db.Exists("empty-range") {
		t.Fatalf("expected empty SETRANGE on missing key to avoid creating the key")
	}

	assertRespString(t, (&command.SetCommand{}).Execute(db, [][]byte{
		[]byte("range"),
		[]byte("abc"),
	}), '+', "OK")
	assertRespInt(t, (&command.PExpireCommand{}).Execute(db, [][]byte{
		[]byte("range"),
		[]byte("60000"),
	}), 1)
	expireAt, exists := db.GetExpireTime("range")
	if !exists || expireAt <= time.Now().UnixMilli() {
		t.Fatalf("expected range ttl to exist before empty SETRANGE, exists=%v expireAt=%d", exists, expireAt)
	}

	assertRespInt(t, (&command.SetRangeCommand{}).Execute(db, [][]byte{
		[]byte("range"),
		[]byte("1"),
		[]byte(""),
	}), 3)
	assertRespString(t, (&command.GetCommand{}).Execute(db, [][]byte{[]byte("range")}), '$', "abc")
	if afterExpireAt, exists := db.GetExpireTime("range"); !exists || afterExpireAt != expireAt {
		t.Fatalf("expected empty SETRANGE to preserve ttl, exists=%v expireAt=%d want=%d", exists, afterExpireAt, expireAt)
	}

	assertRespString(t, (&command.GetRangeCommand{}).Execute(db, [][]byte{
		[]byte("range"),
		[]byte("-100"),
		[]byte("1"),
	}), '$', "ab")
	assertRespString(t, (&command.GetRangeCommand{}).Execute(db, [][]byte{
		[]byte("range"),
		[]byte("2"),
		[]byte("100"),
	}), '$', "c")
}

func newCommandTestDB(t *testing.T) *database.Database {
	t.Helper()

	cfg := config.DefaultConfig()
	cfg.DataDir = t.TempDir()
	manager := database.NewDBManager(cfg)
	t.Cleanup(func() {
		_ = manager.Close()
	})
	return manager.GetDefaultDB()
}
