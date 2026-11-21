package database

import (
	"testing"

	"github.com/TZJ-BYTE/Kerniva/internal/datastruct"
)

func requireGroupState(t *testing.T, db *Database, lastID string, pending int) *datastruct.ConsumerGroup {
	t.Helper()
	value, found := db.Get("stream")
	if !found {
		t.Fatal("stream is missing")
	}
	group := value.Value.(*datastruct.Stream).FindGroup("group")
	if group == nil || group.LastDeliveredID != lastID || len(group.Pel) != pending {
		t.Fatalf("group = %#v, want last ID %q and %d pending entries", group, lastID, pending)
	}
	return group
}

func TestReadGroupHistoryUsesCurrentConsumerPending(t *testing.T) {
	db, cfg := coldStreamDatabase(t)
	if _, err := db.XAdd("stream", "1002-0", []string{"field", "third"}, -1, false); err != nil {
		t.Fatal(err)
	}
	if entries, err := db.XReadGroup("group", "other", "stream", ">", 1, 0, false); err != nil || len(entries) != 1 || entries[0].ID != "1001-0" {
		t.Fatalf("other consumer = %v, %v", entries, err)
	}
	entries, err := db.XReadGroup("group", "consumer", "stream", "0", 0, 1000, true)
	if err != nil || len(entries) != 1 || entries[0].ID != "1000-0" {
		t.Fatalf("history = %v, %v", entries, err)
	}
	group := requireGroupState(t, db, "1001-0", 2)
	if pending := group.FindPelEntry("1000-0"); pending == nil || pending.DeliveryCount != 2 {
		t.Fatalf("redelivered entry = %#v", pending)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = reopenColdStreamDatabase(t, cfg)
	entries, err = db.XReadGroup("group", "consumer", "stream", "0-0", 0, 0, false)
	if err != nil || len(entries) != 1 || entries[0].ID != "1000-0" {
		t.Fatalf("reopened history = %v, %v", entries, err)
	}
	group = requireGroupState(t, db, "1001-0", 2)
	if pending := group.FindPelEntry("1000-0"); pending == nil || pending.DeliveryCount != 3 {
		t.Fatalf("persisted delivery count = %#v", pending)
	}
	if count, err := db.XAck("stream", "group", []string{"1000-0"}); err != nil || count != 1 {
		t.Fatalf("ack = %d, %v", count, err)
	}
	if entries, err := db.XReadGroup("group", "consumer", "stream", "0", 0, 0, false); err != nil || len(entries) != 0 {
		t.Fatalf("acknowledged history = %v, %v", entries, err)
	}
	if entries, err := db.XReadGroup("group", "consumer", "stream", ">", 0, 0, false); err != nil || len(entries) != 1 || entries[0].ID != "1002-0" {
		t.Fatalf("new messages after history = %v, %v", entries, err)
	}
}

func TestReadGroupDeletedPendingPayloadIsNil(t *testing.T) {
	db, cfg := coldStreamDatabase(t)
	if count, err := db.XDel("stream", []string{"1000-0"}); err != nil || count != 1 {
		t.Fatalf("delete pending payload = %d, %v", count, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = reopenColdStreamDatabase(t, cfg)
	entries, err := db.XReadGroup("group", "consumer", "stream", "0", 0, 0, true)
	if err != nil || len(entries) != 1 || entries[0].ID != "1000-0" || entries[0].Fields != nil {
		t.Fatalf("deleted pending history = %#v, %v", entries, err)
	}
	requireGroupState(t, db, "1000-0", 1)
}

func TestGroupSetIDValidatesBeforeMutationAndResolvesDollar(t *testing.T) {
	db, _ := coldStreamDatabase(t)
	before := db.KeyVersion("stream")
	for _, id := range []string{"", ">", "garbage", "1-2-3", "1-*", "18446744073709551616-0"} {
		if err := db.XGroupSetID("stream", "group", id); err == nil {
			t.Fatalf("SETID accepted %q", id)
		}
		if db.KeyVersion("stream") != before {
			t.Fatalf("invalid SETID %q changed key version", id)
		}
		requireGroupState(t, db, "1000-0", 1)
	}
	if err := db.XGroupSetID("stream", "group", "1000"); err != nil {
		t.Fatal(err)
	}
	requireGroupState(t, db, "1000-0", 1)
	if _, err := db.XDel("stream", []string{"1000-0", "1001-0"}); err != nil {
		t.Fatal(err)
	}
	if err := db.XGroupSetID("stream", "group", "$"); err != nil {
		t.Fatal(err)
	}
	requireGroupState(t, db, "1001-0", 1)
	if _, err := db.XReadGroup("group", "consumer", "stream", "$", 0, 0, false); err == nil {
		t.Fatal("XREADGROUP accepted $")
	}
	if err := db.XGroupCreate("invalid", "group", "bad-id", true); err == nil || db.Exists("invalid") {
		t.Fatalf("invalid CREATE left a stream, error = %v", err)
	}
}

func TestReadGroupResetUpdatesExistingPendingOwner(t *testing.T) {
	db, _ := coldStreamDatabase(t)
	if err := db.XGroupSetID("stream", "group", "0"); err != nil {
		t.Fatal(err)
	}
	entries, err := db.XReadGroup("group", "new-owner", "stream", ">", 1, 0, false)
	if err != nil || len(entries) != 1 || entries[0].ID != "1000-0" {
		t.Fatalf("read after reset = %v, %v", entries, err)
	}
	group := requireGroupState(t, db, "1000-0", 1)
	if pending := group.FindPelEntry("1000-0"); pending.ConsumerName != "new-owner" || pending.DeliveryCount != 1 {
		t.Fatalf("reassigned pending = %#v", pending)
	}
	if history, err := db.XReadGroup("group", "consumer", "stream", "0", 0, 0, false); err != nil || len(history) != 0 {
		t.Fatalf("previous owner history = %v, %v", history, err)
	}
}

func TestReadGroupDeduplicatesLegacyPersistedPending(t *testing.T) {
	db, cfg := coldStreamDatabase(t)
	value, _ := db.Get("stream")
	group := value.Value.(*datastruct.Stream).FindGroup("group")
	duplicate := group.Pel[0]
	duplicate.ConsumerName = "latest-owner"
	duplicate.DeliveredAt++
	group.Pel = append(group.Pel, duplicate)
	if err := db.Set("stream", value); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = reopenColdStreamDatabase(t, cfg)
	entries, err := db.XReadGroup("group", "latest-owner", "stream", "0", 0, 0, false)
	if err != nil || len(entries) != 1 {
		t.Fatalf("legacy history = %v, %v", entries, err)
	}
	group = requireGroupState(t, db, "1000-0", 1)
	if pending := group.Pel[0]; pending.ConsumerName != "latest-owner" || pending.DeliveryCount != 2 {
		t.Fatalf("normalized pending = %#v", pending)
	}
}
