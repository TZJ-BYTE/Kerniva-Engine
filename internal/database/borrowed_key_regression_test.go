package database

import (
	"encoding/binary"
	"testing"

	"github.com/TZJ-BYTE/Kerniva/internal/datastruct"
)

func TestSetBytesReplacementOwnsItsKey(t *testing.T) {
	db := NewDatabase(0)
	defer db.Close()
	key := []byte("target")
	for _, value := range []string{"first", "second"} {
		if err := db.SetBytes(key, &datastruct.DataValue{Value: &datastruct.String{Data: value}}); err != nil {
			t.Fatal(err)
		}
	}
	copy(key, "broken")
	requireString(t, db, "target", "second")
}

func TestRenameReplacementOwnsItsKey(t *testing.T) {
	db := NewDatabase(0)
	defer db.Close()
	if err := db.SetStringBytes([]byte("source"), []byte("source-value")); err != nil {
		t.Fatal(err)
	}
	if err := db.SetStringBytes([]byte("target"), []byte("old-value")); err != nil {
		t.Fatal(err)
	}
	key := []byte("target")
	if renamed, err := db.Rename("source", bytesToString(key), false); err != nil || !renamed {
		t.Fatalf("RENAME=%v err=%v", renamed, err)
	}
	copy(key, "broken")
	requireString(t, db, "target", "source-value")
}

func TestRestoreRejectsMalformedCountAndPreservesExistingValue(t *testing.T) {
	db := NewDatabase(0)
	defer db.Close()
	if err := db.SetStringBytes([]byte("target"), []byte("old-value")); err != nil {
		t.Fatal(err)
	}
	payload := append([]byte{3}, make([]byte, 16)...)
	payload = append(payload, 3)
	payload = binary.LittleEndian.AppendUint32(payload, ^uint32(0))
	if err := db.RestoreSerializedValue("target", payload, 0, true); err == nil {
		t.Fatal("RESTORE accepted a malformed count")
	}
	requireString(t, db, "target", "old-value")
}
