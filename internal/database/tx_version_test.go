package database

import (
	"fmt"
	"testing"
)

func watchKeyForTest(t *testing.T, db *Database, key string) uint64 {
	t.Helper()
	version := db.WatchKeyVersion(key)
	t.Cleanup(func() { db.UnwatchKeyVersion(key) })
	return version
}

func TestUnwatchedKeyChurnDoesNotRetainVersions(t *testing.T) {
	db := NewDatabase(0)
	defer db.Close()
	for i := range 512 {
		key := fmt.Sprintf("churn:%d", i)
		if err := db.SetStringBytes([]byte(key), []byte("value")); err != nil {
			t.Fatal(err)
		}
		if deleted, err := db.DeleteWithError(key); err != nil || !deleted {
			t.Fatalf("DEL %s = %t, %v", key, deleted, err)
		}
	}
	if db.Size() != 0 {
		t.Fatal("churn left live keys")
	}
	for i := range db.versions {
		if versions := db.versions[i].keys; versions != nil {
			t.Fatalf("shard %d retained %d version entries", i, len(versions))
		}
	}
}
