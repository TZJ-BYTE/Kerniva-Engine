package database

import "testing"

func coldStreamDatabase(t *testing.T) (*Database, *DatabaseConfig) {
	t.Helper()
	cfg := &DatabaseConfig{
		Type: Persistent, DataDir: t.TempDir(), ColdStartStrategy: "lazy_load",
		Durability: "sync", WriteMode: "strong",
	}
	db, err := NewDatabaseWithConfig(0, cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"1000-0", "1001-0"} {
		if _, err := db.XAdd("stream", id, []string{"field", id}, -1, false); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.XGroupCreate("stream", "group", "0-0", false); err != nil {
		t.Fatal(err)
	}
	if entries, err := db.XReadGroup("group", "consumer", "stream", ">", 1, 0, false); err != nil || len(entries) != 1 {
		t.Fatalf("seed pending entry = %v, %v", entries, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	return reopenColdStreamDatabase(t, cfg), cfg
}

func reopenColdStreamDatabase(t *testing.T, cfg *DatabaseConfig) *Database {
	t.Helper()
	db, err := NewDatabaseWithConfig(0, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func requirePendingCount(t *testing.T, db *Database, expected int64) {
	t.Helper()
	value, err := db.XPending("stream", "group", "", "", 0, "")
	if err != nil {
		t.Fatal(err)
	}
	summary, ok := value.(XPendingSummary)
	if !ok || summary.Total != expected {
		t.Fatalf("pending = %#v, want %d", value, expected)
	}
}

func TestColdStreamCommandsPreserveStateAcrossRestart(t *testing.T) {
	tests := []struct {
		name   string
		run    func(*testing.T, *Database)
		verify func(*testing.T, *Database)
	}{
		{
			name: "read",
			run: func(t *testing.T, db *Database) {
				values, err := db.XRead([]string{"stream"}, []string{"0-0"}, 0)
				if err != nil || len(values) != 1 || len(values[0].Entries) != 2 {
					t.Fatalf("read = %v, %v", values, err)
				}
			},
		},
		{
			name: "create-group-with-mkstream",
			run: func(t *testing.T, db *Database) {
				if err := db.XGroupCreate("stream", "new-group", "0-0", true); err != nil {
					t.Fatal(err)
				}
			},
			verify: func(t *testing.T, db *Database) {
				if length, err := db.XLen("stream"); err != nil || length != 2 {
					t.Fatalf("stream length = %d, %v", length, err)
				}
			},
		},
		{
			name: "destroy-group",
			run: func(t *testing.T, db *Database) {
				if removed, err := db.XGroupDestroy("stream", "group"); err != nil || removed != 1 {
					t.Fatalf("destroy = %d, %v", removed, err)
				}
			},
			verify: func(t *testing.T, db *Database) {
				if value, err := db.XPending("stream", "group", "", "", 0, ""); err != nil || value != nil {
					t.Fatalf("destroyed group = %#v, %v", value, err)
				}
			},
		},
		{
			name: "set-group-id",
			run: func(t *testing.T, db *Database) {
				if err := db.XGroupSetID("stream", "group", "1001-0"); err != nil {
					t.Fatal(err)
				}
			},
			verify: func(t *testing.T, db *Database) {
				entries, err := db.XReadGroup("group", "consumer", "stream", ">", 0, 0, false)
				if err != nil || len(entries) != 0 {
					t.Fatalf("read after set-id = %v, %v", entries, err)
				}
			},
		},
		{
			name: "read-group",
			run: func(t *testing.T, db *Database) {
				entries, err := db.XReadGroup("group", "consumer", "stream", ">", 0, 0, false)
				if err != nil || len(entries) != 1 || entries[0].ID != "1001-0" {
					t.Fatalf("read-group = %v, %v", entries, err)
				}
			},
			verify: func(t *testing.T, db *Database) { requirePendingCount(t, db, 2) },
		},
		{
			name: "acknowledge",
			run: func(t *testing.T, db *Database) {
				if count, err := db.XAck("stream", "group", []string{"1000-0"}); err != nil || count != 1 {
					t.Fatalf("acknowledge = %d, %v", count, err)
				}
			},
			verify: func(t *testing.T, db *Database) { requirePendingCount(t, db, 0) },
		},
		{
			name: "pending",
			run:  func(t *testing.T, db *Database) { requirePendingCount(t, db, 1) },
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, cfg := coldStreamDatabase(t)
			test.run(t, db)
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if test.verify != nil {
				test.verify(t, reopenColdStreamDatabase(t, cfg))
			}
		})
	}
}

func TestColdMGetStringsLoadsPersistedValues(t *testing.T) {
	db, cfg := persistentTestDB(t, 4096)
	if err := db.MSetStringBytes([][]byte{[]byte("first"), []byte("one"), []byte("second"), []byte("two")}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = reopenColdStreamDatabase(t, cfg)
	values := db.MGetStrings([][]byte{[]byte("second"), []byte("missing"), []byte("first"), []byte("second")})
	want := []string{"two", "", "one", "two"}
	for i := range want {
		if values[i] != want[i] {
			t.Fatalf("MGET[%d] = %q, want %q", i, values[i], want[i])
		}
	}
}

func TestXReadRejectsUnbalancedArguments(t *testing.T) {
	db := NewDatabase(0)
	defer db.Close()
	if _, err := db.XRead([]string{"stream"}, nil, 0); err == nil {
		t.Fatal("XREAD accepted a missing stream ID")
	}
	if _, err := db.XRead(nil, nil, 0); err == nil {
		t.Fatal("XREAD accepted an empty stream list")
	}
}
