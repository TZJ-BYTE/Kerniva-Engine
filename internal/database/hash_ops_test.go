package database

import "testing"

func TestHashOps_HMGetWithExists(t *testing.T) {
	db := NewDatabase(0)
	if _, err := db.HSet("h", "f1", "v1"); err != nil {
		t.Fatalf("hset: %v", err)
	}
	values, ok, err := db.HMGetWithExists("h", []string{"f1", "f2"})
	if err != nil {
		t.Fatalf("hmget: %v", err)
	}
	if len(values) != 2 || len(ok) != 2 {
		t.Fatalf("lens")
	}
	if !ok[0] || values[0] != "v1" {
		t.Fatalf("f1")
	}
	if ok[1] {
		t.Fatalf("f2 should be missing")
	}
}

func TestHashOps_HValsFollowFieldOrder(t *testing.T) {
	db := NewDatabase(0)
	if _, err := db.HSet("h", "b", "2"); err != nil {
		t.Fatalf("hset b: %v", err)
	}
	if _, err := db.HSet("h", "a", "1"); err != nil {
		t.Fatalf("hset a: %v", err)
	}

	keys, err := db.HKeys("h")
	if err != nil {
		t.Fatalf("hkeys: %v", err)
	}
	if len(keys) != 2 || keys[0] != "a" || keys[1] != "b" {
		t.Fatalf("unexpected keys: %v", keys)
	}

	values, err := db.HVals("h")
	if err != nil {
		t.Fatalf("hvals: %v", err)
	}
	if len(values) != 2 || values[0] != "1" || values[1] != "2" {
		t.Fatalf("unexpected values: %v", values)
	}
}

func TestHashOps_HScanMatchAndCount(t *testing.T) {
	db := NewDatabase(0)
	fixtures := []struct {
		field string
		value string
	}{
		{field: "alpha", value: "1"},
		{field: "beta", value: "2"},
		{field: "bravo", value: "3"},
		{field: "charlie", value: "4"},
	}
	for _, fixture := range fixtures {
		if _, err := db.HSet("h", fixture.field, fixture.value); err != nil {
			t.Fatalf("hset %s: %v", fixture.field, err)
		}
	}

	next, items, err := db.HScan("h", 0, "b*", 1)
	if err != nil {
		t.Fatalf("hscan first: %v", err)
	}
	if next != 2 {
		t.Fatalf("unexpected first cursor: %d", next)
	}
	if len(items) != 2 || items[0] != "beta" || items[1] != "2" {
		t.Fatalf("unexpected first items: %v", items)
	}

	next, items, err = db.HScan("h", next, "b*", 1)
	if err != nil {
		t.Fatalf("hscan second: %v", err)
	}
	if next != 3 {
		t.Fatalf("unexpected second cursor: %d", next)
	}
	if len(items) != 2 || items[0] != "bravo" || items[1] != "3" {
		t.Fatalf("unexpected second items: %v", items)
	}

	next, items, err = db.HScan("h", next, "b*", 1)
	if err != nil {
		t.Fatalf("hscan third: %v", err)
	}
	if next != 0 {
		t.Fatalf("unexpected third cursor: %d", next)
	}
	if len(items) != 0 {
		t.Fatalf("expected empty third page, got %v", items)
	}
}

func TestHashOps_HRandFieldVariants(t *testing.T) {
	db := NewDatabase(0)
	if _, err := db.HSet("h", "f1", "v1"); err != nil {
		t.Fatalf("hset f1: %v", err)
	}
	if _, err := db.HSet("h", "f2", "v2"); err != nil {
		t.Fatalf("hset f2: %v", err)
	}

	field, ok, err := db.HRandField("h")
	if err != nil {
		t.Fatalf("hrandfield single: %v", err)
	}
	if !ok || (field != "f1" && field != "f2") {
		t.Fatalf("unexpected single field: %q ok=%v", field, ok)
	}

	fields, err := db.HRandFields("h", 2, false)
	if err != nil {
		t.Fatalf("hrandfields: %v", err)
	}
	if len(fields) != 2 {
		t.Fatalf("unexpected field count: %v", fields)
	}
	seen := map[string]bool{}
	for _, f := range fields {
		if f != "f1" && f != "f2" {
			t.Fatalf("unexpected field entry: %q", f)
		}
		seen[f] = true
	}
	if len(seen) != 2 {
		t.Fatalf("expected unique fields, got %v", fields)
	}

	withValues, err := db.HRandFields("h", -3, true)
	if err != nil {
		t.Fatalf("hrandfields with values: %v", err)
	}
	if len(withValues) != 6 {
		t.Fatalf("unexpected withvalues payload: %v", withValues)
	}
	for i := 0; i < len(withValues); i += 2 {
		switch withValues[i] {
		case "f1":
			if withValues[i+1] != "v1" {
				t.Fatalf("unexpected value pair: %v", withValues)
			}
		case "f2":
			if withValues[i+1] != "v2" {
				t.Fatalf("unexpected value pair: %v", withValues)
			}
		default:
			t.Fatalf("unexpected field/value pair: %v", withValues)
		}
	}
}
