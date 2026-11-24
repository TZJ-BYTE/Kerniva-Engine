package rustengine

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strings"
	"testing"
)

func TestRestoreGoLogDeltaBlock(t *testing.T) {
	large := bytes.Repeat([]byte{0, 255}, 1<<19)
	block := appendTestDeltaRecord(nil, 1, []byte{0, 255}, large)
	block = appendTestDeltaRecord(block, 2, []byte("deleted"), nil)
	block = appendTestDeltaRecord(block, 1, nil, nil)

	type record struct {
		key, value []byte
		deleted    bool
	}
	var records []record
	err := restoreGoLogDeltaBlock(block, func(key, value []byte, deleted bool) error {
		records = append(records, record{key, value, deleted})
		if len(records) == 1 {
			key[0] = 3
			value[0] = 4
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if block[9] != 0 || block[11] != 0 {
		t.Fatal("callback modified the source block")
	}
	clear(block)
	if len(records) != 3 {
		t.Fatalf("got %d records, want 3", len(records))
	}
	large[0] = 4
	if records[0].deleted || !bytes.Equal(records[0].key, []byte{3, 255}) || !bytes.Equal(records[0].value, large) {
		t.Fatal("binary put changed after its source block was cleared")
	}
	if !records[1].deleted || string(records[1].key) != "deleted" || records[1].value != nil {
		t.Fatalf("invalid delete: %+v", records[1])
	}
	if records[2].deleted || len(records[2].key) != 0 || len(records[2].value) != 0 {
		t.Fatalf("invalid empty put: %+v", records[2])
	}
}

func TestRestoreGoLogDeltaRejectsMalformedBlocks(t *testing.T) {
	for _, test := range []struct {
		name  string
		block []byte
		want  string
	}{
		{"empty", nil, "empty"},
		{"header", []byte{1, 0, 0}, "truncated Go-log delta record header"},
		{"body", appendTestDeltaRecord(nil, 1, []byte("key"), nil)[:11], "truncated Go-log delta record body"},
		{"overflow", []byte{1, 255, 255, 255, 255, 1, 0, 0, 0}, "truncated Go-log delta record body"},
		{"kind", appendTestDeltaRecord(nil, 3, nil, nil), "invalid Go-log delta record kind"},
		{"delete value", appendTestDeltaRecord(nil, 2, nil, []byte("unexpected")), "delete record has a value"},
		{"trailing", append(appendTestDeltaRecord(nil, 1, nil, nil), 0), "truncated Go-log delta record header"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := restoreGoLogDeltaBlock(test.block, nil); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("got %v, want error containing %q", err, test.want)
			}
		})
	}
}

func TestRestoreGoLogDeltaStopsOnCallbackError(t *testing.T) {
	block := appendTestDeltaRecord(nil, 1, []byte("first"), nil)
	block = appendTestDeltaRecord(block, 2, []byte("second"), nil)
	wantErr := errors.New("stop recovery")
	var calls int
	err := restoreGoLogDeltaBlock(block, func(_, _ []byte, _ bool) error {
		calls++
		return wantErr
	})
	if !errors.Is(err, wantErr) || calls != 1 {
		t.Fatalf("got %d calls and error %v, want one call and %v", calls, err, wantErr)
	}
}

func appendTestDeltaRecord(block []byte, kind byte, key, value []byte) []byte {
	block = append(block, kind)
	block = binary.LittleEndian.AppendUint32(block, uint32(len(key)))
	block = binary.LittleEndian.AppendUint32(block, uint32(len(value)))
	block = append(block, key...)
	return append(block, value...)
}
