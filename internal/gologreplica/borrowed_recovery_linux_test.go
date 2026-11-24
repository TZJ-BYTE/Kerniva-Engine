//go:build linux

package gologreplica

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/TZJ-BYTE/Kerniva/internal/datastruct"
)

func TestFullRecoveryDecoderOwnsAllV3Values(t *testing.T) {
	stream := &datastruct.Stream{
		LastMS: 12, LastSeq: 8,
		Entries: []datastruct.StreamEntry{{ID: "12-8", Fields: []string{"field", "value"}}},
		Groups: []datastruct.ConsumerGroup{{
			Name: "workers", LastDeliveredID: "12-8",
			Pel:       []datastruct.PendingEntry{{ID: "12-8", ConsumerName: "reader", DeliveredAt: 11, DeliveryCount: 2}},
			Consumers: []datastruct.Consumer{{Name: "reader", SeenTime: 19}},
		}},
	}
	for _, tc := range []struct {
		name  string
		value any
	}{
		{"string", &datastruct.String{Data: "plain\x00text"}},
		{"bytes", &datastruct.BytesString{Data: []byte{0, 255, 1}}},
		{"list", &datastruct.List{Data: []string{"first", "second"}}},
		{"hash", &datastruct.Hash{Data: map[string]string{"a-field": "value A", "b-field": "value B"}}},
		{"set", &datastruct.Set{Data: map[string]struct{}{"first": {}, "second": {}}}},
		{"zset", &datastruct.ZSet{
			Data:   map[string]float64{"high": 10, "low": -3},
			Scores: []datastruct.ZSetMember{{Member: "low", Score: -3}, {Member: "high", Score: 10}},
		}},
		{"stream", stream},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want := &datastruct.DataValue{Value: tc.value, ExpireTime: 123, LastAccessedAt: 456}
			borrowed, err := want.Serialize()
			check(t, err)
			if borrowed[0] != 3 {
				t.Fatalf("encoded version = %d, want 3", borrowed[0])
			}
			got, err := datastruct.DeserializeDataValue(borrowed)
			check(t, err)
			clear(borrowed)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("decoded value changed after the input buffer was overwritten: got %#v, want %#v", got.Value, want.Value)
			}
		})
	}
}

func TestFullRecoveryRetainsKeysAndHistoricalValuesAfterNativeRelease(t *testing.T) {
	requireNativeTools(t)
	primary := t.TempDir()
	writer := openWriter(t, filepath.Join(primary, dbName(0)))
	values := make(map[string]string)
	for i := range 48 {
		key := fmt.Sprintf("a-key-%02d-shared-prefix", i)
		value := fmt.Sprintf("%02d%s", i, strings.Repeat("\x00\xff", 1024))
		entry := &datastruct.DataValue{Value: &datastruct.BytesString{Data: []byte(value)}}
		raw, err := entry.Serialize()
		check(t, err)
		check(t, writer.Put([]byte(key), raw))
		values[key] = value
	}
	for _, prefix := range []byte{0, 1, 2} {
		var raw bytes.Buffer
		if prefix != 0 {
			raw.WriteByte(prefix)
		}
		encoder := gob.NewEncoder(&raw)
		check(t, encoder.Encode(time.Now().Add(time.Hour).UnixMilli()))
		if prefix == 2 {
			check(t, encoder.Encode(int64(1000)))
		}
		key := fmt.Sprintf("z-legacy-%d", prefix)
		var value any = &datastruct.String{Data: strings.Repeat(key, 128)}
		check(t, encoder.Encode(&value))
		check(t, writer.Put([]byte(key), raw.Bytes()))
		values[key] = value.(*datastruct.String).Data
	}
	check(t, writer.Close())
	handler, err := NewSource(primary)
	check(t, err)
	upstream := httptest.NewServer(handler)
	defer upstream.Close()
	root := t.TempDir()
	follower, err := OpenFollower(root, upstream.URL, []int{0})
	check(t, err)
	t.Cleanup(func() { follower.Close() })
	poll(t, follower)
	for key, value := range values {
		expectGet(t, follower, 0, key, http.StatusOK, value)
	}
	check(t, follower.Close())
	upstream.Close()
	reopened, err := OpenFollower(root, upstream.URL, []int{0})
	check(t, err)
	t.Cleanup(func() { reopened.Close() })
	for key, value := range values {
		expectGet(t, reopened, 0, key, http.StatusOK, value)
	}
}
