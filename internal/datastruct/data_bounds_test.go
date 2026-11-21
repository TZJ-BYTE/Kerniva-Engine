package datastruct

import (
	"encoding/binary"
	"testing"
)

func encodedValueForBoundsTest(valueType byte, numbers ...uint32) []byte {
	value := append([]byte{3}, make([]byte, 16)...)
	value = append(value, valueType)
	for _, number := range numbers {
		value = binary.LittleEndian.AppendUint32(value, number)
	}
	return value
}

func TestDeserializeRejectsCountsExceedingRemainingPayload(t *testing.T) {
	const oversized = ^uint32(0)
	tests := []struct {
		name    string
		payload []byte
	}{
		{"list", encodedValueForBoundsTest(3, oversized)},
		{"hash", encodedValueForBoundsTest(4, oversized)},
		{"set", encodedValueForBoundsTest(5, oversized)},
		{"zset", encodedValueForBoundsTest(6, oversized)},
		{"stream entries", encodedValueForBoundsTest(7, 0, 0, 0, 0, oversized)},
		{"stream fields", encodedValueForBoundsTest(7, 0, 0, 0, 0, 1, 0, oversized)},
		{"stream groups", encodedValueForBoundsTest(7, 0, 0, 0, 0, 0, oversized)},
		{"stream pending", encodedValueForBoundsTest(7, 0, 0, 0, 0, 0, 1, 0, 0, oversized, 0)},
		{"stream consumers", encodedValueForBoundsTest(7, 0, 0, 0, 0, 0, 1, 0, 0, 0, oversized)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if value, err := DeserializeDataValue(test.payload); err == nil {
				FreeDataValue(value)
				t.Fatal("accepted a count exceeding the encoded payload")
			}
		})
	}
}

func TestDeserializeStreamRejectsTruncatedGroupsAndAcceptsLegacyTail(t *testing.T) {
	legacy := encodedValueForBoundsTest(7, 0, 0, 0, 0, 0)
	value, err := DeserializeDataValue(legacy)
	if err != nil {
		t.Fatalf("legacy stream without groups: %v", err)
	}
	FreeDataValue(value)
	truncated := append(append([]byte(nil), legacy...), 1, 0)
	if value, err := DeserializeDataValue(truncated); err == nil {
		FreeDataValue(value)
		t.Fatal("accepted a truncated group count")
	}
	stream := &DataValue{Value: &Stream{Groups: []ConsumerGroup{{
		Name: "group", LastDeliveredID: "0-0",
		Pel:       []PendingEntry{{ID: "1-0", ConsumerName: "consumer", DeliveryCount: 1}},
		Consumers: []Consumer{{Name: "consumer", SeenTime: 1}},
	}}}}
	encoded, err := stream.Serialize()
	if err != nil {
		t.Fatal(err)
	}
	if value, err := DeserializeDataValue(encoded[:len(encoded)-1]); err == nil {
		FreeDataValue(value)
		t.Fatal("accepted a truncated consumer record")
	}
	value, err = DeserializeDataValue(encoded)
	if err != nil {
		t.Fatal(err)
	}
	defer FreeDataValue(value)
	groups := value.Value.(*Stream).Groups
	if len(groups) != 1 || len(groups[0].Pel) != 1 || len(groups[0].Consumers) != 1 {
		t.Fatalf("stream group round trip=%#v", groups)
	}
}
