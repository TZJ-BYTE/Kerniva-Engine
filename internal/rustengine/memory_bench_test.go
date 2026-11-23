package rustengine

import (
	"bytes"
	"encoding/binary"
	"sync/atomic"
	"testing"
)

// BenchmarkMemoryABI exercises the actual Go wrapper for 32-byte values, with
// no TCP, parser or response encoding. Both operations use resident keys.
func BenchmarkMemoryABI(b *testing.B) {
	const keyCount = 100_000
	e, err := OpenMemory(16, nil)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := e.Close(); err != nil {
			b.Fatal(err)
		}
	})
	keys := make([][8]byte, keyCount)
	var value [32]byte
	for i := range value {
		value[i] = byte(i)
	}
	for i := range keys {
		binary.LittleEndian.PutUint64(keys[i][:], uint64(i))
		if err := e.Put(keys[i][:], value[:]); err != nil {
			b.Fatal(err)
		}
	}
	b.Run("GetInto32B", func(b *testing.B) {
		var dst [32]byte
		b.ReportAllocs()
		b.SetBytes(int64(len(value)))
		for i := 0; b.Loop(); i++ {
			if n, found, err := e.GetInto(keys[(i*79)%keyCount][:], dst[:]); err != nil || !found || n != len(value) {
				b.Fatalf("read returned size %d, found %v, error %v", n, found, err)
			}
		}
		if !bytes.Equal(dst[:], value[:]) {
			b.Fatalf("read returned %x, wanted %x", dst, value)
		}
	})
	b.Run("Replace32B", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(value)))
		for i := 0; b.Loop(); i++ {
			if err := e.Put(keys[(i*79)%keyCount][:], value[:]); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("ParallelGetInto32B", func(b *testing.B) {
		var worker atomic.Int64
		b.ReportAllocs()
		b.SetBytes(int64(len(value)))
		b.RunParallel(func(pb *testing.PB) {
			var dst [32]byte
			index := int(worker.Add(1)) % keyCount
			for pb.Next() {
				if n, found, err := e.GetInto(keys[index][:], dst[:]); err != nil || !found || n != len(value) {
					b.Errorf("read returned size %d, found %v, error %v", n, found, err)
					return
				}
				if !bytes.Equal(dst[:], value[:]) {
					b.Errorf("read returned %x, wanted %x", dst, value)
					return
				}
				index = (index + 79) % keyCount
			}
		})
	})
}
