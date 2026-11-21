package datastruct

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

func TestZSetIncrementalScoreIndex(t *testing.T) {
	set := &ZSet{Data: map[string]float64{"legacy": 1}}
	set.Add("legacy", 1)
	random := rand.New(rand.NewSource(2026))
	for operation := 0; operation < 2000; operation++ {
		member := fmt.Sprintf("m%03d", random.Intn(100))
		if random.Intn(4) == 0 {
			set.Remove(member)
		} else {
			set.Add(member, float64(random.Intn(12)-6))
		}
		expected := make([]ZSetMember, 0, len(set.Data))
		for member, score := range set.Data {
			expected = append(expected, ZSetMember{Member: member, Score: score})
		}
		sort.Slice(expected, func(i, j int) bool {
			if expected[i].Score == expected[j].Score {
				return expected[i].Member < expected[j].Member
			}
			return expected[i].Score < expected[j].Score
		})
		if len(expected) != len(set.Scores) {
			t.Fatalf("index size at operation %d", operation)
		}
		for i := range expected {
			if expected[i] != set.Scores[i] {
				t.Fatalf("operation %d index %d got %v want %v", operation, i, set.Scores[i], expected[i])
			}
		}
	}
	if got := set.AddMany([]ZSetMember{{Member: "new", Score: 2}, {Member: "new", Score: 3}}); got != 1 {
		t.Fatalf("duplicate batch count=%d", got)
	}
}

func BenchmarkZSetScoreUpdate(b *testing.B) {
	for _, size := range []int{100, 10000} {
		for _, rebuild := range []bool{false, true} {
			mode := "incremental"
			if rebuild {
				mode = "rebuild"
			}
			b.Run(fmt.Sprintf("%s/%d", mode, size), func(b *testing.B) {
				entries := make([]ZSetMember, size)
				for i := range entries {
					entries[i] = ZSetMember{Member: fmt.Sprintf("m%08d", i), Score: float64(i)}
				}
				set := &ZSet{}
				set.AddMany(entries)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if rebuild {
						set.Data[entries[size/2].Member] = float64(i % size)
						set.updateScores()
					} else {
						set.Add(entries[size/2].Member, float64(i%size))
					}
				}
			})
		}
	}
}
