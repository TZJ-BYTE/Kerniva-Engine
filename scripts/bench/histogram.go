package main

import (
	"math"
	"math/bits"
	"time"
)

// Bins are exact below 512ns. Above that each bin spans at most 1/256
// of its lower bound. Percentiles return the bin's inclusive upper bound.
const histogramBins = 512 + 55*256

type latencyHistogram struct {
	counts [histogramBins]uint64
	count  uint64
	sum    float64
	max    int64
}

type latencySummary struct {
	Count  uint64  `json:"count"`
	MeanUs float64 `json:"mean_us"`
	P50Us  float64 `json:"p50_us"`
	P95Us  float64 `json:"p95_us"`
	P99Us  float64 `json:"p99_us"`
	P999Us float64 `json:"p99_9_us"`
	MaxUs  float64 `json:"max_us"`
}

type histogramBin struct {
	Index uint32 `json:"index"`
	Count uint64 `json:"count"`
}

type histogramSnapshot struct {
	Bins  []histogramBin `json:"bins"`
	Count uint64         `json:"count"`
	SumNs float64        `json:"sum_ns"`
	MaxNs int64          `json:"max_ns"`
}

func histogramIndex(ns int64) int {
	if ns < 0 {
		return 0
	}
	if ns < 512 {
		return int(ns)
	}
	shift := bits.Len64(uint64(ns)) - 9
	return 512 + (shift-1)*256 + int(uint64(ns)>>shift) - 256
}

func histogramUpper(index int) int64 {
	if index < 512 {
		return int64(index)
	}
	shift := uint((index-512)/256 + 1)
	mantissa := uint64((index-512)%256 + 256)
	upper := ((mantissa + 1) << shift) - 1
	if upper > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(upper)
}

func (h *latencyHistogram) record(duration time.Duration) {
	ns := max(int64(duration), 0)
	h.counts[histogramIndex(ns)]++
	h.count++
	h.sum += float64(ns)
	h.max = max(h.max, ns)
}

func (h *latencyHistogram) snapshot() histogramSnapshot {
	snapshot := histogramSnapshot{Count: h.count, SumNs: h.sum, MaxNs: h.max}
	for index, count := range h.counts {
		if count != 0 {
			snapshot.Bins = append(snapshot.Bins, histogramBin{Index: uint32(index), Count: count})
		}
	}
	return snapshot
}

func (h *latencyHistogram) merge(snapshot histogramSnapshot) {
	for _, bin := range snapshot.Bins {
		h.counts[bin.Index] += bin.Count
	}
	h.count += snapshot.Count
	h.sum += snapshot.SumNs
	h.max = max(h.max, snapshot.MaxNs)
}

func (h *latencyHistogram) percentile(fraction float64) float64 {
	if h.count == 0 {
		return 0
	}
	rank := uint64(math.Ceil(float64(h.count) * fraction))
	var seen uint64
	for index, count := range h.counts {
		seen += count
		if seen >= rank {
			return float64(min(histogramUpper(index), h.max)) / float64(time.Microsecond)
		}
	}
	return 0
}

func (h *latencyHistogram) summary() latencySummary {
	if h.count == 0 {
		return latencySummary{}
	}
	return latencySummary{
		Count: h.count, MeanUs: h.sum / float64(h.count) / float64(time.Microsecond),
		P50Us: h.percentile(.50), P95Us: h.percentile(.95), P99Us: h.percentile(.99), P999Us: h.percentile(.999),
		MaxUs: float64(h.max) / float64(time.Microsecond),
	}
}
