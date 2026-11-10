//go:build integration

package examples_test

import "testing"

// Fixed 0.1ms bins up to 60 seconds. The last bin counts overflow.
// Percentiles return the upper bin edge; overflow percentiles are lower bounds.
type loadHistogram struct {
	bins            []uint64
	count, overflow uint64
}

func newLoadHistogram() *loadHistogram { return &loadHistogram{bins: make([]uint64, 600001)} }
func (h *loadHistogram) add(ms float64) {
	i := int(ms * 10)
	if i < 0 {
		i = 0
	}
	if i >= len(h.bins)-1 {
		i = len(h.bins) - 1
		h.overflow++
	}
	h.bins[i]++
	h.count++
}
func (h *loadHistogram) percentile(q float64) float64 {
	if h.count == 0 {
		return 0
	}
	target := uint64(float64(h.count-1)*q) + 1
	var seen uint64
	for i, n := range h.bins {
		seen += n
		if seen >= target {
			if i == len(h.bins)-1 {
				return 60000
			}
			return float64(i+1) / 10
		}
	}
	return 60000
}
func TestLoadHistogramBounds(t *testing.T) {
	h := newLoadHistogram()
	for _, ms := range []float64{0, 1.25, 2.34, 60001} {
		h.add(ms)
	}
	if h.count != 4 || h.overflow != 1 || h.percentile(.5) != 1.3 || h.percentile(1) != 60000 {
		t.Fatal("incorrect histogram")
	}
	for i := 0; i < 100000; i++ {
		h.add(1)
	}
	if len(h.bins) != 600001 {
		t.Fatal("unbounded storage")
	}
}
