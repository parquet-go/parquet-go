//go:build !purego && amd64

package parquet

import (
	"math"
	"testing"

	"github.com/parquet-go/parquet-go/encoding"
)

func TestInt64PageBoundsAVX512VectorSlots(t *testing.T) {
	if !hasAVX512VL {
		t.Skipf("requires AVX-512VL (hasAVX512VL=%t)", hasAVX512VL)
	}

	const (
		vectorWidth = 32
		vectorStart = 32 // Avoid the first iteration's broadcast seed.
	)
	// Round up past the dispatch start to leave a one-value scalar tail after
	// the 32-wide vector prefix, regardless of the configured page threshold.
	count := combinedBoundsInt64Threshold + (vectorWidth - combinedBoundsInt64Threshold%vectorWidth) + 1

	t.Run("all-lanes", func(t *testing.T) {
		for slot := 0; slot < vectorWidth; slot++ {
			values := make([]int64, count)
			state := uint64(count) + 0x9E3779B97F4A7C15
			for i := range values {
				state = state*6364136223846793005 + 1442695040888963407
				values[i] = int64(state >> 1)
			}

			// Rotate both extrema through a non-initial vector iteration. This
			// covers every lane of all four ZMM load/accumulator streams.
			minIndex := vectorStart + slot
			maxIndex := vectorStart + (slot+1)%vectorWidth
			values[minIndex] = math.MinInt64
			values[maxIndex] = math.MaxInt64

			wantMin, wantMax := values[0], values[0]
			for _, value := range values[1:] {
				if value < wantMin {
					wantMin = value
				}
				if value > wantMax {
					wantMax = value
				}
			}

			page := newInt64Page(Int64Type, 0, int32(len(values)), encoding.Int64Values(values))
			min, max, ok := page.Bounds()
			if !ok {
				t.Errorf("slot %d: Bounds() returned ok=false", slot)
				continue
			}
			if got := min.Int64(); got != wantMin {
				t.Errorf("slot %d: min = %d, want %d", slot, got, wantMin)
			}
			if got := max.Int64(); got != wantMax {
				t.Errorf("slot %d: max = %d, want %d", slot, got, wantMax)
			}
		}
	})
}
