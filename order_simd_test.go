//go:build goexperiment.simd

package parquet

import (
	"math"
	"testing"
)

// The vector order scans cover the array with chunked compares plus one or
// two overlapping windows; this exhaustively injects a violation at every
// position for lengths around the chunking boundaries to verify no pair is
// skipped, and checks NaN handling of the float kernels at every position.
func TestOrderOfVectorCoverage(t *testing.T) {
	lengths := []int{2, 3, 7, 8, 9, 15, 16, 17, 24, 31, 32, 33, 47, 48, 49, 63, 64, 65, 100}

	for _, n := range lengths {
		for pos := 0; pos < n-1; pos++ {
			if n == 2 {
				// A single violated pair is a valid sequence of the
				// opposite order.
				break
			}
			asc := make([]int32, n)
			for i := range asc {
				asc[i] = int32(2 * i)
			}
			asc[pos] = asc[pos+1] + 1 // violate ascending at pair (pos, pos+1)
			if got := orderOfInt32(asc); got != 0 {
				t.Fatalf("int32 len=%d violation at %d: got order %d, want 0", n, pos, got)
			}

			desc := make([]int64, n)
			for i := range desc {
				desc[i] = int64(-2 * i)
			}
			desc[pos] = desc[pos+1] - 1 // violate descending at pair (pos, pos+1)
			if got := orderOfInt64(desc); got != 0 {
				t.Fatalf("int64 len=%d violation at %d: got order %d, want 0", n, pos, got)
			}

			f := make([]float64, n)
			for i := range f {
				f[i] = float64(i)
			}
			f[pos] = math.NaN()
			if got := orderOfFloat64(f); got != 0 {
				t.Fatalf("float64 len=%d NaN at %d: got order %d, want 0", n, pos, got)
			}
		}

		// Clean sequences must report their order.
		asc := make([]uint32, n)
		desc := make([]uint64, n)
		for i := range asc {
			asc[i] = uint32(i)
			desc[i] = uint64(n - i)
		}
		if got := orderOfUint32(asc); got != +1 {
			t.Fatalf("uint32 len=%d ascending: got order %d, want +1", n, got)
		}
		if got := orderOfUint64(desc); got != -1 {
			t.Fatalf("uint64 len=%d descending: got order %d, want -1", n, got)
		}
	}
}
