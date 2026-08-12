//go:build goexperiment.simd

package parquet

import "testing"

// The assembly implementation of broadcastRangeInt32 had a bug in its scalar
// tail (it computed base*(i+1) instead of base+i), masked by the main test
// only using base == 1. This test pins the correct behavior of the archsimd
// implementation for other bases and lengths that exercise the tail.
func TestBroadcastRangeInt32Simd(t *testing.T) {
	for _, base := range []int32{-3, 0, 1, 42} {
		for _, n := range []int{0, 1, 7, 8, 9, 100, 1023} {
			dst := make([]int32, n)
			broadcastRangeInt32(dst, base)
			for i, v := range dst {
				if v := int32(v); v != base+int32(i) {
					t.Fatalf("base=%d len=%d: dst[%d] = %d, want %d", base, n, i, v, base+int32(i))
				}
			}
		}
	}
}
