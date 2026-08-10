//go:build !purego && goexperiment.simd

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

func TestMemsetValuesSimd(t *testing.T) {
	model := makeValueBytes(ByteArray, []byte("0123456789"))
	model.columnIndex = ^uint16(7)
	model.definitionLevel = 3
	model.repetitionLevel = 1

	for _, n := range []int{0, 1, 3, 4, 5, 63, 64, 65} {
		values := make([]Value, n)
		memsetValues(values, model)
		for i := range values {
			if values[i] != model {
				t.Fatalf("len=%d: values[%d] = %+v, want %+v", n, i, values[i], model)
			}
		}
	}
}

func BenchmarkMemsetValues(b *testing.B) {
	model := makeValueBytes(ByteArray, []byte("0123456789"))
	values := make([]Value, 1024)
	b.SetBytes(int64(len(values) * 24))
	for i := 0; i < b.N; i++ {
		memsetValues(values, model)
	}
}
