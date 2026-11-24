//go:build goexperiment.simd && go1.27

// Go 1.27 simd/archsimd API. The Go 1.26 kernels stay in the sibling file gated !go1.27.

package parquet

import (
	"simd/archsimd"

	"github.com/parquet-go/bitpack/unsafecast"
	"github.com/parquet-go/parquet-go/internal/bytealg"
	"github.com/parquet-go/parquet-go/sparse"
)

// column_buffer_amd64.s stays out of the Go 1.27 simd build because its
// broadcastRangeInt32 tail computes base*(i+1). The vector loop here is
// slower than that assembly; the scalar tail is the reason it is kept.

func broadcastValueInt32(dst []int32, src int8) {
	bytealg.Broadcast(unsafecast.Slice[byte](dst), byte(src))
}

var rangeIota8 = [8]int32{0, 1, 2, 3, 4, 5, 6, 7}

func broadcastRangeInt32(dst []int32, base int32) {
	i := 0
	if archsimd.X86.AVX2() && len(dst) >= 8 {
		v := archsimd.BroadcastInt32x8(base).Add(archsimd.LoadInt32x8(rangeIota8[:]))
		step := archsimd.BroadcastInt32x8(8)
		for ; i+8 <= len(dst); i += 8 {
			v.Store(dst[i:])
			v = v.Add(step)
		}
		archsimd.ClearAVXUpperBits()
	}
	for ; i < len(dst); i++ {
		dst[i] = base + int32(i)
	}
}

func writePointersBE128(values [][16]byte, rows sparse.Array) {
	for i := range values {
		p := *(**[16]byte)(rows.Index(i))

		if p != nil {
			values[i] = *p
		} else {
			values[i] = [16]byte{}
		}
	}
}
