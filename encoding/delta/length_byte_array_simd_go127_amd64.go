//go:build goexperiment.simd && go1.27

// Go 1.27 simd/archsimd API. The Go 1.26 kernels stay in the sibling file gated !go1.27.
package delta

import (
	"simd/archsimd"

	"github.com/parquet-go/bitpack/unsafecast"
)

// encodeByteArrayLengths is the archsimd kernel. decodeByteArrayLengths stays
// on the SSE2 assembly in decode_lengths_amd64.s: the archsimd prefix sum was
// slower than that assembly.

func encodeByteArrayLengths(lengths []int32, offsets []uint32) {
	i := 0
	switch {
	case archsimd.X86.AVX512() && len(lengths) >= 16 && len(offsets) > len(lengths):
		n := len(lengths) / 16
		ca := unsafecast.Slice[[16]uint32](offsets)[:n]
		cb := unsafecast.Slice[[16]uint32](offsets[1:])[:n]
		cl := unsafecast.Slice[[16]int32](lengths)[:n]
		for j := range cl {
			a := archsimd.LoadUint32x16(ca[j][:])
			b := archsimd.LoadUint32x16(cb[j][:])
			b.Sub(a).AsInt32x16().Store(cl[j][:])
		}
		i = n * 16
		archsimd.ClearAVXUpperBits()
	case archsimd.X86.AVX2() && len(lengths) >= 8 && len(offsets) > len(lengths):
		// The two loads are offset by one element, which defeats a single
		// chunk view; two chunk views (one over offsets, one over
		// offsets[1:]) turn both into constant-length loads that need no
		// bounds checks or per-iteration slice headers.
		n := len(lengths) / 8
		ca := unsafecast.Slice[[8]uint32](offsets)[:n]
		cb := unsafecast.Slice[[8]uint32](offsets[1:])[:n]
		cl := unsafecast.Slice[[8]int32](lengths)[:n]
		for j := range cl {
			a := archsimd.LoadUint32x8(ca[j][:])
			b := archsimd.LoadUint32x8(cb[j][:])
			b.Sub(a).AsInt32x8().Store(cl[j][:])
		}
		i = len(cl) * 8
		archsimd.ClearAVXUpperBits()
	}
	for ; i < len(lengths); i++ {
		lengths[i] = int32(offsets[i+1] - offsets[i])
	}
}

// decodeByteArrayLengths stays on the SSE2 assembly in decode_lengths_amd64.s.
// The archsimd prefix sum was slower. purego has its own body.
