//go:build !purego && goexperiment.simd

package sparse

import (
	"unsafe"

	"github.com/parquet-go/bitpack/unsafecast"
)

// The assembly versions of these kernels are scalar loops; the Go
// implementations below compile to comparable code, so the GOEXPERIMENT=simd
// build uses them instead of the assembly. The AVX2 gather kernels keep
// their assembly (no gather in archsimd).

func gatherBitsDefault(dst []byte, src Uint8Array) {
	n := src.Len() / 8
	for j := range n {
		i := j * 8
		dst[j] = (src.Index(i+0) & 1) |
			((src.Index(i+1) & 1) << 1) |
			((src.Index(i+2) & 1) << 2) |
			((src.Index(i+3) & 1) << 3) |
			((src.Index(i+4) & 1) << 4) |
			((src.Index(i+5) & 1) << 5) |
			((src.Index(i+6) & 1) << 6) |
			((src.Index(i+7) & 1) << 7)
	}
}

func gather128(dst [][16]byte, src Uint128Array) int {
	n := min(len(dst), src.Len())
	if n == 0 {
		return 0
	}
	p := src.index(0)
	off := src.off
	if off == 16 {
		// Dense values: the gather is a contiguous copy, and memmove beats
		// an explicit load/store loop at every size.
		copy(unsafecast.Slice[byte](dst[:n]), unsafe.Slice((*byte)(p), 16*n))
		return n
	}
	c := unsafecast.Slice[[4][16]byte](dst[:n])
	for j := range c {
		d := &c[j]
		d[0] = *(*[16]byte)(p)
		d[1] = *(*[16]byte)(unsafe.Add(p, off))
		d[2] = *(*[16]byte)(unsafe.Add(p, 2*off))
		d[3] = *(*[16]byte)(unsafe.Add(p, 3*off))
		p = unsafe.Add(p, 4*off)
	}
	for i := len(c) * 4; i < n; i++ {
		dst[i] = *(*[16]byte)(p)
		p = unsafe.Add(p, off)
	}
	return n
}
