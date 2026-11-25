//go:build !purego && goexperiment.simd && go1.27

// Go 1.27 simd/archsimd API. The Go 1.26 kernels stay in the sibling file gated !go1.27.
package sparse

import (
	"encoding/binary"
	"unsafe"

	"simd/archsimd"

	"github.com/parquet-go/bitpack/unsafecast"

	"golang.org/x/sys/cpu"
)

// Dense gatherBits is a vector compare. Strided gather32 and gatherBits use
// the same VPGATHER dispatch as the assembly build. gather64 stays a scalar
// walk, which still beats VPGATHERQQ. Dense gather64 and gather128 are a
// contiguous copy.

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
		q := unsafe.Add(p, uintptr(4*j)*off)
		d := &c[j]
		d[0] = *(*[16]byte)(q)
		d[1] = *(*[16]byte)(unsafe.Add(q, off))
		d[2] = *(*[16]byte)(unsafe.Add(q, 2*off))
		d[3] = *(*[16]byte)(unsafe.Add(q, 3*off))
	}
	for i := len(c) * 4; i < n; i++ {
		dst[i] = *(*[16]byte)(unsafe.Add(p, uintptr(i)*off))
	}
	return n
}

func gatherBits(dst []byte, src Uint8Array) int {
	n := min(len(dst)*8, src.Len())
	i := 0
	if k := (n / 64) * 64; k > 0 && src.off == 1 && archsimd.X86.AVX512() {
		b := unsafe.Slice((*byte)(src.index(0)), k)
		one := archsimd.BroadcastUint8x64(1)
		for ; i+64 <= k; i += 64 {
			m := archsimd.LoadUint8x64(b[i : i+64]).And(one).Equal(one).ToBits()
			binary.LittleEndian.PutUint64(dst[i/8:], m)
		}
		archsimd.ClearAVXUpperBits()
	}
	// Strided input uses the same VPGATHER dispatch as the assembly build.
	// VPGATHERDD needs 4 bytes of padding at each index.
	if i == 0 && n >= 8 {
		i = (n / 8) * 8
		if src.off >= 4 && cpu.X86.HasAVX2 {
			gatherBitsAVX2(dst, src.Slice(0, i))
		} else {
			gatherBitsDefault(dst, src.Slice(0, i))
		}
	}
	for i < n {
		x := i / 8
		y := i % 8
		b := src.Index(i)
		dst[x] = ((b & 1) << y) | (dst[x] & ^(1 << y))
		i++
	}
	return n
}

func gather32(dst []uint32, src Uint32Array) int {
	n := min(len(dst), src.Len())
	i := 0
	if n >= 16 && cpu.X86.HasAVX2 {
		i = (n / 8) * 8
		gather32AVX2(dst[:i:i], src)
	}
	for i < n {
		dst[i] = src.Index(i)
		i++
	}
	return n
}

func gather64(dst []uint64, src Uint64Array) int {
	n := min(len(dst), src.Len())
	if n == 0 {
		return 0
	}
	p := src.index(0)
	off := src.off
	if off == 8 {
		copy(dst[:n], unsafe.Slice((*uint64)(p), n))
		return n
	}
	c := unsafecast.Slice[[4]uint64](dst[:n])
	for j := range c {
		q := unsafe.Add(p, uintptr(4*j)*off)
		d := &c[j]
		d[0] = *(*uint64)(q)
		d[1] = *(*uint64)(unsafe.Add(q, off))
		d[2] = *(*uint64)(unsafe.Add(q, 2*off))
		d[3] = *(*uint64)(unsafe.Add(q, 3*off))
	}
	for i := len(c) * 4; i < n; i++ {
		dst[i] = *(*uint64)(unsafe.Add(p, uintptr(i)*off))
	}
	return n
}
