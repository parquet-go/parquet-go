//go:build !purego && goexperiment.simd

package parquet

import (
	"unsafe"

	"simd/archsimd"

	"github.com/parquet-go/parquet-go/sparse"
)

// The assembly versions of nullIndex8/128 are scalar loops (nullIndex128
// also used ungated SSE4.1 PCMPEQQ); the generic Go implementation compiles
// to comparable code. nullIndex32/64 used VPGATHER in the assembly and are
// tiered instead: densely packed values take a vector compare + ToBits path
// building whole 64 bit words, and other strides walk the pointer
// additively, assembling each word in a register before a single store.

func nullIndex8(bits *uint64, rows sparse.Array) {
	nullIndex[uint8](unsafe.Slice(bits, (rows.Len()+63)/64), rows)
}

func nullIndex128(bits *uint64, rows sparse.Array) {
	nullIndex[[16]byte](unsafe.Slice(bits, (rows.Len()+63)/64), rows)
}

func nullIndex32(bits *uint64, rows sparse.Array) {
	n := rows.Len()
	if n == 0 {
		return
	}
	words := unsafe.Slice(bits, (n+63)/64)
	p := rows.Index(0)
	off := uintptr(4)
	if n > 1 {
		off = uintptr(rows.Index(1)) - uintptr(rows.Index(0))
	}
	i := 0
	if off == 4 && archsimd.X86.AVX512() {
		v := unsafe.Slice((*uint32)(p), n)
		zero := archsimd.BroadcastUint32x16(0)
		for ; i+64 <= n; i += 64 {
			m0 := archsimd.LoadUint32x16Slice(v[i : i+16]).NotEqual(zero).ToBits()
			m1 := archsimd.LoadUint32x16Slice(v[i+16 : i+32]).NotEqual(zero).ToBits()
			m2 := archsimd.LoadUint32x16Slice(v[i+32 : i+48]).NotEqual(zero).ToBits()
			m3 := archsimd.LoadUint32x16Slice(v[i+48 : i+64]).NotEqual(zero).ToBits()
			words[i/64] = uint64(m0) | uint64(m1)<<16 | uint64(m2)<<32 | uint64(m3)<<48
		}
		archsimd.ClearAVXUpperBits()
		p = unsafe.Add(p, uintptr(i)*off)
	}
	for i < n {
		var w uint64
		k := min(n-i, 64)
		for j := range k {
			var b uint64
			if *(*uint32)(p) != 0 {
				b = 1
			}
			w |= b << j
			p = unsafe.Add(p, off)
		}
		words[i/64] = w
		i += k
	}
}

func nullIndex64(bits *uint64, rows sparse.Array) {
	n := rows.Len()
	if n == 0 {
		return
	}
	words := unsafe.Slice(bits, (n+63)/64)
	p := rows.Index(0)
	off := uintptr(8)
	if n > 1 {
		off = uintptr(rows.Index(1)) - uintptr(rows.Index(0))
	}
	i := 0
	if off == 8 && archsimd.X86.AVX512() {
		v := unsafe.Slice((*uint64)(p), n)
		zero := archsimd.BroadcastUint64x8(0)
		for ; i+64 <= n; i += 64 {
			var w uint64
			for g := 0; g < 64; g += 8 {
				m := archsimd.LoadUint64x8Slice(v[i+g : i+g+8]).NotEqual(zero).ToBits()
				w |= uint64(m) << g
			}
			words[i/64] = w
		}
		archsimd.ClearAVXUpperBits()
		p = unsafe.Add(p, uintptr(i)*off)
	}
	for i < n {
		var w uint64
		k := min(n-i, 64)
		for j := range k {
			var b uint64
			if *(*uint64)(p) != 0 {
				b = 1
			}
			w |= b << j
			p = unsafe.Add(p, off)
		}
		words[i/64] = w
		i += k
	}
}
