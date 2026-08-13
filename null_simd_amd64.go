//go:build !purego && goexperiment.simd

package parquet

import (
	"unsafe"

	"simd/archsimd"

	"github.com/parquet-go/parquet-go/sparse"
)

// The null index kernels are tiered: densely packed values take a vector
// compare + ToBits path building whole 64 bit words; strided rows use the
// VPGATHER assembly for the 32/64 bit kernels (hardware gathers beat plain
// loads on cache resident data) and the generic nullIndex[T] loop for the
// 8/128 bit ones, whose assembly was scalar anyway.

func nullIndex8(bits *uint64, rows sparse.Array) {
	n := rows.Len()
	if n == 0 {
		return
	}
	if n > 1 && archsimd.X86.AVX512() {
		if off := uintptr(rows.Index(1)) - uintptr(rows.Index(0)); off == 1 {
			words := unsafe.Slice(bits, (n+63)/64)
			v := unsafe.Slice((*uint8)(rows.Index(0)), n)
			zero := archsimd.BroadcastUint8x64(0)
			i := 0
			for ; i+64 <= n; i += 64 {
				words[i/64] = archsimd.LoadUint8x64Slice(v[i : i+64]).NotEqual(zero).ToBits()
			}
			archsimd.ClearAVXUpperBits()
			if i < n {
				var w uint64
				for j, b := range v[i:] {
					if b != 0 {
						w |= 1 << j
					}
				}
				words[i/64] = w
			}
			return
		}
	}
	nullIndex[uint8](unsafe.Slice(bits, (n+63)/64), rows)
}

func nullIndex128(bits *uint64, rows sparse.Array) {
	n := rows.Len()
	if n == 0 {
		return
	}
	if n > 1 && archsimd.X86.AVX512() {
		if off := uintptr(rows.Index(1)) - uintptr(rows.Index(0)); off == 16 {
			words := unsafe.Slice(bits, (n+63)/64)
			v := unsafe.Slice((*uint64)(rows.Index(0)), 2*n)
			zero := archsimd.BroadcastUint64x8(0)
			i := 0
			for ; i+64 <= n; i += 64 {
				var w uint64
				for g := 0; g < 64; g += 4 {
					m := uint64(archsimd.LoadUint64x8Slice(v[2*(i+g) : 2*(i+g)+8]).NotEqual(zero).ToBits())
					r := m | m>>1
					nib := (r & 1) | ((r >> 1) & 2) | ((r >> 2) & 4) | ((r >> 3) & 8)
					w |= nib << g
				}
				words[i/64] = w
			}
			archsimd.ClearAVXUpperBits()
			if i < n {
				var w uint64
				for j := i; j < n; j++ {
					if v[2*j]|v[2*j+1] != 0 {
						w |= 1 << (j - i)
					}
				}
				words[i/64] = w
			}
			return
		}
	}
	nullIndex[[16]byte](unsafe.Slice(bits, (n+63)/64), rows)
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
	if off != 4 || !archsimd.X86.AVX512() {
		nullIndexGather32(bits, rows)
		return
	}
	i := 0
	{
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
	}
	for i < n {
		var w uint64
		k := min(n-i, 64)
		for j := range k {
			var b uint64
			if *(*uint32)(unsafe.Add(p, uintptr(i+j)*off)) != 0 {
				b = 1
			}
			w |= b << j
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
	if off != 8 || !archsimd.X86.AVX512() {
		nullIndexGather64(bits, rows)
		return
	}
	i := 0
	{
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
	}
	for i < n {
		var w uint64
		k := min(n-i, 64)
		for j := range k {
			var b uint64
			if *(*uint64)(unsafe.Add(p, uintptr(i+j)*off)) != 0 {
				b = 1
			}
			w |= b << j
		}
		words[i/64] = w
		i += k
	}
}
