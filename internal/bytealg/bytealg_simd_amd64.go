//go:build !purego && goexperiment.simd

package bytealg

import (
	"math/bits"
	"simd/archsimd"
)

// This file provides implementations of the bytealg functions based on the
// simd/archsimd package, replacing the hand-written assembly of
// count_amd64.s and broadcast_amd64.s when GOEXPERIMENT=simd is set.

var (
	hasAVX2   = archsimd.X86.AVX2()
	hasAVX512 = archsimd.X86.AVX512()
)

// Count returns the number of occurrences of value in data.
//
// The AVX-512 path compares 256 bytes per iteration into mask registers and
// accumulates the population counts in independent counters to break the
// dependency chain, mirroring the structure of the retired assembly version.
func Count(data []byte, value byte) int {
	i, n := 0, 0
	if hasAVX512 && len(data) >= 256 {
		v := archsimd.BroadcastUint8x64(value)
		c0, c1, c2, c3 := 0, 0, 0, 0
		for ; i+256 <= len(data); i += 256 {
			c0 += bits.OnesCount64(archsimd.LoadUint8x64Slice(data[i:]).Equal(v).ToBits())
			c1 += bits.OnesCount64(archsimd.LoadUint8x64Slice(data[i+64:]).Equal(v).ToBits())
			c2 += bits.OnesCount64(archsimd.LoadUint8x64Slice(data[i+128:]).Equal(v).ToBits())
			c3 += bits.OnesCount64(archsimd.LoadUint8x64Slice(data[i+192:]).Equal(v).ToBits())
		}
		n = c0 + c1 + c2 + c3
		for ; i+64 <= len(data); i += 64 {
			n += bits.OnesCount64(archsimd.LoadUint8x64Slice(data[i:]).Equal(v).ToBits())
		}
	} else if hasAVX2 && len(data) >= 32 {
		v := archsimd.BroadcastUint8x32(value)
		for ; i+64 <= len(data); i += 64 {
			n += bits.OnesCount32(archsimd.LoadUint8x32Slice(data[i:]).Equal(v).ToBits())
			n += bits.OnesCount32(archsimd.LoadUint8x32Slice(data[i+32:]).Equal(v).ToBits())
		}
		for ; i+32 <= len(data); i += 32 {
			n += bits.OnesCount32(archsimd.LoadUint8x32Slice(data[i:]).Equal(v).ToBits())
		}
	}
	for ; i < len(data); i++ {
		if data[i] == value {
			n++
		}
	}
	return n
}

// Broadcast writes the src value to all bytes of dst.
func Broadcast(dst []byte, src byte) {
	if hasAVX2 && len(dst) >= 32 {
		v := archsimd.BroadcastUint8x32(src)
		i := 0
		for ; i+32 <= len(dst); i += 32 {
			v.StoreSlice(dst[i:])
		}
		if i < len(dst) {
			v.StoreSlice(dst[len(dst)-32:])
		}
		return
	}
	for i := range dst {
		dst[i] = src
	}
}
