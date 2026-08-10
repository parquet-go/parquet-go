//go:build !purego && goexperiment.simd

package bytealg

import (
	"encoding/binary"
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
	n := 0
	d := data
	if hasAVX512 && len(d) >= 64 {
		v := archsimd.BroadcastUint8x64(value)
		c0, c1, c2, c3 := 0, 0, 0, 0
		// The array pointer conversion carries the only bounds check of the
		// loop body; the constant-bounds subslices below are proven safe at
		// compile time.
		for len(d) >= 256 {
			c := (*[256]uint8)(d)
			c0 += bits.OnesCount64(archsimd.LoadUint8x64Slice(c[0:64]).Equal(v).ToBits())
			c1 += bits.OnesCount64(archsimd.LoadUint8x64Slice(c[64:128]).Equal(v).ToBits())
			c2 += bits.OnesCount64(archsimd.LoadUint8x64Slice(c[128:192]).Equal(v).ToBits())
			c3 += bits.OnesCount64(archsimd.LoadUint8x64Slice(c[192:256]).Equal(v).ToBits())
			d = d[256:]
		}
		for len(d) >= 64 {
			c0 += bits.OnesCount64(archsimd.LoadUint8x64Slice(d).Equal(v).ToBits())
			d = d[64:]
		}
		n = c0 + c1 + c2 + c3
	} else if hasAVX2 && len(d) >= 32 {
		v := archsimd.BroadcastUint8x32(value)
		for len(d) >= 128 {
			c := (*[128]uint8)(d)
			n += bits.OnesCount32(archsimd.LoadUint8x32Slice(c[0:32]).Equal(v).ToBits())
			n += bits.OnesCount32(archsimd.LoadUint8x32Slice(c[32:64]).Equal(v).ToBits())
			n += bits.OnesCount32(archsimd.LoadUint8x32Slice(c[64:96]).Equal(v).ToBits())
			n += bits.OnesCount32(archsimd.LoadUint8x32Slice(c[96:128]).Equal(v).ToBits())
			d = d[128:]
		}
		for len(d) >= 32 {
			n += bits.OnesCount32(archsimd.LoadUint8x32Slice(d).Equal(v).ToBits())
			d = d[32:]
		}
	}
	for i := range d {
		if d[i] == value {
			n++
		}
	}
	return n
}

// Broadcast writes the src value to all bytes of dst.
func Broadcast(dst []byte, src byte) {
	if hasAVX2 && len(dst) >= 32 {
		v := archsimd.BroadcastUint8x32(src)
		d := dst
		for len(d) >= 256 {
			c := (*[256]uint8)(d)
			v.StoreSlice(c[0:32])
			v.StoreSlice(c[32:64])
			v.StoreSlice(c[64:96])
			v.StoreSlice(c[96:128])
			v.StoreSlice(c[128:160])
			v.StoreSlice(c[160:192])
			v.StoreSlice(c[192:224])
			v.StoreSlice(c[224:256])
			d = d[256:]
		}
		for len(d) >= 64 {
			c := (*[64]uint8)(d)
			v.StoreSlice(c[0:32])
			v.StoreSlice(c[32:64])
			d = d[64:]
		}
		if len(d) >= 32 {
			v.StoreSlice(d)
			d = d[32:]
		}
		if len(d) > 0 {
			v.StoreSlice(dst[len(dst)-32:])
		}
		return
	}
	if len(dst) >= 8 {
		// Splat the byte across a word with a single multiply and store 8
		// bytes at a time, with one overlapping store for the tail;
		// PutUint64 compiles to a plain 8-byte store.
		x := 0x0101010101010101 * uint64(src)
		d := dst
		for len(d) >= 8 {
			binary.LittleEndian.PutUint64(d, x)
			d = d[8:]
		}
		if len(d) > 0 {
			binary.LittleEndian.PutUint64(dst[len(dst)-8:], x)
		}
		return
	}
	for i := range dst {
		dst[i] = src
	}
}
