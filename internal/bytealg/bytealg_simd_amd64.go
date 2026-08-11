//go:build !purego && goexperiment.simd

package bytealg

import (
	"encoding/binary"
	"math/bits"
	"simd/archsimd"

	"github.com/parquet-go/bitpack/unsafecast"
)

// This file provides implementations of the bytealg functions based on the
// simd/archsimd package, replacing the hand-written assembly of
// count_amd64.s and broadcast_amd64.s when GOEXPERIMENT=simd is set.

// Count returns the number of occurrences of value in data.
//
// The counts accumulate in vector registers: each compare adds 1 to a lane
// of a byte accumulator (via a masked add), and the byte accumulators are
// flushed into 64-bit lane totals with SumAbsDiff (VPSADBW against zero)
// before they can overflow, at most every 255 rounds. The add uses the
// Add+Merge form so it lowers to a single merge-masked VPADDB on AVX-512;
// the Masked (zeroing) form lowers to a masked broadcast, which competes
// with the compares for the shuffle port and halves the loop throughput. Unlike the retired
// assembly version, which moved every compare result to a general purpose
// register and popcounted it, the inner loop performs no scalar work at all.
func Count(data []byte, value byte) int {
	n := 0
	d := data
	if archsimd.X86.AVX512() && len(d) >= 64 {
		v := archsimd.BroadcastUint8x64(value)
		ones := archsimd.BroadcastUint8x64(1)
		zero := archsimd.BroadcastUint8x64(0)
		var totals [8]uint64
		total := archsimd.LoadUint64x8Slice(totals[:])
		chunks := unsafecast.Slice[[256]uint8](d)
		for i := 0; i < len(chunks); {
			m := min(i+255, len(chunks))
			a0, a1, a2, a3 := zero, zero, zero, zero
			for ; i < m; i++ {
				c := &chunks[i]
				a0 = a0.Add(ones).Merge(a0, archsimd.LoadUint8x64Slice(c[0:64]).Equal(v))
				a1 = a1.Add(ones).Merge(a1, archsimd.LoadUint8x64Slice(c[64:128]).Equal(v))
				a2 = a2.Add(ones).Merge(a2, archsimd.LoadUint8x64Slice(c[128:192]).Equal(v))
				a3 = a3.Add(ones).Merge(a3, archsimd.LoadUint8x64Slice(c[192:256]).Equal(v))
			}
			total = total.Add(a0.SumAbsDiff(zero).AsUint64x8()).
				Add(a1.SumAbsDiff(zero).AsUint64x8()).
				Add(a2.SumAbsDiff(zero).AsUint64x8()).
				Add(a3.SumAbsDiff(zero).AsUint64x8())
		}
		total.StoreSlice(totals[:])
		for _, t := range totals {
			n += int(t)
		}
		d = d[len(chunks)*256:]
		for len(d) >= 64 {
			n += bits.OnesCount64(archsimd.LoadUint8x64Slice(d).Equal(v).ToBits())
			d = d[64:]
		}
	} else if archsimd.X86.AVX2() && len(d) >= 32 {
		v := archsimd.BroadcastUint8x32(value)
		ones := archsimd.BroadcastUint8x32(1)
		zero := archsimd.BroadcastUint8x32(0)
		var totals [4]uint64
		total := archsimd.LoadUint64x4Slice(totals[:])
		chunks := unsafecast.Slice[[128]uint8](d)
		for i := 0; i < len(chunks); {
			m := min(i+255, len(chunks))
			a0, a1, a2, a3 := zero, zero, zero, zero
			for ; i < m; i++ {
				c := &chunks[i]
				a0 = a0.Add(ones).Merge(a0, archsimd.LoadUint8x32Slice(c[0:32]).Equal(v))
				a1 = a1.Add(ones).Merge(a1, archsimd.LoadUint8x32Slice(c[32:64]).Equal(v))
				a2 = a2.Add(ones).Merge(a2, archsimd.LoadUint8x32Slice(c[64:96]).Equal(v))
				a3 = a3.Add(ones).Merge(a3, archsimd.LoadUint8x32Slice(c[96:128]).Equal(v))
			}
			total = total.Add(a0.SumAbsDiff(zero).AsUint64x4()).
				Add(a1.SumAbsDiff(zero).AsUint64x4()).
				Add(a2.SumAbsDiff(zero).AsUint64x4()).
				Add(a3.SumAbsDiff(zero).AsUint64x4())
		}
		total.StoreSlice(totals[:])
		for _, t := range totals {
			n += int(t)
		}
		d = d[len(chunks)*128:]
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
	if archsimd.X86.AVX2() && len(dst) >= 32 {
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
