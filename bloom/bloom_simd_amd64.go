//go:build !purego && goexperiment.simd

package bloom

import (
	"simd/archsimd"

	"github.com/parquet-go/bitpack/unsafecast"
)

// This file provides implementations of the block and filter operations based
// on the simd/archsimd package, replacing the hand-written assembly of
// block_amd64.s and filter_amd64.s when GOEXPERIMENT=simd is set.

var hasAVX2 = archsimd.X86.AVX2()

var blockSalt = [8]uint32{salt0, salt1, salt2, salt3, salt4, salt5, salt6, salt7}

// blockMask computes the 8 bit masks of the parquet split-block bloom filter:
// mask[i] = 1 << ((x * salt[i]) >> 27).
func blockMask(x uint32) archsimd.Uint32x8 {
	salt := archsimd.LoadUint32x8Slice(blockSalt[:])
	return archsimd.BroadcastUint32x8(1).
		ShiftLeft(archsimd.BroadcastUint32x8(x).Mul(salt).ShiftAllRight(27))
}

func (b *Block) Insert(x uint32) {
	if hasAVX2 {
		w := unsafecast.Slice[uint32](b[:])
		archsimd.LoadUint32x8Slice(w).Or(blockMask(x)).StoreSlice(w)
		return
	}
	b[0] |= 1 << ((x * salt0) >> 27)
	b[1] |= 1 << ((x * salt1) >> 27)
	b[2] |= 1 << ((x * salt2) >> 27)
	b[3] |= 1 << ((x * salt3) >> 27)
	b[4] |= 1 << ((x * salt4) >> 27)
	b[5] |= 1 << ((x * salt5) >> 27)
	b[6] |= 1 << ((x * salt6) >> 27)
	b[7] |= 1 << ((x * salt7) >> 27)
}

func (b *Block) Check(x uint32) bool {
	if hasAVX2 {
		m := blockMask(x)
		w := unsafecast.Slice[uint32](b[:])
		return archsimd.LoadUint32x8Slice(w).And(m).Equal(m).ToBits() == 0xFF
	}
	return ((b[0] & (1 << ((x * salt0) >> 27))) != 0) &&
		((b[1] & (1 << ((x * salt1) >> 27))) != 0) &&
		((b[2] & (1 << ((x * salt2) >> 27))) != 0) &&
		((b[3] & (1 << ((x * salt3) >> 27))) != 0) &&
		((b[4] & (1 << ((x * salt4) >> 27))) != 0) &&
		((b[5] & (1 << ((x * salt5) >> 27))) != 0) &&
		((b[6] & (1 << ((x * salt6) >> 27))) != 0) &&
		((b[7] & (1 << ((x * salt7) >> 27))) != 0)
}

func filterInsertBulk(f []Block, x []uint64) {
	for i := range x {
		filterInsert(f, x[i])
	}
}

func filterInsert(f []Block, x uint64) {
	f[fasthash1x64(x, int32(len(f)))].Insert(uint32(x))
}

func filterCheck(f []Block, x uint64) bool {
	return f[fasthash1x64(x, int32(len(f)))].Check(uint32(x))
}
