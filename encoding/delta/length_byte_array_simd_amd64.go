//go:build goexperiment.simd

package delta

import "simd/archsimd"

// This file provides implementations of the DELTA_LENGTH_BYTE_ARRAY length
// kernels based on the simd/archsimd package, replacing the hand-written
// assembly of length_byte_array_amd64.s when GOEXPERIMENT=simd is set. The
// assembly deliberately stayed on SSE2 (4 lanes); these use AVX2 (8 lanes).

func encodeByteArrayLengths(lengths []int32, offsets []uint32) {
	i := 0
	if archsimd.X86.AVX2() {
		for ; i+8 <= len(lengths); i += 8 {
			a := archsimd.LoadUint32x8Slice(offsets[i:])
			b := archsimd.LoadUint32x8Slice(offsets[i+1:])
			b.Sub(a).AsInt32x8().StoreSlice(lengths[i:])
		}
	}
	for ; i < len(lengths); i++ {
		lengths[i] = int32(offsets[i+1] - offsets[i])
	}
}

// Index vectors of the prefix sum ladder: each step shifts the lanes up by
// 1, 2 or 4 positions; the lanes shifted in are zeroed with the matching
// masks below (the index value of a masked-out lane is irrelevant).
var (
	shiftLanes1 = [8]uint32{0, 0, 1, 2, 3, 4, 5, 6}
	shiftLanes2 = [8]uint32{0, 0, 0, 1, 2, 3, 4, 5}
	shiftLanes4 = [8]uint32{0, 0, 0, 0, 0, 1, 2, 3}
	laneIndexes = [8]int32{0, 1, 2, 3, 4, 5, 6, 7}
)

// decodeByteArrayLengths computes the exclusive prefix sum of lengths into
// offsets, returning the total and the first negative length if any (the
// caller reports it as a data corruption error).
//
// Unlike the assembly version, which reported any negative length as -1,
// this reports the first negative value exactly like the purego version.
func decodeByteArrayLengths(offsets []uint32, lengths []int32) (uint32, int32) {
	lastOffset := uint32(0)
	i := 0
	if archsimd.X86.AVX2() && len(lengths) >= 8 {
		zero := archsimd.BroadcastInt32x8(0)
		idx1 := archsimd.LoadUint32x8Slice(shiftLanes1[:])
		idx2 := archsimd.LoadUint32x8Slice(shiftLanes2[:])
		idx4 := archsimd.LoadUint32x8Slice(shiftLanes4[:])
		// The masks select the lanes NOT shifted in by each ladder step.
		// They are built with compares because Mask32x8FromBits lowers to
		// KMOVD, which requires AVX-512.
		iota8 := archsimd.LoadInt32x8Slice(laneIndexes[:])
		m1 := iota8.Greater(zero)
		m2 := iota8.Greater(archsimd.BroadcastInt32x8(1))
		m4 := iota8.Greater(archsimd.BroadcastInt32x8(3))
		for ; i+8 <= len(lengths); i += 8 {
			v := archsimd.LoadInt32x8Slice(lengths[i:])
			if v.Less(zero).ToBits() != 0 {
				// A negative length: let the scalar loop locate it.
				break
			}
			// Inclusive prefix sum of the 8 lanes by shift-and-add.
			s := v.Add(v.Permute(idx1).Merge(zero, m1))
			s = s.Add(s.Permute(idx2).Merge(zero, m2))
			s = s.Add(s.Permute(idx4).Merge(zero, m4))
			// The offsets are the exclusive prefix sums plus the running
			// total: shift the inclusive sums up one lane.
			ex := s.Permute(idx1).Merge(zero, m1).Add(archsimd.BroadcastInt32x8(int32(lastOffset)))
			ex.AsUint32x8().StoreSlice(offsets[i:])
			lastOffset += uint32(s.GetHi().GetElem(3))
		}
	}
	for ; i < len(lengths); i++ {
		n := lengths[i]
		if n < 0 {
			return lastOffset, n
		}
		offsets[i] = lastOffset
		lastOffset += uint32(n)
	}
	offsets[len(lengths)] = lastOffset
	return lastOffset, 0
}
