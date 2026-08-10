//go:build !purego && goexperiment.simd

package rle

import "simd/archsimd"

// The functions in this file are simd/archsimd replacements for some of the
// assembly kernels declared in rle_amd64.go. The BMI2 bit-packing kernels
// have no archsimd equivalent (PDEP/PEXT are not exposed) and remain in
// assembly.
//
// Go compiles the files of a package in file name order, so this init runs
// after the one in rle_amd64.go and overrides its choice of implementation.
func init() {
	if archsimd.X86.AVX2() {
		encodeInt32IndexEqual8Contiguous = encodeInt32IndexEqual8ContiguousSIMD
	}
}

// encodeInt32IndexEqual8ContiguousSIMD returns the index of the first group of
// 8 words that all have the same value.
//
// The loop is unrolled 4 groups per iteration to amortize the loop overhead;
// checking the group masks in order preserves the first-match semantics.
func encodeInt32IndexEqual8ContiguousSIMD(words [][8]int32) (n int) {
	for n+4 <= len(words) {
		e0 := archsimd.LoadInt32x8Slice(words[n][:]).Equal(archsimd.BroadcastInt32x8(words[n][0])).ToBits()
		e1 := archsimd.LoadInt32x8Slice(words[n+1][:]).Equal(archsimd.BroadcastInt32x8(words[n+1][0])).ToBits()
		e2 := archsimd.LoadInt32x8Slice(words[n+2][:]).Equal(archsimd.BroadcastInt32x8(words[n+2][0])).ToBits()
		e3 := archsimd.LoadInt32x8Slice(words[n+3][:]).Equal(archsimd.BroadcastInt32x8(words[n+3][0])).ToBits()
		// e+1 carries into bit 8 only when e == 0xFF, so this tests whether
		// any of the four groups is uniform with a single branch.
		any := (uint32(e0) + 1) | (uint32(e1) + 1) | (uint32(e2) + 1) | (uint32(e3) + 1)
		if any&0x100 == 0 {
			n += 4
			continue
		}
		switch {
		case e0 == 0xFF:
		case e1 == 0xFF:
			n++
		case e2 == 0xFF:
			n += 2
		default:
			n += 3
		}
		return n
	}
	for n < len(words) {
		w := archsimd.LoadInt32x8Slice(words[n][:])
		if w.Equal(archsimd.BroadcastInt32x8(words[n][0])).ToBits() == 0xFF {
			break
		}
		n++
	}
	return n
}
