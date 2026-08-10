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
func encodeInt32IndexEqual8ContiguousSIMD(words [][8]int32) (n int) {
	for n < len(words) {
		w := archsimd.LoadInt32x8Slice(words[n][:])
		if w.Equal(archsimd.BroadcastInt32x8(words[n][0])).ToBits() == 0xFF {
			break
		}
		n++
	}
	return n
}
