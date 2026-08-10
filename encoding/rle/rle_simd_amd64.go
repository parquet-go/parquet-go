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

// rotate1x8 rotates the 8 lanes of a vector left by one position; a group is
// uniform if and only if it equals its own rotation.
var rotate1x8 = [8]uint32{1, 2, 3, 4, 5, 6, 7, 0}

// encodeInt32IndexEqual8ContiguousSIMD returns the index of the first group of
// 8 words that all have the same value.
//
// Comparing each group against its own lane rotation avoids the
// memory-to-GPR-to-vector round trip of broadcasting the first element, and
// the loop is unrolled 4 groups per iteration to amortize the loop overhead;
// checking the group masks in order preserves the first-match semantics.
func encodeInt32IndexEqual8ContiguousSIMD(words [][8]int32) (n int) {
	rot := archsimd.LoadUint32x8Slice(rotate1x8[:])
	// The array pointer conversion carries the only bounds check of the loop
	// body; the constant group indexes below compile to constant address
	// offsets with no checks.
	d := words
	for len(d) >= 4 {
		c := (*[4][8]int32)(d)
		w0 := archsimd.LoadInt32x8Slice(c[0][:])
		w1 := archsimd.LoadInt32x8Slice(c[1][:])
		w2 := archsimd.LoadInt32x8Slice(c[2][:])
		w3 := archsimd.LoadInt32x8Slice(c[3][:])
		e0 := w0.Equal(w0.Permute(rot)).ToBits()
		e1 := w1.Equal(w1.Permute(rot)).ToBits()
		e2 := w2.Equal(w2.Permute(rot)).ToBits()
		e3 := w3.Equal(w3.Permute(rot)).ToBits()
		// e+1 carries into bit 8 only when e == 0xFF, so this tests whether
		// any of the four groups is uniform with a single branch.
		any := (uint32(e0) + 1) | (uint32(e1) + 1) | (uint32(e2) + 1) | (uint32(e3) + 1)
		if any&0x100 == 0 {
			d = d[4:]
			continue
		}
		n = len(words) - len(d)
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
	for n = len(words) - len(d); n < len(words); n++ {
		w := archsimd.LoadInt32x8Slice(words[n][:])
		if w.Equal(w.Permute(rot)).ToBits() == 0xFF {
			break
		}
	}
	return n
}
