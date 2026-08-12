//go:build goexperiment.simd

package rle

import (
	"encoding/binary"

	"simd/archsimd"

	"github.com/parquet-go/bitpack/unsafecast"
)

// The functions in this file are simd/archsimd replacements for some of the
// kernels dispatched through the function variables of rle_amd64.go (assembly
// builds) or rle_purego.go (purego builds). The BMI2 bit-packing kernels have
// no archsimd equivalent (PDEP/PEXT are not exposed) and keep their default
// dispatch.
//
// The override is safe in both builds: package variable initializers run
// before all init functions, and Go compiles the files of a package in file
// name order, so this init runs after the one in rle_amd64.go.
func init() {
	if archsimd.X86.AVX2() {
		encodeInt32IndexEqual8Contiguous = encodeInt32IndexEqual8ContiguousSIMD
	}
	if archsimd.X86.AVX512() {
		encodeInt32Bitpack = encodeInt32BitpackSIMD
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
		archsimd.ClearAVXUpperBits()
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
	archsimd.ClearAVXUpperBits()
	return n
}

// Lane indexes folding odd lanes onto even lanes and lanes {2,6} onto {0,4}
// in the packing reduction (same shape as the DELTA_BINARY_PACKED mini block
// packer; the useful results live in lanes 0 and 4 afterwards).
var (
	bitpackFoldOdd  = [8]uint64{1, 1, 3, 3, 5, 5, 7, 7}
	bitpackFoldPair = [8]uint64{2, 2, 2, 2, 6, 6, 6, 6}
)

func encodeInt32BitpackSIMD(dst []byte, src [][8]int32, bitWidth uint) int {
	switch {
	case bitWidth == 0:
		return 0
	case bitWidth <= 16:
		return encodeInt32Bitpack1to16bitsSIMD(dst, src, bitWidth)
	default:
		return encodeInt32BitpackDefault(dst, src, bitWidth)
	}
}

// encodeInt32Bitpack1to16bitsSIMD packs groups of 8 values of 1 to 16 bits
// each with the fold reduction used by the DELTA_BINARY_PACKED mini block
// packers: two vector fold steps leave 4 packed values in lane 0 and 4 in
// lane 4, and a scalar 128 bits stitch combines them. Each group spans
// exactly bitWidth bytes and stores 16 bytes at its byte aligned offset; the
// zeroed tail is overwritten by the next group and the caller reserves 32
// bytes of headroom (appendBitPackedInt32), like it did for the assembly.
func encodeInt32Bitpack1to16bitsSIMD(dst []byte, src [][8]int32, bitWidth uint) int {
	w := uint64(bitWidth)
	var sh1v [8]uint64
	var sh2v [8]uint64
	for i := range sh1v {
		if i%2 == 1 {
			sh1v[i] = w
		}
		if i == 2 || i == 6 {
			sh2v[i] = 2 * w
		}
	}
	sh1 := archsimd.LoadUint64x8Slice(sh1v[:])
	sh2 := archsimd.LoadUint64x8Slice(sh2v[:])
	fo := archsimd.LoadUint64x8Slice(bitpackFoldOdd[:])
	fp := archsimd.LoadUint64x8Slice(bitpackFoldPair[:])
	off := uint(0)
	for j := range src {
		u := unsafecast.Slice[uint32](src[j][:])
		t := archsimd.LoadUint32x8Slice(u).ExtendToUint64().ShiftLeft(sh1)
		t = t.Or(t.Permute(fo))
		t = t.ShiftLeft(sh2)
		t = t.Or(t.Permute(fp))
		q0 := t.GetLo().GetLo().GetElem(0)
		q1 := t.GetHi().GetLo().GetElem(0)
		lo := q0 | q1<<(4*w)
		hi := q1 >> (64 - 4*w)
		binary.LittleEndian.PutUint64(dst[off:], lo)
		binary.LittleEndian.PutUint64(dst[off+8:], hi)
		off += uint(w)
	}
	archsimd.ClearAVXUpperBits()
	return int(off)
}
