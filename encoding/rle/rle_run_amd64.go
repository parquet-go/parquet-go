//go:build !purego && goexperiment.simd && go1.27

package rle

import "simd/archsimd"

// The archsimd run detector is slower than these kernels. purego builds keep
// the Go fallback from rle_purego.go; this file only exists so the assembly
// can be linked beside the bit-packing kernels that stay in Go.

func init() {
	if archsimd.X86.AVX2() {
		encodeInt32IndexEqual8Contiguous = encodeInt32IndexEqual8ContiguousAVX2
		// The archsimd int32 packer is slower at every width the benchmark
		// measures.
		encodeInt32Bitpack = encodeInt32BitpackAVX2
	} else {
		encodeInt32IndexEqual8Contiguous = encodeInt32IndexEqual8ContiguousSSE
	}
}

func useBytesBitpackBMI2() {
	encodeBytesBitpack = encodeBytesBitpackBMI2
	decodeBytesBitpack = decodeBytesBitpackBMI2
}

func encodeInt32BitpackAVX2(dst []byte, src [][8]int32, bitWidth uint) int {
	switch {
	case bitWidth == 0:
		return 0
	case bitWidth <= 16:
		return encodeInt32Bitpack1to16bitsAVX2(dst, src, bitWidth)
	default:
		return encodeInt32BitpackDefault(dst, src, bitWidth)
	}
}

//go:noescape
func encodeInt32IndexEqual8ContiguousAVX2(words [][8]int32) int

//go:noescape
func encodeInt32IndexEqual8ContiguousSSE(words [][8]int32) int

//go:noescape
func encodeInt32Bitpack1to16bitsAVX2(dst []byte, src [][8]int32, bitWidth uint) int
