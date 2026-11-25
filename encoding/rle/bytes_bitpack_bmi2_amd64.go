//go:build !purego && (!goexperiment.simd || go1.27)

package rle

//go:noescape
func encodeBytesBitpackBMI2(dst []byte, src []uint64, bitWidth uint) int

//go:noescape
func decodeBytesBitpackBMI2(dst, src []byte, count, bitWidth uint)
