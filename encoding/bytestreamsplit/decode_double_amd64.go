//go:build !purego && goexperiment.simd && go1.27

package bytestreamsplit

import "golang.org/x/sys/cpu"

// decodeDouble stays on the AVX-512 assembly. The archsimd transpose was
// slower at every size measured, and it does not share data with the float
// kernels that remain in Go.

var decodeDoubleHasAVX512 = cpu.X86.HasAVX512 &&
	cpu.X86.HasAVX512F &&
	cpu.X86.HasAVX512VL &&
	cpu.X86.HasAVX512VBMI // VPERMB

//go:noescape
func decodeDouble(dst, src []byte)
