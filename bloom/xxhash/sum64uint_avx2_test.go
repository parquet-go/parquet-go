//go:build goexperiment.simd

package xxhash

import (
	"testing"

	"simd/archsimd"
)

// The dispatcher prefers the AVX-512 tier wherever it exists, so the AVX2
// kernels are validated directly against the scalar reference.

func TestMultiSum64AVX2Kernels(t *testing.T) {
	if !archsimd.X86.AVX2() {
		t.Skip("requires AVX2")
	}
	v64 := make([]uint64, 259)
	v32 := make([]uint32, 259)
	for i := range v64 {
		v64[i] = uint64(i) * 0x9E3779B97F4A7C15
		v32[i] = uint32(i) * 0x9E3779B9
	}
	h := make([]uint64, len(v64))
	n := multiSum64Uint64AVX2(h, v64, len(v64))
	for i := range n {
		if want := Sum64Uint64(v64[i]); h[i] != want {
			t.Fatalf("uint64 kernel: h[%d] = %016x, want %016x", i, h[i], want)
		}
	}
	n = multiSum64Uint32AVX2(h, v32, len(v32))
	for i := range n {
		if want := Sum64Uint32(v32[i]); h[i] != want {
			t.Fatalf("uint32 kernel: h[%d] = %016x, want %016x", i, h[i], want)
		}
	}
}
