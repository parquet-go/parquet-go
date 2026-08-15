//go:build goexperiment.simd

package xxhash

import (
	"testing"

	"simd/archsimd"
)

// Direct benchmarks of the AVX2 tier kernels: the dispatcher prefers the
// AVX-512 tier on machines that have it, so these call the AVX2 kernels
// explicitly to compare them against the scalar fallback they replace on
// AVX2-only hosts.

func BenchmarkMultiSum64Uint64AVX2(b *testing.B) {
	if !archsimd.X86.AVX2() {
		b.Skip("requires AVX2")
	}
	h := make([]uint64, 512)
	v := make([]uint64, 512)
	for i := range v {
		v[i] = uint64(i) * 0x9E3779B97F4A7C15
	}
	b.SetBytes(8 * int64(len(v)))
	for b.Loop() {
		multiSum64Uint64AVX2(h, v, len(v))
	}
}

func BenchmarkMultiSum64Uint32AVX2(b *testing.B) {
	if !archsimd.X86.AVX2() {
		b.Skip("requires AVX2")
	}
	h := make([]uint64, 512)
	v := make([]uint32, 512)
	for i := range v {
		v[i] = uint32(i) * 0x9E3779B9
	}
	b.SetBytes(4 * int64(len(v)))
	for b.Loop() {
		multiSum64Uint32AVX2(h, v, len(v))
	}
}

func BenchmarkMultiSum64Uint64Scalar(b *testing.B) {
	h := make([]uint64, 512)
	v := make([]uint64, 512)
	for i := range v {
		v[i] = uint64(i) * 0x9E3779B97F4A7C15
	}
	b.SetBytes(8 * int64(len(v)))
	for b.Loop() {
		for i := range v {
			h[i] = Sum64Uint64(v[i])
		}
	}
}

func BenchmarkMultiSum64Uint32Scalar(b *testing.B) {
	h := make([]uint64, 512)
	v := make([]uint32, 512)
	for i := range v {
		v[i] = uint32(i) * 0x9E3779B9
	}
	b.SetBytes(4 * int64(len(v)))
	for b.Loop() {
		for i := range v {
			h[i] = Sum64Uint32(v[i])
		}
	}
}
