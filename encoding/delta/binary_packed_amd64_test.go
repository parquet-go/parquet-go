//go:build amd64 && !purego && !goexperiment.simd

package delta

import (
	"testing"

	"golang.org/x/sys/cpu"
)

func requireAVX2(t testing.TB) {
	if !cpu.X86.HasAVX2 {
		t.Skip("CPU does not support AVX2")
	}
}

func TestBlockDeltaInt32AVX2(t *testing.T) {
	requireAVX2(t)
	testBlockDeltaInt32(t, blockDeltaInt32AVX2)
}

func TestBlockMinInt32AVX2(t *testing.T) {
	requireAVX2(t)
	testBlockMinInt32(t, blockMinInt32AVX2)
}

func TestBlockSubInt32AVX2(t *testing.T) {
	requireAVX2(t)
	testBlockSubInt32(t, blockSubInt32AVX2)
}

func TestBlockBitWidthsInt32AVX2(t *testing.T) {
	requireAVX2(t)
	testBlockBitWidthsInt32(t, blockBitWidthsInt32AVX2)
}

func TestEncodeMiniBlockInt32AVX2(t *testing.T) {
	requireAVX2(t)
	testEncodeMiniBlockInt32(t,
		func(dst []byte, src *[miniBlockSize]int32, bitWidth uint) {
			encodeMiniBlockInt32AVX2(&dst[0], src, bitWidth)
		},
	)
}

func BenchmarkBlockDeltaInt32AVX2(b *testing.B) {
	requireAVX2(b)
	benchmarkBlockDeltaInt32(b, blockDeltaInt32AVX2)
}

func BenchmarkBlockMinInt32AVX2(b *testing.B) {
	requireAVX2(b)
	benchmarkBlockMinInt32(b, blockMinInt32AVX2)
}

func BenchmarkBlockSubInt32AVX2(b *testing.B) {
	requireAVX2(b)
	benchmarkBlockSubInt32(b, blockSubInt32AVX2)
}

func BenchmarkBlockBitWidthsInt32AVX2(b *testing.B) {
	requireAVX2(b)
	benchmarkBlockBitWidthsInt32(b, blockBitWidthsInt32AVX2)
}

func BenchmarkEncodeMiniBlockInt32AVX2(b *testing.B) {
	requireAVX2(b)
	benchmarkEncodeMiniBlockInt32(b,
		func(dst []byte, src *[miniBlockSize]int32, bitWidth uint) {
			encodeMiniBlockInt32AVX2(&dst[0], src, bitWidth)
		},
	)
}

func TestBlockDeltaInt64AVX2(t *testing.T) {
	requireAVX2(t)
	testBlockDeltaInt64(t, blockDeltaInt64AVX2)
}

func TestBlockMinInt64AVX2(t *testing.T) {
	requireAVX2(t)
	testBlockMinInt64(t, blockMinInt64AVX2)
}

func TestBlockSubInt64AVX2(t *testing.T) {
	requireAVX2(t)
	testBlockSubInt64(t, blockSubInt64AVX2)
}

func TestBlockBitWidthsInt64AVX2(t *testing.T) {
	requireAVX2(t)
	testBlockBitWidthsInt64(t, blockBitWidthsInt64AVX2)
}

func TestEncodeMiniBlockInt64AVX2(t *testing.T) {
	requireAVX2(t)
	testEncodeMiniBlockInt64(t,
		func(dst []byte, src *[miniBlockSize]int64, bitWidth uint) {
			encodeMiniBlockInt64AVX2(&dst[0], src, bitWidth)
		},
	)
}

func BenchmarkBlockDeltaInt64AVX2(b *testing.B) {
	requireAVX2(b)
	benchmarkBlockDeltaInt64(b, blockDeltaInt64AVX2)
}

func BenchmarkBlockMinInt64AVX2(b *testing.B) {
	requireAVX2(b)
	benchmarkBlockMinInt64(b, blockMinInt64AVX2)
}

func BenchmarkBlockSubInt64AVX2(b *testing.B) {
	requireAVX2(b)
	benchmarkBlockSubInt64(b, blockSubInt64AVX2)
}

func BenchmarkBlockBitWidthsInt64AVX2(b *testing.B) {
	requireAVX2(b)
	benchmarkBlockBitWidthsInt64(b, blockBitWidthsInt64AVX2)
}

func BenchmarkEncodeMiniBlockInt64AVX2(b *testing.B) {
	requireAVX2(b)
	benchmarkEncodeMiniBlockInt64(b,
		func(dst []byte, src *[miniBlockSize]int64, bitWidth uint) {
			encodeMiniBlockInt64AVX2(&dst[0], src, bitWidth)
		},
	)
}
