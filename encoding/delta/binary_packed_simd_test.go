//go:build goexperiment.simd

package delta

import "testing"

func TestBlockDeltaInt32SIMD(t *testing.T) {
	testBlockDeltaInt32(t, blockDeltaInt32SIMD)
}

func TestBlockMinInt32SIMD(t *testing.T) {
	testBlockMinInt32(t, blockMinInt32SIMD)
}

func TestBlockSubInt32SIMD(t *testing.T) {
	testBlockSubInt32(t, blockSubInt32SIMD)
}

func TestBlockBitWidthsInt32SIMD(t *testing.T) {
	testBlockBitWidthsInt32(t, blockBitWidthsInt32SIMD)
}

func TestEncodeMiniBlockInt32SIMD(t *testing.T) {
	testEncodeMiniBlockInt32(t, encodeMiniBlockInt32SIMD)
}

func TestBlockDeltaInt64SIMD(t *testing.T) {
	testBlockDeltaInt64(t, blockDeltaInt64SIMD)
}

func TestBlockMinInt64SIMD(t *testing.T) {
	testBlockMinInt64(t, blockMinInt64SIMD)
}

func TestBlockSubInt64SIMD(t *testing.T) {
	testBlockSubInt64(t, blockSubInt64SIMD)
}

func TestBlockBitWidthsInt64SIMD(t *testing.T) {
	testBlockBitWidthsInt64(t, blockBitWidthsInt64SIMD)
}

func TestEncodeMiniBlockInt64SIMD(t *testing.T) {
	testEncodeMiniBlockInt64(t, encodeMiniBlockInt64SIMD)
}

func BenchmarkBlockDeltaInt32SIMD(b *testing.B) {
	benchmarkBlockDeltaInt32(b, blockDeltaInt32SIMD)
}

func BenchmarkBlockMinInt32SIMD(b *testing.B) {
	benchmarkBlockMinInt32(b, blockMinInt32SIMD)
}

func BenchmarkBlockSubInt32SIMD(b *testing.B) {
	benchmarkBlockSubInt32(b, blockSubInt32SIMD)
}

func BenchmarkBlockBitWidthsInt32SIMD(b *testing.B) {
	benchmarkBlockBitWidthsInt32(b, blockBitWidthsInt32SIMD)
}
