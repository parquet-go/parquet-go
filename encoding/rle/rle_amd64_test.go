//go:build !purego && amd64
// +build !purego,amd64

package rle

import (
	"testing"

	"golang.org/x/sys/cpu"
)

func requireAVX2(tb testing.TB) {
	if !cpu.X86.HasAVX2 {
		tb.Skip("AVX2 not supported")
	}
}

func TestEncodeInt32IndexEqual8ContiguousAVX2(t *testing.T) {
	requireAVX2(t)
	testEncodeInt32IndexEqual8Contiguous(t, encodeInt32IndexEqual8ContiguousAVX2)
}

func TestEncodeInt32IndexEqual8ContiguousSSE(t *testing.T) {
	requireAVX2(t)
	testEncodeInt32IndexEqual8Contiguous(t, encodeInt32IndexEqual8ContiguousSSE)
}

func BenchmarkEncodeInt32IndexEqual8ContiguousAVX2(b *testing.B) {
	requireAVX2(b)
	benchmarkEncodeInt32IndexEqual8Contiguous(b, encodeInt32IndexEqual8ContiguousAVX2)
}

func BenchmarkEncodeInt32IndexEqual8ContiguousSSE(b *testing.B) {
	requireAVX2(b)
	benchmarkEncodeInt32IndexEqual8Contiguous(b, encodeInt32IndexEqual8ContiguousSSE)
}
