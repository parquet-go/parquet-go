package bytestreamsplit

import (
	"fmt"
	"testing"
)

func benchmarkTranspose(b *testing.B, size int, codec func(dst, src []byte)) {
	for _, numBytes := range []int{4 * 1024, 256 * 1024, 2048 * 1024} {
		b.Run(fmt.Sprintf("%dKiB", numBytes/1024), func(b *testing.B) {
			src := make([]byte, numBytes)
			dst := make([]byte, numBytes)
			for i := range src {
				src[i] = byte(i)
			}
			b.SetBytes(int64(numBytes))
			for b.Loop() {
				codec(dst, src)
			}
		})
	}
}

func BenchmarkEncodeFloat(b *testing.B)  { benchmarkTranspose(b, 4, encodeFloat) }
func BenchmarkDecodeFloat(b *testing.B)  { benchmarkTranspose(b, 4, decodeFloat) }
func BenchmarkEncodeDouble(b *testing.B) { benchmarkTranspose(b, 8, encodeDouble) }
func BenchmarkDecodeDouble(b *testing.B) { benchmarkTranspose(b, 8, decodeDouble) }
