//go:build goexperiment.simd

package parquet

import (
	"bytes"
	"math/rand"
	"testing"
)

// Differential test of the vectorized BE128 min/max against the scalar
// implementations, covering duplicates, extreme values, and lengths around
// the 8 value chunking boundary. The vector path only runs on AVX-512
// hardware; elsewhere both sides take the scalar path.
func TestMinMaxBE128SIMD(t *testing.T) {
	prng := rand.New(rand.NewSource(3))
	for _, n := range []int{1, 7, 8, 9, 15, 16, 17, 100, 1000} {
		for trial := 0; trial < 10; trial++ {
			data := make([][16]byte, n)
			for i := range data {
				prng.Read(data[i][:])
				switch trial {
				case 1:
					// Many duplicates.
					data[i] = data[0]
				case 2:
					// Extreme values that tie naive sentinels.
					if i%3 == 0 {
						data[i] = [16]byte{}
					}
					if i%3 == 1 {
						data[i] = [16]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
					}
				}
			}
			if got, want := minBE128(data), minBE128Scalar(data); !bytes.Equal(got, want) {
				t.Fatalf("minBE128 n=%d trial=%d: got %x, want %x", n, trial, got, want)
			}
			if got, want := maxBE128(data), maxBE128Scalar(data); !bytes.Equal(got, want) {
				t.Fatalf("maxBE128 n=%d trial=%d: got %x, want %x", n, trial, got, want)
			}
		}
	}
}
