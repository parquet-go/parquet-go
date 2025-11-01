package bytealg_test

import (
	"fmt"
	"testing"

	"github.com/parquet-go/parquet-go/internal/bytealg"
)

func TestBroadcast(t *testing.T) {
	const N = 100_0000
	buffer := make([]byte, N)

	for n := 1; n <= N; n = (n * 2) + 1 {
		t.Run(fmt.Sprintf("size=%d", n), func(t *testing.T) {
			b := buffer[:n]

			for i := range b {
				b[i] = 0
			}

			bytealg.Broadcast(b, 42)

			for i, c := range b {
				if c != 42 {
					t.Fatalf("byte at index %d has value %d", i, c)
				}
			}
		})
	}
}

func BenchmarkBroadcast(b *testing.B) {
	for _, size := range []int{0, 10, 100, 1000, 10_000} {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			data := make([]byte, size)

			for b.Loop() {
				bytealg.Broadcast(data, 1)
			}

			b.SetBytes(int64(size))
		})
	}
}
