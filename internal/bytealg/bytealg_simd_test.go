//go:build goexperiment.simd

package bytealg

import (
	"bytes"
	"testing"
)

// TestCountAccumulatorFlush exercises buffer sizes around the byte
// accumulator flush boundaries of the vectorized Count (255 rounds of 128 or
// 256 bytes), which the small random buffers of TestCount never reach, and a
// worst-case input where every byte matches so the accumulators grow at the
// maximum rate.
func TestCountAccumulatorFlush(t *testing.T) {
	for _, size := range []int{
		255*128 - 1, 255 * 128, 255*128 + 1,
		255*256 - 1, 255 * 256, 255*256 + 1,
		3*255*256 + 77,
	} {
		data := make([]byte, size)
		for i := range data {
			data[i] = byte(i % 3)
		}
		for _, v := range []byte{0, 1, 2, 3} {
			want := bytes.Count(data, []byte{v})
			if got := Count(data, v); got != want {
				t.Fatalf("size=%d value=%d: got %d, want %d", size, v, got, want)
			}
		}

		uniform := bytes.Repeat([]byte{42}, size)
		if got := Count(uniform, 42); got != size {
			t.Fatalf("uniform size=%d: got %d, want %d", size, got, size)
		}
	}
}
