//go:build goexperiment.simd

package rle

import "testing"

func TestEncodeInt32IndexEqual8ContiguousSIMD(t *testing.T) {
	reference := func(words [][8]int32) (n int) {
		for n < len(words) && words[n] != broadcast8x4(words[n][0]) {
			n++
		}
		return n
	}

	for _, size := range []int{0, 1, 2, 3, 4, 5, 7, 8, 9, 100} {
		for uniform := -1; uniform < size; uniform++ {
			words := make([][8]int32, size)
			for i := range words {
				for j := range words[i] {
					words[i][j] = int32(8*i + j)
				}
			}
			if uniform >= 0 {
				words[uniform] = [8]int32{42, 42, 42, 42, 42, 42, 42, 42}
			}
			want := reference(words)
			if got := encodeInt32IndexEqual8ContiguousSIMD(words); got != want {
				t.Fatalf("size=%d uniform=%d: got %d, want %d", size, uniform, got, want)
			}
		}
	}
}
