//go:build goexperiment.simd

package rle

import (
	"bytes"
	"math/rand"
	"testing"

	"simd/archsimd"
)

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

func TestBytesBitpackSIMD(t *testing.T) {
	if !archsimd.X86.AVX512() || !archsimd.X86.AVX512VBMI() {
		t.Skip("requires AVX512VBMI")
	}
	prng := rand.New(rand.NewSource(0))
	for bitWidth := uint(1); bitWidth <= 8; bitWidth++ {
		for _, words := range []int{0, 1, 2, 7, 8, 9, 63, 64, 100, 1000} {
			src := make([]uint64, words)
			for i := range src {
				src[i] = prng.Uint64()
			}
			want := make([]byte, words*8+8)
			got := make([]byte, words*8+8)
			n1 := encodeBytesBitpackDefault(want, src, bitWidth)
			n2 := encodeBytesBitpackSIMD(got, src, bitWidth)
			if n1 != n2 || !bytes.Equal(want[:n1], got[:n1]) {
				t.Fatalf("encode mismatch bitWidth=%d words=%d", bitWidth, words)
			}
			count := uint(words * 8)
			wantDec := make([]byte, (count+7)/8*8)
			gotDec := make([]byte, (count+7)/8*8)
			decodeBytesBitpackDefault(wantDec, want[:n1], count, bitWidth)
			decodeBytesBitpackSIMD(gotDec, want[:n1], count, bitWidth)
			if !bytes.Equal(wantDec, gotDec) {
				t.Fatalf("decode mismatch bitWidth=%d words=%d", bitWidth, words)
			}
		}
	}
}
