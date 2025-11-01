package delta

import (
	"bytes"
	"fmt"
	"math/bits"
	"testing"
)

func maxLen32(miniBlock []int32) (maxLen int) {
	for _, v := range miniBlock {
		if n := bits.Len32(uint32(v)); n > maxLen {
			maxLen = n
		}
	}
	return maxLen
}

func maxLen64(miniBlock []int64) (maxLen int) {
	for _, v := range miniBlock {
		if n := bits.Len64(uint64(v)); n > maxLen {
			maxLen = n
		}
	}
	return maxLen
}

func TestBlockDeltaInt32(t *testing.T) {
	testBlockDeltaInt32(t, blockDeltaInt32)
}

func testBlockDeltaInt32(t *testing.T, f func(*[blockSize]int32, int32) int32) {
	t.Helper()
	block := [blockSize]int32{}
	for i := range block {
		block[i] = int32(2 * (i + 1))
	}
	lastValue := f(&block, 0)
	if lastValue != 2*blockSize {
		t.Errorf("wrong last block value: want=%d got=%d", 2*blockSize, lastValue)
	}
	for i := range block {
		j := int32(2 * (i + 0))
		k := int32(2 * (i + 1))
		if block[i] != (k - j) {
			t.Errorf("wrong block delta at index %d: want=%d got=%d", i, k-j, block[i])
		}
	}
}

func TestBlockMinInt32(t *testing.T) {
	testBlockMinInt32(t, blockMinInt32)
}

func testBlockMinInt32(t *testing.T, f func(*[blockSize]int32) int32) {
	t.Helper()
	block := [blockSize]int32{}
	for i := range block {
		block[i] = blockSize - int32(i)
	}
	if min := f(&block); min != 1 {
		t.Errorf("wrong min block value: want=1 got=%d", min)
	}
}

func TestBlockSubInt32(t *testing.T) {
	testBlockSubInt32(t, blockSubInt32)
}

func testBlockSubInt32(t *testing.T, f func(*[blockSize]int32, int32)) {
	t.Helper()
	block := [blockSize]int32{}
	for i := range block {
		block[i] = int32(i)
	}
	f(&block, 1)
	for i := range block {
		if block[i] != int32(i-1) {
			t.Errorf("wrong block value at index %d: want=%d got=%d", i, i-1, block[i])
		}
	}
}

func TestBlockBitWidthsInt32(t *testing.T) {
	testBlockBitWidthsInt32(t, blockBitWidthsInt32)
}

func testBlockBitWidthsInt32(t *testing.T, f func(*[numMiniBlocks]byte, *[blockSize]int32)) {
	t.Helper()
	bitWidths := [numMiniBlocks]byte{}
	block := [blockSize]int32{}
	for i := range block {
		block[i] = int32(i)
	}
	f(&bitWidths, &block)

	want := [numMiniBlocks]byte{}
	for i := range want {
		j := (i + 0) * miniBlockSize
		k := (i + 1) * miniBlockSize
		want[i] = byte(maxLen32(block[j:k]))
	}

	if bitWidths != want {
		t.Errorf("wrong bit widths: want=%d got=%d", want, bitWidths)
	}
}

func TestEncodeMiniBlockInt32(t *testing.T) {
	testEncodeMiniBlockInt32(t, encodeMiniBlockInt32)
}

func testEncodeMiniBlockInt32(t *testing.T, f func([]byte, *[miniBlockSize]int32, uint)) {
	t.Helper()
	for bitWidth := uint(1); bitWidth <= 32; bitWidth++ {
		t.Run(fmt.Sprintf("bitWidth=%d", bitWidth), func(t *testing.T) {
			got := [4*miniBlockSize + 32]byte{}
			src := [miniBlockSize]int32{}
			for i := range src {
				src[i] = int32(i) & int32((1<<bitWidth)-1)
			}

			want := [4*miniBlockSize + 32]byte{}
			bitOffset := uint(0)

			for _, bits := range src {
				for b := uint(0); b < bitWidth; b++ {
					x := bitOffset / 8
					y := bitOffset % 8
					want[x] |= byte(((bits >> b) & 1) << y)
					bitOffset++
				}
			}

			f(got[:], &src, bitWidth)
			n := (miniBlockSize * bitWidth) / 8

			if !bytes.Equal(want[:n], got[:n]) {
				t.Errorf("output mismatch: want=%08x got=%08x", want[:n], got[:n])
			}
		})
	}
}

func BenchmarkBlockDeltaInt32(b *testing.B) {
	benchmarkBlockDeltaInt32(b, blockDeltaInt32)
}

func benchmarkBlockDeltaInt32(b *testing.B, f func(*[blockSize]int32, int32) int32) {
	b.SetBytes(4 * blockSize)
	block := [blockSize]int32{}
	for b.Loop() {
		_ = f(&block, 0)
	}
}

func BenchmarkBlockMinInt32(b *testing.B) {
	benchmarkBlockMinInt32(b, blockMinInt32)
}

func benchmarkBlockMinInt32(b *testing.B, f func(*[blockSize]int32) int32) {
	b.SetBytes(4 * blockSize)
	block := [blockSize]int32{}
	for b.Loop() {
		_ = f(&block)
	}
}

func BenchmarkBlockSubInt32(b *testing.B) {
	benchmarkBlockSubInt32(b, blockSubInt32)
}

func benchmarkBlockSubInt32(b *testing.B, f func(*[blockSize]int32, int32)) {
	b.SetBytes(4 * blockSize)
	block := [blockSize]int32{}
	for b.Loop() {
		f(&block, 42)
	}
}

func BenchmarkBlockBitWidthsInt32(b *testing.B) {
	benchmarkBlockBitWidthsInt32(b, blockBitWidthsInt32)
}

func benchmarkBlockBitWidthsInt32(b *testing.B, f func(*[numMiniBlocks]byte, *[blockSize]int32)) {
	b.SetBytes(4 * blockSize)
	bitWidths := [numMiniBlocks]byte{}
	block := [blockSize]int32{}
	for b.Loop() {
		f(&bitWidths, &block)
	}
}

func BenchmarkEncodeMiniBlockInt32(b *testing.B) {
	benchmarkEncodeMiniBlockInt32(b, encodeMiniBlockInt32)
}

func benchmarkEncodeMiniBlockInt32(b *testing.B, f func([]byte, *[miniBlockSize]int32, uint)) {
	for bitWidth := uint(1); bitWidth <= 32; bitWidth++ {
		b.Run(fmt.Sprintf("bitWidth=%d", bitWidth), func(b *testing.B) {
			b.SetBytes(4 * miniBlockSize)
			dst := [4*miniBlockSize + 32]byte{}
			src := [miniBlockSize]int32{}
			for b.Loop() {
				f(dst[:], &src, bitWidth)
			}
		})
	}
}

func TestBlockDeltaInt64(t *testing.T) {
	testBlockDeltaInt64(t, blockDeltaInt64)
}

func testBlockDeltaInt64(t *testing.T, f func(*[blockSize]int64, int64) int64) {
	t.Helper()
	block := [blockSize]int64{}
	for i := range block {
		block[i] = int64(2 * (i + 1))
	}
	lastValue := f(&block, 0)
	if lastValue != 2*blockSize {
		t.Errorf("wrong last block value: want=%d got=%d", 2*blockSize, lastValue)
	}
	for i := range block {
		j := int64(2 * (i + 0))
		k := int64(2 * (i + 1))
		if block[i] != (k - j) {
			t.Errorf("wrong block delta at index %d: want=%d got=%d", i, k-j, block[i])
		}
	}
}

func TestBlockMinInt64(t *testing.T) {
	testBlockMinInt64(t, blockMinInt64)
}

func testBlockMinInt64(t *testing.T, f func(*[blockSize]int64) int64) {
	block := [blockSize]int64{}
	for i := range block {
		block[i] = blockSize - int64(i)
	}
	if min := f(&block); min != 1 {
		t.Errorf("wrong min block value: want=1 got=%d", min)
	}
}

func TestBlockSubInt64(t *testing.T) {
	testBlockSubInt64(t, blockSubInt64)
}

func testBlockSubInt64(t *testing.T, f func(*[blockSize]int64, int64)) {
	block := [blockSize]int64{}
	for i := range block {
		block[i] = int64(i)
	}
	f(&block, 1)
	for i := range block {
		if block[i] != int64(i-1) {
			t.Errorf("wrong block value at index %d: want=%d got=%d", i, i-1, block[i])
		}
	}
}

func TestBlockBitWidthsInt64(t *testing.T) {
	testBlockBitWidthsInt64(t, blockBitWidthsInt64)
}

func testBlockBitWidthsInt64(t *testing.T, f func(*[numMiniBlocks]byte, *[blockSize]int64)) {
	bitWidths := [numMiniBlocks]byte{}
	block := [blockSize]int64{}
	for i := range block {
		block[i] = int64(i)
	}
	f(&bitWidths, &block)

	want := [numMiniBlocks]byte{}
	for i := range want {
		j := (i + 0) * miniBlockSize
		k := (i + 1) * miniBlockSize
		want[i] = byte(maxLen64(block[j:k]))
	}

	if bitWidths != want {
		t.Errorf("wrong bit widths: want=%d got=%d", want, bitWidths)
	}
}

func TestEncodeMiniBlockInt64(t *testing.T) {
	testEncodeMiniBlockInt64(t, encodeMiniBlockInt64)
}

func testEncodeMiniBlockInt64(t *testing.T, f func([]byte, *[miniBlockSize]int64, uint)) {
	for bitWidth := uint(1); bitWidth <= 64; bitWidth++ {
		t.Run(fmt.Sprintf("bitWidth=%d", bitWidth), func(t *testing.T) {
			got := [8*miniBlockSize + 64]byte{}
			src := [miniBlockSize]int64{}
			for i := range src {
				src[i] = int64(i) & int64((1<<bitWidth)-1)
			}

			want := [8*miniBlockSize + 64]byte{}
			bitOffset := uint(0)

			for _, bits := range src {
				for b := uint(0); b < bitWidth; b++ {
					x := bitOffset / 8
					y := bitOffset % 8
					want[x] |= byte(((bits >> b) & 1) << y)
					bitOffset++
				}
			}

			f(got[:], &src, bitWidth)
			n := (miniBlockSize * bitWidth) / 8

			if !bytes.Equal(want[:n], got[:n]) {
				t.Errorf("output mismatch: want=%08x got=%08x", want[:n], got[:n])
			}
		})
	}
}

func BenchmarkBlockDeltaInt64(b *testing.B) {
	benchmarkBlockDeltaInt64(b, blockDeltaInt64)
}

func benchmarkBlockDeltaInt64(b *testing.B, f func(*[blockSize]int64, int64) int64) {
	b.SetBytes(8 * blockSize)
	block := [blockSize]int64{}
	for b.Loop() {
		_ = f(&block, 0)
	}
}

func BenchmarkBlockMinInt64(b *testing.B) {
	benchmarkBlockMinInt64(b, blockMinInt64)
}

func benchmarkBlockMinInt64(b *testing.B, f func(*[blockSize]int64) int64) {
	b.SetBytes(8 * blockSize)
	block := [blockSize]int64{}
	for b.Loop() {
		_ = f(&block)
	}
}

func BenchmarkBlockSubInt64(b *testing.B) {
	benchmarkBlockSubInt64(b, blockSubInt64)
}

func benchmarkBlockSubInt64(b *testing.B, f func(*[blockSize]int64, int64)) {
	b.SetBytes(8 * blockSize)
	block := [blockSize]int64{}
	for b.Loop() {
		f(&block, 42)
	}
}

func BenchmarkBlockBitWidthsInt64(b *testing.B) {
	benchmarkBlockBitWidthsInt64(b, blockBitWidthsInt64)
}

func benchmarkBlockBitWidthsInt64(b *testing.B, f func(*[numMiniBlocks]byte, *[blockSize]int64)) {
	b.SetBytes(8 * blockSize)
	bitWidths := [numMiniBlocks]byte{}
	block := [blockSize]int64{}
	for b.Loop() {
		f(&bitWidths, &block)
	}
}

func BenchmarkEncodeMiniBlockInt64(b *testing.B) {
	benchmarkEncodeMiniBlockInt64(b, encodeMiniBlockInt64)
}

func benchmarkEncodeMiniBlockInt64(b *testing.B, f func([]byte, *[miniBlockSize]int64, uint)) {
	for bitWidth := uint(1); bitWidth <= 64; bitWidth++ {
		b.Run(fmt.Sprintf("bitWidth=%d", bitWidth), func(b *testing.B) {
			b.SetBytes(8 * miniBlockSize)
			dst := [8*miniBlockSize + 64]byte{}
			src := [miniBlockSize]int64{}
			for b.Loop() {
				f(dst[:], &src, bitWidth)
			}
		})
	}
}
