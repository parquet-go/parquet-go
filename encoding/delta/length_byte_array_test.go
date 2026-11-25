package delta

import (
	"bytes"
	"math"
	"math/rand"
	"slices"
	"testing"
)

func TestDecodeByteArrayLengths(t *testing.T) {
	lengths := make([]int32, 999)
	offsets := make([]uint32, len(lengths)+1)

	totalLength := uint32(0)
	for i := range lengths {
		lengths[i] = int32(i)
		totalLength += uint32(i)
	}

	lastOffset, invalidLength := decodeByteArrayLengths(offsets, lengths)
	if invalidLength != 0 {
		t.Fatal("wrong invalid length:", invalidLength)
	}
	if lastOffset != totalLength {
		t.Fatalf("wrong last offset: want=%d got=%d", lastOffset, totalLength)
	}

	expectOffset := uint32(0)
	for i, offset := range offsets[:len(lengths)] {
		if offset != expectOffset {
			t.Fatalf("wrong offset at index %d: want=%d got=%d", i, expectOffset, offset)
		}
		expectOffset += uint32(lengths[i])
	}

	if offsets[len(lengths)] != lastOffset {
		t.Fatalf("wrong last offset: want=%d got=%d", lastOffset, offsets[len(lengths)])
	}
}

func TestEstimateDecodeLengthByteArraySize(t *testing.T) {
	e := &LengthByteArrayEncoding{}
	values, offsets := generatePrefixHeavyByteArrays(1000, rand.New(rand.NewSource(1)))
	encoded, err := e.EncodeByteArray(nil, values, offsets)
	if err != nil {
		t.Fatal(err)
	}
	decoded, _, err := e.DecodeByteArray(nil, encoded, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := e.EstimateDecodeByteArraySize(encoded); got != len(decoded) {
		t.Fatalf("valid page: estimate = %d, decoded = %d", got, len(decoded))
	}

	for _, src := range [][]byte{
		nil,
		{0xff, 0xff, 0xff, 0xff, 0xff},
		encodeInt32(nil, []int32{-1}),
		encodeInt32(nil, []int32{math.MaxInt32, math.MaxInt32}),
	} {
		if got := e.EstimateDecodeByteArraySize(src); got != 0 {
			t.Fatalf("corrupt page %x: estimate = %d, want 0", src, got)
		}
	}

	// One claimed byte and no payload. The length fits in the encoded page
	// and not in the bytes that follow the length list.
	page := encodeInt32(nil, []int32{1})
	if got := e.EstimateDecodeByteArraySize(page); got != 0 {
		t.Fatalf("length past payload: estimate = %d, page = %d, want 0", got, len(page))
	}
}

func BenchmarkEncodeByteArrayLengths(b *testing.B) {
	offsets := make([]uint32, 1001)
	lengths := make([]int32, 1000)
	for i := range offsets {
		offsets[i] = uint32(i * 3)
	}
	b.SetBytes(int64(len(lengths) * 4))
	for b.Loop() {
		encodeByteArrayLengths(lengths, offsets)
	}
}

func BenchmarkEstimateDecodeLengthByteArraySize(b *testing.B) {
	e := &LengthByteArrayEncoding{}
	values, offsets := generatePrefixHeavyByteArrays(10000, rand.New(rand.NewSource(1)))
	encoded, err := e.EncodeByteArray(nil, values, offsets)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(values)))
	for b.Loop() {
		_ = e.EstimateDecodeByteArraySize(encoded)
	}
}

func BenchmarkDecodeByteArrayLengths(b *testing.B) {
	offsets := make([]uint32, 1001)
	lengths := make([]int32, 1000)
	for i := range lengths {
		lengths[i] = int32(i % 100)
	}
	b.SetBytes(int64(len(lengths) * 4))
	for b.Loop() {
		decodeByteArrayLengths(offsets, lengths)
	}
}

func TestLengthByteArrayDecodeBuffers(t *testing.T) {
	for _, mode := range []string{"in-place", "out-of-place", "zero-capacity"} {
		t.Run(mode, func(t *testing.T) {
			e := &LengthByteArrayEncoding{}
			want := []byte("abc")
			wantOffsets := []uint32{0, 1, 1, 3}
			src, err := e.EncodeByteArray(nil, want, wantOffsets)
			if err != nil {
				t.Fatal(err)
			}
			end := len(src)
			src = append(src, "tail"...)
			var dst []byte
			switch mode {
			case "in-place":
				dst = src
			case "out-of-place":
				dst = make([]byte, 10)
			case "zero-capacity":
				dst = src[:0:0]
			}
			got, offsets, err := e.DecodeByteArray(dst, src, nil)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, want) || !slices.Equal(offsets, wantOffsets) {
				t.Fatalf("got %q, %v; want %q, %v", got, offsets, want, wantOffsets)
			}
			switch mode {
			case "in-place":
				if cap(got) != len(got) {
					t.Fatalf("decoded capacity = %d, want %d", cap(got), len(got))
				}
				_ = append(got, '!')
				if string(src[end:]) != "tail" {
					t.Fatalf("append overwrote trailing bytes: %q", src[end:])
				}
			case "out-of-place", "zero-capacity":
				clear(src)
				if !bytes.Equal(got, want) {
					t.Fatalf("source mutation changed decoded bytes: %q", got)
				}
			}
		})
	}
}

func TestLengthByteArrayDecodeEmptyBufferReuse(t *testing.T) {
	e := &LengthByteArrayEncoding{}
	empty, err := e.EncodeByteArray(nil, nil, []uint32{0, 0})
	if err != nil {
		t.Fatal(err)
	}
	one, err := e.EncodeByteArray(nil, []byte("x"), []uint32{0, 1})
	if err != nil {
		t.Fatal(err)
	}
	dst, offsets, err := e.DecodeByteArray(empty[:0], empty, nil)
	if err != nil {
		t.Fatal(err)
	}
	dst, _, err = e.DecodeByteArray(dst, one, offsets)
	if err != nil {
		t.Fatal(err)
	}
	if string(dst) != "x" || &dst[0] != &empty[0] {
		t.Fatalf("empty decode lost reusable destination buffer: %q", dst)
	}
}
