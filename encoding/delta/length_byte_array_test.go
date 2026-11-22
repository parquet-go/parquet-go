package delta

import (
	"math"
	"math/rand"
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
