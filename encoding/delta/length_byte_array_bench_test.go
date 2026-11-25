package delta

import "testing"

// Benchmarks matching the #794 measurements: one value into a separate buffer
// with spare capacity, and one three-byte value into a destination of capacity 1.
// BenchmarkDeltaLengthDecodeAliased restores the encoded page on every
// iteration so the loop stays valid after the payload is moved to dst[0].

func BenchmarkDeltaLengthDecodeSeparate(b *testing.B) {
	e := &LengthByteArrayEncoding{}
	src, err := e.EncodeByteArray(nil, []byte{'x'}, []uint32{0, 1})
	if err != nil {
		b.Fatal(err)
	}
	dst := make([]byte, 0, 32)
	offsets := make([]uint32, 0, 4)
	b.ResetTimer()
	for b.Loop() {
		dst, offsets, err = e.DecodeByteArray(dst, src, offsets)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDeltaLengthDecodeShortCap(b *testing.B) {
	e := &LengthByteArrayEncoding{}
	src, err := e.EncodeByteArray(nil, []byte("abc"), []uint32{0, 3})
	if err != nil {
		b.Fatal(err)
	}
	hold := make([]byte, 0, 1)
	offsets := make([]uint32, 0, 4)
	b.ResetTimer()
	for b.Loop() {
		_, offsets, err = e.DecodeByteArray(hold[:0:1], src, offsets)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDeltaLengthDecodeAliased(b *testing.B) {
	e := &LengthByteArrayEncoding{}
	const n = 4000
	values := make([]byte, 0, n*40)
	offsets := make([]uint32, 0, n+1)
	token := []byte("https://example.com/api/v1/users/00000000")
	for range n {
		offsets = append(offsets, uint32(len(values)))
		values = append(values, token...)
	}
	offsets = append(offsets, uint32(len(values)))
	encoded, err := e.EncodeByteArray(nil, values, offsets)
	if err != nil {
		b.Fatal(err)
	}
	buf := make([]byte, len(encoded))
	dstOffsets := make([]uint32, 0, len(offsets))
	b.SetBytes(int64(len(values)))
	b.ResetTimer()
	for b.Loop() {
		copy(buf, encoded)
		_, dstOffsets, err = e.DecodeByteArray(buf, buf, dstOffsets)
		if err != nil {
			b.Fatal(err)
		}
	}
}
