//go:build !s390x

// TODO: tests in this file are disabled for s390x because they break,
// we need to investigate.
//
// Note that we were not running these when we initially added s390x support,
// so no regression was introduced; either the tests assume little-endinaness,
// or there is a bug in the s390x implementation.

package rle

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/parquet-go/parquet-go/encoding/fuzz"
	"github.com/parquet-go/parquet-go/internal/quick"
)

func FuzzEncodeBoolean(f *testing.F) {
	fuzz.EncodeBoolean(f, &Encoding{BitWidth: 1})
}

func TestBooleanRunValueIsZeroOrOne(t *testing.T) {
	enc := &Encoding{BitWidth: 1}
	got, err := enc.EncodeBoolean(nil, []byte{0xFF, 0xFF, 0xFF})
	if err != nil {
		t.Fatal(err)
	}
	if got[len(got)-1] != 1 {
		t.Fatalf("run value = %#x, want 1", got[len(got)-1])
	}
	back, err := enc.DecodeBoolean(nil, got)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(back, []byte{0xFF, 0xFF, 0xFF}) {
		t.Fatalf("round trip = %x", back)
	}

	zeros, err := enc.EncodeBoolean(nil, []byte{0x00, 0x00})
	if err != nil {
		t.Fatal(err)
	}
	if zeros[len(zeros)-1] != 0 {
		t.Fatalf("zero run value = %#x, want 0", zeros[len(zeros)-1])
	}
}

func TestDecodeBooleanRefusesARunWithoutItsValue(t *testing.T) {
	enc := &Encoding{BitWidth: 1}
	payload := appendUvarint(nil, uint64(8)<<1)
	_, err := enc.DecodeBoolean(nil, booleanPayload(payload))
	if err == nil {
		t.Fatal("a run without its value byte decoded without an error")
	}
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("err = %v", err)
	}
}

func TestDecodeBooleanSpecRuns(t *testing.T) {
	enc := &Encoding{BitWidth: 1}

	// 1001 ones, then 7 zeros. The second run starts one bit into the last byte.
	payload := appendUvarint(nil, uint64(1001)<<1)
	payload = append(payload, 1)
	payload = appendUvarint(payload, uint64(7)<<1)
	payload = append(payload, 0)
	got, err := enc.DecodeBoolean(nil, booleanPayload(payload))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 126 {
		t.Fatalf("len = %d, want 126", len(got))
	}
	for i := 0; i < 1001; i++ {
		if !booleanBit(got, i) {
			t.Fatalf("bit %d clear, want set", i)
		}
	}
	for i := 1001; i < 1008; i++ {
		if booleanBit(got, i) {
			t.Fatalf("bit %d set, want clear", i)
		}
	}

	// Value 1 is the spec. Value 0xFF is what this encoder used to write.
	for _, value := range []byte{1, 0xFF} {
		payload = appendUvarint(nil, uint64(24)<<1)
		payload = append(payload, value)
		got, err = enc.DecodeBoolean(nil, booleanPayload(payload))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, []byte{0xFF, 0xFF, 0xFF}) {
			t.Fatalf("value %#x decoded to %x", value, got)
		}
	}

	// One true, then eight packed values whose first is true.
	payload = appendUvarint(nil, uint64(1)<<1)
	payload = append(payload, 1)
	payload = appendUvarint(payload, uint64(1)<<1|1)
	payload = append(payload, 0x01)
	got, err = enc.DecodeBoolean(nil, booleanPayload(payload))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte{0x03, 0x00}) {
		t.Fatalf("shifted pack = %x, want 0300", got)
	}
}

func TestBooleanRoundTrip(t *testing.T) {
	enc := &Encoding{BitWidth: 1}
	src := []byte{0xFF, 0x00, 0x0F, 0xF0, 0xFF, 0xFF, 0x01, 0x00, 0x00}
	got, err := enc.EncodeBoolean(nil, src)
	if err != nil {
		t.Fatal(err)
	}
	back, err := enc.DecodeBoolean(nil, got)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(back, src) {
		t.Fatalf("got %x want %x", back, src)
	}
}

func booleanPayload(payload []byte) []byte {
	src := make([]byte, 4+len(payload))
	binary.LittleEndian.PutUint32(src, uint32(len(payload)))
	copy(src[4:], payload)
	return src
}

func booleanBit(buf []byte, i int) bool {
	return buf[i/8]&(1<<uint(i%8)) != 0
}

func FuzzEncodeLevels(f *testing.F) {
	fuzz.EncodeLevels(f, &Encoding{BitWidth: 8})
}

func FuzzEncodeInt32(f *testing.F) {
	fuzz.EncodeInt32(f, &Encoding{BitWidth: 32})
}

func TestEncodeInt32IndexEqual8Contiguous(t *testing.T) {
	testEncodeInt32IndexEqual8Contiguous(t, encodeInt32IndexEqual8Contiguous)
}

func testEncodeInt32IndexEqual8Contiguous(t *testing.T, f func([][8]int32) int) {
	t.Helper()

	err := quick.Check(func(words [][8]int32) bool {
		want := 0

		for want < len(words) && words[want] != broadcast8x4(words[want][0]) {
			want++
		}

		if got := f(words); got != want {
			t.Errorf("want=%d got=%d", want, got)
			return false
		}

		return true
	})
	if err != nil {
		t.Error(err)
	}
}

func BenchmarkEncodeInt32IndexEqual8Contiguous(b *testing.B) {
	benchmarkEncodeInt32IndexEqual8Contiguous(b, encodeInt32IndexEqual8Contiguous)
}

func benchmarkEncodeInt32IndexEqual8Contiguous(b *testing.B, f func([][8]int32) int) {
	words := make([][8]int32, 1000)
	for i := range words {
		words[i][0] = 1
	}
	for b.Loop() {
		_ = f(words)
	}
	b.SetBytes(32 * int64(len(words)))
}

func TestDecodeInt32LargeRun(t *testing.T) {
	enc := &Encoding{BitWidth: 1}
	count := 16_777_217 // one more than old maxSupportedValueCount
	src := make([]int32, count)
	for i := range src {
		src[i] = 1
	}
	encoded, err := enc.EncodeInt32(nil, src)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := enc.DecodeInt32(make([]int32, count), encoded)
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded) != count {
		t.Fatalf("expected %d values, got %d", count, len(decoded))
	}
	for i, v := range decoded {
		if v != 1 {
			t.Fatalf("value at index %d: expected 1, got %d", i, v)
		}
	}
}

func BenchmarkEncodeInt32Bitpack(b *testing.B) {
	words := make([][8]int32, 128)
	for i := range words {
		for j := range words[i] {
			words[i][j] = int32(i+j) & 0x7FFF
		}
	}
	dst := make([]byte, len(words)*16+32)
	for _, bitWidth := range []uint{1, 2, 5, 8, 15} {
		b.Run(fmt.Sprintf("bitWidth=%d", bitWidth), func(b *testing.B) {
			b.SetBytes(int64(len(words) * 32))
			for b.Loop() {
				encodeInt32Bitpack(dst, words, bitWidth)
			}
		})
	}
}

func BenchmarkEncodeBytesBitpack(b *testing.B) {
	src := make([]uint64, 128)
	for i := range src {
		src[i] = uint64(i) * 0x0101010101010101
	}
	dst := make([]byte, len(src)*8+8)
	for _, bitWidth := range []uint{1, 2, 3, 5, 8} {
		b.Run(fmt.Sprintf("bitWidth=%d", bitWidth), func(b *testing.B) {
			b.SetBytes(int64(len(src) * 8))
			for b.Loop() {
				encodeBytesBitpack(dst, src, bitWidth)
			}
		})
	}
}

func BenchmarkDecodeBytesBitpack(b *testing.B) {
	words := 128
	src := make([]uint64, words)
	for i := range src {
		src[i] = uint64(i) * 0x0101010101010101
	}
	buf := make([]byte, words*8+8)
	dst := make([]byte, words*8)
	for _, bitWidth := range []uint{1, 2, 3, 5, 8} {
		n := encodeBytesBitpack(buf, src, bitWidth)
		b.Run(fmt.Sprintf("bitWidth=%d", bitWidth), func(b *testing.B) {
			b.SetBytes(int64(words * 8))
			for b.Loop() {
				decodeBytesBitpack(dst, buf[:n], uint(words*8), bitWidth)
			}
		})
	}
}
