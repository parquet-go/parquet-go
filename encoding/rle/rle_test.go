//go:build !s390x

// TODO: tests in this file are disabled for s390x because they break,
// we need to investigate.
//
// Note that we were not running these when we initially added s390x support,
// so no regression was introduced; either the tests assume little-endinaness,
// or there is a bug in the s390x implementation.

package rle

import (
	"testing"

	"github.com/parquet-go/parquet-go/encoding/fuzz"
	"github.com/parquet-go/parquet-go/internal/quick"
)

func FuzzEncodeBoolean(f *testing.F) {
	fuzz.EncodeBoolean(f, &Encoding{BitWidth: 1})
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
