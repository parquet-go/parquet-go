package delta

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"testing"
)

func TestLinearSearchPrefixLength(t *testing.T) {
	testSearchPrefixLength(t, linearSearchPrefixLength)
}

func TestWordSearchPrefixLength(t *testing.T) {
	testSearchPrefixLength(t, wordSearchPrefixLength)
}

func TestSearchPrefixLength(t *testing.T) {
	testSearchPrefixLength(t, searchPrefixLength)
}

func TestBinarySearchPrefixLength(t *testing.T) {
	testSearchPrefixLength(t, func(base, data []byte) int {
		return binarySearchPrefixLength(base, data)
	})
}

func testSearchPrefixLength(t *testing.T, prefixLength func(base, data []byte) int) {
	tests := []struct {
		base string
		data string
		len  int
	}{
		{
			base: "",
			data: "",
			len:  0,
		},

		{
			base: "A",
			data: "B",
			len:  0,
		},

		{
			base: "",
			data: "Hello World!",
			len:  0,
		},

		{
			base: "H",
			data: "Hello World!",
			len:  1,
		},

		{
			base: "He",
			data: "Hello World!",
			len:  2,
		},

		{
			base: "Hel",
			data: "Hello World!",
			len:  3,
		},

		{
			base: "Hell",
			data: "Hello World!",
			len:  4,
		},

		{
			base: "Hello",
			data: "Hello World!",
			len:  5,
		},

		{
			base: "Hello ",
			data: "Hello World!",
			len:  6,
		},

		{
			base: "Hello W",
			data: "Hello World!",
			len:  7,
		},

		{
			base: "Hello Wo",
			data: "Hello World!",
			len:  8,
		},

		{
			base: "Hello Wor",
			data: "Hello World!",
			len:  9,
		},

		{
			base: "Hello Worl",
			data: "Hello World!",
			len:  10,
		},

		{
			base: "Hello World",
			data: "Hello World!",
			len:  11,
		},

		{
			base: "Hello World!",
			data: "Hello World!",
			len:  12,
		},

		{
			base: "Hell.......",
			data: "Hello World!",
			len:  4,
		},
	}

	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			n := prefixLength([]byte(test.base), []byte(test.data))
			if n != test.len {
				t.Errorf("prefixLength(%q,%q): want=%d got=%d", test.base, test.data, test.len, n)
			}
		})
	}
}

func TestEstimateDecodeByteArraySize(t *testing.T) {
	e := &ByteArrayEncoding{}

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

	// A length list that does not decode, or that decodes to lengths the
	// byte array format rejects, must not produce a negative or huge size.
	for _, src := range [][]byte{
		nil,
		{0xff, 0xff, 0xff, 0xff, 0xff},
		append(encodeInt32(nil, []int32{-1}), encodeInt32(nil, []int32{0})...),
		append(encodeInt32(nil, []int32{5}), encodeInt32(nil, []int32{0})...),
		append(encodeInt32(nil, []int32{0, 0}), encodeInt32(nil, []int32{1})...),
		append(encodeInt32(nil, []int32{0}), encodeInt32(nil, []int32{math.MaxInt32})...),
	} {
		if got := e.EstimateDecodeByteArraySize(src); got != 0 {
			t.Fatalf("corrupt page %x: estimate = %d, want 0", src, got)
		}
	}

	// Suffix lengths that fit in the whole page, but not in the payload left
	// after the length lists. Prefix repetition would otherwise size a buffer
	// much larger than decode will accept.
	prefix := encodeInt32(nil, []int32{0, 1, 1, 1})
	suffix := encodeInt32(nil, []int32{1, 0, 0, 0})
	page := append(prefix, suffix...)
	if len(page) < 1 {
		t.Fatal("encoded length lists are empty")
	}
	if got := e.EstimateDecodeByteArraySize(page); got != 0 {
		t.Fatalf("suffix past payload: estimate = %d, page = %d, want 0", got, len(page))
	}
}

func TestReconstructedByteArraySize(t *testing.T) {
	// Three copies of a MaxInt32-byte value. The int32 sum used to wrap.
	prefix := []int32{0, math.MaxInt32, math.MaxInt32}
	suffix := []int32{math.MaxInt32, 0, 0}
	got := reconstructedByteArraySize(prefix, suffix, math.MaxInt32)
	want := int64(math.MaxInt32) * 3
	if want > int64(math.MaxInt) {
		if got != 0 {
			t.Fatalf("sum that does not fit in int: got %d, want 0", got)
		}
		return
	}
	if int64(got) != want {
		t.Fatalf("got %d, want %d", got, want)
	}

	if got := reconstructedByteArraySize([]int32{1}, []int32{0}, 0); got != 0 {
		t.Fatalf("prefix past the previous value: got %d, want 0", got)
	}
}

func BenchmarkLinearSearchPrefixLength(b *testing.B) {
	benchmarkSearchPrefixLength(b, linearSearchPrefixLength)
}

func BenchmarkWordSearchPrefixLength(b *testing.B) {
	benchmarkSearchPrefixLength(b, wordSearchPrefixLength)
}

func BenchmarkSearchPrefixLength(b *testing.B) {
	benchmarkSearchPrefixLength(b, searchPrefixLength)
}

func BenchmarkBinarySearchPrefixLength(b *testing.B) {
	benchmarkSearchPrefixLength(b, func(base, data []byte) int {
		return binarySearchPrefixLength(base, data)
	})
}

func benchmarkSearchPrefixLength(b *testing.B, prefixLength func(base, data []byte) int) {
	buffer := bytes.Repeat([]byte("0123456789"), 100)

	for _, size := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			base := buffer[:size]
			data := buffer[:size/2]

			for b.Loop() {
				_ = prefixLength(base, data)
			}
		})
	}
}

func TestEncodeByteArraySuffixes(t *testing.T) {
	// Mix short and long suffixes, repeated/empty values, and a nonzero
	// starting offset. The vector loop must resume scalar copying at the
	// correct source and destination positions.
	sizes := [...]int{1, 31, 32, 33, 64, 65, 96, 97, 128, 129}
	src := []byte("unused")
	offsets := []uint32{uint32(len(src))}
	var prefix, suffix []int32
	var payload, last []byte
	for i := range 128 {
		value := append(bytes.Repeat([]byte{'p'}, 31), bytes.Repeat([]byte{byte(i)}, sizes[i%len(sizes)])...)
		if i%7 == 0 {
			value = last
		}
		if i%11 == 0 {
			value = nil
		}
		p := linearSearchPrefixLength(last, value)
		prefix = append(prefix, int32(p))
		suffix = append(suffix, int32(len(value)-p))
		payload = append(payload, value[p:]...)
		src = append(src, value...)
		offsets = append(offsets, uint32(len(src)))
		last = value
	}
	src = append(src, []byte("unused trailing data")...)
	want := encodeInt32(nil, prefix)
	want = encodeInt32(want, suffix)
	want = append(want, payload...)
	e := new(ByteArrayEncoding)
	var dst []byte
	for range 2 {
		var err error
		dst, err = e.EncodeByteArray(dst, src, offsets)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(dst, want) {
			t.Fatal("encoded bytes differ from scalar suffix concatenation")
		}
	}
}

func TestEncodeByteArrayOverlappingBuffers(t *testing.T) {
	const count = 4096
	prefix := make([]int32, count)
	suffix := make([]int32, count)
	for i := range suffix {
		suffix[i] = 1
	}
	header := encodeInt32(nil, prefix)
	header = encodeInt32(header, suffix)
	start := len(header) + 1
	backing := make([]byte, start+count+1024)
	for i := range backing {
		backing[i] = byte(i)
	}
	offsets := make([]uint32, count+1)
	for i := range offsets {
		offsets[i] = uint32(start + i)
	}

	// Encoding does not promise in-place operation. If the buffers overlap,
	// preserve the existing header writes, zeroing, and per-value Go copies.
	scalar := bytes.Clone(backing)
	want := encodeInt32(scalar[:0], prefix)
	want = encodeInt32(want, suffix)
	want = resize(want, len(want)+count)
	for i := range count {
		want[len(header)+i] = scalar[start+i]
	}
	got, err := new(ByteArrayEncoding).EncodeByteArray(backing[:0], backing[:start+count], offsets)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("overlapping buffers differ from scalar suffix copying")
	}
}
