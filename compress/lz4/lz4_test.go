package lz4_test

import (
	"bytes"
	"errors"
	"math/rand"
	"runtime"
	"strconv"
	"testing"

	"github.com/parquet-go/parquet-go/compress/lz4"
	pierrec "github.com/pierrec/lz4/v4"
)

func TestEncodeLevels(t *testing.T) {
	src := bytes.Repeat([]byte("parquet-go lz4 level test "), 4096)

	for _, test := range []struct {
		name  string
		level lz4.Level
		want  func(dst []byte) (int, error) // generate expected output at that compression level
	}{
		{"fastest", lz4.Fastest, func(dst []byte) (int, error) { return pierrec.CompressBlock(src, dst, nil) }},
		{"fast", lz4.Fast, func(dst []byte) (int, error) { return pierrec.CompressBlockHC(src, dst, lz4.Fast, nil, nil) }},
		{"level9", lz4.Level9, func(dst []byte) (int, error) { return pierrec.CompressBlockHC(src, dst, lz4.Level9, nil, nil) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			codec := &lz4.Codec{Level: test.level}
			got, err := codec.Encode(nil, src) // output from this library
			if err != nil {
				t.Fatal(err)
			}
			want := make([]byte, pierrec.CompressBlockBound(len(src)))
			n, err := test.want(want) // expected output
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, want[:n]) {
				t.Fatalf("level %d: output differs from the expected compressor (%d vs %d bytes)", test.level, len(got), n)
			}
			out, err := codec.Decode(nil, got)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(out, src) {
				t.Fatal("decoded data differs from the input")
			}
		})
	}
}

func TestDecodeUsesDst(t *testing.T) {
	// Random data compresses poorly, so 3*len(compressed) > len(dst).
	src := make([]byte, 64<<10)
	rand.New(rand.NewSource(0)).Read(src)

	codec := &lz4.Codec{Level: lz4.Level1}
	compressed, err := codec.Encode(nil, src)
	if err != nil {
		t.Fatal(err)
	}

	dst := make([]byte, len(src))
	out, err := codec.Decode(dst, compressed)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out, src) {
		t.Fatal("decoded data differs from the input")
	}
	if &out[0] != &dst[0] {
		t.Fatal("Decode reallocated a dst that was large enough")
	}
}

// denseBlock is the longest block of n bytes that pierrec/lz4 decodes.
// n must be at least 4. The decoded length is 255*n - 1001.
func denseBlock(n int) []byte {
	b := make([]byte, n)
	b[1] = 'A'
	b[2] = 1 // offset 1, little endian
	if n == 4 {
		b[0] = 0x1E // 1 literal, match length 18
		return b
	}
	b[0] = 0x1F // 1 literal, match length >= 19
	for i := 4; i < n-1; i++ {
		b[i] = 0xFF
	}
	b[n-1] = 254
	return b
}

func decodedLen(n int) int {
	if n < 2 {
		return 0
	}
	if n < 4 {
		return n - 1
	}
	return 255*n - 1001
}

func TestDecodeCorruptInput(t *testing.T) {
	codec := &lz4.Codec{}
	// A valid block, cut off at the last byte, and the same block with its
	// match offset flipped from 1 to 0. Offset 0 is invalid.
	valid := denseBlock(32)
	truncated := valid[:len(valid)-1]
	flipped := bytes.Clone(valid)
	flipped[2] ^= 0x01

	tests := []struct {
		name string
		src  []byte
	}{
		{name: "01_02_03_04", src: []byte{0x01, 0x02, 0x03, 0x04}},
		{name: "ff", src: []byte{0xff}},
		{name: "truncated", src: truncated},
		{name: "flipped", src: flipped},
		{name: "empty", src: nil},
		{name: "empty_slice", src: []byte{}},
	}
	dsts := []struct {
		name string
		dst  []byte
	}{
		{name: "nil", dst: nil},
		{name: "one_byte", dst: make([]byte, 1)},
	}

	for _, test := range tests {
		for _, dst := range dsts {
			t.Run(test.name+"/"+dst.name, func(t *testing.T) {
				buf := dst.dst
				var out []byte
				var decErr error
				alloc := allocated(func() {
					out, decErr = codec.Decode(buf, test.src)
				})
				if decErr == nil {
					t.Fatalf("Decode returned %d bytes, want error", len(out))
				}
				if !errors.Is(decErr, pierrec.ErrInvalidSourceShortBuffer) {
					t.Fatalf("error %v, want lz4.ErrInvalidSourceShortBuffer", decErr)
				}
				if out != nil {
					t.Fatalf("error returned a buffer of %d bytes", len(out))
				}
				// Doubling stops at the expansion limit, so the buffers
				// allocated along the way sum to less than twice that.
				limit := uint64(64 << 10)
				if n := len(test.src); n >= 4 {
					limit = uint64(2*(255*n-1001) + 64<<10)
				}
				if alloc > limit {
					t.Fatalf("allocated %d bytes, bound %d", alloc, limit)
				}
			})
		}
	}
}

func TestDecodeValidBuffers(t *testing.T) {
	codec := &lz4.Codec{}
	plain := bytes.Repeat([]byte("parquet-go lz4 page "), 128)
	compressed, err := codec.Encode(nil, plain)
	if err != nil {
		t.Fatal(err)
	}

	// Column.decompress sizes dst from the page's uncompressed size
	// (bufferPool.get → Resize), then Decode uses the full capacity.
	// A buffer of that size must decode on the first try.
	pageSized := make([]byte, len(plain))
	allocs := testing.AllocsPerRun(20, func() {
		out, err := codec.Decode(pageSized, compressed)
		if err != nil {
			t.Fatal(err)
		}
		if len(out) != len(plain) || &out[0] != &pageSized[0] || !bytes.Equal(out, plain) {
			t.Fatal("page-sized dst was not decoded in place")
		}
	})
	if allocs != 0 {
		t.Fatalf("page-sized dst allocated %g times", allocs)
	}

	// Pool buckets round the capacity up past the uncompressed size.
	pooled := make([]byte, len(plain), len(plain)+4096)
	out, err := codec.Decode(pooled, compressed)
	if err != nil {
		t.Fatal(err)
	}
	if &out[0] != &pooled[0] || !bytes.Equal(out, plain) {
		t.Fatal("dst with spare capacity was not decoded in place")
	}

	short := make([]byte, 1)
	out, err = codec.Decode(short, compressed)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out, plain) {
		t.Fatal("too-small dst decoded to the wrong bytes")
	}

	out, err = codec.Decode(nil, compressed)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out, plain) {
		t.Fatal("nil dst decoded to the wrong bytes")
	}

	// The empty block is one 0x00 token, which is what Encode emits for
	// empty input. A zero-length source is rejected by TestDecodeCorruptInput.
	encodedEmpty, err := codec.Encode(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encodedEmpty, []byte{0x00}) {
		t.Fatalf("Encode(empty) = %v, want [0x00]", encodedEmpty)
	}
	out, err = codec.Decode(nil, encodedEmpty)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 0 {
		t.Fatalf("empty block decoded to %d bytes", len(out))
	}
	out, err = codec.Decode(nil, []byte{0x10, 'A'})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out, []byte{'A'}) {
		t.Fatalf("short block decoded to %q", out)
	}
	out, err = codec.Decode(nil, []byte{0x20, 'A', 'B'})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out, []byte{'A', 'B'}) {
		t.Fatalf("short block decoded to %q", out)
	}
}

func TestDecodeExpansionLimit(t *testing.T) {
	codec := &lz4.Codec{}
	for _, n := range []int{4, 5, 6, 1000} {
		t.Run(strconv.Itoa(n), func(t *testing.T) {
			src := denseBlock(n)
			wantLen := 255*n - 1001
			want := bytes.Repeat([]byte{'A'}, wantLen)

			out, err := codec.Decode(nil, src)
			if err != nil {
				t.Fatal(err)
			}
			if n == 1000 && float64(len(out))/float64(len(src)) < 250 {
				t.Fatalf("expansion ratio %.3f, want a block near 255x", float64(len(out))/float64(len(src)))
			}
			if !bytes.Equal(out, want) {
				t.Fatalf("nil dst decoded %d bytes, want %d", len(out), wantLen)
			}

			short := make([]byte, 1)
			out, err = codec.Decode(short, src)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(out, want) {
				t.Fatalf("too-small dst decoded %d bytes, want %d", len(out), wantLen)
			}

			exact := make([]byte, wantLen)
			allocs := testing.AllocsPerRun(10, func() {
				out, err := codec.Decode(exact, src)
				if err != nil {
					t.Fatal(err)
				}
				if len(out) != wantLen || &out[0] != &exact[0] {
					t.Fatal("exact dst was not decoded in place")
				}
			})
			if allocs != 0 {
				t.Fatalf("exact dst allocated %g times", allocs)
			}
			if !bytes.Equal(exact[:wantLen], want) {
				t.Fatal("exact dst decoded to the wrong bytes")
			}

			// One byte under the decoded size still has to succeed, so the
			// limit is not short of a block the decoder accepts.
			under := make([]byte, wantLen-1)
			out, err = codec.Decode(under, src)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(out, want) {
				t.Fatalf("under-size dst decoded %d bytes, want %d", len(out), wantLen)
			}

			// A caller buffer past the limit is still used as-is.
			over := make([]byte, wantLen+64)
			out, err = codec.Decode(over, src)
			if err != nil {
				t.Fatal(err)
			}
			if &out[0] != &over[0] || !bytes.Equal(out, want) {
				t.Fatal("over-size dst was not decoded in place")
			}
		})
	}

	// Corrupt input into a buffer already at the limit must not grow it.
	corrupt := []byte{0x01, 0x02, 0x03, 0x04}
	full := make([]byte, decodedLen(len(corrupt)))
	allocs := testing.AllocsPerRun(20, func() {
		out, err := codec.Decode(full, corrupt)
		if err == nil || out != nil {
			t.Fatal("corrupt input at the expansion limit succeeded")
		}
	})
	if allocs != 0 {
		t.Fatalf("corrupt input at the expansion limit allocated %g times", allocs)
	}
}

func FuzzDecode(f *testing.F) {
	f.Add([]byte{0x01, 0x02, 0x03, 0x04})
	f.Add([]byte{0xff})
	f.Add([]byte{})
	f.Add([]byte{0x00})
	f.Add(denseBlock(8))
	f.Add(denseBlock(32))

	codec := &lz4.Codec{}
	plain := bytes.Repeat([]byte("fuzz-lz4"), 64)
	enc, err := codec.Encode(nil, plain)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(enc)

	f.Fuzz(func(t *testing.T, src []byte) {
		if len(src) > 1024 {
			src = src[:1024]
		}
		out, err := codec.Decode(nil, src)
		if err != nil {
			if out != nil {
				t.Fatalf("error returned a buffer of %d bytes", len(out))
			}
			return
		}
		if len(out) > decodedLen(len(src)) {
			t.Fatalf("decoded %d bytes from %d, bound %d", len(out), len(src), decodedLen(len(src)))
		}
		again, err := codec.Encode(nil, out)
		if err != nil {
			t.Fatal(err)
		}
		got, err := codec.Decode(nil, again)
		if err != nil {
			t.Fatalf("re-decode: %v", err)
		}
		if !bytes.Equal(got, out) {
			t.Fatalf("round trip changed %d bytes into %d", len(out), len(got))
		}
	})
}

func BenchmarkDecode(b *testing.B) {
	plain := bytes.Repeat([]byte("1234567890qwertyuiopasdfghjklzxcvbnm"), 8000)
	codec := &lz4.Codec{Level: lz4.DefaultLevel}
	compressed, err := codec.Encode(nil, plain)
	if err != nil {
		b.Fatal(err)
	}
	dst := make([]byte, len(plain))
	b.SetBytes(int64(len(plain)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := codec.Decode(dst, compressed)
		if err != nil {
			b.Fatal(err)
		}
		if len(out) != len(plain) {
			b.Fatalf("len %d", len(out))
		}
	}
}

func allocated(fn func()) uint64 {
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	fn()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}
