package lz4_test

import (
	"bytes"
	"errors"
	"math/rand"
	"runtime"
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
	if n < 4 {
		if n < 2 {
			return 0
		}
		return n - 1
	}
	return 255*n - 1001
}

func TestDecode(t *testing.T) {
	codec := &lz4.Codec{}
	block := denseBlock(32)
	truncated := block[:len(block)-1]
	flipped := bytes.Clone(block)
	flipped[2] ^= 0x01 // offset 1 -> 0, which the decoder rejects

	plain := bytes.Repeat([]byte("parquet-go lz4 page "), 32)
	compressed, err := codec.Encode(nil, plain)
	if err != nil {
		t.Fatal(err)
	}
	pageDst := make([]byte, len(plain))

	tests := []struct {
		name    string
		src     []byte
		dst     []byte
		want    []byte
		wantErr bool
	}{
		{name: "01 02 03 04", src: []byte{0x01, 0x02, 0x03, 0x04}, wantErr: true},
		{name: "ff", src: []byte{0xff}, wantErr: true},
		{name: "truncated", src: truncated, wantErr: true},
		{name: "flipped", src: flipped, wantErr: true},
		{name: "empty", src: nil, want: []byte{}},
		{name: "empty slice", src: []byte{}, want: []byte{}},
		{name: "nil dst", src: compressed, want: plain},
		{name: "tiny dst", src: compressed, dst: []byte{0}, want: plain},
		{name: "exact dst", src: compressed, dst: pageDst, want: plain},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var out []byte
			var decErr error
			alloc := allocated(func() {
				out, decErr = codec.Decode(test.dst, test.src)
			})
			if test.wantErr {
				if !errors.Is(decErr, pierrec.ErrInvalidSourceShortBuffer) {
					t.Fatalf("got %v, want ErrInvalidSourceShortBuffer", decErr)
				}
				if out != nil {
					t.Fatalf("error returned %d bytes", len(out))
				}
				// Growth stops at the expansion limit, so the buffers along
				// the way sum to less than twice that.
				limit := uint64(64 << 10)
				if n := len(test.src); n >= 4 {
					limit = uint64(2*(255*n-1001) + 64<<10)
				}
				if alloc > limit {
					t.Fatalf("allocated %d bytes, bound %d", alloc, limit)
				}
				return
			}
			if decErr != nil {
				t.Fatal(decErr)
			}
			if !bytes.Equal(out, test.want) {
				t.Fatalf("got %d bytes, want %d", len(out), len(test.want))
			}
			if len(test.dst) > 0 && len(test.dst) == len(test.want) && &out[0] != &test.dst[0] {
				t.Fatal("reallocated an exact dst")
			}
		})
	}

	// A non-nil dst is returned as dst[:0]. Callers see the same backing array.
	buf := make([]byte, 8)
	out, err := codec.Decode(buf, nil)
	if err != nil || len(out) != 0 || &out[:1][0] != &buf[0] {
		t.Fatalf("empty src into dst: len=%d cap=%d err=%v", len(out), cap(out), err)
	}
}

func TestDecodeExpansionLimit(t *testing.T) {
	codec := &lz4.Codec{}
	// 1000 input bytes expand 253.999x, next to the 255x asymptote.
	const n = 1000
	src := denseBlock(n)
	want := bytes.Repeat([]byte{'A'}, 255*n-1001)

	for _, dst := range [][]byte{nil, {0}, make([]byte, len(want))} {
		out, err := codec.Decode(dst, src)
		if err != nil {
			t.Fatal(err)
		}
		if float64(len(out))/float64(n) < 250 {
			t.Fatalf("expansion ratio %.3f, want a block near 255x", float64(len(out))/float64(n))
		}
		if !bytes.Equal(out, want) {
			t.Fatalf("got %d bytes, want %d", len(out), len(want))
		}
		if len(dst) == len(want) && &out[0] != &dst[0] {
			t.Fatal("reallocated an exact dst")
		}
	}

	exact := make([]byte, len(want))
	allocs := testing.AllocsPerRun(10, func() {
		out, err := codec.Decode(exact, src)
		if err != nil || len(out) != len(want) || &out[0] != &exact[0] {
			t.Fatalf("exact dst len=%d err=%v", len(out), err)
		}
	})
	if allocs != 0 {
		t.Fatalf("exact dst allocated %g times", allocs)
	}

	// One byte under the decoded size still has to succeed.
	out, err := codec.Decode(make([]byte, len(want)-1), src)
	if err != nil || !bytes.Equal(out, want) {
		t.Fatalf("under-size dst: len=%d err=%v", len(out), err)
	}

	// A caller buffer past the limit is still used as-is.
	over := make([]byte, len(want)+64)
	out, err = codec.Decode(over, src)
	if err != nil || &out[0] != &over[0] || !bytes.Equal(out, want) {
		t.Fatal("over-size dst was not decoded in place")
	}

	// Corrupt input into a buffer already at the limit must not grow it.
	corrupt := []byte{0x01, 0x02, 0x03, 0x04}
	full := make([]byte, 255*len(corrupt)-1001)
	allocs = testing.AllocsPerRun(20, func() {
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

	codec := &lz4.Codec{}
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
