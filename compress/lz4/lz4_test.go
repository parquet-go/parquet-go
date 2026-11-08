package lz4_test

import (
	"bytes"
	"math/rand"
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
		{"fast", lz4.Fast, func(dst []byte) (int, error) { return pierrec.CompressBlock(src, dst, nil) }},
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
