package lz4_test

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/parquet-go/parquet-go/compress/lz4"
)

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
