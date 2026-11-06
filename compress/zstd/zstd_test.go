package zstd_test

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/parquet-go/parquet-go/compress/zstd"
)

func TestWindowSize(t *testing.T) {
	src := make([]byte, 1<<20)
	prng := rand.New(rand.NewSource(0))
	prng.Read(src[:64<<10])
	for i := 64 << 10; i < len(src); i++ {
		src[i] = src[i%(64<<10)]
	}

	for _, size := range []int{0, 1 << 10, 1 << 16, 1 << 20, 1 << 23} {
		codec := &zstd.Codec{WindowSize: size}
		enc, err := codec.Encode(nil, src)
		if err != nil {
			t.Fatalf("window size %d: encode: %v", size, err)
		}
		dec, err := codec.Decode(nil, enc)
		if err != nil {
			t.Fatalf("window size %d: decode: %v", size, err)
		}
		if !bytes.Equal(dec, src) {
			t.Fatalf("window size %d: round trip mismatch", size)
		}
	}
}
