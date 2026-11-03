package gzip_test

import (
	"bytes"
	"testing"

	"github.com/parquet-go/parquet-go/compress/gzip"
)

func TestRoundTrip(t *testing.T) {
	codec := new(gzip.Codec)
	tests := []struct {
		name       string
		inputBytes []byte
	}{
		{name: "empty", inputBytes: nil},
		{name: "short", inputBytes: []byte("hello gzip")},
		{name: "repetitive", inputBytes: bytes.Repeat([]byte("compressible data "), 1024)},
	}

	for _, test := range tests {
		t.Run(test.name, func(*testing.T) {
			compressed, err := codec.Encode(nil, test.inputBytes)
			if err != nil {
				t.Fatal(err)
			}
			out, err := codec.Decode(nil, compressed)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(test.inputBytes, out) {
				t.Fatal("decoded data differs from the input")
			}
		})
	}
}
