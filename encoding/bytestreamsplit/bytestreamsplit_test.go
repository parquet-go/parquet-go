package bytestreamsplit_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/parquet-go/parquet-go/encoding/bytestreamsplit"
	"github.com/parquet-go/parquet-go/encoding/fuzz"
	"github.com/parquet-go/parquet-go/encoding/test"
)

func FuzzEncodeFloat(f *testing.F) {
	fuzz.EncodeFloat(f, new(bytestreamsplit.Encoding))
}

func FuzzEncodeDouble(f *testing.F) {
	fuzz.EncodeDouble(f, new(bytestreamsplit.Encoding))
}

func FuzzEncodeInt32(f *testing.F) {
	fuzz.EncodeInt32(f, new(bytestreamsplit.Encoding))
}

func FuzzEncodeInt64(f *testing.F) {
	fuzz.EncodeInt64(f, new(bytestreamsplit.Encoding))
}

func TestEncodeFloat(t *testing.T) {
	test.EncodeFloat(t, new(bytestreamsplit.Encoding), 0, 100)
}

func TestEncodeDouble(t *testing.T) {
	test.EncodeDouble(t, new(bytestreamsplit.Encoding), 0, 100)
}

func TestEncodeInt32(t *testing.T) {
	test.EncodeInt32(t, new(bytestreamsplit.Encoding), 0, 100, 32)
}

func TestEncodeInt64(t *testing.T) {
	test.EncodeInt64(t, new(bytestreamsplit.Encoding), 0, 100, 64)
}

func TestEncodeFixedLenByteArray(t *testing.T) {
	enc := new(bytestreamsplit.Encoding)
	for _, size := range []int{1, 4, 8, 16} {
		for n := 0; n <= 25; n++ {
			t.Run(fmt.Sprintf("size=%d/N=%d", size, n), func(t *testing.T) {
				src := make([]byte, n*size)
				for i := range src {
					src[i] = byte(i)
				}

				buf, err := enc.EncodeFixedLenByteArray(nil, src, size)
				if err != nil {
					t.Fatalf("encoding %d values of size %d: %v", n, size, err)
				}

				got, err := enc.DecodeFixedLenByteArray(nil, buf, size)
				if err != nil {
					t.Fatalf("decoding %d values of size %d: %v", n, size, err)
				}

				if !bytes.Equal(src, got) {
					t.Fatalf("roundtrip mismatch for size=%d N=%d:\n  src=%v\n  got=%v", size, n, src, got)
				}
			})
		}
	}
}
