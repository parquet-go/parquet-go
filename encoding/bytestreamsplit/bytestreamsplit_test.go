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

// TestEncodeFixedLenByteArrayInvalidSize verifies that size<=0 and size>max
// are rejected with an error rather than panicking (P2 from code review).
func TestEncodeFixedLenByteArrayInvalidSize(t *testing.T) {
	enc := new(bytestreamsplit.Encoding)
	src := make([]byte, 8)

	for _, size := range []int{-1, 0} {
		if _, err := enc.EncodeFixedLenByteArray(nil, src, size); err == nil {
			t.Errorf("EncodeFixedLenByteArray with size=%d: expected error, got nil", size)
		}
		if _, err := enc.DecodeFixedLenByteArray(nil, src, size); err == nil {
			t.Errorf("DecodeFixedLenByteArray with size=%d: expected error, got nil", size)
		}
	}
}

// TestEncodeFixedLenByteArrayStreamLayout verifies the exact encoded byte layout
// required by the Parquet BYTE_STREAM_SPLIT spec: stream s occupies buf[s*n:(s+1)*n]
// and contains byte s of each element in order.
// This also confirms endian-neutral treatment of 4- and 8-byte arrays: the bytes
// of each element must be preserved as-is, not reinterpreted as float/double.
func TestEncodeFixedLenByteArrayStreamLayout(t *testing.T) {
	enc := new(bytestreamsplit.Encoding)

	for _, size := range []int{4, 8} {
		t.Run(fmt.Sprintf("size=%d", size), func(t *testing.T) {
			// Two elements with distinct, non-zero bytes in every position.
			const n = 2
			src := make([]byte, n*size)
			for i := range src {
				src[i] = byte(i + 1)
			}

			buf, err := enc.EncodeFixedLenByteArray(nil, src, size)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}

			// Per spec: stream s is at buf[s*n : (s+1)*n].
			// buf[s*n + i] must equal src[i*size + s] (byte s of element i).
			for s := range size {
				for i := range n {
					got := buf[s*n+i]
					want := src[i*size+s]
					if got != want {
						t.Errorf("buf[stream=%d][elem=%d]: got %#x, want %#x", s, i, got, want)
					}
				}
			}

			decoded, err := enc.DecodeFixedLenByteArray(nil, buf, size)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if !bytes.Equal(src, decoded) {
				t.Fatalf("roundtrip mismatch:\n  src=%v\n  got=%v", src, decoded)
			}
		})
	}
}
