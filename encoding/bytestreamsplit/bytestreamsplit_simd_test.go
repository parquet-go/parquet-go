//go:build goexperiment.simd

package bytestreamsplit

import (
	"bytes"
	"math/rand"
	"testing"
)

// The tests below compare the archsimd transpose implementations against a
// scalar reference for lengths around the 64 value vector chunk boundary,
// so both the vector path and the scalar tail are exercised on hardware
// that supports the vector path.

func refEncode(dst, src []byte, size int) {
	n := len(src) / size
	for i := range n {
		for b := range size {
			dst[b*n+i] = src[i*size+b]
		}
	}
}

func refDecode(dst, src []byte, size int) {
	n := len(src) / size
	for i := range n {
		for b := range size {
			dst[i*size+b] = src[b*n+i]
		}
	}
}

func testTranspose(t *testing.T, size int, encode, decode func(dst, src []byte)) {
	prng := rand.New(rand.NewSource(0))
	for _, n := range []int{0, 1, 2, 63, 64, 65, 100, 127, 128, 1000, 1027} {
		values := make([]byte, n*size)
		prng.Read(values)

		planes := make([]byte, n*size)
		refPlanes := make([]byte, n*size)
		encode(planes, values)
		refEncode(refPlanes, values, size)
		if !bytes.Equal(planes, refPlanes) {
			t.Fatalf("encode mismatch for n=%d", n)
		}

		decoded := make([]byte, n*size)
		refDecoded := make([]byte, n*size)
		decode(decoded, planes)
		refDecode(refDecoded, planes, size)
		if !bytes.Equal(decoded, refDecoded) || !bytes.Equal(decoded, values) {
			t.Fatalf("decode mismatch for n=%d", n)
		}
	}
}

func TestTransposeFloatSIMD(t *testing.T)  { testTranspose(t, 4, encodeFloat, decodeFloat) }
func TestTransposeDoubleSIMD(t *testing.T) { testTranspose(t, 8, encodeDouble, decodeDouble) }

func TestTransposeSIMDBufferBounds(t *testing.T) {
	// The vector paths must not write a single byte past len(dst): unlike
	// some other kernels in this module, the callers do not guarantee any
	// headroom. Run with exact size allocations under -race or with the
	// guard bytes below to catch overruns.
	for _, size := range []int{4, 8} {
		for _, n := range []int{64, 65, 100, 128} {
			src := make([]byte, n*size)
			for i := range src {
				src[i] = byte(i)
			}
			buf := make([]byte, n*size+64)
			for i := n * size; i < len(buf); i++ {
				buf[i] = 0xAB
			}
			fn, gn := encodeFloat, decodeFloat
			if size == 8 {
				fn, gn = encodeDouble, decodeDouble
			}
			fn(buf[:n*size], src)
			for i := n * size; i < len(buf); i++ {
				if buf[i] != 0xAB {
					t.Fatalf("size=%d n=%d: encode wrote past len(dst) at +%d", size, n, i-n*size)
				}
			}
			out := make([]byte, n*size+64)
			for i := n * size; i < len(out); i++ {
				out[i] = 0xAB
			}
			gn(out[:n*size], buf[:n*size])
			for i := n * size; i < len(out); i++ {
				if out[i] != 0xAB {
					t.Fatalf("size=%d n=%d: decode wrote past len(dst) at +%d", size, n, i-n*size)
				}
			}
		}
	}
}
