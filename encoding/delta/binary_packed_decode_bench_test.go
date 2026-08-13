package delta

import (
	"math/rand"
	"testing"

	"github.com/parquet-go/bitpack"
)

// BenchmarkDecodeInt32PrefixLengths reproduces the prefix-length stream of
// the prefix-heavy byte array benchmark in isolation: 10k small values,
// delta binary packed.
func BenchmarkDecodeInt32PrefixLengths(b *testing.B) {
	r := rand.New(rand.NewSource(1))
	values := make([]int32, 10000)
	for i := range values {
		values[i] = int32(r.Intn(64))
	}
	src, err := (&BinaryPackedEncoding{}).EncodeInt32(nil, values)
	if err != nil {
		b.Fatal(err)
	}
	dst := make([]byte, 0, 4*len(values))
	b.SetBytes(int64(4 * len(values)))
	for b.Loop() {
		dst, _, _ = decodeInt32(dst[:0], src)
	}
}

// BenchmarkBitpackUnpackInt32 measures the raw bitpack kernel on one
// miniblock, isolating it from the decodeInt32 caller.
func BenchmarkBitpackUnpackInt32(b *testing.B) {
	src := make([]byte, 32*6+bitpack.PaddingInt32)
	r := rand.New(rand.NewSource(1))
	r.Read(src)
	dst := make([]int32, 128)
	b.SetBytes(int64(4 * len(dst)))
	for b.Loop() {
		bitpack.Unpack(dst, src[:32*6], 6)
	}
}
