package delta_test

import (
	"fmt"
	"math/rand"
	"slices"
	"testing"

	"github.com/parquet-go/parquet-go/encoding/delta"
	"github.com/parquet-go/parquet-go/encoding/fuzz"
	"github.com/parquet-go/parquet-go/encoding/test"
)

func FuzzDeltaBinaryPackedInt32(f *testing.F) {
	fuzz.EncodeInt32(f, new(delta.BinaryPackedEncoding))
}

func FuzzDeltaBinaryPackedInt64(f *testing.F) {
	fuzz.EncodeInt64(f, new(delta.BinaryPackedEncoding))
}

func FuzzDeltaLengthByteArray(f *testing.F) {
	fuzz.EncodeByteArray(f, new(delta.LengthByteArrayEncoding))
}

func FuzzDeltaByteArray(f *testing.F) {
	fuzz.EncodeByteArray(f, new(delta.ByteArrayEncoding))
}

const (
	encodeMinNumValues = 0
	encodeMaxNumValues = 200
)

func TestEncodeInt32(t *testing.T) {
	for bitWidth := uint(0); bitWidth <= 32; bitWidth++ {
		t.Run(fmt.Sprintf("bitWidth=%d", bitWidth), func(t *testing.T) {
			test.EncodeInt32(t,
				new(delta.BinaryPackedEncoding),
				encodeMinNumValues,
				encodeMaxNumValues,
				bitWidth,
			)
		})
	}
}

func TestEncodeInt64(t *testing.T) {
	for bitWidth := uint(0); bitWidth <= 64; bitWidth++ {
		t.Run(fmt.Sprintf("bitWidth=%d", bitWidth), func(t *testing.T) {
			test.EncodeInt64(t,
				new(delta.BinaryPackedEncoding),
				encodeMinNumValues,
				encodeMaxNumValues,
				bitWidth,
			)
		})
	}
}

func TestDecodeInt32ShortBufferDirtyCapacity(t *testing.T) {
	values := make([]int32, 200)
	r := rand.New(rand.NewSource(1))
	for i := range values {
		values[i] = r.Int31()
	}
	enc := new(delta.BinaryPackedEncoding)
	encoded, err := enc.EncodeInt32(nil, values)
	if err != nil {
		t.Fatal(err)
	}
	testShortBufferDirtyCapacity(t, encoded, func(src []byte) ([]int32, error) {
		return enc.DecodeInt32(nil, src)
	})
}

func TestDecodeInt64ShortBufferDirtyCapacity(t *testing.T) {
	values := make([]int64, 200)
	r := rand.New(rand.NewSource(1))
	for i := range values {
		values[i] = r.Int63()
	}
	enc := new(delta.BinaryPackedEncoding)
	encoded, err := enc.EncodeInt64(nil, values)
	if err != nil {
		t.Fatal(err)
	}
	testShortBufferDirtyCapacity(t, encoded, func(src []byte) ([]int64, error) {
		return enc.DecodeInt64(nil, src)
	})
}

func testShortBufferDirtyCapacity[T comparable](t *testing.T, encoded []byte, decode func([]byte) ([]T, error)) {
	const spare = 64
	for drop := 1; drop < len(encoded); drop++ {
		n := len(encoded) - drop
		clean := make([]byte, n, n+spare)
		copy(clean, encoded)
		dirty := slices.Repeat([]byte{0xFF}, n+spare)[:n]
		copy(dirty, encoded)

		want, wantErr := decode(clean)
		got, gotErr := decode(dirty)
		if (wantErr == nil) != (gotErr == nil) {
			t.Fatalf("drop=%d: errors differ: clean=%v dirty=%v", drop, wantErr, gotErr)
		}
		if !slices.Equal(want, got) {
			t.Fatalf("drop=%d: dirty capacity changed decoded values", drop)
		}
	}
}
