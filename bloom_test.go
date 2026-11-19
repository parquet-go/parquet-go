package parquet

import (
	"math/bits"
	"math/rand"
	"testing"

	"github.com/parquet-go/bitpack/unsafecast"
	"github.com/parquet-go/parquet-go/bloom"
	"github.com/parquet-go/parquet-go/deprecated"
	"github.com/parquet-go/parquet-go/internal/quick"
)

func TestSplitBlockFilter(t *testing.T) {
	newFilter := func(numValues int) bloom.SplitBlockFilter {
		return make(bloom.SplitBlockFilter, bloom.NumSplitBlocksOf(int64(numValues), 11))
	}

	enc := SplitBlockFilter(10, "$").Encoding()

	check := func(filter bloom.SplitBlockFilter, value Value) bool {
		return filter.Check(value.hash(&bloom.XXH64{}))
	}

	tests := []struct {
		scenario string
		function any
	}{
		{
			scenario: "BOOLEAN",
			function: func(values []bool) bool {
				filter := newFilter(len(values))
				enc.EncodeBoolean(filter.Bytes(), unsafecast.Slice[byte](values))
				for _, v := range values {
					if !check(filter, ValueOf(v)) {
						return false
					}
				}
				return true
			},
		},

		{
			scenario: "INT32",
			function: func(values []int32) bool {
				filter := newFilter(len(values))
				enc.EncodeInt32(filter.Bytes(), values)
				for _, v := range values {
					if !check(filter, ValueOf(v)) {
						return false
					}
				}
				return true
			},
		},

		{
			scenario: "INT64",
			function: func(values []int64) bool {
				filter := newFilter(len(values))
				enc.EncodeInt64(filter.Bytes(), values)
				for _, v := range values {
					if !check(filter, ValueOf(v)) {
						return false
					}
				}
				return true
			},
		},

		{
			scenario: "INT96",
			function: func(values []deprecated.Int96) bool {
				filter := newFilter(len(values))
				enc.EncodeInt96(filter.Bytes(), values)
				for _, v := range values {
					if !check(filter, ValueOf(v)) {
						return false
					}
				}
				return true
			},
		},

		{
			scenario: "FLOAT",
			function: func(values []float32) bool {
				filter := newFilter(len(values))
				enc.EncodeFloat(filter.Bytes(), values)
				for _, v := range values {
					if !check(filter, ValueOf(v)) {
						return false
					}
				}
				return true
			},
		},

		{
			scenario: "DOUBLE",
			function: func(values []float64) bool {
				filter := newFilter(len(values))
				enc.EncodeDouble(filter.Bytes(), values)
				for _, v := range values {
					if !check(filter, ValueOf(v)) {
						return false
					}
				}
				return true
			},
		},

		{
			scenario: "BYTE_ARRAY",
			function: func(values [][]byte) bool {
				content := make([]byte, 0, 512)
				offsets := make([]uint32, len(values))
				for _, value := range values {
					offsets = append(offsets, uint32(len(content)))
					content = append(content, value...)
				}
				offsets = append(offsets, uint32(len(content)))
				filter := newFilter(len(values))
				enc.EncodeByteArray(filter.Bytes(), content, offsets)
				for _, v := range values {
					if !check(filter, ValueOf(v)) {
						return false
					}
				}
				return true
			},
		},

		{
			scenario: "FIXED_LEN_BYTE_ARRAY",
			function: func(values []byte) bool {
				filter := newFilter(len(values))
				enc.EncodeFixedLenByteArray(filter.Bytes(), values, 1)
				for _, v := range values {
					if !check(filter, ValueOf([1]byte{v})) {
						return false
					}
				}
				return true
			},
		},
	}

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			if err := quick.Check(test.function); err != nil {
				t.Error(err)
			}
		})
	}
}

func BenchmarkSplitBlockFilter(b *testing.B) {
	const N = 1000
	f := make(bloom.SplitBlockFilter, bloom.NumSplitBlocksOf(N, 10)).Bytes()
	e := SplitBlockFilter(10, "$").Encoding()

	v := make([]int64, N)
	r := rand.NewSource(10)
	for i := range v {
		v[i] = r.Int63()
	}

	for b.Loop() {
		e.EncodeInt64(f, v)
	}

	b.SetBytes(8 * N)
}

func TestFoldableNumBlocks(t *testing.T) {
	for _, test := range []struct{ numBlocks, want int }{
		{0, 0},
		{1, 1},
		{15, 15},
		{16, 16},
		{17, 18},
		{65, 72},
		{128, 128},
		{860, 896},
	} {
		if got := foldableNumBlocks(test.numBlocks); got != test.want {
			t.Errorf("foldableNumBlocks(%d): want=%d got=%d", test.numBlocks, test.want, got)
		}
	}

	for numBlocks := 1; numBlocks <= 1<<16; numBlocks++ {
		got := foldableNumBlocks(numBlocks)
		if got < numBlocks {
			t.Fatalf("foldableNumBlocks(%d) rounded down to %d", numBlocks, got)
		}
		// Rounding must add less than 1/8 of the blocks.
		if 8*(got-numBlocks) >= numBlocks {
			t.Fatalf("foldableNumBlocks(%d) rounded up too much to %d", numBlocks, got)
		}
		// Folding down to the odd part must reach at most 15 blocks.
		if odd := got >> bits.TrailingZeros(uint(got)); odd > 15 {
			t.Fatalf("foldableNumBlocks(%d) = %d cannot fold below %d blocks", numBlocks, got, odd)
		}
	}
}
