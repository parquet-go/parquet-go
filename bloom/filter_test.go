package bloom_test

import (
	"bytes"
	"math/rand"
	"slices"
	"testing"

	"github.com/parquet-go/parquet-go/bloom"
)

func TestSplitBlockFilter(t *testing.T) {
	const N = 1000
	const S = 3
	f := make(bloom.SplitBlockFilter, bloom.NumSplitBlocksOf(N, 10))
	p := rand.New(rand.NewSource(S))

	// Half of the values are inserted individually.
	for range N / 2 {
		f.Insert(p.Uint64())
	}
	// The other half is inserted as a bulk operation.
	b := make([]uint64, N/2)
	for i := range b {
		b[i] = p.Uint64()
	}
	f.InsertBulk(b)

	if f.Block(0) == nil {
		t.Fatal("looking up filter block returned impossible nil value")
	}

	for _, test := range []struct {
		scenario string
		filter   bloom.Filter
	}{
		{scenario: "filter", filter: f},
		{scenario: "reader", filter: newSerializedFilter(f.Bytes())},
	} {
		t.Run(test.scenario, func(t *testing.T) {
			p.Seed(S)
			falsePositives := 0

			for i := range N {
				x := p.Uint64()

				if !test.filter.Check(x) {
					t.Fatalf("bloom filter block does not contain the value #%d that was inserted: %d", i, x)
				}
				if test.filter.Check(^x) {
					falsePositives++
				}
			}

			if r := (float64(falsePositives) / N); r > 0.01 {
				t.Fatalf("bloom filter triggered too many false positives: %g%%", r*100)
			}
		})
	}

	t.Run("Reset", func(t *testing.T) {
		allZeros := true
		for _, b := range f.Bytes() {
			if b != 0 {
				allZeros = false
				break
			}
		}
		if allZeros {
			t.Fatal("bloom filter bytes were all zero after inserting keys")
		}
		f.Reset()
		for i, b := range f.Bytes() {
			if b != 0 {
				t.Fatalf("bloom filter byte at index %d was not zero after resetting the filter: %02X", i, b)
			}
		}
	})
}

func TestSplitBlockFilterBug1(t *testing.T) {
	// This test exercises the case where we bulk insert a single key in the
	// filter, which skips the core of the optimized assembly routines and runs
	// through the loop handling tails of remaining keys after consuming groups
	// of two or more.
	//
	// The use of quick.Check in bloom filter tests of the parquet package had
	// uncovered a bug which was reproduced here in isolation when debugging.
	h := [1]uint64{0b1000101001000001001001111000000100011011001000011110011100110000}
	f := make(bloom.SplitBlockFilter, 1)
	f.InsertBulk(h[:])
	if !f.Check(h[0]) {
		t.Error("value inserted in the filter was not found")
	}
}

func TestSplitBlockFilterFold(t *testing.T) {
	const N = 1000
	p := rand.New(rand.NewSource(1))
	x := make([]uint64, N)
	for i := range x {
		x[i] = p.Uint64()
	}

	// Folding must produce exactly the filter that inserting the same values
	// into a filter of the folded size would have.
	f := make(bloom.SplitBlockFilter, 256)
	f.InsertBulk(x)
	for numBlocks := 128; numBlocks >= 1; numBlocks /= 2 {
		f = f.Fold()
		want := make(bloom.SplitBlockFilter, numBlocks)
		want.InsertBulk(x)
		if !bytes.Equal(f.Bytes(), want.Bytes()) {
			t.Fatalf("filter folded to %d blocks differs from a filter of %d blocks", len(f), numBlocks)
		}
		for i, v := range x {
			if !f.Check(v) {
				t.Fatalf("filter folded to %d blocks does not contain the value #%d that was inserted: %d", len(f), i, v)
			}
		}
	}

	t.Run("odd", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatal("folding a filter with an odd number of blocks did not panic")
			}
		}()
		make(bloom.SplitBlockFilter, 3).Fold()
	})
}

func TestSplitBlockFilterFoldTo(t *testing.T) {
	const N = 1000
	const maxFalsePositiveRate = 0.01
	p := rand.New(rand.NewSource(2))
	x := make([]uint64, N)
	for i := range x {
		x[i] = p.Uint64()
	}

	f := make(bloom.SplitBlockFilter, 4096)
	f.InsertBulk(x)
	f = f.FoldTo(maxFalsePositiveRate)

	if r := f.FalsePositiveRate(); r > maxFalsePositiveRate {
		t.Fatalf("folded filter false positive rate is above the target: %g > %g", r, maxFalsePositiveRate)
	}
	if len(f) > 1 {
		if r := slices.Clone(f).Fold().FalsePositiveRate(); r <= maxFalsePositiveRate {
			t.Fatalf("filter of %d blocks could have been folded further: %g <= %g", len(f), r, maxFalsePositiveRate)
		}
	}
	if len(f) >= 4096 {
		t.Fatalf("filter was not folded")
	}
	for i, v := range x {
		if !f.Check(v) {
			t.Fatalf("folded filter does not contain the value #%d that was inserted: %d", i, v)
		}
	}

	// The estimated false positive rate should match the observed one.
	falsePositives := 0
	for range 100_000 {
		if f.Check(p.Uint64()) {
			falsePositives++
		}
	}
	if r := float64(falsePositives) / 100_000; r > 2*maxFalsePositiveRate {
		t.Fatalf("folded filter triggered too many false positives: %g%%", r*100)
	}

	t.Run("empty", func(t *testing.T) {
		if f := make(bloom.SplitBlockFilter, 64).FoldTo(maxFalsePositiveRate); len(f) != 1 {
			t.Fatalf("empty filter folded to %d blocks instead of 1", len(f))
		}
	})
}

func TestBitsPerValueOf(t *testing.T) {
	const N = 20_000
	const M = 200_000
	p := rand.New(rand.NewSource(3))

	for _, falsePositiveRate := range []float64{0.1, 0.01, 0.001} {
		bitsPerValue := bloom.BitsPerValueOf(falsePositiveRate)
		f := make(bloom.SplitBlockFilter, bloom.NumSplitBlocksOf(N, bitsPerValue))
		for range N {
			f.Insert(p.Uint64())
		}
		falsePositives := 0
		for range M {
			if f.Check(p.Uint64()) {
				falsePositives++
			}
		}
		// Allow for sampling noise around the target.
		if r := float64(falsePositives) / M; r > 1.25*falsePositiveRate {
			t.Errorf("%d bits per value for a target of %g: false positive rate is %g", bitsPerValue, falsePositiveRate, r)
		}
		if r := f.FalsePositiveRate(); r > 1.25*falsePositiveRate {
			t.Errorf("%d bits per value for a target of %g: estimated false positive rate is %g", bitsPerValue, falsePositiveRate, r)
		}
	}

	if b := bloom.BitsPerValueOf(0); b != 64 {
		t.Errorf("bits per value for a target of zero is not capped: %d", b)
	}
	if b := bloom.BitsPerValueOf(0.01); b <= bloom.BitsPerValueOf(0.1) || b >= bloom.BitsPerValueOf(0.001) {
		t.Errorf("bits per value does not increase as the target decreases")
	}
}

type serializedFilter struct {
	bytes.Reader
}

func (f *serializedFilter) Check(x uint64) bool {
	ok, _ := bloom.CheckSplitBlock(&f.Reader, f.Size(), x)
	return ok
}

func newSerializedFilter(b []byte) *serializedFilter {
	f := new(serializedFilter)
	f.Reset(b)
	return f
}

func BenchmarkFilterInsertBulk(b *testing.B) {
	f := make(bloom.SplitBlockFilter, 99)
	x := make([]uint64, 16)
	r := rand.NewSource(0).(rand.Source64)

	for i := range x {
		x[i] = r.Uint64()
	}

	for b.Loop() {
		f.InsertBulk(x)
	}

	b.SetBytes(bloom.BlockSize * int64(len(x)))
}

func BenchmarkFilterInsert(b *testing.B) {
	f := make(bloom.SplitBlockFilter, 1)
	for i := 0; b.Loop(); i++ {
		f.Insert(uint64(i))
	}
	b.SetBytes(bloom.BlockSize)
}

func BenchmarkFilterCheck(b *testing.B) {
	f := make(bloom.SplitBlockFilter, 1)
	f.Insert(42)
	for b.Loop() {
		f.Check(42)
	}
	b.SetBytes(bloom.BlockSize)
}
