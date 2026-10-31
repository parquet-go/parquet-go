package bloom

import (
	"io"
	"math"
	"math/bits"

	"github.com/parquet-go/bitpack/unsafecast"
	"github.com/parquet-go/parquet-go/internal/memory"
)

// Filter is an interface representing read-only bloom filters where programs
// can probe for the possible presence of a hash key.
type Filter interface {
	Check(uint64) bool
}

// SplitBlockFilter is an in-memory implementation of the parquet bloom filters.
//
// This type is useful to construct bloom filters that are later serialized
// to a storage medium.
type SplitBlockFilter []Block

// MakeSplitBlockFilter constructs a SplitBlockFilter value from the data byte
// slice.
func MakeSplitBlockFilter(data []byte) SplitBlockFilter {
	return unsafecast.Slice[Block](data)
}

// NumSplitBlocksOf returns the number of blocks in a filter intended to hold
// the given number of values and bits of filter per value.
//
// This function is useful to determine the number of blocks when creating bloom
// filters in memory, for example:
//
//	f := make(bloom.SplitBlockFilter, bloom.NumSplitBlocksOf(n, 10))
func NumSplitBlocksOf(numValues int64, bitsPerValue uint) int {
	numBytes := ((uint(numValues) * bitsPerValue) + 7) / 8
	numBlocks := (numBytes + (BlockSize - 1)) / BlockSize
	return int(numBlocks)
}

// BitsPerValueOf returns the smallest number of bits of filter per value for
// which a split block filter holding distinct values is expected to have a
// false positive rate at or below falsePositiveRate, up to 64.
func BitsPerValueOf(falsePositiveRate float64) uint {
	const maxBitsPerValue = 64
	for bitsPerValue := uint(1); bitsPerValue < maxBitsPerValue; bitsPerValue++ {
		if expectedFalsePositiveRate(bitsPerValue) <= falsePositiveRate {
			return bitsPerValue
		}
	}
	return maxBitsPerValue
}

// The number of values hashed to a block follows a Poisson distribution, and
// each value sets one bit in each of the 8 words of 32 bits of its block.
func expectedFalsePositiveRate(bitsPerValue uint) float64 {
	lambda := float64(8*BlockSize) / float64(bitsPerValue)
	limit := int(lambda+12*math.Sqrt(lambda)) + 16
	p := math.Exp(-lambda) // probability that a block holds i values
	q := 1.0               // probability that a bit is still unset with i values
	sum := 0.0
	for i := 0; i <= limit; i++ {
		if i > 0 {
			p *= lambda / float64(i)
			q *= 31.0 / 32
		}
		sum += p * math.Pow(1-q, 8)
	}
	return sum
}

// Reset clears the content of the filter f.
func (f SplitBlockFilter) Reset() {
	for i := range f {
		f[i] = Block{}
	}
}

// Block returns a pointer to the block that the given value hashes to in the
// bloom filter.
func (f SplitBlockFilter) Block(x uint64) *Block { return &f[fasthash1x64(x, int32(len(f)))] }

// InsertBulk adds all values from x into f.
func (f SplitBlockFilter) InsertBulk(x []uint64) { filterInsertBulk(f, x) }

// Insert adds x to f.
func (f SplitBlockFilter) Insert(x uint64) { filterInsert(f, x) }

// Check tests whether x is in f.
func (f SplitBlockFilter) Check(x uint64) bool { return filterCheck(f, x) }

// Fold halves the number of blocks in f by merging adjacent pairs of blocks,
// returning the folded filter which shares the memory of f.
//
// Block indexes are derived from the hash with a multiply-shift, so halving the
// number of blocks maps a value from block i to block i/2. The folded filter is
// therefore identical to a filter of half the size that the same values had
// been inserted into, and no false negatives are introduced.
//
// The method panics if the number of blocks in f is odd.
func (f SplitBlockFilter) Fold() SplitBlockFilter {
	if len(f)%2 != 0 {
		panic("bloom: cannot fold a split block filter with an odd number of blocks")
	}
	for i := range len(f) / 2 {
		f[i] = mergeBlocks(&f[2*i], &f[2*i+1])
	}
	return f[:len(f)/2]
}

// FoldTo folds f as many times as possible while keeping its false positive
// rate at or below maxFalsePositiveRate, returning the folded filter which
// shares the memory of f.
//
// Folding stops early when the number of blocks becomes odd, so filters are
// best allocated with a power of two number of blocks.
func (f SplitBlockFilter) FoldTo(maxFalsePositiveRate float64) SplitBlockFilter {
	for len(f) > 1 && len(f)%2 == 0 && f.foldedFalsePositiveRate() <= maxFalsePositiveRate {
		f = f.Fold()
	}
	return f
}

// FalsePositiveRate returns the probability that f reports a value which was
// never inserted as present, based on the bits currently set in f.
func (f SplitBlockFilter) FalsePositiveRate() float64 {
	if len(f) == 0 {
		return 0
	}
	sum := 0.0
	for i := range f {
		sum += blockFalsePositiveRate(&f[i])
	}
	return sum / float64(len(f))
}

// foldedFalsePositiveRate returns the false positive rate that f would have
// after a call to Fold, without modifying f.
func (f SplitBlockFilter) foldedFalsePositiveRate() float64 {
	sum := 0.0
	for i := 0; i+1 < len(f); i += 2 {
		b := mergeBlocks(&f[i], &f[i+1])
		sum += blockFalsePositiveRate(&b)
	}
	return sum / float64(len(f)/2)
}

func mergeBlocks(a, b *Block) (m Block) {
	for i := range m {
		m[i] = a[i] | b[i]
	}
	return m
}

// A value tests positive when the bit it selects in each of the 8 words is set,
// with bits selected uniformly.
func blockFalsePositiveRate(b *Block) float64 {
	p := 1.0
	for _, w := range b {
		p *= float64(bits.OnesCount32(uint32(w))) / 32
	}
	return p
}

// Bytes converts f to a byte slice.
//
// The returned slice shares the memory of f. The method is intended to be used
// to serialize the bloom filter to a storage medium.
func (f SplitBlockFilter) Bytes() []byte {
	return unsafecast.Slice[byte](f)
}

// CheckSplitBlock is similar to bloom.SplitBlockFilter.Check but reads the
// bloom filter of n bytes from r.
//
// The size n of the bloom filter is assumed to be a multiple of the block size.
func CheckSplitBlock(r io.ReaderAt, n int64, x uint64) (bool, error) {
	block := acquireBlock()
	defer releaseBlock(block)
	offset := BlockSize * fasthash1x64(x, int32(n/BlockSize))
	_, err := r.ReadAt(block.Bytes(), int64(offset))
	return block.Check(uint32(x)), err
}

var (
	blockPool memory.Pool[Block]
)

func acquireBlock() *Block {
	return blockPool.Get(
		func() *Block { return new(Block) },
		func(b *Block) {},
	)
}

func releaseBlock(b *Block) {
	blockPool.Put(b)
}
