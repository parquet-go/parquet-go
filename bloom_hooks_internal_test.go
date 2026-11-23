package parquet

import (
	"bytes"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/parquet-go/parquet-go/bloom"
)

type bloomHookStats struct {
	finalized    atomic.Int64
	distinctSize atomic.Int64 // last numDistinctValues passed to DistinctSize
	copyChecks   atomic.Int64
}

// halvingBloomFilter is a split block filter which implements the optional
// BloomFilterColumn interfaces: it folds its filter in half once all values
// have been inserted, when the number of blocks is even.
type halvingBloomFilter struct {
	BloomFilterColumn
	stats    *bloomHookStats
	finalize func([]byte) []byte // overrides the folding when set
	canCopy  bool
}

func newHalvingBloomFilter(path string) halvingBloomFilter {
	return halvingBloomFilter{
		BloomFilterColumn: SplitBlockFilter(10, path),
		stats:             new(bloomHookStats),
		canCopy:           true,
	}
}

func halveBloomFilter(filter []byte) []byte {
	f := bloom.MakeSplitBlockFilter(filter)
	if len(f) < 2 || len(f)%2 != 0 {
		return filter
	}
	for i := range len(f) / 2 {
		for j := range f[i] {
			f[i][j] = f[2*i][j] | f[2*i+1][j]
		}
	}
	return filter[:len(filter)/2]
}

func (f halvingBloomFilter) FinalizeBloomFilter(filter []byte) []byte {
	f.stats.finalized.Add(1)
	if f.finalize != nil {
		return f.finalize(filter)
	}
	return halveBloomFilter(filter)
}

func (f halvingBloomFilter) DistinctSize(numDistinctValues int64) int {
	f.stats.distinctSize.Store(numDistinctValues)
	return 2 * f.Size(numDistinctValues)
}

func (f halvingBloomFilter) CanCopyBloomFilter(numValues int64, bitset []byte) bool {
	f.stats.copyChecks.Add(1)
	if !f.canCopy {
		return false
	}
	return len(bitset) == f.finalSize(numValues)
}

// finalSize returns the size of the filter for numValues values once halved.
func (f halvingBloomFilter) finalSize(numValues int64) int {
	size := f.Size(numValues)
	if n := size / bloom.BlockSize; n >= 2 && n%2 == 0 {
		return size / 2
	}
	return size
}

func TestBloomFilterHooks(t *testing.T) {
	rows := makeCopyTestRows(1000)

	checkFilter := func(t *testing.T, chunk ColumnChunk, wantSize int64, values ...Value) {
		t.Helper()
		bf := chunk.BloomFilter()
		if bf == nil {
			t.Fatal("column has no bloom filter")
		}
		if bf.Size() != wantSize {
			t.Fatalf("bloom filter has the wrong size: want=%d got=%d", wantSize, bf.Size())
		}
		for _, v := range values {
			if ok, err := bf.Check(v); err != nil {
				t.Fatal(err)
			} else if !ok {
				t.Fatalf("bloom filter misses present value %v", v)
			}
		}
	}

	// rewrite copies the row groups of src to a new file whose writer uses
	// filter, returning the file and the number of chunks copied verbatim.
	rewrite := func(t *testing.T, src *File, filter halvingBloomFilter) (*File, int64) {
		t.Helper()
		before := copyPathCounter.Load()
		var buf bytes.Buffer
		w := NewGenericWriter[copyTestRow](&buf, BloomFilters(filter))
		for _, rg := range src.RowGroups() {
			if _, err := w.WriteRowGroup(rg); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		out, err := OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		if err != nil {
			t.Fatal(err)
		}
		return out, copyPathCounter.Load() - before
	}

	ids := []Value{ValueOf(int64(0)), ValueOf(int64(1)), ValueOf(int64(500)), ValueOf(int64(999))}
	names := make([]Value, 10)
	for i := range names {
		names[i] = ValueOf(fmt.Sprintf("name-%d", i))
	}

	t.Run("finalize_and_distinct_size", func(t *testing.T) {
		id, name := newHalvingBloomFilter("id"), newHalvingBloomFilter("name")
		f := writeCopyTestFile(t, rows, BloomFilters(id, name))

		if n := id.stats.finalized.Load(); n != 1 {
			t.Fatalf("id filter finalized %d times instead of once", n)
		}
		if n := name.stats.finalized.Load(); n != 1 {
			t.Fatalf("name filter finalized %d times instead of once", n)
		}
		if n := name.stats.distinctSize.Load(); n != 10 {
			t.Fatalf("name filter was sized for %d distinct values instead of 10", n)
		}
		if n := id.stats.distinctSize.Load(); n != 0 {
			t.Fatalf("plain id filter was sized as distinct for %d values", n)
		}

		chunks := f.RowGroups()[0].ColumnChunks()
		checkFilter(t, chunks[0], int64(id.finalSize(int64(len(rows)))), ids...)
		// DistinctSize doubles the size, which finalizing halves again.
		checkFilter(t, chunks[1], int64(name.Size(10)), names...)
	})

	t.Run("finalize_scratch_buffer", func(t *testing.T) {
		// The finalized bitset is copied back into the writer's buffer, so a
		// scratch buffer reused across columns must not leak between them.
		var scratch []byte
		finalize := func(b []byte) []byte {
			scratch = append(scratch[:0], halveBloomFilter(b)...)
			return scratch
		}
		id, name := newHalvingBloomFilter("id"), newHalvingBloomFilter("name")
		id.finalize, name.finalize = finalize, finalize
		f := writeCopyTestFile(t, rows, BloomFilters(id, name))

		want := writeCopyTestFile(t, rows, BloomFilters(newHalvingBloomFilter("id"), newHalvingBloomFilter("name")))
		gotChunks, wantChunks := f.RowGroups()[0].ColumnChunks(), want.RowGroups()[0].ColumnChunks()
		for i := range 2 { // id and name
			got, want := bloomFilterBits(t, gotChunks[i]), bloomFilterBits(t, wantChunks[i])
			if !bytes.Equal(got, want) {
				t.Fatalf("bloom filter of column %d differs from the one finalized without a scratch buffer", i)
			}
		}
	})

	t.Run("invalid_finalize", func(t *testing.T) {
		for _, finalize := range []func([]byte) []byte{
			func([]byte) []byte { return nil },
			func(b []byte) []byte { return b[:len(b)-1] },
			func(b []byte) []byte { return append(b, make([]byte, bloom.BlockSize)...) },
		} {
			id := newHalvingBloomFilter("id")
			id.finalize = finalize
			w := NewGenericWriter[copyTestRow](new(bytes.Buffer), BloomFilters(id))
			if _, err := w.Write(rows); err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err == nil {
				t.Fatal("expected an error when the finalized filter has an invalid size")
			}
		}
	})

	t.Run("copy_checker", func(t *testing.T) {
		src := writeCopyTestFile(t, rows, BloomFilters(newHalvingBloomFilter("id")))
		srcSize := src.RowGroups()[0].ColumnChunks()[0].BloomFilter().Size()

		filter := newHalvingBloomFilter("id")
		out, copied := rewrite(t, src, filter)
		if filter.stats.copyChecks.Load() == 0 {
			t.Fatal("copy checker was not called")
		}
		if copied == 0 {
			t.Fatal("expected verbatim copy when the copy checker accepts the source filter")
		}
		if n := filter.stats.finalized.Load(); n != 0 {
			t.Fatalf("filter of a copied chunk was finalized %d times", n)
		}
		checkFilter(t, out.RowGroups()[0].ColumnChunks()[0], srcSize, ids...)

		filter = newHalvingBloomFilter("id")
		filter.canCopy = false
		out, copied = rewrite(t, src, filter)
		if copied != 0 {
			t.Fatalf("expected no verbatim copy when the copy checker rejects the source filter, %d chunks copied", copied)
		}
		if filter.stats.finalized.Load() == 0 {
			t.Fatal("rebuilt filter was not finalized")
		}
		checkFilter(t, out.RowGroups()[0].ColumnChunks()[0], srcSize, ids...)
	})

	t.Run("copy_checker_partial_block", func(t *testing.T) {
		// The source filter is one byte short of a whole number of blocks;
		// it must be rejected before the checker is asked about it.
		src := writeCopyTestFile(t, rows, BloomFilters(oddSizeBloomFilter{SplitBlockFilter(10, "id")}))
		srcSize := src.RowGroups()[0].ColumnChunks()[0].BloomFilter().Size()
		if srcSize%bloom.BlockSize == 0 {
			t.Fatalf("source bloom filter is made of whole blocks: size=%d", srcSize)
		}

		filter := newHalvingBloomFilter("id")
		_, copied := rewrite(t, src, filter)
		if n := filter.stats.copyChecks.Load(); n != 0 {
			t.Fatalf("copy checker was called %d times for a bitset of partial blocks", n)
		}
		if n := copied; n != 0 {
			t.Fatalf("expected no verbatim copy of a bitset of partial blocks, %d chunks copied", n)
		}
	})
}

// bloomFilterBits returns the bitset of the bloom filter of chunk.
func bloomFilterBits(t *testing.T, chunk ColumnChunk) []byte {
	t.Helper()
	bf := chunk.BloomFilter()
	if bf == nil {
		t.Fatal("column has no bloom filter")
	}
	bits := make([]byte, bf.Size())
	if _, err := bf.ReadAt(bits, 0); err != nil {
		t.Fatal(err)
	}
	return bits
}

// oddSizeBloomFilter sizes filters one byte short of a whole number of blocks,
// to produce source files with invalid bitsets.
type oddSizeBloomFilter struct{ BloomFilterColumn }

func (f oddSizeBloomFilter) Size(numValues int64) int {
	return f.BloomFilterColumn.Size(numValues) - 1
}
