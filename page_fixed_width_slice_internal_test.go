package parquet

import (
	"slices"
	"testing"
	"unsafe"

	"github.com/parquet-go/parquet-go/encoding"
)

func TestFixedWidthPageSliceZeroCopy(t *testing.T) {
	t.Run("INT32", func(t *testing.T) {
		values := []int32{0, 1, 2, 3}
		page := newInt32Page(int32Type{}, 0, int32(len(values)), encoding.Int32Values(values))
		assertFixedWidthPageSliceZeroCopy(t, page, values, func(page Page) []int32 {
			data := page.Data()
			return data.Int32()
		})
	})

	t.Run("INT64", func(t *testing.T) {
		values := []int64{0, 1, 2, 3}
		page := newInt64Page(int64Type{}, 0, int32(len(values)), encoding.Int64Values(values))
		assertFixedWidthPageSliceZeroCopy(t, page, values, func(page Page) []int64 {
			data := page.Data()
			return data.Int64()
		})
	})

	t.Run("UINT32", func(t *testing.T) {
		values := []uint32{0, 1, 2, 3}
		page := newUint32Page(uint32Type{}, 0, int32(len(values)), encoding.Uint32Values(values))
		assertFixedWidthPageSliceZeroCopy(t, page, values, func(page Page) []uint32 {
			data := page.Data()
			return data.Uint32()
		})
	})

	t.Run("UINT64", func(t *testing.T) {
		values := []uint64{0, 1, 2, 3}
		page := newUint64Page(uint64Type{}, 0, int32(len(values)), encoding.Uint64Values(values))
		assertFixedWidthPageSliceZeroCopy(t, page, values, func(page Page) []uint64 {
			data := page.Data()
			return data.Uint64()
		})
	})
}

func assertFixedWidthPageSliceZeroCopy[T comparable](t *testing.T, page Page, values []T, data func(Page) []T) {
	t.Helper()

	sliced := page.Slice(1, 3)
	got := data(sliced)
	if !slices.Equal(got, values[1:3]) {
		t.Fatalf("wrong sliced values: got %v, want %v", got, values[1:3])
	}
	if unsafe.SliceData(got) != unsafe.SliceData(values[1:3]) {
		t.Fatal("sliced page copied the backing array")
	}
	if cap(got) != len(got) {
		t.Fatalf("sliced page capacity was not limited: got %d, want %d", cap(got), len(got))
	}

	nested := data(sliced.Slice(1, 2))
	if !slices.Equal(nested, values[2:3]) {
		t.Fatalf("wrong nested slice values: got %v, want %v", nested, values[2:3])
	}
	if unsafe.SliceData(nested) != unsafe.SliceData(values[2:3]) {
		t.Fatal("nested slice copied the backing array")
	}

	empty := page.Slice(int64(len(values)), int64(len(values)))
	if got := data(empty); len(got) != 0 {
		t.Fatalf("wrong empty slice length: got %d, want 0", len(got))
	}
}

var benchmarkFixedWidthSlicedPage Page

func BenchmarkFixedWidthPageSlice(b *testing.B) {
	const numValues = 1 << 20

	b.Run("INT32", func(b *testing.B) {
		values := make([]int32, numValues)
		page := newInt32Page(int32Type{}, 0, numValues, encoding.Int32Values(values))
		benchmarkFixedWidthPageSlice(b, page)
	})

	b.Run("INT64", func(b *testing.B) {
		values := make([]int64, numValues)
		page := newInt64Page(int64Type{}, 0, numValues, encoding.Int64Values(values))
		benchmarkFixedWidthPageSlice(b, page)
	})

	b.Run("UINT32", func(b *testing.B) {
		values := make([]uint32, numValues)
		page := newUint32Page(uint32Type{}, 0, numValues, encoding.Uint32Values(values))
		benchmarkFixedWidthPageSlice(b, page)
	})

	b.Run("UINT64", func(b *testing.B) {
		values := make([]uint64, numValues)
		page := newUint64Page(uint64Type{}, 0, numValues, encoding.Uint64Values(values))
		benchmarkFixedWidthPageSlice(b, page)
	})
}

func benchmarkFixedWidthPageSlice(b *testing.B, page Page) {
	numValues := page.NumValues()
	benchmarks := []struct {
		name string
		i    int64
		j    int64
	}{
		{name: "full", i: 0, j: numValues},
		{name: "suffix", i: numValues / 2, j: numValues},
		{name: "narrow", i: numValues / 2, j: numValues/2 + 1},
	}
	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				benchmarkFixedWidthSlicedPage = page.Slice(benchmark.i, benchmark.j)
			}
		})
	}
}
