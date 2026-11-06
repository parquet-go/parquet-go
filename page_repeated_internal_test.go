package parquet

import (
	"fmt"
	"slices"
	"testing"

	"github.com/parquet-go/parquet-go/encoding"
)

type repeatedPageSliceExpectation struct {
	repetitionLevels []byte
	definitionLevels []byte
	values           []int64
	numRows          int64
	numNulls         int64
}

func TestRepeatedPageSlice(t *testing.T) {
	base := newInt64Page(int64Type{}, 0, 6, encoding.Int64Values([]int64{10, 11, 12, 13, 14, 15}))
	page := newRepeatedPage(
		base,
		2,
		2,
		[]byte{0, 1, 1, 0, 0, 2, 0, 1, 0, 0},
		[]byte{2, 1, 2, 0, 2, 2, 1, 2, 0, 2},
	)

	if got, want := page.NumRows(), int64(6); got != want {
		t.Fatalf("wrong number of rows: got %d, want %d", got, want)
	}

	tests := []struct {
		name string
		i    int64
		j    int64
		want repeatedPageSliceExpectation
	}{
		{
			name: "full",
			i:    0,
			j:    6,
			want: repeatedPageSliceExpectation{
				repetitionLevels: []byte{0, 1, 1, 0, 0, 2, 0, 1, 0, 0},
				definitionLevels: []byte{2, 1, 2, 0, 2, 2, 1, 2, 0, 2},
				values:           []int64{10, 11, 12, 13, 14, 15},
				numRows:          6,
				numNulls:         4,
			},
		},
		{
			name: "prefix",
			i:    0,
			j:    2,
			want: repeatedPageSliceExpectation{
				repetitionLevels: []byte{0, 1, 1, 0},
				definitionLevels: []byte{2, 1, 2, 0},
				values:           []int64{10, 11},
				numRows:          2,
				numNulls:         2,
			},
		},
		{
			name: "interior",
			i:    1,
			j:    5,
			want: repeatedPageSliceExpectation{
				repetitionLevels: []byte{0, 0, 2, 0, 1, 0},
				definitionLevels: []byte{0, 2, 2, 1, 2, 0},
				values:           []int64{12, 13, 14},
				numRows:          4,
				numNulls:         3,
			},
		},
		{
			name: "suffix",
			i:    2,
			j:    6,
			want: repeatedPageSliceExpectation{
				repetitionLevels: []byte{0, 2, 0, 1, 0, 0},
				definitionLevels: []byte{2, 2, 1, 2, 0, 2},
				values:           []int64{12, 13, 14, 15},
				numRows:          4,
				numNulls:         2,
			},
		},
		{
			name: "single null row",
			i:    4,
			j:    5,
			want: repeatedPageSliceExpectation{
				repetitionLevels: []byte{0},
				definitionLevels: []byte{0},
				values:           nil,
				numRows:          1,
				numNulls:         1,
			},
		},
		{
			name: "empty",
			i:    3,
			j:    3,
			want: repeatedPageSliceExpectation{
				repetitionLevels: nil,
				definitionLevels: nil,
				values:           nil,
				numRows:          0,
				numNulls:         0,
			},
		},
		{
			name: "empty at end",
			i:    6,
			j:    6,
			want: repeatedPageSliceExpectation{
				repetitionLevels: nil,
				definitionLevels: nil,
				values:           nil,
				numRows:          0,
				numNulls:         0,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assertRepeatedPageSlice(t, page.Slice(test.i, test.j), test.want)
		})
	}

	t.Run("nested", func(t *testing.T) {
		sliced := page.Slice(1, 6).Slice(1, 3)
		assertRepeatedPageSlice(t, sliced, repeatedPageSliceExpectation{
			repetitionLevels: []byte{0, 2, 0, 1},
			definitionLevels: []byte{2, 2, 1, 2},
			values:           []int64{12, 13, 14},
			numRows:          2,
			numNulls:         1,
		})
	})
}

func TestRepeatedPageSliceDropsLeadingContinuationValues(t *testing.T) {
	base := newInt64Page(int64Type{}, 0, 3, encoding.Int64Values([]int64{20, 21, 22}))
	page := newRepeatedPage(
		base,
		1,
		2,
		[]byte{1, 0, 1, 0},
		[]byte{2, 2, 1, 2},
	)

	assertRepeatedPageSlice(t, page.Slice(0, page.NumRows()), repeatedPageSliceExpectation{
		repetitionLevels: []byte{0, 1, 0},
		definitionLevels: []byte{2, 1, 2},
		values:           []int64{21, 22},
		numRows:          2,
		numNulls:         1,
	})
}

func TestRepeatedPageSliceBounds(t *testing.T) {
	base := newInt64Page(int64Type{}, 0, 2, encoding.Int64Values([]int64{10, 11}))
	page := newRepeatedPage(base, 1, 1, []byte{0, 0}, []byte{1, 1})

	for _, bounds := range [][2]int64{
		{-1, 0},
		{0, -1},
		{2, 1},
		{0, 3},
		{3, 3},
	} {
		t.Run(fmt.Sprintf("%d:%d", bounds[0], bounds[1]), func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatalf("Slice(%d, %d) did not panic", bounds[0], bounds[1])
				}
			}()
			_ = page.Slice(bounds[0], bounds[1])
		})
	}
}

func assertRepeatedPageSlice(t *testing.T, page Page, want repeatedPageSliceExpectation) {
	t.Helper()

	if got := page.NumRows(); got != want.numRows {
		t.Errorf("wrong number of rows: got %d, want %d", got, want.numRows)
	}
	if got, expected := page.NumValues(), int64(len(want.definitionLevels)); got != expected {
		t.Errorf("wrong number of values: got %d, want %d", got, expected)
	}
	if got := page.NumNulls(); got != want.numNulls {
		t.Errorf("wrong number of nulls: got %d, want %d", got, want.numNulls)
	}
	if got := page.RepetitionLevels(); !slices.Equal(got, want.repetitionLevels) {
		t.Errorf("wrong repetition levels: got %v, want %v", got, want.repetitionLevels)
	}
	if got := page.DefinitionLevels(); !slices.Equal(got, want.definitionLevels) {
		t.Errorf("wrong definition levels: got %v, want %v", got, want.definitionLevels)
	}
	data := page.Data()
	if got := data.Int64(); !slices.Equal(got, want.values) {
		t.Errorf("wrong base values: got %v, want %v", got, want.values)
	}
}

var (
	benchmarkRepeatedSlicedPage Page
	benchmarkRepeatedPageRows   int64
)

func BenchmarkRepeatedPageNumRows(b *testing.B) {
	page := newBenchmarkRepeatedPage(1 << 20)
	b.ReportAllocs()
	for b.Loop() {
		benchmarkRepeatedPageRows = page.NumRows()
	}
}

func BenchmarkRepeatedPageSlice(b *testing.B) {
	page := newBenchmarkRepeatedPage(1 << 20)
	numRows := page.NumRows()
	benchmarks := []struct {
		name string
		i    int64
		j    int64
	}{
		{name: "full", i: 0, j: numRows},
		{name: "suffix", i: numRows / 2, j: numRows},
		{name: "interior", i: numRows / 4, j: numRows / 2},
		{name: "narrow", i: numRows / 2, j: numRows/2 + 1},
	}
	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				benchmarkRepeatedSlicedPage = page.Slice(benchmark.i, benchmark.j)
			}
		})
	}
}

func newBenchmarkRepeatedPage(numValues int) *repeatedPage {
	repetitionLevels := make([]byte, numValues)
	definitionLevels := make([]byte, numValues)
	baseValues := make([]int64, 0, numValues)
	for i := range numValues {
		if i%4 == 0 {
			repetitionLevels[i] = 0
		} else {
			repetitionLevels[i] = 1
		}
		if i%11 == 0 {
			definitionLevels[i] = 0
		} else {
			definitionLevels[i] = 1
			baseValues = append(baseValues, int64(i))
		}
	}

	base := newInt64Page(int64Type{}, 0, int32(len(baseValues)), encoding.Int64Values(baseValues))
	return newRepeatedPage(base, 1, 1, repetitionLevels, definitionLevels)
}
