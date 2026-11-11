package parquet

import (
	"slices"
	"testing"

	"github.com/parquet-go/parquet-go/encoding"
)

type levelPageSliceExpectation struct {
	repetitionLevels []byte
	definitionLevels []byte
	values           []int64
	numRows          int64
	numNulls         int64
}

func TestOptionalPageSlice(t *testing.T) {
	page := newTestOptionalPage(
		[]byte{1, 0, 1, 1, 0, 1},
		10, 11, 12, 13,
	)

	tests := []struct {
		name string
		i    int64
		j    int64
		want levelPageSliceExpectation
	}{
		{
			name: "full",
			i:    0,
			j:    6,
			want: levelPageSliceExpectation{
				definitionLevels: []byte{1, 0, 1, 1, 0, 1},
				values:           []int64{10, 11, 12, 13},
				numRows:          6,
				numNulls:         2,
			},
		},
		{
			name: "suffix",
			i:    2,
			j:    6,
			want: levelPageSliceExpectation{
				definitionLevels: []byte{1, 1, 0, 1},
				values:           []int64{11, 12, 13},
				numRows:          4,
				numNulls:         1,
			},
		},
		{
			name: "interior",
			i:    1,
			j:    5,
			want: levelPageSliceExpectation{
				definitionLevels: []byte{0, 1, 1, 0},
				values:           []int64{11, 12},
				numRows:          4,
				numNulls:         2,
			},
		},
		{
			name: "empty",
			i:    3,
			j:    3,
			want: levelPageSliceExpectation{},
		},
		{
			name: "empty at end",
			i:    6,
			j:    6,
			want: levelPageSliceExpectation{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assertLevelPageSlice(t, page.Slice(test.i, test.j), test.want)
		})
	}

	t.Run("nested suffix", func(t *testing.T) {
		sliced := page.Slice(1, 6).Slice(2, 5)
		assertLevelPageSlice(t, sliced, levelPageSliceExpectation{
			definitionLevels: []byte{1, 0, 1},
			values:           []int64{12, 13},
			numRows:          3,
			numNulls:         1,
		})
	})

	t.Run("all null suffix", func(t *testing.T) {
		page := newTestOptionalPage([]byte{0, 0, 0, 0})
		assertLevelPageSlice(t, page.Slice(2, 4), levelPageSliceExpectation{
			definitionLevels: []byte{0, 0},
			numRows:          2,
			numNulls:         2,
		})
	})
}

func TestRepeatedPageSlice(t *testing.T) {
	page := newTestRepeatedPage(
		[]byte{0, 1, 1, 0, 0, 2, 0, 1, 0, 0},
		[]byte{2, 1, 2, 0, 2, 2, 1, 2, 0, 2},
		10, 11, 12, 13, 14, 15,
	)

	tests := []struct {
		name string
		i    int64
		j    int64
		want levelPageSliceExpectation
	}{
		{
			name: "full",
			i:    0,
			j:    6,
			want: levelPageSliceExpectation{
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
			want: levelPageSliceExpectation{
				repetitionLevels: []byte{0, 1, 1, 0},
				definitionLevels: []byte{2, 1, 2, 0},
				values:           []int64{10, 11},
				numRows:          2,
				numNulls:         2,
			},
		},
		{
			name: "suffix",
			i:    2,
			j:    6,
			want: levelPageSliceExpectation{
				repetitionLevels: []byte{0, 2, 0, 1, 0, 0},
				definitionLevels: []byte{2, 2, 1, 2, 0, 2},
				values:           []int64{12, 13, 14, 15},
				numRows:          4,
				numNulls:         2,
			},
		},
		{
			name: "interior",
			i:    1,
			j:    5,
			want: levelPageSliceExpectation{
				repetitionLevels: []byte{0, 0, 2, 0, 1, 0},
				definitionLevels: []byte{0, 2, 2, 1, 2, 0},
				values:           []int64{12, 13, 14},
				numRows:          4,
				numNulls:         3,
			},
		},
		{
			name: "single null row",
			i:    4,
			j:    5,
			want: levelPageSliceExpectation{
				repetitionLevels: []byte{0},
				definitionLevels: []byte{0},
				numRows:          1,
				numNulls:         1,
			},
		},
		{
			name: "empty",
			i:    3,
			j:    3,
			want: levelPageSliceExpectation{},
		},
		{
			name: "empty at end",
			i:    6,
			j:    6,
			want: levelPageSliceExpectation{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assertLevelPageSlice(t, page.Slice(test.i, test.j), test.want)
		})
	}

	t.Run("nested suffix", func(t *testing.T) {
		sliced := page.Slice(1, 6).Slice(1, 3)
		assertLevelPageSlice(t, sliced, levelPageSliceExpectation{
			repetitionLevels: []byte{0, 2, 0, 1},
			definitionLevels: []byte{2, 2, 1, 2},
			values:           []int64{12, 13, 14},
			numRows:          2,
			numNulls:         1,
		})
	})

	t.Run("leading continuation values", func(t *testing.T) {
		page := newTestRepeatedPage(
			[]byte{1, 0, 1, 0},
			[]byte{2, 2, 1, 2},
			20, 21, 22,
		)
		assertLevelPageSlice(t, page.Slice(0, 2), levelPageSliceExpectation{
			repetitionLevels: []byte{0, 1, 0},
			definitionLevels: []byte{2, 1, 2},
			values:           []int64{21, 22},
			numRows:          2,
			numNulls:         1,
		})
	})

	t.Run("continuation values only", func(t *testing.T) {
		page := newTestRepeatedPage(
			[]byte{1, 1},
			[]byte{2, 2},
			20, 21,
		)
		assertLevelPageSlice(t, page.Slice(0, 0), levelPageSliceExpectation{})
	})

	t.Run("all null suffix", func(t *testing.T) {
		page := newTestRepeatedPage(
			[]byte{0, 1, 0, 0},
			[]byte{0, 0, 0, 0},
		)
		assertLevelPageSlice(t, page.Slice(1, 3), levelPageSliceExpectation{
			repetitionLevels: []byte{0, 0},
			definitionLevels: []byte{0, 0},
			numRows:          2,
			numNulls:         2,
		})
	})
}

func newTestOptionalPage(definitionLevels []byte, values ...int64) *optionalPage {
	base := newInt64Page(int64Type{}, 0, int32(len(values)), encoding.Int64Values(values))
	return newOptionalPage(base, 1, definitionLevels)
}

func newTestRepeatedPage(repetitionLevels, definitionLevels []byte, values ...int64) *repeatedPage {
	base := newInt64Page(int64Type{}, 0, int32(len(values)), encoding.Int64Values(values))
	return newRepeatedPage(base, 2, 2, repetitionLevels, definitionLevels)
}

func assertLevelPageSlice(t *testing.T, page Page, want levelPageSliceExpectation) {
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

var benchmarkLevelSlicedPage Page

func BenchmarkOptionalPageSlice(b *testing.B) {
	const numValues = 1 << 20

	definitionLevels := make([]byte, numValues)
	numBaseValues := 0
	for i := range definitionLevels {
		if i%11 != 0 {
			definitionLevels[i] = 1
			numBaseValues++
		}
	}

	base := &indexedPage{values: make([]int32, numBaseValues)}
	page := newOptionalPage(base, 1, definitionLevels)
	benchmarkLevelPageSlice(b, page)
}

func BenchmarkRepeatedPageSlice(b *testing.B) {
	const numValues = 1 << 20

	repetitionLevels := make([]byte, numValues)
	definitionLevels := make([]byte, numValues)
	numBaseValues := 0
	for i := range repetitionLevels {
		if i%4 != 0 {
			repetitionLevels[i] = 1
		}
		if i%11 != 0 {
			definitionLevels[i] = 1
			numBaseValues++
		}
	}

	base := &indexedPage{values: make([]int32, numBaseValues)}
	page := newRepeatedPage(base, 1, 1, repetitionLevels, definitionLevels)
	benchmarkLevelPageSlice(b, page)
}

func benchmarkLevelPageSlice(b *testing.B, page Page) {
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
				benchmarkLevelSlicedPage = page.Slice(benchmark.i, benchmark.j)
			}
		})
	}
}
