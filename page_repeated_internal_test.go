package parquet

import (
	"fmt"
	"testing"

	"github.com/parquet-go/parquet-go/encoding"
)

func TestRepeatedPageNumRowsCache(t *testing.T) {
	page := newTestRepeatedPage(
		[]byte{0, 1, 0},
		[]byte{2, 2, 2},
		10, 11,
	)

	if got := page.numRows.Load(); got != -1 {
		t.Fatalf("row count was computed eagerly: got %d", got)
	}
	if got, want := page.NumRows(), int64(2); got != want {
		t.Fatalf("wrong number of rows: got %d, want %d", got, want)
	}
	if got, want := page.numRows.Load(), int64(2); got != want {
		t.Fatalf("wrong cached number of rows: got %d, want %d", got, want)
	}
}

func TestRepeatedPageSliceCachesNumRows(t *testing.T) {
	page := newTestRepeatedPage(
		[]byte{0, 1, 1, 0, 0, 2, 0, 1, 0, 0},
		[]byte{2, 1, 2, 0, 2, 2, 1, 2, 0, 2},
		10, 11, 12, 13, 14, 15,
	)

	tests := []struct {
		name string
		i    int64
		j    int64
		want int64
	}{
		{name: "full", i: 0, j: 6, want: 6},
		{name: "prefix", i: 0, j: 2, want: 2},
		{name: "suffix", i: 2, j: 6, want: 4},
		{name: "interior", i: 1, j: 5, want: 4},
		{name: "empty", i: 3, j: 3, want: 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sliced := page.Slice(test.i, test.j).(*repeatedPage)
			if got := sliced.numRows.Load(); got != test.want {
				t.Fatalf("wrong cached number of rows: got %d, want %d", got, test.want)
			}
		})
	}
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

var (
	benchmarkRepeatedPageRows        int64
	benchmarkRepeatedConstructedPage Page
)

func BenchmarkRepeatedPageConstruction(b *testing.B) {
	page := newBenchmarkRepeatedPage(1 << 20)
	base := page.base
	repetitionLevels := page.repetitionLevels
	definitionLevels := page.definitionLevels

	for _, calls := range []int{0, 1, 2} {
		b.Run(fmt.Sprintf("NumRowsCalls=%d", calls), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				page := newRepeatedPage(base, 1, 1, repetitionLevels, definitionLevels)
				for range calls {
					benchmarkRepeatedPageRows = page.NumRows()
				}
				benchmarkRepeatedConstructedPage = page
			}
		})
	}
}

func BenchmarkRepeatedPageNumRows(b *testing.B) {
	page := newBenchmarkRepeatedPage(1 << 20)
	b.ReportAllocs()
	for b.Loop() {
		benchmarkRepeatedPageRows = page.NumRows()
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
