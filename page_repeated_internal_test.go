package parquet

import (
	"fmt"
	"slices"
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

func TestFindRowStart(t *testing.T) {
	for _, size := range []int{0, 1, 255, 256, 257, 511, 512, 513} {
		for _, period := range []int{0, 1, 3, 256} {
			t.Run(fmt.Sprintf("size=%d/period=%d", size, period), func(t *testing.T) {
				levels := make([]byte, size)
				for i := range levels {
					levels[i] = 1
					if period > 0 && i%period == 0 {
						levels[i] = 0
					}
				}

				numRows := countLevelsEqual(levels, 0)
				for row := 0; row <= numRows; row++ {
					want := scalarFindRowStart(levels, int64(row))
					if got := findRowStart(levels, int64(row)); got != want {
						t.Fatalf("row %d: got index %d, want %d", row, got, want)
					}
				}
			})
		}
	}
}

func scalarFindRowStart(levels []byte, rowIndex int64) int {
	for i, level := range levels {
		if level == 0 {
			if rowIndex == 0 {
				return i
			}
			rowIndex--
		}
	}
	return len(levels)
}

// repeatedRowBlockSize is the block size of findRowStart. Slice tests on
// smaller pages never enter that lookup.
const repeatedRowBlockSize = 256

func TestRepeatedPageSliceAcrossBlocks(t *testing.T) {
	const maxDefinitionLevel byte = 2

	t.Run("reference", func(t *testing.T) {
		// Pins the linear oracle to a hand-checked slice before the large
		// pages trust it. Repetition 0 starts a row. Definition below 2 is null.
		rep := []byte{0, 1, 1, 0, 0, 2, 0, 1, 0, 0}
		def := []byte{2, 1, 2, 0, 2, 2, 1, 2, 0, 2}
		values := []int64{10, 11, 12, 13, 14, 15}
		page := newTestRepeatedPage(rep, def, values...)
		got := page.Slice(1, 5)
		if gotRep := got.RepetitionLevels(); !slices.Equal(gotRep, []byte{0, 0, 2, 0, 1, 0}) {
			t.Fatalf("repetition levels: %s", sliceMismatch(gotRep, []byte{0, 0, 2, 0, 1, 0}))
		}
		if gotDef := got.DefinitionLevels(); !slices.Equal(gotDef, []byte{0, 2, 2, 1, 2, 0}) {
			t.Fatalf("definition levels: %s", sliceMismatch(gotDef, []byte{0, 2, 2, 1, 2, 0}))
		}
		data := got.Data()
		if gotValues := data.Int64(); !slices.Equal(gotValues, []int64{12, 13, 14}) {
			t.Fatalf("base values: %s", sliceMismatch(gotValues, []int64{12, 13, 14}))
		}
		if got.NumRows() != 4 || got.NumNulls() != 3 {
			t.Fatalf("NumRows=%d NumNulls=%d, want 4 and 3", got.NumRows(), got.NumNulls())
		}
		assertRepeatedPageSliceMatchesLinear(t, page, values, maxDefinitionLevel, 1, 5)
	})

	pages := []struct {
		name       string
		build      func(*testing.T) (rep, def []byte)
		spansBlock bool
	}{
		{name: "span", spansBlock: true, build: repeatedLevelsSpanningBlock},
		{name: "boundary", build: repeatedLevelsOnBlockBoundaries},
		{name: "leading-continuation", spansBlock: true, build: repeatedLevelsLeadingContinuation},
	}
	for _, test := range pages {
		t.Run(test.name, func(t *testing.T) {
			rep, def := test.build(t)
			if len(rep) != len(def) {
				t.Fatalf("level length mismatch: repetition %d, definition %d", len(rep), len(def))
			}
			if len(rep) <= repeatedRowBlockSize*2 {
				t.Fatalf("page has %d repetition levels, want more than %d", len(rep), repeatedRowBlockSize*2)
			}
			requireBoundaryRowStart(t, rep)
			if test.spansBlock {
				requireRowSpansFullBlock(t, rep, def, maxDefinitionLevel)
			}

			values := baseValuesForDefinition(def, maxDefinitionLevel)
			page := newTestRepeatedPage(rep, def, values...)
			numRows := page.NumRows()
			if numRows < 2 {
				t.Fatalf("expected multiple rows, got %d", numRows)
			}
			// Every row range. That includes each prefix, suffix, and single row.
			for i := int64(0); i <= numRows; i++ {
				for j := i; j <= numRows; j++ {
					assertRepeatedPageSliceMatchesLinear(t, page, values, maxDefinitionLevel, i, j)
				}
			}
		})
	}
}

func assertRepeatedPageSliceMatchesLinear(t *testing.T, page *repeatedPage, values []int64, maxDef byte, i, j int64) {
	t.Helper()

	got := page.Slice(i, j)
	wantRep, wantDef, wantValues, wantRows, wantNulls := linearSliceRepeatedPage(
		page.repetitionLevels,
		page.definitionLevels,
		values,
		maxDef,
		i,
		j,
	)
	if gotRows := got.NumRows(); gotRows != wantRows {
		t.Fatalf("Slice(%d, %d) NumRows = %d, want %d", i, j, gotRows, wantRows)
	}
	if gotNulls := got.NumNulls(); gotNulls != wantNulls {
		t.Fatalf("Slice(%d, %d) NumNulls = %d, want %d", i, j, gotNulls, wantNulls)
	}
	if gotRep := got.RepetitionLevels(); !slices.Equal(gotRep, wantRep) {
		t.Fatalf("Slice(%d, %d) repetition levels: %s", i, j, sliceMismatch(gotRep, wantRep))
	}
	if gotDef := got.DefinitionLevels(); !slices.Equal(gotDef, wantDef) {
		t.Fatalf("Slice(%d, %d) definition levels: %s", i, j, sliceMismatch(gotDef, wantDef))
	}
	data := got.Data()
	if gotValues := data.Int64(); !slices.Equal(gotValues, wantValues) {
		t.Fatalf("Slice(%d, %d) base values: %s", i, j, sliceMismatch(gotValues, wantValues))
	}
}

// linearSliceRepeatedPage is the scalar row scan that Slice used before the
// blocked lookup. Nulls are counted directly so the oracle does not share
// countLevelsEqual with findRowStart.
func linearSliceRepeatedPage(rep, def []byte, values []int64, maxDef byte, i, j int64) (repOut, defOut []byte, valueOut []int64, numRows, numNulls int64) {
	start := scalarFindRowStart(rep, i)
	end := scalarFindRowStart(rep, j)
	repOut = rep[start:end]
	defOut = def[start:end]
	nullsBefore := countDefinitionNulls(def[:start], maxDef)
	nullsHere := countDefinitionNulls(defOut, maxDef)
	valueOut = values[start-nullsBefore : end-nullsBefore-nullsHere]
	return repOut, defOut, valueOut, j - i, int64(nullsHere)
}

func countDefinitionNulls(levels []byte, maxDef byte) int {
	n := 0
	for _, level := range levels {
		if level != maxDef {
			n++
		}
	}
	return n
}

func sliceMismatch[T comparable](got, want []T) string {
	n := min(len(got), len(want))
	for i := range n {
		if got[i] != want[i] {
			return fmt.Sprintf("got len %d want len %d, index %d got %v want %v", len(got), len(want), i, got[i], want[i])
		}
	}
	return fmt.Sprintf("got len %d, want len %d", len(got), len(want))
}

func baseValuesForDefinition(def []byte, maxDef byte) []int64 {
	values := make([]int64, 0, len(def))
	for i, level := range def {
		if level == maxDef {
			values = append(values, int64(i))
		}
	}
	return values
}

func requireBoundaryRowStart(t *testing.T, rep []byte) {
	t.Helper()
	for b := repeatedRowBlockSize; b < len(rep); b += repeatedRowBlockSize {
		if rep[b] == 0 {
			return
		}
	}
	t.Fatal("no row starts on a 256-level block boundary")
}

// requireRowSpansFullBlock requires a row that starts before a 256-level
// block, covers every level of that block, and contains a null inside the
// block. The next row must start after the block, so the block is not a
// row by itself.
func requireRowSpansFullBlock(t *testing.T, rep, def []byte, maxDef byte) {
	t.Helper()
	for b := 0; b+repeatedRowBlockSize <= len(rep); b += repeatedRowBlockSize {
		continuation := true
		for _, level := range rep[b : b+repeatedRowBlockSize] {
			if level == 0 {
				continuation = false
				break
			}
		}
		if !continuation {
			continue
		}
		start := b
		for start > 0 && rep[start] != 0 {
			start--
		}
		if rep[start] != 0 || start == b {
			continue
		}
		end := b + repeatedRowBlockSize
		for end < len(rep) && rep[end] != 0 {
			end++
		}
		if end == b+repeatedRowBlockSize {
			continue
		}
		for _, level := range def[b : b+repeatedRowBlockSize] {
			if level != maxDef {
				return
			}
		}
	}
	t.Fatal("no row continues through a full 256-level block with a definition level below the max")
}

func newRepeatedLevels(n int) (rep, def []byte) {
	rep = slices.Repeat([]byte{1}, n)
	def = slices.Repeat([]byte{2}, n)
	return rep, def
}

func markRowStarts(rep []byte, starts ...int) {
	for _, start := range starts {
		rep[start] = 0
	}
}

func markContinuation(t *testing.T, rep []byte, indexes ...int) {
	t.Helper()
	for _, index := range indexes {
		if rep[index] == 0 {
			t.Fatalf("repetition level %d is a row start", index)
		}
		rep[index] = 2
	}
}

func mustLevel(t *testing.T, levels []byte, index int, want byte) {
	t.Helper()
	if levels[index] != want {
		t.Fatalf("level %d = %d, want %d", index, levels[index], want)
	}
}

// repeatedLevelsSpanningBlock builds a page where one row starts in the first
// block and runs through the whole second block and past it. Later rows start
// on block boundaries. Definition levels inside the spanned block are below
// the max, including both 0 and 1.
func repeatedLevelsSpanningBlock(t *testing.T) (rep, def []byte) {
	t.Helper()
	const n = repeatedRowBlockSize*4 + 80
	rep, def = newRepeatedLevels(n)

	for i := range 200 {
		rep[i] = 0
	}
	rep[200] = 0
	for i := 600; i <= 700; i++ {
		rep[i] = 0
	}
	markRowStarts(rep, 768, 900, 1024)
	markContinuation(t, rep, 201, 350, 500, 530, 901)

	def[10] = 0
	def[199] = 0
	def[200] = 0
	def[256] = 0
	def[257] = 1
	for i := 400; i <= 410; i++ {
		def[i] = 0
	}
	def[511] = 1
	def[520] = 0
	def[768] = 0
	def[1024] = 0
	def[1050] = 1

	mustLevel(t, rep, 200, 0)
	mustLevel(t, rep, 256, 1)
	mustLevel(t, rep, 511, 1)
	mustLevel(t, rep, 600, 0)
	mustLevel(t, rep, 768, 0)
	mustLevel(t, rep, 1024, 0)
	mustLevel(t, def, 256, 0)
	mustLevel(t, def, 257, 1)
	mustLevel(t, def, 520, 0)
	return rep, def
}

// repeatedLevelsOnBlockBoundaries starts rows on every block boundary, on the
// last level of the first block, and on the first level of the next block.
// One of those rows occupies exactly one block and contains nulls.
func repeatedLevelsOnBlockBoundaries(t *testing.T) (rep, def []byte) {
	t.Helper()
	const n = repeatedRowBlockSize*4 + 30
	rep, def = newRepeatedLevels(n)

	for i := range repeatedRowBlockSize {
		rep[i] = 0
	}
	markRowStarts(rep, 256, 300, 400, 512, 768, 900, 1024, 1040)
	markContinuation(t, rep, 257, 520, 901)

	def[10] = 0
	def[255] = 1
	def[256] = 0
	def[301] = 1
	def[450] = 0
	def[600] = 0
	def[767] = 1
	def[768] = 0
	def[1024] = 1
	def[1042] = 0

	mustLevel(t, rep, 255, 0)
	mustLevel(t, rep, 256, 0)
	mustLevel(t, rep, 512, 0)
	mustLevel(t, rep, 767, 1)
	mustLevel(t, rep, 768, 0)
	mustLevel(t, rep, 1024, 0)
	mustLevel(t, def, 600, 0)
	mustLevel(t, def, 767, 1)
	return rep, def
}

// repeatedLevelsLeadingContinuation starts with 300 continuation levels, so
// the first 256-level block contains no row start. The next row covers the
// whole block at 512 and continues past it. A later row starts on a block
// boundary.
func repeatedLevelsLeadingContinuation(t *testing.T) (rep, def []byte) {
	t.Helper()
	const n = repeatedRowBlockSize*4 + 40
	rep, def = newRepeatedLevels(n)

	rep[300] = 0
	rep[900] = 0
	rep[950] = 0
	for i := 1024; i < n; i++ {
		rep[i] = 0
	}
	markContinuation(t, rep, 301, 513, 801)

	def[0] = 0
	def[100] = 1
	def[256] = 0
	def[299] = 1
	def[400] = 0
	def[512] = 0
	def[600] = 1
	def[700] = 0
	def[800] = 1
	def[1024] = 0

	mustLevel(t, rep, 0, 1)
	mustLevel(t, rep, 299, 1)
	mustLevel(t, rep, 300, 0)
	mustLevel(t, rep, 512, 1)
	mustLevel(t, rep, 767, 1)
	mustLevel(t, rep, 900, 0)
	mustLevel(t, rep, 1024, 0)
	mustLevel(t, def, 256, 0)
	mustLevel(t, def, 512, 0)
	mustLevel(t, def, 600, 1)
	mustLevel(t, def, 800, 1)
	return rep, def
}

func BenchmarkFindRowStart(b *testing.B) {
	levels := make([]byte, 1024*1024)
	for i := range levels {
		levels[i] = 1
		if i%4 == 0 {
			levels[i] = 0
		}
	}

	implementations := []struct {
		name string
		fn   func([]byte, int64) int
	}{
		{"scalar", scalarFindRowStart},
		{"blocked", findRowStart},
	}
	for _, implementation := range implementations {
		for _, row := range []int64{0, 64, 1024, 262144} {
			b.Run(fmt.Sprintf("%s/row=%d", implementation.name, row), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					_ = implementation.fn(levels, row)
				}
			})
		}
	}
}
