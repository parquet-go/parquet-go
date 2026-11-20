package parquet_test

import (
	"bytes"
	"cmp"
	"fmt"
	"io"
	"math/rand"
	"slices"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
)

func TestSearchBinary(t *testing.T) {
	testSearch(t, [][]int32{
		{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		{10, 10, 10, 10},
		{21, 22, 24, 25, 30},
		{30, 30},
		{30, 31},
		{32},
		{42, 43, 44, 45, 46, 47, 48, 49},
	}, [][]int{
		{10, 1},
		{0, 0},
		{9, 0},
		// non-existant, but would be in this page
		{23, 2},
		// ensure we find the first page
		{30, 2},
		{31, 4},
		// out of bounds
		{99, 7},
		// out of bounds
		{-1, 7},
	})
}

func TestSearchLinear(t *testing.T) {
	testSearch(t, [][]int32{
		{10, 10, 10, 10},
		{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		{21, 22, 23, 24, 25},
		{19, 18, 17, 16, 14, 13, 12, 11},
		{42, 43, 44, 45, 46, 47, 48, 49},
	}, [][]int{
		{10, 0},
		{0, 1},
		{9, 1},
		{48, 4},
		// non-existant, but could be in this page
		{15, 3},
		// out of bounds
		{99, 5},
		// out of bounds
		{-1, 5},
	})
}

func testSearch(t *testing.T, pages [][]int32, expectIndex [][]int) {
	indexer := parquet.Int32Type.NewColumnIndexer(0)

	for _, values := range pages {
		min := values[0]
		max := values[0]

		for _, v := range values[1:] {
			switch {
			case v < min:
				min = v
			case v > max:
				max = v
			}
		}

		indexer.IndexPage(int64(len(values)), 0,
			parquet.ValueOf(min),
			parquet.ValueOf(max),
		)
	}

	formatIndex := indexer.ColumnIndex()
	columnIndex := parquet.NewColumnIndex(parquet.Int32, &formatIndex)

	for _, values := range expectIndex {
		v := parquet.ValueOf(values[0])
		j := parquet.Search(columnIndex, v, parquet.Int32Type)

		if values[1] != j {
			t.Errorf("searching for value %v: got=%d want=%d", v, j, values[1])
		}
	}
}

// TestSearchOverlappingBounds verifies that binary search correctly handles
// overlapping page bounds that occur when min/max values are truncated due to
// ColumnIndexSizeLimit. This test reproduces the bug fixed in PR #266.
//
// Note: This test uses GenericWriter with manually sorted rows instead of
// SortingWriter. SortingWriter has a bug where it creates invalid parquet files
// that cannot be read back. The issue is caused by shallow copying of slice fields
// in row group metadata (OffsetIndex.PageLocations, ColumnMetaData.EncodingStats,
// Statistics byte slices, etc.) when the writer is reused. When the writer is
// reset between intermediate row groups, it modifies slice headers to length 0
// via operations like `slice = slice[:0]`. These stale headers are then copied
// during the shallow copy, resulting in empty arrays in the serialized metadata,
// which causes thrift decoding errors when reading the file back.
func TestSearchOverlappingBounds(t *testing.T) {
	type Row struct {
		Value string `parquet:"value"`
	}

	rows := make([]Row, 10000)
	for i := range rows {
		rows[i].Value = fmt.Sprintf("value_super_big_%v", i)
	}

	// Shuffle rows to simulate unsorted data
	prng := rand.New(rand.NewSource(0))
	prng.Shuffle(len(rows), func(i, j int) {
		rows[i], rows[j] = rows[j], rows[i]
	})

	// Sort rows manually by value (instead of using SortingWriter due to bug mentioned above)
	slices.SortFunc(rows, func(a, b Row) int { return cmp.Compare(a.Value, b.Value) })

	buffer := bytes.NewBuffer(nil)
	writer := parquet.NewGenericWriter[Row](buffer,
		parquet.PageBufferSize(100),
		parquet.MaxRowsPerRowGroup(20000),
		parquet.ColumnIndexSizeLimit(func(path []string) int { return 5 }),
		parquet.SortingWriterConfig(
			parquet.SortingColumns(
				parquet.Ascending("value"),
			),
		),
	)

	_, err := writer.Write(rows)
	if err != nil {
		t.Fatal(err)
	}

	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	f, err := parquet.OpenFile(bytes.NewReader(buffer.Bytes()), int64(buffer.Len()))
	if err != nil {
		t.Fatal(err)
	}

	if len(f.RowGroups()) != 1 {
		t.Fatal("expected 1 row groups")
	}

	if len(f.RowGroups()[0].ColumnChunks()) != 1 {
		t.Fatal("expected 1 ColumnChunks")
	}

	ci, _ := f.RowGroups()[0].ColumnChunks()[0].ColumnIndex()
	found := parquet.Search(ci, parquet.ValueOf("value_super_big_0"), parquet.ByteArrayType)

	if found >= ci.NumPages() {
		t.Fatal("expected to find at least one row")
	}

	offset, err := f.RowGroups()[0].ColumnChunks()[0].OffsetIndex()
	if err != nil {
		t.Fatal(err)
	}
	rowFound := false

	pages := f.RowGroups()[0].ColumnChunks()[0].Pages()
	row := offset.FirstRowIndex(found)
	err = pages.SeekToRow(row)
	if err != nil {
		t.Fatal(err)
	}

	for {
		page, err := pages.ReadPage()
		if err == io.EOF {
			break
		}

		vr := page.Values()
		values := [100]parquet.Value{}

		for {
			n, err := vr.ReadValues(values[:])
			for _, value := range values[:n] {
				if value.String() == "value_super_big_0" {
					rowFound = true
				}
			}
			if err == io.EOF {
				break
			}
		}
	}

	if !rowFound {
		t.Fatal("expected to find row")
	}
}

// TestSearchBoundaryValues verifies that binary search correctly handles
// edge cases including values exactly at the truncation limit, values shorter
// than the limit, and exact boundary matches (value == min/max).
func TestSearchBoundaryValues(t *testing.T) {
	type Row struct {
		Value string `parquet:"value"`
	}

	tests := []struct {
		name        string
		sizeLimit   int
		values      []string
		searchValue string
		shouldFind  bool
		description string
	}{
		{
			name:        "values at truncation limit",
			sizeLimit:   5,
			values:      []string{"aaaaa", "bbbbb", "ccccc", "ddddd", "eeeee"},
			searchValue: "aaaaa",
			shouldFind:  true,
			description: "5-byte values with 5-byte limit (no truncation)",
		},
		{
			name:        "values below truncation limit",
			sizeLimit:   5,
			values:      []string{"aaa", "bbb", "ccc", "ddd", "eee"},
			searchValue: "aaa",
			shouldFind:  true,
			description: "3-byte values with 5-byte limit (no truncation)",
		},
		{
			name:        "mixed length values",
			sizeLimit:   5,
			values:      []string{"aa", "bbbbb", "cccccccc", "dd", "eeeeeeee"},
			searchValue: "aa",
			shouldFind:  true,
			description: "mix of short, exact, and long values",
		},
		{
			name:        "search for last value",
			sizeLimit:   5,
			values:      []string{"value_long_0", "value_long_1", "value_long_2"},
			searchValue: "value_long_2",
			shouldFind:  true,
			description: "search for last value with truncation",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows := make([]Row, len(tt.values))
			for i, v := range tt.values {
				rows[i].Value = v
			}

			buffer := bytes.NewBuffer(nil)
			sizeLimit := tt.sizeLimit
			writer := parquet.NewSortingWriter[Row](buffer, 99,
				parquet.PageBufferSize(100),
				parquet.MaxRowsPerRowGroup(20000),
				parquet.ColumnIndexSizeLimit(func(path []string) int { return sizeLimit }),
				parquet.SortingWriterConfig(
					parquet.SortingColumns(
						parquet.Ascending("value"),
					),
				),
			)

			_, err := writer.Write(rows)
			if err != nil {
				t.Fatalf("%s: write failed: %v", tt.description, err)
			}

			if err := writer.Close(); err != nil {
				t.Fatalf("%s: close failed: %v", tt.description, err)
			}

			f, err := parquet.OpenFile(bytes.NewReader(buffer.Bytes()), int64(buffer.Len()))
			if err != nil {
				t.Fatalf("%s: open failed: %v", tt.description, err)
			}

			if len(f.RowGroups()) == 0 {
				t.Fatalf("%s: no row groups", tt.description)
			}

			if len(f.RowGroups()[0].ColumnChunks()) == 0 {
				t.Fatalf("%s: no column chunks", tt.description)
			}

			ci, _ := f.RowGroups()[0].ColumnChunks()[0].ColumnIndex()
			found := parquet.Search(ci, parquet.ValueOf(tt.searchValue), parquet.ByteArrayType)

			if tt.shouldFind && found >= ci.NumPages() {
				t.Errorf("%s: expected to find value %q but got page index %d >= %d pages",
					tt.description, tt.searchValue, found, ci.NumPages())
			}

			if !tt.shouldFind && found < ci.NumPages() {
				t.Errorf("%s: expected not to find value %q but got page index %d",
					tt.description, tt.searchValue, found)
			}

			// Verify we can actually read the value from the found page
			if tt.shouldFind && found < ci.NumPages() {
				offset, err := f.RowGroups()[0].ColumnChunks()[0].OffsetIndex()
				if err != nil {
					t.Fatalf("%s: offset index failed: %v", tt.description, err)
				}

				pages := f.RowGroups()[0].ColumnChunks()[0].Pages()
				row := offset.FirstRowIndex(found)
				err = pages.SeekToRow(row)
				if err != nil {
					t.Fatalf("%s: seek failed: %v", tt.description, err)
				}

				rowFound := false
				for {
					page, err := pages.ReadPage()
					if err == io.EOF {
						break
					}
					if err != nil {
						t.Fatalf("%s: read page failed: %v", tt.description, err)
					}

					vr := page.Values()
					values := make([]parquet.Value, 100)

					for {
						n, err := vr.ReadValues(values)
						for _, value := range values[:n] {
							if value.String() == tt.searchValue {
								rowFound = true
							}
						}
						if err == io.EOF {
							break
						}
						if err != nil {
							t.Fatalf("%s: read values failed: %v", tt.description, err)
						}
					}

					if rowFound {
						break
					}
				}

				if !rowFound {
					t.Errorf("%s: found page index %d but couldn't read value %q from it",
						tt.description, found, tt.searchValue)
				}
			}
		})
	}
}

type testPageBounds struct {
	null      bool
	nullCount int64
	min       int32
	max       int32
}

func makeInt32ColumnIndex(pages []testPageBounds, order format.BoundaryOrder) parquet.ColumnIndex {
	indexer := parquet.Int32Type.NewColumnIndexer(0)
	for _, p := range pages {
		if p.null {
			indexer.IndexPage(1, 1, parquet.Value{}, parquet.Value{})
		} else {
			indexer.IndexPage(1+p.nullCount, p.nullCount, parquet.ValueOf(p.min), parquet.ValueOf(p.max))
		}
	}
	fi := indexer.ColumnIndex()
	fi.BoundaryOrder = order
	return parquet.NewColumnIndex(parquet.Int32, &fi)
}

func TestSearchAscendingNullPages(t *testing.T) {
	nullPage := testPageBounds{null: true}
	page := func(min, max int32) testPageBounds {
		return testPageBounds{min: min, max: max}
	}

	type query struct {
		value parquet.Value
		want  int
	}

	tests := []struct {
		name    string
		pages   []testPageBounds
		queries []query
	}{
		{
			name:  "leading null",
			pages: []testPageBounds{nullPage, page(1, 1), page(2, 2)},
			queries: []query{
				{parquet.ValueOf(int32(0)), 3},
				{parquet.ValueOf(int32(1)), 1},
				{parquet.ValueOf(int32(2)), 2},
				{parquet.ValueOf(int32(3)), 3},
				{parquet.Value{}, 0},
			},
		},
		{
			name:  "two leading nulls",
			pages: []testPageBounds{nullPage, nullPage, page(1, 1), page(2, 2)},
			queries: []query{
				{parquet.ValueOf(int32(0)), 4},
				{parquet.ValueOf(int32(1)), 2},
				{parquet.ValueOf(int32(2)), 3},
				{parquet.ValueOf(int32(3)), 4},
				{parquet.Value{}, 0},
			},
		},
		{
			name:  "intervening null",
			pages: []testPageBounds{page(1, 1), nullPage, page(2, 2)},
			queries: []query{
				{parquet.ValueOf(int32(0)), 3},
				{parquet.ValueOf(int32(1)), 0},
				{parquet.ValueOf(int32(2)), 2},
				{parquet.ValueOf(int32(3)), 3},
				{parquet.Value{}, 1},
			},
		},
		{
			name:  "two intervening nulls",
			pages: []testPageBounds{page(1, 1), nullPage, nullPage, page(3, 3)},
			queries: []query{
				{parquet.ValueOf(int32(0)), 4},
				{parquet.ValueOf(int32(1)), 0},
				{parquet.ValueOf(int32(2)), 4},
				{parquet.ValueOf(int32(3)), 3},
				{parquet.ValueOf(int32(4)), 4},
				{parquet.Value{}, 1},
			},
		},
		{
			name:  "trailing null",
			pages: []testPageBounds{page(1, 1), page(2, 2), nullPage},
			queries: []query{
				{parquet.ValueOf(int32(0)), 3},
				{parquet.ValueOf(int32(1)), 0},
				{parquet.ValueOf(int32(2)), 1},
				{parquet.ValueOf(int32(3)), 3},
				{parquet.Value{}, 2},
			},
		},
		{
			name:  "mixed nulls",
			pages: []testPageBounds{nullPage, page(1, 1), nullPage, page(3, 3), nullPage},
			queries: []query{
				{parquet.ValueOf(int32(0)), 5},
				{parquet.ValueOf(int32(1)), 1},
				{parquet.ValueOf(int32(2)), 5},
				{parquet.ValueOf(int32(3)), 3},
				{parquet.ValueOf(int32(4)), 5},
				{parquet.Value{}, 0},
			},
		},
		{
			name:  "overlapping bounds across null",
			pages: []testPageBounds{page(1, 3), nullPage, page(2, 4)},
			queries: []query{
				{parquet.ValueOf(int32(0)), 3},
				{parquet.ValueOf(int32(1)), 0},
				{parquet.ValueOf(int32(2)), 0},
				{parquet.ValueOf(int32(3)), 0},
				{parquet.ValueOf(int32(4)), 2},
				{parquet.ValueOf(int32(5)), 3},
				{parquet.Value{}, 1},
			},
		},
		{
			name:  "all nulls",
			pages: []testPageBounds{nullPage, nullPage},
			queries: []query{
				{parquet.ValueOf(int32(1)), 2},
				{parquet.Value{}, 0},
			},
		},
		{
			name:  "no nulls searching null",
			pages: []testPageBounds{page(1, 1), page(2, 2)},
			queries: []query{
				{parquet.Value{}, 2},
			},
		},
	}

	cmps := []struct {
		name string
		fn   func(parquet.ColumnIndex, parquet.Value) int
	}{
		{
			name: "Search(CompareNullsLast)",
			fn: func(ci parquet.ColumnIndex, v parquet.Value) int {
				return parquet.Search(ci, v, parquet.Int32Type)
			},
		},
		{
			name: "Find(CompareNullsFirst)",
			fn: func(ci parquet.ColumnIndex, v parquet.Value) int {
				return parquet.Find(ci, v, parquet.CompareNullsFirst(parquet.Int32Type.Compare))
			},
		},
	}

	for _, tt := range tests {
		ci := makeInt32ColumnIndex(tt.pages, format.Ascending)
		if !ci.IsAscending() {
			t.Fatalf("%s: expected ascending column index", tt.name)
		}
		for _, c := range cmps {
			for _, q := range tt.queries {
				got := c.fn(ci, q.value)
				if got != q.want {
					t.Errorf("%s/%s(%v): got=%d want=%d", tt.name, c.name, q.value, got, q.want)
				}
			}
		}
	}
}

// TestSearchNullValueInMixedPages verifies that a null search finds pages
// which contain nulls next to non-null values. Page bounds exclude nulls, so
// the bounds alone cannot tell whether a page holds any.
func TestSearchNullValueInMixedPages(t *testing.T) {
	mixedPage := testPageBounds{min: 1, max: 1, nullCount: 1}
	nullPage := testPageBounds{null: true}

	tests := []struct {
		name  string
		pages []testPageBounds
	}{
		{name: "mixed then null page", pages: []testPageBounds{mixedPage, nullPage}},
		{name: "mixed page only", pages: []testPageBounds{mixedPage}},
	}

	for _, tt := range tests {
		for _, order := range []format.BoundaryOrder{format.Ascending, format.Descending, format.Unordered} {
			ci := makeInt32ColumnIndex(tt.pages, order)

			t.Run(tt.name+"/"+order.String(), func(t *testing.T) {
				if got := parquet.Search(ci, parquet.Value{}, parquet.Int32Type); got != 0 {
					t.Errorf("Search(null): got=%d want=0", got)
				}
				if got := parquet.Find(ci, parquet.Value{}, parquet.CompareNullsFirst(parquet.Int32Type.Compare)); got != 0 {
					t.Errorf("Find(null, CompareNullsFirst): got=%d want=0", got)
				}
			})
		}
	}
}

// TestSearchNullPageBoundsIgnored verifies that linear search does not match an
// all-null page through the zero Value its bounds report. A comparator without
// null ordering reads that zero as int32(0).
func TestSearchNullPageBoundsIgnored(t *testing.T) {
	for _, order := range []format.BoundaryOrder{format.Descending, format.Unordered} {
		ci := makeInt32ColumnIndex([]testPageBounds{{null: true}, {min: 0, max: 0}}, order)

		if got := parquet.Find(ci, parquet.ValueOf(int32(0)), parquet.Int32Type.Compare); got != 1 {
			t.Errorf("%v: searching 0: got=%d want=1", order, got)
		}
	}
}

// nullBoundsColumnIndex reports page 0 as an all-null page while still exposing
// non-null bounds for it, which the built-in indexes never do.
type nullBoundsColumnIndex struct{ parquet.ColumnIndex }

func (i nullBoundsColumnIndex) NullPage(j int) bool { return j == 0 }

func TestSearchNullPageWithoutNullBounds(t *testing.T) {
	base := makeInt32ColumnIndex([]testPageBounds{{min: 5, max: 5}, {min: 6, max: 6}}, format.Unordered)
	ci := nullBoundsColumnIndex{ColumnIndex: base}

	if got := parquet.Find(ci, parquet.ValueOf(int32(5)), parquet.Int32Type.Compare); got != 2 {
		t.Errorf("non-null search matched the null page: got=%d want=2", got)
	}
	if got := parquet.Search(ci, parquet.Value{}, parquet.Int32Type); got != 0 {
		t.Errorf("null search: got=%d want=0", got)
	}
}

func TestSearchEmptyColumnIndex(t *testing.T) {
	ci := makeInt32ColumnIndex(nil, format.Unordered)
	if n := ci.NumPages(); n != 0 {
		t.Fatalf("expected an empty column index, got %d pages", n)
	}

	if got := parquet.Search(ci, parquet.ValueOf(int32(1)), parquet.Int32Type); got != 0 {
		t.Errorf("Search(1): got=%d want=0", got)
	}
	if got := parquet.Search(ci, parquet.Value{}, parquet.Int32Type); got != 0 {
		t.Errorf("Search(null): got=%d want=0", got)
	}
}

func TestSearchAscendingNullPagesExhaustive(t *testing.T) {
	for n := 1; n <= 8; n++ {
		for mask := 0; mask < (1 << n); mask++ {
			for _, overlap := range []bool{false, true} {
				pages := make([]testPageBounds, n)
				val := int32(1)
				for i := range n {
					if mask&(1<<i) != 0 {
						pages[i] = testPageBounds{null: true}
					} else if overlap {
						pages[i] = testPageBounds{min: val, max: val + 2}
						val += 2
					} else {
						pages[i] = testPageBounds{min: val, max: val + 1}
						val += 3
					}
				}

				asc := makeInt32ColumnIndex(pages, format.Ascending)
				unord := makeInt32ColumnIndex(pages, format.Unordered)

				queries := make([]parquet.Value, 0, val+3)
				queries = append(queries, parquet.Value{})
				for v := int32(0); v <= val+1; v++ {
					queries = append(queries, parquet.ValueOf(v))
				}

				for _, q := range queries {
					wantLast := parquet.Search(unord, q, parquet.Int32Type)
					gotLast := parquet.Search(asc, q, parquet.Int32Type)
					if gotLast != wantLast {
						t.Fatalf("n=%d mask=%0*b overlap=%v Search(%v): got=%d want=%d",
							n, n, mask, overlap, q, gotLast, wantLast)
					}

					cmpFirst := parquet.CompareNullsFirst(parquet.Int32Type.Compare)
					wantFirst := parquet.Find(unord, q, cmpFirst)
					gotFirst := parquet.Find(asc, q, cmpFirst)
					if gotFirst != wantFirst {
						t.Fatalf("n=%d mask=%0*b overlap=%v Find(NullsFirst, %v): got=%d want=%d",
							n, n, mask, overlap, q, gotFirst, wantFirst)
					}
				}
			}
		}
	}
}

func TestSearchNullValueAbsentNullCounts(t *testing.T) {
	makeIndex := func(pages []testPageBounds, order format.BoundaryOrder, omitNullCounts bool) parquet.ColumnIndex {
		indexer := parquet.Int32Type.NewColumnIndexer(0)
		for _, p := range pages {
			if p.null {
				indexer.IndexPage(1, 1, parquet.Value{}, parquet.Value{})
			} else {
				indexer.IndexPage(2, p.nullCount, parquet.ValueOf(p.min), parquet.ValueOf(p.max))
			}
		}
		fi := indexer.ColumnIndex()
		fi.BoundaryOrder = order
		if omitNullCounts {
			fi.NullCounts = nil
		}
		return parquet.NewColumnIndex(parquet.Int32, &fi)
	}

	tests := []struct {
		name           string
		pages          []testPageBounds
		omitNullCounts bool
		want           int
	}{
		{
			name:           "single mixed page without null counts",
			pages:          []testPageBounds{{min: 1, max: 2}},
			omitNullCounts: true,
			want:           0,
		},
		{
			name:           "mixed page before all-null page without null counts",
			pages:          []testPageBounds{{min: 1, max: 2}, {null: true}},
			omitNullCounts: true,
			want:           0,
		},
		{
			name:           "control with null counts skips zero-null page",
			pages:          []testPageBounds{{min: 1, max: 2, nullCount: 0}, {min: 3, max: 4, nullCount: 1}},
			omitNullCounts: false,
			want:           1,
		},
		{
			name:           "control with all zero null counts returns NumPages",
			pages:          []testPageBounds{{min: 1, max: 2, nullCount: 0}, {min: 3, max: 4, nullCount: 0}},
			omitNullCounts: false,
			want:           2,
		},
		{
			name:           "empty index without null counts",
			pages:          nil,
			omitNullCounts: true,
			want:           0,
		},
	}

	for _, tt := range tests {
		for _, order := range []format.BoundaryOrder{format.Ascending, format.Descending, format.Unordered} {
			ci := makeIndex(tt.pages, order, tt.omitNullCounts)

			t.Run(tt.name+"/"+order.String(), func(t *testing.T) {
				if got := parquet.Search(ci, parquet.Value{}, parquet.Int32Type); got != tt.want {
					t.Errorf("Search(null): got=%d want=%d", got, tt.want)
				}
				if got := parquet.Find(ci, parquet.Value{}, parquet.CompareNullsFirst(parquet.Int32Type.Compare)); got != tt.want {
					t.Errorf("Find(null, CompareNullsFirst): got=%d want=%d", got, tt.want)
				}
			})
		}
	}
}
