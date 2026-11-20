package parquet_test

import (
	"bytes"
	"fmt"
	"slices"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
)

func TestBinaryColumnIndexMinMax(t *testing.T) {
	testCases := [][]any{
		// kind, type, page min, page max, size limit, [value to search, expected result]...
		{parquet.ByteArray, parquet.ByteArrayType,
			[]byte{0, 0, 0, 0, 0, 0}, []byte{1, 2, 3, 4, 5, 6}, 4,
			[]byte{0, 0, 0, 0, 0, 0}, true,
			[]byte{0, 1, 2, 3, 4, 5}, true,
			[]byte{1, 2, 3, 4}, true,
			[]byte{1, 2, 3, 4, 5, 6}, true, // the page max value should be a hit
			[]byte{1, 2, 3, 4, 5, 7}, true, // false positive due to size limit
			[]byte{1, 2, 3, 5}, true, // false positive due to size limit
			[]byte{1, 2, 3, 5, 6, 7}, false, // should be no hit since it definitely exceeds page max
			[]byte{2, 3, 4, 5}, false, // should be no hit since it definitely exceeds page max
		},
		{parquet.FixedLenByteArray, parquet.FixedLenByteArrayType(6),
			[]byte{0, 0, 0, 0, 0, 0}, []byte{1, 2, 3, 4, 5, 6}, 4,
			[]byte{0, 0, 0, 0, 0, 0}, true,
			[]byte{0, 1, 2, 3, 4, 5}, true,
			[]byte{1, 2, 3, 4, 0, 0}, true,
			[]byte{1, 2, 3, 4, 5, 6}, true, // the page max value should be a hit
			[]byte{1, 2, 3, 4, 5, 7}, true, // false positive due to size limit
			[]byte{1, 2, 3, 4, 0xFF, 0xFF}, true, // false positive due to size limit
			[]byte{1, 2, 3, 5, 0, 0}, false, // should be no hit since it definitely exceeds page max
			[]byte{1, 2, 3, 5, 6, 7}, false, // should be no hit since it definitely exceeds page max
			[]byte{2, 3, 4, 5, 0, 0}, false, // should be no hit since it definitely exceeds page max
		},
	}
	for _, testCase := range testCases {
		kind := testCase[0].(parquet.Kind)
		typ := testCase[1].(parquet.Type)
		min := testCase[2].([]byte)
		max := testCase[3].([]byte)
		sizeLimit := testCase[4].(int)
		indexer := typ.NewColumnIndexer(sizeLimit)
		indexer.IndexPage(100, 0,
			parquet.ValueOf(min),
			parquet.ValueOf(max),
		)
		formatIndex := indexer.ColumnIndex()
		columnIndex := parquet.NewColumnIndex(kind, &formatIndex)
		for i := 5; i < len(testCase); i += 2 {
			value := testCase[i].([]byte)
			expected := testCase[i+1].(bool)

			v := parquet.ValueOf(value)
			actual := parquet.Search(columnIndex, v, typ) == 0
			if actual != expected {
				t.Errorf("checkByteArrayMinMax(%v, %v, %v, %v) = %v, want %v", min, max, value, sizeLimit, actual, expected)
			}
		}
	}
}

func Test_ColumnIndexReuse(t *testing.T) {
	min := "a"
	max := "z"
	indexer := parquet.ByteArrayType.NewColumnIndexer(16)
	indexer.IndexPage(100, 0,
		parquet.ValueOf(min),
		parquet.ValueOf(max),
	)
	before := indexer.ColumnIndex()
	if len(before.NullPages) != 1 {
		t.Fatalf("expected 1 null page, got %d", len(before.NullPages))
	}
	if before.NullPages[0] {
		t.Fatalf("unexpected null page 0")
	}

	// Reset the indexer. Should be safe for reuse.
	indexer.Reset()

	// Index two pages that are both nul pages, expect the previous index to not have changed.
	indexer.IndexPage(100, 100,
		parquet.ValueOf(min),
		parquet.ValueOf(max),
	)
	indexer.IndexPage(10, 10,
		parquet.ValueOf(min),
		parquet.ValueOf(max),
	)
	after := indexer.ColumnIndex()

	if len(after.NullPages) != 2 {
		t.Fatalf("expected 2 null pages, got %d", len(after.NullPages))
	}
	if !after.NullPages[0] {
		t.Fatalf("expected null page 0")
	}
	if !after.NullPages[1] {
		t.Fatalf("expected null page 1")
	}

	// Validate null pages of the previous index.
	if len(before.NullPages) != 1 {
		t.Fatalf("expected 1 null page, got %d", len(before.NullPages))
	}
	if before.NullPages[0] {
		t.Fatalf("unexpected null page 0")
	}
}

func TestFixedLenByteArrayColumnIndexNullPages(t *testing.T) {
	tests := []struct {
		name       string
		pages      []byte // 0 is a null page, any other value fills a page
		rowGroups  int
		skipBounds bool
		order      format.BoundaryOrder
	}{
		{"leading nulls", []byte{0, 0, 1, 2}, 1, false, format.Ascending},
		{"null between pages", []byte{1, 0, 2}, 1, false, format.Ascending},
		{"skipped bounds", []byte{1, 0, 2}, 1, true, format.Unordered},
		{"skipped bounds single page", []byte{1}, 1, true, format.Unordered},
		{"second row group", []byte{1, 0, 2}, 2, false, format.Ascending},
	}

	// 16 bytes has its own column type and indexer.
	for _, size := range []int{2, 16} {
		for _, test := range tests {
			t.Run(fmt.Sprintf("size=%d/%s", size, test.name), func(t *testing.T) {
				schema := parquet.NewSchema("row", parquet.Group{
					"f": parquet.Optional(parquet.Leaf(parquet.FixedLenByteArrayType(size))),
				})
				options := []parquet.WriterOption{schema, parquet.PageBufferSize(1)}
				if test.skipBounds {
					options = append(options, parquet.SkipPageBounds("f"))
				}
				output := new(bytes.Buffer)
				w := parquet.NewWriter(output, options...)

				nullPages := make([]bool, len(test.pages))
				bounds := make([][]byte, len(test.pages))
				for rowGroup := range test.rowGroups {
					if rowGroup > 0 {
						if err := w.Flush(); err != nil {
							t.Fatal(err)
						}
					}
					for i, b := range test.pages {
						row := parquet.Row{parquet.NullValue()}
						if b != 0 {
							value := bytes.Repeat([]byte{b}, size)
							row = parquet.Row{parquet.FixedLenByteArrayValue(value).Level(0, 1, 0)}
							if !test.skipBounds {
								bounds[i] = value
							}
						}
						nullPages[i] = b == 0
						if _, err := w.WriteRows([]parquet.Row{row}); err != nil {
							t.Fatal(err)
						}
					}
				}
				if err := w.Close(); err != nil {
					t.Fatal(err)
				}

				f, err := parquet.OpenFile(bytes.NewReader(output.Bytes()), int64(output.Len()))
				if err != nil {
					t.Fatal(err)
				}
				if n := len(f.RowGroups()); n != test.rowGroups {
					t.Fatalf("%d row groups, want %d", n, test.rowGroups)
				}
				columnIndexes := f.ColumnIndexes()
				for rowGroup, written := range columnIndexes {
					if test.skipBounds {
						// The format requires bounds on every page which is not a
						// null page, so the index drops the page entries with them.
						if len(written.NullPages) != 0 || len(written.NullCounts) != 0 {
							t.Errorf("row group %d: %d null pages, %d null counts, want none without page bounds", rowGroup, len(written.NullPages), len(written.NullCounts))
						}
						if len(written.MinValues) != 0 || len(written.MaxValues) != 0 {
							t.Errorf("row group %d: bounds %x/%x, want none without page bounds", rowGroup, written.MinValues, written.MaxValues)
						}
						if written.BoundaryOrder != format.Unordered {
							t.Errorf("row group %d: boundary order %v, want %v", rowGroup, written.BoundaryOrder, format.Unordered)
						}
						continue
					}
					if !slices.Equal(written.NullPages, nullPages) {
						t.Errorf("row group %d: null pages %v, want %v", rowGroup, written.NullPages, nullPages)
					}
					if written.BoundaryOrder != test.order {
						t.Errorf("row group %d: boundary order %v, want %v", rowGroup, written.BoundaryOrder, test.order)
					}
					if !slices.EqualFunc(written.MinValues, bounds, bytes.Equal) {
						t.Errorf("row group %d: min values %x, want %x", rowGroup, written.MinValues, bounds)
					}
					if !slices.EqualFunc(written.MaxValues, bounds, bytes.Equal) {
						t.Errorf("row group %d: max values %x, want %x", rowGroup, written.MaxValues, bounds)
					}
				}

				if test.skipBounds {
					// Searching a column without bounds must not reach page bounds
					// which are not there: 16-byte values compare as be128 and read
					// past empty bounds, and the file-backed index derives its page
					// count from the null pages.
					fileIndex, err := f.RowGroups()[0].ColumnChunks()[0].ColumnIndex()
					if err != nil {
						t.Fatal(err)
					}
					memoryIndex := parquet.NewColumnIndex(parquet.FixedLenByteArray, &columnIndexes[0])
					for _, index := range []parquet.ColumnIndex{fileIndex, memoryIndex} {
						if n := index.NumPages(); n != 0 {
							t.Errorf("%T: %d pages, want 0 without page bounds", index, n)
						}
						for _, b := range []byte{1, 3} {
							value := parquet.FixedLenByteArrayValue(bytes.Repeat([]byte{b}, size))
							if n := parquet.Search(index, value, parquet.FixedLenByteArrayType(size)); n != index.NumPages() {
								t.Errorf("%T: search %x = %d, want %d", index, b, n, index.NumPages())
							}
						}
					}
					return
				}

				index, err := f.RowGroups()[0].ColumnChunks()[0].ColumnIndex()
				if err != nil {
					t.Fatal(err)
				}
				for i, want := range bounds {
					if want == nil {
						continue
					}
					if min := index.MinValue(i).ByteArray(); !bytes.Equal(min, want) {
						t.Errorf("page %d: min %x, want %x", i, min, want)
					}
					if max := index.MaxValue(i).ByteArray(); !bytes.Equal(max, want) {
						t.Errorf("page %d: max %x, want %x", i, max, want)
					}
				}
			})
		}
	}
}

func TestFixedLenByteArrayColumnIndexResetMissingBounds(t *testing.T) {
	for _, size := range []int{2, 16} {
		t.Run(fmt.Sprintf("size=%d", size), func(t *testing.T) {
			indexer := parquet.FixedLenByteArrayType(size).NewColumnIndexer(0)
			indexer.IndexPage(1, 0, parquet.NullValue(), parquet.NullValue())
			if index := indexer.ColumnIndex(); len(index.MinValues) != 0 || len(index.MaxValues) != 0 {
				t.Fatalf("bounds %x/%x, want none without page bounds", index.MinValues, index.MaxValues)
			}

			indexer.Reset()

			min := bytes.Repeat([]byte{1}, size)
			max := bytes.Repeat([]byte{2}, size)
			indexer.IndexPage(1, 0, parquet.FixedLenByteArrayValue(min), parquet.FixedLenByteArrayValue(max))
			indexer.IndexPage(1, 0, parquet.FixedLenByteArrayValue(max), parquet.FixedLenByteArrayValue(max))
			index := indexer.ColumnIndex()
			if !slices.EqualFunc(index.MinValues, [][]byte{min, max}, bytes.Equal) {
				t.Errorf("min values %x, want %x", index.MinValues, [][]byte{min, max})
			}
			if !slices.EqualFunc(index.MaxValues, [][]byte{max, max}, bytes.Equal) {
				t.Errorf("max values %x, want %x", index.MaxValues, [][]byte{max, max})
			}
			if index.BoundaryOrder != format.Ascending {
				t.Errorf("boundary order %v, want %v", index.BoundaryOrder, format.Ascending)
			}
		})
	}
}
