package parquet_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
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
		name         string
		pages        []byte // 0 is a null page, any other value fills a page
		rowGroups    int
		skipBounds   bool
		omitColIndex bool
		order        format.BoundaryOrder
	}{
		{"leading nulls", []byte{0, 0, 1, 2}, 1, false, false, format.Ascending},
		{"null between pages", []byte{1, 0, 2}, 1, false, false, format.Ascending},
		{"all nulls", []byte{0, 0}, 1, false, false, format.Unordered},
		{"all nulls skipped bounds", []byte{0, 0}, 1, true, false, format.Unordered},
		{"skipped bounds", []byte{1, 0, 2}, 1, true, true, format.Unordered},
		{"skipped bounds single page", []byte{1}, 1, true, true, format.Unordered},
		{"second row group", []byte{1, 0, 2}, 2, false, false, format.Ascending},
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

				data := output.Bytes()
				f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
				if err != nil {
					t.Fatal(err)
				}
				fLazy, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)), parquet.SkipPageIndex(true))
				if err != nil {
					t.Fatal(err)
				}
				if n := len(f.RowGroups()); n != test.rowGroups {
					t.Fatalf("%d row groups, want %d", n, test.rowGroups)
				}

				if test.omitColIndex {
					if n := len(f.ColumnIndexes()); n != 0 {
						t.Errorf("ColumnIndexes() has %d entries, want 0", n)
					}
					for rowGroup := range test.rowGroups {
						colMeta := f.Metadata().RowGroups[rowGroup].Columns[0]
						if colMeta.ColumnIndexOffset != 0 || colMeta.ColumnIndexLength != 0 {
							t.Errorf("row group %d: column index offset=%d length=%d, want 0/0", rowGroup, colMeta.ColumnIndexOffset, colMeta.ColumnIndexLength)
						}
						for _, opened := range []*parquet.File{f, fLazy} {
							chunk := opened.RowGroups()[rowGroup].ColumnChunks()[0]
							if _, err := chunk.ColumnIndex(); !errors.Is(err, parquet.ErrMissingColumnIndex) {
								t.Errorf("row group %d: ColumnIndex() error = %v, want ErrMissingColumnIndex", rowGroup, err)
							}
							oi, err := chunk.OffsetIndex()
							if err != nil {
								t.Fatalf("row group %d: OffsetIndex(): %v", rowGroup, err)
							}
							if oi.NumPages() != len(test.pages) {
								t.Errorf("row group %d: OffsetIndex().NumPages() = %d, want %d", rowGroup, oi.NumPages(), len(test.pages))
							}
						}
					}

					// Verify offset index seeking and row reading still work when
					// all column indexes are omitted.
					rg := f.RowGroups()[0]
					rows := rg.Rows()
					defer rows.Close()
					targetRow := int64(len(test.pages) - 1)
					if err := rows.SeekToRow(targetRow); err != nil {
						t.Fatalf("SeekToRow(%d): %v", targetRow, err)
					}
					buf := make([]parquet.Row, 1)
					n, err := rows.ReadRows(buf)
					if err != nil && !errors.Is(err, io.EOF) {
						t.Fatalf("ReadRows after SeekToRow(%d): %v", targetRow, err)
					}
					if n != 1 {
						t.Fatalf("ReadRows after SeekToRow(%d) = %d rows, want 1", targetRow, n)
					}
					wantVal := bytes.Repeat([]byte{test.pages[targetRow]}, size)
					if got := buf[0][0].ByteArray(); !bytes.Equal(got, wantVal) {
						t.Errorf("row %d: got %x, want %x", targetRow, got, wantVal)
					}

					// Searching an empty in-memory ColumnIndex must not panic or
					// read past empty bounds.
					memoryIndex := parquet.NewColumnIndex(parquet.FixedLenByteArray, &format.ColumnIndex{})
					if n := memoryIndex.NumPages(); n != 0 {
						t.Errorf("memoryIndex: %d pages, want 0", n)
					}
					for _, b := range []byte{1, 3} {
						value := parquet.FixedLenByteArrayValue(bytes.Repeat([]byte{b}, size))
						if n := parquet.Search(memoryIndex, value, parquet.FixedLenByteArrayType(size)); n != 0 {
							t.Errorf("memoryIndex: search %x = %d, want 0", b, n)
						}
					}
					return
				}

				columnIndexes := f.ColumnIndexes()
				if len(columnIndexes) != test.rowGroups {
					t.Fatalf("ColumnIndexes() has %d entries, want %d", len(columnIndexes), test.rowGroups)
				}
				for rowGroup, written := range columnIndexes {
					colMeta := f.Metadata().RowGroups[rowGroup].Columns[0]
					if colMeta.ColumnIndexOffset <= 0 || colMeta.ColumnIndexLength <= 0 {
						t.Errorf("row group %d: column index offset=%d length=%d, want >0", rowGroup, colMeta.ColumnIndexOffset, colMeta.ColumnIndexLength)
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

func TestPartialColumnIndexMultiColumn(t *testing.T) {
	schema := parquet.NewSchema("row", parquet.Group{
		"f":  parquet.Optional(parquet.Leaf(parquet.FixedLenByteArrayType(16))),
		"id": parquet.Leaf(parquet.Int32Type),
	})
	output := new(bytes.Buffer)
	w := parquet.NewWriter(output, schema, parquet.PageBufferSize(1), parquet.SkipPageBounds("f"))
	for i := range int32(3) {
		val := bytes.Repeat([]byte{byte(i + 1)}, 16)
		row := parquet.Row{
			parquet.FixedLenByteArrayValue(val).Level(0, 1, 0),
			parquet.Int32Value((i+1)*10).Level(0, 0, 1),
		}
		if _, err := w.WriteRows([]parquet.Row{row}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	data := output.Bytes()
	fDefault, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	fLazy, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)), parquet.SkipPageIndex(true))
	if err != nil {
		t.Fatal(err)
	}

	colMeta := fDefault.Metadata().RowGroups[0].Columns
	if colMeta[0].ColumnIndexOffset != 0 || colMeta[0].ColumnIndexLength != 0 {
		t.Fatalf("col 0: column index offset=%d length=%d, want 0/0", colMeta[0].ColumnIndexOffset, colMeta[0].ColumnIndexLength)
	}
	if colMeta[1].ColumnIndexOffset <= 0 || colMeta[1].ColumnIndexLength <= 0 {
		t.Fatalf("col 1: column index offset=%d length=%d, want >0", colMeta[1].ColumnIndexOffset, colMeta[1].ColumnIndexLength)
	}

	rawCI, rawOI, err := fDefault.ReadPageIndex()
	if err != nil {
		t.Fatalf("ReadPageIndex: %v", err)
	}
	if len(rawCI) != 2 || len(rawOI) != 2 {
		t.Fatalf("ReadPageIndex lengths = %d/%d, want 2/2", len(rawCI), len(rawOI))
	}
	if len(rawCI[0].NullPages) != 0 {
		t.Errorf("ReadPageIndex col 0 NullPages = %v, want empty", rawCI[0].NullPages)
	}
	if len(rawCI[1].NullPages) != 3 {
		t.Errorf("ReadPageIndex col 1 NullPages = %d, want 3", len(rawCI[1].NullPages))
	}

	for name, f := range map[string]*parquet.File{"default": fDefault, "lazy": fLazy} {
		t.Run(name, func(t *testing.T) {
			chunks := f.RowGroups()[0].ColumnChunks()
			if _, err := chunks[0].ColumnIndex(); !errors.Is(err, parquet.ErrMissingColumnIndex) {
				t.Errorf("col 0 ColumnIndex() = %v, want ErrMissingColumnIndex", err)
			}
			ci, err := chunks[1].ColumnIndex()
			if err != nil {
				t.Fatalf("col 1 ColumnIndex(): %v", err)
			}
			if ci.NumPages() != 3 {
				t.Fatalf("col 1 NumPages() = %d, want 3", ci.NumPages())
			}
			for page := range 3 {
				want := int32((page + 1) * 10)
				if got := ci.MinValue(page).Int32(); got != want {
					t.Errorf("page %d min = %d, want %d", page, got, want)
				}
				if got := ci.MaxValue(page).Int32(); got != want {
					t.Errorf("page %d max = %d, want %d", page, got, want)
				}
			}
			if got := parquet.Search(ci, parquet.Int32Value(20), parquet.Int32Type); got != 1 {
				t.Errorf("Search(20) = %d, want 1", got)
			}
		})
	}
}

func TestPartialColumnIndexMultiRowGroup(t *testing.T) {
	schema := parquet.NewSchema("row", parquet.Group{
		"f": parquet.Optional(parquet.Leaf(parquet.FixedLenByteArrayType(16))),
	})
	output := new(bytes.Buffer)
	w := parquet.NewWriter(output, schema, parquet.PageBufferSize(1), parquet.SkipPageBounds("f"))

	// RG 0: non-null value with SkipPageBounds -> no column index written.
	val := bytes.Repeat([]byte{7}, 16)
	if _, err := w.WriteRows([]parquet.Row{{parquet.FixedLenByteArrayValue(val).Level(0, 1, 0)}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	// RG 1: all-null page -> valid column index with NullPages=[true].
	if _, err := w.WriteRows([]parquet.Row{{parquet.NullValue()}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	data := output.Bytes()
	fDefault, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	fLazy, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)), parquet.SkipPageIndex(true))
	if err != nil {
		t.Fatal(err)
	}

	for name, f := range map[string]*parquet.File{"default": fDefault, "lazy": fLazy} {
		t.Run(name, func(t *testing.T) {
			rgs := f.RowGroups()
			if len(rgs) != 2 {
				t.Fatalf("got %d row groups, want 2", len(rgs))
			}
			if _, err := rgs[0].ColumnChunks()[0].ColumnIndex(); !errors.Is(err, parquet.ErrMissingColumnIndex) {
				t.Errorf("rg 0 ColumnIndex() = %v, want ErrMissingColumnIndex", err)
			}
			ci, err := rgs[1].ColumnChunks()[0].ColumnIndex()
			if err != nil {
				t.Fatalf("rg 1 ColumnIndex(): %v", err)
			}
			if ci.NumPages() != 1 || !ci.NullPage(0) {
				t.Errorf("rg 1 ColumnIndex: NumPages=%d NullPage(0)=%v, want 1/true", ci.NumPages(), ci.NullPage(0))
			}
		})
	}
}

func TestPartialColumnIndexEncryptedMultiColumn(t *testing.T) {
	footerKey := aes128Key(0x11)
	colKey := aes128Key(0x22)

	configs := []struct {
		name string
		cfg  *parquet.EncryptionConfig
		keys parquet.KeyRetriever
	}{
		{
			name: "footer key",
			cfg: &parquet.EncryptionConfig{
				FooterKey: footerKey,
			},
			keys: &staticKeyRetriever{footerKey: footerKey},
		},
		{
			name: "column key",
			cfg: &parquet.EncryptionConfig{
				FooterKey:  footerKey,
				ColumnKeys: map[string][]byte{"id": colKey},
			},
			keys: &staticKeyRetriever{
				footerKey:  footerKey,
				columnKeys: map[string][]byte{"id": colKey},
			},
		},
	}

	schema := parquet.NewSchema("row", parquet.Group{
		"f":  parquet.Optional(parquet.Leaf(parquet.FixedLenByteArrayType(16))),
		"id": parquet.Leaf(parquet.Int32Type),
	})

	for _, tc := range configs {
		t.Run(tc.name, func(t *testing.T) {
			output := new(bytes.Buffer)
			w := parquet.NewWriter(output, schema,
				parquet.PageBufferSize(1),
				parquet.SkipPageBounds("f"),
				parquet.WithEncryption(tc.cfg),
			)
			for i := range int32(3) {
				val := bytes.Repeat([]byte{byte(i + 1)}, 16)
				row := parquet.Row{
					parquet.FixedLenByteArrayValue(val).Level(0, 1, 0),
					parquet.Int32Value((i+1)*100).Level(0, 0, 1),
				}
				if _, err := w.WriteRows([]parquet.Row{row}); err != nil {
					t.Fatal(err)
				}
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}

			data := output.Bytes()
			fDefault, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)), parquet.WithDecryption(tc.keys))
			if err != nil {
				t.Fatal(err)
			}
			fLazy, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)), parquet.WithDecryption(tc.keys), parquet.SkipPageIndex(true))
			if err != nil {
				t.Fatal(err)
			}

			for mode, f := range map[string]*parquet.File{"default": fDefault, "lazy": fLazy} {
				t.Run(mode, func(t *testing.T) {
					chunks := f.RowGroups()[0].ColumnChunks()
					if _, err := chunks[0].ColumnIndex(); !errors.Is(err, parquet.ErrMissingColumnIndex) {
						t.Errorf("col 0 ColumnIndex() = %v, want ErrMissingColumnIndex", err)
					}
					ci, err := chunks[1].ColumnIndex()
					if err != nil {
						t.Fatalf("col 1 ColumnIndex(): %v", err)
					}
					if ci.NumPages() != 3 {
						t.Fatalf("col 1 NumPages() = %d, want 3", ci.NumPages())
					}
					if got := ci.MinValue(1).Int32(); got != 200 {
						t.Errorf("page 1 min = %d, want 200", got)
					}
					if got := ci.MaxValue(2).Int32(); got != 300 {
						t.Errorf("page 2 max = %d, want 300", got)
					}
				})
			}
		})
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
