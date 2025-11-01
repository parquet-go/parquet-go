package parquet_test

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"testing"

	"github.com/parquet-go/parquet-go"
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
func TestSearchOverlappingBounds(t *testing.T) {
	type Row struct {
		Value string `parquet:"value"`
	}

	rows := make([]Row, 10000)
	for i := range rows {
		rows[i].Value = fmt.Sprintf("value_super_big_%v", i)
	}

	prng := rand.New(rand.NewSource(0))
	prng.Shuffle(len(rows), func(i, j int) {
		rows[i], rows[j] = rows[j], rows[i]
	})

	buffer := bytes.NewBuffer(nil)
	writer := parquet.NewSortingWriter[Row](buffer, 99,
		parquet.PageBufferSize(100),
		parquet.MaxRowsPerRowGroup(20000),
		parquet.ColumnIndexSizeLimit(5),
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
