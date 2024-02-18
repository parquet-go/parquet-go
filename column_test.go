package parquet_test

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/google/uuid"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/deprecated"
	"github.com/parquet-go/parquet-go/format"
)

func TestColumnPageIndex(t *testing.T) {
	for _, config := range [...]struct {
		name string
		test func(*testing.T, rows) bool
	}{
		{
			name: "buffer",
			test: testColumnPageIndexWithBuffer,
		},
		{
			name: "file",
			test: testColumnPageIndexWithFile,
		},
	} {
		t.Run(config.name, func(t *testing.T) {
			for _, test := range [...]struct {
				scenario string
				function func(*testing.T) interface{}
			}{
				{
					scenario: "boolean",
					function: func(t *testing.T) interface{} {
						return func(rows []struct{ Value bool }) bool { return config.test(t, makeRows(rows)) }
					},
				},

				{
					scenario: "int32",
					function: func(t *testing.T) interface{} {
						return func(rows []struct{ Value int32 }) bool { return config.test(t, makeRows(rows)) }
					},
				},

				{
					scenario: "int64",
					function: func(t *testing.T) interface{} {
						return func(rows []struct{ Value int64 }) bool { return config.test(t, makeRows(rows)) }
					},
				},

				{
					scenario: "int96",
					function: func(t *testing.T) interface{} {
						return func(rows []struct{ Value deprecated.Int96 }) bool { return config.test(t, makeRows(rows)) }
					},
				},

				{
					scenario: "uint32",
					function: func(t *testing.T) interface{} {
						return func(rows []struct{ Value uint32 }) bool { return config.test(t, makeRows(rows)) }
					},
				},

				{
					scenario: "uint64",
					function: func(t *testing.T) interface{} {
						return func(rows []struct{ Value uint64 }) bool { return config.test(t, makeRows(rows)) }
					},
				},

				{
					scenario: "float32",
					function: func(t *testing.T) interface{} {
						return func(rows []struct{ Value float32 }) bool { return config.test(t, makeRows(rows)) }
					},
				},

				{
					scenario: "float64",
					function: func(t *testing.T) interface{} {
						return func(rows []struct{ Value float64 }) bool { return config.test(t, makeRows(rows)) }
					},
				},

				{
					scenario: "string",
					function: func(t *testing.T) interface{} {
						return func(rows []struct{ Value string }) bool { return config.test(t, makeRows(rows)) }
					},
				},

				{
					scenario: "uuid",
					function: func(t *testing.T) interface{} {
						return func(rows []struct{ Value uuid.UUID }) bool { return config.test(t, makeRows(rows)) }
					},
				},
			} {
				t.Run(test.scenario, func(t *testing.T) {
					if err := quickCheck(test.function(t)); err != nil {
						t.Error(err)
					}
				})
			}
		})
	}
}

func testColumnPageIndexWithBuffer(t *testing.T, rows rows) bool {
	if len(rows) > 0 {
		b := parquet.NewBuffer()
		for _, row := range rows {
			b.Write(row)
		}
		if err := checkRowGroupColumnIndex(b); err != nil {
			t.Error(err)
			return false
		}
		if err := checkRowGroupOffsetIndex(b); err != nil {
			t.Error(err)
			return false
		}
	}
	return true
}

func checkRowGroupColumnIndex(rowGroup parquet.RowGroup) error {
	for i, column := range rowGroup.ColumnChunks() {
		if err := checkColumnChunkColumnIndex(column); err != nil {
			return fmt.Errorf("column chunk @i=%d: %w", i, err)
		}
	}
	return nil
}

func checkColumnChunkColumnIndex(columnChunk parquet.ColumnChunk) error {
	columnType := columnChunk.Type()
	columnIndex, _ := columnChunk.ColumnIndex()
	numPages := columnIndex.NumPages()
	pagesRead := 0
	stats := newColumnStats(columnType)
	pages := columnChunk.Pages()
	defer pages.Close()

	err := forEachPage(pages, func(page parquet.Page) error {
		pageMin, pageMax, hasBounds := page.Bounds()
		if !hasBounds {
			return fmt.Errorf("page bounds are missing")
		}
		indexMin := columnIndex.MinValue(pagesRead)
		indexMax := columnIndex.MaxValue(pagesRead)

		if !parquet.Equal(pageMin, indexMin) {
			return fmt.Errorf("max page value mismatch: index=%q page=%q", indexMin, pageMin)
		}
		if !parquet.Equal(pageMax, indexMax) {
			return fmt.Errorf("max page value mismatch: index=%q page=%q", indexMax, pageMax)
		}

		numNulls := int64(0)
		numValues := int64(0)
		err := forEachValue(page.Values(), func(value parquet.Value) error {
			stats.observe(value)
			if value.IsNull() {
				numNulls++
			}
			numValues++
			return nil
		})
		if err != nil {
			return err
		}

		nullCount := columnIndex.NullCount(pagesRead)
		if numNulls != nullCount {
			return fmt.Errorf("number of null values mimatch: index=%d page=%d", nullCount, numNulls)
		}

		nullPage := columnIndex.NullPage(pagesRead)
		if numNulls > 0 && numNulls == numValues && !nullPage {
			return fmt.Errorf("page only contained null values but the index did not categorize it as a null page: nulls=%d", numNulls)
		}

		stats.pageRead()
		pagesRead++
		return nil
	})
	if err != nil {
		return fmt.Errorf("page @i=%d: %w", pagesRead, err)
	}
	if pagesRead != numPages {
		return fmt.Errorf("number of pages found in column index differs from the number of pages read: index=%d read=%d", numPages, pagesRead)
	}

	actualOrder := columnIndexOrder(columnIndex)
	observedOrder := observedIndexOrder(columnType, stats.minValues, stats.maxValues)
	xorAscending := (columnIndex.IsAscending() || observedOrder == ascendingIndexOrder) &&
		!(columnIndex.IsAscending() && observedOrder == ascendingIndexOrder)
	xorDescending := (columnIndex.IsDescending() || observedOrder == descendingIndexOrder) &&
		!(columnIndex.IsDescending() && observedOrder == descendingIndexOrder)

	if xorAscending || xorDescending {
		return fmt.Errorf("column index is declared to be %s while observed values %s (min values %s, max values %s)",
			actualOrder,
			observedOrder,
			valueOrder(columnType, stats.minValues),
			valueOrder(columnType, stats.maxValues),
		)
	}

	return nil
}

func checkRowGroupOffsetIndex(rowGroup parquet.RowGroup) error {
	for i, column := range rowGroup.ColumnChunks() {
		if err := checkColumnChunkOffsetIndex(column); err != nil {
			return fmt.Errorf("column chunk @i=%d: %w", i, err)
		}
	}
	return nil
}

func checkColumnChunkOffsetIndex(columnChunk parquet.ColumnChunk) error {
	offsetIndex, _ := columnChunk.OffsetIndex()
	numPages := offsetIndex.NumPages()
	pagesRead := 0
	rowIndex := int64(0)

	pages := columnChunk.Pages()
	defer pages.Close()

	err := forEachPage(pages, func(page parquet.Page) error {
		if firstRowIndex := offsetIndex.FirstRowIndex(pagesRead); firstRowIndex != rowIndex {
			return fmt.Errorf("row number mismatch: index=%d page=%d", firstRowIndex, rowIndex)
		}
		rowIndex += int64(page.NumRows())
		pagesRead++
		return nil
	})
	if err != nil {
		return fmt.Errorf("page @i=%d: %w", pagesRead, err)
	}

	if pagesRead != numPages {
		return fmt.Errorf("number of pages found in offset index differs from the number of pages read: index=%d read=%d", numPages, pagesRead)
	}

	return nil
}

func testColumnPageIndexWithFile(t *testing.T, rows rows) bool {
	if len(rows) > 0 {
		r := rand.New(rand.NewSource(5))
		f, err := createParquetFile(rows,
			parquet.PageBufferSize(r.Intn(49)+1),
			parquet.ColumnIndexSizeLimit(4096),
		)
		if err != nil {
			t.Error(err)
			return false
		}
		if err := checkFileColumnIndex(f); err != nil {
			t.Error(err)
			return false
		}
		if err := checkFileOffsetIndex(f); err != nil {
			t.Error(err)
			return false
		}
		for i, rowGroup := range f.RowGroups() {
			if err := checkRowGroupColumnIndex(rowGroup); err != nil {
				t.Errorf("checking column index of row group @i=%d: %v", i, err)
				return false
			}
			if err := checkRowGroupOffsetIndex(rowGroup); err != nil {
				t.Errorf("checking offset index of row group @i=%d: %v", i, err)
				return false
			}
		}
	}
	return true
}

func checkFileColumnIndex(f *parquet.File) error {
	columnIndexes := f.ColumnIndexes()
	i := 0
	return forEachColumnChunk(f, func(col *parquet.Column, chunk parquet.ColumnChunk) error {
		columnIndex, _ := chunk.ColumnIndex()
		if n := columnIndex.NumPages(); n <= 0 {
			return fmt.Errorf("invalid number of pages found in the column index: %d", n)
		}
		if i >= len(columnIndexes) {
			return fmt.Errorf("more column indexes were read when iterating over column chunks than when reading from the file (i=%d,n=%d)", i, len(columnIndexes))
		}

		index1 := columnIndex
		index2 := &fileColumnIndex{
			kind:        col.Type().Kind(),
			ColumnIndex: columnIndexes[i],
		}

		numPages1 := index1.NumPages()
		numPages2 := index2.NumPages()
		if numPages1 != numPages2 {
			return fmt.Errorf("number of pages mismatch: got=%d want=%d", numPages1, numPages2)
		}

		for j := 0; j < numPages1; j++ {
			nullCount1 := index1.NullCount(j)
			nullCount2 := index2.NullCount(j)
			if nullCount1 != nullCount2 {
				return fmt.Errorf("null count of page %d/%d mismatch: got=%d want=%d", i, numPages1, nullCount1, nullCount2)
			}

			nullPage1 := index1.NullPage(j)
			nullPage2 := index2.NullPage(j)
			if nullPage1 != nullPage2 {
				return fmt.Errorf("null page of page %d/%d mismatch: got=%t want=%t", i, numPages1, nullPage1, nullPage2)
			}

			minValue1 := index1.MinValue(j)
			minValue2 := index2.MinValue(j)
			if !parquet.Equal(minValue1, minValue2) {
				return fmt.Errorf("min value of page %d/%d mismatch: got=%v want=%v", i, numPages1, minValue1, minValue2)
			}

			maxValue1 := index1.MaxValue(j)
			maxValue2 := index2.MaxValue(j)
			if !parquet.Equal(maxValue1, maxValue2) {
				return fmt.Errorf("max value of page %d/%d mismatch: got=%v want=%v", i, numPages1, maxValue1, maxValue2)
			}

			isAscending1 := index1.IsAscending()
			isAscending2 := index2.IsAscending()
			if isAscending1 != isAscending2 {
				return fmt.Errorf("ascending state of page %d/%d mismatch: got=%t want=%t", i, numPages1, isAscending1, isAscending2)
			}

			isDescending1 := index1.IsDescending()
			isDescending2 := index2.IsDescending()
			if isDescending1 != isDescending2 {
				return fmt.Errorf("descending state of page %d/%d mismatch: got=%t want=%t", i, numPages1, isDescending1, isDescending2)
			}
		}

		i++
		return nil
	})
}

func checkFileOffsetIndex(f *parquet.File) error {
	offsetIndexes := f.OffsetIndexes()
	i := 0
	return forEachColumnChunk(f, func(col *parquet.Column, chunk parquet.ColumnChunk) error {
		offsetIndex, _ := chunk.OffsetIndex()
		if n := offsetIndex.NumPages(); n <= 0 {
			return fmt.Errorf("invalid number of pages found in the offset index: %d", n)
		}
		if i >= len(offsetIndexes) {
			return fmt.Errorf("more offset indexes were read when iterating over column chunks than when reading from the file (i=%d,n=%d)", i, len(offsetIndexes))
		}

		index1 := offsetIndex
		index2 := (*fileOffsetIndex)(&offsetIndexes[i])

		numPages1 := index1.NumPages()
		numPages2 := index2.NumPages()
		if numPages1 != numPages2 {
			return fmt.Errorf("number of pages mismatch: got=%d want=%d", numPages1, numPages2)
		}

		for j := 0; j < numPages1; j++ {
			offset1 := index1.Offset(j)
			offset2 := index2.Offset(j)
			if offset1 != offset2 {
				return fmt.Errorf("offsets of page %d/%d mismatch: got=%d want=%d", i, numPages1, offset1, offset2)
			}

			compressedPageSize1 := index1.CompressedPageSize(j)
			compressedPageSize2 := index2.CompressedPageSize(j)
			if compressedPageSize1 != compressedPageSize2 {
				return fmt.Errorf("compressed page size of page %d/%d mismatch: got=%d want=%d", i, numPages1, compressedPageSize1, compressedPageSize2)
			}

			firstRowIndex1 := index1.FirstRowIndex(j)
			firstRowIndex2 := index2.FirstRowIndex(j)
			if firstRowIndex1 != firstRowIndex2 {
				return fmt.Errorf("first row index of page %d/%d mismatch: got=%d want=%d", i, numPages1, firstRowIndex1, firstRowIndex2)
			}
		}

		i++
		return nil
	})
}

type fileColumnIndex struct {
	kind parquet.Kind
	format.ColumnIndex
}

func (i *fileColumnIndex) NumPages() int                { return len(i.NullPages) }
func (i *fileColumnIndex) NullCount(j int) int64        { return i.NullCounts[j] }
func (i *fileColumnIndex) NullPage(j int) bool          { return i.NullPages[j] }
func (i *fileColumnIndex) MinValue(j int) parquet.Value { return i.kind.Value(i.MinValues[j]) }
func (i *fileColumnIndex) MaxValue(j int) parquet.Value { return i.kind.Value(i.MaxValues[j]) }
func (i *fileColumnIndex) IsAscending() bool            { return i.BoundaryOrder == format.Ascending }
func (i *fileColumnIndex) IsDescending() bool           { return i.BoundaryOrder == format.Descending }

type fileOffsetIndex format.OffsetIndex

func (i *fileOffsetIndex) NumPages() int      { return len(i.PageLocations) }
func (i *fileOffsetIndex) Offset(j int) int64 { return i.PageLocations[j].Offset }
func (i *fileOffsetIndex) CompressedPageSize(j int) int64 {
	return int64(i.PageLocations[j].CompressedPageSize)
}
func (i *fileOffsetIndex) FirstRowIndex(j int) int64 { return i.PageLocations[j].FirstRowIndex }

type columnStats struct {
	page       int
	columnType parquet.Type
	minValues  []parquet.Value
	maxValues  []parquet.Value
}

func newColumnStats(columnType parquet.Type) *columnStats {
	return &columnStats{columnType: columnType}
}

func (c *columnStats) observe(value parquet.Value) {
	if c.page >= len(c.minValues) {
		c.minValues = append(c.minValues, value.Clone())
	} else if c.columnType.Compare(c.minValues[c.page], value) > 0 {
		c.minValues[c.page] = value.Clone()
	}

	if c.page >= len(c.maxValues) {
		c.maxValues = append(c.maxValues, value.Clone())
	} else if c.columnType.Compare(c.maxValues[c.page], value) < 0 {
		c.maxValues[c.page] = value.Clone()
	}
}

func (c *columnStats) pageRead() {
	c.page++
}

type indexOrder int

const (
	invalidIndexOrder indexOrder = iota
	unorderedIndexOrder
	ascendingIndexOrder
	descendingIndexOrder
)

func (o indexOrder) String() string {
	switch o {
	case unorderedIndexOrder:
		return "unordered"
	case ascendingIndexOrder:
		return "ascending"
	case descendingIndexOrder:
		return "descending"
	default:
		return "invalid"
	}
}

func columnIndexOrder(index parquet.ColumnIndex) indexOrder {
	switch {
	case index.IsAscending() && index.IsDescending():
		return invalidIndexOrder
	case index.IsAscending():
		return ascendingIndexOrder
	case index.IsDescending():
		return descendingIndexOrder
	default:
		return unorderedIndexOrder
	}
}

func observedIndexOrder(columnType parquet.Type, minValues []parquet.Value, maxValues []parquet.Value) indexOrder {
	a := valueOrder(columnType, minValues)
	b := valueOrder(columnType, maxValues)

	switch {
	case a == ascendingIndexOrder && b == ascendingIndexOrder:
		return ascendingIndexOrder
	case a == descendingIndexOrder && b == descendingIndexOrder:
		return descendingIndexOrder
	default:
		return unorderedIndexOrder
	}
}

func valueOrder(columnType parquet.Type, values []parquet.Value) indexOrder {
	switch len(values) {
	case 0, 1:
		return unorderedIndexOrder
	}

	var order int
	for i := 1; i < len(values); i++ {
		next := columnType.Compare(values[i-1], values[i])
		if next == 0 {
			continue
		}
		if order == 0 {
			order = next
			continue
		}
		if order != next {
			return unorderedIndexOrder
		}
	}

	if order > 0 {
		return descendingIndexOrder
	}

	return ascendingIndexOrder
}
