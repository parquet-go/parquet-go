package parquet_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"slices"
	"testing"

	"github.com/google/uuid"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/deprecated"
	"github.com/parquet-go/parquet-go/encoding/thrift"
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
				function func(*testing.T) any
			}{
				{
					scenario: "boolean",
					function: func(t *testing.T) any {
						return func(rows []struct{ Value bool }) bool { return config.test(t, makeRows(rows)) }
					},
				},

				{
					scenario: "int32",
					function: func(t *testing.T) any {
						return func(rows []struct{ Value int32 }) bool { return config.test(t, makeRows(rows)) }
					},
				},

				{
					scenario: "int64",
					function: func(t *testing.T) any {
						return func(rows []struct{ Value int64 }) bool { return config.test(t, makeRows(rows)) }
					},
				},

				{
					scenario: "int96",
					function: func(t *testing.T) any {
						return func(rows []struct{ Value deprecated.Int96 }) bool { return config.test(t, makeRows(rows)) }
					},
				},

				{
					scenario: "uint32",
					function: func(t *testing.T) any {
						return func(rows []struct{ Value uint32 }) bool { return config.test(t, makeRows(rows)) }
					},
				},

				{
					scenario: "uint64",
					function: func(t *testing.T) any {
						return func(rows []struct{ Value uint64 }) bool { return config.test(t, makeRows(rows)) }
					},
				},

				{
					scenario: "float32",
					function: func(t *testing.T) any {
						return func(rows []struct{ Value float32 }) bool { return config.test(t, makeRows(rows)) }
					},
				},

				{
					scenario: "float64",
					function: func(t *testing.T) any {
						return func(rows []struct{ Value float64 }) bool { return config.test(t, makeRows(rows)) }
					},
				},

				{
					scenario: "string",
					function: func(t *testing.T) any {
						return func(rows []struct{ Value string }) bool { return config.test(t, makeRows(rows)) }
					},
				},

				{
					scenario: "uuid",
					function: func(t *testing.T) any {
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
			return fmt.Errorf("number of null values mismatch: index=%d page=%d", nullCount, numNulls)
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
			parquet.ColumnIndexSizeLimit(func(path []string) int { return 4096 }),
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

		for j := range numPages1 {
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

		for j := range numPages1 {
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

func TestColumnPages_SeekToRow(t *testing.T) {
	type Contact struct {
		ID   int64  `parquet:"id"`
		Name string `parquet:"name"`
		Sex  bool   `parquet:"sex"`
	}

	buf := bytes.Buffer{}
	writer := parquet.NewWriter(&buf)
	data := [][]Contact{
		{
			{ID: 1, Name: "user1"},
			{ID: 2, Name: "user2"},
			{ID: 7, Name: "user7"},
		},
		{
			{ID: 8, Name: "user8"},
			{ID: 10, Name: "user10"},
			{ID: 12, Name: "user12"},
		},
		{
			{ID: 15, Name: "user15"},
			{ID: 16, Name: "user16"},
		},
	}
	for _, rows := range data {
		for _, row := range rows {
			err := writer.Write(&row)
			if err != nil {
				panic(err)
			}
		}
		err := writer.Flush()
		if err != nil {
			panic(err)
		}
	}
	err := writer.Close()
	if err != nil {
		panic(err)
	}

	pr, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Error(err)
	}

	id := pr.Root().Column("id")

	pages := id.Pages()
	defer pages.Close()

	var idx int64
	for _, rows := range data {
		for _, row := range rows {
			err := pages.SeekToRow(idx)
			if err != nil {
				t.Error(err)
			}

			page, err := pages.ReadPage()
			if err != nil {
				t.Error(err)
			}

			var values [1]int64
			page.Values().(interface {
				ReadInt64s(values []int64) (n int, err error)
			}).ReadInt64s(values[:])

			if values[0] != row.ID {
				t.Errorf("read value of page mismatch, row index %d: got=%d want=%d", idx, values[0], row.ID)
			}

			parquet.Release(page)
			idx++
		}
	}
}

// TestOpenFileColumnEncodingSkipsBitPacked verifies that BIT_PACKED is never
// reported as a column's encoding. Legacy writers (parquet-java/Impala) list
// BIT_PACKED in a column chunk's encodings when it was used for repetition
// or definition levels, but it is not a data page encoding: picking it as
// the column encoding makes schemas derived from the file unwritable
// ("encoding not supported for type ...").
func TestOpenFileColumnEncodingSkipsBitPacked(t *testing.T) {
	f, err := os.Open("testdata/alltypes_tiny_pages.parquet")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	pf, err := parquet.OpenFile(f, st.Size())
	if err != nil {
		t.Fatal(err)
	}

	// The test file must actually list BIT_PACKED in its chunk metadata,
	// otherwise this test asserts nothing.
	hasBitPacked := false
	for _, chunk := range pf.Metadata().RowGroups[0].Columns {
		for _, enc := range chunk.MetaData.Encoding {
			hasBitPacked = hasBitPacked || enc == format.BitPacked
		}
	}
	if !hasBitPacked {
		t.Fatal("test file does not use BIT_PACKED level encoding")
	}

	err = forEachLeafColumn(pf.Root(), func(leaf *parquet.Column) error {
		if enc := leaf.Encoding(); enc != nil && enc.Encoding() == format.BitPacked {
			t.Errorf("column %q reports BIT_PACKED as its encoding", leaf.Name())
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// withChunkEncodings rewrites the footer of a parquet file so that the first
// column chunk lists the given encodings, in order, in its metadata. Only the
// footer changes, so page and index offsets stay valid.
func withChunkEncodings(t *testing.T, file []byte, encodings ...format.Encoding) []byte {
	t.Helper()

	// file layout is [pages][page index][footer][footer length (4)]["PAR1" (4)]
	n := len(file)
	footerSize := int(binary.LittleEndian.Uint32(file[n-8 : n-4]))
	footerStart := n - 8 - footerSize

	// decode the footer based on the thrift compact protocol
	var proto thrift.CompactProtocol
	var meta format.FileMetaData
	if err := thrift.Unmarshal(&proto, file[footerStart:n-8], &meta); err != nil {
		t.Fatal(err)
	}

	// doing the actual edit; overwriting the encodings of the first column chunk
	meta.RowGroups[0].Columns[0].MetaData.Encoding = encodings

	// keep everything before the footer as is, then append the new footer
	var out bytes.Buffer
	out.Write(file[:footerStart])
	start := out.Len()
	if err := thrift.NewEncoder(proto.NewWriter(&out)).Encode(&meta); err != nil {
		t.Fatal(err)
	}

	// the footer length may have changed, so write a fresh trailer
	var trailer [8]byte
	binary.LittleEndian.PutUint32(trailer[:4], uint32(out.Len()-start))
	copy(trailer[4:], "PAR1")
	out.Write(trailer[:])
	return out.Bytes()
}

// TestOpenFileColumnEncodingRLE checks that RLE in a chunk's encodings list is
// not reported as the column encoding, except for BOOLEAN where it is a real
// value encoding. Writers list RLE for rep/def levels next to the value
// encoding (e.g. [RLE, PLAIN]). INT32 and DATE are covered because
// rle.Encoding implements EncodeInt32 for dictionary indexes, so canEncode
// alone accepts it there.
func TestOpenFileColumnEncodingRLE(t *testing.T) {
	type testCase struct {
		name  string
		node  parquet.Node
		value any
		// rleFirst rewrites the chunk's encodings to [RLE, PLAIN].
		rleFirst bool
		want     format.Encoding
	}

	tests := []testCase{
		{
			name:     "int64",
			node:     parquet.Leaf(parquet.Int64Type),
			value:    int64(1),
			rleFirst: true,
			want:     format.Plain,
		},
		{
			name:     "int32",
			node:     parquet.Leaf(parquet.Int32Type),
			value:    int32(1),
			rleFirst: true,
			want:     format.Plain,
		},
		{
			name:     "date",
			node:     parquet.Date(),
			value:    int32(1),
			rleFirst: true,
			want:     format.Plain,
		},
		{
			// Written with RLE as its value encoding, so no rewrite needed.
			name:  "boolean",
			node:  parquet.Encoded(parquet.Leaf(parquet.BooleanType), &parquet.RLE),
			value: true,
			want:  format.RLE,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			schema := parquet.NewSchema("t", parquet.Group{"v": test.node})
			var buf bytes.Buffer
			w := parquet.NewGenericWriter[map[string]any](&buf, schema)
			if _, err := w.Write([]map[string]any{{"v": test.value}}); err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			data := buf.Bytes()
			if test.rleFirst {
				data = withChunkEncodings(t, data, format.RLE, format.Plain)
			}

			pf, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
			if err != nil {
				t.Fatal(err)
			}
			// Without RLE in the metadata the test asserts nothing.
			if !slices.Contains(pf.Metadata().RowGroups[0].Columns[0].MetaData.Encoding, format.RLE) {
				t.Fatal("chunk metadata does not list RLE")
			}
			if got := pf.Root().Column("v").Encoding(); got == nil || got.Encoding() != test.want {
				t.Errorf("column encoding = %v, want %s", got, test.want)
			}
		})
	}
}

// flba12_timestamp.parquet is an interoperability fixture from
// apache/parquet-testing. Its timestamp columns use a 12-byte physical
// representation that this reader does not interpret as a timestamp.
func TestReadUnrecognizedTimestampRepresentation(t *testing.T) {
	f, err := os.Open("testdata/flba12_timestamp.parquet")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	file, err := parquet.OpenFile(f, info.Size())
	if err != nil {
		t.Fatal(err)
	}
	for _, chunk := range file.RowGroups()[0].ColumnChunks() {
		if _, _, ok := chunk.(*parquet.FileColumnChunk).Bounds(); ok {
			t.Fatal("unsupported annotation provided ordered statistics")
		}
	}
	rows := file.RowGroups()[0].Rows()
	defer rows.Close()
	values := make([]parquet.Row, 2)
	n, err := rows.ReadRows(values)
	if err != nil || n != len(values) {
		t.Fatalf("ReadRows = (%d, %v)", n, err)
	}
	// The second row is one second after the Unix epoch in all three units.
	want := []uint64{1000, 1000000, 1000000000}
	if len(values[1]) != len(want) {
		t.Fatalf("got %d values, want %d", len(values[1]), len(want))
	}
	for i, value := range values[1] {
		var raw [12]byte
		binary.LittleEndian.PutUint64(raw[:], want[i])
		if value.Kind() != parquet.FixedLenByteArray || !bytes.Equal(value.ByteArray(), raw[:]) {
			t.Fatalf("column %d: got %s %v, want %x", i, value.Kind(), value, raw)
		}
	}
}
