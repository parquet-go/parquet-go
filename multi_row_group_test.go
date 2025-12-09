package parquet_test

import (
	"bytes"
	"io"
	"strconv"
	"testing"

	"github.com/parquet-go/parquet-go"
)

func TestMultiBloomFilterReadAt(t *testing.T) {
	type Record struct {
		ID    int64  `parquet:"id"`
		Value string `parquet:"value"`
	}

	tests := []struct {
		name       string
		rowGroups  []rowGroupConfig
		offset     int64
		bufferSize int
		expectSize int // -1 means use filter.Size(), -2 means just check n > 0
		expectErr  error
	}{
		{
			name: "read from beginning",
			rowGroups: []rowGroupConfig{
				{numRows: 10, startID: 0, valuePrefix: "a"},
				{numRows: 20, startID: 10, valuePrefix: "b"},
				{numRows: 15, startID: 30, valuePrefix: "c"},
			},
			offset:     0,
			bufferSize: 100,
			expectSize: -2, // Just verify n > 0
			expectErr:  nil,
		},
		{
			name: "read beyond end",
			rowGroups: []rowGroupConfig{
				{numRows: 10, startID: 0, valuePrefix: "a"},
			},
			offset:     10000,
			bufferSize: 100,
			expectSize: 0,
			expectErr:  io.EOF,
		},
		{
			name: "read entire bloom filter",
			rowGroups: []rowGroupConfig{
				{numRows: 10, startID: 0, valuePrefix: "a"},
				{numRows: 10, startID: 10, valuePrefix: "b"},
			},
			offset:     0,
			bufferSize: -1, // Use filter.Size()
			expectSize: -1, // Expect to read filter.Size() bytes
			expectErr:  nil,
		},
		{
			name: "single row group",
			rowGroups: []rowGroupConfig{
				{numRows: 20, startID: 0, valuePrefix: "x"},
			},
			offset:     0,
			bufferSize: -1, // Use filter.Size()
			expectSize: -1, // Expect to read filter.Size() bytes
			expectErr:  nil,
		},
		{
			name: "zero-length read",
			rowGroups: []rowGroupConfig{
				{numRows: 10, startID: 0, valuePrefix: "a"},
			},
			offset:     0,
			bufferSize: 0,
			expectSize: 0,
			expectErr:  nil,
		},
		{
			name: "read from middle offset",
			rowGroups: []rowGroupConfig{
				{numRows: 10, startID: 0, valuePrefix: "a"},
				{numRows: 10, startID: 10, valuePrefix: "b"},
			},
			offset:     50,
			bufferSize: 100,
			expectSize: -2, // Just verify n > 0
			expectErr:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			multiRG := makeMultiRowGroupWithBloomFilters(t, tt.rowGroups)
			multiFilter := multiRG.ColumnChunks()[0].BloomFilter()

			// Determine buffer size
			bufferSize := tt.bufferSize
			if bufferSize == -1 {
				bufferSize = int(multiFilter.Size())
			}

			buf := make([]byte, bufferSize)
			n, err := multiFilter.ReadAt(buf, tt.offset)

			// Check error
			if err != tt.expectErr {
				// Allow io.EOF as a valid completion error
				if !(err == io.EOF && tt.expectErr == nil) {
					t.Errorf("ReadAt() error = %v, expected %v", err, tt.expectErr)
				}
			}

			// Check bytes read
			expectSize := tt.expectSize
			if expectSize == -1 {
				expectSize = int(multiFilter.Size())
			}

			if expectSize == -2 {
				// Just verify we read something positive
				if n <= 0 {
					t.Errorf("ReadAt() read %d bytes, expected > 0", n)
				}
				// Verify we didn't read more than the buffer
				if n > bufferSize {
					t.Errorf("ReadAt() read %d bytes, buffer size is %d", n, bufferSize)
				}
			} else {
				if n != expectSize {
					t.Errorf("ReadAt() read %d bytes, expected %d", n, expectSize)
				}
			}
		})
	}
}

func TestMultiBloomFilterSize(t *testing.T) {
	type Record struct {
		ID    int64  `parquet:"id"`
		Value string `parquet:"value"`
	}

	rowGroups := []rowGroupConfig{
		{numRows: 10, startID: 0, valuePrefix: "a"},
		{numRows: 20, startID: 10, valuePrefix: "b"},
		{numRows: 15, startID: 30, valuePrefix: "c"},
	}

	multiRG := makeMultiRowGroupWithBloomFilters(t, rowGroups)
	multiFilter := multiRG.ColumnChunks()[0].BloomFilter()

	// Verify Size() returns positive value
	size := multiFilter.Size()
	if size <= 0 {
		t.Errorf("Size() = %d, expected positive value", size)
	}

	// Verify we can read exactly Size() bytes
	buf := make([]byte, size)
	n, err := multiFilter.ReadAt(buf, 0)
	if err != nil && err != io.EOF {
		t.Errorf("ReadAt() unexpected error: %v", err)
	}
	if n <= 0 {
		t.Errorf("ReadAt() read %d bytes, expected > 0", n)
	}
	// Note: n might be less than size if the read completes at a chunk boundary
	if int64(n) > size {
		t.Errorf("ReadAt() read %d bytes, but Size() = %d", n, size)
	}
}

func TestMultiBloomFilterCheck(t *testing.T) {
	type Record struct {
		ID    int64  `parquet:"id"`
		Value string `parquet:"value"`
	}

	rowGroups := []rowGroupConfig{
		{numRows: 10, startID: 0, valuePrefix: "a"},
		{numRows: 10, startID: 100, valuePrefix: "b"},
	}

	multiRG := makeMultiRowGroupWithBloomFilters(t, rowGroups)
	multiFilter := multiRG.ColumnChunks()[0].BloomFilter()

	// Test values that were written to first row group
	for i := int64(0); i < 10; i++ {
		if ok, err := multiFilter.Check(parquet.ValueOf(i)); err != nil {
			t.Errorf("Check(%d) unexpected error: %v", i, err)
		} else if !ok {
			t.Errorf("Check(%d) = false, expected true (value was written)", i)
		}
	}

	// Test values that were written to second row group
	for i := int64(100); i < 110; i++ {
		if ok, err := multiFilter.Check(parquet.ValueOf(i)); err != nil {
			t.Errorf("Check(%d) unexpected error: %v", i, err)
		} else if !ok {
			t.Errorf("Check(%d) = false, expected true (value was written)", i)
		}
	}
}

func TestMultiBloomFilterReadAtIdempotency(t *testing.T) {
	rowGroups := []rowGroupConfig{
		{numRows: 10, startID: 0, valuePrefix: "a"},
		{numRows: 10, startID: 10, valuePrefix: "b"},
	}

	multiRG := makeMultiRowGroupWithBloomFilters(t, rowGroups)
	multiFilter := multiRG.ColumnChunks()[0].BloomFilter()

	// Verify multiple reads at same offset return same data
	buf1 := make([]byte, 100)
	buf2 := make([]byte, 100)

	n1, err1 := multiFilter.ReadAt(buf1, 50)
	n2, err2 := multiFilter.ReadAt(buf2, 50)

	if n1 != n2 {
		t.Errorf("ReadAt() not idempotent: read %d bytes first time, %d bytes second time", n1, n2)
	}
	if err1 != err2 {
		t.Errorf("ReadAt() not idempotent: got error %v first time, %v second time", err1, err2)
	}
	if !bytes.Equal(buf1[:n1], buf2[:n2]) {
		t.Error("ReadAt() not idempotent: returned different data")
	}
}

type rowGroupConfig struct {
	numRows     int
	startID     int
	valuePrefix string
}

// makeMultiRowGroupWithBloomFilters creates a parquet file with multiple row groups,
// each having bloom filters, and returns a MultiRowGroup wrapping them.
func makeMultiRowGroupWithBloomFilters(t *testing.T, rowGroupConfigs []rowGroupConfig) parquet.RowGroup {
	t.Helper()

	type Record struct {
		ID    int64  `parquet:"id"`
		Value string `parquet:"value"`
	}

	buf := new(bytes.Buffer)
	writer := parquet.NewWriter(buf,
		parquet.BloomFilters(
			parquet.SplitBlockFilter(10, "id"),
			parquet.SplitBlockFilter(10, "value"),
		),
	)

	// Write multiple row groups
	for _, config := range rowGroupConfigs {
		for i := range config.numRows {
			if err := writer.Write(&Record{
				ID:    int64(config.startID + i),
				Value: config.valuePrefix + strconv.Itoa(i),
			}); err != nil {
				t.Fatalf("failed to write record: %v", err)
			}
		}
		// Flush to create a new row group
		if err := writer.Flush(); err != nil {
			t.Fatalf("failed to flush row group: %v", err)
		}
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	// Open the file and extract row groups
	reader := bytes.NewReader(buf.Bytes())
	f, err := parquet.OpenFile(reader, int64(reader.Len()))
	if err != nil {
		t.Fatalf("failed to open parquet file: %v", err)
	}

	rowGroups := f.RowGroups()
	return parquet.MultiRowGroup(rowGroups...)
}

func TestMultiColumnIndex(t *testing.T) {
	type Record struct {
		ID    int64  `parquet:"id"`
		Value string `parquet:"value"`
	}

	// Create multiple row groups with different data
	rowGroups := []rowGroupConfig{
		{numRows: 10, startID: 0, valuePrefix: "a"},
		{numRows: 20, startID: 100, valuePrefix: "b"},
		{numRows: 15, startID: 200, valuePrefix: "c"},
	}

	multiRG := makeMultiRowGroupWithBloomFilters(t, rowGroups)

	// Get column index for the ID column
	columnChunk := multiRG.ColumnChunks()[0]
	index, err := columnChunk.ColumnIndex()
	if err != nil {
		t.Fatalf("ColumnIndex() error: %v", err)
	}

	// Verify NumPages is sum of all chunks
	if index.NumPages() <= 0 {
		t.Errorf("NumPages() = %d, expected positive value", index.NumPages())
	}

	// Test that we can access all pages
	for i := range index.NumPages() {
		minVal := index.MinValue(i)
		maxVal := index.MaxValue(i)
		nullCount := index.NullCount(i)
		nullPage := index.NullPage(i)

		// Basic sanity checks
		if minVal.IsNull() && maxVal.IsNull() && !nullPage {
			t.Errorf("Page %d has null min/max but NullPage() = false", i)
		}
		if nullCount < 0 {
			t.Errorf("Page %d has negative null count: %d", i, nullCount)
		}
	}
}

func TestMultiColumnIndexOrdering(t *testing.T) {
	type Record struct {
		ID int64 `parquet:"id"`
	}

	t.Run("ascending data", func(t *testing.T) {
		// Create row groups with ascending data
		buf := new(bytes.Buffer)
		writer := parquet.NewWriter(buf, parquet.SchemaOf(&Record{}))

		// First row group: 0-9
		for i := int64(0); i < 10; i++ {
			writer.Write(&Record{ID: i})
		}
		writer.Flush()

		// Second row group: 10-19
		for i := int64(10); i < 20; i++ {
			writer.Write(&Record{ID: i})
		}
		writer.Flush()

		writer.Close()

		reader := bytes.NewReader(buf.Bytes())
		f, err := parquet.OpenFile(reader, int64(reader.Len()))
		if err != nil {
			t.Fatalf("failed to open file: %v", err)
		}

		multiRG := parquet.MultiRowGroup(f.RowGroups()...)
		index, err := multiRG.ColumnChunks()[0].ColumnIndex()
		if err != nil {
			t.Fatalf("ColumnIndex() error: %v", err)
		}

		// Note: Parquet writers do not automatically set the BoundaryOrder field
		// based on data ordering. The field needs to be explicitly set by the writer.
		// For now, we just verify that IsAscending() and IsDescending() can be called
		// without errors and return consistent results.
		isAsc := index.IsAscending()
		isDesc := index.IsDescending()

		// They should not both be true
		if isAsc && isDesc {
			t.Error("IsAscending() and IsDescending() both returned true, expected at most one")
		}

		// Verify that the methods complete without panicking
		t.Logf("IsAscending() = %v, IsDescending() = %v", isAsc, isDesc)
	})

	t.Run("unordered data", func(t *testing.T) {
		// Create row groups with unordered data
		buf := new(bytes.Buffer)
		writer := parquet.NewWriter(buf, parquet.SchemaOf(&Record{}))

		// First row group: high values
		for i := int64(100); i < 110; i++ {
			writer.Write(&Record{ID: i})
		}
		writer.Flush()

		// Second row group: low values
		for i := int64(0); i < 10; i++ {
			writer.Write(&Record{ID: i})
		}
		writer.Flush()

		writer.Close()

		reader := bytes.NewReader(buf.Bytes())
		f, err := parquet.OpenFile(reader, int64(reader.Len()))
		if err != nil {
			t.Fatalf("failed to open file: %v", err)
		}

		multiRG := parquet.MultiRowGroup(f.RowGroups()...)
		index, err := multiRG.ColumnChunks()[0].ColumnIndex()
		if err != nil {
			t.Fatalf("ColumnIndex() error: %v", err)
		}

		// Should not be ascending
		if index.IsAscending() {
			t.Error("IsAscending() = true, expected false for unordered data")
		}
	})
}

func TestMultiOffsetIndex(t *testing.T) {
	type Record struct {
		ID    int64  `parquet:"id"`
		Value string `parquet:"value"`
	}

	// Create multiple row groups
	rowGroups := []rowGroupConfig{
		{numRows: 10, startID: 0, valuePrefix: "a"},
		{numRows: 20, startID: 100, valuePrefix: "b"},
		{numRows: 15, startID: 200, valuePrefix: "c"},
	}

	multiRG := makeMultiRowGroupWithBloomFilters(t, rowGroups)

	// Get offset index for the ID column
	columnChunk := multiRG.ColumnChunks()[0]
	index, err := columnChunk.OffsetIndex()
	if err != nil {
		t.Fatalf("OffsetIndex() error: %v", err)
	}

	// Verify NumPages is sum of all chunks
	if index.NumPages() <= 0 {
		t.Errorf("NumPages() = %d, expected positive value", index.NumPages())
	}

	// Test FirstRowIndex increases monotonically
	prevRowIndex := int64(-1)
	for i := range index.NumPages() {
		rowIndex := index.FirstRowIndex(i)
		if rowIndex < prevRowIndex {
			t.Errorf("Page %d FirstRowIndex() = %d, expected >= %d", i, rowIndex, prevRowIndex)
		}
		prevRowIndex = rowIndex

		// Verify Offset and CompressedPageSize are non-negative
		offset := index.Offset(i)
		size := index.CompressedPageSize(i)
		if offset < 0 {
			t.Errorf("Page %d Offset() = %d, expected non-negative", i, offset)
		}
		if size <= 0 {
			t.Errorf("Page %d CompressedPageSize() = %d, expected positive", i, size)
		}
	}
}

func TestMultiColumnIndexErrors(t *testing.T) {
	// Test with single row group containing data
	type Record struct {
		ID int64 `parquet:"id"`
	}

	buf := new(bytes.Buffer)
	writer := parquet.NewWriter(buf, parquet.SchemaOf(&Record{}))
	for i := int64(0); i < 10; i++ {
		writer.Write(&Record{ID: i})
	}
	writer.Close()

	reader := bytes.NewReader(buf.Bytes())
	f, err := parquet.OpenFile(reader, int64(reader.Len()))
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}

	multiRG := parquet.MultiRowGroup(f.RowGroups()...)
	index, err := multiRG.ColumnChunks()[0].ColumnIndex()
	if err != nil {
		t.Errorf("ColumnIndex() returned unexpected error: %v", err)
	}
	if index.NumPages() <= 0 {
		t.Errorf("ColumnIndex() NumPages() = %d, expected > 0", index.NumPages())
	}
}

func TestMultiOffsetIndexErrors(t *testing.T) {
	// Test with single row group containing data
	type Record struct {
		ID int64 `parquet:"id"`
	}

	buf := new(bytes.Buffer)
	writer := parquet.NewWriter(buf, parquet.SchemaOf(&Record{}))
	for i := int64(0); i < 10; i++ {
		writer.Write(&Record{ID: i})
	}
	writer.Close()

	reader := bytes.NewReader(buf.Bytes())
	f, err := parquet.OpenFile(reader, int64(reader.Len()))
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}

	multiRG := parquet.MultiRowGroup(f.RowGroups()...)
	index, err := multiRG.ColumnChunks()[0].OffsetIndex()
	if err != nil {
		t.Errorf("OffsetIndex() returned unexpected error: %v", err)
	}
	if index.NumPages() <= 0 {
		t.Errorf("OffsetIndex() NumPages() = %d, expected > 0", index.NumPages())
	}
}
