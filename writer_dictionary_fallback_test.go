package parquet_test

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/parquet-go/parquet-go"
)

type dictionaryFallbackRow struct {
	ID       int64  `parquet:"id"`
	Unique   string `parquet:"unique,dict"`
	Repeated string `parquet:"repeated,dict"`
}

// TestGenericWriterDictionaryFallbackKeepsValues verifies that no values are
// lost when a dict-encoded column outgrows DictionaryMaxBytes and falls back
// to PLAIN encoding mid-file.
//
// Bug details:
//   - GenericWriter.Write resolves each column's ColumnBuffer once and caches
//     the references (makeWriteFunc).
//   - When a column's dictionary exceeds DictionaryMaxBytes, the next page
//     flush calls fallbackDictionaryToPlain, which swaps the ColumnWriter's
//     columnBuffer for a new PLAIN-encoded buffer.
//   - The cached reference still points to the abandoned dictionary buffer, so
//     every subsequent value for that column is written into a buffer that is
//     never flushed again: the column chunk of the current row group is
//     silently truncated (num_values < num_rows) and the chunks of all later
//     row groups are empty (num_values=0, data_page_offset=0).
//   - The generic write path also never assigned originalColumnBuffer, so the
//     per-row-group reset could not restore the dictionary buffer either.
//
// The resulting files are unreadable around the affected columns (readers fail
// with value-count mismatches or decode garbage), while the writer reports
// every row as successfully written.
func TestGenericWriterDictionaryFallbackKeepsValues(t *testing.T) {
	const (
		totalRows       = 5000
		batchRows       = 250
		rowsPerRowGroup = 1000
		// Small limits so the unique column overflows its dictionary and pages
		// flush mid-row-group, which is what leaves the cached column buffer
		// reference pointing at the abandoned dictionary buffer.
		dictionaryMaxBytes = 1024
		pageBufferSize     = 2048
	)

	rows := make([]dictionaryFallbackRow, totalRows)
	for i := range rows {
		rows[i] = dictionaryFallbackRow{
			ID:       int64(i),
			Unique:   fmt.Sprintf("unique-value-%032d", i),
			Repeated: fmt.Sprintf("repeated-%d", i%3),
		}
	}

	t.Run("concrete type", func(t *testing.T) {
		buffer := new(bytes.Buffer)
		writer := parquet.NewGenericWriter[dictionaryFallbackRow](buffer,
			parquet.DictionaryMaxBytes(dictionaryMaxBytes),
			parquet.PageBufferSize(pageBufferSize),
			parquet.MaxRowsPerRowGroup(rowsPerRowGroup),
		)
		for i := 0; i < totalRows; i += batchRows {
			if _, err := writer.Write(rows[i : i+batchRows]); err != nil {
				t.Fatalf("writing rows %d..%d: %v", i, i+batchRows, err)
			}
		}
		if err := writer.Close(); err != nil {
			t.Fatalf("closing writer: %v", err)
		}
		assertDictionaryFallbackFileComplete(t, buffer.Bytes(), rows)
	})

	t.Run("interface type with schema", func(t *testing.T) {
		buffer := new(bytes.Buffer)
		writer := parquet.NewGenericWriter[any](buffer,
			parquet.SchemaOf(dictionaryFallbackRow{}),
			parquet.DictionaryMaxBytes(dictionaryMaxBytes),
			parquet.PageBufferSize(pageBufferSize),
			parquet.MaxRowsPerRowGroup(rowsPerRowGroup),
		)
		batch := make([]any, batchRows)
		for i := 0; i < totalRows; i += batchRows {
			for j := range batch {
				batch[j] = rows[i+j]
			}
			if _, err := writer.Write(batch); err != nil {
				t.Fatalf("writing rows %d..%d: %v", i, i+batchRows, err)
			}
		}
		if err := writer.Close(); err != nil {
			t.Fatalf("closing writer: %v", err)
		}
		assertDictionaryFallbackFileComplete(t, buffer.Bytes(), rows)
	})
}

func assertDictionaryFallbackFileComplete(t *testing.T, data []byte, want []dictionaryFallbackRow) {
	t.Helper()

	file, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("opening file: %v", err)
	}
	if file.NumRows() != int64(len(want)) {
		t.Fatalf("file has %d rows, want %d", file.NumRows(), len(want))
	}

	schema := file.Schema()
	for groupIndex, rowGroup := range file.RowGroups() {
		for _, chunk := range rowGroup.ColumnChunks() {
			if chunk.NumValues() != rowGroup.NumRows() {
				t.Errorf("row group %d column %q: chunk has %d values, want %d",
					groupIndex, schema.Columns()[chunk.Column()][0], chunk.NumValues(), rowGroup.NumRows())
			}
		}
	}
	if t.Failed() {
		t.FailNow()
	}

	reader := parquet.NewGenericReader[dictionaryFallbackRow](bytes.NewReader(data))
	defer reader.Close()

	got := make([]dictionaryFallbackRow, 0, len(want))
	buffer := make([]dictionaryFallbackRow, 100)
	for {
		n, err := reader.Read(buffer)
		got = append(got, buffer[:n]...)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("reading rows back: %v", err)
		}
	}
	if len(got) != len(want) {
		t.Fatalf("read %d rows back, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("row %d mismatch: got %+v, want %+v", i, got[i], want[i])
		}
	}
}
