package parquet_test

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/bloom"
	"github.com/parquet-go/parquet-go/format"
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

	t.Run("concurrent row group", func(t *testing.T) {
		buffer := new(bytes.Buffer)
		writer := parquet.NewGenericWriter[dictionaryFallbackRow](buffer,
			parquet.DictionaryMaxBytes(dictionaryMaxBytes),
			parquet.PageBufferSize(pageBufferSize),
			parquet.MaxRowsPerRowGroup(rowsPerRowGroup),
		)
		// The dictionary overflows in row groups written through
		// BeginGenericRowGroup, and the writer's own row groups follow them.
		const concurrentRows = 3 * rowsPerRowGroup
		for start := 0; start < concurrentRows; start += rowsPerRowGroup {
			rowGroup := writer.BeginGenericRowGroup()
			for i := start; i < start+rowsPerRowGroup; i += batchRows {
				if _, err := rowGroup.Write(rows[i : i+batchRows]); err != nil {
					t.Fatalf("writing rows %d..%d: %v", i, i+batchRows, err)
				}
			}
			if _, err := rowGroup.Commit(); err != nil {
				t.Fatalf("committing rows %d..%d: %v", start, start+rowsPerRowGroup, err)
			}
		}
		for i := concurrentRows; i < totalRows; i += batchRows {
			if _, err := writer.Write(rows[i : i+batchRows]); err != nil {
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

// TestDictionaryFallbackBloomFilter checks that the bloom filter of a column
// that falls back from dictionary to PLAIN encoding is the filter of the values
// written on both sides of the fallback, and nothing else.
func TestDictionaryFallbackBloomFilter(t *testing.T) {
	type row struct {
		S string `parquet:"s,dict"`
	}
	filterColumn := parquet.SplitBlockFilter(10, "s")
	key := bytes.Repeat([]byte{42}, 16)

	tests := []struct {
		name       string
		version    int
		encrypted  bool
		rowGroup   bool // WriteRowGroup knows the number of values before the first page
		pageSize   int  // 0 keeps one page
		rows       int
		distinct   int
		plainPages bool
	}{
		{name: "Write", version: 1, pageSize: 256, rows: 4000, distinct: 4000, plainPages: true},
		{name: "Write v2", version: 2, pageSize: 256, rows: 4000, distinct: 4000, plainPages: true},
		{name: "Write encrypted", version: 1, encrypted: true, pageSize: 256, rows: 4000, distinct: 4000, plainPages: true},
		{name: "WriteRowGroup", version: 1, rowGroup: true, pageSize: 256, rows: 4000, distinct: 4000, plainPages: true},
		{name: "fallback on the last page", version: 1, rows: 2000, distinct: 100},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rows := make([]row, test.rows)
			for i := range rows {
				rows[i] = row{fmt.Sprintf("value-%06d", i%test.distinct)}
			}
			options := []parquet.WriterOption{
				parquet.DictionaryMaxBytes(64),
				parquet.DataPageVersion(test.version),
				parquet.BloomFilters(filterColumn),
			}
			if test.pageSize > 0 {
				options = append(options, parquet.PageBufferSize(test.pageSize))
			}
			var readOptions []parquet.FileOption
			if test.encrypted {
				options = append(options, parquet.WithEncryption(&parquet.EncryptionConfig{FooterKey: key, EncryptedFooter: true}))
				readOptions = append(readOptions, parquet.WithDecryption(&staticKeyRetriever{footerKey: key}))
			}
			var output bytes.Buffer
			w := parquet.NewGenericWriter[row](&output, options...)
			if test.rowGroup {
				buffer := parquet.NewGenericBuffer[row]()
				if _, err := buffer.Write(rows); err != nil {
					t.Fatal(err)
				}
				if _, err := w.WriteRowGroup(buffer); err != nil {
					t.Fatal(err)
				}
			} else {
				for i := 0; i < len(rows); i += 10 {
					if _, err := w.Write(rows[i : i+10]); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}

			f, err := parquet.OpenFile(bytes.NewReader(output.Bytes()), int64(output.Len()), readOptions...)
			if err != nil {
				t.Fatal(err)
			}
			dataPages := map[format.Encoding]int32{}
			for _, stats := range f.Metadata().RowGroups[0].Columns[0].MetaData.EncodingStats {
				if stats.PageType != format.DictionaryPage {
					dataPages[stats.Encoding] += stats.Count
				}
			}
			if dataPages[format.RLEDictionary] == 0 || (dataPages[format.Plain] > 0) != test.plainPages {
				t.Fatalf("data page encodings %v, want RLE_DICTIONARY and PLAIN pages: %t", dataPages, test.plainPages)
			}

			filter := f.RowGroups()[0].ColumnChunks()[0].BloomFilter()
			if filter == nil {
				t.Fatal("no bloom filter")
			}
			// PLAIN pages are not known to hold distinct values, so the filter
			// is sized for every value written, and otherwise for the dictionary.
			sized := test.distinct
			if test.plainPages {
				sized = test.rows
			}
			want := bloom.MakeSplitBlockFilter(make([]byte, filterColumn.Size(int64(sized))))
			for _, r := range rows[:test.distinct] {
				want.Insert(bloom.XXH64{}.Sum64([]byte(r.S)))
			}
			if size := filter.Size(); size != int64(len(want.Bytes())) {
				t.Fatalf("bloom filter of %d bytes, want %d for %d values", size, len(want.Bytes()), sized)
			}
			got := make([]byte, filter.Size())
			if _, err := filter.ReadAt(got, 0); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, want.Bytes()) {
				t.Errorf("bloom filter differs from the filter of the %d distinct values written", test.distinct)
			}
		})
	}
}

func TestWriterResetAfterDictionaryFallback(t *testing.T) {
	t.Run("required", func(t *testing.T) {
		type row struct {
			Value string `parquet:"value,dict"`
		}
		testWriterResetAfterDictionaryFallback(t, func(s string) row { return row{s} })
	})
	t.Run("optional", func(t *testing.T) {
		type row struct {
			Value *string `parquet:"value,dict"`
		}
		testWriterResetAfterDictionaryFallback(t, func(s string) row {
			if s == "" {
				return row{}
			}
			return row{&s}
		})
	})
	t.Run("repeated", func(t *testing.T) {
		type row struct {
			Value []string `parquet:"value,dict"`
		}
		testWriterResetAfterDictionaryFallback(t, func(s string) row {
			if s == "" {
				return row{}
			}
			return row{[]string{s, s + "-extra"}}
		})
	})
}

func testWriterResetAfterDictionaryFallback[T any](t *testing.T, makeRow func(string) T) {
	t.Helper()
	options := []parquet.WriterOption{
		parquet.DictionaryMaxBytes(64),
		parquet.PageBufferSize(128),
		parquet.BloomFilters(parquet.SplitBlockFilter(10, "value")),
		parquet.DataPageStatistics(true),
	}
	w := parquet.NewGenericWriter[T](nil, options...)
	write := func(t *testing.T, w *parquet.GenericWriter[T], rows []T) {
		t.Helper()
		for i := range rows {
			if _, err := w.Write(rows[i : i+1]); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, fallback := range []bool{false, true} {
		t.Run(fmt.Sprintf("plain=%t", fallback), func(t *testing.T) {
			var abandoned bytes.Buffer
			w.Reset(&abandoned)
			if fallback {
				for i := range 60 {
					write(t, w, []T{makeRow(fmt.Sprintf("old-%032d", i))})
				}
				// Leave a fresh PLAIN page with room for one unflushed row.
				for _, c := range w.ColumnWriters() {
					if err := c.Flush(); err != nil {
						t.Fatal(err)
					}
				}
			}
			write(t, w, []T{makeRow("stale")})

			var output bytes.Buffer
			w.Reset(&output)
			want := make([]T, 60)
			for i := range want {
				value := ""
				if i%3 != 0 {
					value = fmt.Sprintf("new-%032d", i)
				}
				want[i] = makeRow(value)
			}
			write(t, w, want)
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			got, err := parquet.Read[T](bytes.NewReader(output.Bytes()), int64(output.Len()))
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != len(want) {
				t.Errorf("read %d rows after Reset, want %d", len(got), len(want))
			}
			for i := range min(len(got), len(want)) {
				if !w.Schema().Deconstruct(nil, got[i]).Equal(w.Schema().Deconstruct(nil, want[i])) {
					t.Errorf("row %d after Reset = %v, want %v", i, got[i], want[i])
					break
				}
			}

			// Comparing the complete file also checks dictionary and data pages,
			// bloom filters, page indexes, and statistics against a fresh writer.
			var fresh bytes.Buffer
			other := parquet.NewGenericWriter[T](&fresh, options...)
			write(t, other, want)
			if err := other.Close(); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(output.Bytes(), fresh.Bytes()) {
				t.Error("file after Reset differs from a fresh writer")
			}
		})
	}
}
