package parquet

import (
	"bytes"
	"fmt"
	"io"
	"reflect"
	"slices"
	"testing"
)

type copyTestRow struct {
	ID   int64   `parquet:"id"`
	Name string  `parquet:"name,dict"`
	Val  float64 `parquet:"val"`
}

func makeCopyTestRows(n int) []copyTestRow {
	rows := make([]copyTestRow, n)
	for i := range rows {
		rows[i] = copyTestRow{
			ID:   int64(i),
			Name: fmt.Sprintf("name-%d", i%10),
			Val:  float64(i) * 1.5,
		}
	}
	return rows
}

func writeCopyTestFile(t *testing.T, rows []copyTestRow, opts ...WriterOption) *File {
	t.Helper()
	var buf bytes.Buffer
	w := NewGenericWriter[copyTestRow](&buf, opts...)
	if _, err := w.Write(rows); err != nil {
		t.Fatalf("writing source rows: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("closing source writer: %v", err)
	}
	f, err := OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("opening source file: %v", err)
	}
	return f
}

func readCopyTestRows(t *testing.T, b []byte, n int) []copyTestRow {
	t.Helper()
	r := NewGenericReader[copyTestRow](bytes.NewReader(b))
	defer r.Close()
	got := make([]copyTestRow, n)
	read := 0
	for read < n {
		m, err := r.Read(got[read:])
		read += m
		if err != nil {
			break
		}
	}
	return got[:read]
}

// TestWriteRowGroupCopyFastPath verifies that rewriting a file's row groups with
// a matching writer configuration takes the verbatim-copy fast path and produces
// a file with identical data.
func TestWriteRowGroupCopyFastPath(t *testing.T) {
	rows := makeCopyTestRows(1000)
	src := writeCopyTestFile(t, rows)

	before := copyPathCounter.Load()

	var dst bytes.Buffer
	w := NewGenericWriter[copyTestRow](&dst)
	for _, rg := range src.RowGroups() {
		if _, err := w.WriteRowGroup(rg); err != nil {
			t.Fatalf("WriteRowGroup: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("closing dst writer: %v", err)
	}

	if copied := copyPathCounter.Load() - before; copied == 0 {
		t.Fatal("expected the copy fast path to fire, but no columns were copied")
	}

	got := readCopyTestRows(t, dst.Bytes(), len(rows))
	if !reflect.DeepEqual(got, rows) {
		t.Fatalf("round-tripped rows differ from source: got %d rows", len(got))
	}
}

// TestWriteRowGroupCopyMultipleRowGroupsAndIndexes rewrites a multi-row-group
// file via the copy path and verifies the output's offset index (via SeekToRow)
// and column index bounds are intact — properties a plain sequential read would
// not exercise.
func TestWriteRowGroupCopyMultipleRowGroupsAndIndexes(t *testing.T) {
	rows := makeCopyTestRows(1000)
	// Force several row groups in the source.
	src := writeCopyTestFile(t, rows, MaxRowsPerRowGroup(250))
	if got := len(src.RowGroups()); got < 2 {
		t.Fatalf("expected multiple source row groups, got %d", got)
	}

	before := copyPathCounter.Load()

	var dst bytes.Buffer
	w := NewGenericWriter[copyTestRow](&dst, MaxRowsPerRowGroup(250))
	for _, rg := range src.RowGroups() {
		if _, err := w.WriteRowGroup(rg); err != nil {
			t.Fatalf("WriteRowGroup: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("closing dst writer: %v", err)
	}

	// Every column of every row group should have been copied (3 columns).
	if copied := copyPathCounter.Load() - before; copied != int64(3*len(src.RowGroups())) {
		t.Fatalf("expected %d copied columns, got %d", 3*len(src.RowGroups()), copied)
	}

	out, err := OpenFile(bytes.NewReader(dst.Bytes()), int64(dst.Len()))
	if err != nil {
		t.Fatalf("opening output file: %v", err)
	}
	if got := out.NumRows(); got != int64(len(rows)) {
		t.Fatalf("output row count = %d, want %d", got, len(rows))
	}

	// Exercise the offset index: seek into the middle of each row group's "id"
	// column and confirm the value matches the global row position.
	rowBase := int64(0)
	for _, rg := range out.RowGroups() {
		idChunk := rg.ColumnChunks()[0] // "id"
		oi, err := idChunk.OffsetIndex()
		if err != nil {
			t.Fatalf("output offset index missing: %v", err)
		}
		if oi.NumPages() == 0 {
			t.Fatal("output offset index has no pages")
		}

		pages := idChunk.Pages()
		seekTo := rg.NumRows() / 2
		if err := pages.SeekToRow(seekTo); err != nil {
			pages.Close()
			t.Fatalf("SeekToRow(%d): %v", seekTo, err)
		}
		page, err := pages.ReadPage()
		if err != nil {
			pages.Close()
			t.Fatalf("ReadPage after seek: %v", err)
		}
		vals := make([]Value, 1)
		n, _ := page.Values().ReadValues(vals)
		if n != 1 {
			pages.Close()
			t.Fatal("expected to read one value after seek")
		}
		wantID := rowBase + seekTo
		if vals[0].Int64() != wantID {
			pages.Close()
			t.Fatalf("value after seek = %d, want %d", vals[0].Int64(), wantID)
		}
		pages.Close()

		// Column index bounds for "id" must bracket the row group's id range.
		ci, err := idChunk.ColumnIndex()
		if err != nil {
			t.Fatalf("output column index missing: %v", err)
		}
		minSeen := ci.MinValue(0).Int64()
		maxSeen := ci.MaxValue(ci.NumPages() - 1).Int64()
		if minSeen != rowBase {
			t.Fatalf("column index min = %d, want %d", minSeen, rowBase)
		}
		if want := rowBase + rg.NumRows() - 1; maxSeen != want {
			t.Fatalf("column index max = %d, want %d", maxSeen, want)
		}

		rowBase += rg.NumRows()
	}

	got := readCopyTestRows(t, dst.Bytes(), len(rows))
	if !reflect.DeepEqual(got, rows) {
		t.Fatal("round-tripped rows differ across multiple row groups")
	}
}

// TestWriteRowGroupCopyWithCompression exercises the copy path with a matching
// non-default compression codec.
func TestWriteRowGroupCopyWithCompression(t *testing.T) {
	rows := makeCopyTestRows(1000)
	src := writeCopyTestFile(t, rows, Compression(&Snappy))

	before := copyPathCounter.Load()

	var dst bytes.Buffer
	w := NewGenericWriter[copyTestRow](&dst, Compression(&Snappy))
	for _, rg := range src.RowGroups() {
		if _, err := w.WriteRowGroup(rg); err != nil {
			t.Fatalf("WriteRowGroup: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("closing dst writer: %v", err)
	}

	if copied := copyPathCounter.Load() - before; copied == 0 {
		t.Fatal("expected the copy fast path to fire with matching compression")
	}

	got := readCopyTestRows(t, dst.Bytes(), len(rows))
	if !reflect.DeepEqual(got, rows) {
		t.Fatalf("round-tripped rows differ from source with compression")
	}
}

// TestWriteRowGroupCopyDemotesOnCodecMismatch verifies that a mismatched
// compression codec disables the fast path (falling back to re-encode) while
// still producing correct data.
func TestWriteRowGroupCopyDemotesOnCodecMismatch(t *testing.T) {
	rows := makeCopyTestRows(1000)
	src := writeCopyTestFile(t, rows, Compression(&Snappy))

	before := copyPathCounter.Load()

	var dst bytes.Buffer
	w := NewGenericWriter[copyTestRow](&dst, Compression(&Zstd))
	for _, rg := range src.RowGroups() {
		if _, err := w.WriteRowGroup(rg); err != nil {
			t.Fatalf("WriteRowGroup: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("closing dst writer: %v", err)
	}

	if copied := copyPathCounter.Load() - before; copied != 0 {
		t.Fatalf("expected codec mismatch to disable copy, but %d columns were copied", copied)
	}

	got := readCopyTestRows(t, dst.Bytes(), len(rows))
	if !reflect.DeepEqual(got, rows) {
		t.Fatalf("round-tripped rows differ from source after demotion")
	}
}

// benchRow is a wider record used to make the re-encode cost of the baseline
// meaningful (compression + multiple typed columns including a dictionary).
type benchRow struct {
	ID    int64   `parquet:"id"`
	Name  string  `parquet:"name,dict"`
	Email string  `parquet:"email"`
	Score float64 `parquet:"score"`
	Flag  bool    `parquet:"flag"`
	Count int32   `parquet:"count"`
}

func makeBenchRows(n int) []benchRow {
	rows := make([]benchRow, n)
	for i := range rows {
		rows[i] = benchRow{
			ID:    int64(i),
			Name:  fmt.Sprintf("category-%d", i%32),
			Email: fmt.Sprintf("user%d@example.com", i),
			Score: float64(i) * 0.125,
			Flag:  i%2 == 0,
			Count: int32(i % 1000),
		}
	}
	return rows
}

func benchmarkWriteRowGroupRewrite(b *testing.B, disableCopy bool) {
	const numRows = 200_000
	rows := makeBenchRows(numRows)

	var src bytes.Buffer
	sw := NewGenericWriter[benchRow](&src, Compression(&Snappy), MaxRowsPerRowGroup(50_000))
	if _, err := sw.Write(rows); err != nil {
		b.Fatal(err)
	}
	if err := sw.Close(); err != nil {
		b.Fatal(err)
	}
	file, err := OpenFile(bytes.NewReader(src.Bytes()), int64(src.Len()))
	if err != nil {
		b.Fatal(err)
	}
	rowGroups := file.RowGroups()

	defer func(prev bool) { disableWriteCopy = prev }(disableWriteCopy)
	disableWriteCopy = disableCopy

	b.ReportAllocs()
	b.SetBytes(int64(src.Len()))
	b.ResetTimer()

	for b.Loop() {
		w := NewGenericWriter[benchRow](io.Discard, Compression(&Snappy), MaxRowsPerRowGroup(50_000))
		for _, rg := range rowGroups {
			if _, err := w.WriteRowGroup(rg); err != nil {
				b.Fatal(err)
			}
		}
		if err := w.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWriteRowGroupCopy(b *testing.B) {
	benchmarkWriteRowGroupRewrite(b, false)
}

func BenchmarkWriteRowGroupReencode(b *testing.B) {
	benchmarkWriteRowGroupRewrite(b, true)
}

// TestFileColumnChunkOfUnwrap verifies that the copy path can see through a
// positional-remap wrapper to the underlying file-backed chunk, while rejecting
// non-file chunks.
func TestFileColumnChunkOfUnwrap(t *testing.T) {
	rows := makeCopyTestRows(100)
	src := writeCopyTestFile(t, rows)
	fc := src.RowGroups()[0].ColumnChunks()[0].(*FileColumnChunk)

	if got, ok := fileColumnChunkOf(fc); !ok || got != fc {
		t.Fatal("expected direct *FileColumnChunk to unwrap to itself")
	}

	wrapped := &convertedColumnChunk{chunk: fc, targetColumnIndex: ^uint16(2)}
	if got, ok := fileColumnChunkOf(wrapped); !ok || got != fc {
		t.Fatal("expected convertedColumnChunk to unwrap to inner *FileColumnChunk")
	}

	if _, ok := fileColumnChunkOf(&emptyColumnChunk{}); ok {
		t.Fatal("expected non-file chunk to fail unwrap")
	}
}

// TestWriteRowGroupCopyMergeNonOverlapping verifies that writing the result of
// MergeRowGroups over non-overlapping, sorted inputs takes the copy fast path
// per segment and preserves global sort order and data.
func TestWriteRowGroupCopyMergeNonOverlapping(t *testing.T) {
	// Two files with disjoint, sorted id ranges.
	first := make([]copyTestRow, 500)
	for i := range first {
		first[i] = copyTestRow{ID: int64(i), Name: fmt.Sprintf("a-%d", i%7), Val: float64(i)}
	}
	second := make([]copyTestRow, 500)
	for i := range second {
		id := int64(1000 + i)
		second[i] = copyTestRow{ID: id, Name: fmt.Sprintf("b-%d", i%7), Val: float64(id)}
	}

	writerSorting := SortingWriterConfig(SortingColumns(Ascending("id")))
	f1 := writeCopyTestFile(t, first, writerSorting)
	f2 := writeCopyTestFile(t, second, writerSorting)

	merged, err := MergeRowGroups(
		[]RowGroup{f1.RowGroups()[0], f2.RowGroups()[0]},
		SortingRowGroupConfig(SortingColumns(Ascending("id"))),
	)
	if err != nil {
		t.Fatal(err)
	}

	before := copyPathCounter.Load()

	var dst bytes.Buffer
	w := NewGenericWriter[copyTestRow](&dst, writerSorting)
	if _, err := w.WriteRowGroup(merged); err != nil {
		t.Fatalf("WriteRowGroup(merged): %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	if copied := copyPathCounter.Load() - before; copied == 0 {
		t.Fatal("expected the copy fast path to fire for non-overlapping merge segments")
	}

	out, err := OpenFile(bytes.NewReader(dst.Bytes()), int64(dst.Len()))
	if err != nil {
		t.Fatal(err)
	}
	if got := out.NumRows(); got != 1000 {
		t.Fatalf("output row count = %d, want 1000", got)
	}

	// Verify data and global sort order.
	got := readCopyTestRows(t, dst.Bytes(), 1000)
	if len(got) != 1000 {
		t.Fatalf("read %d rows, want 1000", len(got))
	}
	for i := 1; i < len(got); i++ {
		if got[i].ID < got[i-1].ID {
			t.Fatalf("rows out of order at %d: %d < %d", i, got[i].ID, got[i-1].ID)
		}
	}
	want := slices.Concat(first, second)
	if !reflect.DeepEqual(got, want) {
		t.Fatal("merged+copied rows differ from expected")
	}
}
