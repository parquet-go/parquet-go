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

func writeCopyTestFile(t testing.TB, rows []copyTestRow, opts ...WriterOption) *File {
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

func benchmarkWriteRowGroupRewrite(b *testing.B, baseline bool) {
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

	// The baseline is the pure row-oriented path: disable both fast paths so it
	// measures full decode + row assembly + re-encode.
	defer func(c, r bool) { disableWriteCopy = c; disableWriteReencode = r }(disableWriteCopy, disableWriteReencode)
	disableWriteCopy = baseline
	disableWriteReencode = baseline

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

// TestWriteRowGroupDedupNotBypassed is a regression test: WriteRowGroup of a
// dedup-wrapped merge (single sorted source with DropDuplicatedRows) must not
// take the chunk-level fast paths, which would silently retain the duplicates
// (dedupRowGroup's ColumnChunks are promoted unchanged from the wrapped row
// group).
func TestWriteRowGroupDedupNotBypassed(t *testing.T) {
	ids := []int64{1, 1, 2, 2, 3, 4, 5, 5}
	rows := make([]copyTestRow, len(ids))
	for i, id := range ids {
		rows[i] = copyTestRow{ID: id, Name: "x", Val: float64(id)}
	}
	f := writeCopyTestFile(t, rows, SortingWriterConfig(SortingColumns(Ascending("id"))))

	merged, err := MergeRowGroups(
		[]RowGroup{f.RowGroups()[0]},
		SortingRowGroupConfig(SortingColumns(Ascending("id")), DropDuplicatedRows(true)),
	)
	if err != nil {
		t.Fatal(err)
	}

	var dst bytes.Buffer
	w := NewGenericWriter[copyTestRow](&dst, SortingWriterConfig(SortingColumns(Ascending("id"))))
	if _, err := w.WriteRowGroup(merged); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	out, err := OpenFile(bytes.NewReader(dst.Bytes()), int64(dst.Len()))
	if err != nil {
		t.Fatal(err)
	}
	if got := out.NumRows(); got != 5 {
		t.Fatalf("deduplicated output has %d rows, want 5 (fast path bypassed dedup)", got)
	}
}

// TestWriteRowGroupConversionNotBypassed is a regression test: WriteRowGroup of
// a converted merge (source schema differs from the target, requiring value
// conversion) must not take the chunk-level fast paths, which would emit the
// unconverted source values under the converted schema (convertedRowGroup's
// ColumnChunks expose the raw source chunks; conversion only happens in Rows()).
func TestWriteRowGroupConversionNotBypassed(t *testing.T) {
	type rowStr struct {
		ID string `parquet:"id"`
	}
	type rowInt struct {
		ID int64 `parquet:"id"`
	}

	var src bytes.Buffer
	sw := NewGenericWriter[rowStr](&src, SortingWriterConfig(SortingColumns(Ascending("id"))))
	if _, err := sw.Write([]rowStr{{"1"}, {"2"}, {"3"}}); err != nil {
		t.Fatal(err)
	}
	if err := sw.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := OpenFile(bytes.NewReader(src.Bytes()), int64(src.Len()))
	if err != nil {
		t.Fatal(err)
	}

	merged, err := MergeRowGroups(f.RowGroups(),
		&RowGroupConfig{Schema: SchemaOf(rowInt{})},
		SortingRowGroupConfig(SortingColumns(Ascending("id"))),
	)
	if err != nil {
		t.Fatal(err)
	}

	var dst bytes.Buffer
	w := NewGenericWriter[rowInt](&dst)
	if _, err := w.WriteRowGroup(merged); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r := NewGenericReader[rowInt](bytes.NewReader(dst.Bytes()))
	defer r.Close()
	out := make([]rowInt, 4)
	n, _ := r.Read(out)
	if n != 3 {
		t.Fatalf("read %d rows, want 3", n)
	}
	for i, want := range []int64{1, 2, 3} {
		if out[i].ID != want {
			t.Fatalf("row %d = %d, want %d (fast path bypassed value conversion)", i, out[i].ID, want)
		}
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
	// MaxRowsPerRowGroup equal to each source segment's row count so the two
	// non-overlapping segments are not packed together and each takes the
	// verbatim L0 copy path.
	w := NewGenericWriter[copyTestRow](&dst, writerSorting, MaxRowsPerRowGroup(500))
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

// TestWriteRowGroupReencodeMatchesRowPath verifies that the L3 column-oriented
// re-encode produces the same data as the row-oriented fallback, and that it
// actually fires (config differs from source, so L0 cannot apply).
func TestWriteRowGroupReencodeMatchesRowPath(t *testing.T) {
	rows := makeCopyTestRows(5000)
	src := writeCopyTestFile(t, rows, Compression(&Snappy))

	rewrite := func(reencode bool) []byte {
		defer func(prev bool) { disableWriteReencode = prev }(disableWriteReencode)
		disableWriteReencode = !reencode
		var dst bytes.Buffer
		// Codec differs from source (Snappy -> Zstd) so L0 cannot fire.
		w := NewGenericWriter[copyTestRow](&dst, Compression(&Zstd))
		for _, rg := range src.RowGroups() {
			if _, err := w.WriteRowGroup(rg); err != nil {
				t.Fatalf("WriteRowGroup(reencode=%v): %v", reencode, err)
			}
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		return dst.Bytes()
	}

	before := reencodePathCounter.Load()
	l3 := rewrite(true)
	if reencodePathCounter.Load() == before {
		t.Fatal("expected the L3 re-encode path to fire")
	}
	rowPath := rewrite(false)

	gotL3 := readCopyTestRows(t, l3, len(rows))
	gotRow := readCopyTestRows(t, rowPath, len(rows))
	if !reflect.DeepEqual(gotL3, rows) {
		t.Fatal("L3 output differs from source data")
	}
	if !reflect.DeepEqual(gotL3, gotRow) {
		t.Fatal("L3 output differs from row-path output")
	}
}

// BenchmarkWriteRowGroupReencodePaths compares the L3 column-oriented re-encode
// against the row-oriented fallback. The codec differs from the source (Snappy)
// so L0 cannot fire. The "uncompressed" dest isolates the row round-trip cost
// (no compression in the write); the "zstd" dest reflects a realistic codec
// migration.
func BenchmarkWriteRowGroupReencodePaths(b *testing.B) {
	rows := makeBenchRows(200_000)
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

	run := func(b *testing.B, compression WriterOption, reencode bool) {
		defer func(d bool) { disableWriteReencode = d }(disableWriteReencode)
		disableWriteReencode = !reencode
		b.ReportAllocs()
		b.SetBytes(int64(src.Len()))
		b.ResetTimer()
		for b.Loop() {
			w := NewGenericWriter[benchRow](io.Discard, compression, MaxRowsPerRowGroup(50_000))
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

	for _, c := range []struct {
		name string
		opt  WriterOption
	}{
		{"uncompressed", Compression(&Uncompressed)},
		{"zstd", Compression(&Zstd)},
	} {
		b.Run(c.name+"/rowpath", func(b *testing.B) { run(b, c.opt, false) })
		b.Run(c.name+"/L3", func(b *testing.B) { run(b, c.opt, true) })
	}
}

type wideRow struct {
	C0, C1, C2, C3, C4, C5, C6, C7, C8, C9           int64
	C10, C11, C12, C13, C14, C15, C16, C17, C18, C19 int64
	S0, S1, S2, S3                                   string
}

func makeWideRows(n int) []wideRow {
	rows := make([]wideRow, n)
	for i := range rows {
		v := int64(i)
		rows[i] = wideRow{
			C0: v, C1: v + 1, C2: v + 2, C3: v + 3, C4: v + 4,
			C5: v + 5, C6: v + 6, C7: v + 7, C8: v + 8, C9: v + 9,
			C10: v, C11: v + 1, C12: v + 2, C13: v + 3, C14: v + 4,
			C15: v + 5, C16: v + 6, C17: v + 7, C18: v + 8, C19: v + 9,
			S0: fmt.Sprintf("a%d", i%97), S1: fmt.Sprintf("b%d", i%89),
			S2: fmt.Sprintf("c%d", i%83), S3: fmt.Sprintf("d%d", i%79),
		}
	}
	return rows
}

// BenchmarkWriteRowGroupReencodeWide measures L3 vs the row path on a wide
// (24-column) schema, where the row round-trip has more to do.
func BenchmarkWriteRowGroupReencodeWide(b *testing.B) {
	rows := makeWideRows(100_000)
	var src bytes.Buffer
	sw := NewGenericWriter[wideRow](&src, Compression(&Snappy), MaxRowsPerRowGroup(25_000))
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

	run := func(b *testing.B, reencode bool) {
		defer func(d bool) { disableWriteReencode = d }(disableWriteReencode)
		disableWriteReencode = !reencode
		b.ReportAllocs()
		b.SetBytes(int64(src.Len()))
		b.ResetTimer()
		for b.Loop() {
			w := NewGenericWriter[wideRow](io.Discard, Compression(&Zstd), MaxRowsPerRowGroup(25_000))
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

	b.Run("rowpath", func(b *testing.B) { run(b, false) })
	b.Run("L3", func(b *testing.B) { run(b, true) })
}

// buildIDFile writes a single-row-group file whose rows are sorted by id. The
// caller must pass ids in ascending order. Other fields are derived from id so
// that the fully-sorted sequence of rows is uniquely determined by the id set.
func buildIDFile(t testing.TB, ids []int64) *File {
	t.Helper()
	rows := make([]copyTestRow, len(ids))
	for i, id := range ids {
		rows[i] = copyTestRow{ID: id, Name: fmt.Sprintf("n%d", id%13), Val: float64(id)}
	}
	return writeCopyTestFile(t, rows, SortingWriterConfig(SortingColumns(Ascending("id"))))
}

func idSeq(start, end int64) []int64 {
	out := make([]int64, 0, end-start+1)
	for v := start; v <= end; v++ {
		out = append(out, v)
	}
	return out
}

// TestWriteRowGroupMergeRangePatterns exercises the segment-splitting logic for a
// variety of overlapping/non-overlapping range mixes. For each pattern it
// verifies that the optimized merge (L0 copy of non-overlapping single-row-group
// segments + heap merge of overlapping segments) produces exactly the same
// globally-sorted rows as the pure row-path reference, and that the number of
// column chunks copied matches the expected number of single-row-group segments.
func TestWriteRowGroupMergeRangePatterns(t *testing.T) {
	const numColumns = 3 // id, name, val
	// All source files have the same row count and the destination uses that as
	// MaxRowsPerRowGroup, so non-overlapping segments are never packed together
	// (any two would exceed the limit). Each isolated segment therefore takes the
	// verbatim L0 copy path, letting the copied-chunk count verify the
	// segmentation decision directly. Packing is covered separately.
	const rowsPerFile = 64

	// spec describes one source file: count ids starting at start, spaced by step
	// (so files overlap iff their [start, start+(count-1)*step] ranges intersect,
	// while staying unique within a pattern via disjoint step residues).
	type spec struct{ start, step int64 }

	patterns := []struct {
		name               string
		files              []spec
		wantCopiedSegments int // isolated single-row-group segments (numColumns chunks each)
	}{
		{
			name:               "non_overlapping",
			files:              []spec{{0, 1}, {100, 1}, {200, 1}},
			wantCopiedSegments: 3,
		},
		{
			name:               "all_overlapping",
			files:              []spec{{0, 3}, {1, 3}, {2, 3}},
			wantCopiedSegments: 0,
		},
		{
			name:               "mixed",
			files:              []spec{{0, 1}, {100, 2}, {101, 2}, {300, 1}}, // iso, overlap-pair, iso
			wantCopiedSegments: 2,
		},
		{
			name:               "two_overlapping_pairs",
			files:              []spec{{0, 2}, {1, 2}, {200, 2}, {201, 2}},
			wantCopiedSegments: 0,
		},
		{
			name:               "isolated_then_triple_overlap",
			files:              []spec{{0, 1}, {200, 3}, {201, 3}, {202, 3}},
			wantCopiedSegments: 1,
		},
	}

	for _, p := range patterns {
		t.Run(p.name, func(t *testing.T) {
			files := make([]*File, len(p.files))
			var allIDs []int64
			for i, s := range p.files {
				ids := make([]int64, rowsPerFile)
				for j := range ids {
					ids[j] = s.start + int64(j)*s.step
				}
				files[i] = buildIDFile(t, ids)
				allIDs = append(allIDs, ids...)
			}
			slices.Sort(allIDs)
			expected := make([]copyTestRow, len(allIDs))
			for i, id := range allIDs {
				expected[i] = copyTestRow{ID: id, Name: fmt.Sprintf("n%d", id%13), Val: float64(id)}
			}

			writeMerged := func(fastPaths bool) ([]byte, int64) {
				rgs := make([]RowGroup, len(files))
				for i, f := range files {
					rgs[i] = f.RowGroups()[0]
				}
				merged, err := MergeRowGroups(rgs, SortingRowGroupConfig(SortingColumns(Ascending("id"))))
				if err != nil {
					t.Fatalf("MergeRowGroups: %v", err)
				}
				defer func(c, r bool) { disableWriteCopy = c; disableWriteReencode = r }(disableWriteCopy, disableWriteReencode)
				disableWriteCopy = !fastPaths
				disableWriteReencode = !fastPaths

				before := copyPathCounter.Load()
				var dst bytes.Buffer
				w := NewGenericWriter[copyTestRow](&dst, SortingWriterConfig(SortingColumns(Ascending("id"))), MaxRowsPerRowGroup(rowsPerFile))
				if _, err := w.WriteRowGroup(merged); err != nil {
					t.Fatalf("WriteRowGroup: %v", err)
				}
				if err := w.Close(); err != nil {
					t.Fatalf("Close: %v", err)
				}
				return dst.Bytes(), copyPathCounter.Load() - before
			}

			ref, _ := writeMerged(false)
			opt, copied := writeMerged(true)

			gotRef := readCopyTestRows(t, ref, len(expected))
			gotOpt := readCopyTestRows(t, opt, len(expected))

			if !reflect.DeepEqual(gotRef, expected) {
				t.Fatalf("reference (row-path) output is not globally sorted as expected (%d rows)", len(gotRef))
			}
			if !reflect.DeepEqual(gotOpt, expected) {
				t.Fatalf("optimized output differs from expected globally-sorted rows (%d rows)", len(gotOpt))
			}
			if want := int64(p.wantCopiedSegments * numColumns); copied != want {
				t.Fatalf("copied column chunks = %d, want %d (%d single-row-group segments * %d columns)",
					copied, want, p.wantCopiedSegments, numColumns)
			}
		})
	}
}

// TestWriteRowGroupMergeDropDuplicates verifies that when duplicate rows are
// dropped, the merge is NOT split for verbatim copy (dedup spans segment
// boundaries) and the output is correctly deduplicated and sorted.
func TestWriteRowGroupMergeDropDuplicates(t *testing.T) {
	// Overlapping ranges that also share ids 50..99.
	fileA := buildIDFile(t, idSeq(0, 99))
	fileB := buildIDFile(t, idSeq(50, 149))

	merged, err := MergeRowGroups(
		[]RowGroup{fileA.RowGroups()[0], fileB.RowGroups()[0]},
		SortingRowGroupConfig(SortingColumns(Ascending("id")), DropDuplicatedRows(true)),
	)
	if err != nil {
		t.Fatal(err)
	}

	before := copyPathCounter.Load()
	var dst bytes.Buffer
	w := NewGenericWriter[copyTestRow](&dst, SortingWriterConfig(SortingColumns(Ascending("id"))))
	if _, err := w.WriteRowGroup(merged); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	if copied := copyPathCounter.Load() - before; copied != 0 {
		t.Fatalf("expected no verbatim copy when dropping duplicates, but %d chunks were copied", copied)
	}

	out, err := OpenFile(bytes.NewReader(dst.Bytes()), int64(dst.Len()))
	if err != nil {
		t.Fatal(err)
	}
	// Deduplicated union of [0,99] and [50,149] is [0,149].
	if got := out.NumRows(); got != 150 {
		t.Fatalf("deduplicated row count = %d, want 150", got)
	}
	got := readCopyTestRows(t, dst.Bytes(), 150)
	for i, r := range got {
		if r.ID != int64(i) {
			t.Fatalf("row %d has id %d, want %d (expected deduplicated 0..149 sorted)", i, r.ID, i)
		}
	}
}

// TestWriteRowGroupCopyHonorsSmallerMaxRows verifies that a source row group
// larger than the destination's MaxRowsPerRowGroup is NOT copied verbatim (which
// would exceed the configured limit) but split by the row path instead, while
// still producing correct data.
func TestWriteRowGroupCopyHonorsSmallerMaxRows(t *testing.T) {
	rows := makeCopyTestRows(1000)
	src := writeCopyTestFile(t, rows) // single 1000-row row group

	before := copyPathCounter.Load()
	var dst bytes.Buffer
	w := NewGenericWriter[copyTestRow](&dst, MaxRowsPerRowGroup(100))
	for _, rg := range src.RowGroups() {
		if _, err := w.WriteRowGroup(rg); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	if copied := copyPathCounter.Load() - before; copied != 0 {
		t.Fatalf("expected no verbatim copy when source exceeds MaxRowsPerRowGroup, but %d chunks were copied", copied)
	}

	out, err := OpenFile(bytes.NewReader(dst.Bytes()), int64(dst.Len()))
	if err != nil {
		t.Fatal(err)
	}
	if got := len(out.RowGroups()); got != 10 {
		t.Fatalf("output row group count = %d, want 10 (1000 rows / 100 max)", got)
	}
	for _, rg := range out.RowGroups() {
		if rg.NumRows() > 100 {
			t.Fatalf("output row group exceeds MaxRowsPerRowGroup: %d > 100", rg.NumRows())
		}
	}
	got := readCopyTestRows(t, dst.Bytes(), len(rows))
	if !reflect.DeepEqual(got, rows) {
		t.Fatal("round-tripped rows differ after maxRows split")
	}
}

// TestWriteRowGroupMergePacksToMaxRows verifies that merging many small
// non-overlapping row groups packs them into output row groups up to
// MaxRowsPerRowGroup (undershooting, never exceeding) rather than emitting one
// tiny output row group per source segment, while preserving data and order.
func TestWriteRowGroupMergePacksToMaxRows(t *testing.T) {
	// 20 disjoint files of 100 rows each (ids 0..1999).
	const numFiles, perFile = 20, 100
	files := make([]*File, numFiles)
	for i := range files {
		files[i] = buildIDFile(t, idSeq(int64(i*perFile), int64(i*perFile+perFile-1)))
	}

	write := func(maxRows int64) *File {
		rgs := make([]RowGroup, numFiles)
		for i, f := range files {
			rgs[i] = f.RowGroups()[0]
		}
		merged, err := MergeRowGroups(rgs, SortingRowGroupConfig(SortingColumns(Ascending("id"))))
		if err != nil {
			t.Fatal(err)
		}
		var dst bytes.Buffer
		w := NewGenericWriter[copyTestRow](&dst, SortingWriterConfig(SortingColumns(Ascending("id"))), MaxRowsPerRowGroup(maxRows))
		if _, err := w.WriteRowGroup(merged); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		out, err := OpenFile(bytes.NewReader(dst.Bytes()), int64(dst.Len()))
		if err != nil {
			t.Fatal(err)
		}
		// Verify data and global order.
		got := readCopyTestRows(t, dst.Bytes(), numFiles*perFile)
		for i, r := range got {
			if r.ID != int64(i) {
				t.Fatalf("row %d has id %d, want %d", i, r.ID, i)
			}
		}
		return out
	}

	// Large limit: all 2000 rows pack into a single output row group.
	if out := write(100000); len(out.RowGroups()) != 1 {
		t.Fatalf("with large MaxRowsPerRowGroup: output row groups = %d, want 1 (packed)", len(out.RowGroups()))
	}

	// Limit of 500: 2000 rows pack into 4 output row groups, none exceeding 500.
	out := write(500)
	if got := len(out.RowGroups()); got != 4 {
		t.Fatalf("with MaxRowsPerRowGroup=500: output row groups = %d, want 4", got)
	}
	for i, rg := range out.RowGroups() {
		if rg.NumRows() > 500 {
			t.Fatalf("row group %d has %d rows, exceeds MaxRowsPerRowGroup=500", i, rg.NumRows())
		}
	}
}

// TestWriteRowGroupMergePacksWithBloomFilters verifies that a merge of many
// small, sorted, non-overlapping row groups can still enter the segment-packing
// path when L0 copy is disabled by destination bloom filters. The packed output
// should consolidate several input row groups into fewer larger output row
// groups, rebuild bloom filters, and use the L3 column-oriented re-encode path
// instead of falling back to the row-oriented merged reader path.
func TestWriteRowGroupMergePacksWithBloomFilters(t *testing.T) {
	const numFiles, perFile = 20, 100

	files := make([]*File, numFiles)
	for i := range files {
		files[i] = buildIDFile(t, idSeq(int64(i*perFile), int64(i*perFile+perFile-1)))
	}

	rgs := make([]RowGroup, numFiles)
	for i, f := range files {
		rgs[i] = f.RowGroups()[0]
	}

	merged, err := MergeRowGroups(
		rgs,
		SortingRowGroupConfig(SortingColumns(Ascending("id"))),
	)
	if err != nil {
		t.Fatal(err)
	}

	beforeCopy := copyPathCounter.Load()
	beforeReencode := reencodePathCounter.Load()

	var dst bytes.Buffer
	w := NewGenericWriter[copyTestRow](
		&dst,
		SortingWriterConfig(SortingColumns(Ascending("id"))),
		MaxRowsPerRowGroup(500),
		BloomFilters(SplitBlockFilter(10, "id")),
	)
	if _, err := w.WriteRowGroup(merged); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	if copied := copyPathCounter.Load() - beforeCopy; copied != 0 {
		t.Fatalf("expected bloom filters to disable L0 copy, but copied %d column chunks", copied)
	}
	if reencoded := reencodePathCounter.Load() - beforeReencode; reencoded == 0 {
		t.Fatal("expected packed merge with bloom filters to use the L3 re-encode path")
	}

	out, err := OpenFile(bytes.NewReader(dst.Bytes()), int64(dst.Len()))
	if err != nil {
		t.Fatal(err)
	}
	if got := len(out.RowGroups()); got != 4 {
		t.Fatalf("output row groups = %d, want 4 packed groups of 500 rows", got)
	}
	if got := out.NumRows(); got != numFiles*perFile {
		t.Fatalf("output row count = %d, want %d", got, numFiles*perFile)
	}

	for i, rg := range out.RowGroups() {
		if rg.NumRows() > 500 {
			t.Fatalf("row group %d has %d rows, exceeds MaxRowsPerRowGroup=500", i, rg.NumRows())
		}
		filter := rg.ColumnChunks()[0].BloomFilter()
		if filter == nil {
			t.Fatalf("row group %d id column has no bloom filter", i)
		}

		rowBase := int64(i * 500)
		for _, id := range []int64{rowBase, rowBase + rg.NumRows()/2, rowBase + rg.NumRows() - 1} {
			ok, err := filter.Check(ValueOf(id))
			if err != nil {
				t.Fatalf("checking bloom filter for id=%d: %v", id, err)
			}
			if !ok {
				t.Fatalf("bloom filter for row group %d returned false for present id=%d", i, id)
			}
		}
	}

	got := readCopyTestRows(t, dst.Bytes(), numFiles*perFile)
	if len(got) != numFiles*perFile {
		t.Fatalf("read %d rows, want %d", len(got), numFiles*perFile)
	}
	for i, r := range got {
		if r.ID != int64(i) {
			t.Fatalf("row %d has id %d, want %d", i, r.ID, i)
		}
	}
}

// BenchmarkWriteRowGroupMergePackPaths compares the row-oriented packed merge
// path against the optimized column-oriented packed merge path for the same
// scenario as TestWriteRowGroupMergePacksWithBloomFilters:
//
//   - many small sorted, non-overlapping file-backed row groups
//   - destination packs them into larger row groups
//   - destination rebuilds a bloom filter for "id"
//
// "rowpath" disables both write fast paths, so WriteRowGroup(merged) falls back
// to reading merged.Rows() and writing rows. "columnpack" leaves the fast paths
// enabled, so ordered segments are packed column-by-column.
func BenchmarkWriteRowGroupMergePackPaths(b *testing.B) {
	const (
		numFiles = 20
		perFile  = 1_000
		maxRows  = 5_000
	)

	files := make([]*File, numFiles)
	for i := range files {
		files[i] = buildIDFile(b, idSeq(int64(i*perFile), int64(i*perFile+perFile-1)))
	}

	rowGroups := make([]RowGroup, numFiles)
	for i, f := range files {
		rowGroups[i] = f.RowGroups()[0]
	}

	merged, err := MergeRowGroups(
		rowGroups,
		SortingRowGroupConfig(SortingColumns(Ascending("id"))),
	)
	if err != nil {
		b.Fatal(err)
	}

	writerOptions := []WriterOption{
		SortingWriterConfig(SortingColumns(Ascending("id"))),
		MaxRowsPerRowGroup(maxRows),
		BloomFilters(SplitBlockFilter(10, "id")),
	}

	run := func(b *testing.B, fastPaths bool) {
		defer func(copyDisabled, reencodeDisabled bool) {
			disableWriteCopy = copyDisabled
			disableWriteReencode = reencodeDisabled
		}(disableWriteCopy, disableWriteReencode)

		disableWriteCopy = !fastPaths
		disableWriteReencode = !fastPaths

		b.ReportAllocs()
		b.SetBytes(int64(numFiles * perFile))
		b.ResetTimer()

		for b.Loop() {
			w := NewGenericWriter[copyTestRow](io.Discard, writerOptions...)
			if _, err := w.WriteRowGroup(merged); err != nil {
				b.Fatal(err)
			}
			if err := w.Close(); err != nil {
				b.Fatal(err)
			}
		}
	}

	b.Run("rowpath", func(b *testing.B) {
		run(b, false)
	})
	b.Run("columnpack", func(b *testing.B) {
		run(b, true)
	})
}

// TestSortingWriterUsesFastPaths verifies that SortingWriter's close-time merge
// goes through WriteRowGroup and benefits from the chunk-level fast paths when
// the sorted flushes do not overlap (packing them toward MaxRowsPerRowGroup),
// while producing correctly sorted output.
func TestSortingWriterUsesFastPaths(t *testing.T) {
	var buf bytes.Buffer
	sw := NewSortingWriter[copyTestRow](&buf, 1000, SortingWriterConfig(SortingColumns(Ascending("id"))))
	rows := make([]copyTestRow, 5000)
	for i := range rows {
		rows[i] = copyTestRow{ID: int64(i), Name: fmt.Sprintf("n%d", i%13), Val: float64(i)}
	}

	beforeCopy, beforeL3 := copyPathCounter.Load(), reencodePathCounter.Load()
	if _, err := sw.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := sw.Close(); err != nil {
		t.Fatal(err)
	}

	// Already-sorted input produces non-overlapping flush row groups, which the
	// writer packs column-wise (or copies verbatim) instead of heap-merging rows.
	if copyPathCounter.Load() == beforeCopy && reencodePathCounter.Load() == beforeL3 {
		t.Fatal("expected SortingWriter close to use a chunk-level fast path for non-overlapping flushes")
	}

	got := readCopyTestRows(t, buf.Bytes(), 5000)
	if len(got) != 5000 {
		t.Fatalf("read %d rows, want 5000", len(got))
	}
	for i := range got {
		if got[i].ID != int64(i) {
			t.Fatalf("row %d has id %d, want %d", i, got[i].ID, i)
		}
	}
}

// TestSortingWriterDropDuplicatedRows verifies dedup still applies through the
// WriteRowGroup-based close path, for both overlapping and non-overlapping
// flushes.
func TestSortingWriterDropDuplicatedRows(t *testing.T) {
	var buf bytes.Buffer
	sw := NewSortingWriter[copyTestRow](&buf, 4,
		SortingWriterConfig(SortingColumns(Ascending("id")), DropDuplicatedRows(true)))
	// Duplicates within a flush and across flushes (flush size 4).
	ids := []int64{3, 1, 2, 3, 3, 4, 5, 5, 6, 7, 8, 8}
	rows := make([]copyTestRow, len(ids))
	for i, id := range ids {
		rows[i] = copyTestRow{ID: id, Name: "x", Val: float64(id)}
	}
	if _, err := sw.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := sw.Close(); err != nil {
		t.Fatal(err)
	}

	got := readCopyTestRows(t, buf.Bytes(), len(ids))
	want := []int64{1, 2, 3, 4, 5, 6, 7, 8}
	if len(got) != len(want) {
		t.Fatalf("read %d rows, want %d deduplicated", len(got), len(want))
	}
	for i, id := range want {
		if got[i].ID != id {
			t.Fatalf("row %d has id %d, want %d", i, got[i].ID, id)
		}
	}
}

// TestWriteRowGroupCopyBloomFilter verifies that a writer configured with a
// bloom filter still takes the verbatim copy path when the source chunk carries
// an equivalent filter, that the copied filter works in the output file, and
// that mismatched or absent source filters demote to a rebuild.
func TestWriteRowGroupCopyBloomFilter(t *testing.T) {
	rows := makeCopyTestRows(1000)
	filter10 := SplitBlockFilter(10, "id")

	rewrite := func(src *File, opts ...WriterOption) (*File, int64, int64) {
		beforeCopy, beforeL3 := copyPathCounter.Load(), reencodePathCounter.Load()
		var dst bytes.Buffer
		w := NewGenericWriter[copyTestRow](&dst, opts...)
		for _, rg := range src.RowGroups() {
			if _, err := w.WriteRowGroup(rg); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		out, err := OpenFile(bytes.NewReader(dst.Bytes()), int64(dst.Len()))
		if err != nil {
			t.Fatal(err)
		}
		return out, copyPathCounter.Load() - beforeCopy, reencodePathCounter.Load() - beforeL3
	}

	checkFilter := func(t *testing.T, out *File) {
		t.Helper()
		bf := out.RowGroups()[0].ColumnChunks()[0].BloomFilter()
		if bf == nil {
			t.Fatal("output column has no bloom filter")
		}
		for _, id := range []int64{0, 1, 500, 999} {
			ok, err := bf.Check(ValueOf(id))
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				t.Fatalf("bloom filter misses present value %d", id)
			}
		}
		misses := 0
		for id := int64(10_000); id < 10_100; id++ {
			ok, err := bf.Check(ValueOf(id))
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				misses++
			}
		}
		if misses == 0 {
			t.Fatal("bloom filter matches every absent value; likely bogus")
		}
	}

	t.Run("matching_filter_copies", func(t *testing.T) {
		src := writeCopyTestFile(t, rows, BloomFilters(filter10))
		out, copied, _ := rewrite(src, BloomFilters(filter10))
		if copied == 0 {
			t.Fatal("expected verbatim copy with matching source bloom filter")
		}
		checkFilter(t, out)
	})

	t.Run("missing_source_filter_demotes", func(t *testing.T) {
		src := writeCopyTestFile(t, rows) // no filter in source
		out, copied, l3 := rewrite(src, BloomFilters(filter10))
		if copied != 0 {
			t.Fatalf("expected demotion when source lacks a bloom filter, %d chunks copied", copied)
		}
		if l3 == 0 {
			t.Fatal("expected L3 rebuild when source lacks a bloom filter")
		}
		checkFilter(t, out)
	})

	t.Run("different_bits_per_value_demotes", func(t *testing.T) {
		src := writeCopyTestFile(t, rows, BloomFilters(SplitBlockFilter(2, "id")))
		out, copied, l3 := rewrite(src, BloomFilters(filter10))
		if copied != 0 {
			t.Fatalf("expected demotion on bits-per-value mismatch, %d chunks copied", copied)
		}
		if l3 == 0 {
			t.Fatal("expected L3 rebuild on bits-per-value mismatch")
		}
		checkFilter(t, out)
	})
}

// TestWriteRowGroupBufferUsesL3 verifies that writing an in-memory
// parquet.Buffer through WriteRowGroup takes the L3 column-oriented path
// (buffers are columnar and order-consistent across columns) and produces the
// same data as the row-oriented path, including optional columns.
func TestWriteRowGroupBufferUsesL3(t *testing.T) {
	type optRow struct {
		ID   int64    `parquet:"id"`
		Name string   `parquet:"name,dict"`
		Opt  *float64 `parquet:"opt,optional"`
	}

	rows := make([]optRow, 500)
	for i := range rows {
		rows[i] = optRow{ID: int64(i), Name: fmt.Sprintf("n%d", i%7)}
		if i%3 != 0 {
			v := float64(i) * 0.5
			rows[i].Opt = &v
		}
	}

	write := func(l3 bool) []byte {
		defer func(prev bool) { disableWriteReencode = prev }(disableWriteReencode)
		disableWriteReencode = !l3

		buffer := NewGenericBuffer[optRow]()
		if _, err := buffer.Write(rows); err != nil {
			t.Fatal(err)
		}
		var dst bytes.Buffer
		w := NewGenericWriter[optRow](&dst)
		before := reencodePathCounter.Load()
		if _, err := w.WriteRowGroup(buffer); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		if fired := reencodePathCounter.Load() > before; fired != l3 {
			t.Fatalf("L3 fired=%v, want %v", fired, l3)
		}
		return dst.Bytes()
	}

	l3Out := write(true)
	rowOut := write(false)

	readAll := func(b []byte) []optRow {
		r := NewGenericReader[optRow](bytes.NewReader(b))
		defer r.Close()
		out := make([]optRow, len(rows)+1)
		n, _ := r.Read(out)
		return out[:n]
	}

	gotL3 := readAll(l3Out)
	gotRow := readAll(rowOut)
	if !reflect.DeepEqual(gotL3, rows) {
		t.Fatal("L3 buffer output differs from source rows")
	}
	if !reflect.DeepEqual(gotL3, gotRow) {
		t.Fatal("L3 buffer output differs from row-path output")
	}
}
