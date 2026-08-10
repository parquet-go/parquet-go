package parquet

import (
	"bytes"
	"fmt"
	"io"
	"reflect"
	"slices"
	"testing"
)

type rangeTestRow struct {
	ID   int64    `parquet:"id"`
	Name string   `parquet:"name,dict"`
	Opt  *float64 `parquet:"opt,optional"`
	List []int32  `parquet:"list,list"`
}

func makeRangeTestRows(n int) []rangeTestRow {
	rows := make([]rangeTestRow, n)
	for i := range rows {
		rows[i] = rangeTestRow{ID: int64(i), Name: fmt.Sprintf("n%d", i%7)}
		if i%3 != 0 {
			v := float64(i) * 0.5
			rows[i].Opt = &v
		}
		// Variable-length lists (including empty) exercise repeated columns
		// where row count != value count.
		for j := range i % 4 {
			rows[i].List = append(rows[i].List, int32(i+j))
		}
	}
	return rows
}

func writeRangeTestFile(t testing.TB, rows []rangeTestRow, opts ...WriterOption) *File {
	t.Helper()
	var buf bytes.Buffer
	w := NewGenericWriter[rangeTestRow](&buf, opts...)
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func readRowGroupRows(t *testing.T, rg RowGroup) []Row {
	t.Helper()
	rows := rg.Rows()
	defer rows.Close()
	var out []Row
	buf := make([]Row, 33) // odd batch size to exercise batch boundaries
	for {
		n, err := rows.ReadRows(buf)
		for _, row := range buf[:n] {
			out = append(out, row.Clone())
		}
		if err == io.EOF {
			return out
		}
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			t.Fatal("no progress")
		}
	}
}

// TestRowRangeRowGroup verifies that a range view of a file row group returns
// exactly the base rows [off, off+length), across offsets that start and end
// mid-page, with optional and repeated columns, for both data page versions.
func TestRowRangeRowGroup(t *testing.T) {
	rows := makeRangeTestRows(1000)

	for _, version := range []int{1, 2} {
		t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
			// Small pages so ranges span several pages and cut mid-page.
			f := writeRangeTestFile(t, rows, DataPageVersion(version), PageBufferSize(1024))
			base := f.RowGroups()[0]
			all := readRowGroupRows(t, base)
			if len(all) != len(rows) {
				t.Fatalf("base read %d rows, want %d", len(all), len(rows))
			}

			for _, r := range []struct{ off, length int64 }{
				{0, 10},    // prefix
				{990, 10},  // suffix
				{1, 998},   // all but first and last
				{500, 1},   // single mid-file row
				{123, 456}, // arbitrary mid-page to mid-page
			} {
				t.Run(fmt.Sprintf("off=%d,len=%d", r.off, r.length), func(t *testing.T) {
					view := newRowRangeRowGroup(base, r.off, r.length)
					if got := view.NumRows(); got != r.length {
						t.Fatalf("NumRows = %d, want %d", got, r.length)
					}
					got := readRowGroupRows(t, view)
					if int64(len(got)) != r.length {
						t.Fatalf("read %d rows, want %d", len(got), r.length)
					}
					for i, row := range got {
						want := all[r.off+int64(i)]
						if !row.Equal(want) {
							t.Fatalf("row %d differs:\ngot  %v\nwant %v", i, row, want)
						}
					}
				})
			}
		})
	}
}

// TestRowRangeRowGroupWriteRoundTrip writes range views through a writer (which
// exercises the column-oriented read paths end to end) and verifies the output.
func TestRowRangeRowGroupWriteRoundTrip(t *testing.T) {
	rows := makeRangeTestRows(600)
	f := writeRangeTestFile(t, rows, PageBufferSize(2048))
	base := f.RowGroups()[0]

	var dst bytes.Buffer
	w := NewGenericWriter[rangeTestRow](&dst)
	// Two disjoint ranges written in order: rows [50, 250) and [250, 600).
	for _, r := range []struct{ off, length int64 }{{50, 200}, {250, 350}} {
		if _, err := w.WriteRowGroup(newRowRangeRowGroup(base, r.off, r.length)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r := NewGenericReader[rangeTestRow](bytes.NewReader(dst.Bytes()))
	defer r.Close()
	got := make([]rangeTestRow, 600)
	n, _ := r.Read(got)
	if n != 550 {
		t.Fatalf("read %d rows, want 550", n)
	}
	// Normalize nil vs empty list representations before comparing.
	normalize := func(rows []rangeTestRow) {
		for i := range rows {
			if len(rows[i].List) == 0 {
				rows[i].List = nil
			}
		}
	}
	want := slices.Clone(rows[50:600])
	normalize(got[:n])
	normalize(want)
	if !reflect.DeepEqual(got[:n], want) {
		t.Fatal("round-tripped range rows differ from source")
	}
}

// TestRangeColumnChunkMetadata verifies metadata suppression and NumValues
// exactness reporting.
func TestRangeColumnChunkMetadata(t *testing.T) {
	rows := makeRangeTestRows(100)
	f := writeRangeTestFile(t, rows)
	view := newRowRangeRowGroup(f.RowGroups()[0], 10, 50)

	schema := f.RowGroups()[0].Schema()
	repeated := make([]bool, len(view.chunks))
	forEachLeafColumnOf(schema, func(leaf leafColumn) {
		repeated[leaf.columnIndex] = leaf.maxRepetitionLevel > 0
	})

	sawRepeated := false
	for i, chunk := range view.chunks {
		rc := chunk.(*rangeColumnChunk)
		if _, err := rc.ColumnIndex(); err != ErrMissingColumnIndex {
			t.Fatalf("column %d: ColumnIndex error = %v, want ErrMissingColumnIndex", i, err)
		}
		if _, err := rc.OffsetIndex(); err != ErrMissingOffsetIndex {
			t.Fatalf("column %d: OffsetIndex error = %v, want ErrMissingOffsetIndex", i, err)
		}
		if rc.BloomFilter() != nil {
			t.Fatalf("column %d: BloomFilter should be nil", i)
		}
		if repeated[i] {
			sawRepeated = true
			if rc.exactNumValues() {
				t.Fatalf("column %d: repeated column must not report exact NumValues", i)
			}
			if rc.NumValues() < 50 {
				t.Fatalf("column %d: NumValues upper bound %d below row count", i, rc.NumValues())
			}
		} else {
			if !rc.exactNumValues() {
				t.Fatalf("column %d: non-repeated column must report exact NumValues", i)
			}
			if rc.NumValues() != 50 {
				t.Fatalf("column %d: NumValues = %d, want 50", i, rc.NumValues())
			}
		}
	}
	if !sawRepeated {
		t.Fatal("test schema must include a repeated column")
	}
}
