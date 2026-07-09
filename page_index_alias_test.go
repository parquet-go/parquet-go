package parquet_test

import (
	"bytes"
	"testing"

	"github.com/parquet-go/parquet-go"
)

// TestReadPageIndexDoesNotAliasOffsetIndex is a regression test for the column
// index and the offset index sharing one buffer.
//
// Values decoded from a thrift byte reader alias the bytes they were decoded
// from: ColumnIndex.MinValues and MaxValues are sub-slices of the input. If the
// column index and the offset index are read into the same buffer, reading the
// offset index overwrites the bytes the min/max statistics point at, and every
// column silently reports corrupt bounds.
//
// The file below is shaped so the corruption is visible: several row groups and
// several pages per row group, so both indexes are large enough that the offset
// index overlaps the min/max bytes of the column index.
func TestReadPageIndexDoesNotAliasOffsetIndex(t *testing.T) {
	type row struct {
		Key   int64  `parquet:"key"`
		Label string `parquet:"label"`
	}

	const rowsPerGroup = 200

	buf := new(bytes.Buffer)
	w := parquet.NewGenericWriter[row](buf,
		parquet.PageBufferSize(512), // force many pages per chunk
		parquet.MaxRowsPerRowGroup(rowsPerGroup),
		parquet.DataPageStatistics(true),
	)

	rows := make([]row, 0, 3*rowsPerGroup)
	for i := range 3 * rowsPerGroup {
		rows = append(rows, row{Key: int64(i), Label: string(rune('a' + i%26))})
	}
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	b := buf.Bytes()
	f, err := parquet.OpenFile(bytes.NewReader(b), int64(len(b)))
	if err != nil {
		t.Fatal(err)
	}

	columnIndexes, offsetIndexes, err := f.ReadPageIndex()
	if err != nil {
		t.Fatal(err)
	}
	if len(columnIndexes) == 0 || len(offsetIndexes) == 0 {
		t.Fatal("expected the file to carry a page index")
	}

	// The "key" column is column 0. Its per-page bounds must be 8-byte little
	// endian int64s, non-decreasing across pages, and inside [0, len(rows)).
	// Corruption from an aliased buffer shows up as wrong lengths or as bounds
	// far outside the written range.
	numColumns := len(f.Metadata().RowGroups[0].Columns)

	for rg := range f.Metadata().RowGroups {
		index := columnIndexes[rg*numColumns]

		for page := range index.MinValues {
			minValue, maxValue := index.MinValues[page], index.MaxValues[page]

			if len(minValue) != 8 || len(maxValue) != 8 {
				t.Fatalf("rowGroup=%d page=%d: min/max are %d/%d bytes, want 8 (buffer was overwritten)",
					rg, page, len(minValue), len(maxValue))
			}

			lo := int64(0)
			hi := int64(0)
			for i := 7; i >= 0; i-- {
				lo = lo<<8 | int64(minValue[i])
				hi = hi<<8 | int64(maxValue[i])
			}
			if lo < 0 || hi >= int64(len(rows)) || lo > hi {
				t.Fatalf("rowGroup=%d page=%d: bounds [%d,%d] outside [0,%d) (buffer was overwritten)",
					rg, page, lo, hi, len(rows))
			}
		}
	}
}
