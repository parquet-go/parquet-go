package parquet

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"

	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"
)

// TestFilePagesSeekToRowEmptyOffsetIndex tests that SeekToRow(0) returns nil
// (not ErrSeekOutOfRange) when the offset index exists but has no page
// locations. This happens with 0-row row groups written by other Parquet
// implementations (e.g., arrow-go) that still include OffsetIndex metadata.
func TestFilePagesSeekToRowEmptyOffsetIndex(t *testing.T) {
	chunk := &FileColumnChunk{
		file: &File{config: DefaultFileConfig()},
		chunk: &format.ColumnChunk{
			MetaData: format.ColumnMetaData{
				DataPageOffset:      0,
				TotalCompressedSize: 0,
			},
		},
	}
	// Store an offset index with zero page locations.
	chunk.offsetIndex.Store(&FileOffsetIndex{
		index: &format.OffsetIndex{
			PageLocations: []format.PageLocation{},
		},
	})

	var fp FilePages
	fp.chunk = chunk
	fp.section = *io.NewSectionReader(emptyReaderAt{}, 0, 0)

	// SeekToRow(0) should succeed for a 0-row row group.
	if err := fp.SeekToRow(0); err != nil {
		t.Fatalf("SeekToRow(0) with empty offset index: got %v, want nil", err)
	}

	// SeekToRow(1) should still fail.
	if err := fp.SeekToRow(1); err != ErrSeekOutOfRange {
		t.Fatalf("SeekToRow(1) with empty offset index: got %v, want %v", err, ErrSeekOutOfRange)
	}
}

// TestFilePagesSeekToRowEmptyOffsetIndexResetsSkip verifies that SeekToRow(0)
// on an empty offset index resets f.skip to 0. This matters when a prior seek
// set skip to a non-zero value (e.g., on a different column in the same row
// group); without the reset, ReadPage could try to slice into an empty page
// using the stale skip value.
func TestFilePagesSeekToRowEmptyOffsetIndexResetsSkip(t *testing.T) {
	chunk := &FileColumnChunk{
		file: &File{config: DefaultFileConfig()},
		chunk: &format.ColumnChunk{
			MetaData: format.ColumnMetaData{
				DataPageOffset:      0,
				TotalCompressedSize: 0,
			},
		},
	}
	chunk.offsetIndex.Store(&FileOffsetIndex{
		index: &format.OffsetIndex{
			PageLocations: []format.PageLocation{},
		},
	})

	var fp FilePages
	fp.chunk = chunk
	fp.section = *io.NewSectionReader(emptyReaderAt{}, 0, 0)

	// Simulate a stale skip from a prior seek.
	fp.skip = 42

	if err := fp.SeekToRow(0); err != nil {
		t.Fatalf("SeekToRow(0): got %v, want nil", err)
	}
	if fp.skip != 0 {
		t.Fatalf("after SeekToRow(0): skip = %d, want 0", fp.skip)
	}
}

type emptyReaderAt struct{}

func (emptyReaderAt) ReadAt(p []byte, off int64) (int, error) { return 0, io.EOF }

// TestReadFileWithTrailingZeroRowRowGroup is an end-to-end test that constructs
// a parquet file with a trailing 0-row row group (as produced by arrow-go
// compaction pipelines) and verifies that reading through all row groups with
// ReadRows completes without error.
//
// The file layout mimics the production scenario:
//   - Row group 0: N rows of data with page index metadata
//   - Row group 1: 0 rows with empty OffsetIndex/ColumnIndex metadata
func TestReadFileWithTrailingZeroRowRowGroup(t *testing.T) {
	// Step 1: Write a normal parquet file with some data.
	type TestRow struct {
		ID   int64  `parquet:"id"`
		Name string `parquet:"name"`
	}
	testRows := []TestRow{
		{ID: 1, Name: "alice"},
		{ID: 2, Name: "bob"},
		{ID: 3, Name: "charlie"},
	}

	var buf bytes.Buffer
	w := NewWriter(&buf)
	for i := range testRows {
		if err := w.Write(&testRows[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Step 2: Re-open the file, decode the footer, and inject a trailing
	// 0-row row group with offset index metadata.
	data := buf.Bytes()
	patchedData := injectEmptyRowGroup(t, data)

	// Step 3: Open the patched file and read through all row groups.
	reader := bytes.NewReader(patchedData)
	f, err := OpenFile(reader, int64(len(patchedData)))
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}

	rowGroups := f.RowGroups()
	if len(rowGroups) != 2 {
		t.Fatalf("expected 2 row groups, got %d", len(rowGroups))
	}

	// The first row group should have 3 rows.
	if n := rowGroups[0].NumRows(); n != 3 {
		t.Fatalf("row group 0: expected 3 rows, got %d", n)
	}
	// The second row group should have 0 rows.
	if n := rowGroups[1].NumRows(); n != 0 {
		t.Fatalf("row group 1: expected 0 rows, got %d", n)
	}

	// Read all rows from each row group using NewRowGroupRowReader, which is
	// the code path that triggers SeekToRow(0) on the first ReadRows call.
	totalRows := 0
	for i, rg := range rowGroups {
		reader := NewRowGroupRowReader(rg)
		defer reader.Close()

		rowBuf := make([]Row, 10)
		for {
			n, err := reader.ReadRows(rowBuf)
			totalRows += n
			if err != nil {
				if err == io.EOF {
					break
				}
				t.Fatalf("row group %d: ReadRows: %v", i, err)
			}
		}
	}

	if totalRows != 3 {
		t.Fatalf("expected 3 total rows, got %d", totalRows)
	}
}

// injectEmptyRowGroup takes a valid parquet file's bytes and returns a new file
// with an additional trailing 0-row row group that has column and offset index
// metadata (empty page locations). This mimics files produced by arrow-go's
// compaction pipeline.
func injectEmptyRowGroup(t *testing.T, data []byte) []byte {
	t.Helper()

	// Parse footer size from the last 8 bytes: [4-byte footer size][PAR1]
	n := len(data)
	footerSize := int(binary.LittleEndian.Uint32(data[n-8 : n-4]))
	footerData := data[n-8-footerSize : n-8]

	var proto thrift.CompactProtocol
	var meta format.FileMetaData
	if err := thrift.Unmarshal(&proto, footerData, &meta); err != nil {
		t.Fatalf("unmarshal footer: %v", err)
	}

	if len(meta.RowGroups) == 0 {
		t.Fatal("expected at least 1 row group in source file")
	}

	// Clone the column structure from the first row group to build the
	// empty row group. We need the same number of columns with the same
	// type/path metadata so the schema lines up.
	srcRG := meta.RowGroups[0]
	numCols := len(srcRG.Columns)

	emptyColumns := make([]format.ColumnChunk, numCols)
	for i := range numCols {
		emptyColumns[i] = format.ColumnChunk{
			MetaData: format.ColumnMetaData{
				Type:         srcRG.Columns[i].MetaData.Type,
				Encoding:     srcRG.Columns[i].MetaData.Encoding,
				PathInSchema: srcRG.Columns[i].MetaData.PathInSchema,
				Codec:        srcRG.Columns[i].MetaData.Codec,
				// Point data offset at the start of the first row group's data
				// (a valid offset in the file). No data will actually be read
				// for a 0-row row group.
				DataPageOffset:        srcRG.Columns[i].MetaData.DataPageOffset,
				TotalCompressedSize:   0,
				TotalUncompressedSize: 0,
				NumValues:             0,
			},
		}
	}

	emptyRG := format.RowGroup{
		Columns:       emptyColumns,
		TotalByteSize: 0,
		NumRows:       0,
		Ordinal:       int16(len(meta.RowGroups)),
	}
	meta.RowGroups = append(meta.RowGroups, emptyRG)

	// Now we need to rebuild the page index section. We'll write:
	// [original data pages] [column indexes] [offset indexes] [footer] [footer size + PAR1]
	//
	// The original file layout is:
	// [PAR1] [row group data] [column indexes] [offset indexes] [footer] [4-byte footer size] [PAR1]
	//
	// We figure out where the page index section starts from the first row group's
	// column metadata.
	pageIndexStart := int64(n) // default: end of file if no page index
	for _, col := range srcRG.Columns {
		if col.ColumnIndexOffset > 0 && col.ColumnIndexOffset < pageIndexStart {
			pageIndexStart = col.ColumnIndexOffset
		}
		if col.OffsetIndexOffset > 0 && col.OffsetIndexOffset < pageIndexStart {
			pageIndexStart = col.OffsetIndexOffset
		}
	}

	// Start building the new file: keep everything up to the page index section.
	var out bytes.Buffer
	out.Write(data[:pageIndexStart])

	// Write column indexes for all row groups.
	encoder := thrift.NewEncoder(proto.NewWriter(&out))
	baseOffset := pageIndexStart

	// Re-encode column indexes for the original row group(s)
	for i := range len(meta.RowGroups) - 1 {
		rg := &meta.RowGroups[i]
		for j := range rg.Columns {
			col := &rg.Columns[j]
			// Read original column index data
			origOffset := col.ColumnIndexOffset
			origLength := int64(col.ColumnIndexLength)
			if origOffset == 0 || origLength == 0 {
				continue
			}
			var ci format.ColumnIndex
			if err := thrift.Unmarshal(&proto, data[origOffset:origOffset+origLength], &ci); err != nil {
				t.Fatalf("unmarshal column index [%d][%d]: %v", i, j, err)
			}

			col.ColumnIndexOffset = baseOffset + int64(out.Len()) - pageIndexStart
			before := out.Len()
			if err := encoder.Encode(&ci); err != nil {
				t.Fatalf("encode column index: %v", err)
			}
			col.ColumnIndexLength = int32(out.Len() - before)
		}
	}

	// Write empty column indexes for the trailing 0-row row group
	emptyRGIdx := len(meta.RowGroups) - 1
	for j := range meta.RowGroups[emptyRGIdx].Columns {
		col := &meta.RowGroups[emptyRGIdx].Columns[j]
		emptyCI := format.ColumnIndex{}
		col.ColumnIndexOffset = baseOffset + int64(out.Len()) - pageIndexStart
		before := out.Len()
		if err := encoder.Encode(&emptyCI); err != nil {
			t.Fatalf("encode empty column index: %v", err)
		}
		col.ColumnIndexLength = int32(out.Len() - before)
	}

	// Write offset indexes for the original row group(s)
	for i := range len(meta.RowGroups) - 1 {
		rg := &meta.RowGroups[i]
		for j := range rg.Columns {
			col := &rg.Columns[j]
			origOffset := col.OffsetIndexOffset
			origLength := int64(col.OffsetIndexLength)
			if origOffset == 0 || origLength == 0 {
				continue
			}
			var oi format.OffsetIndex
			if err := thrift.Unmarshal(&proto, data[origOffset:origOffset+origLength], &oi); err != nil {
				t.Fatalf("unmarshal offset index [%d][%d]: %v", i, j, err)
			}

			col.OffsetIndexOffset = baseOffset + int64(out.Len()) - pageIndexStart
			before := out.Len()
			if err := encoder.Encode(&oi); err != nil {
				t.Fatalf("encode offset index: %v", err)
			}
			col.OffsetIndexLength = int32(out.Len() - before)
		}
	}

	// Write empty offset indexes for the trailing 0-row row group (0 page locations)
	for j := range meta.RowGroups[emptyRGIdx].Columns {
		col := &meta.RowGroups[emptyRGIdx].Columns[j]
		emptyOI := format.OffsetIndex{
			PageLocations: []format.PageLocation{}, // key: empty page locations
		}
		col.OffsetIndexOffset = baseOffset + int64(out.Len()) - pageIndexStart
		before := out.Len()
		if err := encoder.Encode(&emptyOI); err != nil {
			t.Fatalf("encode empty offset index: %v", err)
		}
		col.OffsetIndexLength = int32(out.Len() - before)
	}

	// Write the footer (FileMetaData)
	footerStart := out.Len()
	if err := encoder.Encode(&meta); err != nil {
		t.Fatalf("encode footer: %v", err)
	}
	newFooterSize := out.Len() - footerStart

	// Write footer size + magic
	var trailer [8]byte
	binary.LittleEndian.PutUint32(trailer[:4], uint32(newFooterSize))
	copy(trailer[4:], "PAR1")
	out.Write(trailer[:])

	return out.Bytes()
}
