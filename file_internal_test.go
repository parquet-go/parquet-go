package parquet

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	"github.com/parquet-go/parquet-go/encoding"
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

func newDictionaryPageHeader(body int32) []byte {
	header := format.PageHeader{
		Type:                 format.DictionaryPage,
		UncompressedPageSize: body,
		CompressedPageSize:   body,
	}
	var buf bytes.Buffer
	proto := thrift.CompactProtocol{}
	if err := thrift.NewEncoder(proto.NewWriter(&buf)).Encode(&header); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func dictionaryFilePages(t *testing.T, data []byte) FilePages {
	t.Helper()
	var fp FilePages
	fp.chunk = &FileColumnChunk{column: &Column{path: columnPath{"", "values"}}}
	fp.dictionary = Int32Type.NewDictionary(0, 0, encoding.Int32Values(nil))
	fp.rbuf = bufio.NewReader(bytes.NewReader(data))
	fp.decoder.Reset(fp.protocol.NewReader(fp.rbuf))
	return fp
}

func TestReadPageDiscardShortDictionaryPage(t *testing.T) {
	fp := dictionaryFilePages(t, newDictionaryPageHeader(8))
	_, err := fp.ReadPage()
	if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("ReadPage: got %v, want a short read", err)
	}
}

func TestReadPageDiscardDuplicateDictionaryPage(t *testing.T) {
	body := []byte{1, 2, 3, 4}
	data := append(newDictionaryPageHeader(int32(len(body))), body...)
	fp := dictionaryFilePages(t, data)
	_, err := fp.ReadPage()
	if !errors.Is(err, io.EOF) {
		t.Fatalf("ReadPage after discarding the dictionary page: got %v, want EOF", err)
	}
}

func buildSingleColumnParquet(t *testing.T, version int32, numRows, numValues, dictOffset, dataOffset int64, writeEmptyDictPage bool) []byte {
	t.Helper()

	var out bytes.Buffer
	out.WriteString("PAR1")

	var proto thrift.CompactProtocol
	enc := thrift.NewEncoder(proto.NewWriter(&out))

	var chunkSize int64
	if writeEmptyDictPage {
		dictHeader := format.PageHeader{
			Type:                 format.DictionaryPage,
			UncompressedPageSize: 0,
			CompressedPageSize:   0,
			DictionaryPageHeader: thrift.New(format.DictionaryPageHeader{
				NumValues: 0,
				Encoding:  format.Plain,
			}),
		}
		before := out.Len()
		if err := enc.Encode(&dictHeader); err != nil {
			t.Fatalf("encode dictionary page header: %v", err)
		}
		chunkSize = int64(out.Len() - before)
	}

	encodings := []format.Encoding{format.Plain}
	if dictOffset != 0 {
		if version == 2 {
			encodings = []format.Encoding{format.Plain, format.RLEDictionary}
		} else {
			encodings = []format.Encoding{format.Plain, format.PlainDictionary}
		}
	}

	meta := format.FileMetaData{
		Version: version,
		Schema: []format.SchemaElement{
			{
				Name:        "schema",
				NumChildren: thrift.New(int32(1)),
			},
			{
				Name:           "x",
				Type:           thrift.New(format.Int64),
				RepetitionType: thrift.New(format.Optional),
			},
		},
		NumRows: numRows,
		RowGroups: []format.RowGroup{
			{
				NumRows:       numRows,
				TotalByteSize: chunkSize,
				Columns: []format.ColumnChunk{
					{
						FileOffset: dictOffset,
						MetaData: format.ColumnMetaData{
							Type:                  format.Int64,
							Encoding:              encodings,
							PathInSchema:          []string{"x"},
							Codec:                 format.Uncompressed,
							NumValues:             numValues,
							TotalUncompressedSize: chunkSize,
							TotalCompressedSize:   chunkSize,
							DataPageOffset:        dataOffset,
							DictionaryPageOffset:  dictOffset,
						},
					},
				},
			},
		},
	}

	footerStart := out.Len()
	if err := enc.Encode(&meta); err != nil {
		t.Fatalf("encode footer: %v", err)
	}
	footerSize := out.Len() - footerStart

	var trailer [8]byte
	binary.LittleEndian.PutUint32(trailer[:4], uint32(footerSize))
	copy(trailer[4:], "PAR1")
	out.Write(trailer[:])
	return out.Bytes()
}

func TestEmptyDictionaryColumnChunk(t *testing.T) {
	type row struct {
		X *int64 `parquet:"x,optional"`
	}

	cases := []struct {
		name               string
		version            int32
		dictOffset         int64
		dataOffset         int64
		writeEmptyDictPage bool
	}{
		{
			name:               "v1_dict4_data0",
			version:            1,
			dictOffset:         4,
			dataOffset:         0,
			writeEmptyDictPage: true,
		},
		{
			name:               "v2_dict4_data0",
			version:            2,
			dictOffset:         4,
			dataOffset:         0,
			writeEmptyDictPage: true,
		},
		{
			name:               "v1_dict4_data0_zero_size",
			version:            1,
			dictOffset:         4,
			dataOffset:         0,
			writeEmptyDictPage: false,
		},
		{
			name:               "v1_dict4_data4_control",
			version:            1,
			dictOffset:         4,
			dataOffset:         4,
			writeEmptyDictPage: true,
		},
		{
			name:               "v1_no_dict_data0",
			version:            1,
			dictOffset:         0,
			dataOffset:         0,
			writeEmptyDictPage: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data := buildSingleColumnParquet(t, tc.version, 0, 0, tc.dictOffset, tc.dataOffset, tc.writeEmptyDictPage)

			rows, err := Read[row](bytes.NewReader(data), int64(len(data)))
			if err != nil {
				t.Fatalf("Read: got err=%v, want nil", err)
			}
			if len(rows) != 0 {
				t.Fatalf("Read: got %d rows, want 0", len(rows))
			}

			f, err := OpenFile(bytes.NewReader(data), int64(len(data)))
			if err != nil {
				t.Fatalf("OpenFile: %v", err)
			}

			r := NewGenericReader[row](f)
			defer r.Close()
			n, err := r.Read(make([]row, 4))
			if n != 0 || err != io.EOF {
				t.Fatalf("GenericReader.Read: got n=%d err=%v, want n=0 err=EOF", n, err)
			}

			chunk := f.RowGroups()[0].ColumnChunks()[0]

			pages := chunk.Pages()
			if p, err := pages.ReadPage(); p != nil || err != io.EOF {
				pages.Close()
				t.Fatalf("Pages.ReadPage: got p=%v err=%v, want nil, EOF", p, err)
			}
			if err := pages.Close(); err != nil {
				t.Fatalf("Pages.Close: %v", err)
			}

			for _, seekRow := range []int64{0, 5} {
				p := chunk.Pages()
				if err := p.SeekToRow(seekRow); err != nil {
					p.Close()
					t.Fatalf("Pages.SeekToRow(%d): got %v, want nil", seekRow, err)
				}
				if page, err := p.ReadPage(); page != nil || err != io.EOF {
					p.Close()
					t.Fatalf("Pages.ReadPage after SeekToRow(%d): got page=%v err=%v, want nil, EOF", seekRow, page, err)
				}
				if err := p.Close(); err != nil {
					t.Fatalf("Pages.Close: %v", err)
				}
			}
		})
	}
}

func TestNonEmptyCorruptDataPageOffsetFails(t *testing.T) {
	type row struct {
		X *int64 `parquet:"x,optional"`
	}

	cases := []struct {
		name       string
		numRows    int64
		numValues  int64
		dictOffset int64
		dataOffset int64
	}{
		{
			name:       "non_empty_values",
			numRows:    1,
			numValues:  1,
			dictOffset: 4,
			dataOffset: 0,
		},
		{
			// A row group with rows but no values in the column chunk is
			// corrupt, not an empty column chunk.
			name:       "inconsistent_nonzero_rows_zero_values",
			numRows:    1,
			numValues:  0,
			dictOffset: 4,
			dataOffset: 0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data := buildSingleColumnParquet(t, 1, tc.numRows, tc.numValues, tc.dictOffset, tc.dataOffset, true)

			if _, err := Read[row](bytes.NewReader(data), int64(len(data))); err == nil {
				t.Fatal("Read on corrupt chunk: got nil error, want error")
			}

			f, err := OpenFile(bytes.NewReader(data), int64(len(data)))
			if err != nil {
				t.Fatalf("OpenFile: %v", err)
			}

			r := NewGenericReader[row](f)
			defer r.Close()
			if _, err := r.Read(make([]row, 4)); err == nil {
				t.Fatal("GenericReader.Read on corrupt chunk: got nil error, want error")
			}

			pages := f.RowGroups()[0].ColumnChunks()[0].Pages()
			defer pages.Close()
			if err := pages.SeekToRow(0); err == nil {
				t.Fatal("Pages.SeekToRow(0) on corrupt chunk: got nil error, want error")
			}
		})
	}
}
