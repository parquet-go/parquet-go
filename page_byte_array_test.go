package parquet_test

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/encoding"
)

// validateParquetFileByteArrayOffsets checks that all byte array pages have the
// correct number of offsets relative to the header's NumValues - NumNulls.
// This is a regression test for a bug where the header NumValues didn't match
// the actual number of values encoded in the page data.
//
// The bug manifests as:
// - Header says numValues=X, numNulls=Y
// - The encoded byte array data should have (X-Y+1) offsets
// - When the counts don't match, reading the page produces corrupted data
//
// See page_byte_array.go lines 21-26 and 68-72 for the workaround that
// prevents crashes when reading invalid files.
func validateParquetFileByteArrayOffsets(pf *parquet.File) error {
	for rgIdx, rg := range pf.RowGroups() {
		for colIdx, chunk := range rg.ColumnChunks() {
			pages := chunk.Pages()
			pageIdx := 0
			for {
				page, err := pages.ReadPage()
				if err == io.EOF {
					break
				}
				if err != nil {
					return fmt.Errorf("reading page: %w", err)
				}

				pageData := page.Data()
				if pageData.Kind() == encoding.ByteArray {
					_, offsets := pageData.ByteArray()
					numValues := page.NumValues()
					numNulls := page.NumNulls()
					expectedLen := int(numValues-numNulls) + 1

					if len(offsets) != expectedLen {
						return fmt.Errorf(
							"rowGroup=%d column=%d page=%d: numValues=%d numNulls=%d expectedOffsets=%d actualOffsets=%d",
							rgIdx, colIdx, pageIdx, numValues, numNulls, expectedLen, len(offsets),
						)
					}

					// Also check for invalid offsets (j > k means data corruption)
					for i := 0; i < len(offsets)-1; i++ {
						if offsets[i] > offsets[i+1] {
							return fmt.Errorf(
								"rowGroup=%d column=%d page=%d: invalid offset at index %d: %d > %d",
								rgIdx, colIdx, pageIdx, i, offsets[i], offsets[i+1],
							)
						}
					}
				}
				pageIdx++
			}
			pages.Close()
		}
	}
	return nil
}

// TestInvalidParquetFileHasOffsetBug validates that testdata/invalid-slice-oob.parquet
// exhibits the byte array offset mismatch bug. This test is a regression test that
// FAILS when the bug is present in the test file, documenting the issue.
//
// The bug: When writing parquet files with certain nested repeated byte array columns,
// the DataPageHeaderV2.NumValues can be set incorrectly, causing a mismatch between
// the header and the actual encoded data.
//
// When the root cause is fixed and invalid files are no longer created, this test
// should be updated to verify the fix (or the invalid-slice-oob.parquet file should be
// regenerated to demonstrate it no longer contains the bug).
func TestInvalidParquetFileHasOffsetBug(t *testing.T) {
	f, err := os.Open("testdata/invalid-slice-oob.parquet")
	if err != nil {
		t.Skipf("Skipping: %v", err)
	}
	defer f.Close()

	stat, _ := f.Stat()
	pf, err := parquet.OpenFile(f, stat.Size())
	if err != nil {
		t.Fatalf("open file failed: %v", err)
	}

	err = validateParquetFileByteArrayOffsets(pf)
	if err != nil {
		// BUG: The test file has invalid byte array offset data.
		// This test fails until the root cause is fixed.
		// The workaround in page_byte_array.go prevents crashes, but the underlying
		// bug that creates invalid files needs to be fixed.
		t.Fatalf("BUG: invalid-slice-oob.parquet has byte array offset corruption that needs fixing: %v", err)
	}
}

// TestSortingWriterByteArrayOffsetIntegrity tests that SortingWriter produces valid
// byte array pages where the header NumValues matches the actual encoded data.
//
// This is a regression test for a bug where sorted byte array data could produce
// pages with mismatched NumValues counts.
func TestSortingWriterByteArrayOffsetIntegrity(t *testing.T) {
	type Row struct {
		Tag string `parquet:"tag"`
	}

	// Test with various row counts and page sizes to catch edge cases
	testCases := []struct {
		name           string
		rowCount       int
		pageBufferSize int
		sortRowCount   int64
	}{
		{"small", 50, 1024, 50},
		{"medium", 210, 2560, 210},
		{"large", 500, 4096, 100},
		{"tiny_pages", 100, 256, 100},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rows := make([]Row, tc.rowCount)
			for i := range rows {
				rows[i].Tag = fmt.Sprintf("test-%03d-%s", i, string(bytes.Repeat([]byte("x"), 100)))
			}

			buffer := bytes.NewBuffer(nil)

			writer := parquet.NewSortingWriter[Row](buffer, tc.sortRowCount,
				&parquet.WriterConfig{
					PageBufferSize: tc.pageBufferSize,
					Sorting: parquet.SortingConfig{
						SortingColumns: []parquet.SortingColumn{
							parquet.Ascending("tag"),
						},
					},
				})

			if _, err := writer.Write(rows); err != nil {
				t.Fatalf("write failed: %v", err)
			}

			if err := writer.Close(); err != nil {
				t.Fatalf("close failed: %v", err)
			}

			pf, err := parquet.OpenFile(bytes.NewReader(buffer.Bytes()), int64(buffer.Len()))
			if err != nil {
				t.Fatalf("open file failed: %v", err)
			}

			if err := validateParquetFileByteArrayOffsets(pf); err != nil {
				t.Fatalf("validation failed: %v", err)
			}
		})
	}
}

// TestMergeRowGroupsByteArrayOffsetIntegrity tests that MergeRowGroups produces valid
// byte array pages when merging sorted row groups.
//
// This is a regression test for a bug where merged byte array data could produce
// pages with mismatched NumValues counts.
func TestMergeRowGroupsByteArrayOffsetIntegrity(t *testing.T) {
	type Row struct {
		Tag string `parquet:"tag"`
	}

	// Test with various configurations
	testCases := []struct {
		name           string
		rowCount       int
		pageBufferSize int
		numFiles       int
	}{
		{"two_files", 210, 2560, 2},
		{"three_files", 300, 1024, 3},
		{"many_files", 500, 512, 5},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rows := make([]Row, tc.rowCount)
			for i := range rows {
				rows[i].Tag = fmt.Sprintf("test-%03d-%s", i, string(bytes.Repeat([]byte("x"), 100)))
			}

			// Create multiple files with portions of the data
			files := make([]*parquet.File, tc.numFiles)
			rowsPerFile := tc.rowCount / tc.numFiles

			for i := range tc.numFiles {
				buffer := bytes.NewBuffer(nil)

				writer := parquet.NewSortingWriter[Row](buffer, int64(rowsPerFile),
					&parquet.WriterConfig{
						PageBufferSize: tc.pageBufferSize,
						Sorting: parquet.SortingConfig{
							SortingColumns: []parquet.SortingColumn{
								parquet.Ascending("tag"),
							},
						},
					})

				start := i * rowsPerFile
				end := start + rowsPerFile
				if end > tc.rowCount {
					end = tc.rowCount
				}

				_, err := writer.Write(rows[start:end])
				if err != nil {
					t.Fatalf("write failed: %v", err)
				}

				if err := writer.Close(); err != nil {
					t.Fatalf("close failed: %v", err)
				}

				f, err := parquet.OpenFile(bytes.NewReader(buffer.Bytes()), int64(buffer.Len()))
				if err != nil {
					t.Fatalf("open file failed: %v", err)
				}

				// Validate each individual file
				if err := validateParquetFileByteArrayOffsets(f); err != nil {
					t.Fatalf("individual file %d validation failed: %v", i, err)
				}

				files[i] = f
			}

			// Collect all row groups
			rowGroups := make([]parquet.RowGroup, 0, tc.numFiles)
			for _, f := range files {
				rowGroups = append(rowGroups, f.RowGroups()...)
			}

			// Merge the row groups
			merged, err := parquet.MergeRowGroups(rowGroups,
				parquet.SortingRowGroupConfig(parquet.SortingColumns(parquet.Ascending("tag"))),
			)
			if err != nil {
				t.Fatalf("merge failed: %v", err)
			}

			// Write the merged row group to a new file
			mergedBuffer := bytes.NewBuffer(nil)
			writer := parquet.NewGenericWriter[Row](mergedBuffer)

			rowsWritten, err := parquet.CopyRows(writer, merged.Rows())
			if err != nil {
				t.Fatalf("copy rows failed: %v", err)
			}
			t.Logf("Copied %d rows to merged file", rowsWritten)

			if err := writer.Close(); err != nil {
				t.Fatalf("close merged writer failed: %v", err)
			}

			// Validate the merged file
			pf, err := parquet.OpenFile(bytes.NewReader(mergedBuffer.Bytes()), int64(mergedBuffer.Len()))
			if err != nil {
				t.Fatalf("open merged file failed: %v", err)
			}

			if err := validateParquetFileByteArrayOffsets(pf); err != nil {
				t.Fatalf("merged file validation failed: %v", err)
			}
		})
	}
}

// TestOptionalByteArrayOffsetIntegrity tests byte array offset integrity with
// optional (nullable) columns which have different encoding patterns.
func TestOptionalByteArrayOffsetIntegrity(t *testing.T) {
	type Row struct {
		Name  *string `parquet:"name,optional"`
		Value *string `parquet:"value,optional"`
	}

	testCases := []struct {
		name           string
		rowCount       int
		pageBufferSize int
		nullRatio      int // 1 in N values will be null
	}{
		{"sparse_nulls", 200, 1024, 10},
		{"many_nulls", 200, 1024, 2},
		{"no_nulls", 200, 1024, 0},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rows := make([]Row, tc.rowCount)
			for i := range rows {
				if tc.nullRatio == 0 || i%tc.nullRatio != 0 {
					name := fmt.Sprintf("name-%03d-%s", i, string(bytes.Repeat([]byte("n"), 50)))
					rows[i].Name = &name
				}
				if tc.nullRatio == 0 || (i+1)%tc.nullRatio != 0 {
					value := fmt.Sprintf("value-%03d-%s", i, string(bytes.Repeat([]byte("v"), 50)))
					rows[i].Value = &value
				}
			}

			buffer := bytes.NewBuffer(nil)

			writer := parquet.NewSortingWriter[Row](buffer, int64(tc.rowCount/2),
				&parquet.WriterConfig{
					PageBufferSize: tc.pageBufferSize,
					Sorting: parquet.SortingConfig{
						SortingColumns: []parquet.SortingColumn{
							parquet.Ascending("name"),
						},
					},
				})

			if _, err := writer.Write(rows); err != nil {
				t.Fatalf("write failed: %v", err)
			}

			if err := writer.Close(); err != nil {
				t.Fatalf("close failed: %v", err)
			}

			pf, err := parquet.OpenFile(bytes.NewReader(buffer.Bytes()), int64(buffer.Len()))
			if err != nil {
				t.Fatalf("open file failed: %v", err)
			}

			if err := validateParquetFileByteArrayOffsets(pf); err != nil {
				t.Fatalf("validation failed: %v", err)
			}
		})
	}
}
