package parquet_test

import (
	"bytes"
	"testing"

	"github.com/parquet-go/parquet-go"
)

// TestWriteReadNullOnlyColumn tests writing and reading back a row with a null optional field
func TestWriteReadNullOnlyColumn(t *testing.T) {
	type Record struct {
		Required string  `parquet:"required"`
		Optional *string `parquet:"optional,optional"`
	}

	// Write a file with one row where optional field is null
	var buf bytes.Buffer
	writer := parquet.NewWriter(&buf)

	record := Record{
		Required: "value1",
		Optional: nil, // null value
	}

	if err := writer.Write(&record); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Now try to read it back
	file := bytes.NewReader(buf.Bytes())
	reader := parquet.NewReader(file)
	defer reader.Close()

	var readRecord Record
	if err := reader.Read(&readRecord); err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if readRecord.Required != "value1" {
		t.Errorf("Required field: got %q, want %q", readRecord.Required, "value1")
	}

	if readRecord.Optional != nil {
		t.Errorf("Optional field: got %v, want nil", readRecord.Optional)
	}
}

// TestWriteReadMultipleNullOnlyColumns tests multiple optional columns that are all null
func TestWriteReadMultipleNullOnlyColumns(t *testing.T) {
	type Record struct {
		ID      int64   `parquet:"id"`
		Field1  *string `parquet:"field1,optional"`
		Field2  *string `parquet:"field2,optional"`
		Field3  *string `parquet:"field3,optional"`
	}

	var buf bytes.Buffer
	writer := parquet.NewWriter(&buf)

	// Write multiple rows where all optional fields are null
	records := []Record{
		{ID: 1, Field1: nil, Field2: nil, Field3: nil},
		{ID: 2, Field1: nil, Field2: nil, Field3: nil},
		{ID: 3, Field1: nil, Field2: nil, Field3: nil},
	}

	for _, rec := range records {
		if err := writer.Write(&rec); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Read back and verify
	file := bytes.NewReader(buf.Bytes())
	reader := parquet.NewReader(file)
	defer reader.Close()

	for i := 0; i < 3; i++ {
		var readRecord Record
		if err := reader.Read(&readRecord); err != nil {
			t.Fatalf("Read record %d failed: %v", i, err)
		}

		if readRecord.ID != int64(i+1) {
			t.Errorf("Record %d: ID got %d, want %d", i, readRecord.ID, i+1)
		}

		if readRecord.Field1 != nil || readRecord.Field2 != nil || readRecord.Field3 != nil {
			t.Errorf("Record %d: expected all fields to be nil, got Field1=%v, Field2=%v, Field3=%v",
				i, readRecord.Field1, readRecord.Field2, readRecord.Field3)
		}
	}
}

// TestMergeRowGroupsWithNullOnlyColumns tests merging files that contain null-only optional columns
func TestMergeRowGroupsWithNullOnlyColumns(t *testing.T) {
	type Record struct {
		ID       int64   `parquet:"id"`
		Required string  `parquet:"required"`
		Optional *string `parquet:"optional,optional"`
	}

	// Create first file with one row (null optional field)
	var buf1 bytes.Buffer
	writer1 := parquet.NewWriter(&buf1)
	if err := writer1.Write(&Record{ID: 1, Required: "file1", Optional: nil}); err != nil {
		t.Fatalf("Write to file1 failed: %v", err)
	}
	if err := writer1.Close(); err != nil {
		t.Fatalf("Close file1 failed: %v", err)
	}

	// Create second file with one row (null optional field)
	var buf2 bytes.Buffer
	writer2 := parquet.NewWriter(&buf2)
	if err := writer2.Write(&Record{ID: 2, Required: "file2", Optional: nil}); err != nil {
		t.Fatalf("Write to file2 failed: %v", err)
	}
	if err := writer2.Close(); err != nil {
		t.Fatalf("Close file2 failed: %v", err)
	}

	// Open both files and get their row groups
	file1, err := parquet.OpenFile(bytes.NewReader(buf1.Bytes()), int64(buf1.Len()))
	if err != nil {
		t.Fatalf("OpenFile1 failed: %v", err)
	}

	file2, err := parquet.OpenFile(bytes.NewReader(buf2.Bytes()), int64(buf2.Len()))
	if err != nil {
		t.Fatalf("OpenFile2 failed: %v", err)
	}

	// Merge row groups
	rowGroups := append(file1.RowGroups(), file2.RowGroups()...)
	merged, err := parquet.MergeRowGroups(rowGroups)
	if err != nil {
		t.Fatalf("MergeRowGroups failed: %v", err)
	}

	// Write merged row group to a new file
	var mergedBuf bytes.Buffer
	mergedWriter := parquet.NewWriter(&mergedBuf, merged.Schema())

	if _, err := parquet.CopyRows(mergedWriter, merged.Rows()); err != nil {
		t.Fatalf("CopyRows failed: %v", err)
	}

	if err := mergedWriter.Close(); err != nil {
		t.Fatalf("Close merged writer failed: %v", err)
	}

	// Try to read back the merged file
	mergedFile, err := parquet.OpenFile(bytes.NewReader(mergedBuf.Bytes()), int64(mergedBuf.Len()))
	if err != nil {
		t.Fatalf("OpenFile merged failed: %v", err)
	}

	// Verify we can read all rows
	reader := parquet.NewReader(mergedFile)
	defer reader.Close()

	expectedRecords := []Record{
		{ID: 1, Required: "file1", Optional: nil},
		{ID: 2, Required: "file2", Optional: nil},
	}

	for i, expected := range expectedRecords {
		var got Record
		if err := reader.Read(&got); err != nil {
			t.Fatalf("Read record %d failed: %v", i, err)
		}

		if got.ID != expected.ID {
			t.Errorf("Record %d: ID got %d, want %d", i, got.ID, expected.ID)
		}

		if got.Required != expected.Required {
			t.Errorf("Record %d: Required got %q, want %q", i, got.Required, expected.Required)
		}

		if got.Optional != expected.Optional {
			t.Errorf("Record %d: Optional got %v, want %v", i, got.Optional, expected.Optional)
		}
	}
}
