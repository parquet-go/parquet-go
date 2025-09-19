package parquet_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/parquet-go/parquet-go"
)

// TestRecord defines the schema for our test data
type TestRecord struct {
	ID     int64   `parquet:"id"`
	Name   string  `parquet:"name"`
	Age    int32   `parquet:"age"`
	Score  float64 `parquet:"score"`
	Active bool    `parquet:"active"`
}

// TestSliceBoundsOutOfRange reproduces the slice bounds out of range bug
// when MaxRowsPerRowGroup is set to a small value.
//
// Bug details:
// - parquet-go version: v0.25.1
// - Error: panic: runtime error: slice bounds out of range [64:56:]
// - Location: writer.go:191 in GenericWriter.Write method
// - Trigger: Small MaxRowsPerRowGroup + batch size > 64 rows
//
// Root cause: In writeRows method, remain = w.maxRows - w.numRows can be negative
// when w.numRows > w.maxRows due to flush timing issues. This negative value
// is cast to int and used as slice length, causing invalid slice bounds.
func TestSliceBoundsOutOfRange(t *testing.T) {
	// Create temporary file in current directory
	file, err := os.CreateTemp("./", "parquet_bug_test_*.parquet")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(file.Name())
	defer file.Close()

	// Print the file path
	t.Logf("创建的文件路径: %s", file.Name())

	// Create schema
	schema := parquet.SchemaOf(TestRecord{})

	// Create writer with small MaxRowsPerRowGroup - this triggers the bug
	writer := parquet.NewGenericWriter[map[string]interface{}](
		file,
		schema,
		parquet.MaxRowsPerRowGroup(120), // Small value is key to reproducing the bug
	)
	defer writer.Close()

	// Test parameters that reproduce the bug
	batchSize := 100  // Must be > 64 (maxRowsPerWrite) to trigger the bug
	totalBatches := 1 // Multiple batches to cross row group boundaries

	t.Logf("Test configuration: MaxRowsPerRowGroup=120, BatchSize=%d", batchSize)

	for batch := 0; batch < totalBatches; batch++ {
		// Generate test data
		records := make([]map[string]interface{}, batchSize)
		for i := 0; i < batchSize; i++ {
			recordID := int64(batch*batchSize + i)
			records[i] = map[string]interface{}{
				"id":     recordID,
				"name":   fmt.Sprintf("user_%d", recordID),
				"age":    int32(20 + (recordID % 50)),
				"score":  float64(recordID%100) + 0.5,
				"active": recordID%2 == 0,
			}
		}

		t.Logf("Writing batch %d with %d records", batch+1, len(records))

		// This is where the panic occurs: slice bounds out of range [64:56:]
		_, err := writer.Write(records)
		if err != nil {
			t.Fatalf("Write failed on batch %d: %v", batch+1, err)
		}

		// Flush after each batch
		err = writer.Flush()
		if err != nil {
			t.Fatalf("Flush failed on batch %d: %v", batch+1, err)
		}

		t.Logf("Batch %d completed successfully", batch+1)
	}
}

// Workaround test: shows that the bug doesn't occur with larger MaxRowsPerRowGroup
func TestWorkaroundWithLargeMaxRows(t *testing.T) {
	file, err := os.CreateTemp("./", "parquet_workaround_test_*.parquet")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(file.Name())
	defer file.Close()

	// Print the file path
	t.Logf("创建的文件路径: %s", file.Name())

	schema := parquet.SchemaOf(TestRecord{})

	// Use large MaxRowsPerRowGroup - this avoids the bug
	writer := parquet.NewGenericWriter[map[string]interface{}](
		file,
		schema,
		parquet.MaxRowsPerRowGroup(10000), // Large value avoids the bug
	)
	defer writer.Close()

	batchSize := 100
	totalBatches := 3

	t.Logf("Workaround test: MaxRowsPerRowGroup=10000, BatchSize=%d", batchSize)

	for batch := 0; batch < totalBatches; batch++ {
		records := make([]map[string]interface{}, batchSize)
		for i := 0; i < batchSize; i++ {
			recordID := int64(batch*batchSize + i)
			records[i] = map[string]interface{}{
				"id":     recordID,
				"name":   fmt.Sprintf("user_%d", recordID),
				"age":    int32(20 + (recordID % 50)),
				"score":  float64(recordID%100) + 0.5,
				"active": recordID%2 == 0,
			}
		}

		_, err := writer.Write(records)
		if err != nil {
			t.Fatalf("Write failed on batch %d: %v", batch+1, err)
		}

		err = writer.Flush()
		if err != nil {
			t.Fatalf("Flush failed on batch %d: %v", batch+1, err)
		}
	}

	t.Log("Workaround test completed successfully - no panic with large MaxRowsPerRowGroup")
}
