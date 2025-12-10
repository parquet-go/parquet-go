package parquet_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
)

// TestIssue301DictionaryEncodingForByteArray tests that ByteArray columns
// can use dictionary encoding for better compression of data with repeated values.
//
// This test demonstrates the recommended approach for enabling dictionary encoding
// as documented in https://github.com/parquet-go/parquet-go/issues/301
//
// There are two ways to enable dictionary encoding:
// 1. Use parquet.Encoded() to wrap schema nodes
// 2. Use parquet.DefaultEncodingFor() when creating the writer
func TestIssue301DictionaryEncodingForByteArray(t *testing.T) {
	// Method 1: Use parquet.Encoded() to wrap schema nodes with dictionary encoding
	schema := parquet.NewSchema("test", parquet.Group{
		"ID":     parquet.Leaf(parquet.Int64Type),
		"Name":   parquet.Encoded(parquet.String(), &parquet.RLEDictionary),
		"City":   parquet.Encoded(parquet.String(), &parquet.RLEDictionary),
		"Status": parquet.Encoded(parquet.String(), &parquet.RLEDictionary),
	})

	// These repeated values should compress well with dictionary encoding
	records := []map[string]any{
		{"ID": int64(1), "Name": "Alice", "City": "New York", "Status": "active"},
		{"ID": int64(2), "Name": "Bob", "City": "Los Angeles", "Status": "inactive"},
		{"ID": int64(3), "Name": "Alice", "City": "New York", "Status": "active"},
		{"ID": int64(4), "Name": "Charlie", "City": "Chicago", "Status": "active"},
		{"ID": int64(5), "Name": "Bob", "City": "Los Angeles", "Status": "inactive"},
		{"ID": int64(6), "Name": "Alice", "City": "New York", "Status": "active"},
		{"ID": int64(7), "Name": "Diana", "City": "San Francisco", "Status": "pending"},
		{"ID": int64(8), "Name": "Alice", "City": "New York", "Status": "active"},
		{"ID": int64(9), "Name": "Bob", "City": "Los Angeles", "Status": "inactive"},
		{"ID": int64(10), "Name": "Charlie", "City": "Chicago", "Status": "active"},
	}

	buffer := &bytes.Buffer{}
	writer := parquet.NewGenericWriter[map[string]any](buffer, schema)

	if _, err := writer.Write(records); err != nil {
		t.Fatalf("Failed to write records: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	// Read back the file and check encodings
	reader := bytes.NewReader(buffer.Bytes())
	file, err := parquet.OpenFile(reader, reader.Size())
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}

	// Check that string columns use dictionary encoding
	rowGroups := file.RowGroups()
	if len(rowGroups) == 0 {
		t.Fatal("No row groups in file")
	}

	rowGroup := rowGroups[0]
	metadata := file.Metadata()

	// Find the string columns and verify their encoding
	stringColumns := []string{"Name", "City", "Status"}
	for _, colName := range stringColumns {
		t.Run(colName, func(t *testing.T) {
			// Find column index
			colIndex := -1
			for i, col := range file.Schema().Columns() {
				if len(col) == 1 && col[0] == colName {
					colIndex = i
					break
				}
			}
			if colIndex == -1 {
				t.Fatalf("Column %s not found", colName)
			}

			// Check the column chunk metadata for encoding
			colChunkMeta := metadata.RowGroups[0].Columns[colIndex]
			encodings := colChunkMeta.MetaData.Encoding

			// Dictionary encoding should be present
			hasDictionary := false
			for _, enc := range encodings {
				if enc == format.PlainDictionary || enc == format.RLEDictionary {
					hasDictionary = true
					break
				}
			}

			if !hasDictionary {
				t.Errorf("Column %s should use dictionary encoding, got encodings: %v", colName, encodings)
			}

			// Also verify by checking actual pages
			chunk := rowGroup.ColumnChunks()[colIndex]
			pages := chunk.Pages()
			defer pages.Close()

			hasDictionaryPage := false
			for {
				page, err := pages.ReadPage()
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatalf("Error reading page: %v", err)
				}
				if page.Dictionary() != nil {
					hasDictionaryPage = true
				}
				parquet.Release(page)
				if hasDictionaryPage {
					break
				}
			}

			if !hasDictionaryPage {
				t.Errorf("Column %s pages should have dictionary, but none found", colName)
			}
		})
	}
}

// TestIssue301DictionaryEncodingViaWriterConfig tests enabling dictionary encoding
// via DefaultEncodingFor option when creating the writer.
func TestIssue301DictionaryEncodingViaWriterConfig(t *testing.T) {
	// Method 2: Use DefaultEncodingFor option to set default encoding for ByteArray
	schema := parquet.NewSchema("test", parquet.Group{
		"ID":     parquet.Leaf(parquet.Int64Type),
		"Name":   parquet.String(),
		"City":   parquet.String(),
		"Status": parquet.String(),
	})

	var records []map[string]any
	cities := []string{"New York", "Los Angeles", "Chicago", "Houston", "Phoenix"}
	statuses := []string{"active", "inactive", "pending"}
	names := []string{"Alice", "Bob", "Charlie", "Diana", "Eve"}

	for i := 0; i < 100; i++ {
		records = append(records, map[string]any{
			"ID":     int64(i),
			"Name":   names[i%len(names)],
			"City":   cities[i%len(cities)],
			"Status": statuses[i%len(statuses)],
		})
	}

	// Write with explicit dictionary encoding via DefaultEncodingFor option
	buffer := &bytes.Buffer{}
	writer := parquet.NewGenericWriter[map[string]any](buffer, schema,
		parquet.DefaultEncodingFor(parquet.ByteArray, &parquet.RLEDictionary),
	)
	if _, err := writer.Write(records); err != nil {
		t.Fatalf("Failed to write records: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	// Verify dictionary encoding was used
	reader := bytes.NewReader(buffer.Bytes())
	file, err := parquet.OpenFile(reader, reader.Size())
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}

	metadata := file.Metadata()
	rowGroups := file.RowGroups()
	if len(rowGroups) == 0 {
		t.Fatal("No row groups")
	}

	// Find Name column by name
	colIndex := -1
	for i, col := range file.Schema().Columns() {
		if len(col) == 1 && col[0] == "Name" {
			colIndex = i
			break
		}
	}
	if colIndex == -1 {
		t.Fatal("Name column not found")
	}

	colChunkMeta := metadata.RowGroups[0].Columns[colIndex]
	encodings := colChunkMeta.MetaData.Encoding
	t.Logf("Name column (index %d) encodings: %v", colIndex, encodings)

	// Check for dictionary encoding in metadata
	hasDictionary := false
	for _, enc := range encodings {
		if enc == format.PlainDictionary || enc == format.RLEDictionary {
			hasDictionary = true
			break
		}
	}

	// Also check actual page dictionary
	rowGroup := rowGroups[0]
	chunk := rowGroup.ColumnChunks()[colIndex]
	pages := chunk.Pages()
	defer pages.Close()

	hasPageDictionary := false
	for {
		page, err := pages.ReadPage()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Error reading page: %v", err)
		}
		if page.Dictionary() != nil {
			hasPageDictionary = true
		}
		parquet.Release(page)
		if hasPageDictionary {
			break
		}
	}
	t.Logf("Name column has page dictionary: %v", hasPageDictionary)

	if !hasDictionary && !hasPageDictionary {
		t.Errorf("Name column should use dictionary encoding, got encodings: %v, has page dictionary: %v", encodings, hasPageDictionary)
	}
}

// TestIssue301CompressionComparison compares file sizes between default encoding
// and dictionary encoding to demonstrate the compression benefit.
func TestIssue301CompressionComparison(t *testing.T) {
	schema := parquet.NewSchema("test", parquet.Group{
		"ID":     parquet.Leaf(parquet.Int64Type),
		"Name":   parquet.String(),
		"City":   parquet.String(),
		"Status": parquet.String(),
	})

	// Create data with repeated values to see compression difference
	var records []map[string]any
	cities := []string{"New York", "Los Angeles", "Chicago", "Houston", "Phoenix"}
	statuses := []string{"active", "inactive", "pending"}
	names := []string{"Alice", "Bob", "Charlie", "Diana", "Eve"}

	for i := 0; i < 1000; i++ {
		records = append(records, map[string]any{
			"ID":     int64(i),
			"Name":   names[i%len(names)],
			"City":   cities[i%len(cities)],
			"Status": statuses[i%len(statuses)],
		})
	}

	// Write without dictionary encoding (default)
	defaultBuffer := &bytes.Buffer{}
	defaultWriter := parquet.NewGenericWriter[map[string]any](defaultBuffer, schema)
	if _, err := defaultWriter.Write(records); err != nil {
		t.Fatalf("Failed to write with default encoding: %v", err)
	}
	if err := defaultWriter.Close(); err != nil {
		t.Fatalf("Failed to close default writer: %v", err)
	}

	// Write with dictionary encoding
	dictBuffer := &bytes.Buffer{}
	dictWriter := parquet.NewGenericWriter[map[string]any](dictBuffer, schema,
		parquet.DefaultEncodingFor(parquet.ByteArray, &parquet.RLEDictionary),
	)
	if _, err := dictWriter.Write(records); err != nil {
		t.Fatalf("Failed to write with dictionary encoding: %v", err)
	}
	if err := dictWriter.Close(); err != nil {
		t.Fatalf("Failed to close dictionary writer: %v", err)
	}

	t.Logf("Default encoding (DELTA_LENGTH_BYTE_ARRAY) file size: %d bytes", defaultBuffer.Len())
	t.Logf("Dictionary encoding (RLE_DICTIONARY) file size: %d bytes", dictBuffer.Len())

	// Dictionary encoding should produce smaller files for repeated string values
	ratio := float64(defaultBuffer.Len()) / float64(dictBuffer.Len())
	t.Logf("Size ratio (default/dictionary): %.2f", ratio)

	// The dictionary-encoded file should be smaller (ratio > 1)
	if ratio > 1.0 {
		t.Logf("Dictionary encoding reduced file size by %.1f%%", (1-1/ratio)*100)
	} else {
		t.Logf("Note: Dictionary encoding did not reduce file size for this data (ratio: %.2f)", ratio)
	}
}
