package parquet_test

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"math/rand"
	"testing"

	"github.com/parquet-go/parquet-go"
)

// TestIssue187SortingWriterCorruption attempts to reproduce the data corruption
// reported in issue #187 where column data from binary/base64 fields leaks into
// other columns when using SortingWriter.
//
// The issue reports:
// - Using SortingWriter with 10,000 row buffer
// - MySQL binlog data with base64-encoded header and event fields
// - Data corruption where base64 data appears in wrong columns
// - Intermittent corruption (some files are fine, others corrupted)
func TestIssue187SortingWriterCorruption(t *testing.T) {
	// Structure similar to the reported issue
	type BinlogRecord struct {
		Schema   string `parquet:"schema"`
		Table    string `parquet:"table"`
		Position uint32 `parquet:"position"`
		Offset   uint32 `parquet:"offset"`
		Header   string `parquet:"header"`   // base64-encoded binary data
		Event    string `parquet:"event"`    // base64-encoded binary data
		TxnID    string `parquet:"txn_id"`
		Timestamp uint32 `parquet:"timestamp"`
	}

	// Generate test data with large base64 strings similar to the issue
	rowCount := 15000 // More than the 10,000 buffer size to force multiple flushes
	records := make([]BinlogRecord, rowCount)

	for i := range records {
		// Create large binary data and encode to base64
		binaryData := make([]byte, 500+rand.Intn(500)) // 500-1000 bytes
		rand.Read(binaryData)
		base64Data := base64.StdEncoding.EncodeToString(binaryData)

		records[i] = BinlogRecord{
			Schema:    fmt.Sprintf("schema_%d", i%10),
			Table:     fmt.Sprintf("table_%d", i%5),
			Position:  uint32(i),
			Offset:    uint32(i * 100),
			Header:    base64Data,                    // Large base64 string
			Event:     base64Data + "_event",         // Large base64 string
			TxnID:     fmt.Sprintf("txn_%d", i),
			Timestamp: uint32(1700000000 + i),
		}
	}

	// Write using SortingWriter with the reported configuration
	var buf bytes.Buffer
	writer := parquet.NewSortingWriter[BinlogRecord](&buf, 10000, // 10k buffer as reported
		parquet.WriterConfig{
			Sorting: parquet.SortingConfig{
				SortingColumns: []parquet.SortingColumn{
					parquet.Ascending("schema"),
					parquet.Ascending("position"),
				},
			},
		},
	)

	// Write all records
	n, err := writer.Write(records)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != rowCount {
		t.Fatalf("Expected to write %d records, wrote %d", rowCount, n)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Read back and verify data integrity
	reader := parquet.NewReader(bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	// Create a map to look up expected records by (schema, position)
	expectedRecords := make(map[string]BinlogRecord)
	for _, rec := range records {
		key := fmt.Sprintf("%s_%d", rec.Schema, rec.Position)
		expectedRecords[key] = rec
	}

	readCount := 0
	for {
		var readRecord BinlogRecord
		err := reader.Read(&readRecord)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Read failed at record %d: %v", readCount, err)
		}

		// Look up expected record
		key := fmt.Sprintf("%s_%d", readRecord.Schema, readRecord.Position)
		expected, ok := expectedRecords[key]
		if !ok {
			t.Fatalf("Record %d: unexpected schema/position combination: %s/%d",
				readCount, readRecord.Schema, readRecord.Position)
		}

		// Verify all fields match
		if readRecord.Table != expected.Table {
			t.Errorf("Record %d: Table mismatch: got %q, want %q",
				readCount, readRecord.Table, expected.Table)
		}
		if readRecord.Offset != expected.Offset {
			t.Errorf("Record %d: Offset mismatch: got %d, want %d",
				readCount, readRecord.Offset, expected.Offset)
		}

		// These are the fields that were reported as corrupted
		if readRecord.Header != expected.Header {
			t.Errorf("Record %d: Header mismatch (len: got %d, want %d)\ngot: %s...\nwant: %s...",
				readCount, len(readRecord.Header), len(expected.Header),
				truncate(readRecord.Header, 50), truncate(expected.Header, 50))
		}
		if readRecord.Event != expected.Event {
			t.Errorf("Record %d: Event mismatch (len: got %d, want %d)\ngot: %s...\nwant: %s...",
				readCount, len(readRecord.Event), len(expected.Event),
				truncate(readRecord.Event, 50), truncate(expected.Event, 50))
		}

		if readRecord.TxnID != expected.TxnID {
			t.Errorf("Record %d: TxnID mismatch: got %q, want %q",
				readCount, readRecord.TxnID, expected.TxnID)
		}
		if readRecord.Timestamp != expected.Timestamp {
			t.Errorf("Record %d: Timestamp mismatch: got %d, want %d",
				readCount, readRecord.Timestamp, expected.Timestamp)
		}

		readCount++
	}

	if readCount != rowCount {
		t.Fatalf("Expected to read %d records, read %d", rowCount, readCount)
	}
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
