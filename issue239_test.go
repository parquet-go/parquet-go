package parquet_test

import (
	"bytes"
	"testing"

	"github.com/parquet-go/parquet-go"
)

// TestIssue239_InvalidMagicFooter tests the scenario from issue #239
// where users get "invalid magic footer" error when trying to open files
func TestIssue239_InvalidMagicFooter(t *testing.T) {
	type Features struct {
		Timestamp int64
		Price     float64
	}

	// Test 1: File not properly closed (Close() never called)
	t.Run("file not closed", func(t *testing.T) {
		buf := new(bytes.Buffer)
		w := parquet.NewGenericWriter[Features](buf)

		// Write some rows
		_, err := w.Write([]Features{
			{Timestamp: 1, Price: 100.0},
			{Timestamp: 2, Price: 200.0},
		})
		if err != nil {
			t.Fatal(err)
		}

		// Intentionally NOT calling w.Close()
		// This should result in an incomplete file

		// Try to open the incomplete file
		r := bytes.NewReader(buf.Bytes())
		_, err = parquet.OpenFile(r, int64(buf.Len()))

		if err == nil {
			t.Error("expected an error when opening incomplete file, got nil")
		} else {
			t.Logf("Got expected error: %v", err)
			t.Logf("File size: %d bytes", buf.Len())
			t.Logf("Last 8 bytes: %q", buf.Bytes()[max(0, buf.Len()-8):])
		}
	})

	// Test 2: Empty file
	t.Run("empty file", func(t *testing.T) {
		buf := new(bytes.Buffer)
		r := bytes.NewReader(buf.Bytes())
		_, err := parquet.OpenFile(r, int64(buf.Len()))

		if err == nil {
			t.Error("expected an error when opening empty file, got nil")
		} else {
			t.Logf("Got expected error: %v", err)
		}
	})

	// Test 3: File with only header
	t.Run("file with only header", func(t *testing.T) {
		buf := bytes.NewBufferString("PAR1")
		r := bytes.NewReader(buf.Bytes())
		_, err := parquet.OpenFile(r, int64(buf.Len()))

		if err == nil {
			t.Error("expected an error when opening file with only header, got nil")
		} else {
			t.Logf("Got expected error: %v", err)
		}
	})

	// Test 4: Truncated file (file closed properly but then truncated)
	t.Run("truncated file", func(t *testing.T) {
		buf := new(bytes.Buffer)
		w := parquet.NewGenericWriter[Features](buf)

		_, err := w.Write([]Features{
			{Timestamp: 1, Price: 100.0},
			{Timestamp: 2, Price: 200.0},
		})
		if err != nil {
			t.Fatal(err)
		}

		// Properly close the file
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}

		// Truncate the last 10 bytes (removing part of the footer)
		truncated := buf.Bytes()[:len(buf.Bytes())-10]
		r := bytes.NewReader(truncated)
		_, err = parquet.OpenFile(r, int64(len(truncated)))

		if err == nil {
			t.Error("expected an error when opening truncated file, got nil")
		} else {
			t.Logf("Got expected error: %v", err)
			t.Logf("Truncated file size: %d bytes", len(truncated))
			t.Logf("Last 8 bytes: %q", truncated[max(0, len(truncated)-8):])
		}
	})

	// Test 5: Properly written and closed file (should work)
	t.Run("properly written file", func(t *testing.T) {
		buf := new(bytes.Buffer)
		w := parquet.NewGenericWriter[Features](buf)

		_, err := w.Write([]Features{
			{Timestamp: 1, Price: 100.0},
			{Timestamp: 2, Price: 200.0},
		})
		if err != nil {
			t.Fatal(err)
		}

		if err := w.Close(); err != nil {
			t.Fatal(err)
		}

		// This should work
		r := bytes.NewReader(buf.Bytes())
		f, err := parquet.OpenFile(r, int64(buf.Len()))
		if err != nil {
			t.Fatalf("unexpected error when opening properly written file: %v", err)
		}

		t.Logf("Successfully opened file with %d rows", f.NumRows())
		t.Logf("File size: %d bytes", buf.Len())
		t.Logf("Last 8 bytes: %q", buf.Bytes()[buf.Len()-8:])
	})
}
