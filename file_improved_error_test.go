package parquet_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
)

// TestImprovedMagicFooterError tests that we get helpful error messages
// when opening files with invalid magic footers
func TestImprovedMagicFooterError(t *testing.T) {
	type Row struct {
		Value int
	}

	t.Run("incomplete file with valid header", func(t *testing.T) {
		// Create a properly written file
		buf := new(bytes.Buffer)
		w := parquet.NewGenericWriter[Row](buf)
		w.Write([]Row{{Value: 42}})
		w.Close()

		// Truncate the last 10 bytes to create an invalid footer
		truncated := buf.Bytes()[:len(buf.Bytes())-10]

		// Try to open the truncated file
		r := bytes.NewReader(truncated)
		_, err := parquet.OpenFile(r, int64(len(truncated)))

		if err == nil {
			t.Fatal("expected an error but got nil")
		}

		errMsg := err.Error()

		// The error should mention that the file was not properly closed
		if !strings.Contains(errMsg, "not properly closed") && !strings.Contains(errMsg, "truncated") {
			t.Errorf("expected error to mention improper closing or truncation, got: %v", err)
		}

		// Should show expected vs actual
		if !strings.Contains(errMsg, "expected") && !strings.Contains(errMsg, "got") {
			t.Errorf("expected error to show expected vs actual bytes, got: %v", err)
		}

		t.Logf("Error message: %v", err)
	})

	t.Run("invalid file without valid header", func(t *testing.T) {
		// Create a file that's not a parquet file
		notParquet := []byte("This is not a parquet file at all!")

		r := bytes.NewReader(notParquet)
		_, err := parquet.OpenFile(r, int64(len(notParquet)))

		if err == nil {
			t.Fatal("expected an error but got nil")
		}

		errMsg := err.Error()

		// Should mention it's not a valid parquet file
		if !strings.Contains(errMsg, "not be a valid parquet file") && !strings.Contains(errMsg, "invalid magic header") {
			t.Errorf("expected error to mention invalid file, got: %v", err)
		}

		t.Logf("Error message: %v", err)
	})

	t.Run("file not closed during writing", func(t *testing.T) {
		// Create a file but don't close it
		buf := new(bytes.Buffer)
		w := parquet.NewGenericWriter[Row](buf)
		w.Write([]Row{{Value: 42}})
		// Intentionally NOT calling w.Close()

		// Try to open the incomplete file
		r := bytes.NewReader(buf.Bytes())
		_, err := parquet.OpenFile(r, int64(buf.Len()))

		if err == nil {
			t.Fatal("expected an error but got nil")
		}

		t.Logf("Error message: %v", err)
		t.Logf("Incomplete file size: %d bytes", buf.Len())
	})
}
