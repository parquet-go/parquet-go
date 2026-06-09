package parquet

import (
	"io"
	"sync/atomic"
)

// This file implements the "L3" re-encode path for Writer.WriteRowGroup: when a
// single file-backed source row group's schema matches the writer but it cannot
// be copied verbatim (the configured codec/encoding/etc. differ), re-encode it
// column-by-column instead of assembling rows and then deconstructing them.
//
// The regular fallback reads the source columns, interleaves their values into
// Row objects, then immediately tears those rows back apart into columns to
// re-encode. L3 skips that round-trip: it reads each column's values directly
// and writes them straight through the column writer (which still re-encodes,
// re-compresses, computes statistics, and populates bloom filters exactly as the
// row path would).
//
// L3 still decodes and re-encodes values, so it is far less of a win than the
// verbatim copy (L0); its benefit is avoiding the row round-trip (measured
// ~1.4–1.8x faster with notably less garbage). See
// docs/merge-copy-optimization.md.

// reencodePathCounter counts row groups written via the L3 column-oriented
// re-encode path. Test/benchmark instrumentation only.
var reencodePathCounter atomic.Int64

// disableWriteReencode forces WriteRowGroup onto the row-oriented fallback. It
// exists only so benchmarks can measure L3 against an otherwise identical
// baseline.
var disableWriteReencode bool

// reencodableRowGroup reports whether rowGroup can be written via the L3
// column-oriented re-encode path: every column must unwrap to a file-backed
// chunk (which guarantees reading column-wise preserves row order — this
// excludes overlapping heap merges, whose order depends on cross-column row
// interleaving), and the source row count must fit the configured row group
// size (otherwise the row path's row-group splitting would differ).
func (w *Writer) reencodableRowGroup(rowGroup RowGroup) ([]ColumnChunk, bool) {
	if disableWriteReencode {
		return nil, false
	}
	dst := w.writer.currentRowGroup.columns
	columns := rowGroup.ColumnChunks()
	if len(columns) == 0 || len(columns) != len(dst) {
		return nil, false
	}
	for _, col := range columns {
		if _, ok := fileColumnChunkOf(col); !ok {
			return nil, false
		}
	}
	if rowGroup.NumRows() > w.writer.currentRowGroup.maxRows {
		return nil, false
	}
	return columns, true
}

// writeRowGroupByColumn re-encodes each source column chunk into its destination
// column writer, reading values column-wise instead of materializing rows.
func (w *Writer) writeRowGroupByColumn(columns []ColumnChunk) error {
	dst := w.writer.currentRowGroup.columns
	for i, col := range columns {
		if err := copyColumnValues(dst[i], col); err != nil {
			return err
		}
	}
	reencodePathCounter.Add(1)
	return nil
}

// reencodeValueBufferSize is the batch size for column-wise value copies. Larger
// than the default value buffer to amortize per-call overhead.
const reencodeValueBufferSize = 1024

// copyColumnValues reads every value of src in order and writes it to dst,
// without materializing rows.
func copyColumnValues(dst *ColumnWriter, src ColumnChunk) error {
	reader := NewColumnChunkValueReader(src)
	defer reader.Close()

	buf := make([]Value, reencodeValueBufferSize)
	for {
		n, err := reader.ReadValues(buf)
		if n > 0 {
			if _, werr := dst.WriteRowValues(buf[:n]); werr != nil {
				return werr
			}
		}
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}
