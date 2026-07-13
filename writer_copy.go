package parquet

import (
	"fmt"
	"io"
	"slices"
	"sync/atomic"

	"github.com/parquet-go/parquet-go/format"
)

// copyPathCounter counts column chunks copied verbatim. It exists so tests can
// assert that the L0 fast path was taken; it is otherwise unused.
var copyPathCounter atomic.Int64

// disableWriteCopy forces WriteRowGroup onto the regular re-encode path. It
// exists only so benchmarks can measure the copy optimization against an
// otherwise identical baseline.
var disableWriteCopy bool

// This file implements the "L0" copy optimization for Writer.WriteRowGroup:
// when a source row group's column chunks can be copied verbatim (compressed
// bytes moved through without decompression, decoding, re-encoding, or
// re-compression), the writer splices the source bytes directly into the output
// and copies the associated statistics and page index.
//
// The optimization is transparent: WriteRowGroup detects copyable sources and
// falls back to the regular re-encode path otherwise. It is conservative — it
// only fires when a verbatim copy is provably indistinguishable from the
// configured re-encode behavior (matching type, encoding, codec, data page
// version; no encryption; no writer-requested bloom filter; source statistics
// and page index present). See docs/merge-copy-optimization.md.
//
// Increment 1 is all-or-nothing per row group: every column chunk must be
// copyable, otherwise the whole row group falls back to the regular path.

// copiedChunk holds the prebuilt metadata for a column chunk copied verbatim
// from a source file, plus the source byte ranges of its dictionary and data
// pages. The bytes are streamed directly from the source into the output during
// row group assembly (no intermediate buffer or per-chunk allocation).
type copiedChunk struct {
	reader        io.ReaderAt // source file reader
	dictOffset    int64       // byte offset of the dictionary page in the source
	dictLength    int64       // length of the dictionary page, 0 if none
	dataOffset    int64       // byte offset of the first data page in the source
	dataLength    int64       // length of all data pages
	columnIndex   format.ColumnIndex
	sizeStats     format.SizeStatistics
	statistics    format.Statistics
	encodingStats []format.PageEncodingStats
	numValues     int64
	numRows       int64

	totalUncompressedSize int64
	totalCompressedSize   int64
}

// orderedRowGroupSegments is implemented by row groups that are the in-order
// concatenation of independently writable sub-row-groups: writing the segments
// in sequence yields the same rows, in the same order, as reading Rows(). It is
// used so the writer can route copyable segments of a merge through the verbatim
// copy fast path. A row group whose order depends on interleaving across its
// children (e.g. an overlapping heap merge) or that applies cross-segment
// deduplication must return nil to opt out.
type orderedRowGroupSegments interface {
	rowGroupSegments() []RowGroup
}

// splittableCopyableSegments returns the writable segments of rowGroup when it
// is a segmented row group and at least one segment can be written through an
// optimized segment path. The writer then emits packed output row groups so
// copyable single segments can take the L0 fast path and multi-segment batches
// can take the L3 column-oriented packing path. Splitting is gated on
// optimization eligibility so that pure re-encode merges keep their existing
// output structure.
func (w *Writer) splittableCopyableSegments(rowGroup RowGroup) ([]RowGroup, bool) {
	if disableWriteCopy && disableWriteReencode {
		return nil, false
	}
	v, ok := rowGroup.(orderedRowGroupSegments)
	if !ok {
		return nil, false
	}
	segments := v.rowGroupSegments()
	if len(segments) <= 1 {
		return nil, false
	}
	for _, seg := range segments {
		if _, ok := w.copyableColumnChunks(seg); ok {
			return segments, true
		}
		if _, ok := w.reencodableRowGroup(seg); ok {
			return segments, true
		}
	}
	return nil, false
}

// copyableColumnChunks returns the source column chunks of rowGroup if the whole
// row group can be copied verbatim into the writer's output, along with true.
// Otherwise it returns nil and false, indicating the caller must fall back to
// the regular re-encode path.
func (w *Writer) copyableColumnChunks(rowGroup RowGroup) ([]*FileColumnChunk, bool) {
	if disableWriteCopy {
		return nil, false
	}
	// A verbatim byte copy cannot honor encryption: per-page AAD keys depend on
	// the output row-group/column/page ordinals, so an encrypted output requires
	// re-encoding from plaintext.
	if w.writer.encryption != nil {
		return nil, false
	}
	// A verbatim copy reproduces the source row group as a single output row
	// group, so it cannot honor a smaller configured MaxRowsPerRowGroup. When the
	// source exceeds the limit, demote so the row path splits it correctly.
	if rowGroup.NumRows() > w.writer.currentRowGroup.maxRows {
		return nil, false
	}
	// Row groups whose Rows() implements semantics beyond reading the column
	// chunks in order cannot be handled at the chunk level.
	if !chunkTransparentRowGroup(rowGroup) {
		return nil, false
	}

	dst := w.writer.currentRowGroup.columns
	columns := rowGroup.ColumnChunks()
	if len(columns) != len(dst) {
		return nil, false
	}

	srcs := make([]*FileColumnChunk, len(columns))
	for i, col := range columns {
		fc, ok := col.(*FileColumnChunk)
		if !ok {
			return nil, false
		}
		if !columnChunkIsCopyable(dst[i], fc) {
			return nil, false
		}
		srcs[i] = fc
	}
	return srcs, true
}

// chunkTransparentRowGroup reports whether reading rowGroup's column chunks in
// order is equivalent to reading its Rows(). The fast paths in this file and in
// writer_reencode.go operate on the column chunks directly, so they must not be
// applied to row group wrappers whose Rows() adds semantics on top of the
// chunks:
//
//   - dedupRowGroup drops duplicated rows in Rows(); its ColumnChunks() are
//     promoted unchanged from the wrapped row group, so a chunk-level copy
//     would silently retain the duplicates.
//   - convertedRowGroup applies value conversion in Rows() (e.g. numeric or
//     time unit conversions); its ColumnChunks() expose the unconverted source
//     chunks, so a chunk-level copy would emit pre-conversion values under the
//     post-conversion schema. Note that a physical type check is not sufficient
//     to detect this: conversions such as timestamp millis→micros change values
//     while preserving the INT64 physical type.
//
// Identity conversions never produce these wrappers (ConvertRowGroup returns
// the row group unwrapped when schemas are equal), so rejecting them does not
// cost the fast paths anything in the matched-schema case.
func chunkTransparentRowGroup(rowGroup RowGroup) bool {
	switch rowGroup.(type) {
	case *dedupRowGroup, *convertedRowGroup:
		return false
	default:
		return true
	}
}

// loadCopiedChunks stages every source column chunk into its destination
// ColumnWriter. The per-column work here is cheap (it reads only the page index
// and records byte ranges); the data bytes are streamed straight to the output
// during row group assembly, so this is done sequentially.
func (w *Writer) loadCopiedChunks(srcs []*FileColumnChunk) error {
	cols := w.writer.currentRowGroup.columns
	for i, src := range srcs {
		if err := cols[i].loadCopiedChunk(src); err != nil {
			return err
		}
	}
	return nil
}

// columnChunkIsCopyable reports whether the source chunk can be copied verbatim
// into the column managed by dst, producing output indistinguishable from the
// configured re-encode behavior.
func columnChunkIsCopyable(dst *ColumnWriter, src *FileColumnChunk) bool {
	// Source must be plaintext; we cannot copy ciphertext into a plaintext file.
	if src.decryptionKey != nil {
		return false
	}
	// Destination must not be encrypting (also covered by the writer-level check,
	// kept here for defensiveness).
	if dst.encKey != nil {
		return false
	}

	meta := &src.chunk.MetaData

	// Physical type must match.
	if meta.Type != format.Type(dst.columnType.Kind()) {
		return false
	}
	// Compression codec must match the configured codec.
	if meta.Codec != dst.compression.CompressionCodec() {
		return false
	}
	// A writer-requested bloom filter would have to be recomputed from decoded
	// values, which a verbatim copy cannot do. Demote.
	if dst.columnFilter != nil {
		return false
	}
	// We copy statistics and the page index from the source; require both to be
	// present so the output honors the writer's statistics configuration.
	if src.chunk.ColumnIndexOffset == 0 || src.chunk.OffsetIndexOffset == 0 {
		return false
	}
	// All data pages must use the configured encoding and data page version, and
	// dictionary presence must match the configured encoding.
	if !encodingStatsMatch(meta.EncodingStats, dst) {
		return false
	}
	return true
}

// encodingStatsMatch reports whether every page recorded in the source encoding
// stats is compatible with the destination column's configured encoding and
// data page version.
func encodingStatsMatch(stats []format.PageEncodingStats, dst *ColumnWriter) bool {
	if len(stats) == 0 {
		return false // cannot verify the source encodings; demote
	}

	wantPageType := dst.header.page.Type // DataPage (v1) or DataPageV2
	wantEncoding := dst.encoding.Encoding()
	wantDict := dst.dictionary != nil
	sawDict := false

	for _, s := range stats {
		switch s.PageType {
		case format.DictionaryPage:
			sawDict = true
			if !wantDict {
				return false
			}
		case format.DataPage, format.DataPageV2:
			if s.PageType != wantPageType {
				return false // data page version mismatch
			}
			if s.Encoding != wantEncoding {
				return false // encoding mismatch
			}
		default:
			return false
		}
	}

	if wantDict != sawDict {
		return false
	}
	return true
}

// loadCopiedChunk prepares the destination ColumnWriter to emit a source column
// chunk verbatim during row group assembly. It does not read the page bytes —
// only the (much smaller) page index and statistics — and records the source
// byte ranges so the data can be streamed straight to the output later, avoiding
// a per-chunk allocation and an extra copy.
func (c *ColumnWriter) loadCopiedChunk(src *FileColumnChunk) error {
	meta := &src.chunk.MetaData

	// The dictionary page (if any) and the data pages are contiguous byte ranges
	// in the source file.
	dataOffset := meta.DataPageOffset
	dataLength := meta.TotalCompressedSize
	var dictOffset, dictLength int64
	if meta.DictionaryPageOffset != 0 {
		dictOffset = meta.DictionaryPageOffset
		dictLength = meta.DataPageOffset - meta.DictionaryPageOffset
		if dictLength < 0 || dictLength > meta.TotalCompressedSize {
			return fmt.Errorf("invalid source column chunk layout: dictionary offset %d, data offset %d, total %d",
				meta.DictionaryPageOffset, meta.DataPageOffset, meta.TotalCompressedSize)
		}
		dataLength = meta.TotalCompressedSize - dictLength
	}

	// Copy the column index verbatim (type matches, so the raw min/max bytes are
	// directly reusable).
	ci, err := src.ColumnIndex()
	if err != nil {
		return fmt.Errorf("reading source column index: %w", err)
	}

	// Rebuild page locations relative to the start of the data region; the writer
	// re-absolutizes them once the output data page offset is known.
	oi, err := src.OffsetIndex()
	if err != nil {
		return fmt.Errorf("reading source offset index: %w", err)
	}
	srcLocations := oi.(*FileOffsetIndex).index.PageLocations
	locations := make([]format.PageLocation, len(srcLocations))
	for i, loc := range srcLocations {
		loc.Offset -= meta.DataPageOffset
		locations[i] = loc
	}

	cc := &copiedChunk{
		reader:                src.file.reader,
		dictOffset:            dictOffset,
		dictLength:            dictLength,
		dataOffset:            dataOffset,
		dataLength:            dataLength,
		columnIndex:           cloneColumnIndex(ci.(*FileColumnIndex).index),
		sizeStats:             cloneSizeStatistics(meta.SizeStatistics),
		statistics:            cloneStatistics(meta.Statistics),
		encodingStats:         slices.Clone(meta.EncodingStats),
		numValues:             meta.NumValues,
		numRows:               src.rowGroup.NumRows,
		totalUncompressedSize: meta.TotalUncompressedSize,
		totalCompressedSize:   meta.TotalCompressedSize,
	}

	c.copied = cc
	c.numRows = cc.numRows
	c.numPages = len(locations)
	c.offsetIndex.PageLocations = locations
	copyPathCounter.Add(1)

	// Populate the column chunk metadata that the regular path would accumulate
	// via recordPageStats.
	c.columnChunk.MetaData.NumValues = cc.numValues
	c.columnChunk.MetaData.TotalUncompressedSize = cc.totalUncompressedSize
	c.columnChunk.MetaData.TotalCompressedSize = cc.totalCompressedSize
	c.columnChunk.MetaData.Statistics = cc.statistics
	c.columnChunk.MetaData.EncodingStats = append(c.columnChunk.MetaData.EncodingStats[:0], cc.encodingStats...)
	return nil
}

func cloneStatistics(s format.Statistics) format.Statistics {
	s.Min = slices.Clone(s.Min)
	s.Max = slices.Clone(s.Max)
	s.MinValue = slices.Clone(s.MinValue)
	s.MaxValue = slices.Clone(s.MaxValue)
	return s
}

func cloneSizeStatistics(s format.SizeStatistics) format.SizeStatistics {
	s.RepetitionLevelHistogram = slices.Clone(s.RepetitionLevelHistogram)
	s.DefinitionLevelHistogram = slices.Clone(s.DefinitionLevelHistogram)
	return s
}

func cloneColumnIndex(index *format.ColumnIndex) format.ColumnIndex {
	out := *index
	out.NullPages = slices.Clone(index.NullPages)
	out.MinValues = slices.Clone(index.MinValues)
	out.MaxValues = slices.Clone(index.MaxValues)
	out.NullCounts = slices.Clone(index.NullCounts)
	out.RepetitionLevelHistogram = slices.Clone(index.RepetitionLevelHistogram)
	out.DefinitionLevelHistogram = slices.Clone(index.DefinitionLevelHistogram)
	return out
}
