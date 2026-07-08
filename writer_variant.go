package parquet

import (
	"fmt"
	"io"
	"slices"
)

// This file integrates the columnar variant copy (CopyVariantRows, see
// variant_column_copy.go) with Writer.WriteRowGroup, so the standard
// compaction pipeline
//
//	merged, _ := parquet.MergeRowGroups(rowGroups)
//	writer.WriteRowGroup(merged)
//
// moves variant columns through VariantReader and VariantColumnWriter
// instead of reconstructing rows. The fast path applies when the row group
// is a concatenation (an unsorted merge, or a single file row group) and
// the schemas match everywhere except inside variant groups; sorted merges
// are inherently row-ordered and keep the row-based path, which re-shreds
// through the schema conversion (see convert_variant.go).

// writeRowGroupVariantColumnar copies a concatenated row group with variant
// columns into the writer column-by-column: variant columns re-shred
// through CopyVariantRows, other columns copy values with their levels
// unchanged. The boolean result reports whether the fast path applied; when
// false the caller must fall back to the row-based copy.
func (w *Writer) writeRowGroupVariantColumnar(rowGroup RowGroup) (int64, bool, error) {
	sources, ok := variantCopySourcesOf(rowGroup, nil)
	if !ok || len(sources) == 0 {
		return 0, false, nil
	}
	plans := make([]*variantCopyPlan, len(sources))
	needsReshred := false
	for i, src := range sources {
		plan, ok := makeVariantCopyPlan(w.schema, src.Schema())
		if !ok {
			return 0, false, nil
		}
		plans[i] = plan
		needsReshred = needsReshred || plan.needsReshred
	}
	if !needsReshred {
		// Every source already stores the variant columns in the target's
		// shredding; the generic row copy moves the column values without
		// decoding anything and is cheaper than re-shredding.
		return 0, false, nil
	}
	if rowGroup.NumRows() > w.writer.currentRowGroup.maxRows {
		// The row group would have to be split; the row-based path commits
		// intermediate row groups as it copies.
		return 0, false, nil
	}

	if err := w.writer.flush(); err != nil {
		return 0, true, err
	}
	w.writer.currentRowGroup.configureBloomFilters(rowGroup.ColumnChunks())

	writers := make(map[string]*VariantColumnWriter, len(plans[0].variantPaths))
	for _, path := range plans[0].variantPaths {
		vw, err := NewVariantColumnWriter(w, path...)
		if err != nil {
			return 0, true, err
		}
		writers[joinPath(path)] = vw
	}

	columns := w.writer.currentRowGroup.columns
	var buf []Value
	var rows int64
	for i, src := range sources {
		plan := plans[i]
		chunks := src.ColumnChunks()
		var err error
		for _, pair := range plan.columnPairs {
			if buf, err = copyColumnChunkRows(columns[pair[0]], chunks[pair[1]], buf); err != nil {
				return 0, true, err
			}
		}
		for _, path := range plan.variantPaths {
			vr, err := NewVariantReader(src, path...)
			if err != nil {
				return 0, true, err
			}
			_, err = CopyVariantRows(writers[joinPath(path)], vr)
			if cerr := vr.Close(); err == nil {
				err = cerr
			}
			if err != nil {
				return 0, true, err
			}
		}
		// Every column must have advanced by the source's row count;
		// anything else means the source columns disagree and the output
		// would be corrupt.
		rows += src.NumRows()
		for _, c := range columns {
			if n := c.totalRowCount(); n != rows {
				return 0, true, fmt.Errorf("parquet: variant row group copy wrote %d rows to column %q, expected %d", n, c.columnPath, rows)
			}
		}
	}

	n, err := w.writer.writeRowGroup(w.writer.currentRowGroup, rowGroup.Schema(), rowGroup.SortingColumns())
	return n, true, err
}

// variantCopySourcesOf decomposes a row group into the ordered list of
// source row groups the columnar copy reads from. Only shapes whose rows
// are exactly the concatenation of their sources qualify: file row groups,
// unsorted merges, and converted views of either (unwrapped to their source
// so variant columns are read in the source's shredding). Sorted merges,
// deduplicated views, and unknown row group types return false.
func variantCopySourcesOf(rowGroup RowGroup, out []RowGroup) ([]RowGroup, bool) {
	switch rg := rowGroup.(type) {
	case *concatenatedRowGroup:
		return variantCopyMultiSourcesOf(&rg.multiRowGroup, out)
	case *multiRowGroup:
		return variantCopyMultiSourcesOf(rg, out)
	case *convertedRowGroup:
		return append(out, rg.rowGroup), true
	case *FileRowGroup:
		return append(out, rg), true
	default:
		return nil, false
	}
}

func variantCopyMultiSourcesOf(m *multiRowGroup, out []RowGroup) ([]RowGroup, bool) {
	if len(m.sorting) != 0 {
		return nil, false
	}
	for _, rg := range m.rowGroups {
		var ok bool
		if out, ok = variantCopySourcesOf(rg, out); !ok {
			return nil, false
		}
	}
	return out, true
}

// variantCopyPlan pairs the leaf columns of a copy-compatible target and
// source schema: variant groups are copied through the variant readers and
// writers, every other leaf column is copied value-by-value.
type variantCopyPlan struct {
	variantPaths [][]string
	columnPairs  [][2]int // {target leaf column, source leaf column}
	// needsReshred is true when at least one variant column is shredded
	// differently in the source than in the target. When false, the plain
	// row copy passes every column value through verbatim and re-shredding
	// machinery would only add overhead.
	needsReshred bool
}

// makeVariantCopyPlan reports whether the source schema can be copied into
// the target column-by-column: the schemas must be equal everywhere except
// inside variant groups (same fields, repetitions, and leaf types, in any
// field order), and variant groups must sit under required fields only. An
// optional or repeated ancestor stores its levels in the variant's leaf
// columns, which the variant readers and writers cannot express: the reader
// reports every enclosing null as a missing row, and the writer writes
// every null row at the variant group's own level, so the copy would
// silently rewrite "ancestor is null" as "variant is null".
func makeVariantCopyPlan(target, source *Schema) (*variantCopyPlan, bool) {
	plan := &variantCopyPlan{}
	if !planVariantCopy(target, source, 0, 0, nil, plan) {
		return nil, false
	}
	if len(plan.variantPaths) == 0 {
		return nil, false // nothing variant about it; use the generic paths
	}
	return plan, true
}

func planVariantCopy(t, s Node, tCol, sCol int, path []string, plan *variantCopyPlan) bool {
	if isVariant(t) || isVariant(s) {
		if !isVariant(t) || !isVariant(s) || !repetitionsAreEqual(t, s) || t.Repeated() {
			return false
		}
		if _, _, err := variantGroupOf(t); err != nil {
			return false
		}
		if _, _, err := variantGroupOf(s); err != nil {
			return false
		}
		if !variantNodesEquivalent(t, s) {
			plan.needsReshred = true
		}
		plan.variantPaths = append(plan.variantPaths, slices.Clone(path))
		return true
	}
	if t.Leaf() || s.Leaf() {
		if !t.Leaf() || !s.Leaf() || !leafNodesAreEqual(t, s) {
			return false
		}
		plan.columnPairs = append(plan.columnPairs, [2]int{tCol, sCol})
		return true
	}
	if !repetitionsAreEqual(t, s) {
		return false
	}
	sFields := s.Fields()
	tFields := t.Fields()
	if len(tFields) != len(sFields) {
		return false
	}
	sColOf := make(map[string]int, len(sFields))
	sFieldOf := make(map[string]Field, len(sFields))
	col := sCol
	for _, sf := range sFields {
		sColOf[sf.Name()] = col
		sFieldOf[sf.Name()] = sf
		col += int(numLeafColumnsOf(sf))
	}
	for _, tf := range tFields {
		sf, ok := sFieldOf[tf.Name()]
		if !ok {
			return false
		}
		if !tf.Required() && !tf.Leaf() && !isVariant(tf) && schemaHasVariant(tf) {
			// A variant group below an optional or repeated field cannot be
			// copied columnar (see makeVariantCopyPlan). The variant group
			// itself may be optional: its own presence is part of what the
			// variant readers and writers track.
			return false
		}
		if !planVariantCopy(tf, sf, tCol, sColOf[tf.Name()], append(path, tf.Name()), plan) {
			return false
		}
		tCol += int(numLeafColumnsOf(tf))
	}
	return true
}

// schemaHasVariant reports whether any variant group appears in the subtree.
func schemaHasVariant(node Node) bool {
	if isVariant(node) {
		return true
	}
	if node.Leaf() {
		return false
	}
	return slices.ContainsFunc(node.Fields(), func(f Field) bool {
		return schemaHasVariant(f)
	})
}

// copyColumnChunkRows copies every value of a column chunk, with its levels,
// into a column writer, batching at row boundaries (a value with repetition
// level zero starts a new row, and a batch must hold whole rows). The value
// buffer is returned for reuse across columns.
func copyColumnChunkRows(dst *ColumnWriter, src ColumnChunk, buf []Value) ([]Value, error) {
	reader := columnChunkValueReader{pages: src.Pages()}
	switch src.Type().Kind() {
	case ByteArray, FixedLenByteArray:
		// Carried values of partial rows must survive page transitions.
		reader.detach = true
	}
	defer reader.Close()

	if len(buf) == 0 {
		buf = make([]Value, defaultValueBufferSize)
	}
	pending := 0
	for {
		n, err := reader.ReadValues(buf[pending:])
		total := pending + n
		if err == io.EOF {
			if total > 0 {
				if _, err := dst.WriteRowValues(buf[:total]); err != nil {
					return buf, err
				}
			}
			return buf, nil
		}
		if err != nil {
			return buf, err
		}
		// Hold back the trailing values that may belong to an incomplete
		// row: only values before the last row start are known-complete.
		cut := 0
		for i := total - 1; i >= 1; i-- {
			if buf[i].repetitionLevel == 0 {
				cut = i
				break
			}
		}
		if cut == 0 {
			if total == len(buf) {
				buf = append(buf, make([]Value, len(buf))...)
			}
			pending = total
			continue
		}
		if _, err := dst.WriteRowValues(buf[:cut]); err != nil {
			return buf, err
		}
		copy(buf, buf[cut:total])
		pending = total - cut
	}
}
