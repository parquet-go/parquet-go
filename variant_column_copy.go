package parquet

import (
	"fmt"
	"io"
	"slices"

	"github.com/google/uuid"
	"github.com/parquet-go/parquet-go/format"
	"github.com/parquet-go/parquet-go/internal/unsafecast"
	"github.com/parquet-go/parquet-go/variant"
)

// This file implements columnar copying of VARIANT columns: CopyVariantRows
// bridges a VariantReader (variant_column_reader.go) to a
// VariantColumnWriter (variant_column_writer.go), re-shredding rows from the
// source file's shredding schema to the destination's. It is the core of a
// compaction pipeline: merging many small files into one, changing the
// shredding schema of existing data, or shredding previously unshredded
// files, all without materializing rows.
//
// Values are moved at the finest granularity the two schemas allow:
//
//   - Rows and subtrees stored in typed_value columns are walked through the
//     reader's cursors and replayed as scalar events, so they flow from the
//     source column buffers to the destination column buffers without ever
//     being decoded to variant binary or boxed into a variant.Value.
//   - Residual variant binary is walked in place and replayed as events
//     (variant.Replay), without materializing a variant.Value. The walk is
//     unavoidable in general: residual bytes reference the source row's
//     metadata dictionary by field ID, and the destination row's dictionary
//     is rebuilt from the events of the whole row.
//
// When the source and destination shredding schemas agree at a position, a
// replayed typed value matches the destination's typed_value column and is
// stored typed again; where they disagree, the destination writer re-shreds
// per its own case tables. Whole-row-group copies between identical file
// schemas do not need this machinery at all: Writer.WriteRowGroup already
// copies row groups without decoding.

// variantCopyWindow is the number of rows CopyVariantRows reads from the
// source per window. It bounds the memory buffered by the reader while
// keeping per-window overhead (level walks, buffer resets) amortized.
const variantCopyWindow = 256

// CopyVariantRows copies every remaining row of the variant column read by r
// into w and returns the number of rows copied (including null rows). The
// source and destination shredding schemas are independent: values are
// re-shredded to w's schema as they are copied.
//
// CopyVariantRows drives r's row window itself; cursors it creates project
// every shredded column of the source. Rows are appended to w one at a time,
// so the caller must keep any other columns of the destination file in step
// (or give the destination a schema whose only columns are the variant's).
// On error, rows already copied remain written and w must be abandoned along
// with its parent writer's current row group, as documented on
// VariantColumnWriter.
//
// The variant column should not sit below optional or repeated fields on
// either side: the reader reports a null enclosing level and a null variant
// column both as missing rows, and the writer writes missing rows as null
// variant columns, so enclosing nulls would not round-trip.
func CopyVariantRows(w *VariantColumnWriter, r *VariantReader) (int64, error) {
	cp, err := newVariantCopier(r.Root())
	if err != nil {
		return 0, err
	}
	var n int64
	for {
		k, err := r.Next(variantCopyWindow)
		if err == io.EOF {
			return n, nil
		}
		if err != nil {
			return n, err
		}
		cp.beginWindow()
		locs := cp.cur.Locs()
		for e := range k {
			if locs[e] == variant.LocMissing {
				err = w.WriteNullRow()
			} else if err = w.BeginRow(); err == nil {
				if err = cp.copyEntry(w, e); err == nil {
					err = w.EndRow()
				}
			}
			if err != nil {
				return n, err
			}
			n++
		}
	}
}

// variantCopier mirrors the source's shredded schema tree with one node per
// cursor, caching what the per-row copy loop needs: the scalar emitter of
// leaf positions and the dense-vector position of the current window.
type variantCopier struct {
	cur        *VariantCursor
	emit       func(w *VariantColumnWriter, c *VariantCursor, d int) error
	fieldNames []string
	fields     []*variantCopier
	elems      *variantCopier

	// skipShredded reports whether a residual field name is covered by the
	// shredded schema, for filtering partial-object leftovers. Built once
	// so the per-row replay does not allocate a closure.
	skipShredded func(name string) bool

	// dense is the index of the next typed value of the current window.
	// Entries are visited in entry order and every variant.LocTyped entry is
	// visited exactly once (typed entries exist only below typed ancestors,
	// which the copy always descends into), so a running counter matches
	// the dense vectors without materializing TypedRows' inverse.
	dense int
}

func newVariantCopier(c *VariantCursor) (*variantCopier, error) {
	cp := &variantCopier{cur: c}
	switch c.Kind() {
	case VariantCursorLeaf:
		emit, err := variantTypedEmitter(c.LeafType())
		if err != nil {
			return nil, err
		}
		cp.emit = emit
	case VariantCursorObject:
		cp.fieldNames = c.Fields()
		cp.fields = make([]*variantCopier, len(cp.fieldNames))
		for i, name := range cp.fieldNames {
			ch, err := newVariantCopier(c.Field(name))
			if err != nil {
				return nil, err
			}
			cp.fields[i] = ch
		}
		cp.skipShredded = func(name string) bool {
			return slices.Contains(cp.fieldNames, name)
		}
	case VariantCursorList:
		ch, err := newVariantCopier(c.Elements())
		if err != nil {
			return nil, err
		}
		cp.elems = ch
	}
	return cp, nil
}

func (cp *variantCopier) beginWindow() {
	cp.dense = 0
	for _, f := range cp.fields {
		f.beginWindow()
	}
	if cp.elems != nil {
		cp.elems.beginWindow()
	}
}

// copyEntry replays the value at entry e of cp's cursor as events on w.
// Callers skip variant.LocMissing entries (missing object fields emit
// nothing, missing rows become WriteNullRow); a Missing entry reaching here
// means the source file is corrupt, and degrades to a variant null.
func (cp *variantCopier) copyEntry(w *VariantColumnWriter, e int) error {
	switch cp.cur.Locs()[e] {
	case variant.LocTyped:
		d := cp.dense
		cp.dense++
		return cp.emit(w, cp.cur, d)

	case variant.LocTypedObject:
		w.BeginObject()
		for i, name := range cp.fieldNames {
			ch := cp.fields[i]
			if ch.cur.Locs()[e] == variant.LocMissing {
				continue
			}
			w.Field(name)
			if err := ch.copyEntry(w, e); err != nil {
				return err
			}
		}
		// Leftover fields of a partially shredded object. Fields whose name
		// is in the shredded schema are ignored like the row-based reader
		// does: the shredded field wins over a non-compliant residual copy.
		// ReplayObjectFields replays nothing if the bytes are not an object
		// (the spec requires an object here).
		if r := cp.cur.residualAt(e); r != nil {
			if r.decoded {
				// Already decoded by the reader for cursor navigation.
				if v := r.val; v.Basic() == variant.BasicObject {
					for _, f := range v.ObjectValue().Fields {
						if cp.skipShredded(f.Name) {
							continue
						}
						w.Field(f.Name)
						f.Value.Write(w)
					}
				}
			} else if r.bytes != nil {
				m, err := cp.cur.reader.metadataFor(cp.cur.rowOf(e))
				if err != nil {
					return err
				}
				if err := variant.ReplayObjectFields(w, m, r.bytes, cp.skipShredded); err != nil {
					return err
				}
			}
		}
		w.EndObject()

	case variant.LocTypedList:
		offsets := cp.cur.ListOffsets()
		w.BeginArray()
		for i := offsets[e]; i < offsets[e+1]; i++ {
			if err := cp.elems.copyEntry(w, int(i)); err != nil {
				return err
			}
		}
		w.EndArray()

	case variant.LocResidual:
		r := cp.cur.residualAt(e)
		if r == nil {
			w.Null()
			break
		}
		if r.decoded {
			// Already decoded by the reader for cursor navigation
			// (navigated entries and residuals below cursors with
			// children).
			r.val.Write(w)
			break
		}
		if r.bytes == nil {
			w.Null()
			break
		}
		// Replay the residual bytes as events without materializing a
		// variant.Value; this is the only per-row work of the copy that
		// would otherwise allocate.
		m, err := cp.cur.reader.metadataFor(cp.cur.rowOf(e))
		if err != nil {
			return err
		}
		if err := variant.Replay(w, m, r.bytes); err != nil {
			return err
		}

	default: // variant.LocNull, variant.LocMissing
		w.Null()
	}
	return nil
}

// The remainder of this file integrates the columnar copy with
// Writer.WriteRowGroup, so the standard compaction pipeline
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

// variantTypedEmitter returns a function that emits the d-th dense typed
// value of a leaf cursor as the matching variant scalar event, per the
// shredded types table (the event-space image of parquetToVariantValue).
func variantTypedEmitter(typ Type) (func(w *VariantColumnWriter, c *VariantCursor, d int) error, error) {
	lt := typ.LogicalType()
	var ltValue format.LogicalTypeValue
	if lt != nil {
		ltValue = lt.Value
	}
	switch lt := ltValue.(type) {
	case nil:
		switch typ.Kind() {
		case Boolean:
			return func(w *VariantColumnWriter, c *VariantCursor, d int) error {
				w.Bool(c.Booleans()[d])
				return nil
			}, nil
		case Int32:
			return func(w *VariantColumnWriter, c *VariantCursor, d int) error {
				w.Int32(c.Int32s()[d])
				return nil
			}, nil
		case Int64:
			return func(w *VariantColumnWriter, c *VariantCursor, d int) error {
				w.Int64(c.Int64s()[d])
				return nil
			}, nil
		case Float:
			return func(w *VariantColumnWriter, c *VariantCursor, d int) error {
				w.Float(c.Floats()[d])
				return nil
			}, nil
		case Double:
			return func(w *VariantColumnWriter, c *VariantCursor, d int) error {
				w.Double(c.Doubles()[d])
				return nil
			}, nil
		case ByteArray:
			return func(w *VariantColumnWriter, c *VariantCursor, d int) error {
				slab, offsets := c.ByteArrays()
				w.Binary(slab[offsets[d]:offsets[d+1]])
				return nil
			}, nil
		}

	case *format.StringType:
		return func(w *VariantColumnWriter, c *VariantCursor, d int) error {
			slab, offsets := c.ByteArrays()
			// The writer copies the string bytes before the slab is
			// invalidated by the next window, so an unsafe view is fine.
			w.String(unsafecast.String(slab[offsets[d]:offsets[d+1]]))
			return nil
		}, nil

	case *format.IntType:
		switch lt.BitWidth {
		case 8:
			return func(w *VariantColumnWriter, c *VariantCursor, d int) error {
				w.Int8(int8(c.Int32s()[d]))
				return nil
			}, nil
		case 16:
			return func(w *VariantColumnWriter, c *VariantCursor, d int) error {
				w.Int16(int16(c.Int32s()[d]))
				return nil
			}, nil
		case 32:
			return func(w *VariantColumnWriter, c *VariantCursor, d int) error {
				w.Int32(c.Int32s()[d])
				return nil
			}, nil
		case 64:
			return func(w *VariantColumnWriter, c *VariantCursor, d int) error {
				w.Int64(c.Int64s()[d])
				return nil
			}, nil
		}

	case *format.DateType:
		return func(w *VariantColumnWriter, c *VariantCursor, d int) error {
			w.Date(c.Int32s()[d])
			return nil
		}, nil

	case *format.TimeType:
		if _, ok := lt.Unit.Value.(*format.MicroSeconds); ok {
			return func(w *VariantColumnWriter, c *VariantCursor, d int) error {
				w.Time(c.Int64s()[d])
				return nil
			}, nil
		}

	case *format.TimestampType:
		utc := lt.IsAdjustedToUTC
		switch lt.Unit.Value.(type) {
		case *format.MicroSeconds:
			return func(w *VariantColumnWriter, c *VariantCursor, d int) error {
				if utc {
					w.Timestamp(c.Int64s()[d])
				} else {
					w.TimestampNTZ(c.Int64s()[d])
				}
				return nil
			}, nil
		case *format.NanoSeconds:
			return func(w *VariantColumnWriter, c *VariantCursor, d int) error {
				if utc {
					w.TimestampNanos(c.Int64s()[d])
				} else {
					w.TimestampNTZNanos(c.Int64s()[d])
				}
				return nil
			}, nil
		}

	case *format.DecimalType:
		scale := byte(lt.Scale)
		switch typ.Kind() {
		case Int32:
			return func(w *VariantColumnWriter, c *VariantCursor, d int) error {
				w.Decimal4(c.Int32s()[d], scale)
				return nil
			}, nil
		case Int64:
			return func(w *VariantColumnWriter, c *VariantCursor, d int) error {
				w.Decimal8(c.Int64s()[d], scale)
				return nil
			}, nil
		case FixedLenByteArray:
			if typ.Length() > 16 {
				break // variant decimals are at most 16 bytes
			}
			return func(w *VariantColumnWriter, c *VariantCursor, d int) error {
				slab, size := c.FixedLenByteArrays()
				w.Decimal16(bigEndianToLittleEndian16(slab[d*size:(d+1)*size]), scale)
				return nil
			}, nil
		case ByteArray:
			return func(w *VariantColumnWriter, c *VariantCursor, d int) error {
				slab, offsets := c.ByteArrays()
				b := slab[offsets[d]:offsets[d+1]]
				if len(b) > 16 {
					return fmt.Errorf("variant: decimal value wider than 16 bytes")
				}
				w.Decimal16(bigEndianToLittleEndian16(b), scale)
				return nil
			}, nil
		}

	case *format.UUIDType:
		if typ.Kind() != FixedLenByteArray || typ.Length() != 16 {
			break // UUIDs are FIXED_LEN_BYTE_ARRAY(16)
		}
		return func(w *VariantColumnWriter, c *VariantCursor, d int) error {
			slab, size := c.FixedLenByteArrays()
			w.UUID(uuid.UUID(slab[d*size : (d+1)*size]))
			return nil
		}, nil
	}
	return nil, fmt.Errorf("variant: unsupported shredded leaf type %s", typ)
}
