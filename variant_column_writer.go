package parquet

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/parquet-go/parquet-go/variant"
)

// This file implements a columnar, streaming writer for VARIANT columns.
//
// Unlike the row-based write path (column_buffer_variant.go) — which decodes
// each row into a variant.Value tree and shreds the tree — VariantColumnWriter
// consumes variant.ValueWriter events and shreds them as they arrive: values
// matching the shredded type are appended directly to the typed_value column
// buffers, and everything else is encoded to variant binary residuals with a
// streaming variant.Builder, sharing one metadata dictionary per row. No
// intermediate tree, map, or []parquet.Value is ever built, so transcoding
// e.g. JSON into a shredded variant column runs allocation-free per row once
// the writer's buffers are warm.
//
// The shredding semantics are identical to the row path (both follow the
// case tables of the Variant Shredding specification and share
// variantToParquetValue and the shreddedVariantGroup tree):
//
//   - Scalars that exactly match the shredded type go into typed_value with
//     a null value column; mismatches are encoded into the value column.
//   - Objects shred field-wise when the position is shredded as an object:
//     shredded fields descend, missing shredded fields write nulls,
//     non-shredded fields stream into a partial-object residual.
//   - Arrays shred element-wise through the 3-level LIST structure when the
//     position is shredded as a list.
//   - All object field names of the row, shredded or not, are interned into
//     the row's metadata dictionary (as VariantShredding.md requires).

// VariantColumnWriter writes one VARIANT column of a parquet file in
// streaming, columnar form. It implements variant.ValueWriter: each row is
// bracketed by BeginRow and EndRow, and the row's value is described by the
// event methods in between, following the event-sequence rules documented
// on variant.ValueWriter. WriteValue and WriteNullRow are row-level
// conveniences. String and []byte event arguments, and Field names, are
// copied; callers may reuse their backing memory after the call returns.
//
// The writer appends to the column buffers of the ColumnWriters backing the
// variant column's leaf columns and flushes full pages at row boundaries,
// like ColumnWriter.WriteRowValues. Other columns of the file must be
// written separately (e.g. through Writer.ColumnWriters) with the same
// number of rows, and the parent writer must be closed (or its row group
// committed) as usual to flush buffered sub-page data to the file.
//
// Errors are sticky: the first misuse or write failure is retained,
// subsequent events are ignored, and the error is reported by Err, EndRow,
// and row-level methods. After an error the underlying columns may hold a
// partial row, so the parent writer's current row group is inconsistent and
// must be abandoned as a whole, including the other columns' writers: for a
// ConcurrentRowGroupWriter target, do not Commit; for a Writer or
// GenericWriter target, discard the output.
//
// A VariantColumnWriter is not safe for concurrent use, and the parent
// writer must not be flushed, reset, or closed between BeginRow and EndRow
// (rows cannot span pages or row groups).
type VariantColumnWriter struct {
	group    *shreddedVariantGroup
	groupDef int  // definition level at which the variant group is present
	optional bool // whether the variant group node itself is optional
	columns  []*ColumnWriter
	types    []Type // leaf types, indexed by relative column

	// rootValueRequired is set for plain unshredded variant groups, whose
	// value field is required rather than optional.
	rootValueRequired bool

	meta     variant.MetadataBuilder
	metaBuf  []byte
	builders map[*shreddedVariantGroup]*variant.Builder

	// Row state. cur is the shredded node awaiting a value event, nil when
	// no value is expected (inside an object before a Field call, or after
	// the row's value completed). frames tracks open shredded containers,
	// residual an active residual stream.
	inRow       bool
	rowHasValue bool
	cur         *shreddedVariantGroup
	curLevels   shredLevels
	frames      []variantWriterFrame
	residual    variantResidualState
	err         error
}

// variantWriterFrame is one open shredded container (an object or list
// being shredded field-wise or element-wise).
type variantWriterFrame struct {
	node    *shreddedVariantGroup
	isList  bool
	levels  shredLevels // level context of this container occurrence
	seen    []bool      // objects: shredded fields that received a value
	partial *variant.Builder
	elems   int // lists: elements written so far
}

// variantResidualState tracks an active residual stream: events are
// forwarded to b until the container depth returns to base.
type variantResidualState struct {
	b      *variant.Builder
	node   *shreddedVariantGroup
	levels shredLevels
	depth  int
	base   int
}

// VariantWriterTarget is the writer surface VariantColumnWriter binds to;
// it is satisfied by *Writer, *GenericWriter[T], and
// *ConcurrentRowGroupWriter.
type VariantWriterTarget interface {
	Schema() *Schema
	ColumnWriters() []*ColumnWriter
}

var (
	_ VariantWriterTarget = (*Writer)(nil)
	_ VariantWriterTarget = (*GenericWriter[any])(nil)
	_ VariantWriterTarget = (*ConcurrentRowGroupWriter)(nil)
)

// NewVariantColumnWriter returns a streaming columnar writer for the VARIANT
// column at the given path in w's schema (e.g. "event" for a top-level
// column, "attrs", "v" for one nested in a group). The column may be
// unshredded (metadata, value) or shredded (metadata, value, typed_value)
// in any shape permitted by the Variant Shredding specification.
func NewVariantColumnWriter(w VariantWriterTarget, path ...string) (*VariantColumnWriter, error) {
	if len(path) == 0 {
		return nil, fmt.Errorf("variant: NewVariantColumnWriter requires a column path")
	}
	schema := w.Schema()
	if schema == nil {
		return nil, fmt.Errorf("variant: the writer has no schema configured")
	}
	node := Node(schema)
	def := 0
	col := 0
	optional := false
	for _, name := range path {
		if node.Leaf() {
			return nil, fmt.Errorf("variant: column %q not found: %q is a leaf", joinPath(path), name)
		}
		var next Node
		for _, f := range node.Fields() {
			if f.Name() == name {
				next = f
				break
			}
			col += int(numLeafColumnsOf(f))
		}
		if next == nil {
			return nil, fmt.Errorf("variant: column %q not found: no field %q", joinPath(path), name)
		}
		if next.Repeated() {
			return nil, fmt.Errorf("variant: column %q is beneath a repeated field, which VariantColumnWriter does not support", joinPath(path))
		}
		optional = next.Optional()
		if optional {
			def++
		}
		node = next
	}
	if node.Leaf() {
		return nil, fmt.Errorf("variant: column %q is not a variant group", joinPath(path))
	}

	group, err := buildShreddedVariantGroup(node, 0, 0, 0)
	valueRequired := false
	if err != nil {
		// Plain unshredded variant groups declare "required binary value"
		// (VariantEncoding.md), which the shredded-schema validation
		// rejects; accept that shape here.
		if g, ok := unshreddedVariantGroupOf(node); ok {
			group, valueRequired = g, true
		} else {
			return nil, err
		}
	}
	if group.metadataCol < 0 {
		return nil, fmt.Errorf("variant: column %q has no metadata field", joinPath(path))
	}

	columnWriters := w.ColumnWriters()
	if col+group.numCols > len(columnWriters) {
		return nil, fmt.Errorf("variant: column %q spans columns [%d, %d) but the writer has %d", joinPath(path), col, col+group.numCols, len(columnWriters))
	}

	vw := &VariantColumnWriter{
		group:             group,
		groupDef:          def,
		optional:          optional,
		columns:           columnWriters[col : col+group.numCols],
		types:             make([]Type, group.numCols),
		rootValueRequired: valueRequired,
		builders:          make(map[*shreddedVariantGroup]*variant.Builder),
	}
	i := 0
	forEachLeafColumnOf(node, func(leaf leafColumn) {
		if i < len(vw.types) {
			vw.types[i] = leaf.node.Type()
		}
		i++
	})
	return vw, nil
}

// Err returns the first error encountered, or nil.
func (w *VariantColumnWriter) Err() error { return w.err }

func (w *VariantColumnWriter) fail(format string, args ...any) {
	if w.err == nil {
		w.err = fmt.Errorf("variant writer: "+format, args...)
	}
}

// BeginRow starts a new row. Exactly one value — a single scalar event, or
// a single balanced object or array — must be written before the matching
// EndRow.
func (w *VariantColumnWriter) BeginRow() error {
	if w.err != nil {
		return w.err
	}
	if w.inRow {
		w.fail("BeginRow inside a row")
		return w.err
	}
	w.inRow = true
	w.rowHasValue = false
	w.cur = w.group
	w.curLevels = shredLevels{baseDef: w.groupDef}
	w.meta.Reset()
	return nil
}

// EndRow completes the current row: it writes the row's metadata dictionary
// and flushes any column whose buffered page reached the writer's page size.
func (w *VariantColumnWriter) EndRow() error {
	if w.err == nil && !w.inRow {
		w.fail("EndRow outside of a row")
	}
	if w.err == nil && (w.residual.b != nil || len(w.frames) > 0) {
		w.fail("EndRow with an unclosed object or array")
	}
	if w.err == nil && !w.rowHasValue {
		w.fail("EndRow without a value; use WriteNullRow for SQL null rows")
	}
	if w.err != nil {
		return w.err
	}
	w.metaBuf = w.meta.AppendTo(w.metaBuf[:0])
	w.leaf(w.group.metadataCol).writeByteArray(w.levels(byte(w.groupDef), 0), w.metaBuf)
	w.inRow = false
	w.cur = nil
	return w.flush()
}

// WriteNullRow writes a row where the variant column itself is null (a SQL
// null, distinct from the variant null value). The variant group must be
// optional. Any optional ancestors of the variant group are written as
// present; the writer cannot express a row where an ancestor group is null.
func (w *VariantColumnWriter) WriteNullRow() error {
	if w.err != nil {
		return w.err
	}
	if w.inRow {
		w.fail("WriteNullRow inside a row")
		return w.err
	}
	if !w.optional {
		w.fail("WriteNullRow on a required variant column")
		return w.err
	}
	lv := w.levels(byte(w.groupDef-1), 0)
	for col := range w.columns {
		w.leaf(col).writeNull(lv)
	}
	return w.flush()
}

// WriteValue writes one row holding the given variant value: it brackets
// v.Write(w) with BeginRow and EndRow and reports any error either raised.
func (w *VariantColumnWriter) WriteValue(v variant.Value) error {
	if err := w.BeginRow(); err != nil {
		return err
	}
	v.Write(w)
	return w.EndRow()
}

// leaf returns the column buffer of the relative leaf column, creating it on
// first use like ColumnWriter.WriteRowValues does.
func (w *VariantColumnWriter) leaf(col int) ColumnBuffer {
	c := w.columns[col]
	if c.columnBuffer == nil {
		c.columnBuffer = c.newColumnBuffer()
		if c.originalColumnBuffer == nil {
			c.originalColumnBuffer = c.columnBuffer
		}
	}
	return c.columnBuffer
}

func (w *VariantColumnWriter) levels(def, rep byte) columnLevels {
	return columnLevels{definitionLevel: def, repetitionLevel: rep}
}

// flush flushes any column whose buffer reached the configured page size.
// It only runs at row boundaries, since a row cannot span two pages.
func (w *VariantColumnWriter) flush() error {
	for _, c := range w.columns {
		if c.columnBuffer != nil && c.columnBuffer.Size() >= int64(c.bufferSize) {
			if err := c.Flush(); err != nil {
				w.err = err
				return err
			}
		}
	}
	return nil
}

// builder returns the residual builder of a shredded node, sharing the
// row's metadata dictionary.
func (w *VariantColumnWriter) builder(node *shreddedVariantGroup) *variant.Builder {
	b := w.builders[node]
	if b == nil {
		b = variant.NewBuilderWithMetadata(&w.meta)
		w.builders[node] = b
	}
	b.Reset()
	return b
}

// writeNulls writes one null to every leaf column in [startCol, endCol).
func (w *VariantColumnWriter) writeNulls(startCol, endCol int, def, rep byte) {
	lv := w.levels(def, rep)
	for col := startCol; col < endCol; col++ {
		w.leaf(col).writeNull(lv)
	}
}

// writeMissing writes a missing occurrence of a shredded node (both value
// and typed_value null), which is how absent object fields are represented.
func (w *VariantColumnWriter) writeMissing(g *shreddedVariantGroup, l shredLevels) {
	if g.valueCol >= 0 {
		w.leaf(g.valueCol).writeNull(w.levels(l.def(g.defLevel), l.rep))
	}
	if g.typed != nil {
		w.writeNulls(g.typed.startCol, g.typed.startCol+g.typed.numCols, l.def(g.defLevel), l.rep)
	}
}

// writeResidualBytes writes encoded variant bytes to the node's value
// column.
func (w *VariantColumnWriter) writeResidualBytes(g *shreddedVariantGroup, l shredLevels, data []byte) {
	if g.valueCol < 0 {
		w.fail("value does not match the shredded type and the schema has no value column to fall back to")
		return
	}
	def := l.def(g.defLevel) + 1
	if g == w.group && w.rootValueRequired {
		def = byte(w.groupDef)
	}
	w.leaf(g.valueCol).writeByteArray(w.levels(def, l.rep), data)
}

// writeTyped writes a matched scalar to the node's typed_value column.
func (w *VariantColumnWriter) writeTyped(col int, pv Value, def, rep byte) {
	buf := w.leaf(col)
	lv := w.levels(def, rep)
	switch pv.Kind() {
	case Boolean:
		buf.writeBoolean(lv, pv.Boolean())
	case Int32:
		buf.writeInt32(lv, pv.Int32())
	case Int64:
		buf.writeInt64(lv, pv.Int64())
	case Float:
		buf.writeFloat(lv, pv.Float())
	case Double:
		buf.writeDouble(lv, pv.Double())
	default:
		buf.writeByteArray(lv, pv.ByteArray())
	}
}

// valueDone advances the row state after one value completed at node level:
// back to the row, to the enclosing object (awaiting the next Field), or to
// the next element of the enclosing list.
func (w *VariantColumnWriter) valueDone() {
	w.cur = nil
	if n := len(w.frames); n > 0 {
		f := &w.frames[n-1]
		if f.isList {
			f.elems++
			w.cur = f.node.typed.element
			w.curLevels = f.levels
			w.curLevels.rep = byte(f.levels.baseRep + f.node.typed.element.repDepth)
		}
	} else {
		w.rowHasValue = true
	}
}

// forward reports whether events must stream into an active residual.
func (w *VariantColumnWriter) forward() bool {
	return w.err == nil && w.residual.b != nil
}

// residualDone finalizes a completed residual stream.
func (w *VariantColumnWriter) residualDone() {
	r := w.residual
	w.residual = variantResidualState{}
	if r.base > 0 {
		// A residual field of a partial object completed; the partial
		// object builder stays open in its frame.
		return
	}
	data, err := r.b.Bytes()
	if err != nil {
		w.err = err
		return
	}
	w.writeResidualBytes(r.node, r.levels, data)
	w.valueDone()
}

func (w *VariantColumnWriter) afterResidualScalar() {
	if w.residual.depth == w.residual.base {
		w.residualDone()
	}
}

// scalar shreds one scalar value at the current position: into typed_value
// on an exact type match, into the value column as variant binary otherwise.
func (w *VariantColumnWriter) scalar(v variant.Value) {
	if w.err != nil {
		return
	}
	node := w.cur
	if node == nil {
		w.fail("unexpected value event")
		return
	}
	l := w.curLevels
	if t := node.typed; t != nil && t.kind == shreddedTypedPrimitive {
		if pv, ok := variantToParquetValue(v, w.types[t.startCol]); ok {
			w.writeTyped(t.startCol, pv, l.def(t.defLevel), l.rep)
			if node.valueCol >= 0 {
				w.leaf(node.valueCol).writeNull(w.levels(l.def(node.defLevel), l.rep))
			}
			w.valueDone()
			return
		}
	}
	b := w.builder(node)
	v.Write(b)
	data, err := b.Bytes()
	if err != nil {
		w.err = err
		return
	}
	w.writeResidualBytes(node, l, data)
	if node.typed != nil {
		w.writeNulls(node.typed.startCol, node.typed.startCol+node.typed.numCols, l.def(node.defLevel), l.rep)
	}
	w.valueDone()
}

// startResidual begins streaming a container that does not match the
// shredded type into the node's residual builder, nulling the typed columns.
func (w *VariantColumnWriter) startResidual(node *shreddedVariantGroup, l shredLevels) *variant.Builder {
	if node.typed != nil {
		w.writeNulls(node.typed.startCol, node.typed.startCol+node.typed.numCols, l.def(node.defLevel), l.rep)
	}
	b := w.builder(node)
	w.residual = variantResidualState{b: b, node: node, levels: l}
	return b
}

// BeginObject starts an object value at the current position. If the
// position is shredded as an object, its fields shred individually; any
// other shredded type is a conflict and the whole object streams into the
// position's value column.
func (w *VariantColumnWriter) BeginObject() {
	if w.forward() {
		w.residual.b.BeginObject()
		w.residual.depth++
		return
	}
	if w.err != nil {
		return
	}
	node := w.cur
	if node == nil {
		w.fail("unexpected BeginObject event")
		return
	}
	l := w.curLevels
	if t := node.typed; t != nil && t.kind == shreddedTypedObject {
		w.pushFrame(node, false, l)
		w.cur = nil
		return
	}
	b := w.startResidual(node, l)
	b.BeginObject()
	w.residual.depth = 1
	w.cur = nil
}

// Field names the next value written as a field of the innermost open
// object. Values of shredded fields go to the field's columns; others
// stream into the object's partial residual.
func (w *VariantColumnWriter) Field(name string) {
	if w.forward() {
		w.residual.b.Field(name)
		return
	}
	if w.err != nil {
		return
	}
	n := len(w.frames)
	if n == 0 || w.frames[n-1].isList {
		w.fail("Field %q outside of an object", name)
		return
	}
	f := &w.frames[n-1]
	if w.cur != nil {
		w.fail("Field %q: previous field has no value", name)
		return
	}
	// VariantShredding.md requires all field names, shredded or not, to be
	// present in the metadata dictionary.
	w.meta.Add(name)
	node := f.node
	for i := range node.typed.fields {
		if node.typed.fields[i].name == name {
			if f.seen[i] {
				w.fail("duplicate object field %q", name)
				return
			}
			f.seen[i] = true
			w.cur = node.typed.fields[i].group
			w.curLevels = f.levels
			return
		}
	}
	// Not a shredded field: it goes into the partial-object residual.
	if f.partial == nil {
		f.partial = w.builder(node)
		f.partial.BeginObject()
	}
	f.partial.Field(name)
	w.residual = variantResidualState{b: f.partial, node: node, levels: f.levels, depth: 1, base: 1}
}

// EndObject closes the innermost open object, recording shredded fields
// that received no value as missing and committing the partial residual, if
// any, to the object's value column.
func (w *VariantColumnWriter) EndObject() {
	if w.forward() {
		w.residual.b.EndObject()
		w.residual.depth--
		if w.residual.depth == w.residual.base {
			w.residualDone()
		}
		return
	}
	if w.err != nil {
		return
	}
	n := len(w.frames)
	if n == 0 || w.frames[n-1].isList {
		w.fail("EndObject without matching BeginObject")
		return
	}
	f := &w.frames[n-1]
	if w.cur != nil {
		w.fail("EndObject: last field has no value")
		return
	}
	node, l := f.node, f.levels
	for i := range node.typed.fields {
		if !f.seen[i] {
			w.writeMissing(node.typed.fields[i].group, l)
		}
	}
	if f.partial != nil {
		f.partial.EndObject()
		data, err := f.partial.Bytes()
		if err != nil {
			w.err = err
			return
		}
		w.writeResidualBytes(node, l, data)
	} else if node.valueCol >= 0 {
		w.leaf(node.valueCol).writeNull(w.levels(l.def(node.defLevel), l.rep))
	}
	w.frames = w.frames[:n-1]
	w.valueDone()
}

// BeginArray starts an array value at the current position. If the position
// is shredded as a list, every value until the matching EndArray shreds as
// one element; any other shredded type is a conflict and the whole array
// streams into the position's value column.
func (w *VariantColumnWriter) BeginArray() {
	if w.forward() {
		w.residual.b.BeginArray()
		w.residual.depth++
		return
	}
	if w.err != nil {
		return
	}
	node := w.cur
	if node == nil {
		w.fail("unexpected BeginArray event")
		return
	}
	l := w.curLevels
	if t := node.typed; t != nil && t.kind == shreddedTypedList {
		w.pushFrame(node, true, l)
		// The first element carries the enclosing occurrence's repetition
		// level; valueDone switches to the element repetition level for
		// subsequent elements.
		w.cur = t.element
		w.curLevels = l
		return
	}
	b := w.startResidual(node, l)
	b.BeginArray()
	w.residual.depth = 1
	w.cur = nil
}

// EndArray closes the innermost open array.
func (w *VariantColumnWriter) EndArray() {
	if w.forward() {
		w.residual.b.EndArray()
		w.residual.depth--
		if w.residual.depth == w.residual.base {
			w.residualDone()
		}
		return
	}
	if w.err != nil {
		return
	}
	n := len(w.frames)
	if n == 0 || !w.frames[n-1].isList {
		w.fail("EndArray without matching BeginArray")
		return
	}
	f := &w.frames[n-1]
	node, l := f.node, f.levels
	if f.elems == 0 {
		// typed_value present but the list is empty: every leaf records
		// the list's definition level.
		t := node.typed
		w.writeNulls(t.startCol, t.startCol+t.numCols, l.def(t.defLevel), l.rep)
	}
	if node.valueCol >= 0 {
		w.leaf(node.valueCol).writeNull(w.levels(l.def(node.defLevel), l.rep))
	}
	w.frames = w.frames[:n-1]
	w.valueDone()
}

func (w *VariantColumnWriter) pushFrame(node *shreddedVariantGroup, isList bool, l shredLevels) {
	if len(w.frames) < cap(w.frames) {
		w.frames = w.frames[:len(w.frames)+1]
	} else {
		w.frames = append(w.frames, variantWriterFrame{})
	}
	f := &w.frames[len(w.frames)-1]
	f.node = node
	f.isList = isList
	f.levels = l
	f.partial = nil
	f.elems = 0
	if !isList {
		numFields := len(node.typed.fields)
		if cap(f.seen) < numFields {
			f.seen = make([]bool, numFields)
		} else {
			f.seen = f.seen[:numFields]
			clear(f.seen)
		}
	}
}

// The scalar event methods below write one scalar value at the current
// position: the row's value, the current object field, or the next array
// element. A scalar matching the position's shredded type goes to its
// typed_value column; any other scalar encodes into the value column.

// Null writes the variant null value (distinct from a missing field and
// from a SQL null row; see WriteNullRow). Variant null never matches a
// shredded type: it encodes into the position's value column, or fails the
// row when the position is shredded without one.
func (w *VariantColumnWriter) Null() {
	if w.forward() {
		w.residual.b.Null()
		w.afterResidualScalar()
		return
	}
	w.scalar(variant.Null())
}

func (w *VariantColumnWriter) Bool(v bool) {
	if w.forward() {
		w.residual.b.Bool(v)
		w.afterResidualScalar()
		return
	}
	w.scalar(variant.Bool(v))
}

func (w *VariantColumnWriter) Int8(v int8) {
	if w.forward() {
		w.residual.b.Int8(v)
		w.afterResidualScalar()
		return
	}
	w.scalar(variant.Int8(v))
}

func (w *VariantColumnWriter) Int16(v int16) {
	if w.forward() {
		w.residual.b.Int16(v)
		w.afterResidualScalar()
		return
	}
	w.scalar(variant.Int16(v))
}

func (w *VariantColumnWriter) Int32(v int32) {
	if w.forward() {
		w.residual.b.Int32(v)
		w.afterResidualScalar()
		return
	}
	w.scalar(variant.Int32(v))
}

func (w *VariantColumnWriter) Int64(v int64) {
	if w.forward() {
		w.residual.b.Int64(v)
		w.afterResidualScalar()
		return
	}
	w.scalar(variant.Int64(v))
}

func (w *VariantColumnWriter) Float(v float32) {
	if w.forward() {
		w.residual.b.Float(v)
		w.afterResidualScalar()
		return
	}
	w.scalar(variant.Float(v))
}

func (w *VariantColumnWriter) Double(v float64) {
	if w.forward() {
		w.residual.b.Double(v)
		w.afterResidualScalar()
		return
	}
	w.scalar(variant.Double(v))
}

func (w *VariantColumnWriter) String(v string) {
	if w.forward() {
		w.residual.b.String(v)
		w.afterResidualScalar()
		return
	}
	w.scalar(variant.String(v))
}

func (w *VariantColumnWriter) Binary(v []byte) {
	if w.forward() {
		w.residual.b.Binary(v)
		w.afterResidualScalar()
		return
	}
	w.scalar(variant.Binary(v))
}

func (w *VariantColumnWriter) Date(days int32) {
	if w.forward() {
		w.residual.b.Date(days)
		w.afterResidualScalar()
		return
	}
	w.scalar(variant.Date(days))
}

func (w *VariantColumnWriter) Time(micros int64) {
	if w.forward() {
		w.residual.b.Time(micros)
		w.afterResidualScalar()
		return
	}
	w.scalar(variant.Time(micros))
}

func (w *VariantColumnWriter) Timestamp(micros int64) {
	if w.forward() {
		w.residual.b.Timestamp(micros)
		w.afterResidualScalar()
		return
	}
	w.scalar(variant.Timestamp(micros))
}

func (w *VariantColumnWriter) TimestampNTZ(micros int64) {
	if w.forward() {
		w.residual.b.TimestampNTZ(micros)
		w.afterResidualScalar()
		return
	}
	w.scalar(variant.TimestampNTZ(micros))
}

func (w *VariantColumnWriter) TimestampNanos(nanos int64) {
	if w.forward() {
		w.residual.b.TimestampNanos(nanos)
		w.afterResidualScalar()
		return
	}
	w.scalar(variant.TimestampNanos(nanos))
}

func (w *VariantColumnWriter) TimestampNTZNanos(nanos int64) {
	if w.forward() {
		w.residual.b.TimestampNTZNanos(nanos)
		w.afterResidualScalar()
		return
	}
	w.scalar(variant.TimestampNTZNanos(nanos))
}

func (w *VariantColumnWriter) UUID(v uuid.UUID) {
	if w.forward() {
		w.residual.b.UUID(v)
		w.afterResidualScalar()
		return
	}
	w.scalar(variant.UUID(v))
}

func (w *VariantColumnWriter) Decimal4(unscaled int32, scale byte) {
	if w.forward() {
		w.residual.b.Decimal4(unscaled, scale)
		w.afterResidualScalar()
		return
	}
	w.scalar(variant.Decimal4(unscaled, scale))
}

func (w *VariantColumnWriter) Decimal8(unscaled int64, scale byte) {
	if w.forward() {
		w.residual.b.Decimal8(unscaled, scale)
		w.afterResidualScalar()
		return
	}
	w.scalar(variant.Decimal8(unscaled, scale))
}

func (w *VariantColumnWriter) Decimal16(unscaled [16]byte, scale byte) {
	if w.forward() {
		w.residual.b.Decimal16(unscaled, scale)
		w.afterResidualScalar()
		return
	}
	w.scalar(variant.Decimal16(unscaled, scale))
}

var _ variant.ValueWriter = (*VariantColumnWriter)(nil)
