package parquet

import (
	"bytes"
	"fmt"
	"io"
	"reflect"
	"unsafe"

	"github.com/parquet-go/parquet-go/encoding"
	"github.com/parquet-go/parquet-go/internal/unsafecast"
)

// This file implements the bulk decoding path used by GenericReader for
// struct fields holding lists of fixed-size primitive values (e.g. a
// []float32 field storing vector embeddings).
//
// Instead of materializing one parquet.Value per list element, copying it
// into a Row, and assigning it to the Go slice through reflection, eligible
// columns are "detached" from the row reading pipeline (see rowGroupRows)
// and read by a listColumnReader which copies list elements page-wise,
// straight from the decoded page data into a backing array shared by all
// rows of a read batch.
//
// Pages detected to contain fixed-length lists (fixedRepeatedPage) have
// their row boundaries computed arithmetically; other pages fall back to
// deriving boundaries from the decoded repetition/definition levels, but
// still bypass the Value pipeline. The results are bit-identical to the
// regular path in both cases.

// listFastPathReader drives the bulk list read path of a GenericReader.
type listFastPathReader struct {
	schema      *Schema
	rowGroup    RowGroup
	detached    []bool
	columns     []listFastPathColumn
	allDetached bool
	rows        *rowGroupRows
	rowIndex    int64
	numRows     int64
}

type listFastPathColumn struct {
	field    Field
	setSlice func(fieldPtr, base unsafe.Pointer, n int)
	reader   listColumnReader
}

// setSliceOf writes a []E slice header at fieldPtr without going through
// reflection. A nil base produces a nil slice; a non-nil base with n == 0
// produces an empty non-nil slice, matching what Schema.Reconstruct returns
// for null and empty lists respectively.
func setSliceOf[E any](fieldPtr, base unsafe.Pointer, n int) {
	if base == nil {
		*(*[]E)(fieldPtr) = nil
	} else {
		*(*[]E)(fieldPtr) = unsafe.Slice((*E)(base), n)
	}
}

// newListFastPathReader analyzes the pairing of a row group schema with the
// Go struct type rows are read into, and returns a listFastPathReader if at
// least one column is eligible for the bulk list read path.
//
// The schema must be the exact schema of the row group (no conversion), and
// rowType must be the struct type the schema was generated from.
func newListFastPathReader(schema *Schema, rowGroup RowGroup, rowType reflect.Type) *listFastPathReader {
	if !fixedListFastPathEnabled {
		return nil
	}
	if schema == nil || rowType == nil || rowType.Kind() != reflect.Struct {
		return nil
	}
	columnChunks := rowGroup.ColumnChunks()
	numColumns := len(schema.Columns())
	if numColumns != len(columnChunks) {
		return nil
	}

	detached := make([]bool, numColumns)
	columns := []listFastPathColumn(nil)
	columnIndex := 0

	for _, field := range schema.Fields() {
		numLeaves := numLeafColumns(field, 0)
		leafIndex := columnIndex
		columnIndex += numLeaves

		if numLeaves != 1 || leafIndex >= numColumns {
			continue
		}
		leaf, nullDef, ok := eligibleListField(field)
		if !ok {
			continue
		}
		setSlice, elemSize, ok := listSliceSetterOf(rowType, field, leaf)
		if !ok {
			continue
		}

		maxDef := byte(1)
		if field.Optional() {
			maxDef = 2
		}
		detached[leafIndex] = true
		columns = append(columns, listFastPathColumn{
			field:    field,
			setSlice: setSlice,
			reader: listColumnReader{
				chunk:    columnChunks[leafIndex],
				kind:     encodingKindOf(leaf.Type().Kind()),
				elemSize: elemSize,
				maxDef:   maxDef,
				nullDef:  nullDef,
			},
		})
	}

	if len(columns) == 0 {
		return nil
	}
	return &listFastPathReader{
		schema:      schema,
		rowGroup:    rowGroup,
		detached:    detached,
		columns:     columns,
		allDetached: len(columns) == numColumns,
		rowIndex:    -1,
		numRows:     rowGroup.NumRows(),
	}
}

// eligibleListField reports whether a top-level schema field is a list of
// required fixed-size primitive elements with a repetition level of one.
// It returns the leaf node holding the values and the definition level at or
// below which the list itself is null (-1 when the list cannot be null).
func eligibleListField(field Field) (leaf Node, nullDef int, ok bool) {
	nullDef = -1
	if field.Optional() {
		// One definition level for the optional wrapper: level 0 is a null
		// list, level 1 an empty one.
		nullDef = 0
	}
	switch {
	case field.Leaf():
		if !field.Repeated() {
			return nil, 0, false
		}
		leaf = field
	case isList(field):
		defer func() {
			// listElementOf panics on malformed list nodes.
			if recover() != nil {
				leaf, ok = nil, false
			}
		}()
		elem := listElementOf(field)
		if !elem.Leaf() || elem.Optional() || elem.Repeated() {
			return nil, 0, false
		}
		leaf = elem
	default:
		return nil, 0, false
	}
	switch leaf.Type().Kind() {
	case Int32, Int64, Float, Double:
		return leaf, nullDef, true
	}
	return nil, 0, false
}

// listSliceSetterOf checks that the Go struct field paired with the schema
// field is a slice of the primitive type matching the parquet leaf column,
// and returns the function used to write slice headers into row values.
func listSliceSetterOf(rowType reflect.Type, field Field, leaf Node) (setSlice func(fieldPtr, base unsafe.Pointer, n int), elemSize int, ok bool) {
	defer func() {
		// Field.Value may panic if the schema was not generated from rowType.
		if recover() != nil {
			setSlice, elemSize, ok = nil, 0, false
		}
	}()

	fieldValue := field.Value(reflect.New(rowType).Elem())
	if !fieldValue.IsValid() || fieldValue.Kind() != reflect.Slice {
		return nil, 0, false
	}

	switch elem, kind := fieldValue.Type().Elem(), leaf.Type().Kind(); {
	case elem == reflect.TypeFor[float32]() && kind == Float:
		return setSliceOf[float32], 4, true
	case elem == reflect.TypeFor[float64]() && kind == Double:
		return setSliceOf[float64], 8, true
	case elem == reflect.TypeFor[int32]() && kind == Int32:
		return setSliceOf[int32], 4, true
	case elem == reflect.TypeFor[uint32]() && kind == Int32:
		return setSliceOf[uint32], 4, true
	case elem == reflect.TypeFor[int64]() && kind == Int64:
		return setSliceOf[int64], 8, true
	case elem == reflect.TypeFor[uint64]() && kind == Int64:
		return setSliceOf[uint64], 8, true
	}
	return nil, 0, false
}

// baseRows returns the row reader used for the non-detached columns,
// creating it on the first call.
func (f *listFastPathReader) baseRows() *rowGroupRows {
	if f.rows == nil {
		f.rows = newRowGroupRowsDetached(f.schema, f.rowGroup.ColumnChunks(), defaultValueBufferSize, f.detached)
	}
	return f.rows
}

func (f *listFastPathReader) SeekToRow(rowIndex int64) error {
	if !f.allDetached {
		if err := f.baseRows().SeekToRow(rowIndex); err != nil {
			return err
		}
	}
	for i := range f.columns {
		if err := f.columns[i].reader.seekToRow(rowIndex); err != nil {
			return err
		}
	}
	f.rowIndex = rowIndex
	return nil
}

// invalidate forces the next read to re-seek all readers, realigning the
// columns after an error interrupted a read batch mid-way.
func (f *listFastPathReader) invalidate() {
	f.rowIndex = -1
}

func (f *listFastPathReader) Close() error {
	var err error
	if f.rows != nil {
		err = f.rows.Close()
	}
	for i := range f.columns {
		if closeErr := f.columns[i].reader.close(); err == nil {
			err = closeErr
		}
	}
	return err
}

// listSpan describes the list of one row within the backing array produced
// by a call to listColumnReader.readLists.
type listSpan struct {
	start int // offset of the first element (in elements)
	n     int // number of elements
	null  bool
}

// emptyListBase is the base pointer used for empty (but non-null) list
// slices when a read batch produced no elements at all. It points to an
// 8-byte aligned allocation so that it can be converted to a pointer of any
// of the supported element types.
var emptyListBase = new(uint64)

// encodingKindOf translates a parquet physical type to the corresponding
// encoding.Values kind.
func encodingKindOf(kind Kind) encoding.Kind {
	switch kind {
	case Boolean:
		return encoding.Boolean
	case Int32:
		return encoding.Int32
	case Int64:
		return encoding.Int64
	case Int96:
		return encoding.Int96
	case Float:
		return encoding.Float
	case Double:
		return encoding.Double
	case ByteArray:
		return encoding.ByteArray
	case FixedLenByteArray:
		return encoding.FixedLenByteArray
	default:
		return encoding.Undefined
	}
}

// listColumnReader reads the pages of one repeated leaf column and assembles
// the raw list elements of consecutive rows.
type listColumnReader struct {
	chunk    ColumnChunk
	kind     encoding.Kind
	elemSize int
	maxDef   byte
	nullDef  int

	// Current page state. elems views the page's decoded values when the
	// page is plain-encoded; dictionary-encoded pages expose indices and
	// dictData instead.
	pages    Pages
	page     Page
	fixedN   int    // > 0 when the current page holds fixed-length lists
	reps     []byte // repetition levels (nil in fixed mode)
	defs     []byte // definition levels (nil in fixed mode)
	elems    []byte
	indices  []int32
	dictData []byte
	numElems int // total elements in the current page
	slot     int // next level slot (general mode)
	elemOff  int // next element index

	// Per-batch outputs of readLists. The backing array is allocated fresh
	// for every batch because its ownership is transferred to the assembled
	// row values; spans are reused across batches.
	backing []byte
	spans   []listSpan
}

// appendBacking appends src to the batch backing array, keeping the array in
// 8-byte aligned allocations so that the assembled slices are always aligned
// for their element type.
func (c *listColumnReader) appendBacking(src []byte) {
	if size := len(c.backing) + len(src); size > cap(c.backing) {
		grown := unsafecast.Slice[byte](make([]uint64, (max(size, 2*cap(c.backing), 512)+7)/8))
		c.backing = grown[:copy(grown[:len(c.backing)], c.backing)]
	}
	c.backing = append(c.backing, src...)
}

func (c *listColumnReader) close() (err error) {
	c.releasePage()
	if c.pages != nil {
		err = c.pages.Close()
		c.pages = nil
	}
	return err
}

func (c *listColumnReader) seekToRow(rowIndex int64) error {
	if c.pages == nil {
		c.pages = c.chunk.Pages()
	}
	c.releasePage()
	return c.pages.SeekToRow(rowIndex)
}

func (c *listColumnReader) releasePage() {
	if c.page != nil {
		Release(c.page)
		c.page = nil
		c.reps, c.defs = nil, nil
		c.elems, c.indices, c.dictData = nil, nil, nil
		c.fixedN, c.numElems, c.slot, c.elemOff = 0, 0, 0, 0
	}
}

func (c *listColumnReader) nextPage() error {
	c.releasePage()
	if c.pages == nil {
		c.pages = c.chunk.Pages()
	}
	for {
		page, err := c.pages.ReadPage()
		if err != nil {
			return err
		}
		if page.NumValues() == 0 {
			Release(page)
			continue
		}
		if err := c.setPage(page); err != nil {
			Release(page)
			return err
		}
		return nil
	}
}

func (c *listColumnReader) setPage(page Page) error {
	inner := Page(page)
	if buffered, ok := inner.(*bufferedPage); ok {
		inner = buffered.Page
	}

	valuesPage := inner
	if fixed, ok := inner.(*fixedRepeatedPage); ok {
		c.fixedN = fixed.listLength
		valuesPage = fixed.base
	} else {
		c.reps = inner.RepetitionLevels()
		c.defs = inner.DefinitionLevels()
		if len(c.defs) == 0 || len(c.reps) != len(c.defs) {
			return fmt.Errorf("parquet: list column %d page has malformed levels (%d repetition, %d definition)",
				page.Column(), len(c.reps), len(c.defs))
		}
	}

	data := valuesPage.Data()
	if dict := page.Dictionary(); dict != nil {
		if data.Kind() != encoding.Int32 {
			return fmt.Errorf("parquet: list column %d dictionary page indexes have kind %d", page.Column(), data.Kind())
		}
		c.indices = data.Int32()
		dictValues := dict.Page().Data()
		if dictValues.Kind() != c.kind {
			return fmt.Errorf("parquet: list column %d dictionary holds unexpected data of kind %d", page.Column(), dictValues.Kind())
		}
		c.dictData, _ = dictValues.Data()
		c.numElems = len(c.indices)
		// Validate the indexes ahead of time so that gathering values from
		// the dictionary cannot access memory out of bounds on corrupted
		// inputs.
		numDictValues := int32(len(c.dictData) / c.elemSize)
		for _, index := range c.indices {
			if index < 0 || index >= numDictValues {
				return fmt.Errorf("parquet: list column %d contains dictionary index out of bounds: %d/%d",
					page.Column(), index, numDictValues)
			}
		}
	} else {
		elems, _ := data.Data()
		if data.Kind() != c.kind || len(elems)%c.elemSize != 0 {
			return fmt.Errorf("parquet: list column %d page holds unexpected data of kind %d", page.Column(), data.Kind())
		}
		c.elems = elems
		c.numElems = len(elems) / c.elemSize
	}
	c.page = page
	return nil
}

// appendElems appends elements [elemOff, elemOff+n) of the current page to
// the backing array.
func (c *listColumnReader) appendElems(n int) {
	if c.indices == nil {
		c.appendBacking(c.elems[c.elemOff*c.elemSize : (c.elemOff+n)*c.elemSize])
	} else {
		size := c.elemSize
		for _, index := range c.indices[c.elemOff : c.elemOff+n] {
			c.appendBacking(c.dictData[int(index)*size : (int(index)+1)*size])
		}
	}
	c.elemOff += n
}

func (c *listColumnReader) pageDone() bool {
	if c.fixedN > 0 {
		return c.elemOff == c.numElems
	}
	return c.slot == len(c.defs)
}

// readLists assembles the lists of the next count rows into c.backing and
// c.spans. It returns an error if the column runs out of values before count
// rows were assembled, which would indicate that the column is out of sync
// with the rest of the row group.
func (c *listColumnReader) readLists(count int) error {
	c.backing = nil
	c.spans = c.spans[:0]

	for len(c.spans) < count {
		if c.page == nil || c.pageDone() {
			if err := c.nextPage(); err != nil {
				if err == io.EOF {
					err = io.ErrUnexpectedEOF
				}
				return err
			}
		}
		if c.fixedN > 0 {
			c.readFixedRows(count - len(c.spans))
		} else if err := c.readGeneralRow(); err != nil {
			return err
		}
	}
	return nil
}

// readFixedRows consumes up to count rows from the current fixed-length list
// page with a single bulk copy.
func (c *listColumnReader) readFixedRows(count int) {
	n := c.fixedN
	if rowsLeft := (c.numElems - c.elemOff) / n; count > rowsLeft {
		count = rowsLeft
	}
	start := len(c.backing) / c.elemSize
	c.appendElems(count * n)
	for i := range count {
		c.spans = append(c.spans, listSpan{start: start + i*n, n: n})
	}
}

// readGeneralRow consumes one row from the current general (variable-length)
// page, following the row across page boundaries when it does not end on the
// current page (which may happen in data pages v1).
func (c *listColumnReader) readGeneralRow() error {
	if c.reps[c.slot] != 0 {
		return fmt.Errorf("parquet: list column %d row starts with repetition level %d", c.page.Column(), c.reps[c.slot])
	}
	if def := c.defs[c.slot]; def < c.maxDef {
		// Null or empty list, occupying a single value slot.
		c.spans = append(c.spans, listSpan{null: int(def) <= c.nullDef})
		c.slot++
		return nil
	}

	start := len(c.backing) / c.elemSize
	n := 0
	for {
		// All slots of a present list hold present elements: the element is
		// required, so definition levels below the maximum can only appear
		// on the empty and null list slots handled above.
		k := 1 + bytes.IndexByte(c.reps[c.slot+1:], 0)
		last := k == 0
		if last {
			k = len(c.reps) - c.slot
		}
		if countLevelsEqual(c.defs[c.slot:c.slot+k], c.maxDef) != k {
			return fmt.Errorf("parquet: list column %d contains null elements", c.page.Column())
		}
		c.appendElems(k)
		c.slot += k
		n += k
		if !last {
			break
		}
		// The row may continue on the next page. A page starting with a
		// non-zero repetition level continues the current row; any other
		// page (including fixed-length list pages, which always start on a
		// row boundary) belongs to the next row.
		if err := c.nextPage(); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if c.fixedN > 0 || c.reps[0] == 0 {
			break
		}
	}
	c.spans = append(c.spans, listSpan{start: start, n: n})
	return nil
}

// setRowSlices writes the assembled lists of the last readLists call into
// the rows' struct fields. rowOf must return the addressable reflect.Value
// of the i-th row.
func (col *listFastPathColumn) setRowSlices(rowOf func(int) reflect.Value, numRows int) {
	c := &col.reader
	var base unsafe.Pointer
	if len(c.backing) > 0 {
		base = unsafe.Pointer(&c.backing[0])
	} else {
		// Empty lists still need a non-nil base pointer to produce empty
		// non-nil slices.
		base = unsafe.Pointer(emptyListBase)
	}

	elemSize := uintptr(c.elemSize)
	for i := range numRows {
		span := c.spans[i]
		fieldPtr := unsafe.Pointer(col.field.Value(rowOf(i)).UnsafeAddr())
		if span.null {
			col.setSlice(fieldPtr, nil, 0)
		} else {
			col.setSlice(fieldPtr, unsafe.Add(base, uintptr(span.start)*elemSize), span.n)
		}
	}
}
