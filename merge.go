package parquet

import (
	"cmp"
	"container/heap"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"

	"github.com/parquet-go/parquet-go/encoding"
	"github.com/parquet-go/parquet-go/format"
)

// MergeRowGroups constructs a row group which is a merged view of rowGroups. If
// rowGroups are sorted and the passed options include sorting, the merged row
// group will also be sorted.
//
// The function validates the input to ensure that the merge operation is
// possible, ensuring that the schemas match or can be converted to an
// optionally configured target schema passed as argument in the option list.
//
// The sorting columns of each row group are also consulted to determine whether
// the output can be represented. If sorting columns are configured on the merge
// they must be a prefix of sorting columns of all row groups being merged.
func MergeRowGroups(rowGroups []RowGroup, options ...RowGroupOption) (RowGroup, error) {
	config, err := NewRowGroupConfig(options...)
	if err != nil {
		return nil, err
	}

	schema := config.Schema
	if len(rowGroups) == 0 {
		return newEmptyRowGroup(schema), nil
	}
	if schema == nil {
		schemas := make([]Node, len(rowGroups))
		for i, rowGroup := range rowGroups {
			schemas[i] = rowGroup.Schema()
		}
		schema = NewSchema(rowGroups[0].Schema().Name(), MergeNodes(schemas...))
	}

	mergedRowGroups := make([]RowGroup, len(rowGroups))
	copy(mergedRowGroups, rowGroups)

	for i, rowGroup := range mergedRowGroups {
		rowGroupSchema := rowGroup.Schema()
		// Always apply conversion when merging multiple row groups to ensure
		// column indices match the merged schema layout. The merge process can
		// reorder fields even when schemas are otherwise identical.
		conv, err := Convert(schema, rowGroupSchema)
		if err != nil {
			return nil, fmt.Errorf("cannot merge row groups: %w", err)
		}
		mergedRowGroups[i] = ConvertRowGroup(rowGroup, conv)
	}

	if len(config.Sorting.SortingColumns) == 0 {
		// When the row group has no ordering, use a simpler version of the
		// merger which simply concatenates rows from each of the row groups.
		// This is preferable because it makes the output deterministic, the
		// heap merge may otherwise reorder rows across groups.
		//
		// IMPORTANT: We need to ensure conversions are applied even in the simple
		// concatenation path. Instead of returning the multiRowGroup directly
		// (which bypasses row-level conversion), we create a simple concatenating
		// row reader that preserves the conversion logic.
		c := new(concatRowGroup)
		c.init(schema, mergedRowGroups)
		return c, nil
	}

	m := &mergedRowGroup{sorting: config.Sorting.SortingColumns}
	m.init(schema, mergedRowGroups)

	for _, rowGroup := range m.rowGroups {
		if !sortingColumnsHavePrefix(rowGroup.SortingColumns(), m.sorting) {
			return nil, ErrRowGroupSortingColumnsMismatch
		}
	}

	m.compare = compareRowsFuncOf(schema, m.sorting)
	return m, nil
}

// concatRowGroup is used when there's no sorting requirement.
// It provides the same interface as multiRowGroup but ensures that row-level
// conversions are applied by reading through the converted row groups instead
// of directly from column chunks.
type concatRowGroup struct {
	multiRowGroup
	//rowGroups []RowGroup // The converted row groups
}

func (s *concatRowGroup) Rows() Rows {
	rowReaders := make([]Rows, len(s.rowGroups))
	for i, rowGroup := range s.rowGroups {
		rowReaders[i] = rowGroup.Rows()
	}
	return &concatRows{readers: rowReaders}
}

// concatRows reads rows sequentially from multiple converted row groups
type concatRows struct {
	readers []Rows
	current int
}

func (s *concatRows) ReadRows(rows []Row) (int, error) {
	for s.current < len(s.readers) {
		n, err := s.readers[s.current].ReadRows(rows)
		if n > 0 {
			return n, nil
		}
		if err != io.EOF {
			return 0, err
		}
		s.current++
	}
	return 0, io.EOF
}

func (s *concatRows) Close() error {
	var lastErr error
	for _, reader := range s.readers {
		if err := reader.Close(); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

func (s *concatRows) Schema() *Schema {
	if len(s.readers) > 0 {
		return s.readers[0].Schema()
	}
	return nil
}

func (s *concatRows) SeekToRow(rowIndex int64) error {
	// Simple implementation: just reset current reader
	// More sophisticated seeking would require tracking row counts
	s.current = 0
	if len(s.readers) > 0 {
		return s.readers[0].SeekToRow(rowIndex)
	}
	return io.EOF
}

type mergedRowGroup struct {
	multiRowGroup
	sorting []SortingColumn
	compare func(Row, Row) int
}

func (m *mergedRowGroup) SortingColumns() []SortingColumn {
	return m.sorting
}

func (m *mergedRowGroup) Rows() Rows {
	// The row group needs to respect a sorting order; the merged row reader
	// uses a heap to merge rows from the row groups.
	rows := make([]Rows, len(m.rowGroups))
	for i := range rows {
		rows[i] = m.rowGroups[i].Rows()
	}
	return &mergedRowGroupRows{
		merge: mergedRowReader{
			compare: m.compare,
			readers: makeBufferedRowReaders(len(rows), func(i int) RowReader { return rows[i] }),
		},
		rows:   rows,
		schema: m.schema,
	}
}

type mergedRowGroupRows struct {
	merge     mergedRowReader
	rowIndex  int64
	seekToRow int64
	rows      []Rows
	schema    *Schema
}

func (r *mergedRowGroupRows) WriteRowsTo(w RowWriter) (n int64, err error) {
	b := newMergeBuffer()
	b.setup(r.rows, r.merge.compare)
	n, err = b.WriteRowsTo(w)
	r.rowIndex += int64(n)
	b.release()
	return
}

func (r *mergedRowGroupRows) readInternal(rows []Row) (int, error) {
	n, err := r.merge.ReadRows(rows)
	r.rowIndex += int64(n)
	return n, err
}

func (r *mergedRowGroupRows) Close() (lastErr error) {
	r.merge.close()
	r.rowIndex = 0
	r.seekToRow = 0

	for _, rows := range r.rows {
		if err := rows.Close(); err != nil {
			lastErr = err
		}
	}

	return lastErr
}

func (r *mergedRowGroupRows) ReadRows(rows []Row) (int, error) {
	for r.rowIndex < r.seekToRow {
		n := min(int(r.seekToRow-r.rowIndex), len(rows))
		n, err := r.readInternal(rows[:n])
		if err != nil {
			return 0, err
		}
	}

	return r.readInternal(rows)
}

func (r *mergedRowGroupRows) SeekToRow(rowIndex int64) error {
	if rowIndex >= r.rowIndex {
		r.seekToRow = rowIndex
		return nil
	}
	return fmt.Errorf("SeekToRow: merged row reader cannot seek backward from row %d to %d", r.rowIndex, rowIndex)
}

func (r *mergedRowGroupRows) Schema() *Schema {
	return r.schema
}

// MergeRowReader constructs a RowReader which creates an ordered sequence of
// all the readers using the given compare function as the ordering predicate.
func MergeRowReaders(readers []RowReader, compare func(Row, Row) int) RowReader {
	return &mergedRowReader{
		compare: compare,
		readers: makeBufferedRowReaders(len(readers), func(i int) RowReader { return readers[i] }),
	}
}

func makeBufferedRowReaders(numReaders int, readerAt func(int) RowReader) []*bufferedRowReader {
	buffers := make([]bufferedRowReader, numReaders)
	readers := make([]*bufferedRowReader, numReaders)

	for i := range readers {
		buffers[i].rows = readerAt(i)
		readers[i] = &buffers[i]
	}

	return readers
}

type mergedRowReader struct {
	compare     func(Row, Row) int
	readers     []*bufferedRowReader
	initialized bool
}

func (m *mergedRowReader) initialize() error {
	for i, r := range m.readers {
		switch err := r.read(); err {
		case nil:
		case io.EOF:
			m.readers[i] = nil
		default:
			m.readers = nil
			return err
		}
	}

	n := 0
	for _, r := range m.readers {
		if r != nil {
			m.readers[n] = r
			n++
		}
	}

	clear := m.readers[n:]
	for i := range clear {
		clear[i] = nil
	}

	m.readers = m.readers[:n]
	heap.Init(m)
	return nil
}

func (m *mergedRowReader) close() {
	for _, r := range m.readers {
		r.close()
	}
	m.readers = nil
}

func (m *mergedRowReader) ReadRows(rows []Row) (n int, err error) {
	if !m.initialized {
		m.initialized = true

		if err := m.initialize(); err != nil {
			return 0, err
		}
	}

	for n < len(rows) && len(m.readers) != 0 {
		r := m.readers[0]
		if r.end == r.off { // This readers buffer has been exhausted, repopulate it.
			if err := r.read(); err != nil {
				if err == io.EOF {
					heap.Pop(m)
					continue
				}
				return n, err
			} else {
				heap.Fix(m, 0)
				continue
			}
		}

		rows[n] = append(rows[n][:0], r.head()...)
		n++

		if err := r.next(); err != nil {
			if err != io.EOF {
				return n, err
			}
			return n, nil
		} else {
			heap.Fix(m, 0)
		}
	}

	if len(m.readers) == 0 {
		err = io.EOF
	}

	return n, err
}

func (m *mergedRowReader) Less(i, j int) bool {
	return m.compare(m.readers[i].head(), m.readers[j].head()) < 0
}

func (m *mergedRowReader) Len() int {
	return len(m.readers)
}

func (m *mergedRowReader) Swap(i, j int) {
	m.readers[i], m.readers[j] = m.readers[j], m.readers[i]
}

func (m *mergedRowReader) Push(x any) {
	panic("NOT IMPLEMENTED")
}

func (m *mergedRowReader) Pop() any {
	i := len(m.readers) - 1
	r := m.readers[i]
	m.readers = m.readers[:i]
	return r
}

type bufferedRowReader struct {
	rows RowReader
	off  int32
	end  int32
	buf  [10]Row
}

func (r *bufferedRowReader) head() Row {
	return r.buf[r.off]
}

func (r *bufferedRowReader) next() error {
	if r.off++; r.off == r.end {
		r.off = 0
		r.end = 0
		// We need to read more rows, however it is unsafe to do so here because we haven't
		// returned the current rows to the caller yet which may cause buffer corruption.
		return io.EOF
	}
	return nil
}

func (r *bufferedRowReader) read() error {
	if r.rows == nil {
		return io.EOF
	}
	n, err := r.rows.ReadRows(r.buf[r.end:])
	if err != nil && n == 0 {
		return err
	}
	r.end += int32(n)
	return nil
}

func (r *bufferedRowReader) close() {
	r.rows = nil
	r.off = 0
	r.end = 0
}

type mergeBuffer struct {
	compare func(Row, Row) int
	rows    []Rows
	buffer  [][]Row
	head    []int
	len     int
	copy    [mergeBufferSize]Row
}

const mergeBufferSize = 1 << 10

func newMergeBuffer() *mergeBuffer {
	return mergeBufferPool.Get().(*mergeBuffer)
}

var mergeBufferPool = &sync.Pool{
	New: func() any {
		return new(mergeBuffer)
	},
}

func (m *mergeBuffer) setup(rows []Rows, compare func(Row, Row) int) {
	m.compare = compare
	m.rows = append(m.rows, rows...)
	size := len(rows)
	if len(m.buffer) < size {
		extra := size - len(m.buffer)
		b := make([][]Row, extra)
		for i := range b {
			b[i] = make([]Row, 0, mergeBufferSize)
		}
		m.buffer = append(m.buffer, b...)
		m.head = append(m.head, make([]int, extra)...)
	}
	m.len = size
}

func (m *mergeBuffer) reset() {
	for i := range m.rows {
		m.buffer[i] = m.buffer[i][:0]
		m.head[i] = 0
	}
	m.rows = m.rows[:0]
	m.compare = nil
	for i := range m.copy {
		m.copy[i] = nil
	}
	m.len = 0
}

func (m *mergeBuffer) release() {
	m.reset()
	mergeBufferPool.Put(m)
}

func (m *mergeBuffer) fill() error {
	m.len = len(m.rows)
	for i := range m.rows {
		if m.head[i] < len(m.buffer[i]) {
			// There is still rows data in m.buffer[i]. Skip filling the row buffer until
			// all rows have been read.
			continue
		}
		m.head[i] = 0
		m.buffer[i] = m.buffer[i][:mergeBufferSize]
		n, err := m.rows[i].ReadRows(m.buffer[i])
		if err != nil {
			if !errors.Is(err, io.EOF) {
				return err
			}
		}
		m.buffer[i] = m.buffer[i][:n]
	}
	heap.Init(m)
	return nil
}

func (m *mergeBuffer) Less(i, j int) bool {
	x := m.buffer[i]
	if len(x) == 0 {
		return false
	}
	y := m.buffer[j]
	if len(y) == 0 {
		return true
	}
	return m.compare(x[m.head[i]], y[m.head[j]]) == -1
}

func (m *mergeBuffer) Pop() any {
	m.len--
	// We don't use the popped value.
	return nil
}

func (m *mergeBuffer) Len() int {
	return m.len
}

func (m *mergeBuffer) Swap(i, j int) {
	m.buffer[i], m.buffer[j] = m.buffer[j], m.buffer[i]
	m.head[i], m.head[j] = m.head[j], m.head[i]
}

func (m *mergeBuffer) Push(x any) {
	panic("NOT IMPLEMENTED")
}

func (m *mergeBuffer) WriteRowsTo(w RowWriter) (n int64, err error) {
	err = m.fill()
	if err != nil {
		return 0, err
	}
	var count int
	for m.left() {
		size := m.read()
		if size == 0 {
			break
		}
		count, err = w.WriteRows(m.copy[:size])
		if err != nil {
			return
		}
		n += int64(count)
		err = m.fill()
		if err != nil {
			return
		}
	}
	return
}

func (m *mergeBuffer) left() bool {
	for i := range m.len {
		if m.head[i] < len(m.buffer[i]) {
			return true
		}
	}
	return false
}

func (m *mergeBuffer) read() (n int64) {
	for n < int64(len(m.copy)) && m.Len() != 0 {
		r := m.buffer[:m.len][0]
		if len(r) == 0 {
			heap.Pop(m)
			continue
		}
		m.copy[n] = append(m.copy[n][:0], r[m.head[0]]...)
		m.head[0]++
		n++
		if m.head[0] < len(r) {
			// There is still rows in this row group. Adjust  the heap
			heap.Fix(m, 0)
		} else {
			heap.Pop(m)
		}
	}
	return
}

// MergeNodes takes a list of nodes and greedily retains properties of the schemas:
// - keeps last compression that is not nil
// - keeps last non-plain encoding that is not nil
// - keeps last non-zero field id
// - union of all columns for group nodes
// - retains the most permissive repetition (required < optional < repeated)
func MergeNodes(nodes ...Node) Node {
	switch len(nodes) {
	case 0:
		return nil
	case 1:
		return nodes[0]
	default:
		merged := nodes[0]
		for _, node := range nodes[1:] {
			merged = mergeTwoNodes(merged, node)
		}
		return merged
	}
}

// mergeTwoNodes merges two nodes using greedy property retention
func mergeTwoNodes(a, b Node) Node {
	leaf1 := a.Leaf()
	leaf2 := b.Leaf()
	// Both must be either leaf or group nodes
	if leaf1 != leaf2 {
		// Cannot merge leaf with group - return the last one
		return b
	}

	var merged Node
	if leaf1 {
		merged = Leaf(b.Type())

		// Apply compression (keep last non-nil)
		compression1 := a.Compression()
		compression2 := b.Compression()
		compression := cmp.Or(compression2, compression1)
		if compression != nil {
			merged = Compressed(merged, compression)
		}

		// Apply encoding (keep last non-plain, non-nil)
		encoding := encoding.Encoding(&Plain)
		encoding1 := a.Encoding()
		encoding2 := b.Encoding()
		if !isPlainEncoding(encoding1) {
			encoding = encoding1
		}
		if !isPlainEncoding(encoding2) {
			encoding = encoding2
		}
		if encoding != nil {
			merged = Encoded(merged, encoding)
		}
	} else {
		fields1 := slices.Clone(a.Fields())
		fields2 := slices.Clone(b.Fields())
		sortFields(fields1)
		sortFields(fields2)

		group := make(Group, len(fields1))
		i1 := 0
		i2 := 0
		for i1 < len(fields1) && i2 < len(fields2) {
			name1 := fields1[i1].Name()
			name2 := fields2[i2].Name()
			switch {
			case name1 < name2:
				group[name1] = nullable(fields1[i1])
				i1++
			case name1 > name2:
				group[name2] = nullable(fields2[i2])
				i2++
			default:
				group[name1] = mergeTwoNodes(fields1[i1], fields2[i2])
				i1++
				i2++
			}
		}

		for _, field := range fields1[i1:] {
			group[field.Name()] = nullable(field)
		}

		for _, field := range fields2[i2:] {
			group[field.Name()] = nullable(field)
		}

		merged = group

		if logicalType := b.Type().LogicalType(); logicalType != nil {
			switch {
			case logicalType.List != nil:
				merged = &listNode{group}
			case logicalType.Map != nil:
				merged = &mapNode{group}
			}
		}
	}

	// Apply repetition (most permissive: required < optional < repeated)
	if a.Repeated() || b.Repeated() {
		merged = Repeated(merged)
	} else if a.Optional() || b.Optional() {
		merged = Optional(merged)
	} else {
		merged = Required(merged)
	}

	// Apply field ID (keep last non-zero)
	return FieldID(merged, cmp.Or(b.ID(), a.ID()))
}

// isPlainEncoding checks if the encoding is plain encoding
func isPlainEncoding(enc encoding.Encoding) bool {
	return enc == nil || enc.Encoding() == format.Plain
}

func nullable(n Node) Node {
	if !n.Repeated() {
		return Optional(n)
	}
	return n
}

var (
	_ RowReaderWithSchema = (*mergedRowGroupRows)(nil)
	_ RowWriterTo         = (*mergedRowGroupRows)(nil)
	_ heap.Interface      = (*mergeBuffer)(nil)
	_ RowWriterTo         = (*mergeBuffer)(nil)
)
