package parquet

import (
	"container/heap"
	"errors"
	"fmt"
	"io"
	"sync"
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
		schema = rowGroups[0].Schema()

		for _, rowGroup := range rowGroups[1:] {
			if !nodesAreEqual(schema, rowGroup.Schema()) {
				return nil, ErrRowGroupSchemaMismatch
			}
		}
	}

	mergedRowGroups := make([]RowGroup, len(rowGroups))
	copy(mergedRowGroups, rowGroups)

	for i, rowGroup := range mergedRowGroups {
		if rowGroupSchema := rowGroup.Schema(); !nodesAreEqual(schema, rowGroupSchema) {
			conv, err := Convert(schema, rowGroupSchema)
			if err != nil {
				return nil, fmt.Errorf("cannot merge row groups: %w", err)
			}
			mergedRowGroups[i] = ConvertRowGroup(rowGroup, conv)
		}
	}

	m := &mergedRowGroup{sorting: config.Sorting.SortingColumns}
	m.init(schema, mergedRowGroups)

	if len(m.sorting) == 0 {
		// When the row group has no ordering, use a simpler version of the
		// merger which simply concatenates rows from each of the row groups.
		// This is preferable because it makes the output deterministic, the
		// heap merge may otherwise reorder rows across groups.
		return &m.multiRowGroup, nil
	}

	for _, rowGroup := range m.rowGroups {
		if !sortingColumnsHavePrefix(rowGroup.SortingColumns(), m.sorting) {
			return nil, ErrRowGroupSortingColumnsMismatch
		}
	}

	m.compare = compareRowsFuncOf(schema, m.sorting)
	return m, nil
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
			r:       makeBufferedRowReaders(len(rows), func(i int) RowReader { return rows[i] }),
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

func (r *mergedRowGroupRows) WriteRowsTo(w RowWriter) (n int64, err error) {
	n, err = r.merge.WriteRowsTo(w)
	r.rowIndex += int64(n)
	return n, err
}

func (r *mergedRowGroupRows) ReadRows(rows []Row) (int, error) {
	for r.rowIndex < r.seekToRow {
		n := int(r.seekToRow - r.rowIndex)
		if n > len(rows) {
			n = len(rows)
		}
		_, err := r.readInternal(rows[:n])
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
		r:       makeBufferedRowReaders(len(readers), func(i int) RowReader { return readers[i] }),
	}
}

func makeBufferedRowReaders(numReaders int, readerAt func(int) RowReader) []*bufferedRowReader {
	readers := make([]*bufferedRowReader, numReaders)
	for i := range readers {
		readers[i] = newBufferedRowReader()
		readers[i].rows = readerAt(i)
	}
	return readers
}

type mergedRowReader struct {
	compare     func(Row, Row) int
	r           []*bufferedRowReader
	len         int
	initialized bool
}

func (m *mergedRowReader) readers() []*bufferedRowReader {
	return m.r[:m.len]
}

func (m *mergedRowReader) reset() {
	for i := range m.r {
		m.r[i].clear()
	}
}

func (m *mergedRowReader) initialize() error {
	m.len = len(m.r)
	for i, r := range m.r {
		switch err := r.read(); err {
		case nil:
		case io.EOF:
			m.r[i].close()
			m.r[i] = nil
		default:
			m.len = 0
			return err
		}
	}

	n := 0
	for _, r := range m.r {
		if r != nil {
			m.r[n] = r
			n++
		}
	}
	clear := m.r[n:]
	for i := range clear {
		if clear[i] != nil {
			clear[i].close()
			clear[i] = nil
		}
	}
	m.len = n
	m.r = m.r[:n]
	heap.Init(m)
	return nil
}

func (m *mergedRowReader) close() {
	for _, r := range m.r {
		r.close()
	}
	m.r = nil
}

func (m *mergedRowReader) WriteRowsTo(w RowWriter) (n int64, err error) {
	err = m.fill()
	if err != nil {
		return
	}
	b := newChunk()
	rows := b.buf
	m.len = len(m.r)
	if !m.canPop() {
		return 0, io.EOF
	}
	defer b.release()
	// at least one row should be available
	for m.canPop() {
		count := m.writeTo(rows)
		if count == 0 {
			return
		}
		_, err = w.WriteRows(rows[:count])
		if err != nil {
			return
		}
		n += count
		m.len = len(m.r)
		err = m.fill()
		if err != nil {
			return
		}
		b.reset()
	}
	println(n)
	return
}

func (m *mergedRowReader) writeTo(rows []Row) (n int64) {
	for n < int64(len(rows)) && m.Len() != 0 {
		r := m.readers()[0]
		rows[n] = append(rows[n][:0], r.head()...)
		n++
		if !r.buf.canPop() {
			heap.Pop(m)
		} else {
			heap.Fix(m, 0)
		}
	}
	return
}

func (m *mergedRowReader) canPop() bool {
	for _, r := range m.readers() {
		if r.buf.canPop() {
			return true
		}
	}
	return false
}

func (m *mergedRowReader) fill() error {
	m.len = len(m.r)
	for _, r := range m.r {
		err := r.fill()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				return err
			}
		}
	}
	heap.Init(m)
	return nil
}

func (m *mergedRowReader) ReadRows(rows []Row) (n int, err error) {
	if !m.initialized {
		m.initialized = true

		if err := m.initialize(); err != nil {
			return 0, err
		}
	} else {
		m.reset()
	}

	for n < len(rows) && len(m.readers()) != 0 {
		r := m.readers()[0]

		rows[n] = append(rows[n][:0], r.head()...)
		n++

		if err := r.next(); err != nil {
			if err != io.EOF {
				return n, err
			}
			heap.Pop(m)
		} else {
			heap.Fix(m, 0)
		}
	}

	if len(m.readers()) == 0 {
		err = io.EOF
	}

	return n, err
}

func (m *mergedRowReader) Less(i, j int) bool {
	x, y := m.readers()[i].peek(), m.readers()[j].peek()
	if len(x) == 0 {
		return false
	}
	if len(y) == 0 {
		return true
	}
	return m.compare(x, y) < 0
}

func (m *mergedRowReader) Len() int {
	return m.len
}

func (m *mergedRowReader) Swap(i, j int) {
	r := m.readers()
	r[i], r[j] = r[j], r[i]
}

func (m *mergedRowReader) Push(x interface{}) {
	panic("NOT IMPLEMENTED")
}

func (m *mergedRowReader) Pop() interface{} {
	r := m.r[m.len-1]
	m.len--
	return r
}

type bufferedRowReader struct {
	rows    RowReader
	discard []*chunk
	buf     *chunk
}

type chunk struct {
	head, tail int
	buf        []Row
}

func (c *chunk) canPop() bool {
	return c.head < c.tail
}

func (c *chunk) pop() Row {
	if c.head < c.tail {
		r := c.buf[c.head]
		c.head++
		return r
	}
	return nil
}

func (c *chunk) peek() Row {
	if c.head < c.tail {
		r := c.buf[c.head]
		return r
	}
	return nil
}

func (c *chunk) write(n int) {
	c.tail += n
}

func (c *chunk) writable() []Row {
	if c.tail < len(c.buf)-1 {
		return c.buf[c.tail:]
	}
	return nil
}

func newChunk() *chunk {
	return chunkPool.Get().(*chunk)
}

func (c *chunk) reset() {
	for i := range c.buf {
		c.buf[i] = c.buf[i][:0]
	}
	c.head, c.tail = 0, 0
}

func (c *chunk) release() {
	c.reset()
	chunkPool.Put(c)
}

var chunkPool = &sync.Pool{
	New: func() any {
		return &chunk{
			buf: make([]Row, 1<<10),
		}
	},
}

func newBufferedRowReader() *bufferedRowReader {
	return bufferedRowReaderPool.Get().(*bufferedRowReader)
}

var bufferedRowReaderPool = &sync.Pool{
	New: func() any {
		return &bufferedRowReader{
			discard: make([]*chunk, 0, 32),
		}
	},
}

func (r *bufferedRowReader) head() Row {
	return r.buf.pop()
}

func (r *bufferedRowReader) peek() Row {
	return r.buf.peek()
}

func (r *bufferedRowReader) next() error {
	if r.buf == nil || !r.buf.canPop() {
		return r.read()
	}
	return nil
}

func (r *bufferedRowReader) fill() error {
	if r.rows == nil {
		return io.EOF
	}
	if r.buf == nil {
		r.buf = newChunk()
	} else {
		r.buf.reset()
	}
	w := r.buf.writable()
	n, err := r.rows.ReadRows(w)
	if err != nil && n == 0 {
		return err
	}
	r.buf.write(n)
	return nil
}

func (r *bufferedRowReader) read() error {
	if r.rows == nil {
		return io.EOF
	}
	if r.buf == nil {
		r.buf = newChunk()
	}
	if r.buf.canPop() {
		// There is still data in the buffer
		return nil
	}
	w := r.buf.writable()
	if len(w) == 0 {
		r.discard = append(r.discard, r.buf)
		r.buf = newChunk()
		w = r.buf.writable()
	}
	n, err := r.rows.ReadRows(w)
	if err != nil && n == 0 {
		return err
	}
	r.buf.write(n)
	return nil
}

func (r *bufferedRowReader) clear() {
	for i := range r.discard {
		r.discard[i].release()
		r.discard[i] = nil
	}
	r.discard = r.discard[:0]
}

func (r *bufferedRowReader) close() {
	r.rows = nil
	r.clear()
	if r.buf != nil {
		r.buf.release()
		r.buf = nil
	}
	bufferedRowReaderPool.Put(r)
}

var (
	_ RowReaderWithSchema = (*mergedRowGroupRows)(nil)
	_ RowWriterTo         = (*mergedRowGroupRows)(nil)
)
