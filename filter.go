package parquet

import "io"

// FilterRowReader constructs a RowReader which exposes rows from reader for
// which the predicate has returned true.
func FilterRowReader(reader RowReader, predicate func(Row) bool) RowReader {
	f := &filterRowReader{reader: reader, predicate: predicate}
	for i := range f.rows {
		f.rows[i] = f.values[i : i : i+1]
	}
	return f
}

type filterRowReader struct {
	reader    RowReader
	predicate func(Row) bool
	rows      [defaultRowBufferSize]Row
	values    [defaultRowBufferSize]Value
}

func (f *filterRowReader) ReadRows(rows []Row) (n int, err error) {
	for n < len(rows) {
		r := min(len(rows)-n, len(f.rows))

		r, err = f.reader.ReadRows(f.rows[:r])

		for i := range r {
			if f.predicate(f.rows[i]) {
				rows[n] = append(rows[n][:0], f.rows[i]...)
				n++
			}
		}

		if err != nil {
			break
		}
	}
	return n, err
}

// FilterRowWriter constructs a RowWriter which writes rows to writer for which
// the predicate has returned true.
//
// WriteRows returns how many input rows were consumed, not how many were
// passed to writer. Rows the predicate rejects are consumed: they produce no
// downstream write. When writer accepts only a prefix of the filtered rows,
// the consumed count runs through the last accepted row and any rejected rows
// before the next filtered row that was not written. When writer accepts every
// filtered row, the whole input is consumed, including a trailing run of
// rejected rows.
//
// Errors from writer are returned as-is. A short write with a nil error is
// returned as io.ErrShortWrite, matching MultiRowWriter.
func FilterRowWriter(writer RowWriter, predicate func(Row) bool) RowWriter {
	return &filterRowWriter{writer: writer, predicate: predicate}
}

type filterRowWriter struct {
	writer    RowWriter
	predicate func(Row) bool
	rows      [defaultRowBufferSize]Row
}

func (f *filterRowWriter) WriteRows(rows []Row) (n int, err error) {
	defer clear(f.rows[:])

	// Window index of each accepted row. The success path only records these;
	// they are read when a downstream write is short.
	var offset [defaultRowBufferSize]int

	for n < len(rows) {
		i := 0
		j := min(len(rows)-n, len(f.rows))

		for k, row := range rows[n : n+j] {
			if f.predicate(row) {
				f.rows[i] = row
				offset[i] = k
				i++
			}
		}

		if i > 0 {
			wn, werr := f.writer.WriteRows(f.rows[:i])
			if werr != nil || wn < i {
				if werr == nil {
					werr = io.ErrShortWrite
				}
				// Keep the index inside offset[:i]. A count past i means every
				// accepted row was written.
				if wn < 0 {
					wn = 0
				} else if wn > i {
					wn = i
				}
				n += consumedInputRows(j, offset[:i], wn)
				return n, werr
			}
		}

		n += j
	}

	return n, err
}

// consumedInputRows maps a downstream write of wn filtered rows onto the
// number of leading input rows consumed from a window of size window.
// match is the window index of each row the predicate accepted, in order,
// and wn is in [0, len(match)].
//
// The consumed prefix stops at the next accepted row that was not written, so
// it includes the last row the downstream writer actually wrote and any
// rejected rows before the following match. Those rejected rows are consumed:
// the filter has already dropped them, and the next match is the first input
// row that still has to be written. When every accepted row was written, the
// whole window is consumed, including rejected rows after the last match.
func consumedInputRows(window int, match []int, wn int) int {
	if wn >= len(match) {
		return window
	}
	return match[wn]
}
