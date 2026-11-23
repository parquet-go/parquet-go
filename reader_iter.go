package parquet

import (
	"errors"
	"io"
	"iter"
)

// genericReaderIterBatch is the number of rows Iter pulls per Read call.
// Callers still receive one row at a time. 128 amortizes the fixed cost of
// Read without parking a long tail when the caller breaks out of the loop.
const genericReaderIterBatch = 128

// errIterRowsBuffered is returned by ReadRows while Iter has buffered rows
// that Read and Iter have not consumed yet.
var errIterRowsBuffered = errors.New("parquet: GenericReader.Iter has buffered rows; consume them with Read or Iter before ReadRows")

// iterHold is the tail of a batch Iter pulled and then stopped before yielding.
type iterHold[T any] struct {
	tail []T
	err  error
}

// Iter returns an iterator over the remaining rows.
//
// Each step yields one row. Rows are read in batches through Read and the
// batch buffer is reused across those reads. The yielded value does not alias
// that buffer or the reader's page buffers. It remains valid after later
// iterations and after a later Read.
//
// io.EOF ends the sequence and is not yielded. Any other error is yielded
// once, paired with the zero T, after every row that was read successfully.
// The sequence stops at that error.
//
// Breaking out of the loop does not close the reader and does not start a
// goroutine. Rows already pulled into the current batch and not yet yielded
// stay buffered on the reader. A later Read or Iter returns those rows before
// reading further. SeekToRow drops the buffer only after the seek succeeds.
// Reset and Close drop it unconditionally. ReadRows does not return buffered
// rows; it returns an error until they are consumed or discarded, so a raw
// read cannot skip ahead of the rows still owed to the typed reader.
//
// The reader must not be used concurrently with Iter, including from the
// loop body.
func (r *GenericReader[T]) Iter() iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		buf := make([]T, genericReaderIterBatch)
		for {
			n, err := r.Read(buf)
			for i := range n {
				if !yield(buf[i], nil) {
					r.holdIter(buf[i+1:n], err)
					clear(buf[:n])
					return
				}
			}
			clear(buf[:n])
			if err != nil {
				if errors.Is(err, io.EOF) {
					return
				}
				var zero T
				yield(zero, err)
				return
			}
			if n == 0 {
				var zero T
				yield(zero, io.ErrNoProgress)
				return
			}
		}
	}
}

// holdIter parks the unread suffix of a batch. Read has already drained any
// previous hold into that batch, so replacing iterHold does not drop rows.
func (r *GenericReader[T]) holdIter(unread []T, err error) {
	if len(unread) == 0 && (err == nil || errors.Is(err, io.EOF)) {
		return
	}
	tail := make([]T, len(unread))
	copy(tail, unread)
	r.iterHold = &iterHold[T]{tail: tail, err: err}
}

// readWithHold returns buffered rows before reading further.
// done reports that rows already contains the full result.
func (r *GenericReader[T]) readWithHold(rows []T) (int, error) {
	n, err, done := r.takeIterHold(rows)
	if done {
		return n, err
	}
	m, err := r.read(r, rows[n:])
	return n + m, err
}

// takeIterHold copies buffered rows into rows.
// done is false only when the buffer was fully consumed, it carried no
// error, and rows still has room for a further read.
func (r *GenericReader[T]) takeIterHold(rows []T) (n int, err error, done bool) {
	hold := r.iterHold
	n = copy(rows, hold.tail)
	if n < len(hold.tail) {
		clear(hold.tail[:n])
		hold.tail = hold.tail[n:]
		return n, nil, true
	}
	err = hold.err
	clear(hold.tail)
	r.iterHold = nil
	if err != nil || n == len(rows) {
		return n, err, true
	}
	return n, nil, false
}
