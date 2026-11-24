package parquet

import (
	"errors"
	"io"
	"slices"
	"testing"
)

func TestFilterRowWriterReleasesRows(t *testing.T) {
	for name, writeErr := range map[string]error{"success": nil, "error": io.ErrClosedPipe} {
		t.Run(name, func(t *testing.T) {
			writer := FilterRowWriter(RowWriterFunc(func(rows []Row) (int, error) {
				return len(rows), writeErr
			}), func(Row) bool { return true }).(*filterRowWriter)

			rows := make([]Row, defaultRowBufferSize+1)
			for i := range rows {
				rows[i] = Row{Int64Value(int64(i))}
			}
			for _, batch := range [][]Row{rows, rows[:1], rows[:0]} {
				writer.WriteRows(batch)
				for i, row := range writer.rows {
					if row != nil {
						t.Fatalf("staging row %d retained after WriteRows", i)
					}
				}
			}
		})
	}
}

// BenchmarkFilterRowWriter measures the copy path: every input row matches and
// is forwarded to the downstream writer.
func BenchmarkFilterRowWriter(b *testing.B) {
	const rowsPerBatch = defaultRowBufferSize * 16
	rows := make([]Row, rowsPerBatch)
	for i := range rows {
		rows[i] = Row{Int64Value(int64(i))}
	}
	writer := FilterRowWriter(RowWriterFunc(func(in []Row) (int, error) {
		return len(in), nil
	}), func(Row) bool { return true })

	b.ReportAllocs()
	for b.Loop() {
		n, err := writer.WriteRows(rows)
		if err != nil || n != len(rows) {
			b.Fatalf("WriteRows() = %d, %v", n, err)
		}
	}
}

func TestFilterRowWriterWriteRows(t *testing.T) {
	keepAll := func(Row) bool { return true }
	keep := func(accept func(int) bool) func(Row) bool {
		return func(row Row) bool { return accept(int(row[0].Int64())) }
	}

	tests := []struct {
		name      string
		rows      int
		predicate func(Row) bool
		write     func(call int, rows []Row) (int, error)
		wantN     int
		wantErr   error
		wantCalls int
		wantBatch []int
	}{
		{
			name:      "closed pipe",
			rows:      3,
			predicate: keepAll,
			write: func(int, []Row) (int, error) {
				return 0, io.ErrClosedPipe
			},
			wantN:     0,
			wantErr:   io.ErrClosedPipe,
			wantCalls: 1,
			wantBatch: []int{3},
		},
		{
			// 47 input rows. The first 42-row window is accepted; the second
			// window returns a one-row short write.
			name:      "second window short write",
			rows:      defaultRowBufferSize + 5,
			predicate: keepAll,
			write: func(call int, rows []Row) (int, error) {
				if call == 0 {
					return len(rows), nil
				}
				return 1, io.ErrShortWrite
			},
			wantN:     defaultRowBufferSize + 1,
			wantErr:   io.ErrShortWrite,
			wantCalls: 2,
			wantBatch: []int{defaultRowBufferSize, 5},
		},
		{
			name:      "short write without error",
			rows:      3,
			predicate: keepAll,
			write: func(int, []Row) (int, error) {
				return 1, nil
			},
			wantN:     1,
			wantErr:   io.ErrShortWrite,
			wantCalls: 1,
			wantBatch: []int{3},
		},
		{
			name:      "zero write without error",
			rows:      3,
			predicate: keepAll,
			write: func(int, []Row) (int, error) {
				return 0, nil
			},
			wantN:     0,
			wantErr:   io.ErrShortWrite,
			wantCalls: 1,
			wantBatch: []int{3},
		},
		{
			// Indexes 0 and 2 are rejected, 1 and 3 are accepted, 4 is rejected.
			// One accepted row is written, so rows 0..2 are consumed and the
			// match at index 3 is not.
			name:      "short write among filtered rows",
			rows:      5,
			predicate: keep(func(i int) bool { return i%2 == 1 }),
			write: func(int, []Row) (int, error) {
				return 1, nil
			},
			wantN:     3,
			wantErr:   io.ErrShortWrite,
			wantCalls: 1,
			wantBatch: []int{2},
		},
		{
			// Two leading rejected rows, then a downstream write of nothing.
			name:      "closed pipe after filtered rows",
			rows:      4,
			predicate: keep(func(i int) bool { return i >= 2 }),
			write: func(int, []Row) (int, error) {
				return 0, io.ErrClosedPipe
			},
			wantN:     2,
			wantErr:   io.ErrClosedPipe,
			wantCalls: 1,
			wantBatch: []int{2},
		},
		{
			// Every accepted row is written and the downstream returns an
			// error. Trailing rejected rows are consumed with the window.
			name:      "error after every filtered row",
			rows:      5,
			predicate: keep(func(i int) bool { return i == 0 || i == 2 }),
			write: func(_ int, rows []Row) (int, error) {
				return len(rows), io.ErrClosedPipe
			},
			wantN:     5,
			wantErr:   io.ErrClosedPipe,
			wantCalls: 1,
			wantBatch: []int{2},
		},
		{
			name:      "stop after the failing window",
			rows:      defaultRowBufferSize*2 + 1,
			predicate: keepAll,
			write: func(call int, rows []Row) (int, error) {
				if call == 1 {
					return 1, io.ErrShortWrite
				}
				return len(rows), nil
			},
			wantN:     defaultRowBufferSize + 1,
			wantErr:   io.ErrShortWrite,
			wantCalls: 2,
			wantBatch: []int{defaultRowBufferSize, defaultRowBufferSize},
		},
		{
			// The first window is entirely rejected. The second window is
			// skip, keep, skip, keep, skip, and one keep is written.
			name: "filtered short write in the second window",
			rows: defaultRowBufferSize + 5,
			predicate: keep(func(i int) bool {
				if i < defaultRowBufferSize {
					return false
				}
				return (i-defaultRowBufferSize)%2 == 1
			}),
			write: func(int, []Row) (int, error) {
				return 1, nil
			},
			wantN:     defaultRowBufferSize + 3,
			wantErr:   io.ErrShortWrite,
			wantCalls: 1,
			wantBatch: []int{2},
		},
		{
			name:      "all filtered",
			rows:      defaultRowBufferSize + 5,
			predicate: func(Row) bool { return false },
			write: func(int, []Row) (int, error) {
				return 0, io.ErrClosedPipe
			},
			wantN:     defaultRowBufferSize + 5,
			wantErr:   nil,
			wantCalls: 0,
		},
		{
			name:      "accepts every row",
			rows:      defaultRowBufferSize + 5,
			predicate: keepAll,
			write: func(_ int, rows []Row) (int, error) {
				return len(rows), nil
			},
			wantN:     defaultRowBufferSize + 5,
			wantErr:   nil,
			wantCalls: 2,
			wantBatch: []int{defaultRowBufferSize, 5},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rows := make([]Row, test.rows)
			for i := range rows {
				rows[i] = Row{Int64Value(int64(i)), Int64Value(1)}
			}

			var batches []int
			writer := FilterRowWriter(RowWriterFunc(func(in []Row) (int, error) {
				call := len(batches)
				batches = append(batches, len(in))
				return test.write(call, in)
			}), test.predicate)

			n, err := writer.WriteRows(rows)
			if n != test.wantN || !errors.Is(err, test.wantErr) {
				t.Fatalf("WriteRows() = %d, %v, want %d, %v", n, err, test.wantN, test.wantErr)
			}
			if len(batches) != test.wantCalls {
				t.Fatalf("downstream calls = %d (%v), want %d", len(batches), batches, test.wantCalls)
			}
			if test.wantBatch != nil && !slices.Equal(batches, test.wantBatch) {
				t.Fatalf("downstream batch sizes = %v, want %v", batches, test.wantBatch)
			}
			for i, row := range rows {
				if len(row) != 2 || row[0].Int64() != int64(i) || row[1].Int64() != 1 {
					t.Fatalf("WriteRows modified row %d: %v", i, row)
				}
			}
			for i, row := range writer.(*filterRowWriter).rows {
				if row != nil {
					t.Fatalf("staging row %d retained after WriteRows", i)
				}
			}
		})
	}
}
