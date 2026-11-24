package parquet

import (
	"io"
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
