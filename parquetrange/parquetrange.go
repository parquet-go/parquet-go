package parquetrange

import (
	"errors"
	"io"
	"iter"

	"github.com/parquet-go/parquet-go"
)

type IterConfig struct {
	ReuseRows bool // Whether to reuse the same slice for each row read
	ChunkSize int  // Number of rows to read at a time
}

func GenericRows[T any](reader *parquet.GenericReader[T], config IterConfig) iter.Seq2[[]T, error] {
	return func(yield func([]T, error) bool) {
		var rows []T
		if config.ReuseRows {
			rows = make([]T, config.ChunkSize)
		}

		var done bool
		var i int
		for !done {
			if !config.ReuseRows {
				rows = make([]T, config.ChunkSize)
			}

			rowsRead, err := reader.Read(rows)
			switch {
			case err == nil:

			// the parquet library returns an io.EOF error when it reaches the end of the file, so this is expected
			case errors.Is(err, io.EOF):
				// setting done to true so that we exit the loop after this iteration. We can't break here, as we still need
				// to validate the last chunk of rows
				done = true
				// Read() doesn't change the slice length, so we need to slice it to the number of rows read, as any rows
				// after that will still have the data from the previous iteration
				rows = rows[:rowsRead]

			default:
				yield(nil, err)
				return
			}

			for j, parquetRow := range rows {
				rows[j] = parquetRow
				i++
			}

			if !yield(rows, nil) {
				return
			}
		}
	}
}

func Flatten[T any, S ~[]T](seq iter.Seq2[S, error]) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		for vs, err := range seq {
			if err != nil {
				var zero T
				yield(zero, err)
				return
			}
			for _, v := range vs {
				if !yield(v, nil) {
					return
				}
			}
		}
	}
}
