package parquet_test

import (
	"fmt"
	"slices"
	"testing"

	"github.com/parquet-go/parquet-go"
)

func TestFilterRowReader(t *testing.T) {
	rows := []parquet.Row{
		{parquet.Int64Value(0)},
		{parquet.Int64Value(1)},
		{parquet.Int64Value(2)},
		{parquet.Int64Value(3)},
		{parquet.Int64Value(4)},
	}

	want := []parquet.Row{
		{parquet.Int64Value(0)},
		{parquet.Int64Value(2)},
		{parquet.Int64Value(4)},
	}

	reader := parquet.FilterRowReader(&bufferedRows{rows: rows},
		func(row parquet.Row) bool {
			return row[0].Int64()%2 == 0
		},
	)

	writer := &bufferedRows{}
	_, err := parquet.CopyRows(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	assertEqualRows(t, want, writer.rows)
}

func TestFilterRowWriter(t *testing.T) {
	rows := []parquet.Row{
		{parquet.Int64Value(0)},
		{parquet.Int64Value(1)},
		{parquet.Int64Value(2)},
		{parquet.Int64Value(3)},
		{parquet.Int64Value(4)},
	}

	want := []parquet.Row{
		{parquet.Int64Value(1)},
		{parquet.Int64Value(3)},
	}

	buffer := &bufferedRows{}
	writer := parquet.FilterRowWriter(buffer,
		func(row parquet.Row) bool {
			return row[0].Int64()%2 == 1
		},
	)

	reader := &bufferedRows{rows: rows}
	_, err := parquet.CopyRows(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	assertEqualRows(t, want, buffer.rows)
}

func TestFilterRowWriterPreservesRows(t *testing.T) {
	for name, predicate := range map[string]func(parquet.Row) bool{
		"all":  func(parquet.Row) bool { return true },
		"even": func(row parquet.Row) bool { return row[0].Int64()%2 == 0 },
		"odd":  func(row parquet.Row) bool { return row[0].Int64()%2 != 0 },
		"none": func(parquet.Row) bool { return false },
	} {
		for _, fanout := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/fanout=%t", name, fanout), func(t *testing.T) {
				filtered, unfiltered := new(bufferedRows), new(bufferedRows)
				writer := parquet.FilterRowWriter(filtered, predicate)
				if fanout {
					writer = parquet.MultiRowWriter(writer, unfiltered)
				}

				var inputs, originals []parquet.Row
				for _, size := range []int{85, 1, 0} {
					rows := make([]parquet.Row, size)
					var want []parquet.Row
					for i := range rows {
						rows[i] = parquet.Row{
							parquet.Int64Value(int64(i)).Level(0, 0, 0),
							parquet.ByteArrayValue([]byte("hello")).Level(0, 1, 1),
						}
						inputs = append(inputs, rows[i])
						originals = append(originals, rows[i].Clone())
						if predicate(rows[i]) {
							want = append(want, rows[i].Clone())
						}
					}

					filtered.rows, unfiltered.rows = nil, nil
					n, err := writer.WriteRows(rows)
					if n != len(rows) || err != nil {
						t.Fatalf("WriteRows(%d rows) = %d, %v", size, n, err)
					}
					if !slices.EqualFunc(originals, inputs, parquet.Row.Equal) {
						t.Fatalf("WriteRows(%d rows) modified input rows", size)
					}
					assertEqualRows(t, want, filtered.rows)
					if fanout {
						assertEqualRows(t, originals[len(originals)-size:], unfiltered.rows)
					}
				}
			})
		}
	}
}
