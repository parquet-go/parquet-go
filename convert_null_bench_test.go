package parquet_test

import (
	"testing"

	"github.com/parquet-go/parquet-go"
)

func BenchmarkConvertOptionalInt32ToInt64(b *testing.B) {
	benchmarkConvertInt32ToInt64(b, true)
}

func BenchmarkConvertRequiredInt32ToInt64(b *testing.B) {
	benchmarkConvertInt32ToInt64(b, false)
}

func benchmarkConvertInt32ToInt64(b *testing.B, optional bool) {
	b.Helper()
	fromNode := parquet.Leaf(parquet.Int32Type)
	toNode := parquet.Leaf(parquet.Int64Type)
	repetition := 0
	definition := 0
	if optional {
		fromNode = parquet.Repeated(fromNode)
		toNode = parquet.Repeated(toNode)
		definition = 1
	}
	from := parquet.NewSchema("s", parquet.Group{"a": fromNode})
	to := parquet.NewSchema("s", parquet.Group{"a": toNode})
	conv, err := parquet.Convert(to, from)
	if err != nil {
		b.Fatal(err)
	}

	const n = 4096
	row := make(parquet.Row, n)
	for i := range row {
		repetition = 0
		if optional && i > 0 {
			repetition = 1
		}
		row[i] = parquet.Int32Value(int32(i+1)).Level(repetition, definition, 0)
	}
	rows := []parquet.Row{row}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := conv.Convert(rows); err != nil {
			b.Fatal(err)
		}
	}
}
