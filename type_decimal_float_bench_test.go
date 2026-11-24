package parquet

import (
	"math/big"
	"reflect"
	"testing"
)

func BenchmarkFixedDecimalFloat64(b *testing.B) {
	benchmarkFixedDecimalWrite(b, float64(1.25))
}

func BenchmarkFixedDecimalBigFloat(b *testing.B) {
	benchmarkFixedDecimalWrite(b, big.NewFloat(1.25))
}

func benchmarkFixedDecimalWrite(b *testing.B, value any) {
	schema := NewSchema("row", Group{
		"amount": Decimal(2, 38, FixedLenByteArrayType(16)),
	})
	cols := makeColumnBuffersForSchema(schema)
	_, write := writeValueFuncOf(0, schema)
	row := reflect.ValueOf(map[string]any{"amount": value})
	for range 64 {
		write(cols, columnLevels{}, row)
	}
	cols[0].Reset()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		write(cols, columnLevels{}, row)
		if cols[0].Len() >= 4096 {
			cols[0].Reset()
		}
	}
}
