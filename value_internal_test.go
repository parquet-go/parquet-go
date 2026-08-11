package parquet

import "testing"

func TestMemsetValues(t *testing.T) {
	model := makeValueBytes(ByteArray, []byte("0123456789"))
	model.columnIndex = ^uint16(7)
	model.definitionLevel = 3
	model.repetitionLevel = 1

	for _, n := range []int{0, 1, 3, 4, 5, 63, 64, 65} {
		values := make([]Value, n)
		memsetValues(values, model)
		for i := range values {
			if values[i] != model {
				t.Fatalf("len=%d: values[%d] = %+v, want %+v", n, i, values[i], model)
			}
		}
	}
}

func BenchmarkMemsetValues(b *testing.B) {
	model := makeValueBytes(ByteArray, []byte("0123456789"))
	values := make([]Value, 1024)
	b.SetBytes(int64(len(values) * 24))
	for b.Loop() {
		memsetValues(values, model)
	}
}
