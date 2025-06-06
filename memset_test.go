package parquet

import (
	"testing"
	"unsafe"
)

func BenchmarkMemsetValues(b *testing.B) {
	const N = 1024
	values := make([]Value, N)
	model := ValueOf(42)

	for i := 0; i < b.N; i++ {
		memsetValues(values, model)
	}

	b.SetBytes(N * int64(unsafe.Sizeof(Value{})))
}

func BenchmarkMemsetValuesLarge(b *testing.B) {
	const N = 8192  // Larger size to amortize overhead
	values := make([]Value, N)
	model := ValueOf(42)

	for i := 0; i < b.N; i++ {
		memsetValues(values, model)
	}

	b.SetBytes(N * int64(unsafe.Sizeof(Value{})))
}