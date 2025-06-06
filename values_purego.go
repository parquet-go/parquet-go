//go:build purego || (!amd64 && !arm64)

package parquet

func memsetValues(values []Value, model Value) {
	for i := range values {
		values[i] = model
	}
}
