//go:build !purego && arm64

package parquet

//go:noescape
func memsetValuesNEON(values []Value, model Value, _ uint64)

func memsetValues(values []Value, model Value) {
	if len(values) >= 2 {
		memsetValuesNEON(values, model, 0)
	} else {
		for i := range values {
			values[i] = model
		}
	}
}