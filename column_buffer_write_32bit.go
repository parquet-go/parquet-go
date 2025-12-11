//go:build 386 || arm || mips || mipsle

package parquet

import (
	"reflect"

	"github.com/parquet-go/parquet-go/sparse"
)

// makeWriteRowsFuncForNativeInt creates a write function for native int/uint types.
// On 32-bit architectures, native int/uint are 32-bit and match the column size,
// so no special conversion is needed.
func makeWriteRowsFuncForNativeInt(t reflect.Type, columnIndex int16, columnType Type) writeRowsFunc {
	return func(columns []ColumnBuffer, levels columnLevels, rows sparse.Array) {
		columns[columnIndex].writeValues(levels, rows)
	}
}
