//go:build amd64 || arm64 || s390x || ppc64 || ppc64le || mips64 || mips64le || riscv64 || loong64

package parquet

import (
	"reflect"

	"github.com/parquet-go/parquet-go/sparse"
)

// makeWriteRowsFuncForNativeInt creates a write function for native int/uint types.
// On 64-bit architectures, native int/uint are 64-bit, but they may be written to
// 32-bit columns when using int(8/16/32) or uint(8/16/32) tags. In these cases,
// we need element-by-element conversion to avoid endianness issues.
func makeWriteRowsFuncForNativeInt(t reflect.Type, columnIndex int16, columnType Type) writeRowsFunc {
	// Check if we need element-by-element conversion for native int/uint
	if t.Kind() == reflect.Int || t.Kind() == reflect.Uint {
		if columnType.Kind() == Int32 {
			// Native int/uint (64-bit) -> int32 column requires conversion
			return func(columns []ColumnBuffer, levels columnLevels, rows sparse.Array) {
				if buf, ok := columns[columnIndex].(*int32ColumnBuffer); ok {
					buf.writeValuesFromInt64(levels, rows)
				} else if buf, ok := columns[columnIndex].(*uint32ColumnBuffer); ok {
					buf.writeValuesFromUint64(levels, rows)
				} else {
					columns[columnIndex].writeValues(levels, rows)
				}
			}
		}
	}

	// Default fast path
	return func(columns []ColumnBuffer, levels columnLevels, rows sparse.Array) {
		columns[columnIndex].writeValues(levels, rows)
	}
}
