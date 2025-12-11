//go:build amd64 || arm64 || s390x || ppc64 || ppc64le || mips64 || mips64le || riscv64 || loong64

package parquet

import (
	"github.com/parquet-go/parquet-go/sparse"
)

// writeValuesFromInt64 converts int64 values (from native int on 64-bit architectures)
// to int32 element-by-element. This is needed on big-endian systems where the lower
// 32 bits of a 64-bit int are at offset +4, not offset +0.
func (col *int32ColumnBuffer) writeValuesFromInt64(levels columnLevels, rows sparse.Array) {
	if n := len(col.values) + rows.Len(); n > cap(col.values) {
		col.values = append(make([]int32, 0, max(n, 2*cap(col.values))), col.values...)
	}
	n := len(col.values)
	col.values = col.values[:n+rows.Len()]
	int64Arr := rows.Int64Array()
	for i := range rows.Len() {
		col.values[n+i] = int32(int64Arr.Index(i))
	}
}
