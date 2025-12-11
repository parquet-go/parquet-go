//go:build amd64 || arm64 || s390x || ppc64 || ppc64le || mips64 || mips64le || riscv64 || loong64

package parquet

import (
	"github.com/parquet-go/parquet-go/sparse"
)

// writeValuesFromUint64 converts uint64 values (from native uint on 64-bit architectures)
// to uint32 element-by-element. This is needed on big-endian systems where the lower
// 32 bits of a 64-bit uint are at offset +4, not offset +0.
func (col *uint32ColumnBuffer) writeValuesFromUint64(levels columnLevels, rows sparse.Array) {
	if n := len(col.values) + rows.Len(); n > cap(col.values) {
		col.values = append(make([]uint32, 0, max(n, 2*cap(col.values))), col.values...)
	}
	n := len(col.values)
	col.values = col.values[:n+rows.Len()]
	uint64Arr := rows.Uint64Array()
	for i := range rows.Len() {
		col.values[n+i] = uint32(uint64Arr.Index(i))
	}
}
