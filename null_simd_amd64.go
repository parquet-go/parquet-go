//go:build !purego && goexperiment.simd

package parquet

import (
	"unsafe"

	"github.com/parquet-go/parquet-go/sparse"
)

// The assembly versions of these kernels are scalar loops (nullIndex128 also
// used ungated SSE4.1 PCMPEQQ); the generic Go implementation compiles to
// comparable code, so the GOEXPERIMENT=simd build uses it instead of the
// assembly. The gather based nullIndex32/64 kernels keep their assembly (no
// gather in archsimd).

func nullIndex8(bits *uint64, rows sparse.Array) {
	nullIndex[uint8](unsafe.Slice(bits, (rows.Len()+63)/64), rows)
}

func nullIndex128(bits *uint64, rows sparse.Array) {
	nullIndex[[16]byte](unsafe.Slice(bits, (rows.Len()+63)/64), rows)
}
