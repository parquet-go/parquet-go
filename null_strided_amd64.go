//go:build !purego && (!goexperiment.simd || go1.27)

package parquet

import "github.com/parquet-go/parquet-go/sparse"

func nullIndexString(bits []uint64, rows sparse.Array) {
	// Offset past the data pointer so the length field is what gets tested.
	// String headers are 16 bytes apart, so this is a strided gather. The
	// Go 1.27 simd nullIndex64 keeps its vector path for dense 8-byte rows
	// and would otherwise probe the stride on every call.
	nullIndexGather64(&bits[0], rows.Offset(8))
}

func nullIndexSlice(bits []uint64, rows sparse.Array) {
	// A slice is null when its data pointer, the first word, is nil.
	// Slice headers are 24 bytes apart.
	nullIndexGather64(&bits[0], rows)
}
