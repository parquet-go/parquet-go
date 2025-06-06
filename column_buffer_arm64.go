//go:build !purego && arm64

package parquet

import (
	"github.com/parquet-go/parquet-go/internal/bytealg"
	"github.com/parquet-go/parquet-go/internal/unsafecast"
	"github.com/parquet-go/parquet-go/sparse"
)

func broadcastValueInt32(dst []int32, src int8) {
	bytealg.Broadcast(unsafecast.Slice[byte](dst), byte(src))
}

//go:noescape
func broadcastRangeInt32ARM64(dst []int32, base int32)

func broadcastRangeInt32(dst []int32, base int32) {
	if len(dst) >= 4 {
		broadcastRangeInt32ARM64(dst, base)
	} else {
		for i := range dst {
			dst[i] = base + int32(i)
		}
	}
}

//go:noescape
func writePointersBE128ARM64(values [][16]byte, rows sparse.Array)

func writePointersBE128(values [][16]byte, rows sparse.Array) {
	if len(values) > 0 {
		writePointersBE128ARM64(values, rows)
	}
}