//go:build !purego && !goexperiment.simd

package parquet

import "github.com/parquet-go/parquet-go/sparse"

//go:noescape
func nullIndex8(bits *uint64, rows sparse.Array)

//go:noescape
func nullIndex128(bits *uint64, rows sparse.Array)
