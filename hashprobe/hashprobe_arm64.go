//go:build !purego && arm64

package hashprobe

import (
	"github.com/parquet-go/parquet-go/sparse"
)

//go:noescape
func multiProbe32NEON(table []table32Group, numKeys int, hashes []uintptr, keys sparse.Uint32Array, values []int32) int

//go:noescape
func multiProbe64NEON(table []table64Group, numKeys int, hashes []uintptr, keys sparse.Uint64Array, values []int32) int

//go:noescape
func multiProbe128NEON(table []byte, tableCap, tableLen int, hashes []uintptr, keys sparse.Uint128Array, values []int32) int

func multiProbe32(table []table32Group, numKeys int, hashes []uintptr, keys sparse.Uint32Array, values []int32) int {
	// Use NEON implementation on ARM64
	return multiProbe32NEON(table, numKeys, hashes, keys, values)
}

func multiProbe64(table []table64Group, numKeys int, hashes []uintptr, keys sparse.Uint64Array, values []int32) int {
	// Use NEON implementation on ARM64
	return multiProbe64NEON(table, numKeys, hashes, keys, values)
}

func multiProbe128(table []byte, tableCap, tableLen int, hashes []uintptr, keys sparse.Uint128Array, values []int32) int {
	// Use NEON implementation on ARM64
	return multiProbe128NEON(table, tableCap, tableLen, hashes, keys, values)
}