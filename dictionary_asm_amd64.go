//go:build !purego && (!goexperiment.simd || go1.27)

package parquet

//go:noescape
func dictionaryBoundsBE128(dict [][16]byte, indexes []int32) (min, max *[16]byte, err errno)
