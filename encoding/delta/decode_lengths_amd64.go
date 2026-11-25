//go:build !purego && goexperiment.simd && go1.27

package delta

//go:noescape
func decodeByteArrayLengths(offsets []uint32, lengths []int32) (lastOffset uint32, invalidLength int32)
