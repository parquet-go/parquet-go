//go:build purego && goexperiment.simd && go1.27

package delta

func decodeByteArrayLengths(offsets []uint32, lengths []int32) (uint32, int32) {
	lastOffset := uint32(0)
	for i, n := range lengths {
		if n < 0 {
			return lastOffset, n
		}
		offsets[i] = lastOffset
		lastOffset += uint32(n)
	}
	offsets[len(lengths)] = lastOffset
	return lastOffset, 0
}
