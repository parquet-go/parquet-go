//go:build !purego && arm64

package parquet

// The min-max algorithms combine looking for the min and max values in a single
// pass over the data. This ARM64 implementation uses NEON vectorization to
// process multiple elements simultaneously while maintaining the same benefits
// as the AMD64 version.

const combinedBoundsThreshold = 1 * 1024 * 1024

//go:noescape
func combinedBoundsInt32ARM64(data []int32) (min, max int32)

//go:noescape
func combinedBoundsInt64ARM64(data []int64) (min, max int64)

//go:noescape
func combinedBoundsUint32ARM64(data []uint32) (min, max uint32)

//go:noescape
func combinedBoundsUint64ARM64(data []uint64) (min, max uint64)

//go:noescape
func combinedBoundsFloat32ARM64(data []float32) (min, max float32)

//go:noescape
func combinedBoundsFloat64ARM64(data []float64) (min, max float64)

func combinedBoundsBool(data []bool) (min, max bool) {
	// For bool arrays, use generic implementation since it's simple
	if len(data) == 0 {
		return false, false
	}
	min, max = data[0], data[0]
	for _, v := range data[1:] {
		if !v && min {
			min = v  // false < true
		}
		if v && !max {
			max = v  // true > false
		}
	}
	return
}

func combinedBoundsInt32(data []int32) (min, max int32) {
	return combinedBoundsInt32ARM64(data)
}

func combinedBoundsInt64(data []int64) (min, max int64) {
	return combinedBoundsInt64ARM64(data)
}

func combinedBoundsUint32(data []uint32) (min, max uint32) {
	return combinedBoundsUint32ARM64(data)
}

func combinedBoundsUint64(data []uint64) (min, max uint64) {
	return combinedBoundsUint64ARM64(data)
}

func combinedBoundsFloat32(data []float32) (min, max float32) {
	return combinedBoundsFloat32ARM64(data)
}

func combinedBoundsFloat64(data []float64) (min, max float64) {
	return combinedBoundsFloat64ARM64(data)
}

func combinedBoundsBE128(data [][16]byte) (min, max []byte) {
	// TODO: 128-bit comparison is complex to vectorize efficiently
	// Use individual min/max functions for now
	min = minBE128(data)
	max = maxBE128(data)
	return
}

func boundsInt32(data []int32) (min, max int32) {
	if 4*len(data) >= combinedBoundsThreshold {
		return combinedBoundsInt32(data)
	}
	min = minInt32(data)
	max = maxInt32(data)
	return
}

func boundsInt64(data []int64) (min, max int64) {
	if 8*len(data) >= combinedBoundsThreshold {
		return combinedBoundsInt64(data)
	}
	min = minInt64(data)
	max = maxInt64(data)
	return
}

func boundsUint32(data []uint32) (min, max uint32) {
	if 4*len(data) >= combinedBoundsThreshold {
		return combinedBoundsUint32(data)
	}
	min = minUint32(data)
	max = maxUint32(data)
	return
}

func boundsUint64(data []uint64) (min, max uint64) {
	if 8*len(data) >= combinedBoundsThreshold {
		return combinedBoundsUint64(data)
	}
	min = minUint64(data)
	max = maxUint64(data)
	return
}

func boundsFloat32(data []float32) (min, max float32) {
	if 4*len(data) >= combinedBoundsThreshold {
		return combinedBoundsFloat32(data)
	}
	min = minFloat32(data)
	max = maxFloat32(data)
	return
}

func boundsFloat64(data []float64) (min, max float64) {
	if 8*len(data) >= combinedBoundsThreshold {
		return combinedBoundsFloat64(data)
	}
	min = minFloat64(data)
	max = maxFloat64(data)
	return
}

func boundsBE128(data [][16]byte) (min, max []byte) {
	// TODO: min/max BE128 is really complex to vectorize, and the returns
	// were barely better than doing the min and max independently, for all
	// input sizes. We should revisit if we find ways to improve the min or
	// max algorithms which can be transposed to the combined version.
	min = minBE128(data)
	max = maxBE128(data)
	return
}