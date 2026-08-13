//go:build purego || !amd64 || goexperiment.simd

package parquet

import (
	"unsafe"

	"github.com/parquet-go/bitpack/unsafecast"
	"github.com/parquet-go/parquet-go/sparse"
)

func (d *int32Dictionary) lookup(indexes []int32, rows sparse.Array) {
	checkLookupIndexBounds(indexes, rows)
	for i, j := range indexes {
		*(*int32)(rows.Index(i)) = d.index(j)
	}
}

func (d *int64Dictionary) lookup(indexes []int32, rows sparse.Array) {
	checkLookupIndexBounds(indexes, rows)
	for i, j := range indexes {
		*(*int64)(rows.Index(i)) = d.index(j)
	}
}

func (d *floatDictionary) lookup(indexes []int32, rows sparse.Array) {
	checkLookupIndexBounds(indexes, rows)
	for i, j := range indexes {
		*(*float32)(rows.Index(i)) = d.index(j)
	}
}

func (d *doubleDictionary) lookup(indexes []int32, rows sparse.Array) {
	checkLookupIndexBounds(indexes, rows)
	for i, j := range indexes {
		*(*float64)(rows.Index(i)) = d.index(j)
	}
}

func (d *byteArrayDictionary) lookupString(indexes []int32, rows sparse.Array) {
	checkLookupIndexBounds(indexes, rows)
	for i, j := range indexes {
		*(*string)(rows.Index(i)) = unsafecast.String(d.index(int(j)))
	}
}

func (d *fixedLenByteArrayDictionary) lookupString(indexes []int32, rows sparse.Array) {
	checkLookupIndexBounds(indexes, rows)
	for i, j := range indexes {
		*(*string)(rows.Index(i)) = unsafecast.String(d.index(j))
	}
}

func (d *uint32Dictionary) lookup(indexes []int32, rows sparse.Array) {
	checkLookupIndexBounds(indexes, rows)
	for i, j := range indexes {
		*(*uint32)(rows.Index(i)) = d.index(j)
	}
}

func (d *uint64Dictionary) lookup(indexes []int32, rows sparse.Array) {
	checkLookupIndexBounds(indexes, rows)
	for i, j := range indexes {
		*(*uint64)(rows.Index(i)) = d.index(j)
	}
}

func (d *be128Dictionary) lookupString(indexes []int32, rows sparse.Array) {
	checkLookupIndexBounds(indexes, rows)
	s := "0123456789ABCDEF"
	for i, j := range indexes {
		*(**[16]byte)(unsafe.Pointer(&s)) = d.index(j)
		*(*string)(rows.Index(i)) = s
	}
}

func (d *be128Dictionary) lookupPointer(indexes []int32, rows sparse.Array) {
	checkLookupIndexBounds(indexes, rows)
	for i, j := range indexes {
		*(**[16]byte)(rows.Index(i)) = d.index(j)
	}
}

func (d *int32Dictionary) bounds(indexes []int32) (min, max int32) {
	return dictionaryBounds(d.values.Slice(), indexes)
}

func (d *int64Dictionary) bounds(indexes []int32) (min, max int64) {
	return dictionaryBounds(d.values.Slice(), indexes)
}

func (d *floatDictionary) bounds(indexes []int32) (min, max float32) {
	return dictionaryBounds(d.values.Slice(), indexes)
}

func (d *doubleDictionary) bounds(indexes []int32) (min, max float64) {
	return dictionaryBounds(d.values.Slice(), indexes)
}

func (d *uint32Dictionary) bounds(indexes []int32) (min, max uint32) {
	return dictionaryBounds(d.values.Slice(), indexes)
}

func (d *uint64Dictionary) bounds(indexes []int32) (min, max uint64) {
	return dictionaryBounds(d.values.Slice(), indexes)
}

func (d *be128Dictionary) bounds(indexes []int32) (min, max *[16]byte) {
	values := [64]*[16]byte{}
	min = d.index(indexes[0])
	max = min

	for i := 1; i < len(indexes); i += len(values) {
		n := len(indexes) - i
		if n > len(values) {
			n = len(values)
		}
		j := i + n
		d.lookupPointer(indexes[i:j:j], makeArrayFromSlice(values[:n:n]))

		for _, value := range values[:n:n] {
			switch {
			case lessBE128(value, min):
				min = value
			case lessBE128(max, value):
				max = value
			}
		}
	}

	return min, max
}

// dictionaryBounds computes the min and max values referenced by indexes
// with four independent accumulator pairs: the dictionary loads are data
// dependent, and independent accumulators let them overlap instead of
// serializing on the compare chain (about 2x on cache resident
// dictionaries).
func dictionaryBounds[T int32 | int64 | uint32 | uint64 | float32 | float64](values []T, indexes []int32) (min, max T) {
	min = values[indexes[0]]
	max = min
	i := 1
	if len(indexes) >= 9 {
		min0, min1, min2, min3 := min, min, min, min
		max0, max1, max2, max3 := min, min, min, min
		for ; i+4 <= len(indexes); i += 4 {
			v0 := values[indexes[i+0]]
			v1 := values[indexes[i+1]]
			v2 := values[indexes[i+2]]
			v3 := values[indexes[i+3]]
			if v0 < min0 {
				min0 = v0
			}
			if v0 > max0 {
				max0 = v0
			}
			if v1 < min1 {
				min1 = v1
			}
			if v1 > max1 {
				max1 = v1
			}
			if v2 < min2 {
				min2 = v2
			}
			if v2 > max2 {
				max2 = v2
			}
			if v3 < min3 {
				min3 = v3
			}
			if v3 > max3 {
				max3 = v3
			}
		}
		min, max = min0, max0
		for _, v := range [3]T{min1, min2, min3} {
			if v < min {
				min = v
			}
		}
		for _, v := range [3]T{max1, max2, max3} {
			if v > max {
				max = v
			}
		}
	}
	for ; i < len(indexes); i++ {
		v := values[indexes[i]]
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}
	return min, max
}
