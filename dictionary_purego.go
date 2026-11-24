//go:build purego || !amd64 || (goexperiment.simd && !go1.27)

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
	min = d.index(indexes[0])
	max = min

	for _, i := range indexes[1:] {
		value := d.index(i)
		if value < min {
			min = value
		}
		if value > max {
			max = value
		}
	}

	return min, max
}

func (d *int64Dictionary) bounds(indexes []int32) (min, max int64) {
	min = d.index(indexes[0])
	max = min

	for _, i := range indexes[1:] {
		value := d.index(i)
		if value < min {
			min = value
		}
		if value > max {
			max = value
		}
	}

	return min, max
}

func (d *floatDictionary) bounds(indexes []int32) (min, max float32) {
	values := d.values.Slice()
	min = values[indexes[0]]
	indexes = indexes[1:]
	if min != min {
		return boundsFloat32AfterNaN(values, indexes, min)
	}
	max = min
	for _, i := range indexes {
		value := values[i]
		switch {
		case value < min:
			min = value
		case value > max:
			max = value
		}
	}
	return min, max
}

//go:noinline
func boundsFloat32AfterNaN(values []float32, indexes []int32, nan float32) (min, max float32) {
	for k, i := range indexes {
		v := values[i]
		if v == v {
			min, max = v, v
			for _, j := range indexes[k+1:] {
				value := values[j]
				switch {
				case value < min:
					min = value
				case value > max:
					max = value
				}
			}
			return min, max
		}
	}
	return nan, nan
}

func (d *doubleDictionary) bounds(indexes []int32) (min, max float64) {
	values := d.values.Slice()
	min = values[indexes[0]]
	indexes = indexes[1:]
	if min != min {
		return boundsFloat64AfterNaN(values, indexes, min)
	}
	max = min
	for _, i := range indexes {
		value := values[i]
		switch {
		case value < min:
			min = value
		case value > max:
			max = value
		}
	}
	return min, max
}

//go:noinline
func boundsFloat64AfterNaN(values []float64, indexes []int32, nan float64) (min, max float64) {
	for k, i := range indexes {
		v := values[i]
		if v == v {
			min, max = v, v
			for _, j := range indexes[k+1:] {
				value := values[j]
				switch {
				case value < min:
					min = value
				case value > max:
					max = value
				}
			}
			return min, max
		}
	}
	return nan, nan
}

func (d *uint32Dictionary) bounds(indexes []int32) (min, max uint32) {
	min = d.index(indexes[0])
	max = min

	for _, i := range indexes[1:] {
		value := d.index(i)
		if value < min {
			min = value
		}
		if value > max {
			max = value
		}
	}

	return min, max
}

func (d *uint64Dictionary) bounds(indexes []int32) (min, max uint64) {
	min = d.index(indexes[0])
	max = min

	for _, i := range indexes[1:] {
		value := d.index(i)
		if value < min {
			min = value
		}
		if value > max {
			max = value
		}
	}

	return min, max
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
