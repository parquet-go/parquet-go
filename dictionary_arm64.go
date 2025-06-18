//go:build !purego && arm64

package parquet

import (
	"unsafe"

	"github.com/parquet-go/parquet-go/internal/unsafecast"
	"github.com/parquet-go/parquet-go/sparse"
)

const errnoIndexOutOfBounds errno = 1

//go:noescape
func dictionaryBoundsInt32(dict []int32, indexes []int32) (min, max int32, err errno)

//go:noescape
func dictionaryBoundsUint32(dict []uint32, indexes []int32) (min, max uint32, err errno)

// Fallback implementations for other bounds functions (using Go)
func dictionaryBoundsInt64(dict []int64, indexes []int32) (min, max int64, err errno) {
	if len(indexes) == 0 {
		return 0, 0, 0
	}
	
	// Bounds check first index
	if int(indexes[0]) >= len(dict) {
		return 0, 0, errnoIndexOutOfBounds
	}
	
	min = dict[indexes[0]]
	max = min

	for _, i := range indexes[1:] {
		if int(i) >= len(dict) {
			return 0, 0, errnoIndexOutOfBounds
		}
		value := dict[i]
		if value < min {
			min = value
		}
		if value > max {
			max = value
		}
	}

	return min, max, 0
}

func dictionaryBoundsFloat32(dict []float32, indexes []int32) (min, max float32, err errno) {
	if len(indexes) == 0 {
		return 0, 0, 0
	}
	
	// Bounds check first index
	if int(indexes[0]) >= len(dict) {
		return 0, 0, errnoIndexOutOfBounds
	}
	
	min = dict[indexes[0]]
	max = min

	for _, i := range indexes[1:] {
		if int(i) >= len(dict) {
			return 0, 0, errnoIndexOutOfBounds
		}
		value := dict[i]
		if value < min {
			min = value
		}
		if value > max {
			max = value
		}
	}

	return min, max, 0
}

func dictionaryBoundsFloat64(dict []float64, indexes []int32) (min, max float64, err errno) {
	if len(indexes) == 0 {
		return 0, 0, 0
	}
	
	// Bounds check first index
	if int(indexes[0]) >= len(dict) {
		return 0, 0, errnoIndexOutOfBounds
	}
	
	min = dict[indexes[0]]
	max = min

	for _, i := range indexes[1:] {
		if int(i) >= len(dict) {
			return 0, 0, errnoIndexOutOfBounds
		}
		value := dict[i]
		if value < min {
			min = value
		}
		if value > max {
			max = value
		}
	}

	return min, max, 0
}

func dictionaryBoundsUint64(dict []uint64, indexes []int32) (min, max uint64, err errno) {
	if len(indexes) == 0 {
		return 0, 0, 0
	}
	
	// Bounds check first index
	if int(indexes[0]) >= len(dict) {
		return 0, 0, errnoIndexOutOfBounds
	}
	
	min = dict[indexes[0]]
	max = min

	for _, i := range indexes[1:] {
		if int(i) >= len(dict) {
			return 0, 0, errnoIndexOutOfBounds
		}
		value := dict[i]
		if value < min {
			min = value
		}
		if value > max {
			max = value
		}
	}

	return min, max, 0
}

func dictionaryBoundsBE128(dict [][16]byte, indexes []int32) (min, max *[16]byte, err errno) {
	if len(indexes) == 0 {
		return nil, nil, 0
	}
	
	// Bounds check first index
	if int(indexes[0]) >= len(dict) {
		return nil, nil, errnoIndexOutOfBounds
	}
	
	min = &dict[indexes[0]]
	max = min

	for _, i := range indexes[1:] {
		if int(i) >= len(dict) {
			return nil, nil, errnoIndexOutOfBounds
		}
		
		value := &dict[i]
		
		// Use proper big-endian comparison
		if lessBE128(value, min) {
			min = value
		}
		if lessBE128(max, value) {
			max = value
		}
	}

	return min, max, 0
}

// Fallback implementations for lookup functions
func dictionaryLookup32(dict []uint32, indexes []int32, rows sparse.Array) errno {
	for i, j := range indexes {
		if int(j) >= len(dict) {
			return errnoIndexOutOfBounds
		}
		*(*uint32)(rows.Index(i)) = dict[j]
	}
	return 0
}

func dictionaryLookup64(dict []uint64, indexes []int32, rows sparse.Array) errno {
	for i, j := range indexes {
		if int(j) >= len(dict) {
			return errnoIndexOutOfBounds
		}
		*(*uint64)(rows.Index(i)) = dict[j]
	}
	return 0
}

func dictionaryLookupByteArrayString(dict []uint32, page []byte, indexes []int32, rows sparse.Array) errno {
	for i, j := range indexes {
		if int(j) >= len(dict)-1 { // -1 because offsets have length+1 elements
			return errnoIndexOutOfBounds
		}
		
		start := dict[j]
		end := dict[j+1]
		
		if int(start) > len(page) || int(end) > len(page) || start > end {
			return errnoIndexOutOfBounds
		}
		
		// Store pointer and length for string
		ptr := unsafe.Pointer(&page[start])
		length := end - start
		
		*(*unsafe.Pointer)(rows.Index(i)) = ptr
		*(*int)(unsafe.Add(rows.Index(i), 8)) = int(length)
	}
	return 0
}

func dictionaryLookupFixedLenByteArrayString(dict []byte, length int, indexes []int32, rows sparse.Array) errno {
	for i, j := range indexes {
		offset := int(j) * length
		if offset >= len(dict) {
			return errnoIndexOutOfBounds
		}
		
		// Store pointer and length for string
		ptr := unsafe.Pointer(&dict[offset])
		
		*(*unsafe.Pointer)(rows.Index(i)) = ptr
		*(*int)(unsafe.Add(rows.Index(i), 8)) = length
	}
	return 0
}

func dictionaryLookupFixedLenByteArrayPointer(dict []byte, length int, indexes []int32, rows sparse.Array) errno {
	for i, j := range indexes {
		offset := int(j) * length
		if offset >= len(dict) {
			return errnoIndexOutOfBounds
		}
		
		// Store just the pointer
		ptr := unsafe.Pointer(&dict[offset])
		*(*unsafe.Pointer)(rows.Index(i)) = ptr
	}
	return 0
}

// Method implementations (same as AMD64 version)
func (d *int32Dictionary) lookup(indexes []int32, rows sparse.Array) {
	checkLookupIndexBounds(indexes, rows)
	dict := unsafecast.Slice[uint32](d.values)
	dictionaryLookup32(dict, indexes, rows).check()
}

func (d *int64Dictionary) lookup(indexes []int32, rows sparse.Array) {
	checkLookupIndexBounds(indexes, rows)
	dict := unsafecast.Slice[uint64](d.values)
	dictionaryLookup64(dict, indexes, rows).check()
}

func (d *floatDictionary) lookup(indexes []int32, rows sparse.Array) {
	checkLookupIndexBounds(indexes, rows)
	dict := unsafecast.Slice[uint32](d.values)
	dictionaryLookup32(dict, indexes, rows).check()
}

func (d *doubleDictionary) lookup(indexes []int32, rows sparse.Array) {
	checkLookupIndexBounds(indexes, rows)
	dict := unsafecast.Slice[uint64](d.values)
	dictionaryLookup64(dict, indexes, rows).check()
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
	dictionaryLookup32(d.values, indexes, rows).check()
}

func (d *uint64Dictionary) lookup(indexes []int32, rows sparse.Array) {
	checkLookupIndexBounds(indexes, rows)
	dictionaryLookup64(d.values, indexes, rows).check()
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
	min, max, err := dictionaryBoundsInt32(d.values, indexes)
	err.check()
	return min, max
}

func (d *int64Dictionary) bounds(indexes []int32) (min, max int64) {
	min, max, err := dictionaryBoundsInt64(d.values, indexes)
	err.check()
	return min, max
}

func (d *floatDictionary) bounds(indexes []int32) (min, max float32) {
	min, max, err := dictionaryBoundsFloat32(d.values, indexes)
	err.check()
	return min, max
}

func (d *doubleDictionary) bounds(indexes []int32) (min, max float64) {
	min, max, err := dictionaryBoundsFloat64(d.values, indexes)
	err.check()
	return min, max
}

func (d *uint32Dictionary) bounds(indexes []int32) (min, max uint32) {
	min, max, err := dictionaryBoundsUint32(d.values, indexes)
	err.check()
	return min, max
}

func (d *uint64Dictionary) bounds(indexes []int32) (min, max uint64) {
	min, max, err := dictionaryBoundsUint64(d.values, indexes)
	err.check()
	return min, max
}

func (d *be128Dictionary) bounds(indexes []int32) (min, max *[16]byte) {
	min, max, err := dictionaryBoundsBE128(d.values, indexes)
	err.check()
	return min, max
}