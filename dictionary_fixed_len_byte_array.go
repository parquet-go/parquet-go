package parquet

import (
	"reflect"
	"unsafe"

	"github.com/parquet-go/bitpack/unsafecast"
	"github.com/parquet-go/parquet-go/encoding"
	"github.com/parquet-go/parquet-go/sparse"
)

type fixedLenByteArrayDictionary struct {
	fixedLenByteArrayPage
	hashmap map[string]int32
}

func newFixedLenByteArrayDictionary(typ Type, columnIndex int16, numValues int32, values encoding.Values) *fixedLenByteArrayDictionary {
	data, size := values.FixedLenByteArray()
	return &fixedLenByteArrayDictionary{
		fixedLenByteArrayPage: fixedLenByteArrayPage{
			typ:         typ,
			size:        size,
			data:        data,
			columnIndex: ^columnIndex,
		},
	}
}

func (d *fixedLenByteArrayDictionary) Type() Type { return newIndexedType(d.typ, d) }

func (d *fixedLenByteArrayDictionary) Len() int { return len(d.data) / d.size }

func (d *fixedLenByteArrayDictionary) Size() int64 { return int64(len(d.data)) }

func (d *fixedLenByteArrayDictionary) Index(i int32) Value {
	return d.makeValueBytes(d.index(i))
}

func (d *fixedLenByteArrayDictionary) index(i int32) []byte {
	j := (int(i) + 0) * d.size
	k := (int(i) + 1) * d.size
	return d.data[j:k:k]
}

func (d *fixedLenByteArrayDictionary) Insert(indexes []int32, values []Value) {
	d.insertValues(indexes, len(values), func(i int) *byte {
		return values[i].ptr
	})
}

func (d *fixedLenByteArrayDictionary) insert(indexes []int32, rows sparse.Array) {
	d.insertValues(indexes, rows.Len(), func(i int) *byte {
		return (*byte)(rows.Index(i))
	})
}

func (d *fixedLenByteArrayDictionary) insertValues(indexes []int32, count int, valueAt func(int) *byte) {
	_ = indexes[:count]

	if d.hashmap == nil {
		d.hashmap = make(map[string]int32, cap(d.data)/d.size)
		for i, j := 0, int32(0); i < len(d.data); i += d.size {
			d.hashmap[string(d.data[i:i+d.size])] = j
			j++
		}
	}

	for i := range count {
		value := unsafe.Slice(valueAt(i), d.size)

		index, exists := d.hashmap[string(value)]
		if !exists {
			index = int32(d.Len())
			start := len(d.data)
			d.data = append(d.data, value...)
			d.hashmap[string(d.data[start:])] = index
		}

		indexes[i] = index
	}
}

func (d *fixedLenByteArrayDictionary) Lookup(indexes []int32, values []Value) {
	model := d.makeValueString("")
	memsetValues(values, model)
	d.lookupString(indexes, makeArrayValue(values, offsetOfPtr))
}

func (d *fixedLenByteArrayDictionary) Bounds(indexes []int32) (min, max Value) {
	if len(indexes) > 0 {
		base := d.index(indexes[0])
		minValue := unsafecast.String(base)
		maxValue := minValue
		values := [64]string{}

		for i := 1; i < len(indexes); i += len(values) {
			n := len(indexes) - i
			if n > len(values) {
				n = len(values)
			}
			j := i + n
			d.lookupString(indexes[i:j:j], makeArrayFromSlice(values[:n:n]))

			for _, value := range values[:n:n] {
				switch {
				case value < minValue:
					minValue = value
				case value > maxValue:
					maxValue = value
				}
			}
		}

		min = d.makeValueString(minValue)
		max = d.makeValueString(maxValue)
	}
	return min, max
}

func (d *fixedLenByteArrayDictionary) Reset() {
	d.data = d.data[:0]
	d.hashmap = nil
}

func (d *fixedLenByteArrayDictionary) Page() Page {
	return &d.fixedLenByteArrayPage
}

func (d *fixedLenByteArrayDictionary) insertReflectValue(value reflect.Value) int32 {
	b := value.Bytes()

	// Use the existing insertValues method
	indexes := [1]int32{0}
	d.insertValues(indexes[:], 1, func(i int) *byte {
		return &b[0]
	})
	return indexes[0]
}
