package parquet

import (
	"fmt"
	"math"

	"github.com/parquet-go/parquet-go/deprecated"
	"github.com/parquet-go/parquet-go/encoding"
	"github.com/parquet-go/parquet-go/hashprobe"
	"github.com/parquet-go/parquet-go/sparse"
)

type uint32Dictionary struct {
	uint32Page
	table *hashprobe.Uint32Table
}

func newUint32Dictionary(typ Type, columnIndex int16, numValues int32, data encoding.Values) *uint32Dictionary {
	return &uint32Dictionary{
		uint32Page: uint32Page{
			typ:         typ,
			values:      data.Uint32()[:numValues],
			columnIndex: ^columnIndex,
		},
	}
}

func (d *uint32Dictionary) Type() Type { return newIndexedType(d.typ, d) }

func (d *uint32Dictionary) Len() int { return len(d.values) }

func (d *uint32Dictionary) Size() int64 { return int64(len(d.values) * 4) }

func (d *uint32Dictionary) Index(i int32) Value { return d.makeValue(d.index(i)) }

func (d *uint32Dictionary) index(i int32) uint32 { return d.values[i] }

func (d *uint32Dictionary) Insert(indexes []int32, values []Value) {
	d.insert(indexes, makeArrayValue(values, offsetOfU32))
}

func (d *uint32Dictionary) init(indexes []int32) {
	d.table = hashprobe.NewUint32Table(len(d.values), hashprobeTableMaxLoad)

	n := min(len(d.values), len(indexes))

	for i := 0; i < len(d.values); i += n {
		j := min(i+n, len(d.values))
		d.table.Probe(d.values[i:j:j], indexes[:n:n])
	}
}

func (d *uint32Dictionary) insert(indexes []int32, rows sparse.Array) {
	const chunkSize = insertsTargetCacheFootprint / 4

	if d.table == nil {
		d.init(indexes)
	}

	values := rows.Uint32Array()

	for i := 0; i < values.Len(); i += chunkSize {
		j := min(i+chunkSize, values.Len())

		if d.table.ProbeArray(values.Slice(i, j), indexes[i:j:j]) > 0 {
			for k, index := range indexes[i:j] {
				if index == int32(len(d.values)) {
					d.values = append(d.values, values.Index(i+k))
				}
			}
		}
	}
}

func (d *uint32Dictionary) Lookup(indexes []int32, values []Value) {
	model := d.makeValue(0)
	memsetValues(values, model)
	d.lookup(indexes, makeArrayValue(values, offsetOfU32))
}

func (d *uint32Dictionary) Bounds(indexes []int32) (min, max Value) {
	if len(indexes) > 0 {
		minValue, maxValue := d.bounds(indexes)
		min = d.makeValue(minValue)
		max = d.makeValue(maxValue)
	}
	return min, max
}

func (d *uint32Dictionary) Reset() {
	d.values = d.values[:0]
	if d.table != nil {
		d.table.Reset()
	}
}

func (d *uint32Dictionary) Page() Page {
	return &d.uint32Page
}

func (d *uint32Dictionary) insertBoolean(value bool) int32 {
	panic("cannot insert boolean value into uint32 dictionary")
}

func (d *uint32Dictionary) insertInt32(value int32) int32 {
	if value < 0 {
		panic(fmt.Sprintf("int32 value %d out of range for uint32", value))
	}
	v := uint32(value)
	var indexes [1]int32
	d.insert(indexes[:], makeArrayFromPointer(&v))
	return indexes[0]
}

func (d *uint32Dictionary) insertInt64(value int64) int32 {
	if value < 0 || value > math.MaxUint32 {
		panic(fmt.Sprintf("int64 value %d out of range for uint32", value))
	}
	v := uint32(value)
	var indexes [1]int32
	d.insert(indexes[:], makeArrayFromPointer(&v))
	return indexes[0]
}

func (d *uint32Dictionary) insertInt96(value deprecated.Int96) int32 {
	panic("cannot insert int96 value into uint32 dictionary")
}

func (d *uint32Dictionary) insertFloat(value float32) int32 {
	panic("cannot insert float value into uint32 dictionary")
}

func (d *uint32Dictionary) insertDouble(value float64) int32 {
	panic("cannot insert double value into uint32 dictionary")
}

func (d *uint32Dictionary) insertByteArray(value []byte) int32 {
	panic("cannot insert byte array value into uint32 dictionary")
}
