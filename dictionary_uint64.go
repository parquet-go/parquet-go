package parquet

import (
	"fmt"

	"github.com/parquet-go/parquet-go/deprecated"
	"github.com/parquet-go/parquet-go/encoding"
	"github.com/parquet-go/parquet-go/hashprobe"
	"github.com/parquet-go/parquet-go/sparse"
)

type uint64Dictionary struct {
	uint64Page
	table *hashprobe.Uint64Table
}

func newUint64Dictionary(typ Type, columnIndex int16, numValues int32, data encoding.Values) *uint64Dictionary {
	return &uint64Dictionary{
		uint64Page: uint64Page{
			typ:         typ,
			values:      data.Uint64()[:numValues],
			columnIndex: ^columnIndex,
		},
	}
}

func (d *uint64Dictionary) Type() Type { return newIndexedType(d.typ, d) }

func (d *uint64Dictionary) Len() int { return len(d.values) }

func (d *uint64Dictionary) Size() int64 { return int64(len(d.values) * 8) }

func (d *uint64Dictionary) Index(i int32) Value { return d.makeValue(d.index(i)) }

func (d *uint64Dictionary) index(i int32) uint64 { return d.values[i] }

func (d *uint64Dictionary) Insert(indexes []int32, values []Value) {
	d.insert(indexes, makeArrayValue(values, offsetOfU64))
}

func (d *uint64Dictionary) init(indexes []int32) {
	d.table = hashprobe.NewUint64Table(len(d.values), hashprobeTableMaxLoad)

	n := min(len(d.values), len(indexes))

	for i := 0; i < len(d.values); i += n {
		j := min(i+n, len(d.values))
		d.table.Probe(d.values[i:j:j], indexes[:n:n])
	}
}

func (d *uint64Dictionary) insert(indexes []int32, rows sparse.Array) {
	const chunkSize = insertsTargetCacheFootprint / 8

	if d.table == nil {
		d.init(indexes)
	}

	values := rows.Uint64Array()

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

func (d *uint64Dictionary) Lookup(indexes []int32, values []Value) {
	model := d.makeValue(0)
	memsetValues(values, model)
	d.lookup(indexes, makeArrayValue(values, offsetOfU64))
}

func (d *uint64Dictionary) Bounds(indexes []int32) (min, max Value) {
	if len(indexes) > 0 {
		minValue, maxValue := d.bounds(indexes)
		min = d.makeValue(minValue)
		max = d.makeValue(maxValue)
	}
	return min, max
}

func (d *uint64Dictionary) Reset() {
	d.values = d.values[:0]
	if d.table != nil {
		d.table.Reset()
	}
}

func (d *uint64Dictionary) Page() Page {
	return &d.uint64Page
}

func (d *uint64Dictionary) insertBoolean(value bool) int32 {
	panic("cannot insert boolean value into uint64 dictionary")
}

func (d *uint64Dictionary) insertInt32(value int32) int32 {
	if value < 0 {
		panic(fmt.Sprintf("int32 value %d out of range for uint64", value))
	}
	v := uint64(value)
	var indexes [1]int32
	d.insert(indexes[:], makeArrayFromPointer(&v))
	return indexes[0]
}

func (d *uint64Dictionary) insertInt64(value int64) int32 {
	if value < 0 {
		panic(fmt.Sprintf("int64 value %d out of range for uint64", value))
	}
	v := uint64(value)
	var indexes [1]int32
	d.insert(indexes[:], makeArrayFromPointer(&v))
	return indexes[0]
}

func (d *uint64Dictionary) insertInt96(value deprecated.Int96) int32 {
	panic("cannot insert int96 value into uint64 dictionary")
}

func (d *uint64Dictionary) insertFloat(value float32) int32 {
	panic("cannot insert float value into uint64 dictionary")
}

func (d *uint64Dictionary) insertDouble(value float64) int32 {
	panic("cannot insert double value into uint64 dictionary")
}

func (d *uint64Dictionary) insertByteArray(value []byte) int32 {
	panic("cannot insert byte array value into uint64 dictionary")
}
