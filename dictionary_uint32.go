package parquet

import (
	"fmt"
	"math"
	"reflect"

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

func (d *uint32Dictionary) insertReflectValue(value reflect.Value) int32 {
	var v uint32
	switch value.Kind() {
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		u := value.Uint()
		if u > math.MaxUint32 {
			panic(fmt.Sprintf("uint value %d out of range for uint32", u))
		}
		v = uint32(u)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		i := value.Int()
		if i < 0 || i > math.MaxUint32 {
			panic(fmt.Sprintf("int value %d out of range for uint32", i))
		}
		v = uint32(i)
	default:
		panic("cannot insert value of type " + value.Type().String() + " into uint32 dictionary")
	}

	var indexes [1]int32
	d.insert(indexes[:], makeArrayFromPointer(&v))
	return indexes[0]
}
