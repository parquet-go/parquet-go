package parquet

import (
	"fmt"
	"math"
	"reflect"

	"github.com/parquet-go/parquet-go/encoding"
	"github.com/parquet-go/parquet-go/hashprobe"
	"github.com/parquet-go/parquet-go/sparse"
)

type int64Dictionary struct {
	int64Page
	table *hashprobe.Int64Table
}

func newInt64Dictionary(typ Type, columnIndex int16, numValues int32, data encoding.Values) *int64Dictionary {
	return &int64Dictionary{
		int64Page: int64Page{
			typ:         typ,
			values:      data.Int64()[:numValues],
			columnIndex: ^columnIndex,
		},
	}
}

func (d *int64Dictionary) Type() Type { return newIndexedType(d.typ, d) }

func (d *int64Dictionary) Len() int { return len(d.values) }

func (d *int64Dictionary) Size() int64 { return int64(len(d.values) * 8) }

func (d *int64Dictionary) Index(i int32) Value { return d.makeValue(d.index(i)) }

func (d *int64Dictionary) index(i int32) int64 { return d.values[i] }

func (d *int64Dictionary) Insert(indexes []int32, values []Value) {
	d.insert(indexes, makeArrayValue(values, offsetOfU64))
}

func (d *int64Dictionary) init(indexes []int32) {
	d.table = hashprobe.NewInt64Table(len(d.values), hashprobeTableMaxLoad)

	n := min(len(d.values), len(indexes))

	for i := 0; i < len(d.values); i += n {
		j := min(i+n, len(d.values))
		d.table.Probe(d.values[i:j:j], indexes[:n:n])
	}
}

func (d *int64Dictionary) insert(indexes []int32, rows sparse.Array) {
	const chunkSize = insertsTargetCacheFootprint / 8

	if d.table == nil {
		d.init(indexes)
	}

	values := rows.Int64Array()

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

func (d *int64Dictionary) Lookup(indexes []int32, values []Value) {
	model := d.makeValue(0)
	memsetValues(values, model)
	d.lookup(indexes, makeArrayValue(values, offsetOfU64))
}

func (d *int64Dictionary) Bounds(indexes []int32) (min, max Value) {
	if len(indexes) > 0 {
		minValue, maxValue := d.bounds(indexes)
		min = d.makeValue(minValue)
		max = d.makeValue(maxValue)
	}
	return min, max
}

func (d *int64Dictionary) Reset() {
	d.values = d.values[:0]
	if d.table != nil {
		d.table.Reset()
	}
}

func (d *int64Dictionary) Page() Page {
	return &d.int64Page
}

func (d *int64Dictionary) insertReflectValue(value reflect.Value) int32 {
	var v int64
	switch value.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v = value.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		u := value.Uint()
		if u > math.MaxInt64 {
			panic(fmt.Sprintf("uint value %d out of range for int64", u))
		}
		v = int64(u)
	default:
		panic("cannot insert value of type " + value.Type().String() + " into int64 dictionary")
	}

	var indexes [1]int32
	d.insert(indexes[:], makeArrayFromPointer(&v))
	return indexes[0]
}
