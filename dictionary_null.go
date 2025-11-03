package parquet

import (
	"reflect"

	"github.com/parquet-go/parquet-go/encoding"
	"github.com/parquet-go/parquet-go/sparse"
)

// nullDictionary is a dictionary for NULL type columns where all operations are no-ops.
type nullDictionary struct {
	nullPage
}

func newNullDictionary(typ Type, columnIndex int16, numValues int32, _ encoding.Values) *nullDictionary {
	return &nullDictionary{
		nullPage: *newNullPage(typ, columnIndex, numValues),
	}
}

func (d *nullDictionary) Type() Type { return d.nullPage.Type() }

func (d *nullDictionary) Len() int { return int(d.nullPage.count) }

func (d *nullDictionary) Size() int64 { return 0 }

func (d *nullDictionary) Index(i int32) Value { return NullValue() }

func (d *nullDictionary) Lookup(indexes []int32, values []Value) {
	checkLookupIndexBounds(indexes, makeArrayValue(values, 0))
	for i := range indexes {
		values[i] = NullValue()
	}
}

func (d *nullDictionary) Insert(indexes []int32, values []Value) {}

func (d *nullDictionary) Bounds(indexes []int32) (min, max Value) {
	return NullValue(), NullValue()
}

func (d *nullDictionary) Reset() {
	d.nullPage.count = 0
}

func (d *nullDictionary) Page() Page { return &d.nullPage }

func (d *nullDictionary) insert(indexes []int32, rows sparse.Array) {}

func (d *nullDictionary) insertReflectValue(value reflect.Value) int32 {
	// Null dictionary has only one value (null), always return index 0
	return 0
}
