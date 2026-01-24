package parquet

import (
	"math"
	"reflect"

	"github.com/parquet-go/parquet-go/deprecated"
	"github.com/parquet-go/parquet-go/format"
)

// Decimal constructs a leaf node of decimal logical type with the given
// scale, precision, and underlying type.
//
// https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#decimal
func Decimal(scale, precision int, typ Type) Node {
	switch typ.Kind() {
	case Int32, Int64, ByteArray, FixedLenByteArray:
	default:
		panic("DECIMAL node must annotate Int32, Int64, ByteArray or FixedLenByteArray but got " + typ.String())
	}
	return Leaf(&decimalType{
		decimal: format.DecimalType{
			Scale:     int32(scale),
			Precision: int32(precision),
		},
		Type: typ,
	})
}

type decimalType struct {
	decimal format.DecimalType
	Type
}

func (t *decimalType) String() string { return t.decimal.String() }

func (t *decimalType) LogicalType() *format.LogicalType {
	return &format.LogicalType{Decimal: &t.decimal}
}

func (t *decimalType) ConvertedType() *deprecated.ConvertedType {
	return &convertedTypes[deprecated.Decimal]
}

func (t *decimalType) AssignValue(dst reflect.Value, src Value) error {
	switch t.Type {
	case Int32Type:
		val := float32(src.int32()) / float32(math.Pow10(int(t.decimal.Scale)))
		dst.Set(reflect.ValueOf(val))
	case Int64Type:
		val := float64(src.int64()) / math.Pow10(int(t.decimal.Scale))
		dst.Set(reflect.ValueOf(val))
	}

	return nil
}
