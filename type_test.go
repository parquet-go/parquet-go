package parquet

import (
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go/format"
)

func TestShreddedVariant(t *testing.T) {
	errTestCases := []Node{
		Variant(),
		Map(String(), Leaf(ByteArrayType)),
		Decimal(0, 39, FixedLenByteArrayType(16)),
		Uint(8),
		Uint(16),
		Uint(32),
		Uint(64),
		Repeated(Leaf(int32Type{})),
		Repeated(String()),
		Enum(),
		// Also test some logical types that we don't yet support or provide API to construct
		Leaf(logicalType{Type: ByteArrayType, lt: format.LogicalType{Unknown: &format.NullType{}}}),
		Leaf(logicalType{Type: ByteArrayType, lt: format.LogicalType{Float16: &format.Float16Type{}}}),
		Leaf(logicalType{Type: ByteArrayType, lt: format.LogicalType{Geometry: &format.GeometryType{}}}),
		Leaf(logicalType{Type: ByteArrayType, lt: format.LogicalType{Geography: &format.GeographyType{}}}),
	}
	for _, testCase := range errTestCases {
		// Direct
		_, err := ShreddedVariant(testCase)
		if err == nil {
			t.Errorf("ShreddedVariant(%v) should have returned an error", testCase)
		} else if !strings.Contains(err.Error(), "not allowed") {
			t.Errorf("error message should contain 'not allowed': %q", err.Error())
		}
		// Indirect (group that contains the offending type)
		group := Group{"a": String(), "b": testCase}
		_, err = ShreddedVariant(group)
		if err == nil {
			t.Errorf("ShreddedVariant(%v) should have returned an error", group)
		} else if !strings.Contains(err.Error(), "not allowed") {
			t.Errorf("error message should contain 'not allowed': %q", err.Error())
		}
	}
}

type logicalType struct {
	Type
	lt format.LogicalType
}

func (l logicalType) LogicalType() *format.LogicalType {
	return &l.lt
}
