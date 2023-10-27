package parquet_test

import (
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/deprecated"
	"reflect"
	"testing"
)

func toPtr[T any](obj T) *T {
	return &obj
}

func TestAssignValues(t *testing.T) {
	var (
		boolType    = reflect.TypeOf(true)
		int32Type   = reflect.TypeOf(int32(0))
		int64Type   = reflect.TypeOf(int64(0))
		int96Type   = reflect.TypeOf(deprecated.Int96{})
		float32Type = reflect.TypeOf(float32(0))
		float64Type = reflect.TypeOf(float64(0))
		stringType  = reflect.TypeOf("")
	)

	var assignValueTests = [...]struct {
		scenario  string
		fromType  parquet.Type
		fromValue parquet.Value
		toType    reflect.Type
		toValue   reflect.Value
	}{
		{
			scenario:  "bool to bool",
			fromType:  parquet.BooleanType,
			fromValue: parquet.BooleanValue(true),
			toType:    boolType,
			toValue:   reflect.ValueOf(true),
		},
		{
			scenario:  "bool to *bool",
			fromType:  parquet.BooleanType,
			fromValue: parquet.BooleanValue(true),
			toType:    reflect.PtrTo(boolType),
			toValue:   reflect.ValueOf(toPtr(true)),
		},
		{
			scenario:  "int32 to int32",
			fromType:  parquet.Int32Type,
			fromValue: parquet.Int32Value(1254),
			toType:    int32Type,
			toValue:   reflect.ValueOf(int32(1254)),
		},
		{
			scenario:  "int32 to *int32",
			fromType:  parquet.Int32Type,
			fromValue: parquet.Int32Value(1254),
			toType:    reflect.PtrTo(int32Type),
			toValue:   reflect.ValueOf(toPtr(int32(1254))),
		},
		{
			scenario:  "int64 to int64",
			fromType:  parquet.Int64Type,
			fromValue: parquet.Int64Value(1254),
			toType:    int64Type,
			toValue:   reflect.ValueOf(int64(1254)),
		},
		{
			scenario:  "int64 to *int64",
			fromType:  parquet.Int64Type,
			fromValue: parquet.Int32Value(1254),
			toType:    reflect.PtrTo(int64Type),
			toValue:   reflect.ValueOf(toPtr(int64(1254))),
		},
		{
			scenario:  "int96 to int96",
			fromType:  parquet.Int96Type,
			fromValue: parquet.Int96Value(deprecated.Int96{0: 1254}),
			toType:    int96Type,
			toValue:   reflect.ValueOf(deprecated.Int96{0: 1254}),
		},
		{
			scenario:  "int96 to *int96",
			fromType:  parquet.Int96Type,
			fromValue: parquet.Int96Value(deprecated.Int96{0: 1254}),
			toType:    reflect.PtrTo(int96Type),
			toValue:   reflect.ValueOf(toPtr(deprecated.Int96{0: 1254})),
		},
		{
			scenario:  "float32 to float32",
			fromType:  parquet.FloatType,
			fromValue: parquet.FloatValue(float32(12.54)),
			toType:    float32Type,
			toValue:   reflect.ValueOf(float32(12.54)),
		},
		{
			scenario:  "float32 to *float32",
			fromType:  parquet.FloatType,
			fromValue: parquet.FloatValue(float32(12.54)),
			toType:    reflect.PtrTo(float32Type),
			toValue:   reflect.ValueOf(toPtr(float32(12.54))),
		},
		{
			scenario:  "float64 to float64",
			fromType:  parquet.DoubleType,
			fromValue: parquet.DoubleValue(12.54),
			toType:    float64Type,
			toValue:   reflect.ValueOf(12.54),
		},
		{
			scenario:  "float64 to *float64",
			fromType:  parquet.DoubleType,
			fromValue: parquet.DoubleValue(12.54),
			toType:    reflect.PtrTo(float64Type),
			toValue:   reflect.ValueOf(toPtr(12.54)),
		},
		{
			scenario:  "byteArray to string",
			fromType:  parquet.ByteArrayType,
			fromValue: parquet.ByteArrayValue([]byte("test value")),
			toType:    stringType,
			toValue:   reflect.ValueOf("test value"),
		},
		{
			scenario:  "byteArray to *string",
			fromType:  parquet.ByteArrayType,
			fromValue: parquet.ByteArrayValue([]byte("test value")),
			toType:    reflect.PtrTo(stringType),
			toValue:   reflect.ValueOf(toPtr("test value")),
		},
		// type conversion tests
		{
			scenario:  "int32 to *int64",
			fromType:  parquet.Int32Type,
			fromValue: parquet.Int32Value(1254),
			toType:    reflect.PtrTo(int64Type),
			toValue:   reflect.ValueOf(toPtr(int64(1254))),
		},
		{
			scenario:  "int64 to *int32",
			fromType:  parquet.Int64Type,
			fromValue: parquet.Int64Value(1254),
			toType:    reflect.PtrTo(int32Type),
			toValue:   reflect.ValueOf(toPtr(int32(1254))),
		},
		{
			scenario:  "float64 to *float32",
			fromType:  parquet.DoubleType,
			fromValue: parquet.DoubleValue(12.54),
			toType:    reflect.PtrTo(float32Type),
			toValue:   reflect.ValueOf(toPtr(float32(12.54))),
		},
		// testing float32 to float64 is not viable, as it is prone to value mismatch due to precision errors
	}

	for _, testCase := range assignValueTests {
		t.Run(testCase.scenario, func(t *testing.T) {
			got := reflect.New(testCase.toType)

			err := testCase.fromType.AssignValue(got.Elem(), testCase.fromValue)
			if err != nil {
				t.Fatal(err)
			}

			if got.Elem().Kind() != testCase.toValue.Kind() {
				t.Errorf("assigned kinds not equal: \nwant = %+v\ngot  = %+v", testCase.toValue.Kind(),
					got.Elem().Kind())
			}
			if !reflect.Indirect(got.Elem()).Equal(reflect.Indirect(testCase.toValue)) {
				t.Errorf("assigned values not equal: \nwant = %+v\ngot  = %+v", reflect.Indirect(testCase.toValue),
					reflect.Indirect(got.Elem()))
			}
		})
	}
}
