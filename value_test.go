package parquet_test

import (
	"bytes"
	"math"
	"testing"
	"time"
	"unsafe"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/deprecated"
)

func TestSizeOfValue(t *testing.T) {
	t.Logf("sizeof(parquet.Value) = %d", unsafe.Sizeof(parquet.Value{}))
}

func BenchmarkValueAppend(b *testing.B) {
	const N = 1024
	row := make(parquet.Row, 0, N)
	val := parquet.ValueOf(42)

	for i := 0; i < b.N; i++ {
		row = row[:0]
		for j := 0; j < N; j++ {
			row = append(row, val)
		}
	}

	b.SetBytes(N * int64(unsafe.Sizeof(parquet.Value{})))
}

func TestValueClone(t *testing.T) {
	tests := []struct {
		scenario string
		values   []interface{}
	}{
		{
			scenario: "BOOLEAN",
			values:   []interface{}{false, true},
		},

		{
			scenario: "INT8",
			values:   []interface{}{int8(0), int8(1), int8(math.MinInt8), int8(math.MaxInt8)},
		},

		{
			scenario: "INT32",
			values:   []interface{}{int32(0), int32(1), int32(math.MinInt32), int32(math.MaxInt32)},
		},

		{
			scenario: "INT64",
			values:   []interface{}{int64(0), int64(1), int64(math.MinInt64), int64(math.MaxInt64)},
		},

		{
			scenario: "FLOAT",
			values:   []interface{}{float32(0), float32(1), float32(-1)},
		},

		{
			scenario: "DOUBLE",
			values:   []interface{}{float64(0), float64(1), float64(-1)},
		},

		{
			scenario: "BYTE_ARRAY",
			values:   []interface{}{"", "A", "ABC", "Hello World!"},
		},

		{
			scenario: "FIXED_LEN_BYTE_ARRAY",
			values:   []interface{}{[1]byte{42}, [16]byte{0: 1}},
		},

		{
			scenario: "TIME",
			values: []interface{}{
				time.Date(2020, 1, 2, 3, 4, 5, 7, time.UTC),
				time.Date(2021, 2, 3, 4, 5, 6, 8, time.UTC),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			for _, value := range test.values {
				v := parquet.ValueOf(value)
				c := v.Clone()

				if !parquet.DeepEqual(v, c) {
					t.Errorf("cloned values are not equal: want=%#v got=%#v", v, c)
				}
				if v.RepetitionLevel() != c.RepetitionLevel() {
					t.Error("cloned values do not have the same repetition level")
				}
				if v.DefinitionLevel() != c.DefinitionLevel() {
					t.Error("cloned values do not have the same definition level")
				}
				if v.Column() != c.Column() {
					t.Error("cloned values do not have the same column index")
				}
			}
		})
	}
}

func TestZeroValue(t *testing.T) {
	var v parquet.Value
	if !v.IsNull() {
		t.Error("expected zero value parquet.Value to be null")
	}

	if v.Byte() != byte(0) {
		t.Errorf("byte not zero value: got=%#v", v.Byte())
	}

	if v.Boolean() != false {
		t.Errorf("boolean not zero value: got=%#v", v.Boolean())
	}

	if v.Int32() != 0 {
		t.Errorf("int32 not zero value: got=%#v", v.Int32())
	}

	if v.Int64() != 0 {
		t.Errorf("int64 not zero value: got=%#v", v.Int64())
	}

	var zeroInt96 deprecated.Int96
	if v.Int96() != zeroInt96 {
		t.Errorf("int96 not zero value: got=%#v", zeroInt96)
	}

	if v.Float() != 0 {
		t.Errorf("float not zero value: got=%#v", v.Float())
	}

	if v.Double() != 0 {
		t.Errorf("double not zero value: got=%#v", v.Double())
	}

	if v.Uint32() != 0 {
		t.Errorf("uint32 not zero value: got=%#v", v.Uint32())
	}

	if v.Uint64() != 0 {
		t.Errorf("uint64 not zero value: got=%#v", v.Uint64())
	}

	var zeroByte []byte
	if !bytes.Equal(v.ByteArray(), zeroByte) {
		t.Errorf("byte array not zero value: got=%#v", v.ByteArray())
	}
}
