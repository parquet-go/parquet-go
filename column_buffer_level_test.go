package parquet

import (
	"bytes"
	"io"
	"testing"

	"github.com/parquet-go/parquet-go/deprecated"
	"github.com/parquet-go/parquet-go/encoding"
)

func TestReadValuesAtLevel(t *testing.T) {
	fixed := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	be := [16]byte{0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}
	tests := []struct {
		name   string
		col    levelValueReader
		values []Value
		level  byte
	}{
		{
			name:   "boolean",
			col:    newBooleanColumnBuffer(BooleanType, 1, 0),
			values: []Value{BooleanValue(true), BooleanValue(false), BooleanValue(true)},
			level:  1,
		},
		{
			name:   "int32",
			col:    newInt32ColumnBuffer(Int32Type, 2, 0),
			values: []Value{Int32Value(11), Int32Value(-22), Int32Value(0)},
			level:  3,
		},
		{
			name:   "int64",
			col:    newInt64ColumnBuffer(Int64Type, 3, 0),
			values: []Value{Int64Value(1 << 40), Int64Value(-1)},
			level:  1,
		},
		{
			name:   "int96",
			col:    newInt96ColumnBuffer(Int96Type, 4, 0),
			values: []Value{Int96Value(deprecated.Int96{1, 2, 3})},
			level:  2,
		},
		{
			name:   "float",
			col:    newFloatColumnBuffer(FloatType, 5, 0),
			values: []Value{FloatValue(1.5), FloatValue(-0)},
			level:  1,
		},
		{
			name:   "double",
			col:    newDoubleColumnBuffer(DoubleType, 6, 0),
			values: []Value{DoubleValue(1.5), DoubleValue(-0)},
			level:  1,
		},
		{
			name:   "byte-array",
			col:    newByteArrayColumnBuffer(ByteArrayType, 7, 0),
			values: []Value{ByteArrayValue([]byte("ab")), ByteArrayValue(nil), ByteArrayValue([]byte{})},
			level:  2,
		},
		{
			name:   "fixed",
			col:    newFixedLenByteArrayColumnBuffer(FixedLenByteArrayType(len(fixed)), 8, 0),
			values: []Value{FixedLenByteArrayValue(fixed), FixedLenByteArrayValue(bytes.Repeat([]byte{9}, len(fixed)))},
			level:  1,
		},
		{
			name:   "uint32",
			col:    newUint32ColumnBuffer(uint32Type{}, 9, 0),
			values: []Value{Int32Value(-1), Int32Value(7)},
			level:  1,
		},
		{
			name:   "uint64",
			col:    newUint64ColumnBuffer(uint64Type{}, 10, 0),
			values: []Value{Int64Value(-1), Int64Value(7)},
			level:  1,
		},
		{
			name:   "be128",
			col:    newBE128ColumnBuffer(FixedLenByteArrayType(16), 11, 0),
			values: []Value{FixedLenByteArrayValue(be[:])},
			level:  1,
		},
		{
			name:   "null",
			col:    newNullColumnBuffer(&nullType{}, 12, 0),
			values: []Value{NullValue(), NullValue()},
			level:  1,
		},
	}

	dictValues := encoding.ByteArrayValues(nil, nil)
	dict := String().Type().NewDictionary(13, 0, dictValues)
	indexed := dict.Type().NewColumnBuffer(13, 0)
	tests = append(tests, struct {
		name   string
		col    levelValueReader
		values []Value
		level  byte
	}{
		name:   "dictionary",
		col:    indexed.(levelValueReader),
		values: []Value{ByteArrayValue([]byte("a")), ByteArrayValue([]byte("b")), ByteArrayValue([]byte("a"))},
		level:  1,
	})

	decimalNode := Decimal(2, 10, FixedLenByteArrayType(len(fixed)))
	decimalCol := decimalNode.Type().NewColumnBuffer(14, 0)
	tests = append(tests, struct {
		name   string
		col    levelValueReader
		values []Value
		level  byte
	}{
		name:   "decimal",
		col:    decimalCol.(levelValueReader),
		values: []Value{FixedLenByteArrayValue(fixed)},
		level:  2,
	})

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			writer := test.col.(ValueWriter)
			if _, err := writer.WriteValues(test.values); err != nil {
				t.Fatal(err)
			}
			compareLevelRead(t, test.col, len(test.values), test.level)
		})
	}
}

func compareLevelRead(t *testing.T, col levelValueReader, n int, level byte) {
	t.Helper()
	reader := col.(ValueReaderAt)
	plain := make([]Value, n)
	got := make([]Value, n)
	pn, perr := reader.ReadValuesAt(plain, 0)
	gn, gerr := col.readValuesAtLevel(got, 0, level)
	if pn != gn || perr != nil || gerr != nil {
		t.Fatalf("read plain (%d,%v) level (%d,%v)", pn, perr, gn, gerr)
	}
	for i := range plain {
		plain[i].definitionLevel = level
		if !valuesSame(got[i], plain[i]) {
			t.Fatalf("value %d\ngot  %#v %q\nwant %#v %q", i, got[i], valueBytes(got[i]), plain[i], valueBytes(plain[i]))
		}
	}

	// A short read at a non-zero offset stops at the destination length and
	// reports no error while values remain.
	if n >= 2 {
		one := make([]Value, 1)
		want := make([]Value, 1)
		wn, werr := reader.ReadValuesAt(want, 1)
		gn, gerr = col.readValuesAtLevel(one, 1, level)
		if wn != 1 || gn != 1 || werr != nil || gerr != nil {
			t.Fatalf("offset read plain (%d,%v) level (%d,%v)", wn, werr, gn, gerr)
		}
		want[0].definitionLevel = level
		if !valuesSame(one[0], want[0]) {
			t.Fatalf("offset value\ngot  %#v\nwant %#v", one[0], want[0])
		}
	}

	_, err := col.readValuesAtLevel(got[:0], int64(n), level)
	if err != io.EOF {
		t.Fatalf("offset at end: %v", err)
	}
	if _, err := col.readValuesAtLevel(got[:1], -1, level); err == nil {
		t.Fatal("negative offset returned nil error")
	}
}

func valuesSame(a, b Value) bool {
	if a.kind != b.kind || a.u64 != b.u64 || a.definitionLevel != b.definitionLevel || a.repetitionLevel != b.repetitionLevel || a.columnIndex != b.columnIndex {
		return false
	}
	switch Kind(^a.kind) { // kind is stored XOR'd; zero is null
	case ByteArray, FixedLenByteArray, Int96:
		return bytes.Equal(a.byteArray(), b.byteArray())
	default:
		return a.ptr == b.ptr
	}
}

func valueBytes(v Value) []byte {
	if v.ptr == nil || v.u64 > 64 {
		return nil
	}
	return append([]byte(nil), v.byteArray()...)
}
