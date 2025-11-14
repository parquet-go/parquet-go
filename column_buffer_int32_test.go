package parquet

import (
	"math"
	"testing"
)

// TestInt32ColumnBufferWriteTypes tests that int32ColumnBuffer
// correctly converts various types to int32
func TestColumnBufferInt32WriteTypes(t *testing.T) {
	tests := []struct {
		name     string
		writeOp  func(*int32ColumnBuffer)
		expected int32
	}{
		{
			name: "writeBoolean_false",
			writeOp: func(col *int32ColumnBuffer) {
				col.writeBoolean(columnLevels{}, false)
			},
			expected: 0,
		},
		{
			name: "writeBoolean_true",
			writeOp: func(col *int32ColumnBuffer) {
				col.writeBoolean(columnLevels{}, true)
			},
			expected: 1,
		},
		{
			name: "writeInt32_positive",
			writeOp: func(col *int32ColumnBuffer) {
				col.writeInt32(columnLevels{}, 42)
			},
			expected: 42,
		},
		{
			name: "writeInt32_negative",
			writeOp: func(col *int32ColumnBuffer) {
				col.writeInt32(columnLevels{}, -123)
			},
			expected: -123,
		},
		{
			name: "writeInt32_zero",
			writeOp: func(col *int32ColumnBuffer) {
				col.writeInt32(columnLevels{}, 0)
			},
			expected: 0,
		},
		{
			name: "writeInt32_max",
			writeOp: func(col *int32ColumnBuffer) {
				col.writeInt32(columnLevels{}, math.MaxInt32)
			},
			expected: math.MaxInt32,
		},
		{
			name: "writeInt32_min",
			writeOp: func(col *int32ColumnBuffer) {
				col.writeInt32(columnLevels{}, math.MinInt32)
			},
			expected: math.MinInt32,
		},
		{
			name: "writeInt64_small",
			writeOp: func(col *int32ColumnBuffer) {
				col.writeInt64(columnLevels{}, 12345)
			},
			expected: 12345,
		},
		{
			name: "writeFloat_positive",
			writeOp: func(col *int32ColumnBuffer) {
				col.writeFloat(columnLevels{}, 3.14)
			},
			expected: 3,
		},
		{
			name: "writeFloat_negative",
			writeOp: func(col *int32ColumnBuffer) {
				col.writeFloat(columnLevels{}, -2.9)
			},
			expected: -2,
		},
		{
			name: "writeDouble_positive",
			writeOp: func(col *int32ColumnBuffer) {
				col.writeDouble(columnLevels{}, 42.7)
			},
			expected: 42,
		},
		{
			name: "writeByteArray_valid",
			writeOp: func(col *int32ColumnBuffer) {
				col.writeByteArray(columnLevels{}, []byte("42"))
			},
			expected: 42,
		},
		{
			name: "writeByteArray_negative",
			writeOp: func(col *int32ColumnBuffer) {
				col.writeByteArray(columnLevels{}, []byte("-999"))
			},
			expected: -999,
		},
		{
			name: "writeNull",
			writeOp: func(col *int32ColumnBuffer) {
				col.writeNull(columnLevels{})
			},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col := newInt32ColumnBuffer(Int32Type, 0, 10)
			tt.writeOp(col)

			if col.Len() != 1 {
				t.Fatalf("expected 1 value, got %d", col.Len())
			}

			values := make([]Value, 1)
			n, err := col.ReadValuesAt(values, 0)
			if err != nil || n != 1 {
				t.Fatalf("failed to read value: %v", err)
			}

			actual := values[0].Int32()
			if actual != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, actual)
			}
		})
	}
}

// TestInt32ColumnBufferWriteInt96 tests writeInt96 conversion
func TestColumnBufferInt32WriteInt96(t *testing.T) {
	col := newInt32ColumnBuffer(Int32Type, 0, 10)
	col.writeInt96(columnLevels{}, [3]uint32{42, 0, 0})

	if col.Len() != 1 {
		t.Fatalf("expected 1 value, got %d", col.Len())
	}

	values := make([]Value, 1)
	n, err := col.ReadValuesAt(values, 0)
	if err != nil || n != 1 {
		t.Fatalf("failed to read value: %v", err)
	}

	actual := values[0].Int32()
	if actual != 42 {
		t.Errorf("expected 42, got %v", actual)
	}
}

// TestInt32ColumnBufferOverflow documents truncation behavior for out-of-range values
func TestColumnBufferInt32Overflow(t *testing.T) {
	t.Run("int64_overflow", func(t *testing.T) {
		col := newInt32ColumnBuffer(Int32Type, 0, 10)
		col.writeInt64(columnLevels{}, int64(math.MaxInt32)+1)

		values := make([]Value, 1)
		col.ReadValuesAt(values, 0)

		actual := values[0].Int32()
		if actual != math.MinInt32 {
			t.Errorf("expected truncation to MinInt32, got %d", actual)
		}
	})

	t.Run("large_float", func(t *testing.T) {
		col := newInt32ColumnBuffer(Int32Type, 0, 10)
		col.writeFloat(columnLevels{}, 1e10)

		values := make([]Value, 1)
		col.ReadValuesAt(values, 0)

		values[0].Int32()
	})
}

// TestInt32ColumnBufferWriteByteArrayInvalid tests that invalid strings panic
func TestColumnBufferInt32WriteByteArrayInvalid(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for invalid byte array")
		}
	}()

	col := newInt32ColumnBuffer(Int32Type, 0, 10)
	col.writeByteArray(columnLevels{}, []byte("not a number"))
}

// TestInt32ColumnBufferWriteMultipleValues tests writing multiple values
func TestColumnBufferInt32WriteMultipleValues(t *testing.T) {
	col := newInt32ColumnBuffer(Int32Type, 0, 10)

	col.writeInt32(columnLevels{}, 1)
	col.writeInt64(columnLevels{}, 2)
	col.writeFloat(columnLevels{}, 3.9)
	col.writeBoolean(columnLevels{}, true)
	col.writeNull(columnLevels{})

	if col.Len() != 5 {
		t.Fatalf("expected 5 values, got %d", col.Len())
	}

	expected := []int32{1, 2, 3, 1, 0}
	values := make([]Value, 5)
	n, err := col.ReadValuesAt(values, 0)
	if err != nil || n != 5 {
		t.Fatalf("failed to read values: %v", err)
	}

	for i, exp := range expected {
		actual := values[i].Int32()
		if actual != exp {
			t.Errorf("index %d: expected %d, got %d", i, exp, actual)
		}
	}
}
