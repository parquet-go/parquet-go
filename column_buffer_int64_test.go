package parquet

import (
	"math"
	"testing"
)

// TestInt64ColumnBufferWriteTypes tests that int64ColumnBuffer
// correctly converts various types to int64
func TestColumnBufferInt64WriteTypes(t *testing.T) {
	tests := []struct {
		name     string
		writeOp  func(*int64ColumnBuffer)
		expected int64
	}{
		{
			name: "writeBoolean_false",
			writeOp: func(col *int64ColumnBuffer) {
				col.writeBoolean(columnLevels{}, false)
			},
			expected: 0,
		},
		{
			name: "writeBoolean_true",
			writeOp: func(col *int64ColumnBuffer) {
				col.writeBoolean(columnLevels{}, true)
			},
			expected: 1,
		},
		{
			name: "writeInt32_positive",
			writeOp: func(col *int64ColumnBuffer) {
				col.writeInt32(columnLevels{}, 42)
			},
			expected: 42,
		},
		{
			name: "writeInt32_negative",
			writeOp: func(col *int64ColumnBuffer) {
				col.writeInt32(columnLevels{}, -123)
			},
			expected: -123,
		},
		{
			name: "writeInt64_positive",
			writeOp: func(col *int64ColumnBuffer) {
				col.writeInt64(columnLevels{}, 123456789012345)
			},
			expected: 123456789012345,
		},
		{
			name: "writeInt64_negative",
			writeOp: func(col *int64ColumnBuffer) {
				col.writeInt64(columnLevels{}, -987654321098765)
			},
			expected: -987654321098765,
		},
		{
			name: "writeInt64_max",
			writeOp: func(col *int64ColumnBuffer) {
				col.writeInt64(columnLevels{}, math.MaxInt64)
			},
			expected: math.MaxInt64,
		},
		{
			name: "writeInt64_min",
			writeOp: func(col *int64ColumnBuffer) {
				col.writeInt64(columnLevels{}, math.MinInt64)
			},
			expected: math.MinInt64,
		},
		{
			name: "writeFloat_positive",
			writeOp: func(col *int64ColumnBuffer) {
				col.writeFloat(columnLevels{}, 3.14)
			},
			expected: 3,
		},
		{
			name: "writeDouble_positive",
			writeOp: func(col *int64ColumnBuffer) {
				col.writeDouble(columnLevels{}, 42.7)
			},
			expected: 42,
		},
		{
			name: "writeByteArray_valid",
			writeOp: func(col *int64ColumnBuffer) {
				col.writeByteArray(columnLevels{}, []byte("123456789"))
			},
			expected: 123456789,
		},
		{
			name: "writeNull",
			writeOp: func(col *int64ColumnBuffer) {
				col.writeNull(columnLevels{})
			},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col := newInt64ColumnBuffer(Int64Type, 0, 10)
			tt.writeOp(col)

			if col.Len() != 1 {
				t.Fatalf("expected 1 value, got %d", col.Len())
			}

			values := make([]Value, 1)
			n, err := col.ReadValuesAt(values, 0)
			if err != nil || n != 1 {
				t.Fatalf("failed to read value: %v", err)
			}

			actual := values[0].Int64()
			if actual != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, actual)
			}
		})
	}
}

// TestInt64ColumnBufferWriteInt96 tests writeInt96 conversion
func TestColumnBufferInt64WriteInt96(t *testing.T) {
	col := newInt64ColumnBuffer(Int64Type, 0, 10)
	col.writeInt96(columnLevels{}, [3]uint32{42, 0, 0})

	values := make([]Value, 1)
	col.ReadValuesAt(values, 0)

	actual := values[0].Int64()
	if actual != 42 {
		t.Errorf("expected 42, got %v", actual)
	}
}

// TestInt64ColumnBufferPrecisionLoss documents precision loss for large floats
func TestColumnBufferInt64PrecisionLoss(t *testing.T) {
	t.Run("large_float", func(t *testing.T) {
		col := newInt64ColumnBuffer(Int64Type, 0, 10)
		col.writeFloat(columnLevels{}, 1e15)

		values := make([]Value, 1)
		col.ReadValuesAt(values, 0)

		values[0].Int64()
	})

	t.Run("large_double", func(t *testing.T) {
		col := newInt64ColumnBuffer(Int64Type, 0, 10)
		col.writeDouble(columnLevels{}, 1e18)

		values := make([]Value, 1)
		col.ReadValuesAt(values, 0)

		values[0].Int64()
	})
}

// TestInt64ColumnBufferWriteMultipleValues tests writing multiple values
func TestColumnBufferInt64WriteMultipleValues(t *testing.T) {
	col := newInt64ColumnBuffer(Int64Type, 0, 10)

	col.writeInt32(columnLevels{}, 1)
	col.writeInt64(columnLevels{}, 2)
	col.writeFloat(columnLevels{}, 3.9)
	col.writeBoolean(columnLevels{}, true)
	col.writeNull(columnLevels{})

	if col.Len() != 5 {
		t.Fatalf("expected 5 values, got %d", col.Len())
	}

	expected := []int64{1, 2, 3, 1, 0}
	values := make([]Value, 5)
	n, err := col.ReadValuesAt(values, 0)
	if err != nil || n != 5 {
		t.Fatalf("failed to read values: %v", err)
	}

	for i, exp := range expected {
		actual := values[i].Int64()
		if actual != exp {
			t.Errorf("index %d: expected %d, got %d", i, exp, actual)
		}
	}
}
