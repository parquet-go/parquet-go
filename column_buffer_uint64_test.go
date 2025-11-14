package parquet

import (
	"math"
	"testing"
)

// TestUint64ColumnBufferWriteTypes tests that uint64ColumnBuffer
// correctly converts various types to uint64
func TestColumnBufferUint64WriteTypes(t *testing.T) {
	tests := []struct {
		name     string
		writeOp  func(*uint64ColumnBuffer)
		expected uint64
	}{
		{
			name: "writeBoolean_false",
			writeOp: func(col *uint64ColumnBuffer) {
				col.writeBoolean(columnLevels{}, false)
			},
			expected: 0,
		},
		{
			name: "writeBoolean_true",
			writeOp: func(col *uint64ColumnBuffer) {
				col.writeBoolean(columnLevels{}, true)
			},
			expected: 1,
		},
		{
			name: "writeInt32_positive",
			writeOp: func(col *uint64ColumnBuffer) {
				col.writeInt32(columnLevels{}, 42)
			},
			expected: 42,
		},
		{
			name: "writeInt64_positive",
			writeOp: func(col *uint64ColumnBuffer) {
				col.writeInt64(columnLevels{}, 123456789012345)
			},
			expected: 123456789012345,
		},
		{
			name: "writeFloat_positive",
			writeOp: func(col *uint64ColumnBuffer) {
				col.writeFloat(columnLevels{}, 3.14)
			},
			expected: 3,
		},
		{
			name: "writeDouble_positive",
			writeOp: func(col *uint64ColumnBuffer) {
				col.writeDouble(columnLevels{}, 42.7)
			},
			expected: 42,
		},
		{
			name: "writeByteArray_valid",
			writeOp: func(col *uint64ColumnBuffer) {
				col.writeByteArray(columnLevels{}, []byte("123456789"))
			},
			expected: 123456789,
		},
		{
			name: "writeNull",
			writeOp: func(col *uint64ColumnBuffer) {
				col.writeNull(columnLevels{})
			},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col := newUint64ColumnBuffer(Uint(64).Type(), 0, 10)
			tt.writeOp(col)

			if col.Len() != 1 {
				t.Fatalf("expected 1 value, got %d", col.Len())
			}

			values := make([]Value, 1)
			n, err := col.ReadValuesAt(values, 0)
			if err != nil || n != 1 {
				t.Fatalf("failed to read value: %v", err)
			}

			actual := values[0].Uint64()
			if actual != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, actual)
			}
		})
	}
}

// TestUint64ColumnBufferWriteInt96 tests writeInt96 conversion
func TestColumnBufferUint64WriteInt96(t *testing.T) {
	col := newUint64ColumnBuffer(Uint(64).Type(), 0, 10)
	col.writeInt96(columnLevels{}, [3]uint32{42, 0, 0})

	values := make([]Value, 1)
	col.ReadValuesAt(values, 0)

	actual := values[0].Uint64()
	if actual != 42 {
		t.Errorf("expected 42, got %v", actual)
	}
}

// TestUint64ColumnBufferWrapping documents wrapping behavior for negative values
func TestColumnBufferUint64Wrapping(t *testing.T) {
	t.Run("negative_int32_wraps", func(t *testing.T) {
		col := newUint64ColumnBuffer(Uint(64).Type(), 0, 10)
		col.writeInt32(columnLevels{}, -1)

		values := make([]Value, 1)
		col.ReadValuesAt(values, 0)

		actual := values[0].Uint64()
		if actual != math.MaxUint64 {
			t.Errorf("expected MaxUint64, got %d", actual)
		}
	})

	t.Run("negative_int64_wraps", func(t *testing.T) {
		col := newUint64ColumnBuffer(Uint(64).Type(), 0, 10)
		col.writeInt64(columnLevels{}, -1)

		values := make([]Value, 1)
		col.ReadValuesAt(values, 0)

		actual := values[0].Uint64()
		if actual != math.MaxUint64 {
			t.Errorf("expected MaxUint64, got %d", actual)
		}
	})

	t.Run("negative_float_wraps", func(t *testing.T) {
		col := newUint64ColumnBuffer(Uint(64).Type(), 0, 10)
		col.writeFloat(columnLevels{}, -1.0)

		values := make([]Value, 1)
		col.ReadValuesAt(values, 0)

		values[0].Uint64()
	})

	t.Run("negative_double_wraps", func(t *testing.T) {
		col := newUint64ColumnBuffer(Uint(64).Type(), 0, 10)
		col.writeDouble(columnLevels{}, -1.0)

		values := make([]Value, 1)
		col.ReadValuesAt(values, 0)

		values[0].Uint64()
	})
}

// TestUint64ColumnBufferWriteByteArrayInvalid tests that invalid strings default to 0
func TestColumnBufferUint64WriteByteArrayInvalid(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for invalid byte array")
		}
	}()

	col := newUint64ColumnBuffer(Uint(64).Type(), 0, 10)
	col.writeByteArray(columnLevels{}, []byte("not a number"))
}

// TestUint64ColumnBufferWriteMultipleValues tests writing multiple values
func TestColumnBufferUint64WriteMultipleValues(t *testing.T) {
	col := newUint64ColumnBuffer(Uint(64).Type(), 0, 10)

	col.writeInt32(columnLevels{}, 1)
	col.writeInt64(columnLevels{}, 2)
	col.writeFloat(columnLevels{}, 3.9)
	col.writeBoolean(columnLevels{}, true)
	col.writeNull(columnLevels{})

	if col.Len() != 5 {
		t.Fatalf("expected 5 values, got %d", col.Len())
	}

	expected := []uint64{1, 2, 3, 1, 0}
	values := make([]Value, 5)
	n, err := col.ReadValuesAt(values, 0)
	if err != nil || n != 5 {
		t.Fatalf("failed to read values: %v", err)
	}

	for i, exp := range expected {
		actual := values[i].Uint64()
		if actual != exp {
			t.Errorf("index %d: expected %d, got %d", i, exp, actual)
		}
	}
}
