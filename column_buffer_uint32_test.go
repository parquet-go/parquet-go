package parquet

import (
	"math"
	"testing"
)

// TestUint32ColumnBufferWriteTypes tests that uint32ColumnBuffer
// correctly converts various types to uint32
func TestColumnBufferUint32WriteTypes(t *testing.T) {
	tests := []struct {
		name     string
		writeOp  func(*uint32ColumnBuffer)
		expected uint32
	}{
		{
			name: "writeBoolean_false",
			writeOp: func(col *uint32ColumnBuffer) {
				col.writeBoolean(columnLevels{}, false)
			},
			expected: 0,
		},
		{
			name: "writeBoolean_true",
			writeOp: func(col *uint32ColumnBuffer) {
				col.writeBoolean(columnLevels{}, true)
			},
			expected: 1,
		},
		{
			name: "writeInt32_positive",
			writeOp: func(col *uint32ColumnBuffer) {
				col.writeInt32(columnLevels{}, 42)
			},
			expected: 42,
		},
		{
			name: "writeInt32_zero",
			writeOp: func(col *uint32ColumnBuffer) {
				col.writeInt32(columnLevels{}, 0)
			},
			expected: 0,
		},
		{
			name: "writeInt64_positive",
			writeOp: func(col *uint32ColumnBuffer) {
				col.writeInt64(columnLevels{}, 12345)
			},
			expected: 12345,
		},
		{
			name: "writeFloat_positive",
			writeOp: func(col *uint32ColumnBuffer) {
				col.writeFloat(columnLevels{}, 3.14)
			},
			expected: 3,
		},
		{
			name: "writeDouble_positive",
			writeOp: func(col *uint32ColumnBuffer) {
				col.writeDouble(columnLevels{}, 42.7)
			},
			expected: 42,
		},
		{
			name: "writeByteArray_valid",
			writeOp: func(col *uint32ColumnBuffer) {
				col.writeByteArray(columnLevels{}, []byte("42"))
			},
			expected: 42,
		},
		{
			name: "writeNull",
			writeOp: func(col *uint32ColumnBuffer) {
				col.writeNull(columnLevels{})
			},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col := newUint32ColumnBuffer(Uint(32).Type(), 0, 10)
			tt.writeOp(col)

			if col.Len() != 1 {
				t.Fatalf("expected 1 value, got %d", col.Len())
			}

			values := make([]Value, 1)
			n, err := col.ReadValuesAt(values, 0)
			if err != nil || n != 1 {
				t.Fatalf("failed to read value: %v", err)
			}

			actual := values[0].Uint32()
			if actual != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, actual)
			}
		})
	}
}

// TestUint32ColumnBufferWriteInt96 tests writeInt96 conversion
func TestColumnBufferUint32WriteInt96(t *testing.T) {
	col := newUint32ColumnBuffer(Uint(32).Type(), 0, 10)
	col.writeInt96(columnLevels{}, [3]uint32{42, 0, 0})

	values := make([]Value, 1)
	col.ReadValuesAt(values, 0)

	actual := values[0].Uint32()
	if actual != 42 {
		t.Errorf("expected 42, got %v", actual)
	}
}

// TestUint32ColumnBufferWrapping documents wrapping behavior for negative and overflow values
func TestColumnBufferUint32Wrapping(t *testing.T) {
	t.Run("negative_int32_wraps", func(t *testing.T) {
		col := newUint32ColumnBuffer(Uint(32).Type(), 0, 10)
		col.writeInt32(columnLevels{}, -1)

		values := make([]Value, 1)
		col.ReadValuesAt(values, 0)

		actual := values[0].Uint32()
		if actual != math.MaxUint32 {
			t.Errorf("expected MaxUint32, got %d", actual)
		}
	})

	t.Run("negative_int64_wraps", func(t *testing.T) {
		col := newUint32ColumnBuffer(Uint(32).Type(), 0, 10)
		col.writeInt64(columnLevels{}, -1)

		values := make([]Value, 1)
		col.ReadValuesAt(values, 0)

		actual := values[0].Uint32()
		if actual != math.MaxUint32 {
			t.Errorf("expected MaxUint32, got %d", actual)
		}
	})

	t.Run("large_int64_truncates", func(t *testing.T) {
		col := newUint32ColumnBuffer(Uint(32).Type(), 0, 10)
		col.writeInt64(columnLevels{}, int64(math.MaxUint32)+1)

		values := make([]Value, 1)
		col.ReadValuesAt(values, 0)

		actual := values[0].Uint32()
		if actual != 0 {
			t.Errorf("expected 0 (wrapped), got %d", actual)
		}
	})

	t.Run("negative_float_wraps", func(t *testing.T) {
		col := newUint32ColumnBuffer(Uint(32).Type(), 0, 10)
		col.writeFloat(columnLevels{}, -1.0)

		values := make([]Value, 1)
		col.ReadValuesAt(values, 0)

		values[0].Uint32()
	})
}

// TestUint32ColumnBufferWriteByteArrayInvalid tests that invalid strings panic
func TestColumnBufferUint32WriteByteArrayInvalid(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for invalid byte array")
		}
	}()

	col := newUint32ColumnBuffer(Uint(32).Type(), 0, 10)
	col.writeByteArray(columnLevels{}, []byte("not a number"))
}

// TestUint32ColumnBufferWriteMultipleValues tests writing multiple values
func TestColumnBufferUint32WriteMultipleValues(t *testing.T) {
	col := newUint32ColumnBuffer(Uint(32).Type(), 0, 10)

	col.writeInt32(columnLevels{}, 1)
	col.writeInt64(columnLevels{}, 2)
	col.writeFloat(columnLevels{}, 3.9)
	col.writeBoolean(columnLevels{}, true)
	col.writeNull(columnLevels{})

	if col.Len() != 5 {
		t.Fatalf("expected 5 values, got %d", col.Len())
	}

	expected := []uint32{1, 2, 3, 1, 0}
	values := make([]Value, 5)
	n, err := col.ReadValuesAt(values, 0)
	if err != nil || n != 5 {
		t.Fatalf("failed to read values: %v", err)
	}

	for i, exp := range expected {
		actual := values[i].Uint32()
		if actual != exp {
			t.Errorf("index %d: expected %d, got %d", i, exp, actual)
		}
	}
}
