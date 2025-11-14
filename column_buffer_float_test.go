package parquet

import (
	"math"
	"testing"
)

// TestFloatColumnBufferWriteTypes tests that floatColumnBuffer
// correctly converts various types to float32
func TestColumnBufferFloatWriteTypes(t *testing.T) {
	tests := []struct {
		name     string
		writeOp  func(*floatColumnBuffer)
		expected float32
	}{
		{
			name: "writeBoolean_false",
			writeOp: func(col *floatColumnBuffer) {
				col.writeBoolean(columnLevels{}, false)
			},
			expected: 0,
		},
		{
			name: "writeBoolean_true",
			writeOp: func(col *floatColumnBuffer) {
				col.writeBoolean(columnLevels{}, true)
			},
			expected: 1,
		},
		{
			name: "writeInt32_positive",
			writeOp: func(col *floatColumnBuffer) {
				col.writeInt32(columnLevels{}, 42)
			},
			expected: 42,
		},
		{
			name: "writeInt32_negative",
			writeOp: func(col *floatColumnBuffer) {
				col.writeInt32(columnLevels{}, -123)
			},
			expected: -123,
		},
		{
			name: "writeInt64_small",
			writeOp: func(col *floatColumnBuffer) {
				col.writeInt64(columnLevels{}, 12345)
			},
			expected: 12345,
		},
		{
			name: "writeFloat_positive",
			writeOp: func(col *floatColumnBuffer) {
				col.writeFloat(columnLevels{}, 3.14)
			},
			expected: 3.14,
		},
		{
			name: "writeFloat_negative",
			writeOp: func(col *floatColumnBuffer) {
				col.writeFloat(columnLevels{}, -2.5)
			},
			expected: -2.5,
		},
		{
			name: "writeFloat_zero",
			writeOp: func(col *floatColumnBuffer) {
				col.writeFloat(columnLevels{}, 0.0)
			},
			expected: 0,
		},
		{
			name: "writeDouble_small",
			writeOp: func(col *floatColumnBuffer) {
				col.writeDouble(columnLevels{}, 2.718)
			},
			expected: 2.718,
		},
		{
			name: "writeByteArray_valid",
			writeOp: func(col *floatColumnBuffer) {
				col.writeByteArray(columnLevels{}, []byte("3.14"))
			},
			expected: 3.14,
		},
		{
			name: "writeNull",
			writeOp: func(col *floatColumnBuffer) {
				col.writeNull(columnLevels{})
			},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col := newFloatColumnBuffer(FloatType, 0, 10)
			tt.writeOp(col)

			if col.Len() != 1 {
				t.Fatalf("expected 1 value, got %d", col.Len())
			}

			values := make([]Value, 1)
			n, err := col.ReadValuesAt(values, 0)
			if err != nil || n != 1 {
				t.Fatalf("failed to read value: %v", err)
			}

			actual := values[0].Float()
			if actual != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, actual)
			}
		})
	}
}

// TestFloatColumnBufferWriteInt96 tests writeInt96 conversion
func TestColumnBufferFloatWriteInt96(t *testing.T) {
	col := newFloatColumnBuffer(FloatType, 0, 10)
	col.writeInt96(columnLevels{}, [3]uint32{42, 0, 0})

	values := make([]Value, 1)
	col.ReadValuesAt(values, 0)

	actual := values[0].Float()
	if actual != 42 {
		t.Errorf("expected 42, got %v", actual)
	}
}

// TestFloatColumnBufferPrecisionLoss documents precision loss for large values
func TestColumnBufferFloatPrecisionLoss(t *testing.T) {
	t.Run("large_int64", func(t *testing.T) {
		col := newFloatColumnBuffer(FloatType, 0, 10)
		col.writeInt64(columnLevels{}, 123456789012345)

		values := make([]Value, 1)
		col.ReadValuesAt(values, 0)

		values[0].Float()
	})

	t.Run("large_double", func(t *testing.T) {
		col := newFloatColumnBuffer(FloatType, 0, 10)
		col.writeDouble(columnLevels{}, 1.123456789012345)

		values := make([]Value, 1)
		col.ReadValuesAt(values, 0)

		actual := values[0].Float()
		expected := float32(1.123456789012345)
		if actual != expected {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	})
}

// TestFloatColumnBufferWriteByteArrayInvalid tests that invalid strings panic
func TestColumnBufferFloatWriteByteArrayInvalid(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for invalid byte array")
		}
	}()

	col := newFloatColumnBuffer(FloatType, 0, 10)
	col.writeByteArray(columnLevels{}, []byte("not a number"))
}

// TestFloatColumnBufferWriteMultipleValues tests writing multiple values
func TestColumnBufferFloatWriteMultipleValues(t *testing.T) {
	col := newFloatColumnBuffer(FloatType, 0, 10)

	col.writeInt32(columnLevels{}, 1)
	col.writeInt64(columnLevels{}, 2)
	col.writeFloat(columnLevels{}, 3.5)
	col.writeBoolean(columnLevels{}, true)
	col.writeNull(columnLevels{})

	if col.Len() != 5 {
		t.Fatalf("expected 5 values, got %d", col.Len())
	}

	expected := []float32{1, 2, 3.5, 1, 0}
	values := make([]Value, 5)
	n, err := col.ReadValuesAt(values, 0)
	if err != nil || n != 5 {
		t.Fatalf("failed to read values: %v", err)
	}

	for i, exp := range expected {
		actual := values[i].Float()
		if actual != exp {
			t.Errorf("index %d: expected %v, got %v", i, exp, actual)
		}
	}
}

// TestFloatColumnBufferSpecialValues tests special float values
func TestColumnBufferFloatSpecialValues(t *testing.T) {
	tests := []struct {
		name  string
		value float32
	}{
		{"infinity", float32(math.Inf(1))},
		{"negative_infinity", float32(math.Inf(-1))},
		{"nan", float32(math.NaN())},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col := newFloatColumnBuffer(FloatType, 0, 10)
			col.writeFloat(columnLevels{}, tt.value)

			values := make([]Value, 1)
			col.ReadValuesAt(values, 0)

			actual := values[0].Float()
			if tt.name == "nan" {
				if !math.IsNaN(float64(actual)) {
					t.Errorf("expected NaN, got %v", actual)
				}
			} else {
				if actual != tt.value {
					t.Errorf("expected %v, got %v", tt.value, actual)
				}
			}
		})
	}
}
