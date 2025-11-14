package parquet

import (
	"math"
	"testing"
)

// TestDoubleColumnBufferWriteTypes tests that doubleColumnBuffer
// correctly converts various types to float64
func TestColumnBufferDoubleWriteTypes(t *testing.T) {
	tests := []struct {
		name     string
		writeOp  func(*doubleColumnBuffer)
		expected float64
	}{
		{
			name: "writeBoolean_false",
			writeOp: func(col *doubleColumnBuffer) {
				col.writeBoolean(columnLevels{}, false)
			},
			expected: 0,
		},
		{
			name: "writeBoolean_true",
			writeOp: func(col *doubleColumnBuffer) {
				col.writeBoolean(columnLevels{}, true)
			},
			expected: 1,
		},
		{
			name: "writeInt32_positive",
			writeOp: func(col *doubleColumnBuffer) {
				col.writeInt32(columnLevels{}, 42)
			},
			expected: 42,
		},
		{
			name: "writeInt32_negative",
			writeOp: func(col *doubleColumnBuffer) {
				col.writeInt32(columnLevels{}, -123)
			},
			expected: -123,
		},
		{
			name: "writeInt64_large",
			writeOp: func(col *doubleColumnBuffer) {
				col.writeInt64(columnLevels{}, 123456789012345)
			},
			expected: 123456789012345,
		},
		{
			name: "writeFloat_positive",
			writeOp: func(col *doubleColumnBuffer) {
				col.writeFloat(columnLevels{}, 3.14)
			},
			expected: float64(float32(3.14)), // Float32 precision
		},
		{
			name: "writeDouble_positive",
			writeOp: func(col *doubleColumnBuffer) {
				col.writeDouble(columnLevels{}, 2.718281828459045)
			},
			expected: 2.718281828459045,
		},
		{
			name: "writeDouble_negative",
			writeOp: func(col *doubleColumnBuffer) {
				col.writeDouble(columnLevels{}, -1.414213562373095)
			},
			expected: -1.414213562373095,
		},
		{
			name: "writeDouble_zero",
			writeOp: func(col *doubleColumnBuffer) {
				col.writeDouble(columnLevels{}, 0.0)
			},
			expected: 0,
		},
		{
			name: "writeByteArray_valid",
			writeOp: func(col *doubleColumnBuffer) {
				col.writeByteArray(columnLevels{}, []byte("3.14159"))
			},
			expected: 3.14159,
		},
		{
			name: "writeNull",
			writeOp: func(col *doubleColumnBuffer) {
				col.writeNull(columnLevels{})
			},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col := newDoubleColumnBuffer(DoubleType, 0, 10)
			tt.writeOp(col)

			if col.Len() != 1 {
				t.Fatalf("expected 1 value, got %d", col.Len())
			}

			values := make([]Value, 1)
			n, err := col.ReadValuesAt(values, 0)
			if err != nil || n != 1 {
				t.Fatalf("failed to read value: %v", err)
			}

			actual := values[0].Double()
			if actual != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, actual)
			}
		})
	}
}

// TestDoubleColumnBufferWriteInt96 tests writeInt96 conversion
func TestColumnBufferDoubleWriteInt96(t *testing.T) {
	col := newDoubleColumnBuffer(DoubleType, 0, 10)
	col.writeInt96(columnLevels{}, [3]uint32{42, 0, 0})

	values := make([]Value, 1)
	col.ReadValuesAt(values, 0)

	actual := values[0].Double()
	if actual != 42 {
		t.Errorf("expected 42, got %v", actual)
	}
}

// TestDoubleColumnBufferPrecisionLoss documents precision loss for very large int64
func TestColumnBufferDoublePrecisionLoss(t *testing.T) {
	t.Run("very_large_int64", func(t *testing.T) {
		col := newDoubleColumnBuffer(DoubleType, 0, 10)
		largeInt := int64(math.MaxInt64)
		col.writeInt64(columnLevels{}, largeInt)

		values := make([]Value, 1)
		col.ReadValuesAt(values, 0)

		values[0].Double()
	})
}

// TestDoubleColumnBufferWriteByteArrayInvalid tests that invalid strings panic
func TestColumnBufferDoubleWriteByteArrayInvalid(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for invalid byte array")
		}
	}()

	col := newDoubleColumnBuffer(DoubleType, 0, 10)
	col.writeByteArray(columnLevels{}, []byte("not a number"))
}

// TestDoubleColumnBufferWriteMultipleValues tests writing multiple values
func TestColumnBufferDoubleWriteMultipleValues(t *testing.T) {
	col := newDoubleColumnBuffer(DoubleType, 0, 10)

	col.writeInt32(columnLevels{}, 1)
	col.writeInt64(columnLevels{}, 2)
	col.writeFloat(columnLevels{}, 3.5)
	col.writeDouble(columnLevels{}, 4.5)
	col.writeBoolean(columnLevels{}, true)
	col.writeNull(columnLevels{})

	if col.Len() != 6 {
		t.Fatalf("expected 6 values, got %d", col.Len())
	}

	expected := []float64{1, 2, float64(float32(3.5)), 4.5, 1, 0}
	values := make([]Value, 6)
	n, err := col.ReadValuesAt(values, 0)
	if err != nil || n != 6 {
		t.Fatalf("failed to read values: %v", err)
	}

	for i, exp := range expected {
		actual := values[i].Double()
		if actual != exp {
			t.Errorf("index %d: expected %v, got %v", i, exp, actual)
		}
	}
}

// TestDoubleColumnBufferSpecialValues tests special float values
func TestColumnBufferDoubleSpecialValues(t *testing.T) {
	tests := []struct {
		name  string
		value float64
	}{
		{"infinity", math.Inf(1)},
		{"negative_infinity", math.Inf(-1)},
		{"nan", math.NaN()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col := newDoubleColumnBuffer(DoubleType, 0, 10)
			col.writeDouble(columnLevels{}, tt.value)

			values := make([]Value, 1)
			col.ReadValuesAt(values, 0)

			actual := values[0].Double()
			if tt.name == "nan" {
				if !math.IsNaN(actual) {
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
