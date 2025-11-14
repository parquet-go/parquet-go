package parquet

import (
	"math"
	"testing"

	"github.com/parquet-go/parquet-go/deprecated"
)

// TestInt96ColumnBufferWriteTypes tests that int96ColumnBuffer
// correctly converts various types to int96
func TestColumnBufferInt96WriteTypes(t *testing.T) {
	tests := []struct {
		name     string
		writeOp  func(*int96ColumnBuffer)
		expected deprecated.Int96
	}{
		{
			name: "writeBoolean_false",
			writeOp: func(col *int96ColumnBuffer) {
				col.writeBoolean(columnLevels{}, false)
			},
			expected: deprecated.Int96{0, 0, 0},
		},
		{
			name: "writeBoolean_true",
			writeOp: func(col *int96ColumnBuffer) {
				col.writeBoolean(columnLevels{}, true)
			},
			expected: deprecated.Int96{1, 0, 0},
		},
		{
			name: "writeInt32_positive",
			writeOp: func(col *int96ColumnBuffer) {
				col.writeInt32(columnLevels{}, 42)
			},
			expected: deprecated.Int32ToInt96(42),
		},
		{
			name: "writeInt32_negative",
			writeOp: func(col *int96ColumnBuffer) {
				col.writeInt32(columnLevels{}, -123)
			},
			expected: deprecated.Int32ToInt96(-123),
		},
		{
			name: "writeInt32_zero",
			writeOp: func(col *int96ColumnBuffer) {
				col.writeInt32(columnLevels{}, 0)
			},
			expected: deprecated.Int96{0, 0, 0},
		},
		{
			name: "writeInt32_max",
			writeOp: func(col *int96ColumnBuffer) {
				col.writeInt32(columnLevels{}, math.MaxInt32)
			},
			expected: deprecated.Int32ToInt96(math.MaxInt32),
		},
		{
			name: "writeInt32_min",
			writeOp: func(col *int96ColumnBuffer) {
				col.writeInt32(columnLevels{}, math.MinInt32)
			},
			expected: deprecated.Int32ToInt96(math.MinInt32),
		},
		{
			name: "writeInt64_positive",
			writeOp: func(col *int96ColumnBuffer) {
				col.writeInt64(columnLevels{}, 12345)
			},
			expected: deprecated.Int64ToInt96(12345),
		},
		{
			name: "writeInt64_negative",
			writeOp: func(col *int96ColumnBuffer) {
				col.writeInt64(columnLevels{}, -9876)
			},
			expected: deprecated.Int64ToInt96(-9876),
		},
		{
			name: "writeInt64_max",
			writeOp: func(col *int96ColumnBuffer) {
				col.writeInt64(columnLevels{}, math.MaxInt64)
			},
			expected: deprecated.Int64ToInt96(math.MaxInt64),
		},
		{
			name: "writeInt64_min",
			writeOp: func(col *int96ColumnBuffer) {
				col.writeInt64(columnLevels{}, math.MinInt64)
			},
			expected: deprecated.Int64ToInt96(math.MinInt64),
		},
		{
			name: "writeInt96",
			writeOp: func(col *int96ColumnBuffer) {
				col.writeInt96(columnLevels{}, deprecated.Int96{0x12345678, 0xABCDEF01, 0x87654321})
			},
			expected: deprecated.Int96{0x12345678, 0xABCDEF01, 0x87654321},
		},
		{
			name: "writeFloat_positive",
			writeOp: func(col *int96ColumnBuffer) {
				col.writeFloat(columnLevels{}, 3.14)
			},
			expected: deprecated.Int64ToInt96(3),
		},
		{
			name: "writeFloat_negative",
			writeOp: func(col *int96ColumnBuffer) {
				col.writeFloat(columnLevels{}, -2.9)
			},
			expected: deprecated.Int64ToInt96(-2),
		},
		{
			name: "writeDouble_positive",
			writeOp: func(col *int96ColumnBuffer) {
				col.writeDouble(columnLevels{}, 42.7)
			},
			expected: deprecated.Int64ToInt96(42),
		},
		{
			name: "writeDouble_negative",
			writeOp: func(col *int96ColumnBuffer) {
				col.writeDouble(columnLevels{}, -100.5)
			},
			expected: deprecated.Int64ToInt96(-100),
		},
		{
			name: "writeByteArray_valid_positive",
			writeOp: func(col *int96ColumnBuffer) {
				col.writeByteArray(columnLevels{}, []byte("42"))
			},
			expected: deprecated.Int64ToInt96(42),
		},
		{
			name: "writeByteArray_valid_negative",
			writeOp: func(col *int96ColumnBuffer) {
				col.writeByteArray(columnLevels{}, []byte("-999"))
			},
			expected: deprecated.Int64ToInt96(-999),
		},
		{
			name: "writeByteArray_zero",
			writeOp: func(col *int96ColumnBuffer) {
				col.writeByteArray(columnLevels{}, []byte("0"))
			},
			expected: deprecated.Int96{0, 0, 0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col := newInt96ColumnBuffer(Int96Type, 0, 10)
			tt.writeOp(col)

			if col.Len() != 1 {
				t.Fatalf("expected 1 value, got %d", col.Len())
			}

			if col.values[0] != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, col.values[0])
			}
		})
	}
}

// TestInt96ColumnBufferWriteByteArrayInvalid tests that invalid strings panic
func TestColumnBufferInt96WriteByteArrayInvalid(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for invalid byte array")
		}
	}()

	col := newInt96ColumnBuffer(Int96Type, 0, 10)
	col.writeByteArray(columnLevels{}, []byte("not a number"))
}

// TestInt96ColumnBufferWriteMultipleValues tests writing multiple values
func TestColumnBufferInt96WriteMultipleValues(t *testing.T) {
	col := newInt96ColumnBuffer(Int96Type, 0, 10)

	col.writeInt32(columnLevels{}, 1)
	col.writeInt64(columnLevels{}, 2)
	col.writeFloat(columnLevels{}, 3.9)
	col.writeBoolean(columnLevels{}, true)
	col.writeInt96(columnLevels{}, deprecated.Int96{42, 0, 0})

	if col.Len() != 5 {
		t.Fatalf("expected 5 values, got %d", col.Len())
	}

	expected := []deprecated.Int96{
		deprecated.Int32ToInt96(1),
		deprecated.Int64ToInt96(2),
		deprecated.Int64ToInt96(3),
		deprecated.Int96{1, 0, 0},
		deprecated.Int96{42, 0, 0},
	}

	for i, exp := range expected {
		if col.values[i] != exp {
			t.Errorf("index %d: expected %v, got %v", i, exp, col.values[i])
		}
	}
}

// TestInt96ColumnBufferWriteNull tests that writeNull still panics
func TestColumnBufferInt96WriteNull(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for writeNull")
		}
	}()

	col := newInt96ColumnBuffer(Int96Type, 0, 10)
	col.writeNull(columnLevels{})
}

// TestInt96ColumnBufferPrecisionLoss documents precision loss for large values
func TestColumnBufferInt96PrecisionLoss(t *testing.T) {
	t.Run("large_float", func(t *testing.T) {
		col := newInt96ColumnBuffer(Int96Type, 0, 10)
		col.writeFloat(columnLevels{}, 1e10)

		// Float32 loses precision when converted to int64
		// This test just documents the behavior
		_ = col.values[0]
	})

	t.Run("large_double", func(t *testing.T) {
		col := newInt96ColumnBuffer(Int96Type, 0, 10)
		col.writeDouble(columnLevels{}, 1e15)

		// Conversion to int64 may lose precision for very large doubles
		_ = col.values[0]
	})
}

// TestInt96ColumnBufferReadWriteRoundTrip tests round-trip conversion
func TestColumnBufferInt96ReadWriteRoundTrip(t *testing.T) {
	col := newInt96ColumnBuffer(Int96Type, 0, 10)

	original := deprecated.Int96{0x12345678, 0xABCDEF01, 0x87654321}
	col.writeInt96(columnLevels{}, original)

	if col.Len() != 1 {
		t.Fatalf("expected 1 value, got %d", col.Len())
	}

	values := make([]Value, 1)
	n, err := col.ReadValuesAt(values, 0)
	if err != nil || n != 1 {
		t.Fatalf("failed to read value: %v", err)
	}

	result := values[0].Int96()
	if result != original {
		t.Errorf("round-trip failed: expected %v, got %v", original, result)
	}
}

// TestInt96ColumnBufferSignedConversion tests signed int conversions
func TestColumnBufferInt96SignedConversion(t *testing.T) {
	tests := []struct {
		name  string
		write func(*int96ColumnBuffer)
	}{
		{
			name: "negative_int32",
			write: func(col *int96ColumnBuffer) {
				col.writeInt32(columnLevels{}, -42)
			},
		},
		{
			name: "negative_int64",
			write: func(col *int96ColumnBuffer) {
				col.writeInt64(columnLevels{}, -12345)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col := newInt96ColumnBuffer(Int96Type, 0, 10)
			tt.write(col)

			// Just verify that negative values are handled
			if col.Len() != 1 {
				t.Fatalf("expected 1 value, got %d", col.Len())
			}

			// The value should be stored correctly
			_ = col.values[0]
		})
	}
}
