package parquet

import (
	"bytes"
	"crypto/sha256"
	"math"
	"testing"
)

// TestBooleanColumnBufferWriteTypes tests that booleanColumnBuffer
// correctly converts various types to boolean representation
func TestColumnBufferBooleanWriteTypes(t *testing.T) {
	tests := []struct {
		name     string
		writeOp  func(*booleanColumnBuffer)
		expected bool
	}{
		{
			name: "writeBoolean_true",
			writeOp: func(col *booleanColumnBuffer) {
				col.writeBoolean(columnLevels{}, true)
			},
			expected: true,
		},
		{
			name: "writeBoolean_false",
			writeOp: func(col *booleanColumnBuffer) {
				col.writeBoolean(columnLevels{}, false)
			},
			expected: false,
		},
		{
			name: "writeInt32_zero",
			writeOp: func(col *booleanColumnBuffer) {
				col.writeInt32(columnLevels{}, 0)
			},
			expected: false,
		},
		{
			name: "writeInt32_nonzero",
			writeOp: func(col *booleanColumnBuffer) {
				col.writeInt32(columnLevels{}, 42)
			},
			expected: true,
		},
		{
			name: "writeInt32_negative",
			writeOp: func(col *booleanColumnBuffer) {
				col.writeInt32(columnLevels{}, -1)
			},
			expected: true,
		},
		{
			name: "writeInt64_zero",
			writeOp: func(col *booleanColumnBuffer) {
				col.writeInt64(columnLevels{}, 0)
			},
			expected: false,
		},
		{
			name: "writeInt64_nonzero",
			writeOp: func(col *booleanColumnBuffer) {
				col.writeInt64(columnLevels{}, 123456)
			},
			expected: true,
		},
		{
			name: "writeFloat_zero",
			writeOp: func(col *booleanColumnBuffer) {
				col.writeFloat(columnLevels{}, 0.0)
			},
			expected: false,
		},
		{
			name: "writeFloat_nonzero",
			writeOp: func(col *booleanColumnBuffer) {
				col.writeFloat(columnLevels{}, 3.14)
			},
			expected: true,
		},
		{
			name: "writeDouble_zero",
			writeOp: func(col *booleanColumnBuffer) {
				col.writeDouble(columnLevels{}, 0.0)
			},
			expected: false,
		},
		{
			name: "writeDouble_nonzero",
			writeOp: func(col *booleanColumnBuffer) {
				col.writeDouble(columnLevels{}, 2.718)
			},
			expected: true,
		},
		{
			name: "writeByteArray_empty",
			writeOp: func(col *booleanColumnBuffer) {
				col.writeByteArray(columnLevels{}, []byte{})
			},
			expected: false,
		},
		{
			name: "writeByteArray_nonempty",
			writeOp: func(col *booleanColumnBuffer) {
				col.writeByteArray(columnLevels{}, []byte("hello"))
			},
			expected: true,
		},
		{
			name: "writeNull",
			writeOp: func(col *booleanColumnBuffer) {
				col.writeNull(columnLevels{})
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col := newBooleanColumnBuffer(BooleanType, 0, 10)
			tt.writeOp(col)

			if col.Len() != 1 {
				t.Fatalf("expected 1 value, got %d", col.Len())
			}

			values := make([]Value, 1)
			n, err := col.ReadValuesAt(values, 0)
			if err != nil || n != 1 {
				t.Fatalf("failed to read value: %v", err)
			}

			actual := values[0].Boolean()
			if actual != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, actual)
			}
		})
	}
}

// TestBooleanColumnBufferWriteInt96 tests writeInt96 conversion
func TestColumnBufferBooleanWriteInt96(t *testing.T) {
	tests := []struct {
		name     string
		value    [3]uint32
		expected bool
	}{
		{
			name:     "zero",
			value:    [3]uint32{0, 0, 0},
			expected: false,
		},
		{
			name:     "nonzero",
			value:    [3]uint32{1, 0, 0},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col := newBooleanColumnBuffer(BooleanType, 0, 10)
			col.writeInt96(columnLevels{}, tt.value)

			if col.Len() != 1 {
				t.Fatalf("expected 1 value, got %d", col.Len())
			}

			values := make([]Value, 1)
			n, err := col.ReadValuesAt(values, 0)
			if err != nil || n != 1 {
				t.Fatalf("failed to read value: %v", err)
			}

			actual := values[0].Boolean()
			if actual != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, actual)
			}
		})
	}
}

// TestBooleanColumnBufferWriteMultipleValues tests writing multiple values
func TestColumnBufferBooleanWriteMultipleValues(t *testing.T) {
	col := newBooleanColumnBuffer(BooleanType, 0, 10)

	col.writeBoolean(columnLevels{}, true)
	col.writeInt32(columnLevels{}, 0)
	col.writeInt64(columnLevels{}, 100)
	col.writeFloat(columnLevels{}, 0.0)
	col.writeDouble(columnLevels{}, 1.5)
	col.writeNull(columnLevels{})

	if col.Len() != 6 {
		t.Fatalf("expected 6 values, got %d", col.Len())
	}

	expected := []bool{true, false, true, false, true, false}
	values := make([]Value, 6)
	n, err := col.ReadValuesAt(values, 0)
	if err != nil || n != 6 {
		t.Fatalf("failed to read values: %v", err)
	}

	for i, exp := range expected {
		actual := values[i].Boolean()
		if actual != exp {
			t.Errorf("index %d: expected %v, got %v", i, exp, actual)
		}
	}
}

// TestBooleanColumnBufferEdgeCases documents edge case behavior
func TestColumnBufferBooleanEdgeCases(t *testing.T) {
	t.Run("max_int32_is_true", func(t *testing.T) {
		col := newBooleanColumnBuffer(BooleanType, 0, 10)
		col.writeInt32(columnLevels{}, math.MaxInt32)

		values := make([]Value, 1)
		col.ReadValuesAt(values, 0)
		if !values[0].Boolean() {
			t.Errorf("expected MaxInt32 to be true")
		}
	})

	t.Run("negative_float_is_true", func(t *testing.T) {
		col := newBooleanColumnBuffer(BooleanType, 0, 10)
		col.writeFloat(columnLevels{}, -0.0001)

		values := make([]Value, 1)
		col.ReadValuesAt(values, 0)
		if !values[0].Boolean() {
			t.Errorf("expected negative float to be true")
		}
	})
}

// TestIssue520BooleanDeterministicTrailingBits verifies that writing a
// non-multiple-of-8 number of booleans produces deterministic bytes even when
// the underlying SliceBuffer carries stale memory from a pool.
// Issue: https://github.com/parquet-go/parquet-go/issues/520
func TestIssue520BooleanDeterministicTrailingBits(t *testing.T) {
	contaminate := func(col *booleanColumnBuffer) {
		col.bits.Grow(8)
		col.bits.Resize(8)
		slice := col.bits.Slice()
		for i := range slice {
			slice[i] = 0xFF
		}
		col.bits.Resize(0)
	}

	t.Run("WriteBooleans", func(t *testing.T) {
		col := newBooleanColumnBuffer(BooleanType, 0, 0)
		contaminate(col)
		if _, err := col.WriteBooleans([]bool{true, true, true, true}); err != nil {
			t.Fatal(err)
		}
		got := col.bits.Slice()
		want := []byte{0x0F}
		if !bytes.Equal(got, want) {
			t.Errorf("bits = % x, want % x", got, want)
		}
	})

	t.Run("writeBoolean", func(t *testing.T) {
		col := newBooleanColumnBuffer(BooleanType, 0, 0)
		contaminate(col)
		for range 4 {
			col.writeBoolean(columnLevels{}, true)
		}
		got := col.bits.Slice()
		want := []byte{0x0F}
		if !bytes.Equal(got, want) {
			t.Errorf("bits = % x, want % x", got, want)
		}
	})
}

// TestIssue520WriterByteDeterminism mirrors the reproducer from the issue:
// the same 4-row boolean column written N times must produce byte-identical
// Parquet files.
func TestIssue520WriterByteDeterminism(t *testing.T) {
	type Row struct {
		N      int32 `parquet:"n"`
		Retain bool  `parquet:"retain"`
	}

	rows := []Row{
		{N: 1, Retain: true},
		{N: 2, Retain: true},
		{N: 3, Retain: true},
		{N: 4, Retain: true},
	}

	n := 500
	if testing.Short() {
		n = 50
	}

	hashes := map[[32]byte]int{}
	for range n {
		var buf bytes.Buffer
		w := NewGenericWriter[Row](&buf)
		if _, err := w.Write(rows); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		hashes[sha256.Sum256(buf.Bytes())]++
	}

	if len(hashes) != 1 {
		t.Errorf("expected 1 distinct hash across %d runs, got %d", n, len(hashes))
		for h, c := range hashes {
			t.Logf("  %x ×%d", h, c)
		}
	}
}

// TestIssue406BooleanDictionaryLookup tests that boolean dictionary lookup correctly
// writes values at the proper offset.
// Issue: https://github.com/parquet-go/parquet-go/issues/406
func TestIssue406BooleanDictionaryLookup(t *testing.T) {
	// Create a boolean dictionary with both true and false values
	boolValues := []byte{0b00000010} // false=0, true=1
	dict := newBooleanDictionary(
		BooleanType,
		0,
		2, // two values: false at index 0, true at index 1
		BooleanType.NewValues(boolValues, nil),
	)

	// Lookup values: index 1 (true), index 0 (false), index 1 (true)
	indexes := []int32{1, 0, 1}
	values := make([]Value, len(indexes))

	// Perform the lookup
	dict.Lookup(indexes, values)

	// Check that the boolean values are correctly read
	expected := []bool{true, false, true}
	for i, exp := range expected {
		got := values[i].Boolean()
		if got != exp {
			t.Errorf("index %d: expected %v, got %v", i, exp, got)
		}
	}
}
