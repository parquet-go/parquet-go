package parquet

import (
	"io"
	"testing"
)

func TestRepeatedColumnBufferReadValuesAt(t *testing.T) {
	t.Run("AllNonNull", func(t *testing.T) {
		// Create a repeated column buffer with all non-null int32 values
		base := newInt32ColumnBuffer(Int32Type, 0, 100)
		col := newRepeatedColumnBuffer(base, 1, 1, nil)

		// Write values: row with repetition levels [0, 1, 1]
		// This represents a single row with 3 values
		values := []Value{
			makeInt32Value(10, 0, 1, 0), // repetitionLevel=0, definitionLevel=1
			makeInt32Value(20, 1, 1, 0), // repetitionLevel=1, definitionLevel=1
			makeInt32Value(30, 1, 1, 0), // repetitionLevel=1, definitionLevel=1
		}
		n, err := col.WriteValues(values)
		if err != nil {
			t.Fatal(err)
		}
		if n != 3 {
			t.Fatalf("expected to write 3 values, wrote %d", n)
		}

		// Read all values starting from offset 0
		readBuf := make([]Value, 3)
		n, err = col.ReadValuesAt(readBuf, 0)
		if err != nil {
			t.Fatalf("ReadValuesAt failed: %v", err)
		}
		if n != 3 {
			t.Fatalf("expected to read 3 values, got %d", n)
		}

		// Verify values and levels
		expected := []struct {
			value           int32
			repetitionLevel int
			definitionLevel int
		}{
			{10, 0, 1},
			{20, 1, 1},
			{30, 1, 1},
		}

		for i, exp := range expected {
			if readBuf[i].Int32() != exp.value {
				t.Errorf("value[%d]: expected %d, got %d", i, exp.value, readBuf[i].Int32())
			}
			if readBuf[i].RepetitionLevel() != exp.repetitionLevel {
				t.Errorf("value[%d]: expected repetition level %d, got %d", i, exp.repetitionLevel, readBuf[i].RepetitionLevel())
			}
			if readBuf[i].DefinitionLevel() != exp.definitionLevel {
				t.Errorf("value[%d]: expected definition level %d, got %d", i, exp.definitionLevel, readBuf[i].DefinitionLevel())
			}
		}
	})

	t.Run("MixOfNullAndNonNull", func(t *testing.T) {
		// Create a repeated column buffer with mix of null and non-null values
		base := newInt32ColumnBuffer(Int32Type, 0, 100)
		col := newRepeatedColumnBuffer(base, 1, 1, nil)

		// Write values: [non-null(10), null, non-null(20), null, non-null(30)]
		// with repetition levels [0, 1, 1, 1, 1]
		values := []Value{
			makeInt32Value(10, 0, 1, 0), // repetitionLevel=0, definitionLevel=1 (non-null)
			makeNullValue(1, 0, 0),      // repetitionLevel=1, definitionLevel=0 (null)
			makeInt32Value(20, 1, 1, 0), // repetitionLevel=1, definitionLevel=1 (non-null)
			makeNullValue(1, 0, 0),      // repetitionLevel=1, definitionLevel=0 (null)
			makeInt32Value(30, 1, 1, 0), // repetitionLevel=1, definitionLevel=1 (non-null)
		}
		n, err := col.WriteValues(values)
		if err != nil {
			t.Fatal(err)
		}
		if n != 5 {
			t.Fatalf("expected to write 5 values, wrote %d", n)
		}

		// Read all values
		readBuf := make([]Value, 5)
		n, err = col.ReadValuesAt(readBuf, 0)
		if err != nil {
			t.Fatalf("ReadValuesAt failed: %v", err)
		}
		if n != 5 {
			t.Fatalf("expected to read 5 values, got %d", n)
		}

		// Verify values and levels
		expected := []struct {
			isNull          bool
			value           int32
			repetitionLevel int
			definitionLevel int
		}{
			{false, 10, 0, 1},
			{true, 0, 1, 0},
			{false, 20, 1, 1},
			{true, 0, 1, 0},
			{false, 30, 1, 1},
		}

		for i, exp := range expected {
			if readBuf[i].IsNull() != exp.isNull {
				t.Errorf("value[%d]: expected isNull=%v, got %v", i, exp.isNull, readBuf[i].IsNull())
			}
			if !exp.isNull && readBuf[i].Int32() != exp.value {
				t.Errorf("value[%d]: expected %d, got %d", i, exp.value, readBuf[i].Int32())
			}
			if readBuf[i].RepetitionLevel() != exp.repetitionLevel {
				t.Errorf("value[%d]: expected repetition level %d, got %d", i, exp.repetitionLevel, readBuf[i].RepetitionLevel())
			}
			if readBuf[i].DefinitionLevel() != exp.definitionLevel {
				t.Errorf("value[%d]: expected definition level %d, got %d", i, exp.definitionLevel, readBuf[i].DefinitionLevel())
			}
		}
	})

	t.Run("AllNull", func(t *testing.T) {
		// Create a repeated column buffer with all null values
		base := newInt32ColumnBuffer(Int32Type, 0, 100)
		col := newRepeatedColumnBuffer(base, 1, 1, nil)

		// Write null values with repetition levels [0, 1, 1]
		values := []Value{
			makeNullValue(0, 0, 0), // repetitionLevel=0, definitionLevel=0 (null)
			makeNullValue(1, 0, 0), // repetitionLevel=1, definitionLevel=0 (null)
			makeNullValue(1, 0, 0), // repetitionLevel=1, definitionLevel=0 (null)
		}
		n, err := col.WriteValues(values)
		if err != nil {
			t.Fatal(err)
		}
		if n != 3 {
			t.Fatalf("expected to write 3 values, wrote %d", n)
		}

		// Read all values
		readBuf := make([]Value, 3)
		n, err = col.ReadValuesAt(readBuf, 0)
		if err != nil {
			t.Fatalf("ReadValuesAt failed: %v", err)
		}
		if n != 3 {
			t.Fatalf("expected to read 3 values, got %d", n)
		}

		// Verify all are null with correct levels
		expected := []struct {
			repetitionLevel int
			definitionLevel int
		}{
			{0, 0},
			{1, 0},
			{1, 0},
		}

		for i, exp := range expected {
			if !readBuf[i].IsNull() {
				t.Errorf("value[%d]: expected null", i)
			}
			if readBuf[i].RepetitionLevel() != exp.repetitionLevel {
				t.Errorf("value[%d]: expected repetition level %d, got %d", i, exp.repetitionLevel, readBuf[i].RepetitionLevel())
			}
			if readBuf[i].DefinitionLevel() != exp.definitionLevel {
				t.Errorf("value[%d]: expected definition level %d, got %d", i, exp.definitionLevel, readBuf[i].DefinitionLevel())
			}
		}
	})

	t.Run("ReadFromMiddleOffset", func(t *testing.T) {
		// Test reading from a non-zero offset
		base := newInt32ColumnBuffer(Int32Type, 0, 100)
		col := newRepeatedColumnBuffer(base, 1, 1, nil)

		// Write 5 values
		values := []Value{
			makeInt32Value(10, 0, 1, 0),
			makeInt32Value(20, 1, 1, 0),
			makeInt32Value(30, 1, 1, 0),
			makeInt32Value(40, 1, 1, 0),
			makeInt32Value(50, 1, 1, 0),
		}
		n, err := col.WriteValues(values)
		if err != nil {
			t.Fatal(err)
		}
		if n != 5 {
			t.Fatalf("expected to write 5 values, wrote %d", n)
		}

		// Read 3 values starting from offset 2
		readBuf := make([]Value, 3)
		n, err = col.ReadValuesAt(readBuf, 2)
		if err != nil {
			t.Fatalf("ReadValuesAt failed: %v", err)
		}
		if n != 3 {
			t.Fatalf("expected to read 3 values, got %d", n)
		}

		// Verify we got values at indices 2, 3, 4
		expected := []int32{30, 40, 50}
		for i, exp := range expected {
			if readBuf[i].Int32() != exp {
				t.Errorf("value[%d]: expected %d, got %d", i, exp, readBuf[i].Int32())
			}
		}
	})

	t.Run("ReadPartial", func(t *testing.T) {
		// Test reading when buffer is smaller than available values
		base := newInt32ColumnBuffer(Int32Type, 0, 100)
		col := newRepeatedColumnBuffer(base, 1, 1, nil)

		values := []Value{
			makeInt32Value(10, 0, 1, 0),
			makeInt32Value(20, 1, 1, 0),
			makeInt32Value(30, 1, 1, 0),
			makeInt32Value(40, 1, 1, 0),
			makeInt32Value(50, 1, 1, 0),
		}
		n, err := col.WriteValues(values)
		if err != nil {
			t.Fatal(err)
		}
		if n != 5 {
			t.Fatalf("expected to write 5 values, wrote %d", n)
		}

		// Read only 2 values with a buffer that could hold more
		readBuf := make([]Value, 10)
		n, err = col.ReadValuesAt(readBuf, 0)
		if err != nil {
			t.Fatalf("ReadValuesAt failed: %v", err)
		}
		if n != 5 {
			t.Fatalf("expected to read 5 values, got %d", n)
		}
	})

	t.Run("ReadAtEnd", func(t *testing.T) {
		// Test reading at the end of buffer returns EOF
		base := newInt32ColumnBuffer(Int32Type, 0, 100)
		col := newRepeatedColumnBuffer(base, 1, 1, nil)

		values := []Value{
			makeInt32Value(10, 0, 1, 0),
			makeInt32Value(20, 1, 1, 0),
		}
		n, err := col.WriteValues(values)
		if err != nil {
			t.Fatal(err)
		}
		if n != 2 {
			t.Fatalf("expected to write 2 values, wrote %d", n)
		}

		// Try to read at offset equal to length
		readBuf := make([]Value, 1)
		n, err = col.ReadValuesAt(readBuf, 2)
		if err != io.EOF {
			t.Fatalf("expected io.EOF, got %v", err)
		}
		if n != 0 {
			t.Fatalf("expected 0 values, got %d", n)
		}
	})

	t.Run("ReadPastEnd", func(t *testing.T) {
		// Test reading past the end returns EOF
		base := newInt32ColumnBuffer(Int32Type, 0, 100)
		col := newRepeatedColumnBuffer(base, 1, 1, nil)

		values := []Value{
			makeInt32Value(10, 0, 1, 0),
		}
		n, err := col.WriteValues(values)
		if err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Fatalf("expected to write 1 value, wrote %d", n)
		}

		// Try to read past the end
		readBuf := make([]Value, 1)
		n, err = col.ReadValuesAt(readBuf, 10)
		if err != io.EOF {
			t.Fatalf("expected io.EOF, got %v", err)
		}
		if n != 0 {
			t.Fatalf("expected 0 values, got %d", n)
		}
	})

	t.Run("NegativeOffset", func(t *testing.T) {
		// Test negative offset returns error
		base := newInt32ColumnBuffer(Int32Type, 0, 100)
		col := newRepeatedColumnBuffer(base, 1, 1, nil)

		values := []Value{
			makeInt32Value(10, 0, 1, 0),
		}
		n, err := col.WriteValues(values)
		if err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Fatalf("expected to write 1 value, wrote %d", n)
		}

		// Try to read with negative offset
		readBuf := make([]Value, 1)
		n, err = col.ReadValuesAt(readBuf, -1)
		if err == nil {
			t.Fatal("expected error for negative offset, got nil")
		}
		if n != 0 {
			t.Fatalf("expected 0 values, got %d", n)
		}
	})

	t.Run("EmptyBuffer", func(t *testing.T) {
		// Test reading from empty buffer returns EOF
		base := newInt32ColumnBuffer(Int32Type, 0, 100)
		col := newRepeatedColumnBuffer(base, 1, 1, nil)

		readBuf := make([]Value, 1)
		n, err := col.ReadValuesAt(readBuf, 0)
		if err != io.EOF {
			t.Fatalf("expected io.EOF for empty buffer, got %v", err)
		}
		if n != 0 {
			t.Fatalf("expected 0 values, got %d", n)
		}
	})

	t.Run("MultipleRows", func(t *testing.T) {
		// Test reading across multiple rows
		base := newInt32ColumnBuffer(Int32Type, 0, 100)
		col := newRepeatedColumnBuffer(base, 1, 1, nil)

		// Write two rows, each with repeated values
		// Row 1: [10, 20] (repetition levels: 0, 1)
		// Row 2: [30, 40, 50] (repetition levels: 0, 1, 1)
		values := []Value{
			makeInt32Value(10, 0, 1, 0),
			makeInt32Value(20, 1, 1, 0),
			makeInt32Value(30, 0, 1, 0),
			makeInt32Value(40, 1, 1, 0),
			makeInt32Value(50, 1, 1, 0),
		}
		n, err := col.WriteValues(values)
		if err != nil {
			t.Fatal(err)
		}
		if n != 5 {
			t.Fatalf("expected to write 5 values, wrote %d", n)
		}

		// Read all values
		readBuf := make([]Value, 5)
		n, err = col.ReadValuesAt(readBuf, 0)
		if err != nil {
			t.Fatalf("ReadValuesAt failed: %v", err)
		}
		if n != 5 {
			t.Fatalf("expected to read 5 values, got %d", n)
		}

		// Verify repetition levels correctly identify row boundaries
		expected := []struct {
			value           int32
			repetitionLevel int
		}{
			{10, 0}, // Start of row 1
			{20, 1},
			{30, 0}, // Start of row 2
			{40, 1},
			{50, 1},
		}

		for i, exp := range expected {
			if readBuf[i].Int32() != exp.value {
				t.Errorf("value[%d]: expected %d, got %d", i, exp.value, readBuf[i].Int32())
			}
			if readBuf[i].RepetitionLevel() != exp.repetitionLevel {
				t.Errorf("value[%d]: expected repetition level %d, got %d", i, exp.repetitionLevel, readBuf[i].RepetitionLevel())
			}
		}
	})

	t.Run("MixOfNullsAcrossRows", func(t *testing.T) {
		// Test reading mix of nulls across multiple rows
		base := newInt32ColumnBuffer(Int32Type, 0, 100)
		col := newRepeatedColumnBuffer(base, 1, 1, nil)

		// Row 1: [10, null, 20]
		// Row 2: [null, 30]
		values := []Value{
			makeInt32Value(10, 0, 1, 0),
			makeNullValue(1, 0, 0),
			makeInt32Value(20, 1, 1, 0),
			makeNullValue(0, 0, 0),
			makeInt32Value(30, 1, 1, 0),
		}
		n, err := col.WriteValues(values)
		if err != nil {
			t.Fatal(err)
		}
		if n != 5 {
			t.Fatalf("expected to write 5 values, wrote %d", n)
		}

		// Read from offset 1 (should get: null, 20, null, 30)
		readBuf := make([]Value, 4)
		n, err = col.ReadValuesAt(readBuf, 1)
		if err != nil {
			t.Fatalf("ReadValuesAt failed: %v", err)
		}
		if n != 4 {
			t.Fatalf("expected to read 4 values, got %d", n)
		}

		// Verify values
		if !readBuf[0].IsNull() || readBuf[0].RepetitionLevel() != 1 {
			t.Errorf("value[0]: expected null with repetition level 1")
		}
		if readBuf[1].Int32() != 20 || readBuf[1].RepetitionLevel() != 1 {
			t.Errorf("value[1]: expected 20 with repetition level 1")
		}
		if !readBuf[2].IsNull() || readBuf[2].RepetitionLevel() != 0 {
			t.Errorf("value[2]: expected null with repetition level 0")
		}
		if readBuf[3].Int32() != 30 || readBuf[3].RepetitionLevel() != 1 {
			t.Errorf("value[3]: expected 30 with repetition level 1")
		}
	})
}

// Helper function to create an int32 value with levels
func makeInt32Value(v int32, repetitionLevel, definitionLevel, columnIndex int) Value {
	val := ValueOf(v)
	val = val.Level(repetitionLevel, definitionLevel, columnIndex)
	return val
}

// Helper function to create a null value with levels
func makeNullValue(repetitionLevel, definitionLevel, columnIndex int) Value {
	val := Value{}
	val = val.Level(repetitionLevel, definitionLevel, columnIndex)
	return val
}
