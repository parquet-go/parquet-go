package parquet

import (
	"testing"
)

// TestValuesSliceBufferReserveClearsStaleData tests that the valuesSliceBuffer.reserve
// function properly clears stale slices when reusing capacity from the underlying array.
//
// This is a regression test for issue #406 where stale slice references could corrupt
// data during Schema.Reconstruct when the buffer was reused from the pool.
func TestValuesSliceBufferReserveClearsStaleData(t *testing.T) {
	// Create a buffer with some initial capacity
	buf := &valuesSliceBuffer{
		values: make([][]Value, 0, 10),
	}

	// Simulate first use: populate with some data
	columns := buf.reserve(3)
	staleSlice0 := []Value{{u64: 111}}
	staleSlice1 := []Value{{u64: 222}}
	staleSlice2 := []Value{{u64: 333}}
	columns[0] = staleSlice0
	columns[1] = staleSlice1
	columns[2] = staleSlice2

	// Verify the data is set
	if columns[0] == nil || columns[0][0].u64 != 111 {
		t.Fatal("setup failed: columns[0] not set correctly")
	}

	// Simulate returning to pool: reset length to 0
	buf.values = buf.values[:0]

	// Simulate second use: reserve same number of columns
	// Before the fix, this would return slices with stale data
	columns2 := buf.reserve(3)

	// After the fix, all slices should be nil
	for i, col := range columns2 {
		if col != nil {
			t.Errorf("columns2[%d] should be nil after reserve, got %v", i, col)
		}
	}
}

// TestValuesSliceBufferReserveGrowthClearsData tests that reserve clears data
// even when the requested size equals the capacity exactly.
func TestValuesSliceBufferReserveGrowthClearsData(t *testing.T) {
	// Create buffer with exact capacity
	buf := &valuesSliceBuffer{
		values: make([][]Value, 0, 3),
	}

	// First use
	columns := buf.reserve(3)
	columns[0] = []Value{{u64: 1}}
	columns[1] = []Value{{u64: 2}}
	columns[2] = []Value{{u64: 3}}

	// Reset
	buf.values = buf.values[:0]

	// Second use - reserve exact capacity
	columns2 := buf.reserve(3)

	for i, col := range columns2 {
		if col != nil {
			t.Errorf("columns2[%d] should be nil, got %v", i, col)
		}
	}
}

// TestValuesSliceBufferReservePartialReuse tests that reserve clears data
// when reserving fewer columns than were previously used.
func TestValuesSliceBufferReservePartialReuse(t *testing.T) {
	buf := &valuesSliceBuffer{
		values: make([][]Value, 0, 10),
	}

	// First use with 5 columns
	columns := buf.reserve(5)
	for i := range columns {
		columns[i] = []Value{{u64: uint64(i + 100)}}
	}

	// Reset
	buf.values = buf.values[:0]

	// Second use with only 3 columns
	columns2 := buf.reserve(3)

	// All 3 should be nil
	for i, col := range columns2 {
		if col != nil {
			t.Errorf("columns2[%d] should be nil, got %v", i, col)
		}
	}
}

// TestValuesSliceBufferReserveNewAllocation tests that reserve returns
// zeroed slices when it needs to allocate a new backing array.
func TestValuesSliceBufferReserveNewAllocation(t *testing.T) {
	buf := &valuesSliceBuffer{
		values: make([][]Value, 0, 3),
	}

	// First use
	columns := buf.reserve(3)
	columns[0] = []Value{{u64: 1}}
	columns[1] = []Value{{u64: 2}}
	columns[2] = []Value{{u64: 3}}

	// Reset
	buf.values = buf.values[:0]

	// Request more than capacity - forces new allocation
	columns2 := buf.reserve(5)

	for i, col := range columns2 {
		if col != nil {
			t.Errorf("columns2[%d] should be nil after new allocation, got %v", i, col)
		}
	}
}

// TestIssue406ReconstructWithPoolReuse tests that Schema.Reconstruct correctly
// handles pooled buffer reuse without data corruption.
//
// This is an end-to-end test for issue #406 that verifies boolean fields
// are correctly reconstructed even when the valuesSliceBuffer pool is reused.
func TestIssue406ReconstructWithPoolReuse(t *testing.T) {
	type Row struct {
		Flag  bool
		Count int64
		Name  string
	}

	schema := SchemaOf(Row{})

	// Create test rows with different boolean values
	testCases := []struct {
		row      Row
		parquet  Row // values to reconstruct from
		expected Row
	}{
		{
			row:      Row{},
			parquet:  Row{Flag: true, Count: 1, Name: "first"},
			expected: Row{Flag: true, Count: 1, Name: "first"},
		},
		{
			row:      Row{},
			parquet:  Row{Flag: false, Count: 2, Name: "second"},
			expected: Row{Flag: false, Count: 2, Name: "second"},
		},
		{
			row:      Row{},
			parquet:  Row{Flag: true, Count: 3, Name: "third"},
			expected: Row{Flag: true, Count: 3, Name: "third"},
		},
	}

	// Run multiple iterations to exercise pool reuse
	for iteration := range 5 {
		for i, tc := range testCases {
			// Deconstruct the parquet row into a Row ([]Value)
			parquetRow := schema.Deconstruct(nil, tc.parquet)

			// Reconstruct into the target
			result := tc.row
			if err := schema.Reconstruct(&result, parquetRow); err != nil {
				t.Fatalf("iteration %d, case %d: Reconstruct failed: %v", iteration, i, err)
			}

			// Verify all fields
			if result.Flag != tc.expected.Flag {
				t.Errorf("iteration %d, case %d: Flag mismatch: got %v, want %v",
					iteration, i, result.Flag, tc.expected.Flag)
			}
			if result.Count != tc.expected.Count {
				t.Errorf("iteration %d, case %d: Count mismatch: got %v, want %v",
					iteration, i, result.Count, tc.expected.Count)
			}
			if result.Name != tc.expected.Name {
				t.Errorf("iteration %d, case %d: Name mismatch: got %q, want %q",
					iteration, i, result.Name, tc.expected.Name)
			}
		}
	}
}
