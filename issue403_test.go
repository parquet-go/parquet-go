package parquet_test

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/parquet-go/parquet-go"
)

// TestIssue403 tests that optional nested structs followed by boolean fields
// are correctly read back. See https://github.com/parquet-go/parquet-go/issues/403
//
// The issue reported that when an optional nested struct field is followed by a
// boolean field, the boolean always reads as false. This test verifies the
// correct behavior with various combinations of nil/non-nil optional fields
// and true/false boolean values.
func TestIssue403(t *testing.T) {
	type TrackedExtra struct {
		ID int `parquet:"id"`
	}

	type Frame struct {
		TrackedExtra       *TrackedExtra `parquet:"extra,optional"`
		TrackedExtraExists bool          `parquet:"extra_exists"`
	}

	// Test case 1: optional field is non-nil, boolean is true
	t.Run("non-nil optional with true bool", func(t *testing.T) {
		buf := new(bytes.Buffer)
		w := parquet.NewGenericWriter[Frame](buf)

		expect := []Frame{
			{TrackedExtra: &TrackedExtra{ID: 1}, TrackedExtraExists: true},
			{TrackedExtra: &TrackedExtra{ID: 2}, TrackedExtraExists: true},
			{TrackedExtra: &TrackedExtra{ID: 3}, TrackedExtraExists: true},
		}

		if _, err := w.Write(expect); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}

		r := parquet.NewGenericReader[Frame](bytes.NewReader(buf.Bytes()))
		values := make([]Frame, len(expect))
		if n, err := r.Read(values); n != len(expect) {
			t.Fatalf("expected to read %d values, got %d: %v", len(expect), n, err)
		}

		if !reflect.DeepEqual(expect, values) {
			t.Errorf("value mismatch:\nwant: %+v\ngot:  %+v", expect, values)
		}
	})

	// Test case 2: optional field is nil, boolean is true
	t.Run("nil optional with true bool", func(t *testing.T) {
		buf := new(bytes.Buffer)
		w := parquet.NewGenericWriter[Frame](buf)

		expect := []Frame{
			{TrackedExtra: nil, TrackedExtraExists: true},
			{TrackedExtra: nil, TrackedExtraExists: true},
			{TrackedExtra: nil, TrackedExtraExists: true},
		}

		if _, err := w.Write(expect); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}

		r := parquet.NewGenericReader[Frame](bytes.NewReader(buf.Bytes()))
		values := make([]Frame, len(expect))
		if n, err := r.Read(values); n != len(expect) {
			t.Fatalf("expected to read %d values, got %d: %v", len(expect), n, err)
		}

		if !reflect.DeepEqual(expect, values) {
			t.Errorf("value mismatch:\nwant: %+v\ngot:  %+v", expect, values)
		}
	})

	// Test case 3: mixed nil/non-nil optional with mixed booleans
	t.Run("mixed optional and bool values", func(t *testing.T) {
		buf := new(bytes.Buffer)
		w := parquet.NewGenericWriter[Frame](buf)

		expect := []Frame{
			{TrackedExtra: &TrackedExtra{ID: 1}, TrackedExtraExists: true},
			{TrackedExtra: nil, TrackedExtraExists: false},
			{TrackedExtra: &TrackedExtra{ID: 2}, TrackedExtraExists: true},
			{TrackedExtra: nil, TrackedExtraExists: true},
		}

		if _, err := w.Write(expect); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}

		r := parquet.NewGenericReader[Frame](bytes.NewReader(buf.Bytes()))
		values := make([]Frame, len(expect))
		if n, err := r.Read(values); n != len(expect) {
			t.Fatalf("expected to read %d values, got %d: %v", len(expect), n, err)
		}

		if !reflect.DeepEqual(expect, values) {
			t.Errorf("value mismatch:\nwant: %+v\ngot:  %+v", expect, values)
		}
	})
}
