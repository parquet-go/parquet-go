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

func GetExtraSchema() *parquet.Schema {
	extraSchema := parquet.Group{
		"id": parquet.Required(parquet.Int(8)),
	}
	schema := parquet.NewSchema("schema", parquet.Group{
		"extra_exists": parquet.Required(parquet.Leaf(parquet.BooleanType)),
		"extra":        parquet.Optional(extraSchema),
	})
	return schema
}

func TestIssue403(t *testing.T) {
	type TrackedExtra struct {
		ID int `parquet:"id"`
	}

	type Frame struct {
		TrackedExtra       *TrackedExtra `parquet:"extra,optional"`
		TrackedExtraExists bool          `parquet:"extra_exists"`
	}

	type FramePtr struct {
		TrackedExtra       *TrackedExtra `parquet:"extra,optional"`
		TrackedExtraExists *bool         `parquet:"extra_exists"`
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

	// Test case 4: pass schema to writer
	t.Run("using schema on write", func(t *testing.T) {
		buf := new(bytes.Buffer)
		config := parquet.DefaultWriterConfig()
		config.Apply(GetExtraSchema())
		w := parquet.NewGenericWriter[Frame](buf, config)

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

	// Test case 5: pass schema to writer and reader, panic
	t.Run("using schema on read and write", func(t *testing.T) {
		buf := new(bytes.Buffer)
		config := parquet.DefaultWriterConfig()
		config.Apply(GetExtraSchema())
		w := parquet.NewGenericWriter[Frame](buf, config)

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

		rconfig := parquet.DefaultReaderConfig()
		rconfig.Apply(GetExtraSchema())
		r := parquet.NewGenericReader[Frame](bytes.NewReader(buf.Bytes()), rconfig)
		values := make([]Frame, len(expect))
		if n, err := r.Read(values); n != len(expect) {
			t.Fatalf("expected to read %d values, got %d: %v", len(expect), n, err)
		}

		if !reflect.DeepEqual(expect, values) {
			t.Errorf("value mismatch:\nwant: %+v\ngot:  %+v", expect, values)
		}
	})

	// Test case 6: pass schema to writer and reader, with bool-ptr and using NewReader
	t.Run("using bool ptr, NewReader and schema on read and write", func(t *testing.T) {
		buf := new(bytes.Buffer)
		config := parquet.DefaultWriterConfig()
		config.Apply(GetExtraSchema())
		w := parquet.NewGenericWriter[FramePtr](buf, config)

		tVal := true
		pTrue := &tVal
		fVal := false
		pFalse := &fVal

		expect := []FramePtr{
			{TrackedExtra: &TrackedExtra{ID: 1}, TrackedExtraExists: pTrue},
			{TrackedExtra: nil, TrackedExtraExists: pFalse},
			{TrackedExtra: &TrackedExtra{ID: 2}, TrackedExtraExists: pTrue},
			{TrackedExtra: nil, TrackedExtraExists: pTrue},
		}

		if _, err := w.Write(expect); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}

		readerConf := parquet.DefaultReaderConfig()
		readerConf.Apply(GetExtraSchema())
		reader := parquet.NewReader(bytes.NewReader(buf.Bytes()), readerConf)
		for _, ptr := range expect {
			var boolInst FramePtr
			err := reader.Read(&boolInst)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(ptr, boolInst) {
				t.Errorf("value mismatch:\nwant: %+v\ngot:  %+v", ptr, boolInst)
			}
		}
	})
}
