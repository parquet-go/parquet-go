package parquet_test

import (
	"bytes"
	"io"
	"reflect"
	"testing"

	"github.com/parquet-go/parquet-go"
)

func TestVariantWriteRead(t *testing.T) {
	type Record struct {
		ID   int32 `parquet:"id"`
		Data any   `parquet:"data,variant"`
	}

	records := []Record{
		{ID: 1, Data: "hello"},
		{ID: 2, Data: int32(42)},
		{ID: 3, Data: map[string]any{"key": "value", "num": int32(10)}},
		{ID: 4, Data: []any{int32(1), int32(2), int32(3)}},
		{ID: 5, Data: nil},
	}

	buf := new(bytes.Buffer)
	writer := parquet.NewGenericWriter[Record](buf)
	if _, err := writer.Write(records); err != nil {
		t.Fatalf("failed to write records: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	reader := parquet.NewGenericReader[Record](bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	readRecords := make([]Record, len(records))
	n, err := reader.Read(readRecords)
	if err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}
	if n != len(records) {
		t.Fatalf("read %d records, want %d", n, len(records))
	}

	// Verify the round-trip
	for i, rec := range readRecords {
		if rec.ID != records[i].ID {
			t.Errorf("record %d: ID = %d, want %d", i, rec.ID, records[i].ID)
		}
	}

	// Record 0: string "hello"
	if s, ok := readRecords[0].Data.(string); !ok || s != "hello" {
		t.Errorf("record 0: Data = %v (%T), want string %q", readRecords[0].Data, readRecords[0].Data, "hello")
	}

	// Record 1: int32(42)
	if v, ok := readRecords[1].Data.(int32); !ok || v != 42 {
		t.Errorf("record 1: Data = %v (%T), want int32(42)", readRecords[1].Data, readRecords[1].Data)
	}

	// Record 2: map
	if m, ok := readRecords[2].Data.(map[string]any); !ok {
		t.Errorf("record 2: Data = %v (%T), want map[string]any", readRecords[2].Data, readRecords[2].Data)
	} else {
		if m["key"] != "value" {
			t.Errorf("record 2: key = %v, want value", m["key"])
		}
		if m["num"] != int32(10) {
			t.Errorf("record 2: num = %v (%T), want int32(10)", m["num"], m["num"])
		}
	}

	// Record 3: array
	if a, ok := readRecords[3].Data.([]any); !ok {
		t.Errorf("record 3: Data = %v (%T), want []any", readRecords[3].Data, readRecords[3].Data)
	} else {
		expected := []any{int32(1), int32(2), int32(3)}
		if !reflect.DeepEqual(a, expected) {
			t.Errorf("record 3: Data = %v, want %v", a, expected)
		}
	}

	// Record 4: nil
	if readRecords[4].Data != nil {
		t.Errorf("record 4: Data = %v, want nil", readRecords[4].Data)
	}
}

func TestShreddedVariantPrimitive(t *testing.T) {
	// Create a shredded variant schema where typed_value is a string (ByteArray)
	shreddedNode, err := parquet.ShreddedVariant(parquet.String())
	if err != nil {
		t.Fatalf("ShreddedVariant: %v", err)
	}

	type Record struct {
		ID   int32 `parquet:"id"`
		Data any   `parquet:"data"`
	}

	schema := parquet.NewSchema("test", parquet.Group{
		"id":   parquet.Leaf(parquet.Int32Type),
		"data": shreddedNode,
	})

	records := []Record{
		{ID: 1, Data: "hello"},         // Should be shredded (string matches typed_value)
		{ID: 2, Data: int32(42)},       // Should NOT be shredded (int32 doesn't match string)
		{ID: 3, Data: "world"},         // Should be shredded
		{ID: 4, Data: nil},            // Null
	}

	buf := new(bytes.Buffer)
	writer := parquet.NewGenericWriter[Record](buf, schema)
	if _, err := writer.Write(records); err != nil {
		t.Fatalf("failed to write records: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	reader := parquet.NewGenericReader[Record](bytes.NewReader(buf.Bytes()), schema)
	defer reader.Close()

	readRecords := make([]Record, len(records))
	n, err := reader.Read(readRecords)
	if err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}
	if n != len(records) {
		t.Fatalf("read %d records, want %d", n, len(records))
	}

	// Record 0: string "hello" (shredded)
	if s, ok := readRecords[0].Data.(string); !ok || s != "hello" {
		t.Errorf("record 0: Data = %v (%T), want string %q", readRecords[0].Data, readRecords[0].Data, "hello")
	}

	// Record 1: int32(42) (unshredded, stored in value column)
	if v, ok := readRecords[1].Data.(int32); !ok || v != 42 {
		t.Errorf("record 1: Data = %v (%T), want int32(42)", readRecords[1].Data, readRecords[1].Data)
	}

	// Record 2: string "world" (shredded)
	if s, ok := readRecords[2].Data.(string); !ok || s != "world" {
		t.Errorf("record 2: Data = %v (%T), want string %q", readRecords[2].Data, readRecords[2].Data, "world")
	}

	// Record 3: nil
	if readRecords[3].Data != nil {
		t.Errorf("record 3: Data = %v, want nil", readRecords[3].Data)
	}
}

func TestShreddedVariantInt(t *testing.T) {
	// Create a shredded variant schema where typed_value is Int32
	shreddedNode, err := parquet.ShreddedVariant(parquet.Leaf(parquet.Int32Type))
	if err != nil {
		t.Fatalf("ShreddedVariant: %v", err)
	}

	type Record struct {
		ID   int32 `parquet:"id"`
		Data any   `parquet:"data"`
	}

	schema := parquet.NewSchema("test", parquet.Group{
		"id":   parquet.Leaf(parquet.Int32Type),
		"data": shreddedNode,
	})

	records := []Record{
		{ID: 1, Data: int32(100)},      // Should be shredded
		{ID: 2, Data: "hello"},         // Should NOT be shredded
		{ID: 3, Data: int32(-50)},      // Should be shredded
	}

	buf := new(bytes.Buffer)
	writer := parquet.NewGenericWriter[Record](buf, schema)
	if _, err := writer.Write(records); err != nil {
		t.Fatalf("failed to write records: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	reader := parquet.NewGenericReader[Record](bytes.NewReader(buf.Bytes()), schema)
	defer reader.Close()

	readRecords := make([]Record, len(records))
	n, err := reader.Read(readRecords)
	if err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}
	if n != len(records) {
		t.Fatalf("read %d records, want %d", n, len(records))
	}

	if v, ok := readRecords[0].Data.(int32); !ok || v != 100 {
		t.Errorf("record 0: Data = %v (%T), want int32(100)", readRecords[0].Data, readRecords[0].Data)
	}

	if s, ok := readRecords[1].Data.(string); !ok || s != "hello" {
		t.Errorf("record 1: Data = %v (%T), want string %q", readRecords[1].Data, readRecords[1].Data, "hello")
	}

	if v, ok := readRecords[2].Data.(int32); !ok || v != -50 {
		t.Errorf("record 2: Data = %v (%T), want int32(-50)", readRecords[2].Data, readRecords[2].Data)
	}
}

func TestShreddedVariantObject(t *testing.T) {
	// Create a shredded variant schema where typed_value is a group with "name" (string) and "age" (int32)
	shreddedNode, err := parquet.ShreddedVariant(parquet.Group{
		"name": parquet.String(),
		"age":  parquet.Leaf(parquet.Int32Type),
	})
	if err != nil {
		t.Fatalf("ShreddedVariant: %v", err)
	}

	type Record struct {
		ID   int32 `parquet:"id"`
		Data any   `parquet:"data"`
	}

	schema := parquet.NewSchema("test", parquet.Group{
		"id":   parquet.Leaf(parquet.Int32Type),
		"data": shreddedNode,
	})

	records := []Record{
		{ID: 1, Data: map[string]any{"name": "Alice", "age": int32(30)}}, // All fields match: fully shredded
		{ID: 2, Data: "just a string"},                                    // Type mismatch: stored in value column
		{ID: 3, Data: map[string]any{"name": "Bob", "age": int32(25)}},   // Fully shredded
	}

	buf := new(bytes.Buffer)
	writer := parquet.NewGenericWriter[Record](buf, schema)
	if _, err := writer.Write(records); err != nil {
		t.Fatalf("failed to write records: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	reader := parquet.NewGenericReader[Record](bytes.NewReader(buf.Bytes()), schema)
	defer reader.Close()

	readRecords := make([]Record, len(records))
	n, err := reader.Read(readRecords)
	if err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}
	if n != len(records) {
		t.Fatalf("read %d records, want %d", n, len(records))
	}

	// Record 0: shredded object
	if m, ok := readRecords[0].Data.(map[string]any); !ok {
		t.Errorf("record 0: Data = %v (%T), want map[string]any", readRecords[0].Data, readRecords[0].Data)
	} else {
		if m["name"] != "Alice" {
			t.Errorf("record 0: name = %v, want Alice", m["name"])
		}
		if m["age"] != int32(30) {
			t.Errorf("record 0: age = %v (%T), want int32(30)", m["age"], m["age"])
		}
	}

	// Record 1: unshredded string
	if s, ok := readRecords[1].Data.(string); !ok || s != "just a string" {
		t.Errorf("record 1: Data = %v (%T), want string", readRecords[1].Data, readRecords[1].Data)
	}

	// Record 2: shredded object
	if m, ok := readRecords[2].Data.(map[string]any); !ok {
		t.Errorf("record 2: Data = %v (%T), want map[string]any", readRecords[2].Data, readRecords[2].Data)
	} else {
		if m["name"] != "Bob" {
			t.Errorf("record 2: name = %v, want Bob", m["name"])
		}
		if m["age"] != int32(25) {
			t.Errorf("record 2: age = %v (%T), want int32(25)", m["age"], m["age"])
		}
	}
}

func TestShreddedVariantTypeMismatchFallback(t *testing.T) {
	// String typed_value, but write various non-string types
	shreddedNode, err := parquet.ShreddedVariant(parquet.String())
	if err != nil {
		t.Fatalf("ShreddedVariant: %v", err)
	}

	type Record struct {
		Data any `parquet:"data"`
	}

	schema := parquet.NewSchema("test", parquet.Group{
		"data": shreddedNode,
	})

	records := []Record{
		{Data: int32(42)},
		{Data: true},
		{Data: float64(3.14)},
		{Data: map[string]any{"key": "val"}},
		{Data: []any{int32(1), int32(2)}},
	}

	buf := new(bytes.Buffer)
	writer := parquet.NewGenericWriter[Record](buf, schema)
	if _, err := writer.Write(records); err != nil {
		t.Fatalf("failed to write records: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	reader := parquet.NewGenericReader[Record](bytes.NewReader(buf.Bytes()), schema)
	defer reader.Close()

	readRecords := make([]Record, len(records))
	n, err := reader.Read(readRecords)
	if err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}
	if n != len(records) {
		t.Fatalf("read %d records, want %d", n, len(records))
	}

	// All values should round-trip through the value column (unshredded fallback)
	if v, ok := readRecords[0].Data.(int32); !ok || v != 42 {
		t.Errorf("record 0: got %v (%T), want int32(42)", readRecords[0].Data, readRecords[0].Data)
	}
	if v, ok := readRecords[1].Data.(bool); !ok || v != true {
		t.Errorf("record 1: got %v (%T), want true", readRecords[1].Data, readRecords[1].Data)
	}
	if v, ok := readRecords[2].Data.(float64); !ok || v != 3.14 {
		t.Errorf("record 2: got %v (%T), want float64(3.14)", readRecords[2].Data, readRecords[2].Data)
	}
}

func TestVariantSchemaTag(t *testing.T) {
	type Record struct {
		Data any `parquet:"data,variant"`
	}

	schema := parquet.SchemaOf(Record{})
	fields := schema.Fields()

	found := false
	for _, f := range fields {
		if f.Name() == "data" {
			lt := f.Type().LogicalType()
			if lt == nil || lt.Variant == nil {
				t.Error("expected variant logical type for data field")
			}
			found = true
		}
	}
	if !found {
		t.Error("data field not found in schema")
	}
}
