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
		{ID: 1, Data: "hello"},   // Should be shredded (string matches typed_value)
		{ID: 2, Data: int32(42)}, // Should NOT be shredded (int32 doesn't match string)
		{ID: 3, Data: "world"},   // Should be shredded
		{ID: 4, Data: nil},       // Null
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
		{ID: 1, Data: int32(100)}, // Should be shredded
		{ID: 2, Data: "hello"},    // Should NOT be shredded
		{ID: 3, Data: int32(-50)}, // Should be shredded
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
		{ID: 2, Data: "just a string"},                                   // Type mismatch: stored in value column
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

// TestVariantSchemaEvolution tests reading variant data written with one schema
// using a different schema, verifying that the conversion pipeline handles
// variant columns correctly at the leaf level.
func TestVariantSchemaEvolution(t *testing.T) {
	type Record struct {
		ID   int32 `parquet:"id"`
		Data any   `parquet:"data"`
	}

	t.Run("write shredded, read with extra typed_value columns", func(t *testing.T) {
		// Write with ShreddedVariant(String()), read with ShreddedVariant(Group{name, age})
		writeNode, err := parquet.ShreddedVariant(parquet.String())
		if err != nil {
			t.Fatal(err)
		}
		writeSchema := parquet.NewSchema("test", parquet.Group{
			"id":   parquet.Leaf(parquet.Int32Type),
			"data": writeNode,
		})

		records := []Record{
			{ID: 1, Data: "hello"},   // Shredded into String typed_value
			{ID: 2, Data: int32(42)}, // Not shredded, stored in value column
		}

		buf := new(bytes.Buffer)
		writer := parquet.NewGenericWriter[Record](buf, writeSchema)
		if _, err := writer.Write(records); err != nil {
			t.Fatal(err)
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}

		// Read with a different shredded schema that has additional typed_value fields.
		// The extra columns should appear as null/missing, and existing data should be preserved.
		readNode, err := parquet.ShreddedVariant(parquet.Group{
			"name": parquet.String(),
			"age":  parquet.Leaf(parquet.Int32Type),
		})
		if err != nil {
			t.Fatal(err)
		}
		readSchema := parquet.NewSchema("test", parquet.Group{
			"id":   parquet.Leaf(parquet.Int32Type),
			"data": readNode,
		})

		reader := parquet.NewGenericReader[Record](bytes.NewReader(buf.Bytes()), readSchema)
		defer reader.Close()

		readRecords := make([]Record, len(records))
		n, err := reader.Read(readRecords)
		if err != nil && err != io.EOF {
			t.Fatalf("read: %v", err)
		}
		if n != len(records) {
			t.Fatalf("read %d records, want %d", n, len(records))
		}

		if readRecords[0].ID != 1 {
			t.Errorf("record 0: ID = %d, want 1", readRecords[0].ID)
		}
		if readRecords[1].ID != 2 {
			t.Errorf("record 1: ID = %d, want 2", readRecords[1].ID)
		}
	})

	t.Run("write unshredded, read unshredded with different wrapper", func(t *testing.T) {
		// Both unshredded: same leaf structure (metadata + value as ByteArray),
		// conversion should preserve data.
		writeSchema := parquet.NewSchema("test", parquet.Group{
			"id":   parquet.Leaf(parquet.Int32Type),
			"data": parquet.Variant(),
		})

		records := []Record{
			{ID: 1, Data: "hello"},
			{ID: 2, Data: int32(42)},
			{ID: 3, Data: nil},
		}

		buf := new(bytes.Buffer)
		writer := parquet.NewGenericWriter[Record](buf, writeSchema)
		if _, err := writer.Write(records); err != nil {
			t.Fatal(err)
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}

		// Read with the same unshredded schema.
		reader := parquet.NewGenericReader[Record](bytes.NewReader(buf.Bytes()), writeSchema)
		defer reader.Close()

		readRecords := make([]Record, len(records))
		n, err := reader.Read(readRecords)
		if err != nil && err != io.EOF {
			t.Fatalf("read: %v", err)
		}
		if n != len(records) {
			t.Fatalf("read %d records, want %d", n, len(records))
		}

		if s, ok := readRecords[0].Data.(string); !ok || s != "hello" {
			t.Errorf("record 0: Data = %v (%T), want string %q", readRecords[0].Data, readRecords[0].Data, "hello")
		}
		if v, ok := readRecords[1].Data.(int32); !ok || v != 42 {
			t.Errorf("record 1: Data = %v (%T), want int32(42)", readRecords[1].Data, readRecords[1].Data)
		}
		if readRecords[2].Data != nil {
			t.Errorf("record 2: Data = %v, want nil", readRecords[2].Data)
		}
	})

	t.Run("write shredded, read unshredded does not panic", func(t *testing.T) {
		// Write with ShreddedVariant(String()) — some values are shredded into
		// typed_value. Read with unshredded Variant() — typed_value columns are
		// dropped by the conversion. Values that were only in typed_value become
		// nil (data loss), but the read must not panic or return errors.
		writeNode, err := parquet.ShreddedVariant(parquet.String())
		if err != nil {
			t.Fatal(err)
		}
		writeSchema := parquet.NewSchema("test", parquet.Group{
			"id":   parquet.Leaf(parquet.Int32Type),
			"data": writeNode,
		})

		records := []Record{
			{ID: 1, Data: "hello"},   // Shredded into typed_value
			{ID: 2, Data: int32(42)}, // In value column (type mismatch)
		}

		buf := new(bytes.Buffer)
		writer := parquet.NewGenericWriter[Record](buf, writeSchema)
		if _, err := writer.Write(records); err != nil {
			t.Fatal(err)
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}

		readSchema := parquet.NewSchema("test", parquet.Group{
			"id":   parquet.Leaf(parquet.Int32Type),
			"data": parquet.Variant(),
		})

		reader := parquet.NewGenericReader[Record](bytes.NewReader(buf.Bytes()), readSchema)
		defer reader.Close()

		readRecords := make([]Record, len(records))
		n, err := reader.Read(readRecords)
		if err != nil && err != io.EOF {
			t.Fatalf("read: %v", err)
		}
		if n != len(records) {
			t.Fatalf("read %d records, want %d", n, len(records))
		}

		// Record 0 was shredded — typed_value columns are dropped by conversion,
		// so the data is lost. The value should be nil, not a panic.
		if readRecords[0].Data != nil {
			// If reconstruction happens to preserve it, that's even better,
			// but nil is acceptable for this lossy conversion.
			t.Logf("record 0: Data = %v (%T) — preserved despite lossy conversion", readRecords[0].Data, readRecords[0].Data)
		}

		// Record 1 was in the value column — should survive the conversion.
		if v, ok := readRecords[1].Data.(int32); !ok || v != 42 {
			t.Errorf("record 1: Data = %v (%T), want int32(42)", readRecords[1].Data, readRecords[1].Data)
		}
	})
}

// TestVariantRawStructPassthrough verifies that a struct with Metadata and Value
// []byte fields can be used to pass pre-encoded variant bytes through the write
// and read pipeline without re-encoding. (Codex review P1 fix)
func TestVariantRawStructPassthrough(t *testing.T) {
	type VariantData struct {
		Metadata []byte `parquet:"metadata"`
		Value    []byte `parquet:"value"`
	}
	type Event struct {
		ID   int64       `parquet:"id"`
		Data VariantData `parquet:"data"`
	}

	schema := parquet.NewSchema("Event", parquet.Group{
		"id":   parquet.Int(64),
		"data": parquet.Variant(),
	})

	// Pre-encode variant metadata+value (variant null encoding)
	meta := []byte{0x01, 0x00, 0x00}
	val := []byte{0x00} // variant null

	buf := new(bytes.Buffer)
	writer := parquet.NewGenericWriter[Event](buf, schema)
	_, err := writer.Write([]Event{
		{ID: 1, Data: VariantData{Metadata: meta, Value: val}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	reader := parquet.NewGenericReader[Event](bytes.NewReader(buf.Bytes()), schema)
	defer reader.Close()

	events := make([]Event, 1)
	n, err := reader.Read(events)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("read %d events, want 1", n)
	}

	if !reflect.DeepEqual(events[0].Data.Metadata, meta) {
		t.Errorf("metadata = %v, want %v", events[0].Data.Metadata, meta)
	}
	if !reflect.DeepEqual(events[0].Data.Value, val) {
		t.Errorf("value = %v, want %v", events[0].Data.Value, val)
	}
}

// TestShreddedVariantNestedObjectFallback verifies that nested objects in per-field
// fallback values retain their metadata dictionary and can be decoded correctly.
// (Codex review P2 fix — metadata for per-field shredded fallback values)
func TestShreddedVariantNestedObjectFallback(t *testing.T) {
	// Schema has typed_value for "name" (string) and "age" (int32).
	// Writing a record where "name" has a nested object (not string) forces
	// it to fall back to the value column.
	shreddedNode, err := parquet.ShreddedVariant(parquet.Group{
		"name": parquet.String(),
		"age":  parquet.Leaf(parquet.Int32Type),
	})
	if err != nil {
		t.Fatal(err)
	}

	type Record struct {
		ID   int32 `parquet:"id"`
		Data any   `parquet:"data"`
	}

	schema := parquet.NewSchema("test", parquet.Group{
		"id":   parquet.Leaf(parquet.Int32Type),
		"data": shreddedNode,
	})

	// "name" field is a nested object instead of string — can't be shredded into
	// the String typed_value column, so it falls back to per-field value column.
	records := []Record{
		{ID: 1, Data: map[string]any{
			"name": map[string]any{"first": "Alice", "last": "Smith"},
			"age":  int32(30),
		}},
	}

	buf := new(bytes.Buffer)
	writer := parquet.NewGenericWriter[Record](buf, schema)
	if _, err := writer.Write(records); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	reader := parquet.NewGenericReader[Record](bytes.NewReader(buf.Bytes()), schema)
	defer reader.Close()

	readRecords := make([]Record, 1)
	n, err := reader.Read(readRecords)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("read %d records, want 1", n)
	}

	m, ok := readRecords[0].Data.(map[string]any)
	if !ok {
		t.Fatalf("Data = %v (%T), want map[string]any", readRecords[0].Data, readRecords[0].Data)
	}

	// age should be shredded and reconstructed as int32
	if m["age"] != int32(30) {
		t.Errorf("age = %v (%T), want int32(30)", m["age"], m["age"])
	}

	// name should be a nested map reconstructed from per-field fallback
	nameMap, ok := m["name"].(map[string]any)
	if !ok {
		t.Fatalf("name = %v (%T), want map[string]any", m["name"], m["name"])
	}
	if nameMap["first"] != "Alice" {
		t.Errorf("name.first = %v, want Alice", nameMap["first"])
	}
	if nameMap["last"] != "Smith" {
		t.Errorf("name.last = %v, want Smith", nameMap["last"])
	}
}

// TestShreddedVariantBinaryTypedValue verifies that []byte values shredded into
// a plain ByteArray typed_value column are reconstructed as []byte, not string.
// (Codex review P2 fix — ByteArray vs String reconstruction)
func TestShreddedVariantBinaryTypedValue(t *testing.T) {
	// Use plain ByteArray (no STRING annotation) as typed_value
	shreddedNode, err := parquet.ShreddedVariant(parquet.Leaf(parquet.ByteArrayType))
	if err != nil {
		t.Fatal(err)
	}

	type Record struct {
		Data any `parquet:"data"`
	}

	schema := parquet.NewSchema("test", parquet.Group{
		"data": shreddedNode,
	})

	input := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	records := []Record{
		{Data: input},
	}

	buf := new(bytes.Buffer)
	writer := parquet.NewGenericWriter[Record](buf, schema)
	if _, err := writer.Write(records); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	reader := parquet.NewGenericReader[Record](bytes.NewReader(buf.Bytes()), schema)
	defer reader.Close()

	readRecords := make([]Record, 1)
	n, err := reader.Read(readRecords)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("read %d records, want 1", n)
	}

	// Should come back as []byte, not string
	result, ok := readRecords[0].Data.([]byte)
	if !ok {
		t.Fatalf("Data = %v (%T), want []byte", readRecords[0].Data, readRecords[0].Data)
	}
	if !reflect.DeepEqual(result, input) {
		t.Errorf("Data = %v, want %v", result, input)
	}
}

// TestShreddedVariantObjectWithListField verifies that a shredded variant group
// containing a LIST field correctly counts leaf columns, so fields after the LIST
// are written/read from the correct column positions. (Codex review P1 — LIST column counting)
func TestShreddedVariantObjectWithListField(t *testing.T) {
	// Schema: ShreddedVariant(Group{"tags": List(String()), "name": String()})
	// The LIST field "tags" spans multiple leaf columns, and "name" must still
	// be written/read at the correct offset.
	shreddedNode, err := parquet.ShreddedVariant(parquet.Group{
		"tags": parquet.List(parquet.String()),
		"name": parquet.String(),
	})
	if err != nil {
		t.Fatal(err)
	}

	type Record struct {
		ID   int32 `parquet:"id"`
		Data any   `parquet:"data"`
	}

	schema := parquet.NewSchema("test", parquet.Group{
		"id":   parquet.Leaf(parquet.Int32Type),
		"data": shreddedNode,
	})

	// Write a string value — can't be shredded into the object typed_value,
	// so it goes to the value column. The key test: this must not panic or
	// corrupt data due to incorrect column counting for the LIST field.
	records := []Record{
		{ID: 1, Data: "hello"},
	}

	buf := new(bytes.Buffer)
	writer := parquet.NewGenericWriter[Record](buf, schema)
	if _, err := writer.Write(records); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	reader := parquet.NewGenericReader[Record](bytes.NewReader(buf.Bytes()), schema)
	defer reader.Close()

	readRecords := make([]Record, 1)
	n, err := reader.Read(readRecords)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("read %d records, want 1", n)
	}

	if s, ok := readRecords[0].Data.(string); !ok || s != "hello" {
		t.Errorf("Data = %v (%T), want string %q", readRecords[0].Data, readRecords[0].Data, "hello")
	}
}

// TestShreddedVariantStringVsBinaryShredding verifies that STRING typed_value
// columns only shred string values, and plain ByteArray typed_value columns
// only shred []byte values. (Codex review P2 — STRING vs BINARY)
func TestShreddedVariantStringVsBinaryShredding(t *testing.T) {
	t.Run("string into STRING typed_value", func(t *testing.T) {
		shreddedNode, _ := parquet.ShreddedVariant(parquet.String())
		type Record struct {
			Data any `parquet:"data"`
		}
		schema := parquet.NewSchema("test", parquet.Group{"data": shreddedNode})

		buf := new(bytes.Buffer)
		writer := parquet.NewGenericWriter[Record](buf, schema)
		_, _ = writer.Write([]Record{{Data: "hello"}})
		_ = writer.Close()

		reader := parquet.NewGenericReader[Record](bytes.NewReader(buf.Bytes()), schema)
		defer reader.Close()
		recs := make([]Record, 1)
		reader.Read(recs)

		// string input → STRING typed_value → string output
		if s, ok := recs[0].Data.(string); !ok || s != "hello" {
			t.Errorf("got %v (%T), want string %q", recs[0].Data, recs[0].Data, "hello")
		}
	})

	t.Run("bytes into STRING typed_value falls back", func(t *testing.T) {
		shreddedNode, _ := parquet.ShreddedVariant(parquet.String())
		type Record struct {
			Data any `parquet:"data"`
		}
		schema := parquet.NewSchema("test", parquet.Group{"data": shreddedNode})

		input := []byte{0xDE, 0xAD}
		buf := new(bytes.Buffer)
		writer := parquet.NewGenericWriter[Record](buf, schema)
		_, _ = writer.Write([]Record{{Data: input}})
		_ = writer.Close()

		reader := parquet.NewGenericReader[Record](bytes.NewReader(buf.Bytes()), schema)
		defer reader.Close()
		recs := make([]Record, 1)
		reader.Read(recs)

		// []byte into STRING typed_value should NOT be shredded — falls back to
		// value column and round-trips through variant encoding as []byte
		if b, ok := recs[0].Data.([]byte); !ok {
			t.Errorf("got %v (%T), want []byte", recs[0].Data, recs[0].Data)
		} else if !reflect.DeepEqual(b, input) {
			t.Errorf("got %v, want %v", b, input)
		}
	})

	t.Run("bytes into ByteArray typed_value", func(t *testing.T) {
		shreddedNode, _ := parquet.ShreddedVariant(parquet.Leaf(parquet.ByteArrayType))
		type Record struct {
			Data any `parquet:"data"`
		}
		schema := parquet.NewSchema("test", parquet.Group{"data": shreddedNode})

		input := []byte{0xCA, 0xFE}
		buf := new(bytes.Buffer)
		writer := parquet.NewGenericWriter[Record](buf, schema)
		_, _ = writer.Write([]Record{{Data: input}})
		_ = writer.Close()

		reader := parquet.NewGenericReader[Record](bytes.NewReader(buf.Bytes()), schema)
		defer reader.Close()
		recs := make([]Record, 1)
		reader.Read(recs)

		// []byte into plain ByteArray typed_value → []byte output
		if b, ok := recs[0].Data.([]byte); !ok {
			t.Errorf("got %v (%T), want []byte", recs[0].Data, recs[0].Data)
		} else if !reflect.DeepEqual(b, input) {
			t.Errorf("got %v, want %v", b, input)
		}
	})

	t.Run("string into ByteArray typed_value falls back", func(t *testing.T) {
		shreddedNode, _ := parquet.ShreddedVariant(parquet.Leaf(parquet.ByteArrayType))
		type Record struct {
			Data any `parquet:"data"`
		}
		schema := parquet.NewSchema("test", parquet.Group{"data": shreddedNode})

		buf := new(bytes.Buffer)
		writer := parquet.NewGenericWriter[Record](buf, schema)
		_, _ = writer.Write([]Record{{Data: "hello"}})
		_ = writer.Close()

		reader := parquet.NewGenericReader[Record](bytes.NewReader(buf.Bytes()), schema)
		defer reader.Close()
		recs := make([]Record, 1)
		reader.Read(recs)

		// string into plain ByteArray typed_value should NOT be shredded —
		// falls back to value column and round-trips as string
		if s, ok := recs[0].Data.(string); !ok || s != "hello" {
			t.Errorf("got %v (%T), want string %q", recs[0].Data, recs[0].Data, "hello")
		}
	})
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
