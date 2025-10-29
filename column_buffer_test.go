package parquet

import (
	"bytes"
	"reflect"
	"testing"
)

func TestBroadcastValueInt32(t *testing.T) {
	buf := make([]int32, 123)
	broadcastValueInt32(buf, 0x0A)

	for i, v := range buf {
		if v != 0x0A0A0A0A {
			t.Fatalf("wrong value at index %d: %v", i, v)
		}
	}
}

func TestBroadcastRangeInt32(t *testing.T) {
	buf := make([]int32, 123)
	broadcastRangeInt32(buf, 1)

	for i, v := range buf {
		if v != int32(1+i) {
			t.Fatalf("wrong value at index %d: %v", i, v)
		}
	}
}

func BenchmarkBroadcastValueInt32(b *testing.B) {
	buf := make([]int32, 1000)
	for b.Loop() {
		broadcastValueInt32(buf, -1)
	}
	b.SetBytes(4 * int64(len(buf)))
}

func BenchmarkBroadcastRangeInt32(b *testing.B) {
	buf := make([]int32, 1000)
	for b.Loop() {
		broadcastRangeInt32(buf, 0)
	}
	b.SetBytes(4 * int64(len(buf)))
}

func TestWriteAndReadOptionalList(t *testing.T) {
	type record struct {
		Values []float64 `parquet:"values,list,optional"`
	}

	records := []record{
		{Values: []float64{1.0, 2.0, 3.0}},
		{Values: []float64{}},
		{Values: []float64{4.0, 5.0}},
	}

	buffer := new(bytes.Buffer)
	if err := Write(buffer, records); err != nil {
		t.Fatal(err)
	}

	found, err := Read[record](bytes.NewReader(buffer.Bytes()), int64(buffer.Len()))
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(records, found) {
		t.Fatalf("expected %v, got %v", records, found)
	}
}

func TestWriteAndReadOptionalPointer(t *testing.T) {
	type record struct {
		Value float64 `parquet:"values,optional"`
	}

	records := []record{
		{Value: 1.0},
		{Value: 0.0},
		{Value: 2.0},
		{Value: 0.0},
	}

	buffer := new(bytes.Buffer)
	if err := Write(buffer, records); err != nil {
		t.Fatal(err)
	}

	found, err := Read[record](bytes.NewReader(buffer.Bytes()), int64(buffer.Len()))
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(records, found) {
		t.Fatalf("expected %v, got %v", records, found)
	}
}

// https://github.com/segmentio/parquet-go/issues/501
func TestIssueSegmentio501(t *testing.T) {
	col := newBooleanColumnBuffer(BooleanType, 0, 2055208)

	// write all trues and then flush the buffer
	_, err := col.WriteBooleans([]bool{true, true, true, true, true, true, true, true})
	if err != nil {
		t.Fatal(err)
	}
	col.Reset()

	// write a single false, we are trying to trip a certain line of code in WriteBooleans
	_, err = col.WriteBooleans([]bool{false})
	if err != nil {
		t.Fatal(err)
	}
	// now write 7 booleans at once, this will cause WriteBooleans to attempt its "alignment" logic
	_, err = col.WriteBooleans([]bool{false, false, false, false, false, false, false})
	if err != nil {
		panic(err)
	}

	for i := range 8 {
		read := make([]Value, 1)
		_, err = col.ReadValuesAt(read, int64(i))
		if err != nil {
			t.Fatal(err)
		}
		if read[0].Boolean() {
			t.Fatalf("expected false at index %d", i)
		}
	}
}

func TestWriteRowsFuncOfRequiredColumnNotFound(t *testing.T) {
	schema := NewSchema("test", Group{
		"name": String(),
		"age":  Int(32),
	})

	defer func() {
		if r := recover(); r != nil {
			expected := "parquet: column not found: nonexistent"
			if r != expected {
				t.Fatalf("expected panic message %q, got %q", expected, r)
			}
		} else {
			t.Fatal("expected panic but none occurred")
		}
	}()

	writeRowsFuncOfRequired(reflect.TypeOf(""), schema, columnPath{"nonexistent"}, nil)
}

// TestMapFieldToGroupSchema tests writing a Go struct with a map[string]string
// field to a schema where that field is defined as a GROUP with named optional fields.
// This verifies that NewWriter supports this functionality.
func TestMapFieldToGroupSchema(t *testing.T) {
	// The user's Go type
	type RecordWithMap struct {
		Nested map[string]string
	}

	// The desired schema structure (GROUP with named fields)
	type RecordWithStruct struct {
		Nested struct {
			A string `parquet:",optional"`
			B string `parquet:",optional"`
			C string `parquet:",optional"`
		}
	}

	// Create schema from the struct (this gives us the desired GROUP schema)
	desiredSchema := SchemaOf(RecordWithStruct{})

	// Try to write using the map type
	buf := new(bytes.Buffer)
	writer := NewWriter(buf, desiredSchema)

	// Attempt to write a value with map[string]string
	record := RecordWithMap{
		Nested: map[string]string{
			"A": "value_a",
			"B": "value_b",
			"C": "value_c",
		},
	}

	err := writer.Write(record)
	if err != nil {
		t.Fatalf("failed to write row: %v", err)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	// Try to read it back
	reader := NewReader(bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	var result RecordWithStruct
	if err := reader.Read(&result); err != nil {
		t.Fatalf("failed to read row: %v", err)
	}
}

// TestWhatActuallyHappensWithMapField tests what schema is generated when
// you use a map[string]string field - it creates a MAP logical type, not a GROUP.
func TestWhatActuallyHappensWithMapField(t *testing.T) {
	type RecordWithMap struct {
		Nested map[string]string
	}

	// Get the schema that's naturally generated from the map type
	naturalSchema := SchemaOf(RecordWithMap{})

	// Verify that the natural schema is a MAP type, not a GROUP
	nestedField := naturalSchema.Fields()[0]
	if nestedField.Type().LogicalType() == nil || nestedField.Type().LogicalType().Map == nil {
		t.Fatalf("expected Nested field to have MAP logical type, got: %v", nestedField.Type().LogicalType())
	}

	// Write some data
	buf := new(bytes.Buffer)
	writer := NewWriter(buf, naturalSchema)

	record := RecordWithMap{
		Nested: map[string]string{
			"A": "value_a",
			"B": "value_b",
		},
	}

	if err := writer.Write(record); err != nil {
		t.Fatalf("failed to write: %v", err)
	}
	writer.Close()

	// Read it back and check the schema matches
	reader := NewReader(bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	if !EqualNodes(naturalSchema, reader.Schema()) {
		t.Errorf("reader schema doesn't match natural schema:\nexpected: %s\ngot: %s", naturalSchema, reader.Schema())
	}
}

// TestGenericWriterMapToGroupSchema tests that NewGenericWriter supports
// writing map[string]string fields to a GROUP schema with named optional fields.
func TestGenericWriterMapToGroupSchema(t *testing.T) {
	// The user's Go type
	type RecordWithMap struct {
		Nested map[string]string
	}

	// The desired schema structure (GROUP with named fields)
	type RecordWithStruct struct {
		Nested struct {
			A string `parquet:",optional"`
			B string `parquet:",optional"`
			C string `parquet:",optional"`
		}
	}

	// Create schema from the struct (this gives us the desired GROUP schema)
	desiredSchema := SchemaOf(RecordWithStruct{})

	// Try to write using NewGenericWriter with the map type
	buf := new(bytes.Buffer)
	writer := NewGenericWriter[RecordWithMap](buf, desiredSchema)

	// Attempt to write values with map[string]string
	records := []RecordWithMap{
		{
			Nested: map[string]string{
				"A": "value_a1",
				"B": "value_b1",
				"C": "value_c1",
			},
		},
		{
			Nested: map[string]string{
				"A": "value_a2",
				"B": "value_b2",
				// C is omitted - should be null
			},
		},
	}

	n, err := writer.Write(records)
	if err != nil {
		t.Fatalf("failed to write rows: %v", err)
	}
	if n != 2 {
		t.Fatalf("expected to write 2 rows, wrote %d", n)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	// Try to read it back
	reader := NewReader(bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	var result1 RecordWithStruct
	if err := reader.Read(&result1); err != nil {
		t.Fatalf("failed to read row 1: %v", err)
	}

	if result1.Nested.A != "value_a1" || result1.Nested.B != "value_b1" || result1.Nested.C != "value_c1" {
		t.Errorf("row 1 values incorrect: %+v", result1.Nested)
	}

	var result2 RecordWithStruct
	if err := reader.Read(&result2); err != nil {
		t.Fatalf("failed to read row 2: %v", err)
	}

	if result2.Nested.A != "value_a2" || result2.Nested.B != "value_b2" || result2.Nested.C != "" {
		t.Errorf("row 2 values incorrect: %+v", result2.Nested)
	}
}

// TestGenericWriterMapAnyToNestedGroupSchema tests that NewGenericWriter supports
// writing map[string]any fields to a nested GROUP schema with multiple levels.
// This corresponds to the example:
//
//	message record {
//	  group nested {
//	    group coordinates {
//	      double x;
//	      double y;
//	    }
//	    string id;
//	  }
//	}
func TestGenericWriterMapAnyToNestedGroupSchema(t *testing.T) {
	// The user's Go type with map[string]any
	type RecordWithMap struct {
		Nested map[string]any
	}

	// The desired schema structure (nested GROUPs)
	type RecordWithStruct struct {
		Nested struct {
			Coordinates struct {
				X float64 `parquet:",optional"`
				Y float64 `parquet:",optional"`
			} `parquet:",optional"`
			ID string `parquet:",optional"`
		}
	}

	// Create schema from the struct (this gives us the desired nested GROUP schema)
	desiredSchema := SchemaOf(RecordWithStruct{})

	// Try to write using NewGenericWriter with the map type
	buf := new(bytes.Buffer)
	writer := NewGenericWriter[RecordWithMap](buf, desiredSchema)

	// Attempt to write values with map[string]any
	records := []RecordWithMap{
		{
			Nested: map[string]any{
				"Coordinates": map[string]float64{
					"X": 0.1,
					"Y": 0.2,
				},
				"ID": "1234567890",
			},
		},
		{
			Nested: map[string]any{
				"Coordinates": map[string]any{
					"X": 1.5,
					"Y": 2.5,
				},
				"ID": "abc",
			},
		},
		{
			Nested: map[string]any{
				// Coordinates omitted - should be null
				"ID": "xyz",
			},
		},
	}

	n, err := writer.Write(records)
	if err != nil {
		t.Fatalf("failed to write rows: %v", err)
	}
	if n != 3 {
		t.Fatalf("expected to write 3 rows, wrote %d", n)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	// Try to read it back
	reader := NewReader(bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	var result1 RecordWithStruct
	if err := reader.Read(&result1); err != nil {
		t.Fatalf("failed to read row 1: %v", err)
	}

	if result1.Nested.Coordinates.X != 0.1 || result1.Nested.Coordinates.Y != 0.2 {
		t.Errorf("row 1 coordinates incorrect: %+v", result1.Nested.Coordinates)
	}
	if result1.Nested.ID != "1234567890" {
		t.Errorf("row 1 ID incorrect: %s", result1.Nested.ID)
	}

	var result2 RecordWithStruct
	if err := reader.Read(&result2); err != nil {
		t.Fatalf("failed to read row 2: %v", err)
	}

	if result2.Nested.Coordinates.X != 1.5 || result2.Nested.Coordinates.Y != 2.5 {
		t.Errorf("row 2 coordinates incorrect: %+v", result2.Nested.Coordinates)
	}
	if result2.Nested.ID != "abc" {
		t.Errorf("row 2 ID incorrect: %s", result2.Nested.ID)
	}

	var result3 RecordWithStruct
	if err := reader.Read(&result3); err != nil {
		t.Fatalf("failed to read row 3: %v", err)
	}

	if result3.Nested.Coordinates.X != 0 || result3.Nested.Coordinates.Y != 0 {
		t.Errorf("row 3 coordinates should be zero: %+v", result3.Nested.Coordinates)
	}
	if result3.Nested.ID != "xyz" {
		t.Errorf("row 3 ID incorrect: %s", result3.Nested.ID)
	}
}

// TestGenericWriterMapAnyToGroupWithMixedTypes tests map[string]any mapping
// to a GROUP schema with various primitive types.
func TestGenericWriterMapAnyToGroupWithMixedTypes(t *testing.T) {
	// The user's Go type with map[string]any
	type RecordWithMap struct {
		Data map[string]any
	}

	// The desired schema structure with various types
	type RecordWithStruct struct {
		Data struct {
			Name   string  `parquet:",optional"`
			Age    int32   `parquet:",optional"`
			Score  float64 `parquet:",optional"`
			Active bool    `parquet:",optional"`
		}
	}

	// Create schema from the struct
	desiredSchema := SchemaOf(RecordWithStruct{})

	// Try to write using NewGenericWriter with the map type
	buf := new(bytes.Buffer)
	writer := NewGenericWriter[RecordWithMap](buf, desiredSchema)

	// Attempt to write values with map[string]any containing different types
	records := []RecordWithMap{
		{
			Data: map[string]any{
				"Name":   "Alice",
				"Age":    int32(30),
				"Score":  95.5,
				"Active": true,
			},
		},
		{
			Data: map[string]any{
				"Name":  "Bob",
				"Age":   int32(25),
				"Score": 87.3,
				// Active omitted
			},
		},
	}

	n, err := writer.Write(records)
	if err != nil {
		t.Fatalf("failed to write rows: %v", err)
	}
	if n != 2 {
		t.Fatalf("expected to write 2 rows, wrote %d", n)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	// Try to read it back
	reader := NewReader(bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	var result1 RecordWithStruct
	if err := reader.Read(&result1); err != nil {
		t.Fatalf("failed to read row 1: %v", err)
	}

	if result1.Data.Name != "Alice" {
		t.Errorf("row 1 Name incorrect: %s", result1.Data.Name)
	}
	if result1.Data.Age != 30 {
		t.Errorf("row 1 Age incorrect: %d", result1.Data.Age)
	}
	if result1.Data.Score != 95.5 {
		t.Errorf("row 1 Score incorrect: %f", result1.Data.Score)
	}
	if result1.Data.Active != true {
		t.Errorf("row 1 Active incorrect: %v", result1.Data.Active)
	}

	var result2 RecordWithStruct
	if err := reader.Read(&result2); err != nil {
		t.Fatalf("failed to read row 2: %v", err)
	}

	if result2.Data.Name != "Bob" {
		t.Errorf("row 2 Name incorrect: %s", result2.Data.Name)
	}
	if result2.Data.Age != 25 {
		t.Errorf("row 2 Age incorrect: %d", result2.Data.Age)
	}
	if result2.Data.Score != 87.3 {
		t.Errorf("row 2 Score incorrect: %f", result2.Data.Score)
	}
	if result2.Data.Active != false {
		t.Errorf("row 2 Active should be false: %v", result2.Data.Active)
	}
}

// TestGenericWriterMapAnyToDeeplyNestedGroups tests map[string]any mapping
// to a deeply nested GROUP schema (3+ levels).
func TestGenericWriterMapAnyToDeeplyNestedGroups(t *testing.T) {
	// The user's Go type with map[string]any
	type RecordWithMap struct {
		Root map[string]any
	}

	// The desired schema structure with deep nesting
	type RecordWithStruct struct {
		Root struct {
			Level1 struct {
				Level2 struct {
					Value string `parquet:",optional"`
				} `parquet:",optional"`
				Name *string `parquet:",optional"`
			} `parquet:",optional"`
		}
	}

	// Create schema from the struct
	desiredSchema := SchemaOf(RecordWithStruct{})

	// Try to write using NewGenericWriter with the map type
	buf := new(bytes.Buffer)
	writer := NewGenericWriter[RecordWithMap](buf, desiredSchema)
	strPtr := func(s string) *string { return &s }

	// Attempt to write values with deeply nested maps
	records := []RecordWithMap{
		{
			Root: map[string]any{
				"Level1": map[string]any{
					"Level2": map[string]any{
						"Value": strPtr("deep_value"),
					},
					"Name": "level1_name",
				},
			},
		},
		{
			Root: map[string]any{
				"Level1": map[string]any{
					// Level2 omitted
					"Name": "another_name",
				},
			},
		},
	}

	n, err := writer.Write(records)
	if err != nil {
		t.Fatalf("failed to write rows: %v", err)
	}
	if n != 2 {
		t.Fatalf("expected to write 2 rows, wrote %d", n)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	// Try to read it back
	reader := NewReader(bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	var result1 RecordWithStruct
	if err := reader.Read(&result1); err != nil {
		t.Fatalf("failed to read row 1: %v", err)
	}

	if result1.Root.Level1.Level2.Value != "deep_value" {
		t.Errorf("row 1 deep value incorrect: %s", result1.Root.Level1.Level2.Value)
	}
	if *result1.Root.Level1.Name != "level1_name" {
		t.Errorf("row 1 level1 name incorrect: %s", *result1.Root.Level1.Name)
	}

	var result2 RecordWithStruct
	if err := reader.Read(&result2); err != nil {
		t.Fatalf("failed to read row 2: %v", err)
	}

	if result2.Root.Level1.Level2.Value != "" {
		t.Errorf("row 2 deep value should be empty: %s", result2.Root.Level1.Level2.Value)
	}
	if *result2.Root.Level1.Name != "another_name" {
		t.Errorf("row 2 level1 name incorrect: %s", *result2.Root.Level1.Name)
	}
}
