package parquet

import (
	"bytes"
	"encoding/json"
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

	writeRowsFuncOfRequired(reflect.TypeOf(""), schema, columnPath{"nonexistent"})
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

// BenchmarkMapToGroup benchmarks writing maps to GROUP schemas
func BenchmarkMapToGroup(b *testing.B) {
	b.Run("map[string]string", func(b *testing.B) {
		benchmarkMapToGroupStringString(b)
	})
	b.Run("map[string]any", func(b *testing.B) {
		benchmarkMapToGroupStringAny(b)
	})
	b.Run("map[string]map[string]string", func(b *testing.B) {
		benchmarkMapToGroupNested(b)
	})
}

func benchmarkMapToGroupStringString(b *testing.B) {
	// Go type with map
	type RecordWithMap struct {
		ID   int64
		Data map[string]string
	}

	// Schema type with GROUP
	type RecordWithStruct struct {
		ID   int64
		Data struct {
			Field1 string `parquet:",optional"`
			Field2 string `parquet:",optional"`
			Field3 string `parquet:",optional"`
			Field4 string `parquet:",optional"`
			Field5 string `parquet:",optional"`
		}
	}

	schema := SchemaOf(RecordWithStruct{})

	// Create test data
	records := make([]RecordWithMap, 1000)
	for i := range records {
		records[i] = RecordWithMap{
			ID: int64(i),
			Data: map[string]string{
				"Field1": "value1_" + string(rune('a'+i%26)),
				"Field2": "value2_" + string(rune('a'+i%26)),
				"Field3": "value3_" + string(rune('a'+i%26)),
				"Field4": "value4_" + string(rune('a'+i%26)),
				"Field5": "value5_" + string(rune('a'+i%26)),
			},
		}
	}

	buf := new(bytes.Buffer)
	writer := NewGenericWriter[RecordWithMap](buf, schema)

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		buf.Reset()
		writer.Reset(buf)
		_, err := writer.Write(records)
		if err != nil {
			b.Fatal(err)
		}
		if err := writer.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkMapToGroupStringAny(b *testing.B) {
	// Go type with map[string]any
	type RecordWithMap struct {
		ID   int64
		Data map[string]any
	}

	// Schema type with GROUP containing various types
	type RecordWithStruct struct {
		ID   int64
		Data struct {
			Name   string  `parquet:",optional"`
			Age    int32   `parquet:",optional"`
			Score  float64 `parquet:",optional"`
			Active bool    `parquet:",optional"`
			Count  int64   `parquet:",optional"`
		}
	}

	schema := SchemaOf(RecordWithStruct{})

	// Create test data
	records := make([]RecordWithMap, 1000)
	for i := range records {
		records[i] = RecordWithMap{
			ID: int64(i),
			Data: map[string]any{
				"Name":   "name_" + string(rune('a'+i%26)),
				"Age":    int32(20 + i%50),
				"Score":  float64(i%100) + 0.5,
				"Active": i%2 == 0,
				"Count":  int64(i * 10),
			},
		}
	}

	buf := new(bytes.Buffer)
	writer := NewGenericWriter[RecordWithMap](buf, schema)

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		buf.Reset()
		writer.Reset(buf)
		_, err := writer.Write(records)
		if err != nil {
			b.Fatal(err)
		}
		if err := writer.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkMapToGroupNested(b *testing.B) {
	// Go type with nested maps - using map[string]any for outer map
	// Note: map[string]map[string]string would be more natural but currently has a bug
	// that causes a nil pointer panic (see TestGenericWriterMapMapToNestedGroupSchema)
	type RecordWithMap struct {
		ID     int64
		Nested map[string]map[string]string
	}

	// Schema type with nested GROUPs
	type RecordWithStruct struct {
		ID     int64
		Nested struct {
			Group1 struct {
				A string `parquet:",optional"`
				B string `parquet:",optional"`
				C string `parquet:",optional"`
			} `parquet:",optional"`
			Group2 struct {
				X string `parquet:",optional"`
				Y string `parquet:",optional"`
				Z string `parquet:",optional"`
			} `parquet:",optional"`
		}
	}

	schema := SchemaOf(RecordWithStruct{})

	// Create test data
	records := make([]RecordWithMap, 1000)
	for i := range records {
		records[i] = RecordWithMap{
			ID: int64(i),
			Nested: map[string]map[string]string{
				"Group1": {
					"A": "value_a_" + string(rune('a'+i%26)),
					"B": "value_b_" + string(rune('a'+i%26)),
					"C": "value_c_" + string(rune('a'+i%26)),
				},
				"Group2": {
					"X": "value_x_" + string(rune('a'+i%26)),
					"Y": "value_y_" + string(rune('a'+i%26)),
					"Z": "value_z_" + string(rune('a'+i%26)),
				},
			},
		}
	}

	buf := new(bytes.Buffer)
	writer := NewGenericWriter[RecordWithMap](buf, schema)

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		buf.Reset()
		writer.Reset(buf)
		_, err := writer.Write(records)
		if err != nil {
			b.Fatal(err)
		}
		if err := writer.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

// TestGenericWriterMapMapToNestedGroupSchema tests that map[string]map[string]string
// can be written to a nested GROUP schema.
func TestGenericWriterMapMapToNestedGroupSchema(t *testing.T) {
	// Go type with map[string]map[string]string
	type RecordWithMap struct {
		Nested map[string]map[string]string
	}

	// Schema type with nested GROUPs
	type RecordWithStruct struct {
		Nested struct {
			Group1 struct {
				A string `parquet:",optional"`
				B string `parquet:",optional"`
			} `parquet:",optional"`
			Group2 struct {
				X string `parquet:",optional"`
				Y string `parquet:",optional"`
			} `parquet:",optional"`
		}
	}

	// Create schema from the struct
	desiredSchema := SchemaOf(RecordWithStruct{})

	// Try to write using NewGenericWriter with map[string]map[string]string
	buf := new(bytes.Buffer)
	writer := NewGenericWriter[RecordWithMap](buf, desiredSchema)

	// Attempt to write values
	records := []RecordWithMap{
		{
			Nested: map[string]map[string]string{
				"Group1": {
					"A": "value_a1",
					"B": "value_b1",
				},
				"Group2": {
					"X": "value_x1",
					"Y": "value_y1",
				},
			},
		},
		{
			Nested: map[string]map[string]string{
				"Group1": {
					"A": "value_a2",
					// B omitted
				},
				// Group2 omitted
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

	if result1.Nested.Group1.A != "value_a1" || result1.Nested.Group1.B != "value_b1" {
		t.Errorf("row 1 Group1 incorrect: %+v", result1.Nested.Group1)
	}
	if result1.Nested.Group2.X != "value_x1" || result1.Nested.Group2.Y != "value_y1" {
		t.Errorf("row 1 Group2 incorrect: %+v", result1.Nested.Group2)
	}

	var result2 RecordWithStruct
	if err := reader.Read(&result2); err != nil {
		t.Fatalf("failed to read row 2: %v", err)
	}

	if result2.Nested.Group1.A != "value_a2" || result2.Nested.Group1.B != "" {
		t.Errorf("row 2 Group1 incorrect: %+v", result2.Nested.Group1)
	}
	if result2.Nested.Group2.X != "" || result2.Nested.Group2.Y != "" {
		t.Errorf("row 2 Group2 should be empty: %+v", result2.Nested.Group2)
	}
}

// TestWriteCompositeTypesToJSONByteArray tests that various composite Go types
// (maps, structs, slices) are properly JSON serialized when written to a byte array
// column with JSON logical type.
func TestWriteCompositeTypesToJSONByteArray(t *testing.T) {
	t.Run("MapStringAny", func(t *testing.T) {
		type ReadRecord struct {
			ID   int64
			Data []byte `parquet:"data,json"`
		}
		schema := SchemaOf(ReadRecord{})

		type WriteRecord struct {
			ID   int64
			Data map[string]any `parquet:"data"`
		}

		data := []WriteRecord{
			{
				ID: 1,
				Data: map[string]any{
					"name":   "Alice",
					"age":    30.0,
					"active": true,
				},
			},
			{
				ID: 2,
				Data: map[string]any{
					"name": "Bob",
					"age":  25.0,
				},
			},
		}

		buf := new(bytes.Buffer)
		writer := NewGenericWriter[WriteRecord](buf, schema)

		n, err := writer.Write(data)
		if err != nil {
			t.Fatalf("failed to write: %v", err)
		}
		if n != 2 {
			t.Fatalf("expected to write 2 rows, wrote %d", n)
		}

		if err := writer.Close(); err != nil {
			t.Fatalf("failed to close writer: %v", err)
		}

		// Read back as []byte
		reader := NewReader(bytes.NewReader(buf.Bytes()))
		defer reader.Close()

		var result1 ReadRecord
		if err := reader.Read(&result1); err != nil {
			t.Fatalf("failed to read row 1: %v", err)
		}

		// Verify it's valid JSON
		var unmarshaled1 map[string]any
		if err := json.Unmarshal(result1.Data, &unmarshaled1); err != nil {
			t.Fatalf("failed to unmarshal JSON: %v", err)
		}

		if unmarshaled1["name"] != "Alice" || unmarshaled1["age"] != 30.0 || unmarshaled1["active"] != true {
			t.Errorf("row 1 data incorrect: %+v", unmarshaled1)
		}

		var result2 ReadRecord
		if err := reader.Read(&result2); err != nil {
			t.Fatalf("failed to read row 2: %v", err)
		}

		var unmarshaled2 map[string]any
		if err := json.Unmarshal(result2.Data, &unmarshaled2); err != nil {
			t.Fatalf("failed to unmarshal JSON: %v", err)
		}

		if unmarshaled2["name"] != "Bob" || unmarshaled2["age"] != 25.0 {
			t.Errorf("row 2 data incorrect: %+v", unmarshaled2)
		}
	})

	t.Run("MapStringString", func(t *testing.T) {
		type ReadRecord struct {
			ID       int64  `parquet:"id"`
			Metadata []byte `parquet:"metadata,json"`
		}
		schema := SchemaOf(ReadRecord{})

		type WriteRecord struct {
			ID       int64             `parquet:"id"`
			Metadata map[string]string `parquet:"metadata"`
		}

		data := []WriteRecord{
			{
				ID: 1,
				Metadata: map[string]string{
					"env":     "production",
					"version": "1.2.3",
				},
			},
		}

		buf := new(bytes.Buffer)
		writer := NewGenericWriter[WriteRecord](buf, schema)

		if _, err := writer.Write(data); err != nil {
			t.Fatalf("failed to write: %v", err)
		}
		writer.Close()

		reader := NewReader(bytes.NewReader(buf.Bytes()))
		defer reader.Close()

		var result ReadRecord
		if err := reader.Read(&result); err != nil {
			t.Fatalf("failed to read: %v", err)
		}

		var unmarshaled map[string]string
		if err := json.Unmarshal(result.Metadata, &unmarshaled); err != nil {
			t.Fatalf("failed to unmarshal JSON: %v", err)
		}

		if unmarshaled["env"] != "production" || unmarshaled["version"] != "1.2.3" {
			t.Errorf("metadata incorrect: %+v", unmarshaled)
		}
	})

	t.Run("Struct", func(t *testing.T) {
		type Metadata struct {
			Name string
			Age  int
			Tags []string
		}

		type ReadRecord struct {
			ID       int64  `parquet:"id"`
			Metadata []byte `parquet:"metadata,json"`
		}
		schema := SchemaOf(ReadRecord{})

		type WriteRecord struct {
			ID       int64    `parquet:"id"`
			Metadata Metadata `parquet:"metadata"`
		}

		data := []WriteRecord{
			{
				ID: 1,
				Metadata: Metadata{
					Name: "Alice",
					Age:  30,
					Tags: []string{"admin", "user"},
				},
			},
		}

		buf := new(bytes.Buffer)
		writer := NewGenericWriter[WriteRecord](buf, schema)

		if _, err := writer.Write(data); err != nil {
			t.Fatalf("failed to write: %v", err)
		}
		writer.Close()

		reader := NewReader(bytes.NewReader(buf.Bytes()))
		defer reader.Close()

		var result ReadRecord
		if err := reader.Read(&result); err != nil {
			t.Fatalf("failed to read: %v", err)
		}

		var unmarshaled Metadata
		if err := json.Unmarshal(result.Metadata, &unmarshaled); err != nil {
			t.Fatalf("failed to unmarshal JSON: %v", err)
		}

		if unmarshaled.Name != "Alice" || unmarshaled.Age != 30 || len(unmarshaled.Tags) != 2 {
			t.Errorf("metadata incorrect: %+v", unmarshaled)
		}
	})

	t.Run("Slice", func(t *testing.T) {
		type ReadRecord struct {
			ID    int64  `parquet:"id"`
			Items []byte `parquet:"items,json"`
		}
		schema := SchemaOf(ReadRecord{})

		type WriteRecord struct {
			ID    int64 `parquet:"id"`
			Items []int `parquet:"items"`
		}

		data := []WriteRecord{
			{
				ID:    1,
				Items: []int{10, 20, 30, 40},
			},
		}

		buf := new(bytes.Buffer)
		writer := NewGenericWriter[WriteRecord](buf, schema)

		if _, err := writer.Write(data); err != nil {
			t.Fatalf("failed to write: %v", err)
		}
		writer.Close()

		reader := NewReader(bytes.NewReader(buf.Bytes()))
		defer reader.Close()

		var result ReadRecord
		if err := reader.Read(&result); err != nil {
			t.Fatalf("failed to read: %v", err)
		}

		var unmarshaled []int
		if err := json.Unmarshal(result.Items, &unmarshaled); err != nil {
			t.Fatalf("failed to unmarshal JSON: %v", err)
		}

		if !reflect.DeepEqual(unmarshaled, []int{10, 20, 30, 40}) {
			t.Errorf("items incorrect: %+v", unmarshaled)
		}
	})

	t.Run("NestedSliceOfMaps", func(t *testing.T) {
		type ReadRecord struct {
			ID    int64  `parquet:"id"`
			Items []byte `parquet:"items,json"`
		}
		schema := SchemaOf(ReadRecord{})

		type WriteRecord struct {
			ID    int64            `parquet:"id"`
			Items []map[string]any `parquet:"items"`
		}

		data := []WriteRecord{
			{
				ID: 1,
				Items: []map[string]any{
					{"name": "item1", "count": 5.0},
					{"name": "item2", "count": 10.0},
				},
			},
		}

		buf := new(bytes.Buffer)
		writer := NewGenericWriter[WriteRecord](buf, schema)

		if _, err := writer.Write(data); err != nil {
			t.Fatalf("failed to write: %v", err)
		}
		writer.Close()

		reader := NewReader(bytes.NewReader(buf.Bytes()))
		defer reader.Close()

		var result ReadRecord
		if err := reader.Read(&result); err != nil {
			t.Fatalf("failed to read: %v", err)
		}

		var unmarshaled []map[string]any
		if err := json.Unmarshal(result.Items, &unmarshaled); err != nil {
			t.Fatalf("failed to unmarshal JSON: %v", err)
		}

		if len(unmarshaled) != 2 {
			t.Errorf("expected 2 items, got %d", len(unmarshaled))
		}
		if unmarshaled[0]["name"] != "item1" || unmarshaled[0]["count"] != 5.0 {
			t.Errorf("item 0 incorrect: %+v", unmarshaled[0])
		}
	})

	t.Run("ComplexStruct", func(t *testing.T) {
		type ComplexData struct {
			Name   string
			Values []int
			Meta   map[string]string
		}

		type ReadRecord struct {
			ID   int64
			Data []byte `parquet:"data,json"`
		}
		schema := SchemaOf(ReadRecord{})

		type WriteRecord struct {
			ID   int64
			Data ComplexData `parquet:"data"`
		}

		data := []WriteRecord{
			{
				ID: 1,
				Data: ComplexData{
					Name:   "complex",
					Values: []int{1, 2, 3},
					Meta:   map[string]string{"key": "value"},
				},
			},
		}

		buf := new(bytes.Buffer)
		writer := NewGenericWriter[WriteRecord](buf, schema)

		if _, err := writer.Write(data); err != nil {
			t.Fatalf("failed to write: %v", err)
		}
		writer.Close()

		reader := NewReader(bytes.NewReader(buf.Bytes()))
		defer reader.Close()

		var result ReadRecord
		if err := reader.Read(&result); err != nil {
			t.Fatalf("failed to read: %v", err)
		}

		var unmarshaled ComplexData
		if err := json.Unmarshal(result.Data, &unmarshaled); err != nil {
			t.Fatalf("failed to unmarshal JSON: %v", err)
		}

		if unmarshaled.Name != "complex" || len(unmarshaled.Values) != 3 || unmarshaled.Meta["key"] != "value" {
			t.Errorf("complex data incorrect: %+v", unmarshaled)
		}
	})
}

// TestWriteInterfaceTypeToJSONByteArray tests that an `any` (interface{}) field
// containing various types is properly JSON serialized to a byte array column.
func TestWriteInterfaceTypeToJSONByteArray(t *testing.T) {
	type ReadRecord struct {
		ID   int64
		Data []byte `parquet:"data,json"`
	}
	schema := SchemaOf(ReadRecord{})

	type WriteRecord struct {
		ID   int64
		Data any `parquet:"data"`
	}

	t.Run("InterfaceWithMap", func(t *testing.T) {
		data := []WriteRecord{
			{
				ID:   1,
				Data: map[string]any{"name": "Alice", "age": 30.0},
			},
		}

		buf := new(bytes.Buffer)
		writer := NewGenericWriter[WriteRecord](buf, schema)

		if _, err := writer.Write(data); err != nil {
			t.Fatalf("failed to write: %v", err)
		}
		writer.Close()

		reader := NewReader(bytes.NewReader(buf.Bytes()))
		defer reader.Close()

		var result ReadRecord
		if err := reader.Read(&result); err != nil {
			t.Fatalf("failed to read: %v", err)
		}

		var unmarshaled map[string]any
		if err := json.Unmarshal(result.Data, &unmarshaled); err != nil {
			t.Fatalf("failed to unmarshal JSON: %v", err)
		}

		if unmarshaled["name"] != "Alice" || unmarshaled["age"] != 30.0 {
			t.Errorf("data incorrect: %+v", unmarshaled)
		}
	})

	t.Run("InterfaceWithSlice", func(t *testing.T) {
		data := []WriteRecord{
			{
				ID:   1,
				Data: []string{"a", "b", "c"},
			},
		}

		buf := new(bytes.Buffer)
		writer := NewGenericWriter[WriteRecord](buf, schema)

		if _, err := writer.Write(data); err != nil {
			t.Fatalf("failed to write: %v", err)
		}
		writer.Close()

		reader := NewReader(bytes.NewReader(buf.Bytes()))
		defer reader.Close()

		var result ReadRecord
		if err := reader.Read(&result); err != nil {
			t.Fatalf("failed to read: %v", err)
		}

		var unmarshaled []string
		if err := json.Unmarshal(result.Data, &unmarshaled); err != nil {
			t.Fatalf("failed to unmarshal JSON: %v", err)
		}

		if !reflect.DeepEqual(unmarshaled, []string{"a", "b", "c"}) {
			t.Errorf("data incorrect: %+v", unmarshaled)
		}
	})

	t.Run("InterfaceWithPrimitive", func(t *testing.T) {
		data := []WriteRecord{
			{
				ID:   1,
				Data: 42.0,
			},
		}

		buf := new(bytes.Buffer)
		writer := NewGenericWriter[WriteRecord](buf, schema)

		if _, err := writer.Write(data); err != nil {
			t.Fatalf("failed to write: %v", err)
		}
		writer.Close()

		reader := NewReader(bytes.NewReader(buf.Bytes()))
		defer reader.Close()

		var result ReadRecord
		if err := reader.Read(&result); err != nil {
			t.Fatalf("failed to read: %v", err)
		}

		var unmarshaled float64
		if err := json.Unmarshal(result.Data, &unmarshaled); err != nil {
			t.Fatalf("failed to unmarshal JSON: %v", err)
		}

		if unmarshaled != 42.0 {
			t.Errorf("data incorrect: %v", unmarshaled)
		}
	})
}

// TestWriteSliceOfAnyToJSONByteArray tests that a `[]any` field with mixed types
// is properly JSON serialized to a byte array column.
func TestWriteSliceOfAnyToJSONByteArray(t *testing.T) {
	type ReadRecord struct {
		ID    int64  `parquet:"id"`
		Items []byte `parquet:"items,json"`
	}
	schema := SchemaOf(ReadRecord{})

	type WriteRecord struct {
		ID    int64 `parquet:"id"`
		Items []any `parquet:"items"`
	}

	data := []WriteRecord{
		{
			ID:    1,
			Items: []any{"string", 42.0, map[string]string{"key": "val"}, true, nil},
		},
	}

	buf := new(bytes.Buffer)
	writer := NewGenericWriter[WriteRecord](buf, schema)

	if _, err := writer.Write(data); err != nil {
		t.Fatalf("failed to write: %v", err)
	}
	writer.Close()

	reader := NewReader(bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	var result ReadRecord
	if err := reader.Read(&result); err != nil {
		t.Fatalf("failed to read: %v", err)
	}

	var unmarshaled []any
	if err := json.Unmarshal(result.Items, &unmarshaled); err != nil {
		t.Fatalf("failed to unmarshal JSON: %v", err)
	}

	if len(unmarshaled) != 5 {
		t.Errorf("expected 5 items, got %d", len(unmarshaled))
	}
	if unmarshaled[0] != "string" || unmarshaled[1] != 42.0 || unmarshaled[3] != true || unmarshaled[4] != nil {
		t.Errorf("items incorrect: %+v", unmarshaled)
	}
}

// TestWriteNestedCompositeToJSONByteArray tests deeply nested composite structures.
func TestWriteNestedCompositeToJSONByteArray(t *testing.T) {
	type ReadRecord struct {
		ID      int64  `parquet:"id"`
		Complex []byte `parquet:"complex,json"`
	}
	schema := SchemaOf(ReadRecord{})

	type WriteRecord struct {
		ID      int64                       `parquet:"id"`
		Complex map[string][]map[string]any `parquet:"complex"`
	}

	data := []WriteRecord{
		{
			ID: 1,
			Complex: map[string][]map[string]any{
				"users": {
					{"name": "Alice", "age": 30.0},
					{"name": "Bob", "age": 25.0},
				},
				"admins": {
					{"name": "Charlie", "permissions": []any{"read", "write"}},
				},
			},
		},
	}

	buf := new(bytes.Buffer)
	writer := NewGenericWriter[WriteRecord](buf, schema)

	if _, err := writer.Write(data); err != nil {
		t.Fatalf("failed to write: %v", err)
	}
	writer.Close()

	reader := NewReader(bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	var result ReadRecord
	if err := reader.Read(&result); err != nil {
		t.Fatalf("failed to read: %v", err)
	}

	var unmarshaled map[string][]map[string]any
	if err := json.Unmarshal(result.Complex, &unmarshaled); err != nil {
		t.Fatalf("failed to unmarshal JSON: %v", err)
	}

	if len(unmarshaled["users"]) != 2 {
		t.Errorf("expected 2 users, got %d", len(unmarshaled["users"]))
	}
	if unmarshaled["users"][0]["name"] != "Alice" {
		t.Errorf("user 0 name incorrect: %v", unmarshaled["users"][0]["name"])
	}
}

// TestRoundTripJSONSerialization tests that data can be written as composite types
// and read back correctly through JSON serialization/deserialization.
func TestRoundTripJSONSerialization(t *testing.T) {
	t.Run("MapRoundTrip", func(t *testing.T) {
		type Record struct {
			ID   int64             `parquet:"id"`
			Data map[string]string `parquet:"data,json"`
		}

		original := []Record{
			{
				ID:   1,
				Data: map[string]string{"key1": "val1", "key2": "val2"},
			},
		}

		buf := new(bytes.Buffer)
		if err := Write(buf, original); err != nil {
			t.Fatalf("failed to write: %v", err)
		}

		result, err := Read[Record](bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		if err != nil {
			t.Fatalf("failed to read: %v", err)
		}

		if !reflect.DeepEqual(original, result) {
			t.Errorf("round trip failed:\noriginal: %+v\nresult:   %+v", original, result)
		}
	})

	t.Run("StructRoundTrip", func(t *testing.T) {
		type Inner struct {
			Name  string
			Count int
		}

		type Record struct {
			ID   int64 `parquet:"id"`
			Data Inner `parquet:"data,json"`
		}

		original := []Record{
			{
				ID:   1,
				Data: Inner{Name: "test", Count: 42},
			},
		}

		buf := new(bytes.Buffer)
		if err := Write(buf, original); err != nil {
			t.Fatalf("failed to write: %v", err)
		}

		result, err := Read[Record](bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		if err != nil {
			t.Fatalf("failed to read: %v", err)
		}

		if !reflect.DeepEqual(original, result) {
			t.Errorf("round trip failed:\noriginal: %+v\nresult:   %+v", original, result)
		}
	})

	t.Run("SliceRoundTrip", func(t *testing.T) {
		type Record struct {
			ID    int64   `parquet:"id"`
			Items []int64 `parquet:"items,json"`
		}

		original := []Record{
			{
				ID:    1,
				Items: []int64{10, 20, 30},
			},
		}

		buf := new(bytes.Buffer)
		if err := Write(buf, original); err != nil {
			t.Fatalf("failed to write: %v", err)
		}

		result, err := Read[Record](bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		if err != nil {
			t.Fatalf("failed to read: %v", err)
		}

		if !reflect.DeepEqual(original, result) {
			t.Errorf("round trip failed:\noriginal: %+v\nresult:   %+v", original, result)
		}
	})
}

// TestPrimitiveTypesBypassJSON tests that primitive types and []byte are written
// directly to byte array columns without JSON serialization.
func TestPrimitiveTypesBypassJSON(t *testing.T) {
	t.Run("StringDirect", func(t *testing.T) {
		type Record struct {
			ID   int64
			Data string `parquet:"data,json"`
		}

		original := []Record{
			{
				ID:   1,
				Data: "plain text",
			},
		}

		buf := new(bytes.Buffer)
		if err := Write(buf, original); err != nil {
			t.Fatalf("failed to write: %v", err)
		}

		result, err := Read[Record](bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		if err != nil {
			t.Fatalf("failed to read: %v", err)
		}

		// String should be written directly, not as JSON-encoded string
		if result[0].Data != "plain text" {
			t.Errorf("expected 'plain text', got %q", result[0].Data)
		}

		// Verify it's not JSON-encoded (would be "\"plain text\"" if it were)
		if result[0].Data == `"plain text"` {
			t.Error("string was JSON-encoded when it should have been written directly")
		}
	})

	t.Run("ByteSliceDirect", func(t *testing.T) {
		type Record struct {
			ID   int64
			Data []byte `parquet:"data,json"`
		}

		original := []Record{
			{
				ID:   1,
				Data: []byte("binary data"),
			},
		}

		buf := new(bytes.Buffer)
		if err := Write(buf, original); err != nil {
			t.Fatalf("failed to write: %v", err)
		}

		result, err := Read[Record](bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		if err != nil {
			t.Fatalf("failed to read: %v", err)
		}

		// []byte should be written directly
		if !bytes.Equal(result[0].Data, []byte("binary data")) {
			t.Errorf("expected 'binary data', got %q", result[0].Data)
		}
	})
}
