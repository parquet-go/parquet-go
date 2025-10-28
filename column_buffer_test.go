package parquet

import (
	"bytes"
	"fmt"
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
	for i := 0; i < b.N; i++ {
		broadcastValueInt32(buf, -1)
	}
	b.SetBytes(4 * int64(len(buf)))
}

func BenchmarkBroadcastRangeInt32(b *testing.B) {
	buf := make([]int32, 1000)
	for i := 0; i < b.N; i++ {
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

	t.Logf("Desired schema:\n%s", desiredSchema)

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

	t.Logf("Successfully wrote %d bytes", buf.Len())

	// Try to read it back
	reader := NewReader(bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	var result RecordWithStruct
	if err := reader.Read(&result); err != nil {
		t.Fatalf("failed to read row: %v", err)
	}

	t.Logf("Read back: %+v", result)
}

// TestWhatActuallyHappensWithMapField tests what schema is generated when
// you use a map[string]string field - it creates a MAP logical type, not a GROUP.
func TestWhatActuallyHappensWithMapField(t *testing.T) {
	type RecordWithMap struct {
		Nested map[string]string
	}

	// Get the schema that's naturally generated from the map type
	naturalSchema := SchemaOf(RecordWithMap{})

	fmt.Printf("Natural schema from map[string]string:\n%s\n", naturalSchema)

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

	// Read it back and check the schema
	reader := NewReader(bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	fmt.Printf("\nActual file schema:\n%s\n", reader.Schema())
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

	t.Logf("Desired schema:\n%s", desiredSchema)

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

	t.Logf("Successfully wrote %d bytes", buf.Len())

	// Try to read it back
	reader := NewReader(bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	var result1 RecordWithStruct
	if err := reader.Read(&result1); err != nil {
		t.Fatalf("failed to read row 1: %v", err)
	}
	t.Logf("Row 1: %+v", result1)

	if result1.Nested.A != "value_a1" || result1.Nested.B != "value_b1" || result1.Nested.C != "value_c1" {
		t.Errorf("row 1 values incorrect: %+v", result1.Nested)
	}

	var result2 RecordWithStruct
	if err := reader.Read(&result2); err != nil {
		t.Fatalf("failed to read row 2: %v", err)
	}
	t.Logf("Row 2: %+v", result2)

	if result2.Nested.A != "value_a2" || result2.Nested.B != "value_b2" || result2.Nested.C != "" {
		t.Errorf("row 2 values incorrect: %+v", result2.Nested)
	}
}
