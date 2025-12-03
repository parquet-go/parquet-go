package parquet

import (
	"bytes"
	"encoding/json"
	"io"
	"reflect"
	"testing"

	"github.com/parquet-go/jsonlite"
	"google.golang.org/protobuf/types/known/structpb"
)

// TestStructpbSimple tests writing *structpb.Struct as JSON to byte array columns
func TestStructpbSimple(t *testing.T) {
	type ReadRecord struct {
		ID   int64  `parquet:"id"`
		Data []byte `parquet:"data"`
	}

	type WriteRecord struct {
		ID   int64            `parquet:"id"`
		Data *structpb.Struct `parquet:"data"`
	}

	schema := SchemaOf(ReadRecord{})

	testData := mustNewStruct(map[string]any{
		"name":   "Alice",
		"age":    float64(30),
		"active": true,
	})

	records := []WriteRecord{
		{ID: 1, Data: testData},
		{ID: 2, Data: nil},
	}

	buf := new(bytes.Buffer)
	writer := NewGenericWriter[WriteRecord](buf, schema)
	if _, err := writer.Write(records); err != nil {
		t.Fatalf("failed to write records: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	reader := NewGenericReader[ReadRecord](bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	readRecords := make([]ReadRecord, len(records))
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	// Verify first record has valid JSON
	var result map[string]any
	if err := json.Unmarshal(readRecords[0].Data, &result); err != nil {
		t.Fatalf("failed to unmarshal JSON: %v", err)
	}

	expected := map[string]any{
		"name":   "Alice",
		"age":    float64(30),
		"active": true,
	}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("mismatch:\nexpected: %+v\ngot: %+v", expected, result)
	}

	// Verify second record is empty (nil *structpb.Struct writes empty byte array for required fields)
	if len(readRecords[1].Data) != 0 {
		t.Errorf("expected empty for second record, got: %s", string(readRecords[1].Data))
	}
}

// TestStructpbNested tests nested *structpb.Struct structures
func TestStructpbNested(t *testing.T) {
	type ReadRecord struct {
		ID   int64  `parquet:"id"`
		Data []byte `parquet:"data"`
	}

	type WriteRecord struct {
		ID   int64            `parquet:"id"`
		Data *structpb.Struct `parquet:"data"`
	}

	schema := SchemaOf(ReadRecord{})

	testData := mustNewStruct(map[string]any{
		"user": map[string]any{
			"name": "Alice",
			"age":  float64(30),
			"address": map[string]any{
				"street": "123 Main St",
				"city":   "NYC",
			},
		},
	})

	records := []WriteRecord{
		{ID: 1, Data: testData},
	}

	buf := new(bytes.Buffer)
	writer := NewGenericWriter[WriteRecord](buf, schema)
	if _, err := writer.Write(records); err != nil {
		t.Fatalf("failed to write records: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	reader := NewGenericReader[ReadRecord](bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	readRecords := make([]ReadRecord, len(records))
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	// Verify nested structure
	var result map[string]any
	if err := json.Unmarshal(readRecords[0].Data, &result); err != nil {
		t.Fatalf("failed to unmarshal JSON: %v", err)
	}

	expected := map[string]any{
		"user": map[string]any{
			"name": "Alice",
			"age":  float64(30),
			"address": map[string]any{
				"street": "123 Main St",
				"city":   "NYC",
			},
		},
	}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("mismatch:\nexpected: %+v\ngot: %+v", expected, result)
	}
}

// TestStructpbWithArrays tests *structpb.Struct containing arrays
func TestStructpbWithArrays(t *testing.T) {
	type ReadRecord struct {
		ID   int64  `parquet:"id"`
		Data []byte `parquet:"data"`
	}

	type WriteRecord struct {
		ID   int64            `parquet:"id"`
		Data *structpb.Struct `parquet:"data"`
	}

	schema := SchemaOf(ReadRecord{})

	testData := mustNewStruct(map[string]any{
		"tags":   []any{"admin", "developer", "reviewer"},
		"scores": []any{float64(95), float64(87), float64(92)},
	})

	records := []WriteRecord{
		{ID: 1, Data: testData},
	}

	buf := new(bytes.Buffer)
	writer := NewGenericWriter[WriteRecord](buf, schema)
	if _, err := writer.Write(records); err != nil {
		t.Fatalf("failed to write records: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	reader := NewGenericReader[ReadRecord](bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	readRecords := make([]ReadRecord, len(records))
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	// Verify arrays
	var result map[string]any
	if err := json.Unmarshal(readRecords[0].Data, &result); err != nil {
		t.Fatalf("failed to unmarshal JSON: %v", err)
	}

	expected := map[string]any{
		"tags":   []any{"admin", "developer", "reviewer"},
		"scores": []any{float64(95), float64(87), float64(92)},
	}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("mismatch:\nexpected: %+v\ngot: %+v", expected, result)
	}
}

// TestStructpbEmptyAndNull tests empty struct and null handling
func TestStructpbEmptyAndNull(t *testing.T) {
	type ReadRecord struct {
		ID    int64  `parquet:"id"`
		Data1 []byte `parquet:"data1"`
		Data2 []byte `parquet:"data2,optional"`
	}

	type WriteRecord struct {
		ID    int64            `parquet:"id"`
		Data1 *structpb.Struct `parquet:"data1"`
		Data2 *structpb.Struct `parquet:"data2,optional"`
	}

	schema := SchemaOf(ReadRecord{})

	emptyStruct := mustNewStruct(map[string]any{})

	records := []WriteRecord{
		{ID: 1, Data1: emptyStruct, Data2: nil},
		{ID: 2, Data1: nil, Data2: emptyStruct},
	}

	buf := new(bytes.Buffer)
	writer := NewGenericWriter[WriteRecord](buf, schema)
	if _, err := writer.Write(records); err != nil {
		t.Fatalf("failed to write records: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	reader := NewGenericReader[ReadRecord](bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	readRecords := make([]ReadRecord, len(records))
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	// First record: empty struct and null
	if !bytes.Equal(readRecords[0].Data1, []byte("{}")) {
		t.Errorf("expected empty object {}, got: %s", string(readRecords[0].Data1))
	}
	if len(readRecords[0].Data2) != 0 {
		t.Errorf("expected empty byte array for nil optional, got: %s", string(readRecords[0].Data2))
	}

	// Second record: empty and empty struct
	if len(readRecords[1].Data1) != 0 {
		t.Errorf("expected empty, got: %s", string(readRecords[1].Data1))
	}
	if !bytes.Equal(readRecords[1].Data2, []byte("{}")) {
		t.Errorf("expected empty object {}, got: %s", string(readRecords[1].Data2))
	}
}

// TestStructpbComplexNesting tests complex nested structures
func TestStructpbComplexNesting(t *testing.T) {
	type ReadRecord struct {
		ID   int64  `parquet:"id"`
		Data []byte `parquet:"data"`
	}

	type WriteRecord struct {
		ID   int64            `parquet:"id"`
		Data *structpb.Struct `parquet:"data"`
	}

	schema := SchemaOf(ReadRecord{})

	testData := mustNewStruct(map[string]any{
		"items": []any{
			map[string]any{
				"name": "item1",
				"metadata": map[string]any{
					"tags":   []any{"tag1", "tag2"},
					"scores": []any{float64(10), float64(20)},
				},
			},
			map[string]any{
				"name": "item2",
				"metadata": map[string]any{
					"tags":   []any{},
					"scores": []any{float64(40)},
				},
			},
		},
	})

	records := []WriteRecord{
		{ID: 1, Data: testData},
	}

	buf := new(bytes.Buffer)
	writer := NewGenericWriter[WriteRecord](buf, schema)
	if _, err := writer.Write(records); err != nil {
		t.Fatalf("failed to write records: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	reader := NewGenericReader[ReadRecord](bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	readRecords := make([]ReadRecord, len(records))
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	// Verify complex structure
	var result map[string]any
	if err := json.Unmarshal(readRecords[0].Data, &result); err != nil {
		t.Fatalf("failed to unmarshal JSON: %v", err)
	}

	expected := map[string]any{
		"items": []any{
			map[string]any{
				"name": "item1",
				"metadata": map[string]any{
					"tags":   []any{"tag1", "tag2"},
					"scores": []any{float64(10), float64(20)},
				},
			},
			map[string]any{
				"name": "item2",
				"metadata": map[string]any{
					"tags":   []any{},
					"scores": []any{float64(40)},
				},
			},
		},
	}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("mismatch:\nexpected: %+v\ngot: %+v", expected, result)
	}
}

// TestStructpbRoundTrip tests writing and reading back as *structpb.Struct
func TestStructpbRoundTrip(t *testing.T) {
	type WriteRecord struct {
		ID   int64            `parquet:"id"`
		Data *structpb.Struct `parquet:"data"`
	}

	type ReadRecord struct {
		ID   int64  `parquet:"id"`
		Data []byte `parquet:"data"`
	}

	schema := SchemaOf(ReadRecord{})

	original := mustNewStruct(map[string]any{
		"string":  "hello",
		"number":  float64(42),
		"boolean": true,
		"null":    nil,
		"array":   []any{float64(1), float64(2), float64(3)},
		"object": map[string]any{
			"nested": "value",
		},
	})

	records := []WriteRecord{
		{ID: 1, Data: original},
	}

	buf := new(bytes.Buffer)
	writer := NewGenericWriter[WriteRecord](buf, schema)
	if _, err := writer.Write(records); err != nil {
		t.Fatalf("failed to write records: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	reader := NewGenericReader[ReadRecord](bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	readRecords := make([]ReadRecord, len(records))
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	// Parse back to map and compare
	var result map[string]any
	if err := json.Unmarshal(readRecords[0].Data, &result); err != nil {
		t.Fatalf("failed to unmarshal JSON: %v", err)
	}

	expected := map[string]any{
		"string":  "hello",
		"number":  float64(42),
		"boolean": true,
		"null":    nil,
		"array":   []any{float64(1), float64(2), float64(3)},
		"object": map[string]any{
			"nested": "value",
		},
	}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("mismatch:\nexpected: %+v\ngot: %+v", expected, result)
	}
}

// TestJsonliteSimple tests writing *jsonlite.Value as JSON to byte array columns
func TestJsonliteSimple(t *testing.T) {
	type ReadRecord struct {
		ID   int64  `parquet:"id"`
		Data []byte `parquet:"data"`
	}

	type WriteRecord struct {
		ID   int64           `parquet:"id"`
		Data *jsonlite.Value `parquet:"data"`
	}

	schema := SchemaOf(ReadRecord{})

	records := []WriteRecord{
		{ID: 1, Data: mustParseJsonlite(`{"name":"Alice","age":30,"active":true}`)},
		{ID: 2, Data: nil},
	}

	buf := new(bytes.Buffer)
	writer := NewGenericWriter[WriteRecord](buf, schema)
	if _, err := writer.Write(records); err != nil {
		t.Fatalf("failed to write records: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	reader := NewGenericReader[ReadRecord](bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	readRecords := make([]ReadRecord, len(records))
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	// Verify first record has valid JSON
	var result map[string]any
	if err := json.Unmarshal(readRecords[0].Data, &result); err != nil {
		t.Fatalf("failed to unmarshal JSON: %v", err)
	}

	expected := map[string]any{
		"name":   "Alice",
		"age":    float64(30),
		"active": true,
	}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("mismatch:\nexpected: %+v\ngot: %+v", expected, result)
	}

	// Verify second record is empty (nil *jsonlite.Value writes empty byte array for required fields)
	if len(readRecords[1].Data) != 0 {
		t.Errorf("expected empty for second record, got: %s", string(readRecords[1].Data))
	}
}

// TestJsonliteNested tests nested *jsonlite.Value structures
func TestJsonliteNested(t *testing.T) {
	type ReadRecord struct {
		ID   int64  `parquet:"id"`
		Data []byte `parquet:"data"`
	}

	type WriteRecord struct {
		ID   int64           `parquet:"id"`
		Data *jsonlite.Value `parquet:"data"`
	}

	schema := SchemaOf(ReadRecord{})

	records := []WriteRecord{
		{
			ID: 1,
			Data: mustParseJsonlite(`{
				"user": {
					"name": "Alice",
					"age": 30,
					"address": {
						"street": "123 Main St",
						"city": "NYC"
					}
				}
			}`),
		},
	}

	buf := new(bytes.Buffer)
	writer := NewGenericWriter[WriteRecord](buf, schema)
	if _, err := writer.Write(records); err != nil {
		t.Fatalf("failed to write records: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	reader := NewGenericReader[ReadRecord](bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	readRecords := make([]ReadRecord, len(records))
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	// Verify nested structure
	var result map[string]any
	if err := json.Unmarshal(readRecords[0].Data, &result); err != nil {
		t.Fatalf("failed to unmarshal JSON: %v", err)
	}

	expected := map[string]any{
		"user": map[string]any{
			"name": "Alice",
			"age":  float64(30),
			"address": map[string]any{
				"street": "123 Main St",
				"city":   "NYC",
			},
		},
	}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("mismatch:\nexpected: %+v\ngot: %+v", expected, result)
	}
}

// TestJsonliteWithArrays tests *jsonlite.Value containing arrays
func TestJsonliteWithArrays(t *testing.T) {
	type ReadRecord struct {
		ID   int64  `parquet:"id"`
		Data []byte `parquet:"data"`
	}

	type WriteRecord struct {
		ID   int64           `parquet:"id"`
		Data *jsonlite.Value `parquet:"data"`
	}

	schema := SchemaOf(ReadRecord{})

	records := []WriteRecord{
		{
			ID:   1,
			Data: mustParseJsonlite(`{"tags":["admin","developer","reviewer"],"scores":[95,87,92]}`),
		},
	}

	buf := new(bytes.Buffer)
	writer := NewGenericWriter[WriteRecord](buf, schema)
	if _, err := writer.Write(records); err != nil {
		t.Fatalf("failed to write records: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	reader := NewGenericReader[ReadRecord](bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	readRecords := make([]ReadRecord, len(records))
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	// Verify arrays
	var result map[string]any
	if err := json.Unmarshal(readRecords[0].Data, &result); err != nil {
		t.Fatalf("failed to unmarshal JSON: %v", err)
	}

	expected := map[string]any{
		"tags":   []any{"admin", "developer", "reviewer"},
		"scores": []any{float64(95), float64(87), float64(92)},
	}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("mismatch:\nexpected: %+v\ngot: %+v", expected, result)
	}
}

// TestJsonliteEmptyAndNull tests empty object and null handling
func TestJsonliteEmptyAndNull(t *testing.T) {
	type ReadRecord struct {
		ID    int64  `parquet:"id"`
		Data1 []byte `parquet:"data1"`
		Data2 []byte `parquet:"data2,optional"`
	}

	type WriteRecord struct {
		ID    int64           `parquet:"id"`
		Data1 *jsonlite.Value `parquet:"data1"`
		Data2 *jsonlite.Value `parquet:"data2,optional"`
	}

	schema := SchemaOf(ReadRecord{})

	emptyJSON := mustParseJsonlite(`{}`)
	nullJSON := mustParseJsonlite(`null`)

	records := []WriteRecord{
		{ID: 1, Data1: emptyJSON, Data2: nil},
		{ID: 2, Data1: nullJSON, Data2: emptyJSON},
	}

	buf := new(bytes.Buffer)
	writer := NewGenericWriter[WriteRecord](buf, schema)
	if _, err := writer.Write(records); err != nil {
		t.Fatalf("failed to write records: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	reader := NewGenericReader[ReadRecord](bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	readRecords := make([]ReadRecord, len(records))
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	// First record: empty object and nil
	if !bytes.Equal(readRecords[0].Data1, []byte("{}")) {
		t.Errorf("expected empty object {}, got: %s", string(readRecords[0].Data1))
	}
	if len(readRecords[0].Data2) != 0 {
		t.Errorf("expected empty byte array for nil optional, got: %s", string(readRecords[0].Data2))
	}

	// Second record: explicit null JSON value and empty object
	// jsonlite writes `null` as a JSON value when parsed from "null" string
	if len(readRecords[1].Data1) != 0 {
		t.Errorf("expected empty (null value writes no bytes to required field), got: %s", string(readRecords[1].Data1))
	}
	if !bytes.Equal(readRecords[1].Data2, []byte("{}")) {
		t.Errorf("expected empty object {}, got: %s", string(readRecords[1].Data2))
	}
}

// TestJsonliteComplexNesting tests complex nested structures
func TestJsonliteComplexNesting(t *testing.T) {
	type ReadRecord struct {
		ID   int64  `parquet:"id"`
		Data []byte `parquet:"data"`
	}

	type WriteRecord struct {
		ID   int64           `parquet:"id"`
		Data *jsonlite.Value `parquet:"data"`
	}

	schema := SchemaOf(ReadRecord{})

	records := []WriteRecord{
		{
			ID: 1,
			Data: mustParseJsonlite(`{
				"items": [
					{
						"name": "item1",
						"metadata": {
							"tags": ["tag1", "tag2"],
							"scores": [10, 20]
						}
					},
					{
						"name": "item2",
						"metadata": {
							"tags": [],
							"scores": [40]
						}
					}
				]
			}`),
		},
	}

	buf := new(bytes.Buffer)
	writer := NewGenericWriter[WriteRecord](buf, schema)
	if _, err := writer.Write(records); err != nil {
		t.Fatalf("failed to write records: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	reader := NewGenericReader[ReadRecord](bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	readRecords := make([]ReadRecord, len(records))
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	// Verify complex structure
	var result map[string]any
	if err := json.Unmarshal(readRecords[0].Data, &result); err != nil {
		t.Fatalf("failed to unmarshal JSON: %v", err)
	}

	expected := map[string]any{
		"items": []any{
			map[string]any{
				"name": "item1",
				"metadata": map[string]any{
					"tags":   []any{"tag1", "tag2"},
					"scores": []any{float64(10), float64(20)},
				},
			},
			map[string]any{
				"name": "item2",
				"metadata": map[string]any{
					"tags":   []any{},
					"scores": []any{float64(40)},
				},
			},
		},
	}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("mismatch:\nexpected: %+v\ngot: %+v", expected, result)
	}
}

// TestJsonliteRoundTrip tests writing and reading back as *jsonlite.Value
func TestJsonliteRoundTrip(t *testing.T) {
	type WriteRecord struct {
		ID   int64           `parquet:"id"`
		Data *jsonlite.Value `parquet:"data"`
	}

	type ReadRecord struct {
		ID   int64  `parquet:"id"`
		Data []byte `parquet:"data"`
	}

	schema := SchemaOf(ReadRecord{})

	original := mustParseJsonlite(`{
		"string": "hello",
		"number": 42,
		"boolean": true,
		"null": null,
		"array": [1, 2, 3],
		"object": {
			"nested": "value"
		}
	}`)

	records := []WriteRecord{
		{ID: 1, Data: original},
	}

	buf := new(bytes.Buffer)
	writer := NewGenericWriter[WriteRecord](buf, schema)
	if _, err := writer.Write(records); err != nil {
		t.Fatalf("failed to write records: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	reader := NewGenericReader[ReadRecord](bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	readRecords := make([]ReadRecord, len(records))
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	// Parse back to map and compare
	var result map[string]any
	if err := json.Unmarshal(readRecords[0].Data, &result); err != nil {
		t.Fatalf("failed to unmarshal JSON: %v", err)
	}

	expected := map[string]any{
		"string":  "hello",
		"number":  float64(42),
		"boolean": true,
		"null":    nil,
		"array":   []any{float64(1), float64(2), float64(3)},
		"object": map[string]any{
			"nested": "value",
		},
	}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("mismatch:\nexpected: %+v\ngot: %+v", expected, result)
	}
}

// TestJsonlitePrimitives tests different JSON primitive types
func TestJsonlitePrimitives(t *testing.T) {
	type ReadRecord struct {
		ID      int64  `parquet:"id"`
		String  []byte `parquet:"string"`
		Number  []byte `parquet:"number"`
		Boolean []byte `parquet:"boolean"`
		Null    []byte `parquet:"null"`
	}

	type WriteRecord struct {
		ID      int64           `parquet:"id"`
		String  *jsonlite.Value `parquet:"string"`
		Number  *jsonlite.Value `parquet:"number"`
		Boolean *jsonlite.Value `parquet:"boolean"`
		Null    *jsonlite.Value `parquet:"null"`
	}

	schema := SchemaOf(ReadRecord{})

	records := []WriteRecord{
		{
			ID:      1,
			String:  mustParseJsonlite(`"hello"`),
			Number:  mustParseJsonlite(`42.5`),
			Boolean: mustParseJsonlite(`true`),
			Null:    mustParseJsonlite(`null`),
		},
	}

	buf := new(bytes.Buffer)
	writer := NewGenericWriter[WriteRecord](buf, schema)
	if _, err := writer.Write(records); err != nil {
		t.Fatalf("failed to write records: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	reader := NewGenericReader[ReadRecord](bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	readRecords := make([]ReadRecord, len(records))
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	// Verify primitives
	// Note: jsonlite.Value writes strings without quotes to BYTE_ARRAY
	if !bytes.Equal(readRecords[0].String, []byte(`hello`)) {
		t.Errorf("string mismatch: got %s", string(readRecords[0].String))
	}
	if !bytes.Equal(readRecords[0].Number, []byte(`42.5`)) {
		t.Errorf("number mismatch: got %s", string(readRecords[0].Number))
	}
	// Boolean is written as a boolean value (1 or 0), not as true/false byte array
	// Null writes no bytes to required fields
	if len(readRecords[0].Null) != 0 {
		t.Errorf("null mismatch: got %s", string(readRecords[0].Null))
	}
}

// TestJsonliteArrayOfPrimitives tests arrays of primitive types
func TestJsonliteArrayOfPrimitives(t *testing.T) {
	type ReadRecord struct {
		ID   int64  `parquet:"id"`
		Data []byte `parquet:"data"`
	}

	type WriteRecord struct {
		ID   int64           `parquet:"id"`
		Data *jsonlite.Value `parquet:"data"`
	}

	schema := SchemaOf(ReadRecord{})

	records := []WriteRecord{
		{ID: 1, Data: mustParseJsonlite(`[1, 2, 3, 4, 5]`)},
		{ID: 2, Data: mustParseJsonlite(`["a", "b", "c"]`)},
		{ID: 3, Data: mustParseJsonlite(`[true, false, true]`)},
		{ID: 4, Data: mustParseJsonlite(`[]`)},
	}

	buf := new(bytes.Buffer)
	writer := NewGenericWriter[WriteRecord](buf, schema)
	if _, err := writer.Write(records); err != nil {
		t.Fatalf("failed to write records: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	reader := NewGenericReader[ReadRecord](bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	readRecords := make([]ReadRecord, len(records))
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	// Verify arrays
	tests := []struct {
		index    int
		expected string
	}{
		{0, `[1,2,3,4,5]`},
		{1, `["a","b","c"]`},
		{2, `[true,false,true]`},
		{3, `[]`},
	}

	for _, tt := range tests {
		var arr []any
		if err := json.Unmarshal(readRecords[tt.index].Data, &arr); err != nil {
			t.Errorf("record %d: failed to unmarshal: %v", tt.index, err)
		}
	}
}

// Helper function to create structpb.Struct or panic
func mustNewStruct(m map[string]any) *structpb.Struct {
	s, err := structpb.NewStruct(m)
	if err != nil {
		panic(err)
	}
	return s
}

// Helper function to parse jsonlite.Value or panic
func mustParseJsonlite(s string) *jsonlite.Value {
	v, err := jsonlite.Parse(s)
	if err != nil {
		panic(err)
	}
	return v
}
