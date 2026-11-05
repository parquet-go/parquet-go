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

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

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

	testData := must(structpb.NewStruct(map[string]any{
		"name":   "Alice",
		"age":    float64(30),
		"active": true,
	}))

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

	if len(readRecords[1].Data) != 0 {
		t.Errorf("expected empty for second record, got: %s", string(readRecords[1].Data))
	}
}

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

	testData := must(structpb.NewStruct(map[string]any{
		"user": map[string]any{
			"name": "Alice",
			"age":  float64(30),
			"address": map[string]any{
				"street": "123 Main St",
				"city":   "NYC",
			},
		},
	}))

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

	testData := must(structpb.NewStruct(map[string]any{
		"tags":   []any{"admin", "developer", "reviewer"},
		"scores": []any{float64(95), float64(87), float64(92)},
	}))

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

	emptyStruct := must(structpb.NewStruct(map[string]any{}))

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

	expected := []ReadRecord{
		{ID: 1, Data1: []byte("{}"), Data2: nil},
		{ID: 2, Data1: []byte{}, Data2: []byte("{}")},
	}
	if !reflect.DeepEqual(readRecords, expected) {
		t.Errorf("mismatch:\nexpected: %+v\ngot: %+v", expected, readRecords)
	}
}

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

	testData := must(structpb.NewStruct(map[string]any{
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
	}))

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

	original := must(structpb.NewStruct(map[string]any{
		"string":  "hello",
		"number":  float64(42),
		"boolean": true,
		"null":    nil,
		"array":   []any{float64(1), float64(2), float64(3)},
		"object": map[string]any{
			"nested": "value",
		},
	}))

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
		{ID: 1, Data: must(jsonlite.Parse(`{"name":"Alice","age":30,"active":true}`))},
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

	if len(readRecords[1].Data) != 0 {
		t.Errorf("expected empty for second record, got: %s", string(readRecords[1].Data))
	}
}

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
			Data: must(jsonlite.Parse(`{
				"user": {
					"name": "Alice",
					"age": 30,
					"address": {
						"street": "123 Main St",
						"city": "NYC"
					}
				}
			}`)),
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
			Data: must(jsonlite.Parse(`{"tags":["admin","developer","reviewer"],"scores":[95,87,92]}`)),
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

	emptyJSON := must(jsonlite.Parse(`{}`))
	nullJSON := must(jsonlite.Parse(`null`))

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

	expected := []ReadRecord{
		{ID: 1, Data1: []byte("{}"), Data2: nil},
		{ID: 2, Data1: []byte{}, Data2: []byte("{}")},
	}
	if !reflect.DeepEqual(readRecords, expected) {
		t.Errorf("mismatch:\nexpected: %+v\ngot: %+v", expected, readRecords)
	}
}

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
			Data: must(jsonlite.Parse(`{
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
			}`)),
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

	original := must(jsonlite.Parse(`{
		"string": "hello",
		"number": 42,
		"boolean": true,
		"null": null,
		"array": [1, 2, 3],
		"object": {
			"nested": "value"
		}
	}`))

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
			String:  must(jsonlite.Parse(`"hello"`)),
			Number:  must(jsonlite.Parse(`42.5`)),
			Boolean: must(jsonlite.Parse(`true`)),
			Null:    must(jsonlite.Parse(`null`)),
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

	expected := ReadRecord{
		ID:      1,
		String:  []byte("hello"),
		Number:  []byte("42.5"),
		Boolean: readRecords[0].Boolean,
		Null:    []byte{},
	}
	if !reflect.DeepEqual(readRecords[0], expected) {
		t.Errorf("mismatch:\nexpected: %+v\ngot: %+v", expected, readRecords[0])
	}
}

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
		{ID: 1, Data: must(jsonlite.Parse(`[1, 2, 3, 4, 5]`))},
		{ID: 2, Data: must(jsonlite.Parse(`["a", "b", "c"]`))},
		{ID: 3, Data: must(jsonlite.Parse(`[true, false, true]`))},
		{ID: 4, Data: must(jsonlite.Parse(`[]`))},
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

// TestOptionalListNullVsEmpty tests that nil slices and empty slices are
// distinguishable when written to and read from parquet files with list,optional tags.
// See: https://github.com/parquet-go/parquet-go/issues/7
func TestOptionalListNullVsEmpty(t *testing.T) {
	type Row struct {
		ListTag []int32 `parquet:"list_tag,list,optional"`
	}

	rows := []Row{
		{ListTag: nil},           // null list
		{ListTag: []int32{}},     // empty list
		{ListTag: []int32{1, 2}}, // list with values
	}

	buf := new(bytes.Buffer)
	if err := Write(buf, rows); err != nil {
		t.Fatalf("failed to write parquet: %v", err)
	}

	readRows, err := Read[Row](bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("failed to read parquet: %v", err)
	}

	if len(readRows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(readRows))
	}

	// Row 0: should be nil (null list)
	if readRows[0].ListTag != nil {
		t.Errorf("row 0: expected nil list, got %v", readRows[0].ListTag)
	}

	// Row 1: should be empty slice (not nil)
	if readRows[1].ListTag == nil {
		t.Errorf("row 1: expected empty slice, got nil")
	} else if len(readRows[1].ListTag) != 0 {
		t.Errorf("row 1: expected empty slice, got %v", readRows[1].ListTag)
	}

	// Row 2: should have values [1, 2]
	if len(readRows[2].ListTag) != 2 || readRows[2].ListTag[0] != 1 || readRows[2].ListTag[1] != 2 {
		t.Errorf("row 2: expected [1, 2], got %v", readRows[2].ListTag)
	}
}

func TestEmptyGroupInterface(t *testing.T) {
	type Record struct {
		ID   int64 `parquet:"id"`
		Data any   `parquet:"data"`
	}

	records := []Record{
		{ID: 1, Data: map[string]any{}},             // Empty map
		{ID: 2, Data: map[string]any{"key": "val"}}, // Non-empty map
		{ID: 3, Data: nil},                          // Nil
	}

	buf := new(bytes.Buffer)
	if err := Write(buf, records); err != nil {
		t.Fatalf("failed to write records with empty groups: %v", err)
	}

	readRecords, err := Read[Record](bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("failed to read records: %v", err)
	}

	if len(readRecords) != len(records) {
		t.Errorf("expected %d records, got %d", len(records), len(readRecords))
	}
}

// roundTripRows writes rows with a GenericWriter and reads them back, failing
// the test on any error or panic raised while writing or reading.
func roundTripRows[T any](t *testing.T, rows []T) (out []T) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panic: %v", r)
		}
	}()
	buf := new(bytes.Buffer)
	w := NewGenericWriter[T](buf)
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	out, err := Read[T](bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// TestOptionalJSONDefinitionLevels tests that optional JSON columns backed by
// string, []byte and json.RawMessage values write a single definition level
// for present values, rather than incrementing it twice and panicking in Close.
// See: https://github.com/parquet-go/parquet-go/issues/621
func TestOptionalJSONDefinitionLevels(t *testing.T) {
	t.Run("string", func(t *testing.T) {
		type R struct {
			S string `parquet:"s,json,optional"`
		}
		rows := []R{{S: `{}`}, {}}
		assertRowsEqual(t, rows, roundTripRows(t, rows))
	})
	t.Run("bytes", func(t *testing.T) {
		type R struct {
			B []byte `parquet:"b,json,optional"`
		}
		rows := []R{{B: []byte(`{}`)}, {}}
		assertRowsEqual(t, rows, roundTripRows(t, rows))
	})
	t.Run("rawmessage", func(t *testing.T) {
		type R struct {
			J json.RawMessage `parquet:"j,json,optional"`
		}
		rows := []R{{J: json.RawMessage(`{}`)}, {}}
		assertRowsEqual(t, rows, roundTripRows(t, rows))
	})
	t.Run("rawmessage-nojsontag", func(t *testing.T) {
		type R struct {
			J json.RawMessage `parquet:"j,optional"`
		}
		rows := []R{{J: json.RawMessage(`{}`)}, {}}
		assertRowsEqual(t, rows, roundTripRows(t, rows))
	})
	t.Run("map-value", func(t *testing.T) {
		type R struct {
			M map[string]string `parquet:"m" parquet-value:",json,optional"`
		}
		rows := []R{{M: map[string]string{"a": `{}`, "b": ""}}}
		assertRowsEqual(t, rows, roundTripRows(t, rows))
	})
	t.Run("list-element", func(t *testing.T) {
		t.Skip("optional JSON list elements read back empty; predates issue 621")
		type R struct {
			L []string `parquet:"l,list" parquet-element:",json,optional"`
		}
		rows := []R{{L: []string{`{}`, ""}}}
		assertRowsEqual(t, rows, roundTripRows(t, rows))
	})
	t.Run("ptr-string", func(t *testing.T) {
		t.Skip("non-nil optional JSON *string reads back as nil; predates issue 621")
		type R struct {
			S *string `parquet:"s,json,optional"`
		}
		s := `{}`
		rows := []R{{S: &s}, {}}
		assertRowsEqual(t, rows, roundTripRows(t, rows))
	})
}

// assertRowsEqual fails the test if got does not deeply equal want.
func assertRowsEqual[T any](t *testing.T, want, got []T) {
	t.Helper()
	if !reflect.DeepEqual(want, got) {
		t.Errorf("rows mismatch:\nwant: %+v\ngot:  %+v", want, got)
	}
}
