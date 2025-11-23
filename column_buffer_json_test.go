package parquet

import (
	"bytes"
	"encoding/json"
	"io"
	"reflect"
	"testing"
	"time"
)

// TestJSONRawMessageLeaf tests writing json.RawMessage to leaf columns (primitives)
// This test exercises converting JSON values to actual parquet primitive types
func TestJSONRawMessageLeaf(t *testing.T) {
	type ReadRecord struct {
		ID     int64   `parquet:"id"`
		Name   string  `parquet:"name"`
		Age    int32   `parquet:"age"`
		Active bool    `parquet:"active"`
		Score  float64 `parquet:"score"`
	}

	type WriteRecord struct {
		ID     int64           `parquet:"id"`
		Name   json.RawMessage `parquet:"name"`
		Age    json.RawMessage `parquet:"age"`
		Active json.RawMessage `parquet:"active"`
		Score  json.RawMessage `parquet:"score"`
	}

	schema := SchemaOf(ReadRecord{})

	records := []WriteRecord{
		{
			ID:     1,
			Name:   json.RawMessage(`"Alice"`),
			Age:    json.RawMessage(`30`),
			Active: json.RawMessage(`true`),
			Score:  json.RawMessage(`95.5`),
		},
		{
			ID:     2,
			Name:   json.RawMessage(`"Bob"`),
			Age:    json.RawMessage(`25`),
			Active: json.RawMessage(`false`),
			Score:  json.RawMessage(`87.3`),
		},
		{
			ID:     3,
			Name:   json.RawMessage(`"Charlie"`),
			Age:    json.RawMessage(`35`),
			Active: json.RawMessage(`true`),
			Score:  json.RawMessage(`92.0`),
		},
	}

	expectedRecords := []ReadRecord{
		{ID: 1, Name: "Alice", Age: 30, Active: true, Score: 95.5},
		{ID: 2, Name: "Bob", Age: 25, Active: false, Score: 87.3},
		{ID: 3, Name: "Charlie", Age: 35, Active: true, Score: 92.0},
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

	readRecords := make([]ReadRecord, len(expectedRecords))
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	if !reflect.DeepEqual(expectedRecords, readRecords) {
		t.Errorf("records mismatch:\nexpected: %+v\ngot: %+v", expectedRecords, readRecords)
	}
}

// TestJSONRawMessageGroup tests writing json.RawMessage objects to group columns
func TestJSONRawMessageGroup(t *testing.T) {
	type User struct {
		Name string `parquet:"name"`
		Age  int32  `parquet:"age"`
	}

	type ReadRecord struct {
		ID   int64 `parquet:"id"`
		User User  `parquet:"user"`
	}

	type WriteRecord struct {
		ID   int64           `parquet:"id"`
		User json.RawMessage `parquet:"user"`
	}

	schema := SchemaOf(ReadRecord{})

	records := []WriteRecord{
		{
			ID:   1,
			User: json.RawMessage(`{"name": "Alice", "age": 30}`),
		},
		{
			ID:   2,
			User: json.RawMessage(`{"name": "Bob", "age": 25}`),
		},
		{
			ID:   3,
			User: json.RawMessage(`{"name": "Charlie", "age": 35}`),
		},
	}

	expectedRecords := []ReadRecord{
		{ID: 1, User: User{Name: "Alice", Age: 30}},
		{ID: 2, User: User{Name: "Bob", Age: 25}},
		{ID: 3, User: User{Name: "Charlie", Age: 35}},
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

	readRecords := make([]ReadRecord, len(expectedRecords))
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	if !reflect.DeepEqual(expectedRecords, readRecords) {
		t.Errorf("records mismatch:\nexpected: %+v\ngot: %+v", expectedRecords, readRecords)
	}
}

// TestJSONRawMessageRepeated tests writing json.RawMessage arrays to repeated columns
func TestJSONRawMessageRepeated(t *testing.T) {
	type ReadRecord struct {
		ID       int64    `parquet:"id"`
		Tags     []string `parquet:"tags,list"`
		Scores   []int32  `parquet:"scores,list"`
		Features []bool   `parquet:"features,list"`
	}

	type WriteRecord struct {
		ID       int64           `parquet:"id"`
		Tags     json.RawMessage `parquet:"tags"`
		Scores   json.RawMessage `parquet:"scores"`
		Features json.RawMessage `parquet:"features"`
	}

	schema := SchemaOf(ReadRecord{})

	records := []WriteRecord{
		{
			ID:       1,
			Tags:     json.RawMessage(`["admin", "developer", "reviewer"]`),
			Scores:   json.RawMessage(`[95, 87, 92]`),
			Features: json.RawMessage(`[true, false, true]`),
		},
		{
			ID:       2,
			Tags:     json.RawMessage(`["user"]`),
			Scores:   json.RawMessage(`[80]`),
			Features: json.RawMessage(`[false]`),
		},
		{
			ID:       3,
			Tags:     json.RawMessage(`[]`),
			Scores:   json.RawMessage(`[]`),
			Features: json.RawMessage(`[]`),
		},
	}

	expectedRecords := []ReadRecord{
		{
			ID:       1,
			Tags:     []string{"admin", "developer", "reviewer"},
			Scores:   []int32{95, 87, 92},
			Features: []bool{true, false, true},
		},
		{
			ID:       2,
			Tags:     []string{"user"},
			Scores:   []int32{80},
			Features: []bool{false},
		},
		{
			ID:       3,
			Tags:     []string{},
			Scores:   []int32{},
			Features: []bool{},
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

	readRecords := make([]ReadRecord, len(expectedRecords))
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	if !reflect.DeepEqual(expectedRecords, readRecords) {
		t.Errorf("records mismatch:\nexpected: %+v\ngot: %+v", expectedRecords, readRecords)
	}
}

// TestJSONRawMessageAutoWrapScalar tests auto-wrapping scalars to repeated columns
func TestJSONRawMessageAutoWrapScalar(t *testing.T) {
	type ReadRecord struct {
		ID   int64    `parquet:"id"`
		Tags []string `parquet:"tags,list"`
	}

	type WriteRecord struct {
		ID   int64           `parquet:"id"`
		Tags json.RawMessage `parquet:"tags"`
	}

	schema := SchemaOf(ReadRecord{})

	records := []WriteRecord{
		{
			ID:   1,
			Tags: json.RawMessage(`"single-tag"`), // Scalar string, should auto-wrap
		},
		{
			ID:   2,
			Tags: json.RawMessage(`42`), // Scalar number, should auto-wrap
		},
	}

	expectedRecords := []ReadRecord{
		{ID: 1, Tags: []string{"single-tag"}},
		{ID: 2, Tags: []string{"42"}}, // Number becomes string
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

	readRecords := make([]ReadRecord, len(expectedRecords))
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	if !reflect.DeepEqual(expectedRecords, readRecords) {
		t.Errorf("records mismatch:\nexpected: %+v\ngot: %+v", expectedRecords, readRecords)
	}
}

// TestJSONRawMessageNested tests nested JSON structures
func TestJSONRawMessageNested(t *testing.T) {
	type Address struct {
		Street string `parquet:"street"`
		City   string `parquet:"city"`
	}

	type User struct {
		Name    string  `parquet:"name"`
		Age     int32   `parquet:"age"`
		Address Address `parquet:"address"`
	}

	type ReadRecord struct {
		ID   int64 `parquet:"id"`
		User User  `parquet:"user"`
	}

	type WriteRecord struct {
		ID   int64           `parquet:"id"`
		User json.RawMessage `parquet:"user"`
	}

	schema := SchemaOf(ReadRecord{})

	records := []WriteRecord{
		{
			ID: 1,
			User: json.RawMessage(`{
				"name": "Alice",
				"age": 30,
				"address": {
					"street": "123 Main St",
					"city": "NYC"
				}
			}`),
		},
		{
			ID: 2,
			User: json.RawMessage(`{
				"name": "Bob",
				"age": 25,
				"address": {
					"street": "456 Oak Ave",
					"city": "LA"
				}
			}`),
		},
	}

	expectedRecords := []ReadRecord{
		{
			ID: 1,
			User: User{
				Name: "Alice",
				Age:  30,
				Address: Address{
					Street: "123 Main St",
					City:   "NYC",
				},
			},
		},
		{
			ID: 2,
			User: User{
				Name: "Bob",
				Age:  25,
				Address: Address{
					Street: "456 Oak Ave",
					City:   "LA",
				},
			},
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

	readRecords := make([]ReadRecord, len(expectedRecords))
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	if !reflect.DeepEqual(expectedRecords, readRecords) {
		t.Errorf("records mismatch:\nexpected: %+v\ngot: %+v", expectedRecords, readRecords)
	}
}

// TestJSONRawMessageMissingFields tests JSON objects with missing fields
func TestJSONRawMessageMissingFields(t *testing.T) {
	type User struct {
		Name string `parquet:"name"`
		Age  int32  `parquet:"age"`
	}

	type ReadRecord struct {
		ID   int64 `parquet:"id"`
		User User  `parquet:"user"`
	}

	type WriteRecord struct {
		ID   int64           `parquet:"id"`
		User json.RawMessage `parquet:"user"`
	}

	schema := SchemaOf(ReadRecord{})

	records := []WriteRecord{
		{
			ID:   1,
			User: json.RawMessage(`{"name": "Alice", "age": 30}`),
		},
		{
			ID:   2,
			User: json.RawMessage(`{"name": "Bob"}`), // Missing age field
		},
		{
			ID:   3,
			User: json.RawMessage(`{"age": 35}`), // Missing name field
		},
	}

	expectedRecords := []ReadRecord{
		{ID: 1, User: User{Name: "Alice", Age: 30}},
		{ID: 2, User: User{Name: "Bob", Age: 0}}, // Missing age becomes 0
		{ID: 3, User: User{Name: "", Age: 35}},   // Missing name becomes ""
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

	readRecords := make([]ReadRecord, len(expectedRecords))
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	if !reflect.DeepEqual(expectedRecords, readRecords) {
		t.Errorf("records mismatch:\nexpected: %+v\ngot: %+v", expectedRecords, readRecords)
	}
}

// TestJSONRawMessageExtraFields tests that extra JSON fields are ignored
func TestJSONRawMessageExtraFields(t *testing.T) {
	type User struct {
		Name string `parquet:"name"`
		Age  int32  `parquet:"age"`
	}

	type ReadRecord struct {
		ID   int64 `parquet:"id"`
		User User  `parquet:"user"`
	}

	type WriteRecord struct {
		ID   int64           `parquet:"id"`
		User json.RawMessage `parquet:"user"`
	}

	schema := SchemaOf(ReadRecord{})

	records := []WriteRecord{
		{
			ID: 1,
			User: json.RawMessage(`{
				"name": "Alice",
				"age": 30,
				"email": "alice@example.com",
				"extra": "should be ignored"
			}`),
		},
	}

	expectedRecords := []ReadRecord{
		{ID: 1, User: User{Name: "Alice", Age: 30}},
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

	readRecords := make([]ReadRecord, len(expectedRecords))
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	if !reflect.DeepEqual(expectedRecords, readRecords) {
		t.Errorf("records mismatch:\nexpected: %+v\ngot: %+v", expectedRecords, readRecords)
	}
}

// TestJSONRawMessageOptional tests optional json.RawMessage fields
func TestJSONRawMessageOptional(t *testing.T) {
	type ReadRecord struct {
		ID       int64    `parquet:"id"`
		OptName  *string  `parquet:"opt_name,optional"`
		OptAge   *int32   `parquet:"opt_age,optional"`
		OptScore *float64 `parquet:"opt_score,optional"`
	}

	type WriteRecord struct {
		ID       int64           `parquet:"id"`
		OptName  json.RawMessage `parquet:"opt_name,optional"`
		OptAge   json.RawMessage `parquet:"opt_age,optional"`
		OptScore json.RawMessage `parquet:"opt_score,optional"`
	}

	schema := SchemaOf(ReadRecord{})

	alice := "Alice"
	age30 := int32(30)
	age25 := int32(25)
	score95 := 95.5

	records := []WriteRecord{
		{
			ID:       1,
			OptName:  json.RawMessage(`"Alice"`),
			OptAge:   json.RawMessage(`30`),
			OptScore: json.RawMessage(`95.5`),
		},
		{
			ID:       2,
			OptName:  json.RawMessage(`null`), // Explicit JSON null
			OptAge:   json.RawMessage(`25`),
			OptScore: json.RawMessage(`null`),
		},
		{
			ID:       3,
			OptName:  nil, // nil json.RawMessage
			OptAge:   nil,
			OptScore: nil,
		},
	}

	expectedRecords := []ReadRecord{
		{ID: 1, OptName: &alice, OptAge: &age30, OptScore: &score95},
		{ID: 2, OptName: nil, OptAge: &age25, OptScore: nil},
		{ID: 3, OptName: nil, OptAge: nil, OptScore: nil},
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

	readRecords := make([]ReadRecord, len(expectedRecords))
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	if !reflect.DeepEqual(expectedRecords, readRecords) {
		t.Errorf("records mismatch:\nexpected: %+v\ngot: %+v", expectedRecords, readRecords)
	}
}

func toValue[T any](v *T) T {
	if v == nil {
		return *new(T)
	}
	return *v
}

// TestJSONRawMessageNumericTypes tests various numeric type conversions
func TestJSONRawMessageNumericTypes(t *testing.T) {
	type ReadRecord struct {
		Int32Val  int32   `parquet:"int32_val"`
		Int64Val  int64   `parquet:"int64_val"`
		FloatVal  float32 `parquet:"float_val"`
		DoubleVal float64 `parquet:"double_val"`
	}

	type WriteRecord struct {
		Int32Val  json.RawMessage `parquet:"int32_val"`
		Int64Val  json.RawMessage `parquet:"int64_val"`
		FloatVal  json.RawMessage `parquet:"float_val"`
		DoubleVal json.RawMessage `parquet:"double_val"`
	}

	schema := SchemaOf(ReadRecord{})

	records := []WriteRecord{
		{
			Int32Val:  json.RawMessage(`42`),
			Int64Val:  json.RawMessage(`9223372036854775807`),
			FloatVal:  json.RawMessage(`3.14`),
			DoubleVal: json.RawMessage(`2.718281828459045`),
		},
		{
			Int32Val:  json.RawMessage(`-100`),
			Int64Val:  json.RawMessage(`-9223372036854775808`),
			FloatVal:  json.RawMessage(`-1.5`),
			DoubleVal: json.RawMessage(`-99.999`),
		},
	}

	expectedRecords := []ReadRecord{
		{
			Int32Val:  42,
			Int64Val:  9223372036854775807,
			FloatVal:  3.14,
			DoubleVal: 2.718281828459045,
		},
		{
			Int32Val:  -100,
			Int64Val:  -9223372036854775808,
			FloatVal:  -1.5,
			DoubleVal: -99.999,
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

	readRecords := make([]ReadRecord, len(expectedRecords))
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	if !reflect.DeepEqual(expectedRecords, readRecords) {
		t.Errorf("records mismatch:\nexpected: %+v\ngot: %+v", expectedRecords, readRecords)
	}
}

// TestJSONRawMessageTimestamps tests timestamp logical types
func TestJSONRawMessageTimestamps(t *testing.T) {
	type ReadRecord struct {
		TimestampMillis time.Time `parquet:"timestamp_millis,timestamp(millisecond)"`
		TimestampMicros time.Time `parquet:"timestamp_micros,timestamp(microsecond)"`
		TimestampNanos  time.Time `parquet:"timestamp_nanos,timestamp(nanosecond)"`
	}

	type WriteRecord struct {
		TimestampMillis json.RawMessage `parquet:"timestamp_millis,timestamp(millisecond)"`
		TimestampMicros json.RawMessage `parquet:"timestamp_micros,timestamp(microsecond)"`
		TimestampNanos  json.RawMessage `parquet:"timestamp_nanos,timestamp(nanosecond)"`
	}

	schema := SchemaOf(ReadRecord{})

	// Parse expected values
	timestamp, _ := time.Parse(time.RFC3339, "2024-01-15T10:30:45.123456789Z")

	records := []WriteRecord{
		{
			TimestampMillis: json.RawMessage(`"2024-01-15T10:30:45.123456789Z"`),
			TimestampMicros: json.RawMessage(`"2024-01-15T10:30:45.123456789Z"`),
			TimestampNanos:  json.RawMessage(`"2024-01-15T10:30:45.123456789Z"`),
		},
	}

	expectedRecords := []ReadRecord{
		{
			TimestampMillis: timestamp.Truncate(time.Millisecond),
			TimestampMicros: timestamp.Truncate(time.Microsecond),
			TimestampNanos:  timestamp,
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

	readRecords := make([]ReadRecord, len(expectedRecords))
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	if !reflect.DeepEqual(expectedRecords, readRecords) {
		t.Errorf("records mismatch:\nexpected: %+v\ngot: %+v", expectedRecords, readRecords)
	}
}

// TestJSONRawMessageNullValues tests various null representations
func TestJSONRawMessageNullValues(t *testing.T) {
	type ReadRecord struct {
		ID       int64    `parquet:"id"`
		Name     *string  `parquet:"name,optional"`
		Age      *int32   `parquet:"age,optional"`
		Active   *bool    `parquet:"active,optional"`
		Score    *float64 `parquet:"score,optional"`
		Tags     []string `parquet:"tags,list"`
		Features []bool   `parquet:"features,list"`
	}

	type WriteRecord struct {
		ID       int64           `parquet:"id"`
		Name     json.RawMessage `parquet:"name,optional"`
		Age      json.RawMessage `parquet:"age,optional"`
		Active   json.RawMessage `parquet:"active,optional"`
		Score    json.RawMessage `parquet:"score,optional"`
		Tags     json.RawMessage `parquet:"tags"`
		Features json.RawMessage `parquet:"features"`
	}

	schema := SchemaOf(ReadRecord{})

	name := "Alice"
	records := []WriteRecord{
		{
			ID:       1,
			Name:     json.RawMessage(`"Alice"`),
			Age:      json.RawMessage(`null`),
			Active:   json.RawMessage(`null`),
			Score:    json.RawMessage(`null`),
			Tags:     json.RawMessage(`[]`),     // Empty array
			Features: json.RawMessage(`[null]`), // Array with null element
		},
		{
			ID:       2,
			Name:     nil, // nil RawMessage
			Age:      nil,
			Active:   nil,
			Score:    nil,
			Tags:     json.RawMessage(`[]`),
			Features: json.RawMessage(`[]`),
		},
	}

	expectedRecords := []ReadRecord{
		{
			ID:       1,
			Name:     &name,
			Age:      nil,
			Active:   nil,
			Score:    nil,
			Tags:     []string{},
			Features: []bool{false}, // null becomes zero value
		},
		{
			ID:       2,
			Name:     nil,
			Age:      nil,
			Active:   nil,
			Score:    nil,
			Tags:     []string{},
			Features: []bool{},
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

	readRecords := make([]ReadRecord, len(expectedRecords))
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	if !reflect.DeepEqual(expectedRecords, readRecords) {
		t.Errorf("records mismatch:\nexpected: %+v\ngot: %+v", expectedRecords, readRecords)
	}
}

// TestJSONRawMessageDeepNesting tests deeply nested structures
func TestJSONRawMessageDeepNesting(t *testing.T) {
	type Level3 struct {
		Value string `parquet:"value"`
	}

	type Level2 struct {
		Level3 Level3 `parquet:"level3"`
	}

	type Level1 struct {
		Level2 Level2 `parquet:"level2"`
	}

	type ReadRecord struct {
		ID     int64  `parquet:"id"`
		Level1 Level1 `parquet:"level1"`
	}

	type WriteRecord struct {
		ID     int64           `parquet:"id"`
		Level1 json.RawMessage `parquet:"level1"`
	}

	schema := SchemaOf(ReadRecord{})

	records := []WriteRecord{
		{
			ID: 1,
			Level1: json.RawMessage(`{
				"level2": {
					"level3": {
						"value": "deeply nested"
					}
				}
			}`),
		},
	}

	expectedRecords := []ReadRecord{
		{
			ID: 1,
			Level1: Level1{
				Level2: Level2{
					Level3: Level3{
						Value: "deeply nested",
					},
				},
			},
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

	readRecords := make([]ReadRecord, len(expectedRecords))
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	if !reflect.DeepEqual(expectedRecords, readRecords) {
		t.Errorf("records mismatch:\nexpected: %+v\ngot: %+v", expectedRecords, readRecords)
	}
}

// TestJSONRawMessageListsOfStructs tests arrays of nested objects
func TestJSONRawMessageListsOfStructs(t *testing.T) {
	type Item struct {
		Name  string `parquet:"name"`
		Price int32  `parquet:"price"`
	}

	type ReadRecord struct {
		ID    int64  `parquet:"id"`
		Items []Item `parquet:"items,list"`
	}

	type WriteRecord struct {
		ID    int64           `parquet:"id"`
		Items json.RawMessage `parquet:"items"`
	}

	schema := SchemaOf(ReadRecord{})

	records := []WriteRecord{
		{
			ID: 1,
			Items: json.RawMessage(`[
				{"name": "apple", "price": 100},
				{"name": "banana", "price": 50},
				{"name": "orange", "price": 75}
			]`),
		},
		{
			ID:    2,
			Items: json.RawMessage(`[]`), // Empty list
		},
	}

	expectedRecords := []ReadRecord{
		{
			ID: 1,
			Items: []Item{
				{Name: "apple", Price: 100},
				{Name: "banana", Price: 50},
				{Name: "orange", Price: 75},
			},
		},
		{
			ID:    2,
			Items: []Item{},
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

	readRecords := make([]ReadRecord, len(expectedRecords))
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	if !reflect.DeepEqual(expectedRecords, readRecords) {
		t.Errorf("records mismatch:\nexpected: %+v\ngot: %+v", expectedRecords, readRecords)
	}
}

// TestJSONRawMessageStructsWithLists tests nested structures containing arrays
func TestJSONRawMessageStructsWithLists(t *testing.T) {
	type Contact struct {
		Name   string   `parquet:"name"`
		Emails []string `parquet:"emails,list"`
		Phones []string `parquet:"phones,list"`
	}

	type ReadRecord struct {
		ID       int64   `parquet:"id"`
		Contact  Contact `parquet:"contact"`
		Metadata Contact `parquet:"metadata"`
	}

	type WriteRecord struct {
		ID       int64           `parquet:"id"`
		Contact  json.RawMessage `parquet:"contact"`
		Metadata json.RawMessage `parquet:"metadata"`
	}

	schema := SchemaOf(ReadRecord{})

	records := []WriteRecord{
		{
			ID: 1,
			Contact: json.RawMessage(`{
				"name": "Alice",
				"emails": ["alice@work.com", "alice@home.com"],
				"phones": ["555-1234"]
			}`),
			Metadata: json.RawMessage(`{
				"name": "Bob",
				"emails": [],
				"phones": ["555-5678", "555-9012"]
			}`),
		},
	}

	expectedRecords := []ReadRecord{
		{
			ID: 1,
			Contact: Contact{
				Name:   "Alice",
				Emails: []string{"alice@work.com", "alice@home.com"},
				Phones: []string{"555-1234"},
			},
			Metadata: Contact{
				Name:   "Bob",
				Emails: []string{},
				Phones: []string{"555-5678", "555-9012"},
			},
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

	readRecords := make([]ReadRecord, len(expectedRecords))
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	if !reflect.DeepEqual(expectedRecords, readRecords) {
		t.Errorf("records mismatch:\nexpected: %+v\ngot: %+v", expectedRecords, readRecords)
	}
}

// TestJSONRawMessageMixedNesting tests complex combination of nesting patterns
func TestJSONRawMessageMixedNesting(t *testing.T) {
	type Metadata struct {
		Tags   []string `parquet:"tags,list"`
		Scores []int32  `parquet:"scores,list"`
	}

	type Item struct {
		Name     string   `parquet:"name"`
		Metadata Metadata `parquet:"metadata"`
	}

	type ReadRecord struct {
		ID    int64  `parquet:"id"`
		Items []Item `parquet:"items,list"`
	}

	type WriteRecord struct {
		ID    int64           `parquet:"id"`
		Items json.RawMessage `parquet:"items"`
	}

	schema := SchemaOf(ReadRecord{})

	records := []WriteRecord{
		{
			ID: 1,
			Items: json.RawMessage(`[
				{
					"name": "item1",
					"metadata": {
						"tags": ["tag1", "tag2"],
						"scores": [10, 20, 30]
					}
				},
				{
					"name": "item2",
					"metadata": {
						"tags": [],
						"scores": [40]
					}
				}
			]`),
		},
	}

	expectedRecords := []ReadRecord{
		{
			ID: 1,
			Items: []Item{
				{
					Name: "item1",
					Metadata: Metadata{
						Tags:   []string{"tag1", "tag2"},
						Scores: []int32{10, 20, 30},
					},
				},
				{
					Name: "item2",
					Metadata: Metadata{
						Tags:   []string{},
						Scores: []int32{40},
					},
				},
			},
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

	readRecords := make([]ReadRecord, len(expectedRecords))
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	if !reflect.DeepEqual(expectedRecords, readRecords) {
		t.Errorf("records mismatch:\nexpected: %+v\ngot: %+v", expectedRecords, readRecords)
	}
}

// TestJSONRawMessageNumberConversions tests number to ByteArray and Boolean
func TestJSONRawMessageNumberConversions(t *testing.T) {
	type ReadRecord struct {
		ID         int64  `parquet:"id"`
		NumAsBytes []byte `parquet:"num_as_bytes"`
		NumAsBool  bool   `parquet:"num_as_bool"`
	}

	type WriteRecord struct {
		ID         int64           `parquet:"id"`
		NumAsBytes json.RawMessage `parquet:"num_as_bytes"`
		NumAsBool  json.RawMessage `parquet:"num_as_bool"`
	}

	schema := SchemaOf(ReadRecord{})

	records := []WriteRecord{
		{
			ID:         1,
			NumAsBytes: json.RawMessage(`42`),
			NumAsBool:  json.RawMessage(`1`),
		},
		{
			ID:         2,
			NumAsBytes: json.RawMessage(`3.14`),
			NumAsBool:  json.RawMessage(`0`),
		},
	}

	expectedRecords := []ReadRecord{
		{
			ID:         1,
			NumAsBytes: []byte("42"),
			NumAsBool:  true,
		},
		{
			ID:         2,
			NumAsBytes: []byte("3.14"),
			NumAsBool:  false,
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

	readRecords := make([]ReadRecord, len(expectedRecords))
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	if !reflect.DeepEqual(expectedRecords, readRecords) {
		t.Errorf("records mismatch:\nexpected: %+v\ngot: %+v", expectedRecords, readRecords)
	}
}

// TestJSONRawMessageFixedLenByteArray tests FixedLenByteArray type
func TestJSONRawMessageFixedLenByteArray(t *testing.T) {
	type ReadRecord struct {
		ID   int64    `parquet:"id"`
		UUID [16]byte `parquet:"uuid,fixedlenbytearray"`
	}

	type WriteRecord struct {
		ID   int64           `parquet:"id"`
		UUID json.RawMessage `parquet:"uuid,fixedlenbytearray"`
	}

	schema := SchemaOf(ReadRecord{})

	uuidStr := "0123456789ABCDEF"
	uuid := [16]byte{}
	copy(uuid[:], uuidStr)

	records := []WriteRecord{
		{
			ID:   1,
			UUID: json.RawMessage(`"` + uuidStr + `"`),
		},
	}

	expectedRecords := []ReadRecord{
		{
			ID:   1,
			UUID: uuid,
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

	readRecords := make([]ReadRecord, len(expectedRecords))
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	if !reflect.DeepEqual(expectedRecords, readRecords) {
		t.Errorf("records mismatch:\nexpected: %+v\ngot: %+v", expectedRecords, readRecords)
	}
}

// TestJSONRawMessageNestedJSONToByteArray tests storing nested JSON objects as raw bytes
func TestJSONRawMessageNestedJSONToByteArray(t *testing.T) {
	type ReadRecord struct {
		ID       int64  `parquet:"id"`
		Metadata []byte `parquet:"metadata"`
	}

	type WriteRecord struct {
		ID       int64           `parquet:"id"`
		Metadata json.RawMessage `parquet:"metadata"`
	}

	schema := SchemaOf(ReadRecord{})

	nestedJSON := `{"key":"value","nested":{"array":[1,2,3]}}`

	records := []WriteRecord{
		{
			ID:       1,
			Metadata: json.RawMessage(nestedJSON),
		},
	}

	expectedRecords := []ReadRecord{
		{
			ID:       1,
			Metadata: []byte(nestedJSON),
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

	readRecords := make([]ReadRecord, len(expectedRecords))
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	if !reflect.DeepEqual(expectedRecords, readRecords) {
		t.Errorf("records mismatch:\nexpected: %+v\ngot: %+v", expectedRecords, readRecords)
	}
}

func TestJSONNumberLeafTypes(t *testing.T) {
	type ReadRecord struct {
		Int32Val  int32   `parquet:"int32_val"`
		Int64Val  int64   `parquet:"int64_val"`
		FloatVal  float32 `parquet:"float_val"`
		DoubleVal float64 `parquet:"double_val"`
		BoolVal   bool    `parquet:"bool_val"`
		BytesVal  []byte  `parquet:"bytes_val"`
	}

	type WriteRecord struct {
		Int32Val  json.Number `parquet:"int32_val"`
		Int64Val  json.Number `parquet:"int64_val"`
		FloatVal  any         `parquet:"float_val"`
		DoubleVal any         `parquet:"double_val"`
		BoolVal   any         `parquet:"bool_val"`
		BytesVal  any         `parquet:"bytes_val"`
	}

	schema := SchemaOf(ReadRecord{})

	records := []WriteRecord{
		{
			Int32Val:  json.Number("42"),
			Int64Val:  json.Number("9223372036854775807"),
			FloatVal:  json.Number("3.14"),
			DoubleVal: json.Number("2.718281828459045"),
			BoolVal:   json.Number("1"),
			BytesVal:  json.Number("123.456"),
		},
		{
			Int32Val:  json.Number("-100"),
			Int64Val:  json.Number("-9223372036854775808"),
			FloatVal:  json.Number("-1.5"),
			DoubleVal: json.Number("-99.999"),
			BoolVal:   json.Number("0"),
			BytesVal:  json.Number("789"),
		},
	}

	expectedRecords := []ReadRecord{
		{
			Int32Val:  42,
			Int64Val:  9223372036854775807,
			FloatVal:  3.14,
			DoubleVal: 2.718281828459045,
			BoolVal:   true,
			BytesVal:  []byte("123.456"),
		},
		{
			Int32Val:  -100,
			Int64Val:  -9223372036854775808,
			FloatVal:  -1.5,
			DoubleVal: -99.999,
			BoolVal:   false,
			BytesVal:  []byte("789"),
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

	readRecords := make([]ReadRecord, len(expectedRecords))
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	if !reflect.DeepEqual(expectedRecords, readRecords) {
		t.Errorf("records mismatch:\nexpected: %+v\ngot: %+v", expectedRecords, readRecords)
	}
}

func TestJSONNumberInMapStringAny(t *testing.T) {
	type Metrics struct {
		Count  int32   `parquet:"count"`
		Amount float64 `parquet:"amount"`
		Active bool    `parquet:"active"`
	}

	type ReadRecord struct {
		ID      int64   `parquet:"id"`
		Metrics Metrics `parquet:"metrics"`
	}

	type WriteRecord struct {
		ID      int64          `parquet:"id"`
		Metrics map[string]any `parquet:"metrics"`
	}

	schema := SchemaOf(ReadRecord{})

	records := []WriteRecord{
		{
			ID: 1,
			Metrics: map[string]any{
				"count":  json.Number("42"),
				"amount": json.Number("99.99"),
				"active": json.Number("1"),
			},
		},
		{
			ID: 2,
			Metrics: map[string]any{
				"count":  json.Number("0"),
				"amount": json.Number("0.0"),
				"active": json.Number("0"),
			},
		},
	}

	expectedRecords := []ReadRecord{
		{
			ID: 1,
			Metrics: Metrics{
				Count:  42,
				Amount: 99.99,
				Active: true,
			},
		},
		{
			ID: 2,
			Metrics: Metrics{
				Count:  0,
				Amount: 0.0,
				Active: false,
			},
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

	readRecords := make([]ReadRecord, len(expectedRecords))
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	if !reflect.DeepEqual(expectedRecords, readRecords) {
		t.Errorf("records mismatch:\nexpected: %+v\ngot: %+v", expectedRecords, readRecords)
	}
}

func TestJSONNumberInSliceAny(t *testing.T) {
	type ReadRecord struct {
		ID     int64   `parquet:"id"`
		Values []int32 `parquet:"values,list"`
	}

	type WriteRecord struct {
		ID     int64 `parquet:"id"`
		Values []any `parquet:"values,list"`
	}

	schema := SchemaOf(ReadRecord{})

	records := []WriteRecord{
		{
			ID: 1,
			Values: []any{
				json.Number("1"),
				json.Number("2"),
				json.Number("3"),
			},
		},
		{
			ID: 2,
			Values: []any{
				json.Number("100"),
				json.Number("200"),
			},
		},
	}

	expectedRecords := []ReadRecord{
		{
			ID:     1,
			Values: []int32{1, 2, 3},
		},
		{
			ID:     2,
			Values: []int32{100, 200},
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

	readRecords := make([]ReadRecord, len(expectedRecords))
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	if !reflect.DeepEqual(expectedRecords, readRecords) {
		t.Errorf("records mismatch:\nexpected: %+v\ngot: %+v", expectedRecords, readRecords)
	}
}

func TestJSONNumberInNestedStructures(t *testing.T) {
	type Item struct {
		Name  string  `parquet:"name"`
		Price float64 `parquet:"price"`
		Count int32   `parquet:"count"`
	}

	type ReadRecord struct {
		ID    int64  `parquet:"id"`
		Items []Item `parquet:"items,list"`
	}

	type WriteRecord struct {
		ID    int64            `parquet:"id"`
		Items []map[string]any `parquet:"items,list"`
	}

	schema := SchemaOf(ReadRecord{})

	records := []WriteRecord{
		{
			ID: 1,
			Items: []map[string]any{
				{
					"name":  "apple",
					"price": json.Number("1.99"),
					"count": json.Number("10"),
				},
				{
					"name":  "banana",
					"price": json.Number("0.99"),
					"count": json.Number("20"),
				},
			},
		},
		{
			ID: 2,
			Items: []map[string]any{
				{
					"name":  "orange",
					"price": json.Number("2.50"),
					"count": json.Number("5"),
				},
			},
		},
	}

	expectedRecords := []ReadRecord{
		{
			ID: 1,
			Items: []Item{
				{Name: "apple", Price: 1.99, Count: 10},
				{Name: "banana", Price: 0.99, Count: 20},
			},
		},
		{
			ID: 2,
			Items: []Item{
				{Name: "orange", Price: 2.50, Count: 5},
			},
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

	readRecords := make([]ReadRecord, len(expectedRecords))
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	if !reflect.DeepEqual(expectedRecords, readRecords) {
		t.Errorf("records mismatch:\nexpected: %+v\ngot: %+v", expectedRecords, readRecords)
	}
}

func TestJSONRawMessageNumberToByteArray(t *testing.T) {
	type ReadRecord struct {
		ID    int64  `parquet:"id"`
		Value []byte `parquet:"value"`
	}

	type WriteRecord struct {
		ID    int64           `parquet:"id"`
		Value json.RawMessage `parquet:"value"`
	}

	schema := SchemaOf(ReadRecord{})

	records := []WriteRecord{
		{ID: 1, Value: json.RawMessage(`42`)},
		{ID: 2, Value: json.RawMessage(`3.14`)},
		{ID: 3, Value: json.RawMessage(`-999`)},
	}

	expectedRecords := []ReadRecord{
		{ID: 1, Value: []byte("42")},
		{ID: 2, Value: []byte("3.14")},
		{ID: 3, Value: []byte("-999")},
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

	readRecords := make([]ReadRecord, len(expectedRecords))
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	if !reflect.DeepEqual(expectedRecords, readRecords) {
		t.Errorf("records mismatch:\nexpected: %+v\ngot: %+v", expectedRecords, readRecords)
	}
}

func TestJSONRawMessageNumberToBoolean(t *testing.T) {
	type ReadRecord struct {
		ID    int64 `parquet:"id"`
		Value bool  `parquet:"value"`
	}

	type WriteRecord struct {
		ID    int64           `parquet:"id"`
		Value json.RawMessage `parquet:"value"`
	}

	schema := SchemaOf(ReadRecord{})

	records := []WriteRecord{
		{ID: 1, Value: json.RawMessage(`1`)},
		{ID: 2, Value: json.RawMessage(`0`)},
		{ID: 3, Value: json.RawMessage(`42`)},
		{ID: 4, Value: json.RawMessage(`-1`)},
	}

	expectedRecords := []ReadRecord{
		{ID: 1, Value: true},
		{ID: 2, Value: false},
		{ID: 3, Value: true},
		{ID: 4, Value: true},
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

	readRecords := make([]ReadRecord, len(expectedRecords))
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	if !reflect.DeepEqual(expectedRecords, readRecords) {
		t.Errorf("records mismatch:\nexpected: %+v\ngot: %+v", expectedRecords, readRecords)
	}
}

func TestJSONRawMessageTimestampLogicalType(t *testing.T) {
	type ReadRecord struct {
		ID        int64     `parquet:"id"`
		Timestamp time.Time `parquet:"timestamp,timestamp(microsecond)"`
	}

	type WriteRecord struct {
		ID        int64           `parquet:"id"`
		Timestamp json.RawMessage `parquet:"timestamp"`
	}

	schema := SchemaOf(ReadRecord{})

	records := []WriteRecord{
		{ID: 1, Timestamp: json.RawMessage(`"2024-01-15T10:30:00Z"`)},
		{ID: 2, Timestamp: json.RawMessage(`"2024-12-31T23:59:59+00:00"`)},
	}

	expectedTime1, _ := time.Parse(time.RFC3339, "2024-01-15T10:30:00Z")
	expectedTime2, _ := time.Parse(time.RFC3339, "2024-12-31T23:59:59+00:00")

	expectedRecords := []ReadRecord{
		{ID: 1, Timestamp: expectedTime1},
		{ID: 2, Timestamp: expectedTime2},
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

	readRecords := make([]ReadRecord, len(expectedRecords))
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	for i := range readRecords {
		if !readRecords[i].Timestamp.Equal(expectedRecords[i].Timestamp) {
			t.Errorf("record %d timestamp mismatch:\nexpected: %v\ngot: %v",
				i, expectedRecords[i].Timestamp, readRecords[i].Timestamp)
		}
	}
}

func TestJSONRawMessageDateLogicalType(t *testing.T) {
	type ReadRecord struct {
		ID   int64     `parquet:"id"`
		Date time.Time `parquet:"date,date"`
	}

	type WriteRecord struct {
		ID   int64           `parquet:"id"`
		Date json.RawMessage `parquet:"date"`
	}

	schema := SchemaOf(ReadRecord{})

	records := []WriteRecord{
		{ID: 1, Date: json.RawMessage(`"2024-01-15"`)},
		{ID: 2, Date: json.RawMessage(`"2024-12-31"`)},
	}

	expectedDate1, _ := time.Parse("2006-01-02", "2024-01-15")
	expectedDate2, _ := time.Parse("2006-01-02", "2024-12-31")

	expectedRecords := []ReadRecord{
		{ID: 1, Date: expectedDate1},
		{ID: 2, Date: expectedDate2},
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

	readRecords := make([]ReadRecord, len(expectedRecords))
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	for i := range readRecords {
		if !readRecords[i].Date.Equal(expectedRecords[i].Date) {
			t.Errorf("record %d date mismatch:\nexpected: %v\ngot: %v",
				i, expectedRecords[i].Date, readRecords[i].Date)
		}
	}
}

func TestJSONRawMessageTimeLogicalType(t *testing.T) {
	type ReadRecord struct {
		ID   int64         `parquet:"id"`
		Time time.Duration `parquet:"time,time(microsecond)"`
	}

	type WriteRecord struct {
		ID   int64           `parquet:"id"`
		Time json.RawMessage `parquet:"time"`
	}

	schema := SchemaOf(ReadRecord{})

	records := []WriteRecord{
		{ID: 1, Time: json.RawMessage(`"10:30:45.000000000"`)},
		{ID: 2, Time: json.RawMessage(`"23:59:59.999999999"`)},
	}

	expectedRecords := []ReadRecord{
		{ID: 1, Time: 10*time.Hour + 30*time.Minute + 45*time.Second},
		{ID: 2, Time: 23*time.Hour + 59*time.Minute + 59*time.Second + 999999*time.Microsecond},
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

	readRecords := make([]ReadRecord, len(expectedRecords))
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	if !reflect.DeepEqual(expectedRecords, readRecords) {
		t.Errorf("records mismatch:\nexpected: %+v\ngot: %+v", expectedRecords, readRecords)
	}
}

// TestJSONRawMessageNumberToTimestamp tests number (Unix seconds) to timestamp conversion
func TestJSONRawMessageNumberToTimestamp(t *testing.T) {
	type ReadRecord struct {
		ID        int64     `parquet:"id"`
		Timestamp time.Time `parquet:"timestamp,timestamp(microsecond)"`
	}

	type WriteRecord struct {
		ID        int64           `parquet:"id"`
		Timestamp json.RawMessage `parquet:"timestamp"`
	}

	schema := SchemaOf(ReadRecord{})

	// Unix timestamps in seconds: 1705317000 = 2024-01-15T10:30:00Z
	//                             1735689599 = 2024-12-31T23:59:59Z
	records := []WriteRecord{
		{ID: 1, Timestamp: json.RawMessage(`1705317000`)},
		{ID: 2, Timestamp: json.RawMessage(`1735689599`)},
	}

	expectedTime1 := time.Unix(1705317000, 0).UTC()
	expectedTime2 := time.Unix(1735689599, 0).UTC()

	expectedRecords := []ReadRecord{
		{ID: 1, Timestamp: expectedTime1},
		{ID: 2, Timestamp: expectedTime2},
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

	readRecords := make([]ReadRecord, len(expectedRecords))
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	for i := range readRecords {
		if !readRecords[i].Timestamp.Equal(expectedRecords[i].Timestamp) {
			t.Errorf("record %d timestamp mismatch:\nexpected: %v\ngot: %v",
				i, expectedRecords[i].Timestamp, readRecords[i].Timestamp)
		}
	}
}

// TestJSONRawMessageNumberToDate tests number (seconds since epoch) to date conversion
func TestJSONRawMessageNumberToDate(t *testing.T) {
	type ReadRecord struct {
		ID   int64     `parquet:"id"`
		Date time.Time `parquet:"date,date"`
	}

	type WriteRecord struct {
		ID   int64           `parquet:"id"`
		Date json.RawMessage `parquet:"date"`
	}

	schema := SchemaOf(ReadRecord{})

	// Seconds since epoch: 1705276800 = 2024-01-15T00:00:00Z
	//                      1704067200 = 2024-01-01T00:00:00Z
	records := []WriteRecord{
		{ID: 1, Date: json.RawMessage(`1705276800`)},
		{ID: 2, Date: json.RawMessage(`1704067200`)},
	}

	expectedDate1 := time.Unix(1705276800, 0).UTC()
	expectedDate2 := time.Unix(1704067200, 0).UTC()

	expectedRecords := []ReadRecord{
		{ID: 1, Date: expectedDate1},
		{ID: 2, Date: expectedDate2},
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

	readRecords := make([]ReadRecord, len(expectedRecords))
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	for i := range readRecords {
		if !readRecords[i].Date.Equal(expectedRecords[i].Date) {
			t.Errorf("record %d date mismatch:\nexpected: %v\ngot: %v",
				i, expectedRecords[i].Date, readRecords[i].Date)
		}
	}
}

// TestJSONRawMessageNumberToTime tests number (seconds since midnight) to time conversion
func TestJSONRawMessageNumberToTime(t *testing.T) {
	type ReadRecord struct {
		ID   int64         `parquet:"id"`
		Time time.Duration `parquet:"time,time(microsecond)"`
	}

	type WriteRecord struct {
		ID   int64           `parquet:"id"`
		Time json.RawMessage `parquet:"time"`
	}

	schema := SchemaOf(ReadRecord{})

	// Seconds since midnight: 10:30:45 = 37845 seconds
	//                         23:59:59 = 86399 seconds
	records := []WriteRecord{
		{ID: 1, Time: json.RawMessage(`37845`)},
		{ID: 2, Time: json.RawMessage(`86399`)},
	}

	expectedRecords := []ReadRecord{
		{ID: 1, Time: 10*time.Hour + 30*time.Minute + 45*time.Second},
		{ID: 2, Time: 23*time.Hour + 59*time.Minute + 59*time.Second},
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

	readRecords := make([]ReadRecord, len(expectedRecords))
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	if !reflect.DeepEqual(expectedRecords, readRecords) {
		t.Errorf("records mismatch:\nexpected: %+v\ngot: %+v", expectedRecords, readRecords)
	}
}
