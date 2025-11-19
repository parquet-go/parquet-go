package parquet

import (
	"bytes"
	"reflect"
	"testing"
	"time"
)

// TestWriteValueFuncOfPrimitives tests writing primitive types
func TestWriteValueFuncOfPrimitives(t *testing.T) {
	tests := []struct {
		name     string
		schema   Node
		value    any
		expected any
	}{
		{
			name:     "bool",
			schema:   Leaf(BooleanType),
			value:    true,
			expected: true,
		},
		{
			name:     "int32",
			schema:   Leaf(Int32Type),
			value:    int32(42),
			expected: int32(42),
		},
		{
			name:     "int64",
			schema:   Leaf(Int64Type),
			value:    int64(123456789),
			expected: int64(123456789),
		},
		{
			name:     "float32",
			schema:   Leaf(FloatType),
			value:    float32(3.14),
			expected: float32(3.14),
		},
		{
			name:     "float64",
			schema:   Leaf(DoubleType),
			value:    float64(2.718),
			expected: float64(2.718),
		},
		{
			name:     "string",
			schema:   Leaf(ByteArrayType),
			value:    "hello",
			expected: "hello",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			columns := []ColumnBuffer{makeColumnBuffer(tt.schema, 0, 10)}
			_, writeFunc := writeValueFuncOf(0, tt.schema)

			val := reflect.ValueOf(tt.value)
			writeFunc(columns, columnLevels{}, val)

			if columns[0].Len() != 1 {
				t.Fatalf("expected 1 value, got %d", columns[0].Len())
			}

			values := make([]Value, 1)
			n, err := columns[0].ReadValuesAt(values, 0)
			if err != nil || n != 1 {
				t.Fatalf("failed to read value: %v", err)
			}

			checkValue(t, values[0], tt.expected)
		})
	}
}

// TestWriteValueFuncOfOptional tests optional fields
func TestWriteValueFuncOfOptional(t *testing.T) {
	tests := []struct {
		name       string
		value      any
		expectNull bool
		expected   any
	}{
		{
			name:       "nil pointer",
			value:      (*int32)(nil),
			expectNull: true,
		},
		{
			name:       "valid pointer",
			value:      ptrTo(int32(42)),
			expectNull: false,
			expected:   int32(42),
		},
		{
			name:       "zero value",
			value:      int32(0),
			expectNull: true,
		},
		{
			name:       "non zero value",
			value:      int32(42),
			expectNull: false,
			expected:   int32(42),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema := Optional(Leaf(Int32Type))
			columns := []ColumnBuffer{makeColumnBuffer(schema, 0, 10)}
			_, writeFunc := writeValueFuncOf(0, schema)

			val := reflect.ValueOf(tt.value)
			writeFunc(columns, columnLevels{}, val)

			optCol := columns[0].(*optionalColumnBuffer)
			if optCol.Len() != 1 {
				t.Fatalf("expected 1 row, got %d", optCol.Len())
			}

			isNull := optCol.rows[0] == -1
			if isNull != tt.expectNull {
				t.Errorf("expected null=%v, got null=%v", tt.expectNull, isNull)
			}

			if !tt.expectNull {
				values := make([]Value, 1)
				n, err := optCol.base.ReadValuesAt(values, 0)
				if err != nil || n != 1 {
					t.Fatalf("failed to read value: %v", err)
				}
				checkValue(t, values[0], tt.expected)
			}
		})
	}
}

// TestWriteValueFuncOfOptionalGroup tests optional groups with missing fields
func TestWriteValueFuncOfOptionalGroup(t *testing.T) {
	type Point struct {
		X float64 `parquet:",optional"`
		Y float64 `parquet:",optional"`
	}
	type Record struct {
		Point *Point `parquet:",optional"`
		ID    string `parquet:",optional"`
	}

	schema := SchemaOf(Record{})

	tests := []struct {
		name        string
		value       any
		expectNulls [3]bool // [Point.X, Point.Y, ID]
		expectX     float64
		expectY     float64
		expectID    string
	}{
		{
			name:        "all present",
			value:       Record{Point: &Point{X: 1.5, Y: 2.5}, ID: "test"},
			expectNulls: [3]bool{false, false, false},
			expectX:     1.5,
			expectY:     2.5,
			expectID:    "test",
		},
		{
			name:        "nil point",
			value:       Record{Point: nil, ID: "test"},
			expectNulls: [3]bool{true, true, false},
			expectID:    "test",
		},
		{
			name:        "empty id",
			value:       Record{Point: &Point{X: 3.5, Y: 4.5}, ID: ""},
			expectNulls: [3]bool{false, false, true},
			expectX:     3.5,
			expectY:     4.5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Use proper schema walking to get columns with correct max levels
			columns := makeColumnBuffersForSchema(schema)
			if len(columns) != 3 {
				t.Fatalf("expected 3 columns, got %d", len(columns))
			}

			_, writeFunc := writeValueFuncOf(0, schema)

			val := reflect.ValueOf(tt.value)
			writeFunc(columns, columnLevels{}, val)

			// Verify all columns have one row
			for i := range 3 {
				if columns[i].Len() != 1 {
					t.Errorf("column %d expected 1 row, got %d", i, columns[i].Len())
				}
			}

			// Check values
			checkOptionalColumn(t, "X", columns[0].(*optionalColumnBuffer), tt.expectNulls[0], tt.expectX)
			checkOptionalColumn(t, "Y", columns[1].(*optionalColumnBuffer), tt.expectNulls[1], tt.expectY)
			checkOptionalColumn(t, "ID", columns[2].(*optionalColumnBuffer), tt.expectNulls[2], tt.expectID)
		})
	}
}

// TestWriteValueFuncOfMapToGroup tests writing maps to group schemas
func TestWriteValueFuncOfMapToGroup(t *testing.T) {
	type Point struct {
		X float64 `parquet:",optional"`
		Y float64 `parquet:",optional"`
	}
	schema := SchemaOf(Point{})

	tests := []struct {
		name        string
		value       any
		expectX     float64
		expectY     float64
		expectNulls [2]bool // [X is null, Y is null]
	}{
		{
			name:        "map with both keys",
			value:       map[string]float64{"X": 1.5, "Y": 2.5},
			expectX:     1.5,
			expectY:     2.5,
			expectNulls: [2]bool{false, false},
		},
		{
			name:        "map with missing Y",
			value:       map[string]float64{"X": 1.5},
			expectX:     1.5,
			expectNulls: [2]bool{false, true},
		},
		{
			name:        "map with missing X",
			value:       map[string]float64{"Y": 2.5},
			expectY:     2.5,
			expectNulls: [2]bool{true, false},
		},
		{
			name:        "empty map",
			value:       map[string]float64{},
			expectNulls: [2]bool{true, true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Use proper schema walking to get columns with correct max levels
			columns := makeColumnBuffersForSchema(schema)
			if len(columns) != 2 {
				t.Fatalf("expected 2 columns, got %d", len(columns))
			}

			_, writeFunc := writeValueFuncOf(0, schema)

			val := reflect.ValueOf(tt.value)
			writeFunc(columns, columnLevels{}, val)

			// Verify both columns have one row
			for i := range 2 {
				if columns[i].Len() != 1 {
					t.Errorf("column %d expected 1 row, got %d", i, columns[i].Len())
				}
			}

			// Check values
			checkOptionalColumn(t, "X", columns[0].(*optionalColumnBuffer), tt.expectNulls[0], tt.expectX)
			checkOptionalColumn(t, "Y", columns[1].(*optionalColumnBuffer), tt.expectNulls[1], tt.expectY)
		})
	}
}

// TestWriteValueFuncOfNestedMapToGroup tests nested maps to nested groups
func TestWriteValueFuncOfNestedMapToGroup(t *testing.T) {
	type Record struct {
		Nested struct {
			Coordinates struct {
				X float64 `parquet:",optional"`
				Y float64 `parquet:",optional"`
			} `parquet:",optional"`
			ID string `parquet:",optional"`
		}
	}
	schema := SchemaOf(Record{})

	tests := []struct {
		name        string
		value       any
		expectX     float64
		expectY     float64
		expectID    string
		expectNulls [3]bool // [X, Y, ID]
	}{
		{
			name: "all present",
			value: map[string]any{
				"Nested": map[string]any{
					"Coordinates": map[string]float64{"X": 1.5, "Y": 2.5},
					"ID":          "test123",
				},
			},
			expectX:     1.5,
			expectY:     2.5,
			expectID:    "test123",
			expectNulls: [3]bool{false, false, false},
		},
		{
			name: "missing coordinates",
			value: map[string]any{
				"Nested": map[string]any{
					"ID": "test456",
				},
			},
			expectID:    "test456",
			expectNulls: [3]bool{true, true, false},
		},
		{
			name: "missing id",
			value: map[string]any{
				"Nested": map[string]any{
					"Coordinates": map[string]float64{"X": 3.5, "Y": 4.5},
				},
			},
			expectX:     3.5,
			expectY:     4.5,
			expectNulls: [3]bool{false, false, true},
		},
		{
			name: "missing coord X",
			value: map[string]any{
				"Nested": map[string]any{
					"Coordinates": map[string]float64{"Y": 5.5},
					"ID":          "test789",
				},
			},
			expectY:     5.5,
			expectID:    "test789",
			expectNulls: [3]bool{true, false, false},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Use proper schema walking to get columns with correct max levels
			columns := makeColumnBuffersForSchema(schema)
			if len(columns) != 3 {
				t.Fatalf("expected 3 columns, got %d", len(columns))
			}

			_, writeFunc := writeValueFuncOf(0, schema)

			val := reflect.ValueOf(tt.value)
			t.Logf("Input value: %v (type: %T, kind: %v)", tt.value, tt.value, val.Kind())
			writeFunc(columns, columnLevels{}, val)

			// Verify all columns have one row
			for i := range 3 {
				if columns[i].Len() != 1 {
					t.Errorf("column %d expected 1 row, got %d", i, columns[i].Len())
				}
			}

			// Check values
			optCol0 := columns[0].(*optionalColumnBuffer)
			optCol1 := columns[1].(*optionalColumnBuffer)
			optCol2 := columns[2].(*optionalColumnBuffer)
			t.Logf("Column 0 (X): rows=%v, defLevels=%v, baseLen=%d", optCol0.rows, optCol0.definitionLevels, optCol0.base.Len())
			t.Logf("Column 1 (Y): rows=%v, defLevels=%v, baseLen=%d", optCol1.rows, optCol1.definitionLevels, optCol1.base.Len())
			t.Logf("Column 2 (ID): rows=%v, defLevels=%v, baseLen=%d", optCol2.rows, optCol2.definitionLevels, optCol2.base.Len())

			checkOptionalColumn(t, "X", optCol0, tt.expectNulls[0], tt.expectX)
			checkOptionalColumn(t, "Y", optCol1, tt.expectNulls[1], tt.expectY)
			checkOptionalColumn(t, "ID", optCol2, tt.expectNulls[2], tt.expectID)
		})
	}
}

// TestWriteValueFuncOfRepeated tests repeated/slice fields
func TestWriteValueFuncOfRepeated(t *testing.T) {
	schema := Repeated(Leaf(Int32Type))

	tests := []struct {
		name          string
		value         []int32
		expectedCount int
	}{
		{
			name:          "empty slice",
			value:         []int32{},
			expectedCount: 0,
		},
		{
			name:          "single element",
			value:         []int32{42},
			expectedCount: 1,
		},
		{
			name:          "multiple elements",
			value:         []int32{1, 2, 3, 4, 5},
			expectedCount: 5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			columns := []ColumnBuffer{makeColumnBuffer(schema, 0, 10)}
			_, writeFunc := writeValueFuncOf(0, schema)

			val := reflect.ValueOf(tt.value)
			writeFunc(columns, columnLevels{}, val)

			repCol := columns[0].(*repeatedColumnBuffer)
			if repCol.Len() != 1 {
				t.Fatalf("expected 1 row, got %d", repCol.Len())
			}

			if int(repCol.base.NumValues()) != tt.expectedCount {
				t.Errorf("expected %d values, got %d", tt.expectedCount, repCol.base.NumValues())
			}
		})
	}
}

func TestWriteValueFuncOfRepeatedStruct(t *testing.T) {
	// Test: repeated struct (slice of structs)
	// Schema: repeated group point { required int32 x; required int32 y; }
	schema := Repeated(Group{
		"X": Leaf(Int32Type),
		"Y": Leaf(Int32Type),
	})

	type Point struct {
		X int32
		Y int32
	}

	tests := []struct {
		name           string
		value          []Point
		expectedRows   int
		expectedXCount int
		expectedYCount int
	}{
		{
			name:           "empty slice",
			value:          []Point{},
			expectedRows:   1,
			expectedXCount: 0,
			expectedYCount: 0,
		},
		{
			name:           "single struct",
			value:          []Point{{X: 10, Y: 20}},
			expectedRows:   1,
			expectedXCount: 1,
			expectedYCount: 1,
		},
		{
			name:           "multiple structs",
			value:          []Point{{X: 1, Y: 2}, {X: 3, Y: 4}, {X: 5, Y: 6}},
			expectedRows:   1,
			expectedXCount: 3,
			expectedYCount: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			columns := makeColumnBuffersForSchema(schema)
			if len(columns) != 2 {
				t.Fatalf("expected 2 columns, got %d", len(columns))
			}

			_, writeFunc := writeValueFuncOf(0, schema)

			val := reflect.ValueOf(tt.value)
			writeFunc(columns, columnLevels{}, val)

			xCol := columns[0].(*repeatedColumnBuffer)
			yCol := columns[1].(*repeatedColumnBuffer)

			if xCol.Len() != tt.expectedRows {
				t.Errorf("X column: expected %d rows, got %d", tt.expectedRows, xCol.Len())
			}
			if yCol.Len() != tt.expectedRows {
				t.Errorf("Y column: expected %d rows, got %d", tt.expectedRows, yCol.Len())
			}

			if int(xCol.base.NumValues()) != tt.expectedXCount {
				t.Errorf("X column: expected %d values, got %d", tt.expectedXCount, xCol.base.NumValues())
			}
			if int(yCol.base.NumValues()) != tt.expectedYCount {
				t.Errorf("Y column: expected %d values, got %d", tt.expectedYCount, yCol.base.NumValues())
			}
		})
	}
}

func TestWriteValueFuncOfStructWithRepeated(t *testing.T) {
	// Test: struct containing a repeated field
	// Schema: group record { required string id; repeated int32 values; }
	schema := Group{
		"ID":     Leaf(ByteArrayType),
		"Values": Repeated(Leaf(Int32Type)),
	}

	type Record struct {
		ID     string
		Values []int32
	}

	tests := []struct {
		name               string
		value              Record
		expectedRows       int
		expectedIDCount    int
		expectedValueCount int
	}{
		{
			name:               "empty values",
			value:              Record{ID: "test1", Values: []int32{}},
			expectedRows:       1,
			expectedIDCount:    1,
			expectedValueCount: 0,
		},
		{
			name:               "single value",
			value:              Record{ID: "test2", Values: []int32{42}},
			expectedRows:       1,
			expectedIDCount:    1,
			expectedValueCount: 1,
		},
		{
			name:               "multiple values",
			value:              Record{ID: "test3", Values: []int32{10, 20, 30, 40}},
			expectedRows:       1,
			expectedIDCount:    1,
			expectedValueCount: 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			columns := makeColumnBuffersForSchema(schema)
			if len(columns) != 2 {
				t.Fatalf("expected 2 columns, got %d", len(columns))
			}

			_, writeFunc := writeValueFuncOf(0, schema)

			val := reflect.ValueOf(tt.value)
			writeFunc(columns, columnLevels{}, val)

			idCol := columns[0]
			valCol := columns[1].(*repeatedColumnBuffer)

			if idCol.Len() != tt.expectedRows {
				t.Errorf("ID column: expected %d rows, got %d", tt.expectedRows, idCol.Len())
			}
			if valCol.Len() != tt.expectedRows {
				t.Errorf("Values column: expected %d rows, got %d", tt.expectedRows, valCol.Len())
			}

			if int(idCol.NumValues()) != tt.expectedIDCount {
				t.Errorf("ID column: expected %d values, got %d", tt.expectedIDCount, idCol.NumValues())
			}
			if int(valCol.base.NumValues()) != tt.expectedValueCount {
				t.Errorf("Values column: expected %d values, got %d", tt.expectedValueCount, valCol.base.NumValues())
			}
		})
	}
}

// TestWriteValueFuncOfMapWithSliceValues tests map[string]any where values include slices
// This reproduces the production panic where []any values were not handled correctly
func TestWriteValueFuncOfMapWithSliceValues(t *testing.T) {
	type Record struct {
		Name   string   `parquet:",optional"`
		Values []string `parquet:",optional"`
	}
	schema := SchemaOf(Record{})

	tests := []struct {
		name              string
		value             any
		expectName        string
		expectValues      []string
		expectNameNull    bool
		expectValuesCount int
	}{
		{
			name: "map with slice value",
			value: map[string]any{
				"Name":   "test",
				"Values": []any{"a", "b", "c"},
			},
			expectName:        "test",
			expectValues:      []string{"a", "b", "c"},
			expectNameNull:    false,
			expectValuesCount: 3,
		},
		{
			name: "map with empty slice",
			value: map[string]any{
				"Name":   "test",
				"Values": []any{},
			},
			expectName:        "test",
			expectNameNull:    false,
			expectValuesCount: 0,
		},
		{
			name: "map with nil slice",
			value: map[string]any{
				"Name":   "test",
				"Values": nil,
			},
			expectName:        "test",
			expectNameNull:    false,
			expectValuesCount: 0,
		},
		{
			name: "map missing slice field",
			value: map[string]any{
				"Name": "test",
			},
			expectName:        "test",
			expectNameNull:    false,
			expectValuesCount: 0,
		},
		{
			name: "map with interface slice of any",
			value: map[string]any{
				"Name":   "test",
				"Values": []any{"x", "y"},
			},
			expectName:        "test",
			expectValues:      []string{"x", "y"},
			expectNameNull:    false,
			expectValuesCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			columns := makeColumnBuffersForSchema(schema)
			if len(columns) != 2 {
				t.Fatalf("expected 2 columns, got %d", len(columns))
			}

			_, writeFunc := writeValueFuncOf(0, schema)
			val := reflect.ValueOf(tt.value)
			writeFunc(columns, columnLevels{}, val)

			// Check Name column
			nameCol := columns[0].(*optionalColumnBuffer)
			if nameCol.Len() != 1 {
				t.Fatalf("Name column: expected 1 row, got %d", nameCol.Len())
			}
			checkOptionalColumn(t, "Name", nameCol, tt.expectNameNull, tt.expectName)

			// Check Values column (repeated)
			valuesCol := columns[1].(*repeatedColumnBuffer)
			if valuesCol.Len() != 1 {
				t.Fatalf("Values column: expected 1 row, got %d", valuesCol.Len())
			}
			if int(valuesCol.base.NumValues()) != tt.expectValuesCount {
				t.Errorf("Values column: expected %d values, got %d", tt.expectValuesCount, valuesCol.base.NumValues())
			}

			// If we have values, verify them
			if tt.expectValuesCount > 0 && len(tt.expectValues) > 0 {
				baseCol := valuesCol.base.(*byteArrayColumnBuffer)
				for i, expectedVal := range tt.expectValues {
					actualVal := string(baseCol.index(i))
					if actualVal != expectedVal {
						t.Errorf("Values[%d]: expected %q, got %q", i, expectedVal, actualVal)
					}
				}
			}
		})
	}
}

// Helper functions

func intPtr(v int32) *int32 {
	return &v
}

// makeColumnBuffersForSchema creates column buffers for all leaf columns in a schema
// with properly calculated maxRepetitionLevel and maxDefinitionLevel
func makeColumnBuffersForSchema(schema Node) []ColumnBuffer {
	var columns []ColumnBuffer
	forEachLeafColumnOf(schema, func(leaf leafColumn) {
		typ := leaf.node.Type()
		column := typ.NewColumnBuffer(int(leaf.columnIndex), 10)

		switch {
		case leaf.maxRepetitionLevel > 0:
			column = newRepeatedColumnBuffer(column, nil, nil, leaf.maxRepetitionLevel, leaf.maxDefinitionLevel, nullsGoFirst)
		case leaf.maxDefinitionLevel > 0:
			column = newOptionalColumnBuffer(column, nil, nil, leaf.maxDefinitionLevel, nullsGoFirst)
		}
		columns = append(columns, column)
	})
	return columns
}

// Legacy makeColumnBuffer for simple tests where levels are known
func makeColumnBuffer(node Node, columnIndex int16, bufferCap int) ColumnBuffer {
	typ := node.Type()
	column := typ.NewColumnBuffer(int(columnIndex), bufferCap)

	maxRepLevel := byte(0)
	maxDefLevel := byte(0)

	if node.Repeated() {
		maxRepLevel = 1
		maxDefLevel = 1
	}
	if node.Optional() {
		maxDefLevel++
	}

	switch {
	case maxRepLevel > 0:
		return newRepeatedColumnBuffer(column, nil, nil, maxRepLevel, maxDefLevel, nullsGoFirst)
	case maxDefLevel > 0:
		return newOptionalColumnBuffer(column, nil, nil, maxDefLevel, nullsGoFirst)
	default:
		return column
	}
}

func ptrTo[T any](v T) *T {
	return &v
}

func checkValue(t *testing.T, got Value, expected any) {
	t.Helper()
	switch v := expected.(type) {
	case bool:
		if got.Boolean() != v {
			t.Errorf("expected %v, got %v", v, got.Boolean())
		}
	case int32:
		if got.Int32() != v {
			t.Errorf("expected %v, got %v", v, got.Int32())
		}
	case int64:
		if got.Int64() != v {
			t.Errorf("expected %v, got %v", v, got.Int64())
		}
	case float32:
		if got.Float() != v {
			t.Errorf("expected %v, got %v", v, got.Float())
		}
	case float64:
		if got.Double() != v {
			t.Errorf("expected %v, got %v", v, got.Double())
		}
	case string:
		if string(got.ByteArray()) != v {
			t.Errorf("expected %q, got %q", v, string(got.ByteArray()))
		}
	}
}

func checkOptionalColumn(t *testing.T, name string, col *optionalColumnBuffer, expectNull bool, expectedValue any) {
	t.Helper()
	isNull := col.rows[0] == -1
	if isNull != expectNull {
		t.Errorf("%s: expected null=%v, got null=%v", name, expectNull, isNull)
		return
	}

	if !expectNull {
		values := make([]Value, 1)
		n, err := col.base.ReadValuesAt(values, 0)
		if err != nil || n != 1 {
			t.Errorf("%s: failed to read value from base column: %v", name, err)
			return
		}
		checkValue(t, values[0], expectedValue)
	}
}

// TestWriteValueFuncOfSingleValueRepeatedGroup tests writing single scalar values
// to repeated groups with a single field. This allows writing individual values
// that get wrapped into single-element repeated columns.
func TestWriteValueFuncOfSingleValueRepeatedGroup(t *testing.T) {
	// Schema: repeated group value { required string Element; }
	schema := Repeated(Group{
		"Element": Leaf(ByteArrayType),
	})

	tests := []struct {
		name          string
		value         any
		expectedCount int
		expectedValue string
	}{
		{
			name:          "single string value",
			value:         "hello",
			expectedCount: 1,
			expectedValue: "hello",
		},
		{
			name:          "empty string",
			value:         "",
			expectedCount: 1,
			expectedValue: "",
		},
		{
			name:          "struct with element field",
			value:         struct{ Element string }{Element: "world"},
			expectedCount: 1,
			expectedValue: "world",
		},
		{
			name: "slice of structs",
			value: []struct{ Element string }{
				{Element: "one"},
				{Element: "two"},
			},
			expectedCount: 2,
			expectedValue: "one", // We'll check the first value
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			columns := makeColumnBuffersForSchema(schema)
			if len(columns) != 1 {
				t.Fatalf("expected 1 column, got %d", len(columns))
			}

			_, writeFunc := writeValueFuncOf(0, schema)
			val := reflect.ValueOf(tt.value)
			writeFunc(columns, columnLevels{}, val)

			col := columns[0].(*repeatedColumnBuffer)
			if col.Len() != 1 {
				t.Errorf("expected 1 row, got %d", col.Len())
			}

			if int(col.base.NumValues()) != tt.expectedCount {
				t.Errorf("expected %d values, got %d", tt.expectedCount, col.base.NumValues())
			}

			// Check the first value
			if col.base.NumValues() > 0 {
				values := make([]Value, 1)
				n, err := col.base.ReadValuesAt(values, 0)
				if err != nil || n != 1 {
					t.Fatalf("failed to read value: %v", err)
				}
				if string(values[0].ByteArray()) != tt.expectedValue {
					t.Errorf("expected value %q, got %q", tt.expectedValue, string(values[0].ByteArray()))
				}
			}
		})
	}
}

// TestWriteValueFuncOfSingleValueRepeatedGroupMultipleTypes tests writing
// single scalar values to repeated groups with different field types.
func TestWriteValueFuncOfSingleValueRepeatedGroupMultipleTypes(t *testing.T) {
	tests := []struct {
		name          string
		schema        Node
		value         any
		expectedCount int
		checkValue    func(t *testing.T, val Value)
	}{
		{
			name:          "int32 value",
			schema:        Repeated(Group{"Num": Leaf(Int32Type)}),
			value:         int32(42),
			expectedCount: 1,
			checkValue: func(t *testing.T, val Value) {
				if val.Int32() != 42 {
					t.Errorf("expected 42, got %d", val.Int32())
				}
			},
		},
		{
			name:          "int64 value",
			schema:        Repeated(Group{"Num": Leaf(Int64Type)}),
			value:         int64(999),
			expectedCount: 1,
			checkValue: func(t *testing.T, val Value) {
				if val.Int64() != 999 {
					t.Errorf("expected 999, got %d", val.Int64())
				}
			},
		},
		{
			name:          "float64 value",
			schema:        Repeated(Group{"Val": Leaf(DoubleType)}),
			value:         float64(3.14),
			expectedCount: 1,
			checkValue: func(t *testing.T, val Value) {
				if val.Double() != 3.14 {
					t.Errorf("expected 3.14, got %f", val.Double())
				}
			},
		},
		{
			name:          "bool value",
			schema:        Repeated(Group{"Flag": Leaf(BooleanType)}),
			value:         true,
			expectedCount: 1,
			checkValue: func(t *testing.T, val Value) {
				if !val.Boolean() {
					t.Errorf("expected true, got false")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			columns := makeColumnBuffersForSchema(tt.schema)
			if len(columns) != 1 {
				t.Fatalf("expected 1 column, got %d", len(columns))
			}

			_, writeFunc := writeValueFuncOf(0, tt.schema)
			val := reflect.ValueOf(tt.value)
			writeFunc(columns, columnLevels{}, val)

			col := columns[0].(*repeatedColumnBuffer)
			if col.Len() != 1 {
				t.Errorf("expected 1 row, got %d", col.Len())
			}

			if int(col.base.NumValues()) != tt.expectedCount {
				t.Errorf("expected %d values, got %d", tt.expectedCount, col.base.NumValues())
			}

			if col.base.NumValues() > 0 {
				values := make([]Value, 1)
				n, err := col.base.ReadValuesAt(values, 0)
				if err != nil || n != 1 {
					t.Fatalf("failed to read value: %v", err)
				}
				tt.checkValue(t, values[0])
			}
		})
	}
}

// TestWriteValueFuncOfSingleValueInStruct tests the full use case:
// a struct field mapped to a repeated group, where a single value creates
// a single-element repeated column.
func TestWriteValueFuncOfSingleValueInStruct(t *testing.T) {
	// Schema matching: message { repeated group Value { required string Element; } }
	schema := Group{
		"Value": Repeated(Group{
			"Element": Leaf(ByteArrayType),
		}),
	}

	type Row struct {
		Value string
	}

	tests := []struct {
		name          string
		row           Row
		expectedCount int
		expectedValue string
	}{
		{
			name:          "single value",
			row:           Row{Value: "test"},
			expectedCount: 1,
			expectedValue: "test",
		},
		{
			name:          "empty string",
			row:           Row{Value: ""},
			expectedCount: 1,
			expectedValue: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			columns := makeColumnBuffersForSchema(schema)
			if len(columns) != 1 {
				t.Fatalf("expected 1 column, got %d", len(columns))
			}

			_, writeFunc := writeValueFuncOf(0, schema)
			val := reflect.ValueOf(tt.row)
			writeFunc(columns, columnLevels{}, val)

			col := columns[0].(*repeatedColumnBuffer)
			if col.Len() != 1 {
				t.Errorf("expected 1 row, got %d", col.Len())
			}

			if int(col.base.NumValues()) != tt.expectedCount {
				t.Errorf("expected %d values, got %d", tt.expectedCount, col.base.NumValues())
			}

			if col.base.NumValues() > 0 {
				values := make([]Value, 1)
				n, err := col.base.ReadValuesAt(values, 0)
				if err != nil || n != 1 {
					t.Fatalf("failed to read value: %v", err)
				}
				if string(values[0].ByteArray()) != tt.expectedValue {
					t.Errorf("expected value %q, got %q", tt.expectedValue, string(values[0].ByteArray()))
				}
			}
		})
	}
}

// TestMapStringAnyToRepeatedGroupDirect tests writeValueFuncOf directly
// to debug what's happening with map[string]any values.
func TestMapStringAnyToRepeatedGroupDirect(t *testing.T) {
	// Schema: repeated group value { required string element; }
	schema := Repeated(Group{
		"Element": Leaf(ByteArrayType),
	})

	columns := makeColumnBuffersForSchema(schema)
	_, writeFunc := writeValueFuncOf(0, schema)

	// Test 1: Direct string value
	t.Run("direct string", func(t *testing.T) {
		val := reflect.ValueOf("hello")
		writeFunc(columns, columnLevels{}, val)

		col := columns[0].(*repeatedColumnBuffer)
		if col.base.NumValues() != 1 {
			t.Errorf("expected 1 value, got %d", col.base.NumValues())
		}
	})

	// Test 2: String from map[string]any
	t.Run("string from map any", func(t *testing.T) {
		columns := makeColumnBuffersForSchema(schema)
		_, writeFunc := writeValueFuncOf(0, schema)

		m := map[string]any{"key": "world"}
		v := m["key"]
		val := reflect.ValueOf(v)

		writeFunc(columns, columnLevels{}, val)

		col := columns[0].(*repeatedColumnBuffer)
		if col.base.NumValues() != 1 {
			t.Errorf("expected 1 value, got %d", col.base.NumValues())
		}
	})
}

// TestMapStringAnyToRepeatedGroup tests the dynamic map[string]any code path
// where scalar values should be automatically wrapped into repeated lists.
func TestMapStringAnyToRepeatedGroup(t *testing.T) {
	// Simplified: no optional wrapper, just repeated group
	type SchemaStruct struct {
		Value []struct {
			Element string
		}
	}

	// Define the input type with map[string]any
	type Row struct {
		Value any // Should accept either string or []string
	}

	// Define the expected output type
	type ExpectedRow struct {
		Value []struct {
			Element string
		}
	}

	schema := SchemaOf(SchemaStruct{})

	tests := []struct {
		name     string
		input    Row
		expected ExpectedRow
	}{
		{
			name: "scalar string",
			input: Row{
				Value: "hello, world!",
			},
			expected: ExpectedRow{
				Value: []struct{ Element string }{
					{Element: "hello, world!"},
				},
			},
		},
		{
			name: "empty string",
			input: Row{
				Value: "",
			},
			expected: ExpectedRow{
				Value: []struct{ Element string }{
					{Element: ""},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := new(bytes.Buffer)
			writer := NewGenericWriter[Row](buf, schema)

			n, err := writer.Write([]Row{tt.input})
			if err != nil {
				t.Fatalf("failed to write: %v", err)
			}
			if n != 1 {
				t.Fatalf("expected to write 1 row, wrote %d", n)
			}

			if err := writer.Close(); err != nil {
				t.Fatalf("failed to close writer: %v", err)
			}

			if buf.Len() == 0 {
				t.Fatal("no data was written to buffer")
			}

			reader := NewReader(bytes.NewReader(buf.Bytes()))
			defer reader.Close()

			var got ExpectedRow
			err = reader.Read(&got)
			if err != nil {
				t.Fatalf("failed to read: %v", err)
			}

			if len(got.Value) != len(tt.expected.Value) {
				t.Fatalf("expected %d values, got %d", len(tt.expected.Value), len(got.Value))
			}

			for i, expectedVal := range tt.expected.Value {
				if got.Value[i].Element != expectedVal.Element {
					t.Errorf("value[%d]: expected %q, got %q", i, expectedVal.Element, got.Value[i].Element)
				}
			}
		})
	}
}

// TestTimeTimeRoundTrip tests end-to-end writing and reading of time.Time values
func TestTimeTimeRoundTrip(t *testing.T) {
	type Row struct {
		Time time.Time
	}

	testTime := time.Date(2024, 1, 15, 14, 30, 45, 123456789, time.UTC)

	buf := new(bytes.Buffer)
	writer := NewGenericWriter[Row](buf)

	rows := []Row{{Time: testTime}}
	n, err := writer.Write(rows)
	if err != nil {
		t.Fatalf("failed to write: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected to write 1 row, wrote %d", n)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	reader := NewReader(bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	var got Row
	if err := reader.Read(&got); err != nil {
		t.Fatalf("failed to read: %v", err)
	}

	if !got.Time.Equal(testTime) {
		t.Errorf("expected %v, got %v", testTime, got.Time)
	}
}

// TestTimeDurationRoundTrip tests end-to-end writing and reading of time.Duration values
func TestTimeDurationRoundTrip(t *testing.T) {
	type Row struct {
		Duration time.Duration
	}

	testDuration := 14*time.Hour + 30*time.Minute + 45*time.Second + 123456789*time.Nanosecond

	buf := new(bytes.Buffer)
	writer := NewGenericWriter[Row](buf)

	rows := []Row{{Duration: testDuration}}
	n, err := writer.Write(rows)
	if err != nil {
		t.Fatalf("failed to write: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected to write 1 row, wrote %d", n)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	reader := NewReader(bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	var got Row
	if err := reader.Read(&got); err != nil {
		t.Fatalf("failed to read: %v", err)
	}

	if got.Duration != testDuration {
		t.Errorf("expected %v, got %v", testDuration, got.Duration)
	}
}

// TestTimeTypesWithMultipleRows tests writing and reading multiple rows with time types
func TestTimeTypesWithMultipleRows(t *testing.T) {
	type Event struct {
		Timestamp time.Time
		Duration  time.Duration
		Name      string
	}

	events := []Event{
		{
			Timestamp: time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC),
			Duration:  5 * time.Minute,
			Name:      "event1",
		},
		{
			Timestamp: time.Date(2024, 1, 2, 15, 30, 0, 0, time.UTC),
			Duration:  10 * time.Hour,
			Name:      "event2",
		},
		{
			Timestamp: time.Date(2024, 1, 3, 9, 15, 30, 500000000, time.UTC),
			Duration:  2*time.Hour + 30*time.Minute,
			Name:      "event3",
		},
	}

	buf := new(bytes.Buffer)
	writer := NewGenericWriter[Event](buf)

	n, err := writer.Write(events)
	if err != nil {
		t.Fatalf("failed to write: %v", err)
	}
	if n != len(events) {
		t.Fatalf("expected to write %d rows, wrote %d", len(events), n)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	reader := NewReader(bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	for i, expected := range events {
		var got Event
		if err := reader.Read(&got); err != nil {
			t.Fatalf("failed to read row %d: %v", i, err)
		}

		if !got.Timestamp.Equal(expected.Timestamp) {
			t.Errorf("row %d: expected timestamp %v, got %v", i, expected.Timestamp, got.Timestamp)
		}
		if got.Duration != expected.Duration {
			t.Errorf("row %d: expected duration %v, got %v", i, expected.Duration, got.Duration)
		}
		if got.Name != expected.Name {
			t.Errorf("row %d: expected name %q, got %q", i, expected.Name, got.Name)
		}
	}
}

// TestTimeTypesWithMapIndirection tests the indirection case where we write
// map[string]any but the schema expects concrete time.Time and time.Duration types.
// This exercises the writeTime and writeDuration functions in writeValueFuncOfLeaf.
func TestTimeTypesWithMapIndirection(t *testing.T) {
	type Metadata struct {
		CreatedAt time.Time
		UpdatedAt time.Time
	}

	type Row struct {
		Metadata Metadata
	}

	type DynamicRow struct {
		Metadata map[string]any
	}

	schema := SchemaOf(new(Row))

	testTime1 := time.Date(2024, 1, 15, 10, 30, 45, 123456789, time.UTC)
	testTime2 := time.Date(2024, 2, 20, 15, 45, 30, 987654321, time.UTC)

	dynamicRows := []DynamicRow{
		{
			Metadata: map[string]any{
				"CreatedAt": testTime1,
				"UpdatedAt": testTime2,
			},
		},
	}

	buf := new(bytes.Buffer)
	writer := NewGenericWriter[DynamicRow](buf, schema)

	n, err := writer.Write(dynamicRows)
	if err != nil {
		t.Fatalf("failed to write: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected to write 1 row, wrote %d", n)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	reader := NewReader(bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	var got Row
	if err := reader.Read(&got); err != nil {
		t.Fatalf("failed to read: %v", err)
	}

	if !got.Metadata.CreatedAt.Equal(testTime1) {
		t.Errorf("expected CreatedAt %v, got %v", testTime1, got.Metadata.CreatedAt)
	}
	if !got.Metadata.UpdatedAt.Equal(testTime2) {
		t.Errorf("expected UpdatedAt %v, got %v", testTime2, got.Metadata.UpdatedAt)
	}
}

// TestDurationTypesWithMapIndirection tests time.Duration with map indirection
func TestDurationTypesWithMapIndirection(t *testing.T) {
	type Metrics struct {
		RequestDuration  time.Duration
		ProcessDuration  time.Duration
		ResponseDuration time.Duration
	}

	type Row struct {
		Metrics Metrics
	}

	type DynamicRow struct {
		Metrics map[string]any
	}

	schema := SchemaOf(new(Row))

	dynamicRows := []DynamicRow{
		{
			Metrics: map[string]any{
				"RequestDuration":  5 * time.Second,
				"ProcessDuration":  250 * time.Millisecond,
				"ResponseDuration": 10 * time.Millisecond,
			},
		},
	}

	buf := new(bytes.Buffer)
	writer := NewGenericWriter[DynamicRow](buf, schema)

	n, err := writer.Write(dynamicRows)
	if err != nil {
		t.Fatalf("failed to write: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected to write 1 row, wrote %d", n)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	reader := NewReader(bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	var got Row
	if err := reader.Read(&got); err != nil {
		t.Fatalf("failed to read: %v", err)
	}

	if got.Metrics.RequestDuration != 5*time.Second {
		t.Errorf("expected RequestDuration %v, got %v", 5*time.Second, got.Metrics.RequestDuration)
	}
	if got.Metrics.ProcessDuration != 250*time.Millisecond {
		t.Errorf("expected ProcessDuration %v, got %v", 250*time.Millisecond, got.Metrics.ProcessDuration)
	}
	if got.Metrics.ResponseDuration != 10*time.Millisecond {
		t.Errorf("expected ResponseDuration %v, got %v", 10*time.Millisecond, got.Metrics.ResponseDuration)
	}
}

// TestTimeAndDurationWithMapIndirection tests both time.Time and time.Duration together
func TestTimeAndDurationWithMapIndirection(t *testing.T) {
	type Event struct {
		Timestamp time.Time
		Duration  time.Duration
		Name      string
	}

	type Row struct {
		Event Event
	}

	type DynamicRow struct {
		Event map[string]any
	}

	schema := SchemaOf(new(Row))

	testTime := time.Date(2024, 3, 10, 14, 30, 0, 0, time.UTC)
	testDuration := 2*time.Hour + 15*time.Minute

	dynamicRows := []DynamicRow{
		{
			Event: map[string]any{
				"Timestamp": testTime,
				"Duration":  testDuration,
				"Name":      "test-event",
			},
		},
	}

	buf := new(bytes.Buffer)
	writer := NewGenericWriter[DynamicRow](buf, schema)

	n, err := writer.Write(dynamicRows)
	if err != nil {
		t.Fatalf("failed to write: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected to write 1 row, wrote %d", n)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	reader := NewReader(bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	var got Row
	if err := reader.Read(&got); err != nil {
		t.Fatalf("failed to read: %v", err)
	}

	if !got.Event.Timestamp.Equal(testTime) {
		t.Errorf("expected Timestamp %v, got %v", testTime, got.Event.Timestamp)
	}
	if got.Event.Duration != testDuration {
		t.Errorf("expected Duration %v, got %v", testDuration, got.Event.Duration)
	}
	if got.Event.Name != "test-event" {
		t.Errorf("expected Name %q, got %q", "test-event", got.Event.Name)
	}
}

// TestTimeWithDifferentTimestampUnits tests time.Time with different timestamp units (millis, micros, nanos)
func TestTimeWithDifferentTimestampUnits(t *testing.T) {
	testTime := time.Date(2024, 1, 15, 10, 30, 45, 123456789, time.UTC)

	tests := []struct {
		name    string
		schema  *Schema
		checkFn func(t *testing.T, got time.Time)
	}{
		{
			name:   "timestamp millis",
			schema: NewSchema("Row", Group{"Time": Timestamp(Millisecond)}),
			checkFn: func(t *testing.T, got time.Time) {
				// Millis precision loses sub-millisecond precision
				expected := testTime.Truncate(time.Millisecond)
				if !got.Equal(expected) {
					t.Errorf("expected %v, got %v", expected, got)
				}
			},
		},
		{
			name:   "timestamp micros",
			schema: NewSchema("Row", Group{"Time": Timestamp(Microsecond)}),
			checkFn: func(t *testing.T, got time.Time) {
				// Micros precision loses sub-microsecond precision
				expected := testTime.Truncate(time.Microsecond)
				if !got.Equal(expected) {
					t.Errorf("expected %v, got %v", expected, got)
				}
			},
		},
		{
			name:   "timestamp nanos",
			schema: NewSchema("Row", Group{"Time": Timestamp(Nanosecond)}),
			checkFn: func(t *testing.T, got time.Time) {
				if !got.Equal(testTime) {
					t.Errorf("expected %v, got %v", testTime, got)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			type Row struct {
				Time time.Time
			}

			type DynamicRow struct {
				Time any
			}

			buf := new(bytes.Buffer)
			writer := NewGenericWriter[DynamicRow](buf, tt.schema)

			n, err := writer.Write([]DynamicRow{{Time: testTime}})
			if err != nil {
				t.Fatalf("failed to write: %v", err)
			}
			if n != 1 {
				t.Fatalf("expected to write 1 row, wrote %d", n)
			}

			if err := writer.Close(); err != nil {
				t.Fatalf("failed to close writer: %v", err)
			}

			reader := NewReader(bytes.NewReader(buf.Bytes()))
			defer reader.Close()

			var got Row
			if err := reader.Read(&got); err != nil {
				t.Fatalf("failed to read: %v", err)
			}

			tt.checkFn(t, got.Time)
		})
	}
}

// TestTimeWithDateLogicalType tests time.Time with DATE logical type
func TestTimeWithDateLogicalType(t *testing.T) {
	testTime := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)

	type Row struct {
		Date time.Time
	}

	type DynamicRow struct {
		Date any
	}

	schema := NewSchema("Row", Group{"Date": Date()})

	buf := new(bytes.Buffer)
	writer := NewGenericWriter[DynamicRow](buf, schema)

	n, err := writer.Write([]DynamicRow{{Date: testTime}})
	if err != nil {
		t.Fatalf("failed to write: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected to write 1 row, wrote %d", n)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	reader := NewReader(bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	var got Row
	if err := reader.Read(&got); err != nil {
		t.Fatalf("failed to read: %v", err)
	}

	// DATE type only stores the date part, not time
	expected := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
	if !got.Date.Equal(expected) {
		t.Errorf("expected %v, got %v", expected, got.Date)
	}
}

// TestTimeWithTimeLogicalType tests time.Time with TIME logical type
// Note: TIME type stores time-of-day only and doesn't round-trip well with time.Time.
// This test verifies that writing works without error. Reading TIME as time.Time
// is not a primary use case (use time.Duration or int64 instead).
func TestTimeWithTimeLogicalType(t *testing.T) {
	testTime := time.Date(2024, 1, 15, 14, 30, 45, 123456789, time.UTC)

	tests := []struct {
		name   string
		schema *Schema
	}{
		{
			name:   "time millis",
			schema: NewSchema("Row", Group{"Time": Time(Millisecond)}),
		},
		{
			name:   "time micros",
			schema: NewSchema("Row", Group{"Time": Time(Microsecond)}),
		},
		{
			name:   "time nanos",
			schema: NewSchema("Row", Group{"Time": Time(Nanosecond)}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			type DynamicRow struct {
				Time any
			}

			buf := new(bytes.Buffer)
			writer := NewGenericWriter[DynamicRow](buf, tt.schema)

			n, err := writer.Write([]DynamicRow{{Time: testTime}})
			if err != nil {
				t.Fatalf("failed to write: %v", err)
			}
			if n != 1 {
				t.Fatalf("expected to write 1 row, wrote %d", n)
			}

			if err := writer.Close(); err != nil {
				t.Fatalf("failed to close writer: %v", err)
			}

			// Verify the file was written successfully (has non-zero size)
			if buf.Len() == 0 {
				t.Fatal("no data was written")
			}
		})
	}
}

// TestDurationWithTimeLogicalType tests time.Duration with TIME logical type
// Note: Only TIME(NANOS) round-trips correctly with time.Duration. TIME(MILLIS) and TIME(MICROS)
// use INT32/INT64 which don't automatically convert units when reading back.
func TestDurationWithTimeLogicalType(t *testing.T) {
	testDuration := 14*time.Hour + 30*time.Minute + 45*time.Second + 123456789*time.Nanosecond

	tests := []struct {
		name          string
		schema        *Schema
		expected      time.Duration
		testRoundtrip bool
	}{
		{
			name:          "time millis",
			schema:        NewSchema("Row", Group{"Duration": Time(Millisecond)}),
			testRoundtrip: false, // INT32 doesn't round-trip with time.Duration
		},
		{
			name:          "time micros",
			schema:        NewSchema("Row", Group{"Duration": Time(Microsecond)}),
			testRoundtrip: false, // INT64 microseconds don't round-trip with time.Duration
		},
		{
			name:          "time nanos",
			schema:        NewSchema("Row", Group{"Duration": Time(Nanosecond)}),
			expected:      testDuration,
			testRoundtrip: true, // INT64 nanoseconds == time.Duration
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			type Row struct {
				Duration time.Duration
			}

			type DynamicRow struct {
				Duration any
			}

			buf := new(bytes.Buffer)
			writer := NewGenericWriter[DynamicRow](buf, tt.schema)

			n, err := writer.Write([]DynamicRow{{Duration: testDuration}})
			if err != nil {
				t.Fatalf("failed to write: %v", err)
			}
			if n != 1 {
				t.Fatalf("expected to write 1 row, wrote %d", n)
			}

			if err := writer.Close(); err != nil {
				t.Fatalf("failed to close writer: %v", err)
			}

			if !tt.testRoundtrip {
				// Just verify writing succeeded
				if buf.Len() == 0 {
					t.Fatal("no data was written")
				}
				return
			}

			reader := NewReader(bytes.NewReader(buf.Bytes()))
			defer reader.Close()

			var got Row
			if err := reader.Read(&got); err != nil {
				t.Fatalf("failed to read: %v", err)
			}

			if got.Duration != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, got.Duration)
			}
		})
	}
}
