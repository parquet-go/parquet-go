package parquet

import (
	"bytes"
	"reflect"
	"testing"
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
			name:       "nil_pointer",
			value:      (*int32)(nil),
			expectNull: true,
		},
		{
			name:       "valid_pointer",
			value:      ptrTo(int32(42)),
			expectNull: false,
			expected:   int32(42),
		},
		{
			name:       "zero_value",
			value:      int32(0),
			expectNull: true,
		},
		{
			name:       "non_zero_value",
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
			name:        "all_present",
			value:       Record{Point: &Point{X: 1.5, Y: 2.5}, ID: "test"},
			expectNulls: [3]bool{false, false, false},
			expectX:     1.5,
			expectY:     2.5,
			expectID:    "test",
		},
		{
			name:        "nil_point",
			value:       Record{Point: nil, ID: "test"},
			expectNulls: [3]bool{true, true, false},
			expectID:    "test",
		},
		{
			name:        "empty_id",
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
			name:        "map_with_both_keys",
			value:       map[string]float64{"X": 1.5, "Y": 2.5},
			expectX:     1.5,
			expectY:     2.5,
			expectNulls: [2]bool{false, false},
		},
		{
			name:        "map_with_missing_Y",
			value:       map[string]float64{"X": 1.5},
			expectX:     1.5,
			expectNulls: [2]bool{false, true},
		},
		{
			name:        "map_with_missing_X",
			value:       map[string]float64{"Y": 2.5},
			expectY:     2.5,
			expectNulls: [2]bool{true, false},
		},
		{
			name:        "empty_map",
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
			name: "all_present",
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
			name: "missing_coordinates",
			value: map[string]any{
				"Nested": map[string]any{
					"ID": "test456",
				},
			},
			expectID:    "test456",
			expectNulls: [3]bool{true, true, false},
		},
		{
			name: "missing_id",
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
			name: "missing_coord_X",
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
			name:          "empty_slice",
			value:         []int32{},
			expectedCount: 0,
		},
		{
			name:          "single_element",
			value:         []int32{42},
			expectedCount: 1,
		},
		{
			name:          "multiple_elements",
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
			name:           "empty_slice",
			value:          []Point{},
			expectedRows:   1,
			expectedXCount: 0,
			expectedYCount: 0,
		},
		{
			name:           "single_struct",
			value:          []Point{{X: 10, Y: 20}},
			expectedRows:   1,
			expectedXCount: 1,
			expectedYCount: 1,
		},
		{
			name:           "multiple_structs",
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
			name:               "empty_values",
			value:              Record{ID: "test1", Values: []int32{}},
			expectedRows:       1,
			expectedIDCount:    1,
			expectedValueCount: 0,
		},
		{
			name:               "single_value",
			value:              Record{ID: "test2", Values: []int32{42}},
			expectedRows:       1,
			expectedIDCount:    1,
			expectedValueCount: 1,
		},
		{
			name:               "multiple_values",
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
			name: "map_with_slice_value",
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
			name: "map_with_empty_slice",
			value: map[string]any{
				"Name":   "test",
				"Values": []any{},
			},
			expectName:        "test",
			expectNameNull:    false,
			expectValuesCount: 0,
		},
		{
			name: "map_with_nil_slice",
			value: map[string]any{
				"Name":   "test",
				"Values": nil,
			},
			expectName:        "test",
			expectNameNull:    false,
			expectValuesCount: 0,
		},
		{
			name: "map_missing_slice_field",
			value: map[string]any{
				"Name": "test",
			},
			expectName:        "test",
			expectNameNull:    false,
			expectValuesCount: 0,
		},
		{
			name: "map_with_interface_slice_of_any",
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
			name:          "single_string_value",
			value:         "hello",
			expectedCount: 1,
			expectedValue: "hello",
		},
		{
			name:          "empty_string",
			value:         "",
			expectedCount: 1,
			expectedValue: "",
		},
		{
			name:          "struct_with_element_field",
			value:         struct{ Element string }{Element: "world"},
			expectedCount: 1,
			expectedValue: "world",
		},
		{
			name: "slice_of_structs",
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
			name:          "int32_value",
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
			name:          "int64_value",
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
			name:          "float64_value",
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
			name:          "bool_value",
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
			name:          "single_value",
			row:           Row{Value: "test"},
			expectedCount: 1,
			expectedValue: "test",
		},
		{
			name:          "empty_string",
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
	t.Run("direct_string", func(t *testing.T) {
		val := reflect.ValueOf("hello")
		writeFunc(columns, columnLevels{}, val)

		col := columns[0].(*repeatedColumnBuffer)
		if col.base.NumValues() != 1 {
			t.Errorf("expected 1 value, got %d", col.base.NumValues())
		}
	})

	// Test 2: String from map[string]any
	t.Run("string_from_map_any", func(t *testing.T) {
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
			name: "scalar_string",
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
			name: "empty_string",
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
