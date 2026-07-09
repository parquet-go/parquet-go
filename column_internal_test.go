package parquet

import (
	"testing"

	"github.com/parquet-go/parquet-go/deprecated"
	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"
)

// TestRootSchemaRepeatedType verifies that a Parquet file with the root schema
// element having RepetitionType set to REPEATED (as Apache Arrow does) does not
// incorrectly increment the maxRepetitionLevel for leaf columns.
//
// This is a regression test for issue #458.
// The Parquet spec states the root's repetition_type "should not be specified",
// but Apache Arrow writes files with the root set to REPEATED.
func TestRootSchemaRepeatedType(t *testing.T) {
	// Create metadata simulating what Apache Arrow writes:
	// Root schema element with RepetitionType = REPEATED
	metadata := &format.FileMetaData{
		Version: 1,
		Schema: []format.SchemaElement{
			{
				// Root element with REPEATED (like Apache Arrow writes)
				Name:           "root",
				NumChildren:    thrift.New[int32](1),
				RepetitionType: thrift.New(format.Repeated),
			},
			{
				// Leaf column - a simple required INT64
				Name: "value",
				Type: thrift.New(format.Int64),
				// Required field (no RepetitionType set means required)
			},
		},
		// No row groups needed for this test since we're just testing setLevels
		RowGroups: []format.RowGroup{},
	}

	root, err := openColumns(nil, metadata, nil, nil)
	if err != nil {
		t.Fatalf("openColumns failed: %v", err)
	}

	// The root column's maxRepetitionLevel should be 0 (not 1)
	// because the root's repetition type should be ignored
	if root.maxRepetitionLevel != 0 {
		t.Errorf("root maxRepetitionLevel = %d, want 0", root.maxRepetitionLevel)
	}
	if root.maxDefinitionLevel != 0 {
		t.Errorf("root maxDefinitionLevel = %d, want 0", root.maxDefinitionLevel)
	}

	// Get the leaf column
	if len(root.columns) != 1 {
		t.Fatalf("expected 1 child column, got %d", len(root.columns))
	}
	leaf := root.columns[0]

	// The leaf column's maxRepetitionLevel should also be 0
	// since it's a required field under the root
	if leaf.maxRepetitionLevel != 0 {
		t.Errorf("leaf maxRepetitionLevel = %d, want 0", leaf.maxRepetitionLevel)
	}
	if leaf.maxDefinitionLevel != 0 {
		t.Errorf("leaf maxDefinitionLevel = %d, want 0", leaf.maxDefinitionLevel)
	}
}

// TestRootSchemaRepeatedTypeWithOptionalChild verifies that when the root has
// RepetitionType = REPEATED (ignored) but a child is OPTIONAL, the levels are
// calculated correctly.
func TestRootSchemaRepeatedTypeWithOptionalChild(t *testing.T) {
	metadata := &format.FileMetaData{
		Version: 1,
		Schema: []format.SchemaElement{
			{
				Name:           "root",
				NumChildren:    thrift.New[int32](1),
				RepetitionType: thrift.New(format.Repeated), // Should be ignored
			},
			{
				Name:           "value",
				Type:           thrift.New(format.Int64),
				RepetitionType: thrift.New(format.Optional), // Optional field
			},
		},
		RowGroups: []format.RowGroup{},
	}

	root, err := openColumns(nil, metadata, nil, nil)
	if err != nil {
		t.Fatalf("openColumns failed: %v", err)
	}

	// Root should have 0 for both levels
	if root.maxRepetitionLevel != 0 {
		t.Errorf("root maxRepetitionLevel = %d, want 0", root.maxRepetitionLevel)
	}
	if root.maxDefinitionLevel != 0 {
		t.Errorf("root maxDefinitionLevel = %d, want 0", root.maxDefinitionLevel)
	}

	leaf := root.columns[0]

	// The leaf is OPTIONAL, so it should have:
	// - maxRepetitionLevel = 0 (optional doesn't add repetition)
	// - maxDefinitionLevel = 1 (optional adds 1 to definition level)
	if leaf.maxRepetitionLevel != 0 {
		t.Errorf("optional leaf maxRepetitionLevel = %d, want 0", leaf.maxRepetitionLevel)
	}
	if leaf.maxDefinitionLevel != 1 {
		t.Errorf("optional leaf maxDefinitionLevel = %d, want 1", leaf.maxDefinitionLevel)
	}
}

// TestRootSchemaRepeatedTypeWithRepeatedChild verifies that when the root has
// RepetitionType = REPEATED (ignored) but a child is REPEATED, the levels are
// calculated correctly.
func TestRootSchemaRepeatedTypeWithRepeatedChild(t *testing.T) {
	metadata := &format.FileMetaData{
		Version: 1,
		Schema: []format.SchemaElement{
			{
				Name:           "root",
				NumChildren:    thrift.New[int32](1),
				RepetitionType: thrift.New(format.Repeated), // Should be ignored
			},
			{
				Name:           "value",
				Type:           thrift.New(format.Int64),
				RepetitionType: thrift.New(format.Repeated), // Repeated field
			},
		},
		RowGroups: []format.RowGroup{},
	}

	root, err := openColumns(nil, metadata, nil, nil)
	if err != nil {
		t.Fatalf("openColumns failed: %v", err)
	}

	// Root should have 0 for both levels
	if root.maxRepetitionLevel != 0 {
		t.Errorf("root maxRepetitionLevel = %d, want 0", root.maxRepetitionLevel)
	}
	if root.maxDefinitionLevel != 0 {
		t.Errorf("root maxDefinitionLevel = %d, want 0", root.maxDefinitionLevel)
	}

	leaf := root.columns[0]

	// The leaf is REPEATED, so it should have:
	// - maxRepetitionLevel = 1 (repeated adds 1 to repetition level)
	// - maxDefinitionLevel = 1 (repeated adds 1 to definition level)
	if leaf.maxRepetitionLevel != 1 {
		t.Errorf("repeated leaf maxRepetitionLevel = %d, want 1", leaf.maxRepetitionLevel)
	}
	if leaf.maxDefinitionLevel != 1 {
		t.Errorf("repeated leaf maxDefinitionLevel = %d, want 1", leaf.maxDefinitionLevel)
	}
}

// TestGroupConvertedTypeFallback verifies that LIST and MAP types are correctly
// inferred from ConvertedType when LogicalType is not set, as is the case for
// files written by older parquet writers.
func TestGroupConvertedTypeFallback(t *testing.T) {
	tests := []struct {
		name          string
		convertedType deprecated.ConvertedType
		wantType      Type
	}{
		{
			name:          "list",
			convertedType: deprecated.List,
			wantType:      &listType{},
		},
		{
			name:          "map",
			convertedType: deprecated.Map,
			wantType:      &mapType{},
		},
		{
			name:          "map_key_value",
			convertedType: deprecated.MapKeyValue,
			wantType:      &groupType{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metadata := &format.FileMetaData{
				Version: 1,
				Schema: []format.SchemaElement{
					{
						Name:        "root",
						NumChildren: thrift.New[int32](1),
					},
					{
						Name:          tt.name,
						NumChildren:   thrift.New[int32](1),
						ConvertedType: thrift.New(tt.convertedType),
					},
					{
						Name: "element",
						Type: thrift.New(format.Int64),
					},
				},
				RowGroups: []format.RowGroup{},
			}

			root, err := openColumns(nil, metadata, nil, nil)
			if err != nil {
				t.Fatalf("openColumns failed: %v", err)
			}

			if len(root.columns) != 1 {
				t.Fatalf("expected 1 child column, got %d", len(root.columns))
			}

			group := root.columns[0]
			gotType := group.Type()
			wantLogical := tt.wantType.LogicalType()

			if gotType.LogicalType() != wantLogical {
				t.Errorf("group type LogicalType = %v, want %v", gotType.LogicalType(), wantLogical)
			}
		})
	}
}

func schemaElementOf(lt format.LogicalTypeValue) *format.SchemaElement {
	return &format.SchemaElement{Name: "x", LogicalType: format.LogicalType{Value: lt}}
}

// TestSchemaElementTypeIsCanonical asserts that a logical type recovered from a
// decoded schema element is the same instance this package hands out for the
// equivalent constructed node. Types are compared by pointer on purpose: the
// canonicalIntType, canonicalTimeType and canonicalTimestampType lookups exist
// so that the identity switches in LogicalType find them.
func TestSchemaElementTypeIsCanonical(t *testing.T) {
	tests := []struct {
		scenario    string
		decoded     format.LogicalTypeValue
		constructed Type
	}{
		{
			scenario:    "INT(64,true)",
			decoded:     &format.IntType{BitWidth: 64, IsSigned: true},
			constructed: Int(64).Type(),
		},
		{
			scenario:    "INT(8,false)",
			decoded:     &format.IntType{BitWidth: 8, IsSigned: false},
			constructed: Uint(8).Type(),
		},
		{
			scenario:    "TIME(micros,utc)",
			decoded:     &format.TimeType{IsAdjustedToUTC: true, Unit: format.TimeUnit{Value: &format.MicroSeconds{}}},
			constructed: Time(Microsecond).Type(),
		},
		{
			scenario:    "TIME(millis,local)",
			decoded:     &format.TimeType{IsAdjustedToUTC: false, Unit: format.TimeUnit{Value: &format.MilliSeconds{}}},
			constructed: TimeAdjusted(Millisecond, false).Type(),
		},
		{
			scenario:    "TIMESTAMP(nanos,utc)",
			decoded:     &format.TimestampType{IsAdjustedToUTC: true, Unit: format.TimeUnit{Value: &format.NanoSeconds{}}},
			constructed: Timestamp(Nanosecond).Type(),
		},
		{
			scenario:    "TIMESTAMP(millis,local)",
			decoded:     &format.TimestampType{IsAdjustedToUTC: false, Unit: format.TimeUnit{Value: &format.MilliSeconds{}}},
			constructed: TimestampAdjusted(Millisecond, false).Type(),
		},
	}

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			decoded := schemaElementTypeOf(schemaElementOf(test.decoded))

			if decoded != test.constructed {
				t.Fatalf("decoded type %p is not the canonical instance %p", decoded, test.constructed)
			}
			// Being canonical is what lets LogicalType return a shared value
			// instead of building one on every call.
			if n := testing.AllocsPerRun(100, func() { _ = decoded.LogicalType() }); n != 0 {
				t.Errorf("LogicalType allocates %v times per call on a decoded type", n)
			}
		})
	}
}

// TestSchemaElementTypePreservesUncanonicalValues checks that a value outside
// the set the spec allows is carried through unchanged rather than silently
// rewritten to the nearest canonical instance.
func TestSchemaElementTypePreservesUncanonicalValues(t *testing.T) {
	tests := []struct {
		scenario string
		decoded  format.LogicalTypeValue
		want     string
	}{
		{
			scenario: "bit width the spec does not allow",
			decoded:  &format.IntType{BitWidth: 7, IsSigned: true},
			want:     "INT(7,true)",
		},
		{
			scenario: "time with no unit",
			decoded:  &format.TimeType{IsAdjustedToUTC: true},
			want:     "TIME(isAdjustedToUTC=true,unit=)",
		},
		{
			scenario: "timestamp with no unit",
			decoded:  &format.TimestampType{IsAdjustedToUTC: false},
			want:     "TIMESTAMP(isAdjustedToUTC=false,unit=)",
		},
	}

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			decoded := schemaElementTypeOf(schemaElementOf(test.decoded))
			if got := decoded.LogicalType().String(); got != test.want {
				t.Errorf("logical type = %q, want %q", got, test.want)
			}
		})
	}
}
