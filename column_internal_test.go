package parquet

import (
	"testing"

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
	// Helper to create a pointer to a value
	intPtr := func(v int32) *int32 { return &v }
	typePtr := func(v format.Type) *format.Type { return &v }
	repPtr := func(v format.FieldRepetitionType) *format.FieldRepetitionType { return &v }

	// Create metadata simulating what Apache Arrow writes:
	// Root schema element with RepetitionType = REPEATED
	metadata := &format.FileMetaData{
		Version: 1,
		Schema: []format.SchemaElement{
			{
				// Root element with REPEATED (like Apache Arrow writes)
				Name:           "root",
				NumChildren:    intPtr(1),
				RepetitionType: repPtr(format.Repeated),
			},
			{
				// Leaf column - a simple required INT64
				Name: "value",
				Type: typePtr(format.Int64),
				// Required field (no RepetitionType pointer means nil/required)
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
	intPtr := func(v int32) *int32 { return &v }
	typePtr := func(v format.Type) *format.Type { return &v }
	repPtr := func(v format.FieldRepetitionType) *format.FieldRepetitionType { return &v }

	metadata := &format.FileMetaData{
		Version: 1,
		Schema: []format.SchemaElement{
			{
				Name:           "root",
				NumChildren:    intPtr(1),
				RepetitionType: repPtr(format.Repeated), // Should be ignored
			},
			{
				Name:           "value",
				Type:           typePtr(format.Int64),
				RepetitionType: repPtr(format.Optional), // Optional field
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
	intPtr := func(v int32) *int32 { return &v }
	typePtr := func(v format.Type) *format.Type { return &v }
	repPtr := func(v format.FieldRepetitionType) *format.FieldRepetitionType { return &v }

	metadata := &format.FileMetaData{
		Version: 1,
		Schema: []format.SchemaElement{
			{
				Name:           "root",
				NumChildren:    intPtr(1),
				RepetitionType: repPtr(format.Repeated), // Should be ignored
			},
			{
				Name:           "value",
				Type:           typePtr(format.Int64),
				RepetitionType: repPtr(format.Repeated), // Repeated field
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
