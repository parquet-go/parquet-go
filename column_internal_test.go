package parquet

import (
	"math"
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

// TestOpenColumnsMalformedNumChildren checks that a schema whose NumChildren do
// not add up produces an error rather than a panic.
//
// The column tree is cut out of slabs sized from the schema, so a group that
// claims more children than the schema holds would run the slab dry. That has
// to be reported, not indexed past.
func TestOpenColumnsMalformedNumChildren(t *testing.T) {
	tests := []struct {
		scenario string
		schema   []format.SchemaElement
	}{
		{
			scenario: "root claims more children than the schema has",
			schema: []format.SchemaElement{
				{Name: "root", NumChildren: thrift.New[int32](5)},
				{Name: "a", Type: thrift.New(format.Int64)},
			},
		},
		{
			scenario: "nested group claims more children than remain",
			schema: []format.SchemaElement{
				{Name: "root", NumChildren: thrift.New[int32](1)},
				{Name: "group", NumChildren: thrift.New[int32](4)},
				{Name: "a", Type: thrift.New(format.Int64)},
			},
		},
		{
			scenario: "children beyond the end of the schema",
			schema: []format.SchemaElement{
				{Name: "root", NumChildren: thrift.New[int32](2)},
				{Name: "a", Type: thrift.New(format.Int64)},
			},
		},
		{
			// A corrupt NumChildren must be rejected before it is used to size a
			// slice, or the fallback in allocate would try to make one of it.
			scenario: "absurd number of children",
			schema: []format.SchemaElement{
				{Name: "root", NumChildren: thrift.New[int32](math.MaxInt32)},
				{Name: "a", Type: thrift.New(format.Int64)},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			metadata := &format.FileMetaData{
				Version:   1,
				Schema:    test.schema,
				RowGroups: []format.RowGroup{},
			}

			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("openColumns panicked instead of returning an error: %v", r)
				}
			}()

			if _, err := openColumns(&File{}, metadata, nil, nil); err == nil {
				t.Error("expected an error for a malformed schema")
			}
		})
	}
}

// TestOpenColumnsSlabsDoNotOverlap checks that the slices handed to each column
// out of the shared slabs are disjoint: writing through one must not be visible
// through another.
func TestOpenColumnsSlabsDoNotOverlap(t *testing.T) {
	// root -> (group -> (a, b), c)
	metadata := &format.FileMetaData{
		Version: 1,
		Schema: []format.SchemaElement{
			{Name: "root", NumChildren: thrift.New[int32](2)},
			{Name: "group", NumChildren: thrift.New[int32](2)},
			{Name: "a", Type: thrift.New(format.Int64)},
			{Name: "b", Type: thrift.New(format.Int64)},
			{Name: "c", Type: thrift.New(format.Int64)},
		},
		RowGroups: []format.RowGroup{},
	}

	root, err := openColumns(&File{}, metadata, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	if len(root.columns) != 2 {
		t.Fatalf("root has %d children, want 2", len(root.columns))
	}
	group, c := root.columns[0], root.columns[1]
	if group.Name() != "group" || c.Name() != "c" {
		t.Fatalf("root children are %q, %q; want \"group\", \"c\"", group.Name(), c.Name())
	}
	if len(group.columns) != 2 {
		t.Fatalf("group has %d children, want 2", len(group.columns))
	}
	if group.columns[0].Name() != "a" || group.columns[1].Name() != "b" {
		t.Fatalf("group children are %q, %q; want \"a\", \"b\"", group.columns[0].Name(), group.columns[1].Name())
	}

	// Appending to a group's children must not reach into the sibling's slice:
	// allocate clamps the capacity of every slice it hands out.
	if cap(group.columns) != len(group.columns) {
		t.Errorf("group children have spare capacity %d; appending would overwrite the next column",
			cap(group.columns)-len(group.columns))
	}
	if cap(root.fields) != len(root.fields) {
		t.Errorf("root fields have spare capacity %d", cap(root.fields)-len(root.fields))
	}
}

// TestAllocate covers the slab cutter directly: exact reservations come out of
// the slab, and an exhausted slab falls back to a slice of its own.
func TestAllocate(t *testing.T) {
	slab := make([]int, 4)
	for i := range slab {
		slab[i] = i + 1
	}
	backing := slab

	first := allocate(&slab, 2)
	if len(first) != 2 || cap(first) != 2 {
		t.Fatalf("first = len %d cap %d, want 2/2", len(first), cap(first))
	}
	if &first[0] != &backing[0] {
		t.Error("first slice did not come out of the slab")
	}

	second := allocate(&slab, 2)
	if &second[0] != &backing[2] {
		t.Error("second slice overlaps the first")
	}
	if len(slab) != 0 {
		t.Errorf("slab has %d elements left, want 0", len(slab))
	}

	// Exhausted: allocate must still return a usable slice.
	third := allocate(&slab, 3)
	if len(third) != 3 || cap(third) != 3 {
		t.Fatalf("third = len %d cap %d, want 3/3", len(third), cap(third))
	}
	for i, v := range third {
		if v != 0 {
			t.Errorf("third[%d] = %d, want a zero value", i, v)
		}
	}

	// Zero elements never allocate.
	if got := allocate(&slab, 0); got != nil {
		t.Errorf("allocate(_, 0) = %v, want nil", got)
	}

	// Writing through the returned slices must not disturb their neighbours.
	first[0], first[1] = -1, -2
	second[0], second[1] = -3, -4
	if backing[0] != -1 || backing[1] != -2 || backing[2] != -3 || backing[3] != -4 {
		t.Errorf("slab = %v, want [-1 -2 -3 -4]", backing)
	}
}
