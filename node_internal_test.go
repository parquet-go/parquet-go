package parquet

import (
	"testing"

	"github.com/parquet-go/parquet-go/deprecated"
	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"
)

func TestListElementOf(t *testing.T) {
	tests := []struct {
		name        string
		node        Node
		wantLeaf    bool
		wantPanic   bool
		description string
	}{
		{
			name:        "standard naming (list/element)",
			node:        listNode{Group{"list": Repeated(Group{"element": Leaf(Int32Type)})}},
			wantLeaf:    true,
			description: "standard parquet spec naming",
		},
		{
			name:        "athena naming (bag/array_element)",
			node:        listNode{Group{"bag": Repeated(Group{"array_element": Leaf(Int32Type)})}},
			wantLeaf:    true,
			description: "Athena/parquet-mr non-standard naming",
		},
		{
			name:        "pyarrow/polars naming (list/item)",
			node:        listNode{Group{"list": Repeated(Group{"item": Leaf(Int32Type)})}},
			wantLeaf:    true,
			description: "old PyArrow/Polars naming",
		},
		{
			name:        "leaf node",
			node:        Leaf(Int32Type),
			wantPanic:   true,
			description: "leaf nodes should panic",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.wantPanic {
				defer func() {
					if r := recover(); r == nil {
						t.Error("expected panic but did not get one")
					}
				}()
			}

			elem := listElementOf(tt.node)

			if tt.wantPanic {
				t.Error("expected panic but did not get one")
				return
			}

			if got := elem.Leaf(); got != tt.wantLeaf {
				t.Errorf("listElementOf().Leaf() = %v, want %v", got, tt.wantLeaf)
			}
		})
	}
}

func TestListElementOfWithOpenColumns(t *testing.T) {
	// Simulate an Athena-style schema:
	//   root (NumChildren=2)
	//     name (BYTE_ARRAY, STRING)
	//     favorite_numbers (LIST, NumChildren=1)
	//       bag (REPEATED, NumChildren=1)
	//         array_element (INT32, OPTIONAL)
	metadata := &format.FileMetaData{
		Version: 1,
		Schema: []format.SchemaElement{
			{
				Name:        "root",
				NumChildren: thrift.New[int32](2),
			},
			{
				Name: "name",
				Type: thrift.New(format.ByteArray),
				LogicalType: thrift.New(format.LogicalType{
					UTF8: new(format.StringType),
				}),
			},
			{
				Name:        "favorite_numbers",
				NumChildren: thrift.New[int32](1),
				LogicalType: thrift.New(format.LogicalType{
					List: new(format.ListType),
				}),
				ConvertedType: thrift.New(deprecated.List),
			},
			{
				Name:           "bag",
				NumChildren:    thrift.New[int32](1),
				RepetitionType: thrift.New(format.Repeated),
			},
			{
				Name:           "array_element",
				Type:           thrift.New(format.Int32),
				RepetitionType: thrift.New(format.Optional),
			},
		},
		RowGroups: []format.RowGroup{},
	}

	root, err := openColumns(nil, metadata, nil, nil)
	if err != nil {
		t.Fatalf("openColumns failed: %v", err)
	}

	// Find the favorite_numbers column.
	var favNumbers *Column
	for _, col := range root.columns {
		if col.schema.Name == "favorite_numbers" {
			favNumbers = col
			break
		}
	}
	if favNumbers == nil {
		t.Fatal("favorite_numbers column not found")
	}

	// Verify it is recognized as a LIST type.
	if !isList(favNumbers) {
		t.Fatal("expected favorite_numbers to be a LIST type")
	}

	// Verify listElementOf returns the element without panicking.
	elem := listElementOf(favNumbers)
	if elem == nil {
		t.Fatal("listElementOf returned nil")
	}
	if !elem.Leaf() {
		t.Error("expected list element to be a leaf node")
	}
	if name := elem.(Field).Name(); name != "array_element" {
		t.Errorf("expected element name %q, got %q", "array_element", name)
	}
}
