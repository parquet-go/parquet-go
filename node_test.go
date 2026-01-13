package parquet

import (
	"reflect"
	"testing"

	"github.com/parquet-go/parquet-go/compress"
	"github.com/parquet-go/parquet-go/compress/gzip"
	"github.com/parquet-go/parquet-go/compress/snappy"
	"github.com/parquet-go/parquet-go/deprecated"
	"github.com/parquet-go/parquet-go/encoding"
)

type mockNode struct {
	id          int
	str         string
	typ         func() Type
	optional    bool
	repeated    bool
	required    bool
	leaf        bool
	fields      func() []Field
	enc         func() encoding.Encoding
	compression func() compress.Codec
	goType      func() reflect.Type
}

func (m *mockNode) ID() int                     { return m.id }
func (m *mockNode) String() string              { return m.str }
func (m *mockNode) Type() Type                  { return m.typ() }
func (m *mockNode) Optional() bool              { return m.optional }
func (m *mockNode) Repeated() bool              { return m.repeated }
func (m *mockNode) Required() bool              { return m.required }
func (m *mockNode) Leaf() bool                  { return m.leaf }
func (m *mockNode) Fields() []Field             { return m.fields() }
func (m *mockNode) Encoding() encoding.Encoding { return m.enc() }
func (m *mockNode) Compression() compress.Codec { return m.compression() }
func (m *mockNode) GoType() reflect.Type        { return m.goType() }

func TestEqualNodes(t *testing.T) {
	tests := []struct {
		name     string
		node1    Node
		node2    Node
		expected bool
	}{
		// Basic leaf node equality
		{
			name:     "same leaf nodes - boolean",
			node1:    Leaf(BooleanType),
			node2:    Leaf(BooleanType),
			expected: true,
		},
		{
			name:     "same leaf nodes - int32",
			node1:    Leaf(Int32Type),
			node2:    Leaf(Int32Type),
			expected: true,
		},
		{
			name:     "same leaf nodes - string",
			node1:    String(),
			node2:    String(),
			expected: true,
		},
		{
			name:     "different leaf types - int32 vs int64",
			node1:    Leaf(Int32Type),
			node2:    Leaf(Int64Type),
			expected: false,
		},
		{
			name:     "different leaf types - string vs json",
			node1:    String(),
			node2:    JSON(),
			expected: false,
		},

		// Repetition type differences
		{
			name:     "same types, different repetition - required vs optional",
			node1:    Required(String()),
			node2:    Optional(String()),
			expected: false,
		},
		{
			name:     "same types, different repetition - optional vs repeated",
			node1:    Optional(String()),
			node2:    Repeated(String()),
			expected: false,
		},
		{
			name:     "same types, same repetition - both optional",
			node1:    Optional(String()),
			node2:    Optional(String()),
			expected: true,
		},
		{
			name:     "same types, same repetition - both repeated",
			node1:    Repeated(Int(32)),
			node2:    Repeated(Int(32)),
			expected: true,
		},

		// Logical types
		{
			name:     "same logical types - int32",
			node1:    Int(32),
			node2:    Int(32),
			expected: true,
		},
		{
			name:     "same logical types - uint64",
			node1:    Uint(64),
			node2:    Uint(64),
			expected: true,
		},
		{
			name:     "different logical types - int32 vs uint32",
			node1:    Int(32),
			node2:    Uint(32),
			expected: false,
		},
		{
			name:     "same temporal types - date",
			node1:    Date(),
			node2:    Date(),
			expected: true,
		},
		{
			name:     "same temporal types - timestamp millis",
			node1:    Timestamp(Millisecond),
			node2:    Timestamp(Millisecond),
			expected: true,
		},
		{
			name:     "different temporal types - timestamp units",
			node1:    Timestamp(Millisecond),
			node2:    Timestamp(Microsecond),
			expected: false,
		},
		{
			name:     "same decimal types",
			node1:    Decimal(10, 2, Int32Type),
			node2:    Decimal(10, 2, Int32Type),
			expected: true,
		},
		{
			name:     "different decimal precision",
			node1:    Decimal(10, 2, Int32Type),
			node2:    Decimal(12, 2, Int32Type),
			expected: false,
		},

		// Encoding and compression should be ignored
		{
			name:     "same types, different encoding (should be equal)",
			node1:    Encoded(String(), &DeltaLengthByteArray),
			node2:    Encoded(String(), &Plain),
			expected: true,
		},
		{
			name:     "same types, different compression (should be equal)",
			node1:    Compressed(String(), &gzip.Codec{}),
			node2:    Compressed(String(), &snappy.Codec{}),
			expected: true,
		},

		// Field IDs should be ignored
		{
			name:     "same types, different field IDs (should be equal)",
			node1:    FieldID(String(), 1),
			node2:    FieldID(String(), 2),
			expected: true,
		},

		// Group nodes - empty groups
		{
			name:     "empty groups",
			node1:    Group{},
			node2:    Group{},
			expected: true,
		},

		// Group nodes - single field
		{
			name: "groups with same single field",
			node1: Group{
				"name": String(),
			},
			node2: Group{
				"name": String(),
			},
			expected: true,
		},
		{
			name: "groups with different field names",
			node1: Group{
				"name": String(),
			},
			node2: Group{
				"title": String(),
			},
			expected: false,
		},
		{
			name: "groups with same field name, different types",
			node1: Group{
				"name": String(),
			},
			node2: Group{
				"name": Int(32),
			},
			expected: false,
		},

		// Group nodes - multiple fields
		{
			name: "groups with same multiple fields",
			node1: Group{
				"id":   Int(64),
				"name": String(),
				"age":  Int(32),
			},
			node2: Group{
				"id":   Int(64),
				"name": String(),
				"age":  Int(32),
			},
			expected: true,
		},
		{
			name: "groups with different field count",
			node1: Group{
				"id":   Int(64),
				"name": String(),
			},
			node2: Group{
				"id":   Int(64),
				"name": String(),
				"age":  Int(32),
			},
			expected: false,
		},

		// Group nodes with repetition
		{
			name: "groups with same repetition",
			node1: Optional(Group{
				"name": String(),
			}),
			node2: Optional(Group{
				"name": String(),
			}),
			expected: true,
		},
		{
			name: "groups with different repetition",
			node1: Required(Group{
				"name": String(),
			}),
			node2: Optional(Group{
				"name": String(),
			}),
			expected: false,
		},

		// Nested groups
		{
			name: "nested groups - same structure",
			node1: Group{
				"person": Group{
					"name": String(),
					"age":  Int(32),
				},
			},
			node2: Group{
				"person": Group{
					"name": String(),
					"age":  Int(32),
				},
			},
			expected: true,
		},
		{
			name: "nested groups - different inner structure",
			node1: Group{
				"person": Group{
					"name": String(),
					"age":  Int(32),
				},
			},
			node2: Group{
				"person": Group{
					"name": String(),
					"age":  Int(64),
				},
			},
			expected: false,
		},

		// Mixed leaf and group comparisons
		{
			name:     "leaf vs group",
			node1:    String(),
			node2:    Group{"name": String()},
			expected: false,
		},
		{
			name:     "group vs leaf",
			node1:    Group{"name": String()},
			node2:    String(),
			expected: false,
		},

		// Complex nested structures
		{
			name: "complex nested structure - same",
			node1: Group{
				"user": Group{
					"id": Int(64),
					"profile": Group{
						"name":  String(),
						"email": String(),
						"settings": Group{
							"theme":         String(),
							"notifications": Repeated(String()),
						},
					},
				},
				"metadata": Optional(Group{
					"created_at": Timestamp(Millisecond),
					"tags":       Repeated(String()),
				}),
			},
			node2: Group{
				"user": Group{
					"id": Int(64),
					"profile": Group{
						"name":  String(),
						"email": String(),
						"settings": Group{
							"theme":         String(),
							"notifications": Repeated(String()),
						},
					},
				},
				"metadata": Optional(Group{
					"created_at": Timestamp(Millisecond),
					"tags":       Repeated(String()),
				}),
			},
			expected: true,
		},
		{
			name: "complex nested structure - different deep field",
			node1: Group{
				"user": Group{
					"id": Int(64),
					"profile": Group{
						"name":  String(),
						"email": String(),
						"settings": Group{
							"theme":         String(),
							"notifications": Repeated(String()),
						},
					},
				},
			},
			node2: Group{
				"user": Group{
					"id": Int(64),
					"profile": Group{
						"name":  String(),
						"email": String(),
						"settings": Group{
							"theme":         String(),
							"notifications": Repeated(Int(32)), // Different type here
						},
					},
				},
			},
			expected: false,
		},

		// Map types (group types with MAP logical type)
		{
			name:     "same map types",
			node1:    Map(String(), Int(32)),
			node2:    Map(String(), Int(32)),
			expected: true,
		},
		{
			name:     "different map key types",
			node1:    Map(String(), Int(32)),
			node2:    Map(Int(32), Int(32)),
			expected: false,
		},
		{
			name:     "different map value types",
			node1:    Map(String(), Int(32)),
			node2:    Map(String(), String()),
			expected: false,
		},

		// List types (group types with LIST logical type)
		{
			name:     "same list types",
			node1:    List(String()),
			node2:    List(String()),
			expected: true,
		},
		{
			name:     "different list element types",
			node1:    List(String()),
			node2:    List(Int(32)),
			expected: false,
		},

		// Logical types vs non-logical groups with same structure
		{
			name:  "logical map vs regular group with same structure",
			node1: Map(String(), Int(32)),
			node2: Group{
				"key_value": Repeated(Group{
					"key":   String(),
					"value": Int(32),
				}),
			},
			expected: false,
		},
		{
			name:  "logical list vs regular group with same structure",
			node1: List(String()),
			node2: Group{
				"list": Repeated(Group{
					"element": String(),
				}),
			},
			expected: false,
		},
		{
			name: "regular group vs logical map with same structure",
			node1: Group{
				"key_value": Repeated(Group{
					"key":   String(),
					"value": Int(32),
				}),
			},
			node2:    Map(String(), Int(32)),
			expected: false,
		},
		{
			name: "regular group vs logical list with same structure",
			node1: Group{
				"list": Repeated(Group{
					"element": String(),
				}),
			},
			node2:    List(String()),
			expected: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := EqualNodes(test.node1, test.node2)
			if result != test.expected {
				t.Errorf("EqualNodes(%v, %v) = %v, expected %v",
					test.node1, test.node2, result, test.expected)
			}
		})
	}
}

func TestSameNodes(t *testing.T) {
	tests := []struct {
		name     string
		node1    Node
		node2    Node
		expected bool
	}{
		// Same tests as EqualNodes for leaf nodes (should behave identically)
		{
			name:     "same leaf nodes - boolean",
			node1:    Leaf(BooleanType),
			node2:    Leaf(BooleanType),
			expected: true,
		},
		{
			name:     "different leaf types - int32 vs int64",
			node1:    Leaf(Int32Type),
			node2:    Leaf(Int64Type),
			expected: false,
		},

		// Group nodes - same fields in same order
		{
			name: "groups with same fields in same order",
			node1: Group{
				"id":   Int(64),
				"name": String(),
				"age":  Int(32),
			},
			node2: Group{
				"id":   Int(64),
				"name": String(),
				"age":  Int(32),
			},
			expected: true,
		},

		// Group nodes - same fields in different order (key difference from EqualNodes)
		{
			name: "groups with same fields in different order",
			node1: Group{
				"id":   Int(64),
				"name": String(),
				"age":  Int(32),
			},
			node2: Group{
				"age":  Int(32),
				"id":   Int(64),
				"name": String(),
			},
			expected: true, // This should be true for SameNodes but false for EqualNodes
		},

		// Group nodes - different fields
		{
			name: "groups with different field names",
			node1: Group{
				"id":   Int(64),
				"name": String(),
			},
			node2: Group{
				"id":    Int(64),
				"title": String(),
			},
			expected: false,
		},
		{
			name: "groups with different field types",
			node1: Group{
				"id":   Int(64),
				"name": String(),
			},
			node2: Group{
				"id":   Int(32), // Different type
				"name": String(),
			},
			expected: false,
		},

		// Nested groups - same structure, different order
		{
			name: "nested groups with field reordering",
			node1: Group{
				"user": Group{
					"id":   Int(64),
					"name": String(),
				},
				"metadata": Group{
					"created": Date(),
					"updated": Date(),
				},
			},
			node2: Group{
				"metadata": Group{
					"updated": Date(), // Different order
					"created": Date(),
				},
				"user": Group{
					"name": String(), // Different order
					"id":   Int(64),
				},
			},
			expected: true,
		},

		// Mixed cases
		{
			name: "groups with different repetition",
			node1: Required(Group{
				"name": String(),
			}),
			node2: Optional(Group{
				"name": String(),
			}),
			expected: false,
		},

		// MAP and LIST types - logical type preserved
		{
			name:     "same map types",
			node1:    Map(String(), Int(32)),
			node2:    Map(String(), Int(32)),
			expected: true,
		},
		{
			name:  "map vs regular group with same structure",
			node1: Map(String(), Int(32)),
			node2: Group{
				"key_value": Repeated(Group{
					"key":   String(),
					"value": Int(32),
				}),
			},
			expected: false, // Different logical types
		},

		// Edge cases
		{
			name:     "empty groups",
			node1:    Group{},
			node2:    Group{},
			expected: true,
		},
		{
			name:     "leaf vs group",
			node1:    String(),
			node2:    Group{"name": String()},
			expected: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := SameNodes(test.node1, test.node2)
			if result != test.expected {
				t.Errorf("SameNodes(%v, %v) = %v, expected %v",
					test.node1, test.node2, result, test.expected)
			}

			// Also verify that for leaf nodes, SameNodes behaves identically to EqualNodes
			if test.node1.Leaf() && test.node2.Leaf() {
				equalResult := EqualNodes(test.node1, test.node2)
				if result != equalResult {
					t.Errorf("SameNodes and EqualNodes should be identical for leaf nodes, got SameNodes=%v EqualNodes=%v",
						result, equalResult)
				}
			}
		})
	}
}

func TestSameNodesVsEqualNodes(t *testing.T) {
	// Test case that shows the difference between SameNodes and EqualNodes
	// Create groups dynamically to ensure different field orders

	// Create first group by building it step by step
	group1 := make(Group)
	group1["FieldA"] = String()
	group1["FieldB"] = Int(32)
	group1["FieldC"] = Date()

	// Create second group in different order
	group2 := make(Group)
	group2["FieldC"] = Date() // Different order
	group2["FieldA"] = String()
	group2["FieldB"] = Int(32)

	// Verify the fields are actually in different order by checking field iteration
	fields1 := group1.Fields()
	fields2 := group2.Fields()

	// Check if any field is in a different position
	differentOrder := false
	for i := range fields1 {
		if fields1[i].Name() != fields2[i].Name() {
			differentOrder = true
			break
		}
	}

	// Only run the EqualNodes test if they're actually in different order
	if differentOrder {
		// SameNodes should be true (order-independent)
		if !SameNodes(group1, group2) {
			t.Error("SameNodes should return true for same fields in different order")
		}

		// EqualNodes should be false (order-dependent)
		if EqualNodes(group1, group2) {
			t.Error("EqualNodes should return false for same fields in different order")
		}
	} else {
		// If they happen to be in the same order, both should be true
		t.Logf("Groups ended up in same order, both comparisons should be true")
		if !SameNodes(group1, group2) {
			t.Error("SameNodes should return true for same fields")
		}
		if !EqualNodes(group1, group2) {
			t.Error("EqualNodes should return true for same fields in same order")
		}
	}
}

func TestEncodingOf(t *testing.T) {
	testCases := []struct {
		name             string
		node             Node
		defaultEncodings map[Kind]encoding.Encoding
		expectedEncoding encoding.Encoding
	}{
		{
			name: "node with encoding, no default",
			node: &mockNode{
				enc: func() encoding.Encoding { return &DeltaBinaryPacked },
				typ: func() Type { return Int32Type },
			},
			defaultEncodings: nil,
			expectedEncoding: &DeltaBinaryPacked,
		},
		{
			name: "node with encoding, different default",
			node: &mockNode{
				enc: func() encoding.Encoding { return &DeltaBinaryPacked },
				typ: func() Type { return Int32Type },
			},
			defaultEncodings: map[Kind]encoding.Encoding{
				Int32: &Plain,
			},
			expectedEncoding: &DeltaBinaryPacked,
		},
		{
			name: "node without encoding, with default",
			node: &mockNode{
				enc: func() encoding.Encoding { return nil },
				typ: func() Type { return ByteArrayType },
			},
			defaultEncodings: map[Kind]encoding.Encoding{
				ByteArray: &DeltaByteArray,
			},
			expectedEncoding: &DeltaByteArray,
		},
		{
			name: "node without encoding, no default, not ByteArray",
			node: &mockNode{
				enc: func() encoding.Encoding { return nil },
				typ: func() Type { return Int32Type },
			},
			defaultEncodings: nil,
			expectedEncoding: &Plain,
		},
		{
			name: "node without encoding, no default, ByteArray",
			node: &mockNode{
				enc: func() encoding.Encoding { return nil },
				typ: func() Type { return ByteArrayType },
			},
			defaultEncodings: nil,
			expectedEncoding: &DeltaLengthByteArray,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			enc := encodingOf(tc.node, tc.defaultEncodings)
			if enc == nil {
				t.Error("encodingOf returned nil")
			}
			if enc.String() != tc.expectedEncoding.String() {
				t.Errorf("encodingOf returned %s, but expected %s",
					enc.String(), tc.expectedEncoding.String())
			}
		})
	}
}

func TestGoTypeOfGroup(t *testing.T) {
	groupNode := Group{
		// Normal names
		"AlreadyExported": Leaf(ByteArrayType),
		"not_exported":    Leaf(FloatType),
		// Go keywords are okay because they get upper-cased
		"for":  Leaf(Int32Type),
		"if":   Leaf(Int64Type),
		"type": Leaf(BooleanType),
		// Needs prefix to export
		"_underscore_": String(),
		"123":          String(),
		// Invalid because they contain invalid characters
		"$abc":        Uint(32),
		`abc"def"ghi`: Uint(64),
		// Conflicts
		"alreadyExported": Leaf(DoubleType),
		"_abc":            Leaf(Int96Type),
		"@abc":            Leaf(FixedLenByteArrayType(4)),
	}
	goType := groupNode.GoType()
	var expectedType struct {
		X_abc            int32            `parquet:"$abc"`
		X123             []byte           `parquet:"123"`
		X_abc_           [4]byte          `parquet:"@abc"`
		AlreadyExported  []byte           `parquet:"AlreadyExported"`
		X_abc__          deprecated.Int96 `parquet:"_abc"`
		X_underscore_    []byte           `parquet:"_underscore_"`
		Abc_def_ghi      int64            `parquet:"abc\"def\"ghi"`
		AlreadyExported_ float64          `parquet:"alreadyExported"`
		For              int32            `parquet:"for"`
		If               int64            `parquet:"if"`
		Not_exported     float32          `parquet:"not_exported"`
		Type             bool             `parquet:"type"`
	}
	if !goType.AssignableTo(reflect.TypeOf(expectedType)) {
		t.Errorf("Unexpected GoType for group: %v", goType)
	}

	// GoType is not possible for a group with a field
	// whose name contains a comma since we cannot encode
	// the field name into a valid Go struct tag value.
	var p any
	func() {
		defer func() {
			p = recover()
		}()
		Group{
			"a,b,c": String(),
		}.GoType()
	}()
	if p == nil {
		t.Error("expected GoType() to panic for a group with a field whose name contains a comma")
	} else {
		t.Log(p)
	}
}
