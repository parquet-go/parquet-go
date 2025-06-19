package parquet

import (
	"reflect"
	"testing"

	"github.com/parquet-go/parquet-go/compress"
	"github.com/parquet-go/parquet-go/compress/gzip"
	"github.com/parquet-go/parquet-go/compress/snappy"
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
					"id":      Int(64),
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
					"id":      Int(64),
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
					"id":      Int(64),
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
					"id":      Int(64),
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
			name: "logical map vs regular group with same structure",
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
			name: "logical list vs regular group with same structure",
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
			node2: Map(String(), Int(32)),
			expected: false,
		},
		{
			name: "regular group vs logical list with same structure",
			node1: Group{
				"list": Repeated(Group{
					"element": String(),
				}),
			},
			node2: List(String()),
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
