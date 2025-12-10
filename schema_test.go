package parquet_test

import (
	"bytes"
	"io"
	"reflect"
	"testing"

	"github.com/parquet-go/parquet-go"
)

func TestSchemaOf(t *testing.T) {
	tests := []struct {
		value any
		print string
	}{
		{
			value: new(struct{ Name string }),
			print: `message {
	required binary Name (STRING);
}`,
		},

		{
			value: new(struct {
				X int
				Y int
			}),
			print: `message {
	required int64 X (INT(64,true));
	required int64 Y (INT(64,true));
}`,
		},

		{
			value: new(struct {
				X float32
				Y float32
			}),
			print: `message {
	required float X;
	required float Y;
}`,
		},

		{
			value: new(struct {
				Inner struct {
					FirstName string `parquet:"first_name"`
					LastName  string `parquet:"last_name"`
				} `parquet:"inner,optional"`
			}),
			print: `message {
	optional group inner {
		required binary first_name (STRING);
		required binary last_name (STRING);
	}
}`,
		},

		{
			value: new(struct {
				Short float32 `parquet:"short,split"`
				Long  float64 `parquet:"long,split"`
			}),
			print: `message {
	required float short;
	required double long;
}`,
		},

		{
			value: new(struct {
				Inner struct {
					FirstName          string `parquet:"first_name"`
					ShouldNotBePresent string `parquet:"-"`
				} `parquet:"inner,optional"`
			}),
			print: `message {
	optional group inner {
		required binary first_name (STRING);
	}
}`,
		},

		{
			value: new(struct {
				Inner struct {
					FirstName    string `parquet:"first_name"`
					MyNameIsDash string `parquet:"-,"`
				} `parquet:"inner,optional"`
			}),
			print: `message {
	optional group inner {
		required binary first_name (STRING);
		required binary - (STRING);
	}
}`,
		},

		{
			value: new(struct{ Name *string }),
			print: `message {
	optional binary Name (STRING);
}`,
		},
	}

	for _, test := range tests {
		schema := parquet.SchemaOf(test.value)
		if s := schema.String(); s != test.print {
			t.Errorf("\nexpected:\n\n%s\n\nfound:\n\n%s\n", test.print, s)
		}
	}
}

func TestSchemaOptions(t *testing.T) {
	tests := []struct {
		name    string
		model   any
		options []parquet.SchemaOption
	}{
		{name: "StructTag", model: struct{ FirstName, LastName string }{}, options: []parquet.SchemaOption{
			parquet.StructTag(`parquet:"first_name"`, "FirstName"),
			parquet.StructTag(`parquet:"last_name"`, "LastName"),
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			parquet.SchemaOf(test.model, test.options...)
		})
	}
}

func TestSchemaRoundTrip(t *testing.T) {
	tests := []struct {
		name         string
		schema       *parquet.Schema
		roundTripped string
	}{
		{
			name: "groups",
			schema: parquet.NewSchema("root", parquet.Group{
				"groups": parquet.Optional(parquet.Group{
					"a": parquet.String(),
					"b": parquet.Int(32),
				}),
			}),
			roundTripped: `message root {
	optional group groups {
		required binary a (STRING);
		required int32 b (INT(32,true));
	}
}`,
		},
		{
			name: "optionals",
			schema: parquet.NewSchema("root", parquet.Group{
				"optionals": parquet.Optional(parquet.Group{
					"bool":   parquet.Optional(parquet.Leaf(parquet.BooleanType)),
					"int32":  parquet.Optional(parquet.Int(32)),
					"int64":  parquet.Optional(parquet.Int(64)),
					"float":  parquet.Optional(parquet.Leaf(parquet.FloatType)),
					"double": parquet.Optional(parquet.Leaf(parquet.DoubleType)),
					"string": parquet.Optional(parquet.String()),
					"bytes":  parquet.Optional(parquet.Leaf(parquet.ByteArrayType)),
				}),
			}),
			roundTripped: `message root {
	optional group optionals {
		optional boolean bool;
		optional binary bytes;
		optional double double;
		optional float float;
		optional int32 int32 (INT(32,true));
		optional int64 int64 (INT(64,true));
		optional binary string (STRING);
	}
}`,
		},
		{
			name: "lists",
			schema: parquet.NewSchema("root", parquet.Group{
				"lists": parquet.Optional(parquet.Group{
					"groups": parquet.List(parquet.Group{
						"a": parquet.String(),
						"b": parquet.Int(32),
					}),
					"ints":    parquet.List(parquet.Int(32)),
					"strings": parquet.Optional(parquet.List(parquet.String())),
				}),
			}),
			roundTripped: `message root {
	optional group lists {
		required group groups (LIST) {
			repeated group list {
				required group element {
					required binary a (STRING);
					required int32 b (INT(32,true));
				}
			}
		}
		required group ints (LIST) {
			repeated group list {
				required int32 element (INT(32,true));
			}
		}
		optional group strings (LIST) {
			repeated group list {
				required binary element (STRING);
			}
		}
	}
}`,
		},
		{
			name: "maps",
			schema: parquet.NewSchema("root", parquet.Group{
				"maps": parquet.Optional(parquet.Group{
					"ints2ints":    parquet.Map(parquet.Int(32), parquet.Int(32)),
					"ints2strings": parquet.Optional(parquet.Map(parquet.Int(32), parquet.String())),
					"strings2groups": parquet.Map(parquet.String(), parquet.Optional(parquet.Group{
						"a": parquet.String(),
						"b": parquet.Int(32),
					})),
				}),
			}),
			roundTripped: `message root {
	optional group maps {
		required group ints2ints (MAP) {
			repeated group key_value {
				required int32 key (INT(32,true));
				required int32 value (INT(32,true));
			}
		}
		optional group ints2strings (MAP) {
			repeated group key_value {
				required int32 key (INT(32,true));
				required binary value (STRING);
			}
		}
		required group strings2groups (MAP) {
			repeated group key_value {
				required binary key (STRING);
				optional group value {
					required binary a (STRING);
					required int32 b (INT(32,true));
				}
			}
		}
	}
}`,
		},
		{
			name: "repeated_fields",
			schema: parquet.NewSchema("root", parquet.Group{
				"repeated_fields": parquet.Optional(parquet.Group{
					"groups": parquet.Repeated(parquet.Group{
						"a": parquet.String(),
						"b": parquet.Int(32),
					}),
					"ints":    parquet.Repeated(parquet.Int(32)),
					"strings": parquet.Repeated(parquet.String()),
				}),
			}),
			roundTripped: `message root {
	optional group repeated_fields {
		repeated group groups {
			required binary a (STRING);
			required int32 b (INT(32,true));
		}
		repeated int32 ints (INT(32,true));
		repeated binary strings (STRING);
	}
}`,
		},
		{
			name: "variants",
			schema: parquet.NewSchema("root", parquet.Group{
				"variants": parquet.Optional(parquet.Group{
					"unshredded": parquet.Optional(parquet.Variant()),
					// TODO: Also include shredded variants when they are implemented
				}),
			}),
			roundTripped: `message root {
	optional group variants {
		optional group unshredded (VARIANT) {
			required binary metadata;
			required binary value;
		}
	}
}`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Write schema to file.
			var fileContents bytes.Buffer
			writer := parquet.NewGenericWriter[any](&fileContents, &parquet.WriterConfig{Schema: test.schema})
			// We need at least one row in the file, so synthesize a single row that is empty
			// with zero values for any required columns.
			row := make([]parquet.Value, len(test.schema.Columns()))
			for i, columnPath := range test.schema.Columns() {
				leaf, ok := test.schema.Lookup(columnPath...)
				if !ok {
					t.Fatalf("failed to look up path %q in schema", test.schema.Columns()[i])
				}
				if leaf.MaxDefinitionLevel > 0 {
					// optional
					row[i] = parquet.NullValue().Level(0, 0, i)
				} else {
					// required, so we need a value of the right type
					switch leaf.Node.Type().Kind() {
					case parquet.Boolean:
						row[i] = parquet.BooleanValue(false)
					case parquet.Int32:
						row[i] = parquet.Int32Value(0)
					case parquet.Int64:
						row[i] = parquet.Int64Value(0)
					case parquet.Int96:
						row[i] = parquet.Int96Value([3]uint32{0, 0, 0})
					case parquet.Float:
						row[i] = parquet.FloatValue(0)
					case parquet.Double:
						row[i] = parquet.DoubleValue(0)
					case parquet.ByteArray:
						row[i] = parquet.ByteArrayValue([]byte{})
					case parquet.FixedLenByteArray:
						row[i] = parquet.FixedLenByteArrayValue(make([]byte, leaf.Node.Type().Length()))
					}
				}
			}
			if _, err := writer.WriteRows([]parquet.Row{row}); err != nil {
				t.Fatalf("failed to write rows: %v", err)
			}
			if err := writer.Close(); err != nil {
				t.Fatalf("failed to close writer: %v", err)
			}

			// Read it back and make sure everything looks right.
			file, err := parquet.OpenFile(bytes.NewReader(fileContents.Bytes()), int64(fileContents.Len()))
			if err != nil {
				t.Fatalf("failed to open file: %v", err)
			}
			if s := file.Schema().String(); s != test.roundTripped {
				t.Errorf("\nexpected:\n\n%s\n\nfound:\n\n%s\n", test.roundTripped, s)
			}
		})
	}
}

func TestSchemaOfOptions(t *testing.T) {
	tests := []struct {
		name     string
		value    any
		expected any
		options  []parquet.SchemaOption
	}{
		{
			name: "noop",
			value: new(struct {
				A string
				B struct {
					C string
				}
			}),
			expected: new(struct {
				A string
				B struct {
					C string
				}
			}),
		},
		{
			name: "nested", // Change name, encoding, compression, and drop columns of nested struct
			value: new(struct {
				A string
				B struct {
					C string `parquet:",snappy,dict"`
					D string
				}
			}),
			options: []parquet.SchemaOption{
				parquet.StructTag(`parquet:"C2,zstd,dict"`, "B", "C"),
				parquet.StructTag(`parquet:"-"`, "B", "D"),
			},
			expected: new(struct {
				A string
				B struct {
					C2 string `parquet:",zstd,dict"`
				}
			}),
		},
		{
			name: "embedded", // Change name, encoding, compression, and drop of embedded field
			value: func() any {
				type Embedded struct {
					A string
					B string
				}
				return new(struct {
					Embedded
				})
			}(),
			options: []parquet.SchemaOption{
				parquet.StructTag(`parquet:"A2,snappy,dict"`, "A"),
				parquet.StructTag(`parquet:"-"`, "B"),
			},
			expected: func() any {
				type Embedded struct {
					A2 string `parquet:",snappy,dict"`
				}
				return new(struct {
					Embedded
				})
			}(),
		},
		{
			name: "nested map", // Change name, encoding, compression, and drop of nested map
			value: func() any {
				return new(struct {
					A map[string]struct {
						B string `parquet:",snappy"`
						C string
					}
				})
			}(),
			options: []parquet.SchemaOption{
				parquet.StructTag(`parquet:"B2,zstd,dict"`, "A", "key_value", "value", "B"),
				parquet.StructTag(`parquet:"-"`, "A", "key_value", "value", "C"),
			},
			expected: func() any {
				return new(struct {
					A map[string]struct {
						B2 string `parquet:",zstd,dict"`
					}
				})
			}(),
		},
		{
			name: "nested slice", // Change name, encoding, compression, and drop within nested slice (non-LIST)
			value: func() any {
				return new(struct {
					A []struct {
						B string `parquet:",snappy"`
						C string
					}
				})
			}(),
			options: []parquet.SchemaOption{
				parquet.StructTag(`parquet:"B2,zstd,dict"`, "A", "B"),
				parquet.StructTag(`parquet:"-"`, "A", "C"),
			},
			expected: func() any {
				return new(struct {
					A []struct {
						B2 string `parquet:",zstd,dict"`
					}
				})
			}(),
		},
		{
			name: "nested list", // Change name, encoding, compression, and drop within nested LIST
			value: func() any {
				return new(struct {
					A []struct {
						B string `parquet:",snappy"`
						C string
					} `parquet:",list"`
				})
			}(),
			options: []parquet.SchemaOption{
				parquet.StructTag(`parquet:"B2,zstd,dict"`, "A", "list", "element", "B"),
				parquet.StructTag(`parquet:"-"`, "A", "list", "element", "C"),
			},
			expected: func() any {
				return new(struct {
					A []struct {
						B2 string `parquet:",zstd,dict"`
					} `parquet:",list"`
				})
			}(),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			want := parquet.SchemaOf(test.expected)
			got := parquet.SchemaOf(test.value, test.options...)

			if !identicalNodes(want, got) {
				t.Errorf("schema not identical want: %v got: %v", want, got)
			}
		})
	}
}

// identicalNodes is like EqualNodes but also checks field order, encoding, and compression.
func identicalNodes(node1, node2 parquet.Node) bool {
	if node1.Leaf() {
		return node2.Leaf() && leafNodesAreIdentical(node1, node2)
	} else {
		return !node2.Leaf() && groupNodesAreIdentical(node1, node2)
	}
}

func leafNodesAreIdentical(node1, node2 parquet.Node) bool {
	if node1.ID() != node2.ID() {
		return false
	}

	if !parquet.EqualTypes(node1.Type(), node2.Type()) {
		return false
	}

	// Leaf nodes must have the same final type.
	if node1.GoType() != node2.GoType() {
		return false
	}

	repetitionsAreEqual := node1.Optional() == node2.Optional() && node1.Repeated() == node2.Repeated()
	if !repetitionsAreEqual {
		return false
	}

	enc1 := node1.Encoding()
	enc2 := node2.Encoding()
	if (enc1 != nil) != (enc2 != nil) {
		return false
	}
	if enc1 != nil && enc2 != nil && enc1.String() != enc2.String() {
		return false
	}

	comp1 := node1.Compression()
	comp2 := node2.Compression()
	if (comp1 != nil) != (comp2 != nil) {
		return false
	}
	if comp1 != nil && comp2 != nil && comp1.String() != comp2.String() {
		return false
	}

	return true
}

func groupNodesAreIdentical(node1, node2 parquet.Node) bool {
	if node1.ID() != node2.ID() {
		return false
	}
	fields1 := node1.Fields()
	fields2 := node2.Fields()
	if len(fields1) != len(fields2) {
		return false
	}
	repetitionsAreEqual := node1.Optional() == node2.Optional() && node1.Repeated() == node2.Repeated()
	if !repetitionsAreEqual {
		return false
	}
	if !fieldsAreEqual(fields1, fields2, identicalNodes) {
		return false
	}

	// TODO - Check for equivalent but not exact go type?
	// For testing, a different go type might be present for structs.
	/*if node1.GoType() != node2.GoType() {
		return false
	}*/

	equalLogicalTypes := reflect.DeepEqual(node1.Type().LogicalType(), node2.Type().LogicalType())
	return equalLogicalTypes
}

func fieldsAreEqual(fields1, fields2 []parquet.Field, equal func(parquet.Node, parquet.Node) bool) bool {
	if len(fields1) != len(fields2) {
		return false
	}
	for i := range fields1 {
		if fields1[i].Name() != fields2[i].Name() {
			return false
		}
	}
	for i := range fields1 {
		if !equal(fields1[i], fields2[i]) {
			return false
		}
	}
	return true
}

func TestSchemaInteroperability(t *testing.T) {
	// All of these types generate the same parquet/leaf structure
	// with one column named A. This test verifies that the schemas
	// and structs can be used interchangeably when reading and writing
	// using the generic reader and writer.
	type One struct {
		A int `parquet:",snappy"`
	}
	type Two struct {
		A int `parquet:",gzip"`
		B int `parquet:"-"`
	}
	type Three struct {
		B int `parquet:"A"`
	}
	type Four struct {
		C int
		D int
	}

	var (
		s1      = parquet.SchemaOf(&One{})
		s2      = parquet.SchemaOf(&Two{})
		s3      = parquet.SchemaOf(&Three{})
		tag1    = parquet.StructTag(`parquet:"A"`, "C")
		tag2    = parquet.StructTag(`parquet:"-"`, "D")
		s4      = parquet.SchemaOf(&Four{}, tag1, tag2)
		schemas = []*parquet.Schema{s1, s2, s3, s4}
	)

	t.Run("T=One", func(t *testing.T) {
		t.Run("Schema=nil", func(t *testing.T) {
			roundtripTester[One]{}.test(t, One{A: 1})
		})
		for _, schema := range schemas {
			t.Run("Schema="+schema.Name(), func(t *testing.T) {
				roundtripTester[One]{}.test(t, One{A: 1}, schema)
			})
		}
	})

	t.Run("T=Two", func(t *testing.T) {
		t.Run("Schema=nil", func(t *testing.T) {
			roundtripTester[Two]{}.test(t, Two{A: 1})
		})
		for _, schema := range schemas {
			t.Run("Schema="+schema.Name(), func(t *testing.T) {
				roundtripTester[Two]{}.test(t, Two{A: 1}, schema)
			})
		}
	})
	t.Run("T=Three", func(t *testing.T) {
		t.Run("Schema=nil", func(t *testing.T) {
			roundtripTester[Three]{}.test(t, Three{B: 1})
		})
		for _, schema := range schemas {
			t.Run("Schema="+schema.Name(), func(t *testing.T) {
				roundtripTester[Three]{}.test(t, Three{B: 1}, schema)
			})
		}
	})
	t.Run("T=Four", func(t *testing.T) {
		t.Run("Schema=nil", func(t *testing.T) {
			roundtripTester[Four]{}.test(t, Four{C: 1}, tag1, tag2)
		})
		for _, schema := range schemas {
			t.Run("Schema="+schema.Name(), func(t *testing.T) {
				roundtripTester[Four]{}.test(t, Four{C: 1}, schema, tag1, tag2)
			})
		}
	})
}

type roundtripTester[T any] struct{}

func (_ roundtripTester[T]) test(t *testing.T, val T, options ...any) {
	buf := bytes.NewBuffer(nil)

	readerOptions := []parquet.ReaderOption{}
	writerOptions := []parquet.WriterOption{}
	for _, option := range options {
		if r, ok := option.(parquet.ReaderOption); ok {
			readerOptions = append(readerOptions, r)
		}
		if w, ok := option.(parquet.WriterOption); ok {
			writerOptions = append(writerOptions, w)
		}
	}

	w := parquet.NewGenericWriter[T](buf, writerOptions...)
	if _, err := w.Write([]T{val}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	rows := make([]T, 1)
	r := parquet.NewGenericReader[T](bytes.NewReader(buf.Bytes()), readerOptions...)
	n, err := r.Read(rows)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if n != 1 || len(rows) != 1 {
		t.Fatal("expected 1 row, got ", n)
	}
	if !reflect.DeepEqual(rows[0], val) {
		t.Fatal("expected ", val, " got ", rows[0])
	}
}

// TestIssue163EmbeddedPointerNilValues tests that embedded pointer types
// with nil values are preserved as nil and not converted to zero-values.
// See: https://github.com/parquet-go/parquet-go/issues/163
func TestIssue163EmbeddedPointerNilValues(t *testing.T) {
	type Embedded struct {
		EmbeddedInt    *int
		EmbeddedString *string
	}

	type TestStruct struct {
		Embedded
		TopLevelInt    *int
		TopLevelString *string
	}

	tests := []struct {
		name     string
		input    TestStruct
		expected TestStruct
	}{
		{
			name: "all nil values",
			input: TestStruct{
				Embedded: Embedded{
					EmbeddedInt:    nil,
					EmbeddedString: nil,
				},
				TopLevelInt:    nil,
				TopLevelString: nil,
			},
			expected: TestStruct{
				Embedded: Embedded{
					EmbeddedInt:    nil,
					EmbeddedString: nil,
				},
				TopLevelInt:    nil,
				TopLevelString: nil,
			},
		},
		{
			name: "all values set to 12",
			input: TestStruct{
				Embedded: Embedded{
					EmbeddedInt:    intPtr(12),
					EmbeddedString: strPtr("12"),
				},
				TopLevelInt:    intPtr(12),
				TopLevelString: strPtr("12"),
			},
			expected: TestStruct{
				Embedded: Embedded{
					EmbeddedInt:    intPtr(12),
					EmbeddedString: strPtr("12"),
				},
				TopLevelInt:    intPtr(12),
				TopLevelString: strPtr("12"),
			},
		},
		{
			name: "all values set to zero",
			input: TestStruct{
				Embedded: Embedded{
					EmbeddedInt:    intPtr(0),
					EmbeddedString: strPtr(""),
				},
				TopLevelInt:    intPtr(0),
				TopLevelString: strPtr(""),
			},
			expected: TestStruct{
				Embedded: Embedded{
					EmbeddedInt:    intPtr(0),
					EmbeddedString: strPtr(""),
				},
				TopLevelInt:    intPtr(0),
				TopLevelString: strPtr(""),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Save a copy of the original input for mutation check
			originalInput := tt.input

			// Write to parquet
			buf := new(bytes.Buffer)
			writer := parquet.NewGenericWriter[TestStruct](buf)
			_, err := writer.Write([]TestStruct{tt.input})
			if err != nil {
				t.Fatalf("failed to write: %v", err)
			}
			if err := writer.Close(); err != nil {
				t.Fatalf("failed to close writer: %v", err)
			}

			// Check that the original input was not mutated
			if !reflect.DeepEqual(originalInput, tt.input) {
				t.Errorf("input was mutated during write:\noriginal: %+v\nmutated:  %+v", originalInput, tt.input)
			}

			// Read back from parquet
			reader := parquet.NewGenericReader[TestStruct](bytes.NewReader(buf.Bytes()))
			rows := make([]TestStruct, 1)
			n, err := reader.Read(rows)
			if err != nil && err != io.EOF {
				t.Fatalf("failed to read: %v", err)
			}
			if n != 1 {
				t.Fatalf("expected 1 row, got %d", n)
			}

			// Check that nil values are preserved
			result := rows[0]
			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("result mismatch:\nexpected: %+v\ngot:      %+v", tt.expected, result)

				// Detailed field-by-field comparison for better debugging
				if !ptrEqual(result.EmbeddedInt, tt.expected.EmbeddedInt) {
					t.Errorf("EmbeddedInt mismatch: expected %v, got %v",
						ptrToString(tt.expected.EmbeddedInt), ptrToString(result.EmbeddedInt))
				}
				if !ptrEqual(result.EmbeddedString, tt.expected.EmbeddedString) {
					t.Errorf("EmbeddedString mismatch: expected %v, got %v",
						ptrToString(tt.expected.EmbeddedString), ptrToString(result.EmbeddedString))
				}
				if !ptrEqual(result.TopLevelInt, tt.expected.TopLevelInt) {
					t.Errorf("TopLevelInt mismatch: expected %v, got %v",
						ptrToString(tt.expected.TopLevelInt), ptrToString(result.TopLevelInt))
				}
				if !ptrEqual(result.TopLevelString, tt.expected.TopLevelString) {
					t.Errorf("TopLevelString mismatch: expected %v, got %v",
						ptrToString(tt.expected.TopLevelString), ptrToString(result.TopLevelString))
				}
			}
		})
	}
}

func intPtr(i int) *int       { return &i }
func strPtr(s string) *string { return &s }

func ptrEqual[T comparable](a, b *T) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

func ptrToString[T any](p *T) string {
	if p == nil {
		return "nil"
	}
	return reflect.ValueOf(*p).String()
}
