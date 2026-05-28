package parquet_test

import (
	"io"
	"reflect"
	"testing"

	"github.com/google/uuid"
	"github.com/parquet-go/parquet-go"
)

type bufferedRows struct {
	rows []parquet.Row
}

func (r *bufferedRows) ReadRows(rows []parquet.Row) (int, error) {
	for i := range rows {
		if len(r.rows) == 0 {
			return i, io.EOF
		}
		rows[i] = append(rows[i][:0], r.rows[0]...)
		r.rows = r.rows[1:]
	}
	return len(rows), nil
}

func (w *bufferedRows) WriteRows(rows []parquet.Row) (int, error) {
	for _, row := range rows {
		w.rows = append(w.rows, row.Clone())
	}
	return len(rows), nil
}

func TestMultiRowWriter(t *testing.T) {
	b1 := new(bufferedRows)
	b2 := new(bufferedRows)
	mw := parquet.MultiRowWriter(b1, b2)

	rows := []parquet.Row{
		{
			parquet.Int32Value(10).Level(0, 0, 0),
			parquet.Int32Value(11).Level(0, 0, 1),
			parquet.Int32Value(12).Level(0, 0, 2),
		},
		{
			parquet.Int32Value(20).Level(0, 0, 0),
			parquet.Int32Value(21).Level(0, 0, 1),
			parquet.Int32Value(22).Level(0, 0, 2),
		},
	}

	n, err := mw.WriteRows(rows)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(rows) {
		t.Fatalf("number of rows written mismatch: got=%d want=%d", n, len(rows))
	}

	assertEqualRows(t, rows, b1.rows)
	assertEqualRows(t, rows, b2.rows)
}

func TestRowClone(t *testing.T) {
	row := parquet.Row{
		parquet.ValueOf(42).Level(0, 1, 0),
		parquet.ValueOf("Hello World").Level(1, 1, 1),
	}
	if clone := row.Clone(); !row.Equal(clone) {
		t.Error("row and its clone are not equal")
	}
}

func TestDeconstructionReconstruction(t *testing.T) {
	type Person struct {
		FirstName string
		LastName  string
		Age       int     `parquet:",optional"`
		Weight    float64 `parquet:",optional"`
	}

	type Details struct {
		Person *Person
	}

	type Friend struct {
		ID      [16]byte `parquet:",uuid"`
		Details *Details
	}

	type User struct {
		ID      [16]byte `parquet:",uuid"`
		Details *Details
		Friends []Friend `parquet:",list,optional"`
	}

	type List2 struct {
		Value string `parquet:",optional"`
	}

	type List1 struct {
		List2 []List2 `parquet:",list"`
	}

	type List0 struct {
		List1 []List1 `parquet:",list"`
	}

	type nestedListsLevel1 struct {
		Level2 []string `parquet:"level2"`
	}

	type nestedLists struct {
		Level1 []nestedListsLevel1 `parquet:"level1"`
	}

	tests := []struct {
		scenario string
		input    any
		values   [][]parquet.Value
	}{
		{
			scenario: "single field",
			input: struct {
				Name string
			}{Name: "Luke"},
			values: [][]parquet.Value{
				0: {parquet.ValueOf("Luke").Level(0, 0, 0)},
			},
		},

		{
			scenario: "multiple fields",
			input: Person{
				FirstName: "Han",
				LastName:  "Solo",
				Age:       42,
				Weight:    81.5,
			},
			values: [][]parquet.Value{
				0: {parquet.ValueOf("Han").Level(0, 0, 0)},
				1: {parquet.ValueOf("Solo").Level(0, 0, 1)},
				2: {parquet.ValueOf(42).Level(0, 1, 2)},
				3: {parquet.ValueOf(81.5).Level(0, 1, 3)},
			},
		},

		{
			scenario: "empty repeated field",
			input: struct {
				Symbols []string
			}{
				Symbols: []string{},
			},
			values: [][]parquet.Value{
				0: {parquet.ValueOf(nil).Level(0, 0, 0)},
			},
		},

		{
			scenario: "single repeated field",
			input: struct {
				Symbols []string
			}{
				Symbols: []string{"EUR", "USD", "GBP", "JPY"},
			},
			values: [][]parquet.Value{
				0: {
					parquet.ValueOf("EUR").Level(0, 1, 0),
					parquet.ValueOf("USD").Level(1, 1, 0),
					parquet.ValueOf("GBP").Level(1, 1, 0),
					parquet.ValueOf("JPY").Level(1, 1, 0),
				},
			},
		},

		{
			scenario: "multiple repeated field",
			input: struct {
				Symbols []string
				Values  []float32
			}{
				Symbols: []string{"EUR", "USD", "GBP", "JPY"},
				Values:  []float32{0.1, 0.2, 0.3, 0.4},
			},
			values: [][]parquet.Value{
				0: {
					parquet.ValueOf("EUR").Level(0, 1, 0),
					parquet.ValueOf("USD").Level(1, 1, 0),
					parquet.ValueOf("GBP").Level(1, 1, 0),
					parquet.ValueOf("JPY").Level(1, 1, 0),
				},
				1: {
					parquet.ValueOf(float32(0.1)).Level(0, 1, 0),
					parquet.ValueOf(float32(0.2)).Level(1, 1, 0),
					parquet.ValueOf(float32(0.3)).Level(1, 1, 0),
					parquet.ValueOf(float32(0.4)).Level(1, 1, 0),
				},
			},
		},

		{
			scenario: "top level nil pointer field",
			input: struct {
				Person *Person
			}{
				Person: nil,
			},
			// Here there are four nil values because the Person type has four
			// fields but it is nil.
			values: [][]parquet.Value{
				0: {parquet.ValueOf(nil).Level(0, 0, 0)},
				1: {parquet.ValueOf(nil).Level(0, 0, 0)},
				2: {parquet.ValueOf(nil).Level(0, 0, 0)},
				3: {parquet.ValueOf(nil).Level(0, 0, 0)},
			},
		},

		{
			scenario: "top level slice pointer",
			input: struct {
				List []*List2
			}{
				List: []*List2{
					{Value: "foo"},
					{Value: "bar"},
				},
			},
			values: [][]parquet.Value{
				0: {
					parquet.ValueOf("foo").Level(0, 2, 0),
					parquet.ValueOf("bar").Level(1, 2, 0),
				},
			},
		},

		{
			scenario: "sub level nil pointer field",
			input: User{
				ID: uuid.MustParse("A65B576D-9299-4769-9D93-04BE0583F027"),
				Details: &Details{
					Person: nil,
				},
			},
			// Here there are four nil values because the Person type has four
			// fields but it is nil.
			values: [][]parquet.Value{
				// User.ID
				0: {parquet.ValueOf(uuid.MustParse("A65B576D-9299-4769-9D93-04BE0583F027"))},
				// User.Details.Person
				1: {parquet.ValueOf(nil).Level(0, 1, 0)},
				2: {parquet.ValueOf(nil).Level(0, 1, 0)},
				3: {parquet.ValueOf(nil).Level(0, 1, 0)},
				4: {parquet.ValueOf(nil).Level(0, 1, 0)},
				// User.Friends.ID
				5: {parquet.ValueOf(nil).Level(0, 0, 0)},
				// User.Friends.Details.Person
				6: {parquet.ValueOf(nil).Level(0, 0, 0)},
				7: {parquet.ValueOf(nil).Level(0, 0, 0)},
				8: {parquet.ValueOf(nil).Level(0, 0, 0)},
				9: {parquet.ValueOf(nil).Level(0, 0, 0)},
			},
		},

		{
			scenario: "deeply nested structure",
			input: struct {
				User User
			}{
				User: User{
					ID: uuid.MustParse("A65B576D-9299-4769-9D93-04BE0583F027"),
					Details: &Details{
						Person: &Person{
							FirstName: "Luke",
							LastName:  "Skywalker",
						},
					},
					Friends: []Friend{
						{
							ID: uuid.MustParse("1B76F8D0-82C6-403F-A104-DCDA69207220"),
							Details: &Details{
								Person: &Person{
									FirstName: "Han",
									LastName:  "Solo",
								},
							},
						},

						{
							ID: uuid.MustParse("C43C8852-CCE5-40E6-B0DF-7212A5633346"),
							Details: &Details{
								Person: &Person{
									FirstName: "Leia",
									LastName:  "Skywalker",
								},
							},
						},

						{
							ID: uuid.MustParse("E78642A8-0931-4D5F-918F-24DC8FF445B0"),
							Details: &Details{
								Person: &Person{
									FirstName: "C3PO",
									LastName:  "Droid",
								},
							},
						},
					},
				},
			},

			values: [][]parquet.Value{
				// User.ID
				0: {parquet.ValueOf(uuid.MustParse("A65B576D-9299-4769-9D93-04BE0583F027"))},

				// User.Details
				1: {parquet.ValueOf("Luke").Level(0, 2, 0)},
				2: {parquet.ValueOf("Skywalker").Level(0, 2, 0)},
				3: {parquet.ValueOf(nil).Level(0, 2, 0)},
				4: {parquet.ValueOf(nil).Level(0, 2, 0)},

				5: { // User.Friends.ID
					parquet.ValueOf(uuid.MustParse("1B76F8D0-82C6-403F-A104-DCDA69207220")).Level(0, 2, 0),
					parquet.ValueOf(uuid.MustParse("C43C8852-CCE5-40E6-B0DF-7212A5633346")).Level(1, 2, 0),
					parquet.ValueOf(uuid.MustParse("E78642A8-0931-4D5F-918F-24DC8FF445B0")).Level(1, 2, 0),
				},

				6: { // User.Friends.Details.Person.FirstName
					parquet.ValueOf("Han").Level(0, 4, 0),
					parquet.ValueOf("Leia").Level(1, 4, 0),
					parquet.ValueOf("C3PO").Level(1, 4, 0),
				},

				7: { // User.Friends.Details.Person.LastName
					parquet.ValueOf("Solo").Level(0, 4, 0),
					parquet.ValueOf("Skywalker").Level(1, 4, 0),
					parquet.ValueOf("Droid").Level(1, 4, 0),
				},

				8: { // User.Friends.Details.Person.Age
					parquet.ValueOf(nil).Level(0, 4, 0),
					parquet.ValueOf(nil).Level(1, 4, 0),
					parquet.ValueOf(nil).Level(1, 4, 0),
				},

				9: { // User.Friends.Details.Person.Weight
					parquet.ValueOf(nil).Level(0, 4, 0),
					parquet.ValueOf(nil).Level(1, 4, 0),
					parquet.ValueOf(nil).Level(1, 4, 0),
				},
			},
		},

		{
			scenario: "multiple repeated levels",
			input: List0{
				List1: []List1{
					{List2: []List2{{Value: "A"}, {Value: "B"}}},
					{List2: []List2{}}, // parquet doesn't differentiate between empty repeated and a nil list
					{List2: []List2{{Value: "C"}}},
					{List2: []List2{}},
					{List2: []List2{{Value: "D"}, {Value: "E"}, {Value: "F"}}},
					{List2: []List2{{Value: "G"}, {Value: "H"}, {Value: "I"}}},
				},
			},
			values: [][]parquet.Value{
				{
					parquet.ValueOf("A").Level(0, 3, 0),
					parquet.ValueOf("B").Level(2, 3, 0),
					parquet.ValueOf(nil).Level(1, 1, 0),
					parquet.ValueOf("C").Level(1, 3, 0),
					parquet.ValueOf(nil).Level(1, 1, 0),
					parquet.ValueOf("D").Level(1, 3, 0),
					parquet.ValueOf("E").Level(2, 3, 0),
					parquet.ValueOf("F").Level(2, 3, 0),
					parquet.ValueOf("G").Level(1, 3, 0),
					parquet.ValueOf("H").Level(2, 3, 0),
					parquet.ValueOf("I").Level(2, 3, 0),
				},
			},
		},

		// https://blog.twitter.com/engineering/en_us/a/2013/dremel-made-simple-with-parquet

		// message nestedLists {
		//   repeated group level1 {
		//     repeated string level2;
		//   }
		// }
		// ---
		// {
		//   level1: {
		//     level2: a
		//     level2: b
		//     level2: c
		//   },
		//   level1: {
		//     level2: d
		//     level2: e
		//     level2: f
		//     level2: g
		//   }
		// }
		//
		{
			scenario: "twitter blog example 1",
			input: nestedLists{
				Level1: []nestedListsLevel1{
					{Level2: []string{"a", "b", "c"}},
					{Level2: []string{"d", "e", "f", "g"}},
				},
			},
			values: [][]parquet.Value{
				0: {
					parquet.ValueOf("a").Level(0, 2, 0),
					parquet.ValueOf("b").Level(2, 2, 0),
					parquet.ValueOf("c").Level(2, 2, 0),
					parquet.ValueOf("d").Level(1, 2, 0),
					parquet.ValueOf("e").Level(2, 2, 0),
					parquet.ValueOf("f").Level(2, 2, 0),
					parquet.ValueOf("g").Level(2, 2, 0),
				},
			},
		},

		// message nestedLists {
		//   repeated group level1 {
		//     repeated string level2;
		//   }
		// }
		// ---
		// {
		//   level1: {
		//     level2: h
		//   },
		//   level1: {
		//     level2: i
		//     level2: j
		//   }
		// }
		//
		{
			scenario: "twitter blog example 2",
			input: nestedLists{
				Level1: []nestedListsLevel1{
					{Level2: []string{"h"}},
					{Level2: []string{"i", "j"}},
				},
			},
			values: [][]parquet.Value{
				0: {
					parquet.ValueOf("h").Level(0, 2, 0),
					parquet.ValueOf("i").Level(1, 2, 0),
					parquet.ValueOf("j").Level(2, 2, 0),
				},
			},
		},

		// message AddressBook {
		//   required string owner;
		//   repeated string ownerPhoneNumbers;
		//   repeated group contacts {
		//     required string name;
		//     optional string phoneNumber;
		//   }
		// }
		// ---
		// AddressBook {
		//   owner: "Julien Le Dem",
		//   ownerPhoneNumbers: "555 123 4567",
		//   ownerPhoneNumbers: "555 666 1337",
		//   contacts: {
		//     name: "Dmitriy Ryaboy",
		//     phoneNumber: "555 987 6543",
		//   },
		//   contacts: {
		//     name: "Chris Aniszczyk"
		//   }
		// }
		{
			scenario: "twitter blog example 3",
			input: AddressBook{
				Owner: "Julien Le Dem",
				OwnerPhoneNumbers: []string{
					"555 123 4567",
					"555 666 1337",
				},
				Contacts: []Contact{
					{
						Name:        "Dmitriy Ryaboy",
						PhoneNumber: "555 987 6543",
					},
					{
						Name: "Chris Aniszczyk",
					},
				},
			},
			values: [][]parquet.Value{
				0: { // AddressBook.owner
					parquet.ValueOf("Julien Le Dem").Level(0, 0, 0),
				},
				1: { // AddressBook.ownerPhoneNumbers
					parquet.ValueOf("555 123 4567").Level(0, 1, 0),
					parquet.ValueOf("555 666 1337").Level(1, 1, 0),
				},
				2: { // AddressBook.contacts.name
					parquet.ValueOf("Dmitriy Ryaboy").Level(0, 1, 0),
					parquet.ValueOf("Chris Aniszczyk").Level(1, 1, 0),
				},
				3: { // AddressBook.contacts.phoneNumber
					parquet.ValueOf("555 987 6543").Level(0, 2, 0),
					parquet.ValueOf(nil).Level(1, 1, 0),
				},
			},
		},
		{
			scenario: "uuids",
			input: struct {
				A [16]byte `parquet:",uuid"`
				B string   `parquet:",uuid"`
			}{A: uuid.MustParse("a65b576d-9299-4769-9d93-04be0583f027"), B: "a65b576d-9299-4769-9d93-04be0583f027"},
			values: [][]parquet.Value{
				0: {
					parquet.ValueOf(uuid.MustParse("a65b576d-9299-4769-9d93-04be0583f027")).Level(0, 0, 0),
				},
				1: {
					parquet.ValueOf(uuid.MustParse("a65b576d-9299-4769-9d93-04be0583f027")).Level(0, 0, 1),
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			schema := parquet.SchemaOf(test.input)
			row := schema.Deconstruct(nil, test.input)
			values := columnsOf(row)

			t.Logf("\n%s", schema)

			for columnIndex, expect := range test.values {
				assertEqualValues(t, columnIndex, expect, values[columnIndex])
			}

			newValue := reflect.New(reflect.TypeOf(test.input))
			if err := schema.Reconstruct(newValue.Interface(), row); err != nil {
				t.Errorf("reconstruction of the parquet row into a go value failed:\n\t%v", err)
			} else if !reflect.DeepEqual(newValue.Elem().Interface(), test.input) {
				t.Errorf("reconstruction of the parquet row into a go value produced the wrong output:\nwant = %#v\ngot  = %#v", test.input, newValue.Elem())
			}

			for columnIndex := range test.values {
				values[columnIndex] = nil
			}

			for columnIndex, unexpected := range values {
				if unexpected != nil {
					t.Errorf("unexpected column index %d found with %d values in it", columnIndex, len(unexpected))
				}
			}
		})
	}
}

func TestReconstructMapInInterface(t *testing.T) {
	type S struct {
		M any
	}
	type T struct {
		M map[string]string
	}
	schema := parquet.SchemaOf(T{})

	// Test with non-empty map
	row := schema.Deconstruct(nil, T{M: map[string]string{"hello": "world"}})
	var result S
	err := schema.Reconstruct(&result, row)
	if err != nil {
		t.Fatal(err)
	}
	reconstructedMap, ok := result.M.(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any, got %T", result.M)
	}
	if len(reconstructedMap) != 1 || reconstructedMap["hello"] != "world" {
		t.Errorf("unexpected map content: %#v", reconstructedMap)
	}

	// Test with empty map
	row = schema.Deconstruct(nil, T{M: map[string]string{}})
	result = S{}
	err = schema.Reconstruct(&result, row)
	if err != nil {
		t.Fatal(err)
	}
	reconstructedMap, ok = result.M.(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any, got %T", result.M)
	}
	if len(reconstructedMap) != 0 {
		t.Errorf("expected empty map, got %#v", reconstructedMap)
	}
}

func columnsOf(row parquet.Row) [][]parquet.Value {
	columns := make([][]parquet.Value, 0)
	row.Range(func(_ int, c []parquet.Value) bool {
		columns = append(columns, c)
		return true
	})
	return columns
}

func assertEqualRows(t *testing.T, want, got []parquet.Row) {
	if len(want) != len(got) {
		t.Errorf("number of rows mismatch: want=%d got=%d", len(want), len(got))
		return
	}

	for i := range want {
		row1, row2 := want[i], got[i]

		if len(row1) != len(row2) {
			t.Errorf("number of values in row %d mismatch: want=%d got=%d", i, len(row1), len(row2))
			continue
		}

		for j := range row1 {
			if value1, value2 := row1[j], row2[j]; !parquet.DeepEqual(value1, value2) {
				t.Errorf("values of row %d at index %d mismatch: want=%+v got=%+v", i, j, value1, value2)
			}
		}
	}
}

func assertEqualValues(t *testing.T, columnIndex int, want, got []parquet.Value) {
	n := len(want)

	if len(want) != len(got) {
		t.Errorf("wrong number of values in column %d: want=%d got=%d", columnIndex, len(want), len(got))
		if len(want) > len(got) {
			n = len(got)
		}
	}

	for i := range n {
		v1, v2 := want[i], got[i]

		if !parquet.Equal(v1, v2) {
			t.Errorf("values at index %d mismatch in column %d: want=%#v got=%#v", i, columnIndex, v1, v2)
		}
		if columnIndex != int(v2.Column()) {
			t.Errorf("column index mismatch in column %d: want=%d got=%#v", i, columnIndex, v2)
		}
		if v1.RepetitionLevel() != v2.RepetitionLevel() {
			t.Errorf("repetition levels at index %d mismatch in column %d: want=%#v got=%#v", i, columnIndex, v1, v2)
		}
		if v1.DefinitionLevel() != v2.DefinitionLevel() {
			t.Errorf("definition levels at index %d mismatch in column %d: want=%#v got=%#v", i, columnIndex, v1, v2)
		}
	}
}

func BenchmarkDeconstruct(b *testing.B) {
	row := &AddressBook{
		Owner: "Julien Le Dem",
		OwnerPhoneNumbers: []string{
			"555 123 4567",
			"555 666 1337",
		},
		Contacts: []Contact{
			{
				Name:        "Dmitriy Ryaboy",
				PhoneNumber: "555 987 6543",
			},
			{
				Name: "Chris Aniszczyk",
			},
		},
	}

	schema := parquet.SchemaOf(row)
	buffer := parquet.Row{}

	for b.Loop() {
		buffer = schema.Deconstruct(buffer[:0], row)
	}
}

func BenchmarkReconstruct(b *testing.B) {
	row := &AddressBook{
		Owner: "Julien Le Dem",
		OwnerPhoneNumbers: []string{
			"555 123 4567",
			"555 666 1337",
		},
		Contacts: []Contact{
			{
				Name:        "Dmitriy Ryaboy",
				PhoneNumber: "555 987 6543",
			},
			{
				Name: "Chris Aniszczyk",
			},
		},
	}

	schema := parquet.SchemaOf(row)
	values := schema.Deconstruct(nil, row)
	buffer := AddressBook{}

	for b.Loop() {
		buffer = AddressBook{}

		if err := schema.Reconstruct(&buffer, values); err != nil {
			b.Fatal(err)
		}
	}
}

// Untagged values (e.g. from FixedLenByteArrayValue) must be accepted
// positionally; otherwise Range gap-fills forever on raw columnIndex=0.
func TestRowRangeUntaggedPositional(t *testing.T) {
	row := parquet.Row{parquet.FixedLenByteArrayValue([]byte("0123456789abcdef"))}
	var seen []int
	row.Range(func(c int, v []parquet.Value) bool {
		seen = append(seen, c)
		if len(v) != 1 {
			t.Errorf("col=%d: want 1 value, got %d", c, len(v))
		}
		return true
	})
	if len(seen) != 1 || seen[0] != 0 {
		t.Errorf("want [0], got %v", seen)
	}

	row = parquet.Row{parquet.Int32Value(10), parquet.Int32Value(20), parquet.Int32Value(30)}
	seen = seen[:0]
	row.Range(func(c int, _ []parquet.Value) bool {
		seen = append(seen, c)
		return true
	})
	if want := []int{0, 1, 2}; !reflect.DeepEqual(seen, want) {
		t.Errorf("want %v, got %v", want, seen)
	}
}

// Tagged values pointing past the current column must trigger empty-slice
// gap-fills so cross-routing into a neighbour's slot doesn't happen.
func TestRowRangeGapFillForTaggedValues(t *testing.T) {
	type emit struct{ col, n int }

	row := parquet.Row{parquet.Int64Value(0xCAFE).Level(0, 1, 2)}
	var seen []emit
	row.Range(func(c int, v []parquet.Value) bool {
		seen = append(seen, emit{c, len(v)})
		return true
	})
	if want := []emit{{0, 0}, {1, 0}, {2, 1}}; !reflect.DeepEqual(seen, want) {
		t.Errorf("want %v, got %v", want, seen)
	}

	row = parquet.Row{
		parquet.ByteArrayValue([]byte("a")).Level(0, 0, 0),
		parquet.ByteArrayValue([]byte("b")).Level(1, 0, 0),
		parquet.Int64Value(7).Level(0, 0, 2),
	}
	seen = seen[:0]
	row.Range(func(c int, v []parquet.Value) bool {
		seen = append(seen, emit{c, len(v)})
		return true
	})
	if want := []emit{{0, 2}, {1, 0}, {2, 1}}; !reflect.DeepEqual(seen, want) {
		t.Errorf("want %v, got %v", want, seen)
	}
}

// A column with no values for the row (writer elided its tuple) must not
// fail Reconstruct; the Go field stays at zero.
func TestReconstructLeafToleratesEmptyColumn(t *testing.T) {
	type Row struct {
		Blob    []byte `parquet:"Blob,optional"`
		Trigger int64  `parquet:"Trigger"`
	}

	r := parquet.Row{parquet.Int64Value(42).Level(0, 0, 1)}
	var got Row
	if err := parquet.SchemaOf(Row{}).Reconstruct(&got, r); err != nil {
		t.Fatalf("Reconstruct: %v", err)
	}
	if got.Blob != nil {
		t.Errorf("Blob: want nil, got %v", got.Blob)
	}
	if got.Trigger != 42 {
		t.Errorf("Trigger: want 42, got %d", got.Trigger)
	}
}

// Optional-group presence must be decided by any sibling column, not just
// columns[0]: a low-def placeholder in column 0 shouldn't null the group
// when another column carries a present-level value.
func TestReconstructOptionalScansAllColumnsForPresence(t *testing.T) {
	type Inner struct {
		Blob    []byte `parquet:"Blob,optional"`
		Trigger int64  `parquet:"Trigger"`
	}
	type Outer struct {
		Extras *Inner `parquet:"Extras"`
	}
	schema := parquet.SchemaOf(Outer{})

	r := parquet.Row{
		parquet.NullValue().Level(0, 0, 0),
		parquet.Int64Value(7).Level(0, 1, 1),
	}
	var got Outer
	if err := schema.Reconstruct(&got, r); err != nil {
		t.Fatalf("Reconstruct: %v", err)
	}
	if got.Extras == nil {
		t.Fatalf("Extras: want non-nil, got nil")
	}
	if got.Extras.Trigger != 7 {
		t.Errorf("Trigger: want 7, got %d", got.Extras.Trigger)
	}
	if got.Extras.Blob != nil {
		t.Errorf("Blob: want nil, got %v", got.Extras.Blob)
	}

	// All columns below defLevel → Extras null.
	r = parquet.Row{
		parquet.NullValue().Level(0, 0, 0),
		parquet.NullValue().Level(0, 0, 1),
	}
	got = Outer{}
	if err := schema.Reconstruct(&got, r); err != nil {
		t.Fatalf("Reconstruct: %v", err)
	}
	if got.Extras != nil {
		t.Errorf("Extras: want nil, got %+v", *got.Extras)
	}
}

// TestReconstructMapShortSiblingColumn is a regression test for the
// reconstructFuncOfMap padding shim. It simulates a writer that emitted fewer
// level entries than columns[0] expects on a sibling leaf under a Map subtree,
// and asserts that Reconstruct does not panic and assigns the Go zero value to
// the under-supplied slots while leaving the other slots intact.
func TestReconstructMapShortSiblingColumn(t *testing.T) {
	type Entry struct {
		A string
		B string
	}
	type Outer struct {
		M map[string]Entry
	}

	schema := parquet.SchemaOf(Outer{})

	// Build a fully-populated row, then drop one Value from the leaf column
	// corresponding to field B to simulate writer elision. We rebuild the
	// row column-by-column so the (rep, def) levels carried by each Value
	// remain consistent.
	full := schema.Deconstruct(nil, Outer{M: map[string]Entry{
		"k1": {A: "a1", B: "b1"},
		"k2": {A: "a2", B: "b2"},
	}})

	columns := columnsOf(full)

	// Schema leaves are: key, value.A, value.B. The last column is B —
	// drop its trailing Value so the column has only one entry while the
	// key and A columns still carry two.
	bCol := len(columns) - 1
	if len(columns[bCol]) != 2 {
		t.Fatalf("expected exactly 2 values in B column before truncation, got %d", len(columns[bCol]))
	}
	columns[bCol] = columns[bCol][:1]

	var short parquet.Row
	for _, c := range columns {
		short = append(short, c...)
	}

	var got Outer
	if err := schema.Reconstruct(&got, short); err != nil {
		t.Fatalf("Reconstruct after truncation returned error: %v", err)
	}

	if len(got.M) != 2 {
		t.Fatalf("expected 2 map entries after reconstruction, got %d: %#v", len(got.M), got.M)
	}

	// A column was not truncated, so both A values must round-trip with the
	// correct key→A pairing. Exactly one B must be the Go zero value (the
	// truncated entry); the other must match the original non-empty B for
	// its key. Which key ends up with the zero B depends on iteration order
	// during Deconstruct — both outcomes are valid.
	wantA := map[string]string{"k1": "a1", "k2": "a2"}
	wantB := map[string]string{"k1": "b1", "k2": "b2"}

	emptyB := 0
	nonEmptyB := 0
	for k, e := range got.M {
		if a, ok := wantA[k]; !ok {
			t.Errorf("unexpected key %q in reconstructed map", k)
		} else if e.A != a {
			t.Errorf("key %q: A=%q, want %q", k, e.A, a)
		}

		switch e.B {
		case "":
			emptyB++
		case wantB[k]:
			nonEmptyB++
		default:
			t.Errorf("key %q: B=%q, want %q or empty string", k, e.B, wantB[k])
		}
	}

	if emptyB != 1 || nonEmptyB != 1 {
		t.Errorf("expected exactly one empty and one non-empty B after truncation, got empty=%d non-empty=%d (M=%#v)", emptyB, nonEmptyB, got.M)
	}
}

// TestReconstructMapShortSiblingColumnNestedRepeated covers the case where a
// single Map entry can legitimately contribute multiple Values to a sibling
// leaf (here, a nested Map inside the value group). The row is constructed
// manually with explicit Level() calls so the layout is deterministic and
// independent of Go's map iteration order during Deconstruct.
func TestReconstructMapShortSiblingColumnNestedRepeated(t *testing.T) {
	type Entry struct {
		Inner map[string]string
	}
	type Outer struct {
		M map[string]Entry
	}

	schema := parquet.SchemaOf(Outer{})

	// Schema leaves (column indices):
	//   0: M.key_value.key                          max_def=1, max_rep=1
	//   1: M.key_value.value.Inner.key_value.key    max_def=2, max_rep=2
	//   2: M.key_value.value.Inner.key_value.value  max_def=2, max_rep=2
	//
	// We hand-build the row to simulate a non-conformant writer that
	// emitted the outer map {"k1": {"x":"1","y":"2"}, "k2": {}} but
	// elided the (R:1, D:1) placeholder that a spec-compliant writer
	// would have emitted on each inner leaf for k2's empty inner map.
	//
	// Resulting columns:
	//   key   : [k1@R0D1, k2@R1D1]
	//   inner_key   : [x@R0D2, y@R2D2]   (k2's placeholder elided)
	//   inner_value : [1@R0D2, 2@R2D2]   (k2's placeholder elided)
	row := parquet.Row{
		parquet.ValueOf("k1").Level(0, 1, 0),
		parquet.ValueOf("k2").Level(1, 1, 0),
		parquet.ValueOf("x").Level(0, 2, 1),
		parquet.ValueOf("y").Level(2, 2, 1),
		parquet.ValueOf("1").Level(0, 2, 2),
		parquet.ValueOf("2").Level(2, 2, 2),
	}

	var got Outer
	if err := schema.Reconstruct(&got, row); err != nil {
		t.Fatalf("Reconstruct with nested-Repeated elision returned error: %v", err)
	}

	if len(got.M) != 2 {
		t.Fatalf("expected 2 outer map entries, got %d: %#v", len(got.M), got.M)
	}

	// k1's Inner round-trips both entries; k2's Inner is empty after
	// reconstruction since the placeholder was elided.
	if e, ok := got.M["k1"]; !ok {
		t.Errorf("missing outer key k1")
	} else if want := map[string]string{"x": "1", "y": "2"}; !reflect.DeepEqual(e.Inner, want) {
		t.Errorf("k1.Inner = %#v, want %#v", e.Inner, want)
	}

	if e, ok := got.M["k2"]; !ok {
		t.Errorf("missing outer key k2")
	} else if len(e.Inner) != 0 {
		t.Errorf("k2.Inner expected empty, got %#v", e.Inner)
	}
}
