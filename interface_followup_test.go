package parquet

import (
	"bytes"
	"encoding/json"
	"testing"
)

func writtenRows[T any](t *testing.T, schema *Schema, rows []T) []Row {
	t.Helper()
	var buf bytes.Buffer
	w := NewGenericWriter[T](&buf, schema)
	if _, err := w.Write(rows); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return readAllRows(t, buf.Bytes())
}

func rowsEqual(got, want []Row) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range want {
		if !got[i].Equal(want[i]) {
			return false
		}
	}
	return true
}

// A non-nil pointer stored in an interface field is the slice, or a pointer
// to the slice. Deconstruct used to call Len on the pointer.
func TestDeconstructPointerInInterface(t *testing.T) {
	type model struct {
		X any `parquet:"x"`
	}

	t.Run("optional list", func(t *testing.T) {
		schema := NewSchema("row", Group{"x": Optional(List(Int(32)))})
		values := []int32{7, 0, 9}
		var nilSlice []int32
		inputs := []model{
			{X: &values},
			{X: &nilSlice},
			{X: values},
			{X: nilSlice},
			{X: []int32{}},
			{},
		}
		for _, in := range inputs {
			got := schema.Deconstruct(nil, in)
			want := writtenRows(t, schema, []model{in})
			if len(want) != 1 || !got.Equal(want[0]) {
				t.Fatalf("%#v:\ndeconstruct %+v\nwriter      %+v", in.X, got, want)
			}
		}
	})

	t.Run("list of groups", func(t *testing.T) {
		schema := NewSchema("row", Group{"x": Optional(List(Group{"a": Int(32)}))})
		values := []struct {
			A int32 `parquet:"a"`
		}{{A: 1}, {A: 2}}
		in := model{X: &values}
		got := schema.Deconstruct(nil, in)
		want := writtenRows(t, schema, []model{in})
		if len(want) != 1 || !got.Equal(want[0]) {
			t.Fatalf("deconstruct %+v\nwriter      %+v", got, want)
		}
	})

	t.Run("repeated", func(t *testing.T) {
		schema := NewSchema("row", Group{"x": Repeated(Int(32))})
		values := []int32{4, 5}
		in := model{X: &values}
		got := schema.Deconstruct(nil, in)
		want := writtenRows(t, schema, []model{in})
		if len(want) != 1 || !got.Equal(want[0]) {
			t.Fatalf("deconstruct %+v\nwriter      %+v", got, want)
		}
	})

	t.Run("optional int pointer", func(t *testing.T) {
		schema := NewSchema("row", Group{"x": Optional(Int(32))})
		n := int32(5)
		zero := int32(0)
		for _, in := range []model{{X: &n}, {X: &zero}, {X: (*int32)(nil)}} {
			got := schema.Deconstruct(nil, in)
			want := writtenRows(t, schema, []model{in})
			if len(want) != 1 || !got.Equal(want[0]) {
				t.Fatalf("%#v:\ndeconstruct %+v\nwriter      %+v", in.X, got, want)
			}
		}
	})
}

// Maps stored in an interface used to panic in Len. A nil map is null; an
// empty map is present and empty. A pointer to a nil map is that empty map.
func TestGenericWriterMapInInterface(t *testing.T) {
	schema := NewSchema("row", Group{"x": Optional(Map(String(), Int(32)))})
	type boxed struct {
		X any `parquet:"x"`
	}
	type concrete struct {
		X map[string]int32 `parquet:"x,optional"`
	}

	nilMap := map[string]int32(nil)
	empty := map[string]int32{}
	one := map[string]int32{"a": 1}
	oneAny := map[string]any{"a": int32(1)}

	cases := []struct {
		name     string
		value    any
		concrete map[string]int32
	}{
		{name: "nil", value: nilMap, concrete: nil},
		{name: "empty", value: empty, concrete: empty},
		{name: "one", value: one, concrete: one},
		{name: "any values", value: oneAny, concrete: one},
		{name: "pointer", value: &one, concrete: one},
		{name: "pointer to nil", value: &nilMap, concrete: empty},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := writtenRows(t, schema, []boxed{{X: tc.value}})
			want := writtenRows(t, schema, []concrete{{X: tc.concrete}})
			if !rowsEqual(got, want) {
				t.Fatalf("got %+v\nwant %+v", got, want)
			}
		})
	}
}

// json.RawMessage("null") in an any field is null. A JSON object written to
// an optional map is that map, not a panic.
func TestRawMessageNullAndMap(t *testing.T) {
	type boxed struct {
		X any `parquet:"x"`
	}
	type raw struct {
		X json.RawMessage `parquet:"x"`
	}

	t.Run("null int", func(t *testing.T) {
		schema := NewSchema("row", Group{"x": Optional(Int(32))})
		got := writtenRows(t, schema, []boxed{{X: json.RawMessage("null")}})
		want := writtenRows(t, schema, []boxed{{X: (*int32)(nil)}})
		if !rowsEqual(got, want) {
			t.Fatalf("null: got %+v\nwant %+v", got, want)
		}
		deconstructed := schema.Deconstruct(nil, boxed{X: json.RawMessage("null")})
		if len(want) != 1 || !deconstructed.Equal(want[0]) {
			t.Fatalf("deconstruct null: got %+v\nwant %+v", deconstructed, want)
		}
		n := int32(7)
		got = writtenRows(t, schema, []boxed{{X: json.RawMessage("7")}})
		want = writtenRows(t, schema, []boxed{{X: &n}})
		if !rowsEqual(got, want) {
			t.Fatalf("number: got %+v\nwant %+v", got, want)
		}
	})

	t.Run("null list", func(t *testing.T) {
		schema := NewSchema("row", Group{"x": Optional(List(Int(32)))})
		got := writtenRows(t, schema, []boxed{{X: json.RawMessage("null")}})
		want := writtenRows(t, schema, []boxed{{X: []int32(nil)}})
		if !rowsEqual(got, want) {
			t.Fatalf("null: got %+v\nwant %+v", got, want)
		}
		got = writtenRows(t, schema, []boxed{{X: json.RawMessage("[1,2]")}})
		want = writtenRows(t, schema, []boxed{{X: []int32{1, 2}}})
		if !rowsEqual(got, want) {
			t.Fatalf("array: got %+v\nwant %+v", got, want)
		}
	})

	t.Run("map", func(t *testing.T) {
		schema := NewSchema("row", Group{"x": Optional(Map(String(), Int(32)))})
		type concrete struct {
			X map[string]int32 `parquet:"x,optional"`
		}
		cases := []struct {
			name     string
			json     string
			concrete map[string]int32
		}{
			{name: "null", json: "null", concrete: nil},
			{name: "empty", json: "{}", concrete: map[string]int32{}},
			{name: "object", json: `{"a":1}`, concrete: map[string]int32{"a": 1}},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				msg := json.RawMessage(tc.json)
				want := writtenRows(t, schema, []concrete{{X: tc.concrete}})
				for _, got := range [][]Row{
					writtenRows(t, schema, []raw{{X: msg}}),
					writtenRows(t, schema, []boxed{{X: msg}}),
				} {
					if !rowsEqual(got, want) {
						t.Fatalf("got %+v\nwant %+v", got, want)
					}
				}
				if tc.name == "null" {
					got := schema.Deconstruct(nil, boxed{X: msg})
					if !got.Equal(want[0]) {
						t.Fatalf("deconstruct: got %+v\nwant %+v", got, want[0])
					}
				}
			})
		}
	})

	t.Run("optional map value", func(t *testing.T) {
		schema := NewSchema("row", Group{"x": Optional(Map(String(), Optional(Int(32))))})
		type concrete struct {
			X map[string]*int32 `parquet:"x,optional"`
		}
		n := int32(3)
		want := writtenRows(t, schema, []concrete{{X: map[string]*int32{"a": nil, "b": &n}}})
		got := writtenRows(t, schema, []boxed{{X: json.RawMessage(`{"a":null,"b":3}`)}})
		if !rowsEqual(got, want) {
			t.Fatalf("got %+v\nwant %+v", got, want)
		}
	})

	t.Run("json column", func(t *testing.T) {
		schema := NewSchema("row", Group{"x": Optional(JSON())})
		for _, msg := range []json.RawMessage{json.RawMessage("null"), json.RawMessage(`{"a":1}`)} {
			got := writtenRows(t, schema, []boxed{{X: msg}})
			want := writtenRows(t, schema, []raw{{X: msg}})
			if !rowsEqual(got, want) {
				t.Fatalf("%s: got %+v\nwant %+v", msg, got, want)
			}
		}
	})
}
