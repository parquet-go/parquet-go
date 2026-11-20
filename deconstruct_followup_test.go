package parquet

import (
	"bytes"
	"io"
	"reflect"
	"testing"

	"github.com/google/uuid"
)

func TestDeconstructRepeatedOptionalPointers(t *testing.T) {
	seven := int32(7)
	zero := int32(0)

	t.Run("int32", func(t *testing.T) {
		type model struct {
			X []*int32 `parquet:"x"`
		}
		schema := SchemaOf(new(model))
		got := schema.Deconstruct(nil, model{X: []*int32{&seven, nil, &zero}})
		// A repeated int32 column has one definition level. Nil and a pointer
		// to zero are both stored as 0, matching GenericWriter.
		want := Row{
			Int32Value(7).Level(0, 1, 0),
			Int32Value(0).Level(1, 1, 0),
			Int32Value(0).Level(1, 1, 0),
		}
		if !got.Equal(want) {
			t.Fatalf("got %+v\nwant %+v", got, want)
		}

		for _, in := range []model{{}, {X: []*int32{}}} {
			got = schema.Deconstruct(nil, in)
			want = Row{NullValue().Level(0, 0, 0)}
			if !got.Equal(want) {
				t.Fatalf("%#v: got %+v, want %+v", in.X, got, want)
			}
		}
	})

	t.Run("plain slice still keeps zeros", func(t *testing.T) {
		type model struct {
			X []int32 `parquet:"x"`
		}
		schema := SchemaOf(new(model))
		got := schema.Deconstruct(nil, model{X: []int32{7, 0, 9}})
		want := Row{
			Int32Value(7).Level(0, 1, 0),
			Int32Value(0).Level(1, 1, 0),
			Int32Value(9).Level(1, 1, 0),
		}
		if !got.Equal(want) {
			t.Fatalf("got %+v\nwant %+v", got, want)
		}
	})

}

func TestDeconstructRepeatedOptionalStructAndString(t *testing.T) {
	type inner struct {
		V int32 `parquet:"v"`
	}
	type structs struct {
		Key []*inner `parquet:"key"`
	}
	a := inner{V: 5}
	schema := SchemaOf(new(structs))
	got := schema.Deconstruct(nil, structs{Key: []*inner{nil, &a}})
	want := Row{
		Int32Value(0).Level(0, 1, 0),
		Int32Value(5).Level(1, 1, 0),
	}
	if !got.Equal(want) {
		t.Fatalf("struct: got %+v\nwant %+v", got, want)
	}

	type strings struct {
		Key []*string `parquet:"key"`
	}
	hi := "hi"
	schema = SchemaOf(new(strings))
	got = schema.Deconstruct(nil, strings{Key: []*string{nil, &hi}})
	want = Row{
		ByteArrayValue([]byte("")).Level(0, 1, 0),
		ByteArrayValue([]byte("hi")).Level(1, 1, 0),
	}
	if !got.Equal(want) {
		t.Fatalf("string: got %+v\nwant %+v", got, want)
	}
}

func TestDeconstructRepeatedUUIDNilAndZero(t *testing.T) {
	id := uuid.MustParse("123456ab-0000-0000-0000-000000000000")
	type model struct {
		IDs []*uuid.UUID
	}
	zero := uuid.Nil
	rows := []model{
		{IDs: []*uuid.UUID{&id, nil, &zero}},
		{IDs: []*uuid.UUID{nil, nil}},
		{IDs: nil},
		{IDs: []*uuid.UUID{}},
	}
	schema := SchemaOf(new(model))

	var generic, plain bytes.Buffer
	gw := NewGenericWriter[model](&generic)
	if _, err := gw.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := gw.Close(); err != nil {
		t.Fatal(err)
	}
	w := NewWriter(&plain, schema)
	for i := range rows {
		if err := w.Write(&rows[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	got := readAllRows(t, plain.Bytes())
	want := readAllRows(t, generic.Bytes())
	if len(got) != len(want) {
		t.Fatalf("rows: got %d, want %d", len(got), len(want))
	}
	for i := range want {
		if !got[i].Equal(want[i]) {
			t.Fatalf("row %d:\ngot  %+v\nwant %+v", i, got[i], want[i])
		}
	}
	// Nil is not the zero UUID: the second value of the first row is null,
	// and the third is a present zero.
	if len(got[0]) != 3 || !got[0][1].IsNull() || got[0][2].IsNull() {
		t.Fatalf("nil and zero UUID were not distinguished: %+v", got[0])
	}
}

func TestDeconstructOptionalListPointerNilAndZero(t *testing.T) {
	seven := int32(7)
	zero := int32(0)
	type model struct {
		X []*int32 `parquet:"x,list"`
	}
	schema := SchemaOf(new(model))
	got := schema.Deconstruct(nil, model{X: []*int32{&seven, nil, &zero}})
	want := Row{
		Int32Value(7).Level(0, 2, 0),
		NullValue().Level(1, 1, 0),
		Int32Value(0).Level(1, 2, 0),
	}
	if !got.Equal(want) {
		t.Fatalf("got %+v\nwant %+v", got, want)
	}

	var out model
	if err := schema.Reconstruct(&out, got); err != nil {
		t.Fatal(err)
	}
	if len(out.X) != 3 || out.X[0] == nil || *out.X[0] != 7 || out.X[1] != nil || out.X[2] == nil || *out.X[2] != 0 {
		t.Fatalf("reconstructed %#v", formatInt32Ptrs(out.X))
	}
}

func formatInt32Ptrs(vs []*int32) []any {
	out := make([]any, len(vs))
	for i, v := range vs {
		if v == nil {
			out[i] = nil
			continue
		}
		out[i] = *v
	}
	return out
}

func TestOptionalListElementsRoundTrip(t *testing.T) {
	type model struct {
		X []int32 `parquet:"x,list" parquet-element:",optional"`
	}
	input := model{X: []int32{7, 0, 9}}
	schema := SchemaOf(new(model))

	check := func(t *testing.T, got model) {
		t.Helper()
		if !reflect.DeepEqual(got, input) {
			t.Fatalf("got %#v, want %#v", got, input)
		}
	}

	t.Run("Writer", func(t *testing.T) {
		var buf bytes.Buffer
		w := NewWriter(&buf, schema)
		if err := w.Write(&input); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		rows, err := Read[model](bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) != 1 {
			t.Fatalf("read %d rows", len(rows))
		}
		check(t, rows[0])
	})

	t.Run("Buffer", func(t *testing.T) {
		buf := NewBuffer(schema)
		if err := buf.Write(&input); err != nil {
			t.Fatal(err)
		}
		check(t, reconstructModel[model](t, schema, buf.Rows()))
	})

	t.Run("RowBuffer", func(t *testing.T) {
		buf := NewRowBuffer[model]()
		if _, err := buf.Write([]model{input}); err != nil {
			t.Fatal(err)
		}
		check(t, reconstructModel[model](t, schema, buf.Rows()))
	})
}

func reconstructModel[T any](t *testing.T, schema *Schema, rows Rows) T {
	t.Helper()
	scratch := make([]Row, 1)
	n, err := rows.ReadRows(scratch)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("read %d rows", n)
	}
	var got T
	if err := schema.Reconstruct(&got, scratch[0]); err != nil {
		t.Fatal(err)
	}
	return got
}

func TestDeconstructListOfRepeated(t *testing.T) {
	schema := NewSchema("row", Group{"x": List(Repeated(Int(32)))})

	t.Run("slice of slices", func(t *testing.T) {
		got := schema.Deconstruct(nil, map[string]any{"x": [][]int32{{7, 0}, {9}}})
		want := Row{
			Int32Value(7).Level(0, 2, 0),
			Int32Value(0).Level(2, 2, 0),
			Int32Value(9).Level(1, 2, 0),
		}
		if !got.Equal(want) {
			t.Fatalf("got %+v\nwant %+v", got, want)
		}
	})

	t.Run("slice of int32", func(t *testing.T) {
		got := schema.Deconstruct(nil, map[string]any{"x": []int32{7, 0, 9}})
		want := Row{
			Int32Value(7).Level(0, 2, 0),
			Int32Value(0).Level(1, 2, 0),
			Int32Value(9).Level(1, 2, 0),
		}
		if !got.Equal(want) {
			t.Fatalf("got %+v\nwant %+v", got, want)
		}
	})

	t.Run("generic writer", func(t *testing.T) {
		type model struct {
			X any `parquet:"x"`
		}
		for _, value := range []any{
			[][]int32{{7, 0}, {9}},
			[]int32{7, 0, 9},
		} {
			want := schema.Deconstruct(nil, map[string]any{"x": value})
			var buf bytes.Buffer
			w := NewGenericWriter[model](&buf, schema)
			if _, err := w.Write([]model{{X: value}}); err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			got := readAllRows(t, buf.Bytes())
			if len(got) != 1 || !got[0].Equal(want) {
				t.Fatalf("%T: got %+v, want %+v", value, got, want)
			}
		}
	})

	t.Run("root slice", func(t *testing.T) {
		schema := NewSchema("x", List(Repeated(Int(32))))
		got := schema.Deconstruct(nil, [][]int32{{1, 2}, nil, {3}})
		want := Row{
			Int32Value(1).Level(0, 2, 0),
			Int32Value(2).Level(2, 2, 0),
			NullValue().Level(1, 1, 0),
			Int32Value(3).Level(1, 2, 0),
		}
		if !got.Equal(want) {
			t.Fatalf("got %+v\nwant %+v", got, want)
		}
	})
}

func TestDeconstructOptionalListNilSliceInInterface(t *testing.T) {
	schema := NewSchema("row", Group{"x": Optional(List(Int(32)))})
	var nilSlice []int32

	got := schema.Deconstruct(nil, map[string]any{"x": nilSlice})
	want := Row{NullValue().Level(0, 0, 0)}
	if !got.Equal(want) {
		t.Fatalf("nil slice: got %+v, want %+v", got, want)
	}

	got = schema.Deconstruct(nil, map[string]any{"x": []int32{}})
	want = Row{NullValue().Level(0, 1, 0)}
	if !got.Equal(want) {
		t.Fatalf("empty slice: got %+v, want %+v", got, want)
	}

	got = schema.Deconstruct(nil, map[string]any{"x": []int32{1}})
	want = Row{Int32Value(1).Level(0, 2, 0)}
	if !got.Equal(want) {
		t.Fatalf("values: got %+v, want %+v", got, want)
	}

	type model struct {
		X any `parquet:"x"`
	}
	var buf bytes.Buffer
	w := NewGenericWriter[model](&buf, schema)
	if _, err := w.Write([]model{{X: nilSlice}, {X: []int32{}}, {X: []int32{1}}, {}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	rows := readAllRows(t, buf.Bytes())
	wantRows := []Row{
		{NullValue().Level(0, 0, 0)},
		{NullValue().Level(0, 1, 0)},
		{Int32Value(1).Level(0, 2, 0)},
		{NullValue().Level(0, 0, 0)},
	}
	if len(rows) != len(wantRows) {
		t.Fatalf("generic rows: got %d, want %d", len(rows), len(wantRows))
	}
	for i := range wantRows {
		if !rows[i].Equal(wantRows[i]) {
			t.Fatalf("generic row %d: got %+v, want %+v", i, rows[i], wantRows[i])
		}
	}
}

func readAllRows(t *testing.T, data []byte) []Row {
	t.Helper()
	r := NewReader(bytes.NewReader(data))
	defer r.Close()
	scratch := make([]Row, 1)
	var rows []Row
	for {
		n, err := r.ReadRows(scratch)
		for i := range n {
			rows = append(rows, scratch[i].Clone())
		}
		if err == io.EOF {
			return rows
		}
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			t.Fatal("read returned no rows and no error")
		}
	}
}
