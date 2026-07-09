package parquet_test

import (
	"bytes"
	"fmt"
	"io"
	"math/rand/v2"
	"slices"
	"testing"
	"unsafe"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/variant"
)

// The tests in this file validate the streaming columnar write path
// (VariantColumnWriter) against the row-based write path: both write the
// same values through the same shredding schema, and both files must read
// back structurally equal to the originals. Schemas and values come from
// the same randomized generators as the row-path round-trip tests, so every
// case of the VariantShredding.md tables arises: exact matches, type
// mismatches, partially shredded objects, missing fields, empty containers,
// variant nulls, and nested combinations.

// buildVariantFileStreaming writes values through VariantColumnWriter into
// the same schema shape as buildVariantFile (an id column and an optional
// variant column), driving the id column through ColumnWriter.WriteRowValues
// the way an application writing column-by-column would. A nil value writes
// a null variant row.
func buildVariantFileStreaming(t *testing.T, shred parquet.Node, values []*variant.Value, options ...parquet.WriterOption) []byte {
	t.Helper()
	variantNode := parquet.Variant()
	if shred != nil {
		shredded, err := parquet.ShreddedVariant(shred)
		if err != nil {
			t.Fatalf("building shredded variant node: %v", err)
		}
		variantNode = shredded
	}
	schema := parquet.NewSchema("table", parquet.Group{
		"id":  parquet.Int(32),
		"var": parquet.Optional(variantNode),
	})
	buf := new(bytes.Buffer)
	opts := append([]parquet.WriterOption{schema}, options...)
	w := parquet.NewWriter(buf, opts...)

	vw, err := parquet.NewVariantColumnWriter(w, "var")
	if err != nil {
		t.Fatalf("NewVariantColumnWriter: %v", err)
	}
	idColumn := w.ColumnWriters()[0]
	idValue := make([]parquet.Value, 1)
	for i, v := range values {
		idValue[0] = parquet.Int32Value(int32(i)).Level(0, 0, 0)
		if _, err := idColumn.WriteRowValues(idValue); err != nil {
			t.Fatalf("writing id %d: %v", i, err)
		}
		if v == nil {
			err = vw.WriteNullRow()
		} else {
			err = vw.WriteValue(*v)
		}
		if err != nil {
			t.Fatalf("writing variant row %d: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("closing writer: %v", err)
	}
	return buf.Bytes()
}

// assertVariantFile reads a written file back through the columnar reader
// and asserts every row matches the expected values (nil = null row).
func assertVariantFile(t *testing.T, data []byte, values []*variant.Value, context string) {
	t.Helper()
	got, present := readVariantColumnar(t, data, 1024, "var")
	if len(got) != len(values) {
		t.Fatalf("%s: read %d rows, want %d", context, len(got), len(values))
	}
	for i, want := range values {
		if want == nil {
			if present[i] {
				t.Errorf("%s: row %d: got %#v, want null row", context, i, got[i].GoValue())
			}
			continue
		}
		if !present[i] {
			t.Errorf("%s: row %d: got null row, want %#v", context, i, want.GoValue())
			continue
		}
		if !got[i].Equal(*want) {
			t.Errorf("%s: row %d mismatch:\n got: %#v\nwant: %#v", context, i, got[i].GoValue(), want.GoValue())
		}
	}
}

// TestVariantColumnWriterRandomizedDifferential runs the randomized
// shredding schema × value matrix through both the streaming writer and the
// row-based writer, reading each file back through the columnar reader and
// the streaming file additionally through the row-based conversion read
// path. A shredding bug shared symmetrically by the streaming writer and
// the columnar reader cannot hide: the row-based writer and the conversion
// read path check both files independently.
func TestVariantColumnWriterRandomizedDifferential(t *testing.T) {
	const numSchemas, rowsPerSchema = 48, 11
	r := rand.New(rand.NewPCG(101, 202))
	for i := range numSchemas {
		t.Run(fmt.Sprintf("schema_%02d", i), func(t *testing.T) {
			shred := randomShredNode(r, 0)
			values := make([]*variant.Value, rowsPerSchema)
			for j := range values {
				if r.IntN(8) == 0 {
					continue
				}
				v := randomVariant(r, 0)
				values[j] = &v
			}

			data := buildVariantFileStreaming(t, shred, values)
			assertVariantFile(t, data, values, "streaming write")

			// The row-based writer must produce a file that reads back to
			// the same values through the same columnar reader.
			rowData := buildVariantFile(t, shred, values)
			assertVariantFile(t, rowData, values, "row-based write")

			// The row-based read path (schema conversion to an unshredded
			// variant) is an independent check on the streaming file. Null
			// rows read back as empty metadata/value bytes.
			rows, err := parquet.Read[rawVariantRow](bytes.NewReader(data), int64(len(data)))
			if err != nil {
				t.Fatalf("row-based read: %v", err)
			}
			if len(rows) != len(values) {
				t.Fatalf("row-based read: %d rows, want %d", len(rows), len(values))
			}
			for j, want := range values {
				if want == nil {
					if len(rows[j].Var.Metadata) != 0 || len(rows[j].Var.Value) != 0 {
						t.Errorf("row-based read: row %d: got %d metadata and %d value bytes, want empty null row",
							j, len(rows[j].Var.Metadata), len(rows[j].Var.Value))
					}
					continue
				}
				decoded, err := decodeRawVariant(rows[j].Var)
				if err != nil {
					t.Errorf("row-based read: row %d: %v", j, err)
					continue
				}
				if !decoded.Equal(*want) {
					t.Errorf("row-based read: row %d mismatch:\n got: %#v\nwant: %#v",
						j, decoded.GoValue(), want.GoValue())
				}
			}
		})
	}
}

// TestVariantColumnWriterUnshredded writes through a plain unshredded
// variant column (required binary metadata and value).
func TestVariantColumnWriterUnshredded(t *testing.T) {
	r := rand.New(rand.NewPCG(5, 6))
	values := make([]*variant.Value, 20)
	for i := range values {
		if i%7 == 3 {
			continue
		}
		v := randomVariant(r, 0)
		values[i] = &v
	}
	data := buildVariantFileStreaming(t, nil, values)
	assertVariantFile(t, data, values, "unshredded")

	rows, err := parquet.Read[rawVariantRow](bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("row-based read: %v", err)
	}
	for i, want := range values {
		if want == nil {
			if len(rows[i].Var.Metadata) != 0 || len(rows[i].Var.Value) != 0 {
				t.Errorf("row-based read: row %d: want empty null row", i)
			}
			continue
		}
		decoded, err := decodeRawVariant(rows[i].Var)
		if err != nil {
			t.Errorf("row-based read: row %d: %v", i, err)
			continue
		}
		if !decoded.Equal(*want) {
			t.Errorf("row-based read: row %d mismatch:\n got: %#v\nwant: %#v",
				i, decoded.GoValue(), want.GoValue())
		}
	}
}

// TestVariantColumnWriterErrors exercises the constructor error paths.
func TestVariantColumnWriterErrors(t *testing.T) {
	schema := parquet.NewSchema("table", parquet.Group{
		"id":   parquet.Int(32),
		"sub":  parquet.Group{"x": parquet.Int(64)},
		"list": parquet.Repeated(parquet.Group{"var": parquet.Optional(parquet.Variant())}),
		"var":  parquet.Optional(parquet.Variant()),
	})
	w := parquet.NewWriter(new(bytes.Buffer), schema)

	for _, test := range []struct {
		name string
		path []string
	}{
		{name: "no path", path: nil},
		{name: "missing column", path: []string{"nope"}},
		{name: "path through leaf", path: []string{"id", "x"}},
		{name: "leaf column", path: []string{"id"}},
		{name: "non-variant group", path: []string{"sub"}},
		{name: "beneath repeated field", path: []string{"list", "var"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := parquet.NewVariantColumnWriter(w, test.path...); err == nil {
				t.Fatalf("NewVariantColumnWriter(%q): want error, got nil", test.path)
			}
		})
	}

}

// TestVariantColumnWriterEvents drives the event API directly, without
// building variant.Value trees, the way a JSON transcoder would.
func TestVariantColumnWriterEvents(t *testing.T) {
	shred := parquet.Group{
		"name": parquet.String(),
		"age":  parquet.Int(64),
		"tags": parquet.List(parquet.String()),
	}
	schema := parquet.NewSchema("table", parquet.Group{
		"var": parquet.Optional(mustShreddedVariant(t, shred)),
	})
	buf := new(bytes.Buffer)
	w := parquet.NewWriter(buf, schema)
	vw, err := parquet.NewVariantColumnWriter(w, "var")
	if err != nil {
		t.Fatalf("NewVariantColumnWriter: %v", err)
	}

	// Row 0: fully shredded object with a residual field and a list.
	if err := vw.BeginRow(); err != nil {
		t.Fatal(err)
	}
	vw.BeginObject()
	vw.Field("name")
	vw.String("alice")
	vw.Field("extra") // not shredded: goes into the partial-object residual
	vw.BeginObject()
	vw.Field("x")
	vw.Int64(1)
	vw.EndObject()
	vw.Field("age")
	vw.Int64(30)
	vw.Field("tags")
	vw.BeginArray()
	vw.String("a")
	vw.String("b")
	vw.EndArray()
	vw.EndObject()
	if err := vw.EndRow(); err != nil {
		t.Fatal(err)
	}

	// Row 1: type conflicts everywhere: scalar at the object position.
	if err := vw.BeginRow(); err != nil {
		t.Fatal(err)
	}
	vw.Int32(42)
	if err := vw.EndRow(); err != nil {
		t.Fatal(err)
	}

	// Row 2: object with a type conflict in a field and a mixed list.
	if err := vw.BeginRow(); err != nil {
		t.Fatal(err)
	}
	vw.BeginObject()
	vw.Field("age")
	vw.String("not a number")
	vw.Field("tags")
	vw.BeginArray()
	vw.String("ok")
	vw.Int64(7) // conflicts with the string element type
	vw.Null()
	vw.EndArray()
	vw.EndObject()
	if err := vw.EndRow(); err != nil {
		t.Fatal(err)
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	want := []*variant.Value{
		ptrTo(variant.MakeObject([]variant.Field{
			{Name: "name", Value: variant.String("alice")},
			{Name: "extra", Value: variant.MakeObject([]variant.Field{{Name: "x", Value: variant.Int64(1)}})},
			{Name: "age", Value: variant.Int64(30)},
			{Name: "tags", Value: variant.MakeArray([]variant.Value{variant.String("a"), variant.String("b")})},
		})),
		ptrTo(variant.Int32(42)),
		ptrTo(variant.MakeObject([]variant.Field{
			{Name: "age", Value: variant.String("not a number")},
			{Name: "tags", Value: variant.MakeArray([]variant.Value{variant.String("ok"), variant.Int64(7), variant.Null()})},
		})),
	}
	assertVariantFile(t, buf.Bytes(), want, "event API")
}

// TestVariantColumnWriterFieldCopiesName verifies the documented reuse
// contract for Field: a caller may mutate the name's backing memory after
// the call returns without corrupting the row's metadata dictionary.
func TestVariantColumnWriterFieldCopiesName(t *testing.T) {
	shred := parquet.Group{"age": parquet.Int(64)}
	schema := parquet.NewSchema("table", parquet.Group{
		"var": parquet.Optional(mustShreddedVariant(t, shred)),
	})
	buf := new(bytes.Buffer)
	w := parquet.NewWriter(buf, schema)
	vw, err := parquet.NewVariantColumnWriter(w, "var")
	if err != nil {
		t.Fatal(err)
	}
	if err := vw.BeginRow(); err != nil {
		t.Fatal(err)
	}
	vw.BeginObject()
	scratch := []byte("extra")
	vw.Field(unsafe.String(unsafe.SliceData(scratch), len(scratch)))
	copy(scratch, "XXXXX")
	vw.String("residual")
	vw.Field("age")
	vw.Int64(1)
	vw.EndObject()
	if err := vw.EndRow(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	want := []*variant.Value{ptrTo(variant.MakeObject([]variant.Field{
		{Name: "extra", Value: variant.String("residual")},
		{Name: "age", Value: variant.Int64(1)},
	}))}
	assertVariantFile(t, buf.Bytes(), want, "field name copy")
}

func mustShreddedVariant(t *testing.T, shred parquet.Node) parquet.Node {
	t.Helper()
	node, err := parquet.ShreddedVariant(shred)
	if err != nil {
		t.Fatalf("ShreddedVariant: %v", err)
	}
	return node
}

// TestVariantColumnWriterDeepNesting shreds three container levels (objects
// inside a list inside an object) together with boolean and double typed
// leaves. The randomized generator stops at two container levels, and its
// value distribution does not reliably put a bool or double at a matching
// typed position, so these shapes need a deterministic fixture.
func TestVariantColumnWriterDeepNesting(t *testing.T) {
	shred := parquet.Group{
		"ok":    parquet.Leaf(parquet.BooleanType),
		"score": parquet.Leaf(parquet.DoubleType),
		"items": parquet.List(parquet.Group{"n": parquet.Int(64)}),
	}
	obj := func(fields ...variant.Field) variant.Value { return variant.MakeObject(fields) }
	field := func(name string, v variant.Value) variant.Field { return variant.Field{Name: name, Value: v} }
	arr := func(elems ...variant.Value) variant.Value { return variant.MakeArray(elems) }
	item := func(n int64) variant.Value { return obj(field("n", variant.Int64(n))) }

	values := []*variant.Value{
		// Everything matches: bool, double, and a list of typed objects.
		ptrTo(obj(
			field("ok", variant.Bool(true)),
			field("score", variant.Double(0.5)),
			field("items", arr(item(1), item(2))),
		)),
		// Conflicts at each level: int where bool is shredded, string where
		// double is shredded, and a list mixing typed objects, a scalar
		// conflict, and an object with a residual field.
		ptrTo(obj(
			field("ok", variant.Int64(1)),
			field("score", variant.String("high")),
			field("items", arr(
				item(3),
				variant.String("loose"),
				obj(field("n", variant.Int64(4)), field("extra", variant.Bool(false))),
			)),
		)),
		// Empty list and false/zero values (not Go zero-value confusables).
		ptrTo(obj(
			field("ok", variant.Bool(false)),
			field("score", variant.Double(0)),
			field("items", arr()),
		)),
	}
	data := buildVariantFileStreaming(t, shred, values)
	assertVariantFile(t, data, values, "deep nesting")

	// Independent check through the row-based conversion read path.
	rows, err := parquet.Read[rawVariantRow](bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("row-based read: %v", err)
	}
	for i, want := range values {
		decoded, err := decodeRawVariant(rows[i].Var)
		if err != nil {
			t.Fatalf("row-based read: row %d: %v", i, err)
		}
		if !decoded.Equal(*want) {
			t.Errorf("row-based read: row %d mismatch:\n got: %#v\nwant: %#v",
				i, decoded.GoValue(), want.GoValue())
		}
	}
}

// TestVariantColumnWriterMetadataInterned reads back the raw metadata
// column of a fully shredded row and asserts every object field name is in
// the row's dictionary. VariantShredding.md requires all field names,
// shredded or not, to be present in the metadata; no reconstruction oracle
// can catch a violation because readers recover shredded field names from
// the schema.
func TestVariantColumnWriterMetadataInterned(t *testing.T) {
	shred := parquet.Group{"name": parquet.String(), "age": parquet.Int(64)}
	values := []*variant.Value{
		ptrTo(variant.MakeObject([]variant.Field{
			{Name: "name", Value: variant.String("alice")},
			{Name: "age", Value: variant.Int64(30)},
			{Name: "extra", Value: variant.String("residual")}, // not shredded
		})),
	}
	data := buildVariantFileStreaming(t, shred, values)

	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	// The metadata column is the first leaf of the variant group; the id
	// column sorts before var, so it is leaf column 1 of the file.
	pages := f.RowGroups()[0].ColumnChunks()[1].Pages()
	defer pages.Close()
	p, err := pages.ReadPage()
	if err != nil {
		t.Fatal(err)
	}
	defer parquet.Release(p)
	vals := make([]parquet.Value, 1)
	if _, err := p.Values().ReadValues(vals); err != nil && err != io.EOF {
		t.Fatal(err)
	}
	m, err := variant.DecodeMetadata(vals[0].ByteArray())
	if err != nil {
		t.Fatalf("decoding written metadata: %v", err)
	}
	for _, name := range []string{"name", "age", "extra"} {
		if !slices.Contains(m.Strings, name) {
			t.Errorf("metadata dictionary %q is missing field name %q", m.Strings, name)
		}
	}
}

func ptrTo[T any](v T) *T { return &v }

// TestVariantColumnWriterMultiPage writes enough rows through a small page
// buffer that every column flushes multiple pages, verifying flushes land
// on row boundaries.
func TestVariantColumnWriterMultiPage(t *testing.T) {
	shred := parquet.Group{"a": parquet.Int(64), "b": parquet.String()}
	r := rand.New(rand.NewPCG(31, 41))
	values := make([]*variant.Value, 300)
	for i := range values {
		if i%13 == 5 {
			continue
		}
		v := randomVariant(r, 0)
		values[i] = &v
	}
	data := buildVariantFileStreaming(t, shred, values, parquet.PageBufferSize(256))
	assertVariantFile(t, data, values, "multi-page")
}

// TestVariantColumnWriterNestedEmptyList covers empty and conflicting
// elements inside nested shredded lists, where the empty-list null fill and
// element repetition levels interact.
func TestVariantColumnWriterNestedEmptyList(t *testing.T) {
	node := mustShreddedVariant(t, parquet.List(parquet.List(parquet.Int(64))))
	schema := parquet.NewSchema("table", parquet.Group{"var": parquet.Optional(node)})
	buf := new(bytes.Buffer)
	w := parquet.NewWriter(buf, schema)
	vw, err := parquet.NewVariantColumnWriter(w, "var")
	if err != nil {
		t.Fatal(err)
	}
	// Row 0: [[1, 2], [], "conflict"], row 1: [].
	if err := vw.BeginRow(); err != nil {
		t.Fatal(err)
	}
	vw.BeginArray()
	vw.BeginArray()
	vw.Int64(1)
	vw.Int64(2)
	vw.EndArray()
	vw.BeginArray()
	vw.EndArray()
	vw.String("conflict")
	vw.EndArray()
	if err := vw.EndRow(); err != nil {
		t.Fatal(err)
	}
	if err := vw.BeginRow(); err != nil {
		t.Fatal(err)
	}
	vw.BeginArray()
	vw.EndArray()
	if err := vw.EndRow(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	want := []*variant.Value{
		ptrTo(variant.MakeArray([]variant.Value{
			variant.MakeArray([]variant.Value{variant.Int64(1), variant.Int64(2)}),
			variant.MakeArray(nil),
			variant.String("conflict"),
		})),
		ptrTo(variant.MakeArray(nil)),
	}
	assertVariantFile(t, buf.Bytes(), want, "nested empty list")
}

// TestVariantColumnWriterMultipleRowGroups verifies the writer stays bound
// to its columns across row group flushes, which reset and swap the
// underlying column buffers.
func TestVariantColumnWriterMultipleRowGroups(t *testing.T) {
	shred := parquet.Group{"a": parquet.Int(64)}
	schema := parquet.NewSchema("table", parquet.Group{
		"var": parquet.Optional(mustShreddedVariant(t, shred)),
	})
	buf := new(bytes.Buffer)
	w := parquet.NewWriter(buf, schema)
	vw, err := parquet.NewVariantColumnWriter(w, "var")
	if err != nil {
		t.Fatalf("NewVariantColumnWriter: %v", err)
	}

	r := rand.New(rand.NewPCG(9, 9))
	values := make([]*variant.Value, 30)
	for i := range values {
		v := randomVariant(r, 0)
		values[i] = &v
		if err := vw.WriteValue(v); err != nil {
			t.Fatalf("writing row %d: %v", i, err)
		}
		if i == 9 || i == 19 {
			if err := w.Flush(); err != nil {
				t.Fatalf("flushing row group after row %d: %v", i, err)
			}
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("closing writer: %v", err)
	}

	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("opening file: %v", err)
	}
	if got := len(f.RowGroups()); got != 3 {
		t.Fatalf("row groups: got %d, want 3", got)
	}
	assertVariantFile(t, buf.Bytes(), values, "multiple row groups")
}

// TestVariantColumnWriterMisuse checks that malformed row and event
// sequences produce sticky errors.
func TestVariantColumnWriterMisuse(t *testing.T) {
	newWriter := func(t *testing.T, optional bool) *parquet.VariantColumnWriter {
		t.Helper()
		node := mustShreddedVariant(t, parquet.Group{"a": parquet.Int(64)})
		if optional {
			node = parquet.Optional(node)
		}
		schema := parquet.NewSchema("table", parquet.Group{"var": node})
		w := parquet.NewWriter(io.Discard, schema)
		vw, err := parquet.NewVariantColumnWriter(w, "var")
		if err != nil {
			t.Fatalf("NewVariantColumnWriter: %v", err)
		}
		return vw
	}

	t.Run("EndRow without BeginRow", func(t *testing.T) {
		vw := newWriter(t, true)
		if err := vw.EndRow(); err == nil {
			t.Fatal("expected an error")
		}
	})
	t.Run("BeginRow inside a row", func(t *testing.T) {
		vw := newWriter(t, true)
		if err := vw.BeginRow(); err != nil {
			t.Fatal(err)
		}
		if err := vw.BeginRow(); err == nil {
			t.Fatal("expected an error")
		}
	})
	t.Run("EndRow without a value", func(t *testing.T) {
		vw := newWriter(t, true)
		if err := vw.BeginRow(); err != nil {
			t.Fatal(err)
		}
		if err := vw.EndRow(); err == nil {
			t.Fatal("expected an error")
		}
	})
	t.Run("EndRow with unclosed container", func(t *testing.T) {
		vw := newWriter(t, true)
		if err := vw.BeginRow(); err != nil {
			t.Fatal(err)
		}
		vw.BeginObject()
		if err := vw.EndRow(); err == nil {
			t.Fatal("expected an error")
		}
	})
	t.Run("two values per row", func(t *testing.T) {
		vw := newWriter(t, true)
		if err := vw.BeginRow(); err != nil {
			t.Fatal(err)
		}
		vw.Int32(1)
		vw.Int32(2)
		if err := vw.EndRow(); err == nil {
			t.Fatal("expected an error")
		}
	})
	t.Run("value without Field", func(t *testing.T) {
		vw := newWriter(t, true)
		if err := vw.BeginRow(); err != nil {
			t.Fatal(err)
		}
		vw.BeginObject()
		vw.Int32(1)
		if err := vw.Err(); err == nil {
			t.Fatal("expected an error")
		}
	})
	t.Run("residual field without value", func(t *testing.T) {
		vw := newWriter(t, true)
		if err := vw.BeginRow(); err != nil {
			t.Fatal(err)
		}
		vw.BeginObject()
		vw.Field("resid") // not shredded: opens the partial-object residual
		vw.EndObject()
		if err := vw.EndRow(); err == nil {
			t.Fatal("expected an error")
		}
	})
	t.Run("duplicate residual field", func(t *testing.T) {
		vw := newWriter(t, true)
		if err := vw.BeginRow(); err != nil {
			t.Fatal(err)
		}
		vw.BeginObject()
		vw.Field("x")
		vw.Int64(1)
		vw.Field("x")
		vw.Int64(2)
		vw.EndObject()
		if err := vw.EndRow(); err == nil {
			t.Fatal("expected an error")
		}
	})
	t.Run("duplicate shredded field", func(t *testing.T) {
		vw := newWriter(t, true)
		if err := vw.BeginRow(); err != nil {
			t.Fatal(err)
		}
		vw.BeginObject()
		vw.Field("a")
		vw.Int64(1)
		vw.Field("a")
		vw.Int64(2)
		if err := vw.Err(); err == nil {
			t.Fatal("expected an error")
		}
	})
	t.Run("Field outside an object", func(t *testing.T) {
		vw := newWriter(t, true)
		if err := vw.BeginRow(); err != nil {
			t.Fatal(err)
		}
		vw.Field("a")
		if err := vw.Err(); err == nil {
			t.Fatal("expected an error")
		}
	})
	t.Run("shredded field without value", func(t *testing.T) {
		vw := newWriter(t, true)
		if err := vw.BeginRow(); err != nil {
			t.Fatal(err)
		}
		vw.BeginObject()
		vw.Field("a") // shredded field
		vw.Field("b") // previous field has no value
		if err := vw.Err(); err == nil {
			t.Fatal("expected an error")
		}
	})
	t.Run("EndObject without BeginObject", func(t *testing.T) {
		vw := newWriter(t, true)
		if err := vw.BeginRow(); err != nil {
			t.Fatal(err)
		}
		vw.EndObject()
		if err := vw.Err(); err == nil {
			t.Fatal("expected an error")
		}
	})
	t.Run("EndObject with unvalued shredded field", func(t *testing.T) {
		vw := newWriter(t, true)
		if err := vw.BeginRow(); err != nil {
			t.Fatal(err)
		}
		vw.BeginObject()
		vw.Field("a")
		vw.EndObject()
		if err := vw.Err(); err == nil {
			t.Fatal("expected an error")
		}
	})
	t.Run("EndArray without BeginArray", func(t *testing.T) {
		vw := newWriter(t, true)
		if err := vw.BeginRow(); err != nil {
			t.Fatal(err)
		}
		vw.EndArray()
		if err := vw.Err(); err == nil {
			t.Fatal("expected an error")
		}
	})
	t.Run("second top-level container", func(t *testing.T) {
		vw := newWriter(t, true)
		if err := vw.BeginRow(); err != nil {
			t.Fatal(err)
		}
		vw.Int32(1)
		vw.BeginObject()
		if err := vw.Err(); err == nil {
			t.Fatal("expected an error")
		}
	})
	t.Run("unexpected BeginArray", func(t *testing.T) {
		vw := newWriter(t, true)
		if err := vw.BeginRow(); err != nil {
			t.Fatal(err)
		}
		vw.Int32(1)
		vw.BeginArray()
		if err := vw.Err(); err == nil {
			t.Fatal("expected an error")
		}
	})
	t.Run("Field inside a list", func(t *testing.T) {
		schema := parquet.NewSchema("table", parquet.Group{
			"var": parquet.Optional(mustShreddedVariant(t, parquet.List(parquet.Int(64)))),
		})
		w := parquet.NewWriter(io.Discard, schema)
		vw, err := parquet.NewVariantColumnWriter(w, "var")
		if err != nil {
			t.Fatal(err)
		}
		if err := vw.BeginRow(); err != nil {
			t.Fatal(err)
		}
		vw.BeginArray()
		vw.Field("x")
		if err := vw.Err(); err == nil {
			t.Fatal("expected an error")
		}
	})
	t.Run("WriteNullRow inside a row", func(t *testing.T) {
		vw := newWriter(t, true)
		if err := vw.BeginRow(); err != nil {
			t.Fatal(err)
		}
		if err := vw.WriteNullRow(); err == nil {
			t.Fatal("expected an error")
		}
	})
	t.Run("WriteNullRow on required column", func(t *testing.T) {
		vw := newWriter(t, false)
		if err := vw.WriteNullRow(); err == nil {
			t.Fatal("expected an error")
		}
	})
	t.Run("errors are sticky", func(t *testing.T) {
		vw := newWriter(t, true)
		if err := vw.EndRow(); err == nil {
			t.Fatal("expected an error")
		}
		if err := vw.BeginRow(); err == nil {
			t.Fatal("expected BeginRow to report the sticky error")
		}
	})
}

// TestVariantColumnWriterAllocations checks that writing rows through a
// warmed-up streaming writer does not allocate per row.
func TestVariantColumnWriterAllocations(t *testing.T) {
	shred := parquet.Group{"a": parquet.Int(64), "b": parquet.String()}
	schema := parquet.NewSchema("table", parquet.Group{
		"var": parquet.Optional(mustShreddedVariant(t, shred)),
	})
	w := parquet.NewWriter(io.Discard, schema)
	vw, err := parquet.NewVariantColumnWriter(w, "var")
	if err != nil {
		t.Fatalf("NewVariantColumnWriter: %v", err)
	}

	writeRow := func() {
		if err := vw.BeginRow(); err != nil {
			t.Fatal(err)
		}
		vw.BeginObject()
		vw.Field("a")
		vw.Int64(42)
		vw.Field("b")
		vw.String("hello")
		vw.Field("c") // residual
		vw.Double(1.5)
		vw.EndObject()
		if err := vw.EndRow(); err != nil {
			t.Fatal(err)
		}
	}
	// Warm up buffers (and column buffer growth).
	for range 1000 {
		writeRow()
	}
	allocs := testing.AllocsPerRun(100, writeRow)
	// Column buffers grow amortized; a strict zero is flaky, but per-row
	// work must not allocate proportionally.
	if allocs > 3 {
		t.Errorf("allocations per row: %v, want <= 3", allocs)
	}
}

// BenchmarkVariantColumnWriter compares writing a shredded variant column
// through the streaming event API against the row-based write path
// (GenericWriter.Write of raw variant rows).
func BenchmarkVariantColumnWriter(b *testing.B) {
	shred := parquet.Group{
		"name": parquet.String(),
		"age":  parquet.Int(64),
		"tags": parquet.List(parquet.String()),
	}
	shredded, err := parquet.ShreddedVariant(shred)
	if err != nil {
		b.Fatalf("ShreddedVariant: %v", err)
	}
	schema := parquet.NewSchema("table", parquet.Group{
		"var": parquet.Optional(shredded),
	})

	value := variant.MakeObject([]variant.Field{
		{Name: "name", Value: variant.String("alice")},
		{Name: "age", Value: variant.Int64(30)},
		{Name: "tags", Value: variant.MakeArray([]variant.Value{
			variant.String("x"), variant.String("y"), variant.String("z"),
		})},
		{Name: "extra", Value: variant.Double(1.5)},
	})
	const rowsPerIter = 1000

	b.Run("streaming", func(b *testing.B) {
		w := parquet.NewWriter(io.Discard, schema)
		vw, err := parquet.NewVariantColumnWriter(w, "var")
		if err != nil {
			b.Fatalf("NewVariantColumnWriter: %v", err)
		}
		b.ReportAllocs()
		for b.Loop() {
			for range rowsPerIter {
				if err := vw.BeginRow(); err != nil {
					b.Fatal(err)
				}
				vw.BeginObject()
				vw.Field("name")
				vw.String("alice")
				vw.Field("age")
				vw.Int64(30)
				vw.Field("tags")
				vw.BeginArray()
				vw.String("x")
				vw.String("y")
				vw.String("z")
				vw.EndArray()
				vw.Field("extra")
				vw.Double(1.5)
				vw.EndObject()
				if err := vw.EndRow(); err != nil {
					b.Fatal(err)
				}
			}
		}
	})

	b.Run("rows", func(b *testing.B) {
		type row struct {
			Var rawVariant `parquet:"var,optional,variant"`
		}
		raw := encodeRawVariant(value)
		rows := make([]row, rowsPerIter)
		for i := range rows {
			rows[i] = row{Var: raw}
		}
		w := parquet.NewGenericWriter[row](io.Discard, schema)
		b.ReportAllocs()
		for b.Loop() {
			if _, err := w.Write(rows); err != nil {
				b.Fatal(err)
			}
		}
	})

	// Wide objects exercise the per-event field lookup: with a linear scan
	// this case is quadratic in the field count and dominated field
	// resolution from a few dozen fields up.
	b.Run("streaming-wide", func(b *testing.B) {
		const numFields = 100
		names := make([]string, numFields)
		wideShred := parquet.Group{}
		for j := range numFields {
			names[j] = fmt.Sprintf("f%03d", j)
			wideShred[names[j]] = parquet.Int(64)
		}
		shredded, err := parquet.ShreddedVariant(wideShred)
		if err != nil {
			b.Fatalf("ShreddedVariant: %v", err)
		}
		schema := parquet.NewSchema("table", parquet.Group{
			"var": parquet.Optional(shredded),
		})
		w := parquet.NewWriter(io.Discard, schema)
		vw, err := parquet.NewVariantColumnWriter(w, "var")
		if err != nil {
			b.Fatalf("NewVariantColumnWriter: %v", err)
		}
		b.ReportAllocs()
		for b.Loop() {
			for i := range rowsPerIter {
				if err := vw.BeginRow(); err != nil {
					b.Fatal(err)
				}
				vw.BeginObject()
				for j := range numFields {
					vw.Field(names[j])
					vw.Int64(int64(i + j))
				}
				vw.EndObject()
				if err := vw.EndRow(); err != nil {
					b.Fatal(err)
				}
			}
		}
	})
}
