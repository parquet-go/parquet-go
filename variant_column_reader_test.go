package parquet_test

import (
	"bytes"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/variant"
	"slices"
)

// This file tests the columnar variant reader (variant_column_reader.go).
//
// The main test is differential: a generic reconstruction routine rebuilds
// every row's variant.Value purely from the cursor API (Locs, typed
// vectors, ListOffsets, Residual) and the result must be structurally equal
// to what the row-based read path produces. The routine only uses public
// API and no knowledge of the shredding schema beyond what cursors expose,
// so it doubles as a completeness check: if any value can't be rebuilt
// from cursors, the API is missing something.

// materializeVariantCursors creates cursors for every shredded position so
// the reader projects all columns.
func materializeVariantCursors(c *parquet.VariantCursor) {
	switch c.Kind() {
	case parquet.VariantCursorObject:
		for _, name := range c.Fields() {
			materializeVariantCursors(c.Field(name))
		}
	case parquet.VariantCursorList:
		materializeVariantCursors(c.Elements())
	}
}

// variantTypedIndexes inverts TypedRows: entry index -> dense index in the
// typed vectors, or -1.
func variantTypedIndexes(c *parquet.VariantCursor) []int32 {
	idx := make([]int32, len(c.Locs()))
	for i := range idx {
		idx[i] = -1
	}
	for d, e := range c.TypedRows() {
		idx[e] = int32(d)
	}
	return idx
}

// variantTypedValue converts the d-th dense typed value of a leaf cursor to
// a variant value, mirroring the shredded types table using only the public
// cursor accessors and the leaf's logical type.
func variantTypedValue(c *parquet.VariantCursor, d int) (variant.Value, error) {
	typ := c.LeafType()
	lt := typ.LogicalType()
	switch {
	case lt == nil:
		switch typ.Kind() {
		case parquet.Boolean:
			return variant.Bool(c.Booleans()[d]), nil
		case parquet.Int32:
			return variant.Int32(c.Int32s()[d]), nil
		case parquet.Int64:
			return variant.Int64(c.Int64s()[d]), nil
		case parquet.Float:
			return variant.Float(c.Floats()[d]), nil
		case parquet.Double:
			return variant.Double(c.Doubles()[d]), nil
		case parquet.ByteArray:
			slab, offsets := c.ByteArrays()
			return variant.Binary(slab[offsets[d]:offsets[d+1]]), nil
		}
	case lt.UTF8 != nil:
		slab, offsets := c.ByteArrays()
		return variant.String(string(slab[offsets[d]:offsets[d+1]])), nil
	case lt.Integer != nil:
		switch lt.Integer.BitWidth {
		case 8:
			return variant.Int8(int8(c.Int32s()[d])), nil
		case 16:
			return variant.Int16(int16(c.Int32s()[d])), nil
		case 32:
			return variant.Int32(c.Int32s()[d]), nil
		case 64:
			return variant.Int64(c.Int64s()[d]), nil
		}
	case lt.Date != nil:
		return variant.Date(c.Int32s()[d]), nil
	case lt.Time != nil:
		return variant.Time(c.Int64s()[d]), nil
	case lt.Timestamp != nil:
		v := c.Int64s()[d]
		switch {
		case lt.Timestamp.Unit.Micros != nil:
			if lt.Timestamp.IsAdjustedToUTC {
				return variant.Timestamp(v), nil
			}
			return variant.TimestampNTZ(v), nil
		case lt.Timestamp.Unit.Nanos != nil:
			if lt.Timestamp.IsAdjustedToUTC {
				return variant.TimestampNanos(v), nil
			}
			return variant.TimestampNTZNanos(v), nil
		}
	case lt.Decimal != nil:
		scale := byte(lt.Decimal.Scale)
		switch typ.Kind() {
		case parquet.Int32:
			return variant.Decimal4(c.Int32s()[d], scale), nil
		case parquet.Int64:
			return variant.Decimal8(c.Int64s()[d], scale), nil
		case parquet.FixedLenByteArray:
			slab, size := c.FixedLenByteArrays()
			return variant.Decimal16(bigEndianDecimal16(slab[d*size:(d+1)*size]), scale), nil
		case parquet.ByteArray:
			slab, offsets := c.ByteArrays()
			return variant.Decimal16(bigEndianDecimal16(slab[offsets[d]:offsets[d+1]]), scale), nil
		}
	case lt.UUID != nil:
		slab, size := c.FixedLenByteArrays()
		return variant.UUID(uuid.UUID(slab[d*size : (d+1)*size])), nil
	}
	return variant.Null(), fmt.Errorf("unsupported leaf type %s", typ)
}

// bigEndianDecimal16 converts a big-endian two's complement decimal of up
// to 16 bytes to the little-endian representation of variant decimal16.
func bigEndianDecimal16(b []byte) [16]byte {
	var out [16]byte
	for i := range b {
		out[i] = b[len(b)-1-i]
	}
	if len(b) > 0 && b[0]&0x80 != 0 {
		for i := len(b); i < 16; i++ {
			out[i] = 0xFF
		}
	}
	return out
}

// reconstructVariantEntry rebuilds the variant value of entry e of a cursor
// from the columnar API. The boolean result reports presence (false means
// the value is missing, e.g. an absent object field).
func reconstructVariantEntry(c *parquet.VariantCursor, e int, typedIdx map[*parquet.VariantCursor][]int32) (variant.Value, bool, error) {
	switch c.Locs()[e] {
	case parquet.VariantLocMissing:
		return variant.Null(), false, nil
	case parquet.VariantLocNull:
		return variant.Null(), true, nil
	case parquet.VariantLocResidual:
		v, ok, err := c.Residual(e)
		if err != nil {
			return variant.Null(), false, err
		}
		if !ok {
			return variant.Null(), false, fmt.Errorf("residual entry %d has no residual value", e)
		}
		return v, true, nil
	case parquet.VariantLocTyped:
		d := typedIdx[c][e]
		if d < 0 {
			return variant.Null(), false, fmt.Errorf("typed entry %d not in TypedRows", e)
		}
		v, err := variantTypedValue(c, int(d))
		return v, true, err
	case parquet.VariantLocTypedObject:
		shredded := c.Fields()
		var fields []variant.Field
		for _, name := range shredded {
			fv, present, err := reconstructVariantEntry(c.Field(name), e, typedIdx)
			if err != nil {
				return variant.Null(), false, err
			}
			if present {
				fields = append(fields, variant.Field{Name: name, Value: fv})
			}
		}
		if r, ok, err := c.Residual(e); err != nil {
			return variant.Null(), false, err
		} else if ok {
			if r.Basic() != variant.BasicObject {
				return variant.Null(), false, fmt.Errorf("partial object residual is not an object")
			}
			isShredded := func(name string) bool {
				return slices.Contains(shredded, name)
			}
			for _, f := range r.ObjectValue().Fields {
				if !isShredded(f.Name) {
					fields = append(fields, f)
				}
			}
		}
		return variant.MakeObject(fields), true, nil
	case parquet.VariantLocTypedList:
		offsets := c.ListOffsets()
		el := c.Elements()
		elems := []variant.Value{}
		for i := offsets[e]; i < offsets[e+1]; i++ {
			ev, present, err := reconstructVariantEntry(el, int(i), typedIdx)
			if err != nil {
				return variant.Null(), false, err
			}
			if !present {
				ev = variant.Null()
			}
			elems = append(elems, ev)
		}
		return variant.MakeArray(elems), true, nil
	}
	return variant.Null(), false, fmt.Errorf("unknown loc %v", c.Locs()[e])
}

// fillTypedIndexes populates the entry->dense mapping of every materialized
// cursor for the current window.
func fillTypedIndexes(c *parquet.VariantCursor, into map[*parquet.VariantCursor][]int32) {
	into[c] = variantTypedIndexes(c)
	switch c.Kind() {
	case parquet.VariantCursorObject:
		for _, name := range c.Fields() {
			fillTypedIndexes(c.Field(name), into)
		}
	case parquet.VariantCursorList:
		fillTypedIndexes(c.Elements(), into)
	}
}

// readVariantColumnar reads every row of the file's variant column through
// the columnar reader, reconstructing each row's value from cursors.
// Windows of the given size exercise page/window boundary handling. The
// boolean per row reports presence (false = null variant row).
func readVariantColumnar(t *testing.T, data []byte, window int, path ...string) ([]variant.Value, []bool) {
	t.Helper()
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("opening file: %v", err)
	}
	var values []variant.Value
	var present []bool
	for _, rg := range f.RowGroups() {
		r, err := parquet.NewVariantReader(rg, path...)
		if err != nil {
			t.Fatalf("NewVariantReader: %v", err)
		}
		root := r.Root()
		materializeVariantCursors(root)
		typedIdx := make(map[*parquet.VariantCursor][]int32)
		for {
			n, err := r.Next(window)
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("Next: %v", err)
			}
			fillTypedIndexes(root, typedIdx)
			for e := range n {
				v, ok, err := reconstructVariantEntry(root, e, typedIdx)
				if err != nil {
					t.Fatalf("reconstructing row %d: %v", len(values), err)
				}
				values = append(values, v)
				present = append(present, ok)
			}
		}
		if err := r.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}
	return values, present
}

// TestVariantReaderRandomizedDifferential runs the randomized shredding
// schema × value matrix through the columnar reader and asserts the
// reconstruction equals the original values, across several window sizes.
func TestVariantReaderRandomizedDifferential(t *testing.T) {
	const numSchemas, rowsPerSchema = 48, 11
	r := rand.New(rand.NewPCG(7, 11))
	for i := range numSchemas {
		t.Run(fmt.Sprintf("schema_%02d", i), func(t *testing.T) {
			shred := randomShredNode(r, 0)
			values := make([]variant.Value, rowsPerSchema)
			for j := range values {
				values[j] = randomVariant(r, 0)
			}

			shredded, err := parquet.ShreddedVariant(shred)
			if err != nil {
				t.Fatalf("building shredded variant node: %v", err)
			}
			schema := parquet.NewSchema("table", parquet.Group{
				"id":  parquet.Int(32),
				"var": shredded,
			})
			rows := make([]shreddedVariantRow, len(values))
			for j, v := range values {
				rows[j] = shreddedVariantRow{ID: int32(j), Var: encodeRawVariant(v)}
			}
			buf := new(bytes.Buffer)
			w := parquet.NewGenericWriter[shreddedVariantRow](buf, schema)
			if _, err := w.Write(rows); err != nil {
				t.Fatalf("writing rows: %v", err)
			}
			if err := w.Close(); err != nil {
				t.Fatalf("closing writer: %v", err)
			}

			for _, window := range []int{1, 3, 1000} {
				got, present := readVariantColumnar(t, buf.Bytes(), window, "var")
				if len(got) != len(values) {
					t.Fatalf("window %d: read %d rows, want %d", window, len(got), len(values))
				}
				for j, want := range values {
					if !present[j] {
						t.Errorf("window %d, row %d: reported missing, want %#v", window, j, want.GoValue())
						continue
					}
					if !got[j].Equal(want) {
						t.Errorf("schema:\n%s\nwindow %d, row %d mismatch:\n got: %#v\nwant: %#v",
							schema, window, j, got[j].GoValue(), want.GoValue())
					}
				}
			}
		})
	}
}

// TestVariantReaderUnshredded reads a plain (metadata, value) variant
// column through the columnar reader.
func TestVariantReaderUnshredded(t *testing.T) {
	r := rand.New(rand.NewPCG(21, 42))
	values := make([]variant.Value, 17)
	for j := range values {
		values[j] = randomVariant(r, 0)
	}
	schema := parquet.NewSchema("table", parquet.Group{
		"id":  parquet.Int(32),
		"var": parquet.Variant(),
	})
	rows := make([]shreddedVariantRow, len(values))
	for j, v := range values {
		rows[j] = shreddedVariantRow{ID: int32(j), Var: encodeRawVariant(v)}
	}
	buf := new(bytes.Buffer)
	w := parquet.NewGenericWriter[shreddedVariantRow](buf, schema)
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	got, present := readVariantColumnar(t, buf.Bytes(), 5, "var")
	for j, want := range values {
		if !present[j] {
			t.Fatalf("row %d reported missing", j)
		}
		if !got[j].Equal(want) {
			t.Errorf("row %d mismatch:\n got: %#v\nwant: %#v", j, got[j].GoValue(), want.GoValue())
		}
	}
}

// TestVariantReaderSpecFiles reads the canonical parquet-java-written
// shredded variant files through the columnar reader and compares against
// the same goldens as the row-based reader.
func TestVariantReaderSpecFiles(t *testing.T) {
	cases := []struct {
		file    string
		goldens []string
	}{
		{file: "case-004.parquet", goldens: []string{"case-004_row-0.variant.bin"}},
		{file: "case-045.parquet", goldens: []string{
			"case-045_row-0.variant.bin",
			"case-045_row-1.variant.bin",
			"case-045_row-2.variant.bin",
			"case-045_row-3.variant.bin",
		}},
		{file: "case-134.parquet", goldens: []string{"case-134_row-0.variant.bin"}},
	}
	for _, c := range cases {
		t.Run(c.file, func(t *testing.T) {
			path := filepath.Join("testdata", "shredded_variant", c.file)
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			for _, window := range []int{1, 100} {
				got, present := readVariantColumnar(t, data, window, "var")
				if len(got) != len(c.goldens) {
					t.Fatalf("read %d rows, want %d", len(got), len(c.goldens))
				}
				for i, golden := range c.goldens {
					want := decodeVariantGolden(t, filepath.Join("testdata", "shredded_variant", golden))
					if !present[i] {
						t.Fatalf("row %d reported missing", i)
					}
					if !got[i].Equal(want) {
						t.Errorf("window %d, row %d mismatch:\n got: %#v\nwant: %#v", window, i, got[i].GoValue(), want.GoValue())
					}
				}
			}
		})
	}
}

// buildVariantFile writes the given variant values (nil means a null
// variant row) into a file with the given typed_value shredding schema
// (nil means unshredded), returning the file bytes.
func buildVariantFile(t *testing.T, shred parquet.Node, values []*variant.Value, options ...parquet.WriterOption) []byte {
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
	rows := make([]shreddedVariantRow, len(values))
	for i, v := range values {
		rows[i] = shreddedVariantRow{ID: int32(i)}
		if v != nil {
			rows[i].Var = encodeRawVariant(*v)
		}
	}
	buf := new(bytes.Buffer)
	opts := append([]parquet.WriterOption{schema}, options...)
	w := parquet.NewGenericWriter[shreddedVariantRow](buf, opts...)
	if _, err := w.Write(rows); err != nil {
		t.Fatalf("writing rows: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("closing writer: %v", err)
	}
	return buf.Bytes()
}

func vptr(v variant.Value) *variant.Value { return &v }

// TestVariantReaderLocs checks the per-row location tags of a field
// shredded as int64 across the full disposition matrix, including type
// conflicts surfaced through residuals.
func TestVariantReaderLocs(t *testing.T) {
	obj := func(fields ...variant.Field) *variant.Value { return vptr(variant.MakeObject(fields)) }
	field := func(name string, v variant.Value) variant.Field { return variant.Field{Name: name, Value: v} }

	values := []*variant.Value{
		obj(field("a", variant.Int64(5))),    // typed
		obj(field("a", variant.String("s"))), // type conflict -> residual
		obj(field("a", variant.Null())),      // variant null
		obj(field("b", variant.Int64(1))),    // field missing
		nil,                                  // null variant row
		vptr(variant.Int64(7)),               // root not an object
		obj(field("a", variant.MakeArray([]variant.Value{variant.Int64(1), variant.Int64(2)}))), // list where scalar shredded
	}
	data := buildVariantFile(t, parquet.Group{"a": parquet.Int(64)}, values)

	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	r, err := parquet.NewVariantReader(f.RowGroups()[0], "var")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	a := r.Path("a")
	if a.Kind() != parquet.VariantCursorLeaf {
		t.Fatalf("cursor kind = %v, want leaf", a.Kind())
	}
	n, err := r.Next(100)
	if err != nil || n != len(values) {
		t.Fatalf("Next = (%d, %v), want (%d, nil)", n, err, len(values))
	}

	wantRoot := []parquet.VariantLoc{
		parquet.VariantLocTypedObject,
		parquet.VariantLocTypedObject,
		parquet.VariantLocTypedObject,
		parquet.VariantLocTypedObject,
		parquet.VariantLocMissing,
		parquet.VariantLocResidual,
		parquet.VariantLocTypedObject,
	}
	for i, want := range wantRoot {
		if got := r.Root().Locs()[i]; got != want {
			t.Errorf("root row %d: loc = %v, want %v", i, got, want)
		}
	}

	wantA := []parquet.VariantLoc{
		parquet.VariantLocTyped,
		parquet.VariantLocResidual,
		parquet.VariantLocNull,
		parquet.VariantLocMissing,
		parquet.VariantLocMissing,
		parquet.VariantLocMissing,
		parquet.VariantLocResidual,
	}
	for i, want := range wantA {
		if got := a.Locs()[i]; got != want {
			t.Errorf("a row %d: loc = %v, want %v", i, got, want)
		}
	}

	if got := a.Int64s(); len(got) != 1 || got[0] != 5 {
		t.Errorf("a.Int64s() = %v, want [5]", got)
	}
	if got := a.TypedRows(); len(got) != 1 || got[0] != 0 {
		t.Errorf("a.TypedRows() = %v, want [0]", got)
	}
	if got := a.ResidualCount(); got != 2 {
		t.Errorf("a.ResidualCount() = %d, want 2", got)
	}
	if v, ok, err := a.Residual(1); err != nil || !ok || !v.Equal(variant.String("s")) {
		t.Errorf("a.Residual(1) = (%#v, %v, %v), want string s", v.GoValue(), ok, err)
	}
	if v, ok, err := a.Residual(6); err != nil || !ok || v.Basic() != variant.BasicArray {
		t.Errorf("a.Residual(6) = (%#v, %v, %v), want array", v.GoValue(), ok, err)
	} else if elems := v.ArrayValue().Elements; len(elems) != 2 || elems[0].Int() != 1 {
		t.Errorf("a.Residual(6) elements = %#v", v.GoValue())
	}
}

// TestVariantReaderMixedList checks Elements/ListOffsets over a column
// shredded as a list of int64, with rows mixing typed lists, empty lists,
// scalars (conflicts), and residual objects.
func TestVariantReaderMixedList(t *testing.T) {
	arr := func(elems ...variant.Value) variant.Value { return variant.MakeArray(elems) }
	values := []*variant.Value{
		vptr(arr(variant.Int64(1), variant.Int64(2), variant.Int64(3))),
		vptr(arr()),
		vptr(variant.Int64(9)), // scalar where list shredded
		nil,
		vptr(arr(variant.Int64(4), variant.String("x"), variant.Null())), // mixed element types
	}
	data := buildVariantFile(t, parquet.List(parquet.Int(64)), values)

	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	r, err := parquet.NewVariantReader(f.RowGroups()[0], "var")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	root := r.Root()
	if root.Kind() != parquet.VariantCursorList {
		t.Fatalf("root kind = %v, want list", root.Kind())
	}
	el := root.Elements()
	if _, err := r.Next(100); err != nil {
		t.Fatal(err)
	}

	wantRoot := []parquet.VariantLoc{
		parquet.VariantLocTypedList,
		parquet.VariantLocTypedList,
		parquet.VariantLocResidual,
		parquet.VariantLocMissing,
		parquet.VariantLocTypedList,
	}
	for i, want := range wantRoot {
		if got := root.Locs()[i]; got != want {
			t.Errorf("root row %d: loc = %v, want %v", i, got, want)
		}
	}

	wantOffsets := []int32{0, 3, 3, 3, 3, 6}
	offsets := root.ListOffsets()
	if len(offsets) != len(wantOffsets) {
		t.Fatalf("ListOffsets = %v, want %v", offsets, wantOffsets)
	}
	for i := range wantOffsets {
		if offsets[i] != wantOffsets[i] {
			t.Fatalf("ListOffsets = %v, want %v", offsets, wantOffsets)
		}
	}

	wantElems := []parquet.VariantLoc{
		parquet.VariantLocTyped, parquet.VariantLocTyped, parquet.VariantLocTyped,
		parquet.VariantLocTyped, parquet.VariantLocResidual, parquet.VariantLocNull,
	}
	for i, want := range wantElems {
		if got := el.Locs()[i]; got != want {
			t.Errorf("element %d: loc = %v, want %v", i, got, want)
		}
	}
	wantInts := []int64{1, 2, 3, 4}
	ints := el.Int64s()
	if len(ints) != len(wantInts) {
		t.Fatalf("element Int64s = %v, want %v", ints, wantInts)
	}
	for i := range wantInts {
		if ints[i] != wantInts[i] {
			t.Fatalf("element Int64s = %v, want %v", ints, wantInts)
		}
	}
	wantRows := []int32{0, 0, 0, 4, 4, 4}
	for i, want := range wantRows {
		if got := el.Rows()[i]; got != want {
			t.Errorf("element %d: row = %d, want %d", i, got, want)
		}
	}
	if v, ok, err := el.Residual(4); err != nil || !ok || !v.Equal(variant.String("x")) {
		t.Errorf("el.Residual(4) = (%#v, %v, %v), want string x", v.GoValue(), ok, err)
	}
}

// TestVariantReaderVirtualNavigation navigates below the shredded schema:
// $.a is shredded as an object with typed field b, but the cursor asks for
// $.a.c (never shredded) and $.a.b.d (below a scalar), which must be
// resolved through residuals with accurate tags.
func TestVariantReaderVirtualNavigation(t *testing.T) {
	obj := func(fields ...variant.Field) variant.Value { return variant.MakeObject(fields) }
	field := func(name string, v variant.Value) variant.Field { return variant.Field{Name: name, Value: v} }

	values := []*variant.Value{
		vptr(obj(field("a", obj(field("b", variant.Int64(1)), field("c", variant.String("c0")))))),
		vptr(obj(field("a", obj(field("b", obj(field("d", variant.Int64(42)))))))),  // b conflicts: object where int shredded
		vptr(obj(field("a", variant.Int64(3)))),                                     // a conflicts: scalar where object shredded
		vptr(obj(field("a", obj(field("c", obj(field("e", variant.Bool(true)))))))), // deep under never-shredded c
		nil,
	}
	shred := parquet.Group{"a": parquet.Group{"b": parquet.Int(64)}}
	data := buildVariantFile(t, shred, values)

	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	r, err := parquet.NewVariantReader(f.RowGroups()[0], "var")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	b := r.Path("a", "b")
	c := r.Path("a", "c")
	bd := r.Path("a", "b", "d")
	ce := r.Path("a", "c", "e")
	if b.Kind() != parquet.VariantCursorLeaf {
		t.Fatalf("b kind = %v, want leaf", b.Kind())
	}
	if c.Kind() != parquet.VariantCursorUnshredded {
		t.Fatalf("c kind = %v, want unshredded", c.Kind())
	}

	if _, err := r.Next(100); err != nil {
		t.Fatal(err)
	}

	check := func(name string, cur *parquet.VariantCursor, want []parquet.VariantLoc) {
		t.Helper()
		for i, w := range want {
			if got := cur.Locs()[i]; got != w {
				t.Errorf("%s row %d: loc = %v, want %v", name, i, got, w)
			}
		}
	}
	check("a.b", b, []parquet.VariantLoc{
		parquet.VariantLocTyped,
		parquet.VariantLocResidual, // object where int64 was shredded
		parquet.VariantLocMissing,  // a is a scalar: no field b
		parquet.VariantLocMissing,
		parquet.VariantLocMissing,
	})
	check("a.c", c, []parquet.VariantLoc{
		parquet.VariantLocResidual, // from partial object leftover
		parquet.VariantLocMissing,
		parquet.VariantLocMissing,
		parquet.VariantLocResidual,
		parquet.VariantLocMissing,
	})
	check("a.b.d", bd, []parquet.VariantLoc{
		parquet.VariantLocMissing, // b is int64
		parquet.VariantLocResidual,
		parquet.VariantLocMissing,
		parquet.VariantLocMissing,
		parquet.VariantLocMissing,
	})
	check("a.c.e", ce, []parquet.VariantLoc{
		parquet.VariantLocMissing, // c is a string
		parquet.VariantLocMissing,
		parquet.VariantLocMissing,
		parquet.VariantLocResidual,
		parquet.VariantLocMissing,
	})

	if v, ok, err := bd.Residual(1); err != nil || !ok || v.Int() != 42 {
		t.Errorf("a.b.d.Residual(1) = (%#v, %v, %v), want 42", v.GoValue(), ok, err)
	}
	if v, ok, err := ce.Residual(3); err != nil || !ok || !v.BoolValue() {
		t.Errorf("a.c.e.Residual(3) = (%#v, %v, %v), want true", v.GoValue(), ok, err)
	}
}

// TestVariantReaderSeekToRow seeks around a file large enough to span
// multiple pages and verifies windows read from the requested rows.
func TestVariantReaderSeekToRow(t *testing.T) {
	const numRows = 500
	values := make([]*variant.Value, numRows)
	for i := range values {
		values[i] = vptr(variant.MakeObject([]variant.Field{
			{Name: "a", Value: variant.Int64(int64(i))},
		}))
	}
	data := buildVariantFile(t, parquet.Group{"a": parquet.Int(64)}, values,
		parquet.PageBufferSize(256))

	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	r, err := parquet.NewVariantReader(f.RowGroups()[0], "var")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	a := r.Path("a")

	for _, seek := range []int64{450, 3, 123, 0, 499} {
		if err := r.SeekToRow(seek); err != nil {
			t.Fatalf("SeekToRow(%d): %v", seek, err)
		}
		n, err := r.Next(10)
		if err != nil {
			t.Fatalf("Next after seek %d: %v", seek, err)
		}
		ints := a.Int64s()
		if len(ints) != n {
			t.Fatalf("seek %d: %d typed values for %d rows", seek, len(ints), n)
		}
		for i := range n {
			if ints[i] != seek+int64(i) {
				t.Fatalf("seek %d: row %d = %d, want %d", seek, i, ints[i], seek+int64(i))
			}
		}
	}
}

// TestVariantReaderDictionary reads typed columns written with dictionary
// encoding, which surface as indexed pages and take the dictionary-lookup
// path of the leaf readers.
func TestVariantReaderDictionary(t *testing.T) {
	r := rand.New(rand.NewPCG(5, 9))
	const numRows = 200
	values := make([]*variant.Value, numRows)
	words := []string{"red", "green", "blue"}
	for i := range values {
		values[i] = vptr(variant.MakeObject([]variant.Field{
			{Name: "a", Value: variant.Int64(int64(r.IntN(4)))},
			{Name: "s", Value: variant.String(words[r.IntN(len(words))])},
		}))
	}
	shred := parquet.Group{
		"a": parquet.Encoded(parquet.Int(64), &parquet.RLEDictionary),
		"s": parquet.Encoded(parquet.String(), &parquet.RLEDictionary),
	}
	data := buildVariantFile(t, shred, values)

	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	dictSeen := false
	for _, cc := range f.Metadata().RowGroups[0].Columns {
		for _, enc := range cc.MetaData.Encoding {
			if enc.String() == "RLE_DICTIONARY" || enc.String() == "PLAIN_DICTIONARY" {
				dictSeen = true
			}
		}
	}
	if !dictSeen {
		t.Fatal("test setup: no dictionary-encoded column was written")
	}

	got, present := readVariantColumnar(t, data, 7, "var")
	for i, want := range values {
		if !present[i] {
			t.Fatalf("row %d reported missing", i)
		}
		if !got[i].Equal(*want) {
			t.Errorf("row %d mismatch:\n got: %#v\nwant: %#v", i, got[i].GoValue(), want.GoValue())
		}
	}
}

// BenchmarkVariantReaderScan measures scanning one shredded int64 path
// ($.a) of a variant column, columnar cursors vs. the row-based read path.
func BenchmarkVariantReaderScan(b *testing.B) {
	const numRows = 10000
	values := make([]*variant.Value, numRows)
	for i := range values {
		values[i] = vptr(variant.MakeObject([]variant.Field{
			{Name: "a", Value: variant.Int64(int64(i))},
			{Name: "s", Value: variant.String("payload that is not projected")},
		}))
	}
	shredded, err := parquet.ShreddedVariant(parquet.Group{"a": parquet.Int(64), "s": parquet.String()})
	if err != nil {
		b.Fatal(err)
	}
	schema := parquet.NewSchema("table", parquet.Group{
		"id":  parquet.Int(32),
		"var": shredded,
	})
	rows := make([]shreddedVariantRow, numRows)
	for i, v := range values {
		rows[i] = shreddedVariantRow{ID: int32(i), Var: encodeRawVariant(*v)}
	}
	buf := new(bytes.Buffer)
	w := parquet.NewGenericWriter[shreddedVariantRow](buf, schema)
	if _, err := w.Write(rows); err != nil {
		b.Fatal(err)
	}
	if err := w.Close(); err != nil {
		b.Fatal(err)
	}
	data := buf.Bytes()
	wantSum := int64(numRows) * (numRows - 1) / 2

	b.Run("columnar", func(b *testing.B) {
		f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			b.Fatal(err)
		}
		r, err := parquet.NewVariantReader(f.RowGroups()[0], "var")
		if err != nil {
			b.Fatal(err)
		}
		defer r.Close()
		a := r.Path("a")
		b.ResetTimer()
		for b.Loop() {
			if err := r.SeekToRow(0); err != nil {
				b.Fatal(err)
			}
			sum := int64(0)
			for {
				_, err := r.Next(4096)
				if err == io.EOF {
					break
				}
				if err != nil {
					b.Fatal(err)
				}
				for _, v := range a.Int64s() {
					sum += v
				}
			}
			if sum != wantSum {
				b.Fatalf("sum = %d, want %d", sum, wantSum)
			}
		}
	})

	b.Run("rows", func(b *testing.B) {
		for b.Loop() {
			got, err := parquet.Read[rawVariantRow](bytes.NewReader(data), int64(len(data)))
			if err != nil {
				b.Fatal(err)
			}
			sum := int64(0)
			for i := range got {
				v, err := decodeRawVariant(got[i].Var)
				if err != nil {
					b.Fatal(err)
				}
				for _, f := range v.ObjectValue().Fields {
					if f.Name == "a" {
						sum += f.Value.Int()
					}
				}
			}
			if sum != wantSum {
				b.Fatalf("sum = %d, want %d", sum, wantSum)
			}
		}
	})
}

// TestVariantReaderSmallPages runs the differential reconstruction over a
// file with tiny pages, so windows repeatedly cross page boundaries in
// every column.
func TestVariantReaderSmallPages(t *testing.T) {
	r := rand.New(rand.NewPCG(101, 202))
	const numRows = 100
	values := make([]variant.Value, numRows)
	ptrs := make([]*variant.Value, numRows)
	for i := range values {
		values[i] = randomVariant(r, 0)
		ptrs[i] = &values[i]
	}
	shred := parquet.Group{
		"a": parquet.Int(64),
		"b": parquet.List(parquet.String()),
		"c": parquet.Group{"d": parquet.Leaf(parquet.DoubleType)},
	}
	data := buildVariantFile(t, shred, ptrs, parquet.PageBufferSize(512))

	for _, window := range []int{1, 7, 1000} {
		got, present := readVariantColumnar(t, data, window, "var")
		if len(got) != numRows {
			t.Fatalf("window %d: read %d rows, want %d", window, len(got), numRows)
		}
		for i, want := range values {
			if !present[i] {
				t.Errorf("window %d row %d: reported missing", window, i)
				continue
			}
			if !got[i].Equal(want) {
				t.Errorf("window %d row %d mismatch:\n got: %#v\nwant: %#v", window, i, got[i].GoValue(), want.GoValue())
			}
		}
	}
}
