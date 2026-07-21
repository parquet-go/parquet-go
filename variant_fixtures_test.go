package parquet_test

import (
	"bytes"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"slices"
	"testing"

	"github.com/google/uuid"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
	"github.com/parquet-go/parquet-go/variant"
)

// This file holds the fixtures shared by the variant test files: the
// randomized schema and value generators, the raw variant binary row
// shapes, the test file builders, the cursor-based read-back oracle, and
// the assertion helpers built on it. Helpers used by a single test file
// stay in that file.

// shredFieldNames is the shared object field name pool. Values also use an
// out-of-pool name so residual (never-shredded) fields occur.
var shredFieldNames = [...]string{"a", "b", "c", "d"}

// randomShredNode generates a random typed_value schema node covering the
// full shredded-types table plus LIST and object groups.
func randomShredNode(r *rand.Rand, depth int) parquet.Node {
	if depth < 2 {
		switch r.IntN(8) {
		case 0:
			return parquet.List(randomShredNode(r, depth+1))
		case 1, 2:
			g := parquet.Group{}
			for _, name := range shredFieldNames {
				if r.IntN(2) == 0 {
					g[name] = randomShredNode(r, depth+1)
				}
			}
			if len(g) == 0 { // empty object typed_value groups are invalid
				g[shredFieldNames[r.IntN(len(shredFieldNames))]] = randomShredNode(r, depth+1)
			}
			return g
		}
	}
	switch r.IntN(17) {
	case 0:
		return parquet.Leaf(parquet.BooleanType)
	case 1:
		return parquet.Int(8)
	case 2:
		return parquet.Int(16)
	case 3:
		return parquet.Int(32)
	case 4:
		return parquet.Int(64)
	case 5:
		return parquet.Leaf(parquet.FloatType)
	case 6:
		return parquet.Leaf(parquet.DoubleType)
	case 7:
		return parquet.String()
	case 8:
		return parquet.Leaf(parquet.ByteArrayType)
	case 9:
		return parquet.Date()
	case 10:
		return parquet.UUID()
	case 11:
		return parquet.TimestampAdjusted(parquet.Microsecond, r.IntN(2) == 0)
	case 12:
		return parquet.TimestampAdjusted(parquet.Nanosecond, r.IntN(2) == 0)
	case 13:
		return parquet.TimeAdjusted(parquet.Microsecond, false) // spec: TIME(false, MICROS)
	case 14:
		return parquet.Decimal(2, 9, parquet.Int32Type)
	case 15:
		return parquet.Decimal(2, 18, parquet.Int64Type)
	default:
		return parquet.Decimal(2, 38, parquet.FixedLenByteArrayType(16))
	}
}

// randomVariant generates a random variant value from the same pools, so
// it sometimes matches a generated schema exactly, sometimes partially,
// and sometimes not at all.
func randomVariant(r *rand.Rand, depth int) variant.Value {
	if depth < 3 {
		switch r.IntN(6) {
		case 0:
			elems := make([]variant.Value, r.IntN(4))
			for i := range elems {
				elems[i] = randomVariant(r, depth+1)
			}
			return variant.MakeArray(elems)
		case 1:
			var fields []variant.Field
			for _, name := range shredFieldNames {
				if r.IntN(2) == 0 {
					fields = append(fields, variant.Field{Name: name, Value: randomVariant(r, depth+1)})
				}
			}
			if r.IntN(4) == 0 {
				fields = append(fields, variant.Field{Name: "resid", Value: randomVariant(r, depth+1)})
			}
			return variant.MakeObject(fields)
		}
	}
	switch r.IntN(17) {
	case 0:
		return variant.Null()
	case 1:
		return variant.Bool(r.IntN(2) == 0)
	case 2:
		return variant.Int8(int8(r.IntN(1 << 8)))
	case 3:
		return variant.Int16(int16(r.IntN(1 << 16)))
	case 4:
		return variant.Int32(int32(r.Uint32()))
	case 5:
		return variant.Int64(int64(r.Uint64()))
	case 6:
		return variant.Float(float32(r.NormFloat64()))
	case 7:
		return variant.Double(r.NormFloat64())
	case 8:
		n := r.IntN(80) // crosses the 63-byte short-string boundary
		b := make([]byte, n)
		for i := range b {
			b[i] = byte('a' + r.IntN(26))
		}
		return variant.String(string(b))
	case 9:
		b := make([]byte, r.IntN(20))
		for i := range b {
			b[i] = byte(r.Uint32())
		}
		return variant.Binary(b)
	case 10:
		return variant.Date(int32(r.IntN(40000)))
	case 11:
		var u uuid.UUID
		for i := range u {
			u[i] = byte(r.Uint32())
		}
		return variant.UUID(u)
	case 12:
		ts := int64(r.Uint64() >> 20)
		switch r.IntN(4) {
		case 0:
			return variant.Timestamp(ts)
		case 1:
			return variant.TimestampNTZ(ts)
		case 2:
			return variant.TimestampNanos(ts)
		default:
			return variant.TimestampNTZNanos(ts)
		}
	case 13:
		return variant.Time(int64(r.IntN(86400_000_000)))
	case 14:
		// Scale 2 matches the generated decimal columns; scale 3 must
		// fall back to the value column.
		return variant.Decimal4(int32(r.Uint32()), byte(2+r.IntN(2)))
	case 15:
		return variant.Decimal8(int64(r.Uint64()), byte(2+r.IntN(2)))
	default:
		var d [16]byte
		for i := range d {
			d[i] = byte(r.Uint32())
		}
		return variant.Decimal16(d, byte(2+r.IntN(2)))
	}
}

func encodeRawVariant(v variant.Value) rawVariant {
	var b variant.MetadataBuilder
	value := variant.Encode(&b, v)
	_, metadata := b.Build()
	return rawVariant{Metadata: metadata, Value: value}
}

// shreddedVariantRow is the row shape shared by every write and read path
// below. Var holds a rawVariant on both sides, so values enter and leave as
// variant binary without passing through lossy Go-native representations.
type shreddedVariantRow struct {
	ID  int32 `parquet:"id"`
	Var any   `parquet:"var,variant"`
}

type rawVariant struct {
	Metadata []byte `parquet:"metadata"`
	Value    []byte `parquet:"value"`
}

type rawVariantRow struct {
	ID  int32      `parquet:"id"`
	Var rawVariant `parquet:"var,variant"`
}

// splitVariantBin splits a .variant.bin golden (metadata directly followed
// by value) into its two parts by computing the metadata's encoded size
// from its header.
func splitVariantBin(data []byte) (metadata, value []byte, err error) {
	if len(data) == 0 {
		return nil, nil, fmt.Errorf("empty variant binary")
	}
	offsetSize := int(data[0]>>6)&0x03 + 1
	if len(data) < 1+offsetSize {
		return nil, nil, fmt.Errorf("variant binary too short for dictionary size")
	}
	readUint := func(b []byte) int {
		var v uint32
		for i := range offsetSize {
			v |= uint32(b[i]) << (8 * i)
		}
		return int(v)
	}
	dictSize := readUint(data[1:])
	lastOffsetPos := 1 + offsetSize + dictSize*offsetSize
	if len(data) < lastOffsetPos+offsetSize {
		return nil, nil, fmt.Errorf("variant binary too short for offsets")
	}
	stringsLen := readUint(data[lastOffsetPos:])
	metadataLen := lastOffsetPos + offsetSize + stringsLen
	if len(data) < metadataLen {
		return nil, nil, fmt.Errorf("variant binary too short for dictionary strings")
	}
	return data[:metadataLen], data[metadataLen:], nil
}

func decodeVariantGolden(t *testing.T, path string) variant.Value {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading golden: %v", err)
	}
	metaBytes, valBytes, err := splitVariantBin(data)
	if err != nil {
		t.Fatalf("splitting golden: %v", err)
	}
	m, err := variant.DecodeMetadata(metaBytes)
	if err != nil {
		t.Fatalf("decoding golden metadata: %v", err)
	}
	v, err := variant.Decode(m, valBytes)
	if err != nil {
		t.Fatalf("decoding golden value: %v", err)
	}
	return v
}

func decodeRawVariant(raw rawVariant) (variant.Value, error) {
	m, err := variant.DecodeMetadata(raw.Metadata)
	if err != nil {
		return variant.Null(), fmt.Errorf("metadata: %w", err)
	}
	v, err := variant.Decode(m, raw.Value)
	if err != nil {
		return variant.Null(), fmt.Errorf("value: %w", err)
	}
	return v, nil
}

// variantTestSchema builds the schema shared by the variant test file
// builders: an id column plus an optional "var" variant column shredded per
// shred (nil means unshredded).
func variantTestSchema(tb testing.TB, shred parquet.Node) *parquet.Schema {
	tb.Helper()
	variantNode := parquet.Variant()
	if shred != nil {
		shredded, err := parquet.ShreddedVariant(shred)
		if err != nil {
			tb.Fatalf("building shredded variant node: %v", err)
		}
		variantNode = shredded
	}
	return parquet.NewSchema("table", parquet.Group{
		"id":  parquet.Int(32),
		"var": parquet.Optional(variantNode),
	})
}

// buildVariantIDFile writes rows (ids[i], values[i]) into a file with the
// given typed_value shredding schema (nil means unshredded), returning the
// file bytes. A nil value writes a null variant row.
func buildVariantIDFile(tb testing.TB, shred parquet.Node, ids []int32, values []*variant.Value, options ...parquet.WriterOption) []byte {
	tb.Helper()
	schema := variantTestSchema(tb, shred)
	rows := make([]shreddedVariantRow, len(values))
	for i, v := range values {
		rows[i] = shreddedVariantRow{ID: ids[i]}
		if v != nil {
			rows[i].Var = encodeRawVariant(*v)
		}
	}
	buf := new(bytes.Buffer)
	opts := append([]parquet.WriterOption{schema}, options...)
	w := parquet.NewGenericWriter[shreddedVariantRow](buf, opts...)
	if _, err := w.Write(rows); err != nil {
		tb.Fatalf("writing rows: %v", err)
	}
	if err := w.Close(); err != nil {
		tb.Fatalf("closing writer: %v", err)
	}
	return buf.Bytes()
}

// buildVariantFile is buildVariantIDFile with sequential ids.
func buildVariantFile(tb testing.TB, shred parquet.Node, values []*variant.Value, options ...parquet.WriterOption) []byte {
	tb.Helper()
	ids := make([]int32, len(values))
	for i := range ids {
		ids[i] = int32(i)
	}
	return buildVariantIDFile(tb, shred, ids, values, options...)
}

func vptr(v variant.Value) *variant.Value { return &v }

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
	var value format.LogicalTypeValue
	if lt != nil {
		value = lt.Value
	}
	switch value := value.(type) {
	case nil:
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
	case *format.StringType:
		slab, offsets := c.ByteArrays()
		return variant.String(string(slab[offsets[d]:offsets[d+1]])), nil
	case *format.IntType:
		switch value.BitWidth {
		case 8:
			return variant.Int8(int8(c.Int32s()[d])), nil
		case 16:
			return variant.Int16(int16(c.Int32s()[d])), nil
		case 32:
			return variant.Int32(c.Int32s()[d]), nil
		case 64:
			return variant.Int64(c.Int64s()[d]), nil
		}
	case *format.DateType:
		return variant.Date(c.Int32s()[d]), nil
	case *format.TimeType:
		return variant.Time(c.Int64s()[d]), nil
	case *format.TimestampType:
		v := c.Int64s()[d]
		switch value.Unit.Value.(type) {
		case *format.MicroSeconds:
			if value.IsAdjustedToUTC {
				return variant.Timestamp(v), nil
			}
			return variant.TimestampNTZ(v), nil
		case *format.NanoSeconds:
			if value.IsAdjustedToUTC {
				return variant.TimestampNanos(v), nil
			}
			return variant.TimestampNTZNanos(v), nil
		}
	case *format.DecimalType:
		scale := byte(value.Scale)
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
	case *format.UUIDType:
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
	case variant.LocMissing:
		return variant.Null(), false, nil
	case variant.LocNull:
		return variant.Null(), true, nil
	case variant.LocResidual:
		v, ok, err := c.Residual(e)
		if err != nil {
			return variant.Null(), false, err
		}
		if !ok {
			return variant.Null(), false, fmt.Errorf("residual entry %d has no residual value", e)
		}
		return v, true, nil
	case variant.LocTyped:
		d := typedIdx[c][e]
		if d < 0 {
			return variant.Null(), false, fmt.Errorf("typed entry %d not in TypedRows", e)
		}
		v, err := variantTypedValue(c, int(d))
		return v, true, err
	case variant.LocTypedObject:
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
	case variant.LocTypedList:
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

// assertVariantFieldsTyped opens a written file and asserts that every row
// of the given shredded fields is stored in the typed_value column (not as
// residual variant binary), which is how tests verify a copy or merge kept
// (or re-established) the shredding.
func assertVariantFieldsTyped(t *testing.T, data []byte, numRows int, fields ...string) {
	t.Helper()
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	r, err := parquet.NewVariantReader(f.RowGroups()[0], "var")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	cursors := make(map[string]*parquet.VariantCursor, len(fields))
	for _, name := range fields {
		cursors[name] = r.Path(name)
	}
	n, err := r.Next(numRows)
	if err != nil {
		t.Fatal(err)
	}
	if n != numRows {
		t.Fatalf("read %d rows, want %d", n, numRows)
	}
	for name, c := range cursors {
		for e, loc := range c.Locs() {
			if loc != variant.LocTyped {
				t.Errorf("field %q row %d: location %v, want typed", name, e, loc)
			}
		}
	}
}

// buildMultiSchemaVariantSources builds one file per shredding schema in a
// fixed three-schema mix (two different shreddings and one unshredded) with
// random values and occasional null rows, returning the files and the
// concatenated expected values.
func buildMultiSchemaVariantSources(t *testing.T, r *rand.Rand, rowsPerFile int) ([][]byte, []*variant.Value) {
	t.Helper()
	schemas := []parquet.Node{
		parquet.Group{"a": parquet.Int(64), "b": parquet.String()},
		parquet.Group{"a": parquet.String(), "c": parquet.List(parquet.Int(64))},
		nil, // unshredded
	}
	var files [][]byte
	var all []*variant.Value
	for _, shred := range schemas {
		values := make([]*variant.Value, rowsPerFile)
		for j := range values {
			if r.IntN(6) == 0 {
				continue // null variant row
			}
			values[j] = vptr(randomVariant(r, 0))
		}
		files = append(files, buildVariantFile(t, shred, values))
		all = append(all, values...)
	}
	return files, all
}
