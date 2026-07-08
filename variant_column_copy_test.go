package parquet_test

import (
	"bytes"
	"fmt"
	"io"
	"math/rand/v2"
	"testing"

	"github.com/google/uuid"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/variant"
)

// The tests in this file validate CopyVariantRows (variant_column_copy.go),
// the compaction bridge between the columnar variant reader and writer.
// Sources are written through the row-based write path, so a copy failure
// cannot cancel out against a matching reader/writer bug, and results are
// verified with the same assertions as the writer tests.

// compactVariantFiles merges the "var" variant column of the given source
// files, in order, into one new file shredded per target (nil =
// unshredded), using NewVariantReader + CopyVariantRows per source row
// group.
func compactVariantFiles(t *testing.T, target parquet.Node, sources [][]byte, options ...parquet.WriterOption) []byte {
	t.Helper()
	variantNode := parquet.Variant()
	if target != nil {
		shredded, err := parquet.ShreddedVariant(target)
		if err != nil {
			t.Fatalf("building shredded variant node: %v", err)
		}
		variantNode = shredded
	}
	// The destination schema holds only the variant column: CopyVariantRows
	// fills the variant leaf columns and nothing else.
	schema := parquet.NewSchema("table", parquet.Group{
		"var": parquet.Optional(variantNode),
	})
	buf := new(bytes.Buffer)
	opts := append([]parquet.WriterOption{schema}, options...)
	w := parquet.NewWriter(buf, opts...)
	vw, err := parquet.NewVariantColumnWriter(w, "var")
	if err != nil {
		t.Fatalf("NewVariantColumnWriter: %v", err)
	}
	for i, data := range sources {
		f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			t.Fatalf("opening source %d: %v", i, err)
		}
		for _, rg := range f.RowGroups() {
			r, err := parquet.NewVariantReader(rg, "var")
			if err != nil {
				t.Fatalf("NewVariantReader on source %d: %v", i, err)
			}
			n, err := parquet.CopyVariantRows(vw, r)
			if err != nil {
				t.Fatalf("CopyVariantRows on source %d: %v", i, err)
			}
			if n != rg.NumRows() {
				t.Fatalf("CopyVariantRows on source %d copied %d rows, want %d", i, n, rg.NumRows())
			}
			if err := r.Close(); err != nil {
				t.Fatalf("closing reader on source %d: %v", i, err)
			}
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("closing writer: %v", err)
	}
	return buf.Bytes()
}

// TestCopyVariantRowsRandomized re-shreds the randomized value matrix
// between random source and target shredding schemas (including unshredded
// on either side) and asserts the compacted file reads back equal to the
// originals.
func TestCopyVariantRowsRandomized(t *testing.T) {
	const numSchemas, rowsPerSchema = 48, 13
	r := rand.New(rand.NewPCG(31, 63))
	for i := range numSchemas {
		t.Run(fmt.Sprintf("schema_%02d", i), func(t *testing.T) {
			var source, target parquet.Node
			if r.IntN(4) != 0 {
				source = randomShredNode(r, 0)
			}
			if r.IntN(4) != 0 {
				target = randomShredNode(r, 0)
			}
			values := make([]*variant.Value, rowsPerSchema)
			for j := range values {
				if r.IntN(8) == 0 {
					continue // null variant row
				}
				values[j] = vptr(randomVariant(r, 0))
			}
			data := buildVariantFile(t, source, values)
			compacted := compactVariantFiles(t, target, [][]byte{data})
			assertVariantFile(t, compacted, values, fmt.Sprintf("schema %d", i))
		})
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

// TestCopyVariantRowsMultiFile compacts several files with different
// shredding schemas (and one unshredded) into a single file, shredded and
// unshredded.
func TestCopyVariantRowsMultiFile(t *testing.T) {
	sources, all := buildMultiSchemaVariantSources(t, rand.New(rand.NewPCG(77, 99)), 9)

	targets := map[string]parquet.Node{
		"shredded":   parquet.Group{"a": parquet.Int(64), "c": parquet.List(parquet.Int(64))},
		"unshredded": nil,
	}
	for name, target := range targets {
		t.Run(name, func(t *testing.T) {
			compacted := compactVariantFiles(t, target, sources)
			assertVariantFile(t, compacted, all, name)
		})
	}
}

// TestCopyVariantRowsManyRows crosses copy-window, page, and row-group
// boundaries: the source is split into many small row groups, and the
// destination flushes small pages.
func TestCopyVariantRowsManyRows(t *testing.T) {
	r := rand.New(rand.NewPCG(13, 37))
	shred := parquet.Group{
		"name": parquet.String(),
		"n":    parquet.Int(64),
	}
	const numRows = 700 // several copy windows of 256 rows
	values := make([]*variant.Value, numRows)
	for j := range values {
		switch r.IntN(10) {
		case 0:
			// null variant row
		case 1:
			values[j] = vptr(randomVariant(r, 0))
		default:
			values[j] = vptr(variant.MakeObject([]variant.Field{
				{Name: "name", Value: variant.String(fmt.Sprintf("row-%d", j))},
				{Name: "n", Value: variant.Int64(int64(j))},
			}))
		}
	}
	data := buildVariantFile(t, shred, values, parquet.MaxRowsPerRowGroup(64))
	compacted := compactVariantFiles(t, shred, [][]byte{data}, parquet.PageBufferSize(512))
	assertVariantFile(t, compacted, values, "many rows")
}

// TestCopyVariantRowsStaysTyped verifies that compacting between identical
// shredding schemas keeps values in the typed_value columns instead of
// degrading them to residual variant binary, across every shredded leaf
// type (which also exercises every typed emitter of the copy).
func TestCopyVariantRowsStaysTyped(t *testing.T) {
	shred := parquet.Group{}
	fields := make(map[string]variant.Value)
	u := uuid.MustParse("f47ac10b-58cc-4372-a567-0e02b2c3d479")
	for name, tv := range map[string]struct {
		node  parquet.Node
		value variant.Value
	}{
		"b":      {parquet.Leaf(parquet.BooleanType), variant.Bool(true)},
		"i8":     {parquet.Int(8), variant.Int8(-5)},
		"i16":    {parquet.Int(16), variant.Int16(300)},
		"i32":    {parquet.Int(32), variant.Int32(70000)},
		"i64":    {parquet.Int(64), variant.Int64(1 << 40)},
		"f":      {parquet.Leaf(parquet.FloatType), variant.Float(1.5)},
		"d":      {parquet.Leaf(parquet.DoubleType), variant.Double(2.5)},
		"s":      {parquet.String(), variant.String("hello")},
		"bin":    {parquet.Leaf(parquet.ByteArrayType), variant.Binary([]byte{1, 2, 3})},
		"date":   {parquet.Date(), variant.Date(19000)},
		"uuid":   {parquet.UUID(), variant.UUID(u)},
		"ts":     {parquet.TimestampAdjusted(parquet.Microsecond, true), variant.Timestamp(1234567)},
		"tsntz":  {parquet.TimestampAdjusted(parquet.Microsecond, false), variant.TimestampNTZ(1234567)},
		"tsn":    {parquet.TimestampAdjusted(parquet.Nanosecond, true), variant.TimestampNanos(123456789)},
		"tsnntz": {parquet.TimestampAdjusted(parquet.Nanosecond, false), variant.TimestampNTZNanos(123456789)},
		"time":   {parquet.TimeAdjusted(parquet.Microsecond, false), variant.Time(456789)},
		"dec4":   {parquet.Decimal(2, 9, parquet.Int32Type), variant.Decimal4(12345, 2)},
		"dec8":   {parquet.Decimal(2, 18, parquet.Int64Type), variant.Decimal8(1234567, 2)},
		"dec16":  {parquet.Decimal(2, 38, parquet.FixedLenByteArrayType(16)), variant.Decimal16([16]byte{0x39, 0x30}, 2)}, // 12345 little-endian
	} {
		shred[name] = tv.node
		fields[name] = tv.value
	}
	names := make([]string, 0, len(fields))
	vfields := make([]variant.Field, 0, len(fields))
	for name, v := range fields {
		names = append(names, name)
		vfields = append(vfields, variant.Field{Name: name, Value: v})
	}
	values := make([]*variant.Value, 20)
	for j := range values {
		values[j] = vptr(variant.MakeObject(vfields))
	}
	data := buildVariantFile(t, shred, values)
	compacted := compactVariantFiles(t, shred, [][]byte{data})
	assertVariantFile(t, compacted, values, "stays typed")
	assertVariantFieldsTyped(t, compacted, len(values), names...)
}

// BenchmarkCopyVariantRows compares compacting a shredded variant file
// through CopyVariantRows against the row-based transcode (read rows,
// write rows), both re-shredding to the same schema.
func BenchmarkCopyVariantRows(b *testing.B) {
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

	type row struct {
		Var rawVariant `parquet:"var,optional,variant"`
	}
	const numRows = 5000
	rows := make([]row, numRows)
	for i := range rows {
		rows[i].Var = encodeRawVariant(variant.MakeObject([]variant.Field{
			{Name: "name", Value: variant.String("alice")},
			{Name: "age", Value: variant.Int64(int64(i))},
			{Name: "tags", Value: variant.MakeArray([]variant.Value{
				variant.String("x"), variant.String("y"), variant.String("z"),
			})},
			{Name: "extra", Value: variant.Double(1.5)},
		}))
	}
	buf := new(bytes.Buffer)
	w := parquet.NewGenericWriter[row](buf, schema)
	if _, err := w.Write(rows); err != nil {
		b.Fatal(err)
	}
	if err := w.Close(); err != nil {
		b.Fatal(err)
	}
	data := buf.Bytes()

	b.Run("columnar", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
			if err != nil {
				b.Fatal(err)
			}
			w := parquet.NewWriter(io.Discard, schema)
			vw, err := parquet.NewVariantColumnWriter(w, "var")
			if err != nil {
				b.Fatal(err)
			}
			var n int64
			for _, rg := range f.RowGroups() {
				r, err := parquet.NewVariantReader(rg, "var")
				if err != nil {
					b.Fatal(err)
				}
				k, err := parquet.CopyVariantRows(vw, r)
				if err != nil {
					b.Fatal(err)
				}
				r.Close()
				n += k
			}
			if err := w.Close(); err != nil {
				b.Fatal(err)
			}
			if n != numRows {
				b.Fatalf("copied %d rows, want %d", n, numRows)
			}
		}
	})

	b.Run("rows", func(b *testing.B) {
		out := make([]row, 256)
		b.ReportAllocs()
		for b.Loop() {
			f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
			if err != nil {
				b.Fatal(err)
			}
			r := parquet.NewGenericReader[row](f, schema)
			w := parquet.NewGenericWriter[row](io.Discard, schema)
			n := 0
			for {
				k, err := r.Read(out)
				if k > 0 {
					if _, werr := w.Write(out[:k]); werr != nil {
						b.Fatal(werr)
					}
					n += k
				}
				if err == io.EOF {
					break
				}
				if err != nil {
					b.Fatal(err)
				}
			}
			if err := r.Close(); err != nil {
				b.Fatal(err)
			}
			if err := w.Close(); err != nil {
				b.Fatal(err)
			}
			if n != numRows {
				b.Fatalf("copied %d rows, want %d", n, numRows)
			}
		}
	})
}
