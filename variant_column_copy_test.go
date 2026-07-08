package parquet_test

import (
	"bytes"
	"fmt"
	"io"
	"math/rand/v2"
	"testing"

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
			if _, err := parquet.CopyVariantRows(vw, r); err != nil {
				t.Fatalf("CopyVariantRows on source %d: %v", i, err)
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

// TestCopyVariantRowsMultiFile compacts several files with different
// shredding schemas (and one unshredded) into a single file, shredded and
// unshredded.
func TestCopyVariantRowsMultiFile(t *testing.T) {
	r := rand.New(rand.NewPCG(77, 99))
	schemas := []parquet.Node{
		parquet.Group{"a": parquet.Int(64), "b": parquet.String()},
		parquet.Group{"a": parquet.String(), "c": parquet.List(parquet.Int(64))},
		nil, // unshredded
	}
	var sources [][]byte
	var all []*variant.Value
	for _, shred := range schemas {
		values := make([]*variant.Value, 9)
		for j := range values {
			if r.IntN(6) == 0 {
				continue
			}
			values[j] = vptr(randomVariant(r, 0))
		}
		sources = append(sources, buildVariantFile(t, shred, values))
		all = append(all, values...)
	}

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
// degrading them to residual variant binary.
func TestCopyVariantRowsStaysTyped(t *testing.T) {
	shred := parquet.Group{"a": parquet.Int(64), "b": parquet.String()}
	values := make([]*variant.Value, 20)
	for j := range values {
		values[j] = vptr(variant.MakeObject([]variant.Field{
			{Name: "a", Value: variant.Int64(int64(j))},
			{Name: "b", Value: variant.String(fmt.Sprintf("s%d", j))},
		}))
	}
	data := buildVariantFile(t, shred, values)
	compacted := compactVariantFiles(t, shred, [][]byte{data})
	assertVariantFile(t, compacted, values, "stays typed")

	f, err := parquet.OpenFile(bytes.NewReader(compacted), int64(len(compacted)))
	if err != nil {
		t.Fatal(err)
	}
	r, err := parquet.NewVariantReader(f.RowGroups()[0], "var")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	cursors := map[string]*parquet.VariantCursor{
		"a": r.Path("a"),
		"b": r.Path("b"),
	}
	if _, err := r.Next(len(values)); err != nil {
		t.Fatal(err)
	}
	for name, c := range cursors {
		for e, loc := range c.Locs() {
			if loc != variant.LocTyped {
				t.Errorf("field %q row %d: location %v, want typed", name, e, loc)
			}
		}
	}
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
