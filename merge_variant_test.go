package parquet_test

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/variant"
	"slices"
)

// The tests in this file cover variant columns flowing through the standard
// merge pipeline: MergeRowGroups + Writer.WriteRowGroup. Unsorted merges
// take the columnar copy fast path (variant_column_copy.go); sorted merges
// and schema shapes the fast path rejects fall back to row-based copying
// through the re-shredding schema conversion (convert_variant.go).

// openRowGroups opens the row groups of the given parquet files.
func openRowGroups(t *testing.T, files ...[]byte) []parquet.RowGroup {
	t.Helper()
	var rowGroups []parquet.RowGroup
	for i, data := range files {
		f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			t.Fatalf("opening file %d: %v", i, err)
		}
		rowGroups = append(rowGroups, f.RowGroups()...)
	}
	return rowGroups
}

// mergeVariantFiles merges the given files through MergeRowGroups and
// WriteRowGroup, returning the merged file bytes.
func mergeVariantFiles(t *testing.T, files [][]byte, options ...parquet.RowGroupOption) []byte {
	t.Helper()
	merged, err := parquet.MergeRowGroups(openRowGroups(t, files...), options...)
	if err != nil {
		t.Fatalf("MergeRowGroups: %v", err)
	}
	buf := new(bytes.Buffer)
	w := parquet.NewWriter(buf, merged.Schema())
	if _, err := w.WriteRowGroup(merged); err != nil {
		t.Fatalf("WriteRowGroup: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("closing writer: %v", err)
	}
	return buf.Bytes()
}

// TestMergeNodesVariant checks the variant-specific node merge rules:
// identical shredding is preserved, differing shredding falls back to the
// unshredded form (a field-wise union of two shredding schemas would not be
// a valid shredding schema).
func TestMergeNodesVariant(t *testing.T) {
	shredded := func(g parquet.Group) parquet.Node {
		n, err := parquet.ShreddedVariant(g)
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	a := shredded(parquet.Group{"a": parquet.Int(64)})
	sameAsA := shredded(parquet.Group{"a": parquet.Int(64)})
	b := shredded(parquet.Group{"a": parquet.String()})

	same := parquet.MergeNodes(
		parquet.Group{"var": a},
		parquet.Group{"var": sameAsA},
	)
	if !parquet.SameNodes(fieldByName(t, same, "var"), parquet.Required(sameAsA)) {
		t.Errorf("merging identical shredding should preserve it, got:\n%s", same)
	}

	diff := parquet.MergeNodes(
		parquet.Group{"var": a},
		parquet.Group{"var": b},
	)
	if !parquet.SameNodes(fieldByName(t, diff, "var"), parquet.Required(parquet.Variant())) {
		t.Errorf("merging different shredding should fall back to unshredded, got:\n%s", diff)
	}
}

func fieldByName(t *testing.T, node parquet.Node, name string) parquet.Node {
	t.Helper()
	for _, f := range node.Fields() {
		if f.Name() == name {
			return f
		}
	}
	t.Fatalf("no field %q in %s", name, node)
	return nil
}

// TestMergeRowGroupsVariantReshred merges files whose variant columns are
// shredded differently (and one unshredded): the merged schema falls back
// to unshredded, and every value must survive both the columnar fast path
// (WriteRowGroup) and the row-based path (ReadRowsFrom of the merged rows).
func TestMergeRowGroupsVariantReshred(t *testing.T) {
	r := rand.New(rand.NewPCG(5, 25))
	schemas := []parquet.Node{
		parquet.Group{"a": parquet.Int(64), "b": parquet.String()},
		parquet.Group{"a": parquet.String(), "c": parquet.List(parquet.Int(64))},
		nil, // unshredded
	}
	var files [][]byte
	var all []*variant.Value
	for _, shred := range schemas {
		values := make([]*variant.Value, 7)
		for j := range values {
			if r.IntN(6) == 0 {
				continue // null variant row
			}
			values[j] = vptr(randomVariant(r, 0))
		}
		files = append(files, buildVariantFile(t, shred, values))
		all = append(all, values...)
	}

	// Columnar fast path through WriteRowGroup.
	data := mergeVariantFiles(t, files)
	assertVariantFile(t, data, all, "merge via WriteRowGroup")

	// Row-based path: read the merged rows and write them row by row.
	merged, err := parquet.MergeRowGroups(openRowGroups(t, files...))
	if err != nil {
		t.Fatal(err)
	}
	buf := new(bytes.Buffer)
	w := parquet.NewWriter(buf, merged.Schema())
	rows := merged.Rows()
	if _, err := w.ReadRowsFrom(rows); err != nil {
		t.Fatalf("ReadRowsFrom: %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	assertVariantFile(t, buf.Bytes(), all, "merge via rows")
}

// TestMergeRowGroupsVariantExplicitSchema merges into an explicit target
// schema that re-shreds every source, and verifies the merged file keeps
// matching values in typed_value columns.
func TestMergeRowGroupsVariantExplicitSchema(t *testing.T) {
	values := func(n int, f func(int) variant.Value) []*variant.Value {
		out := make([]*variant.Value, n)
		for j := range out {
			out[j] = vptr(f(j))
		}
		return out
	}
	obj := func(j int) variant.Value {
		return variant.MakeObject([]variant.Field{
			{Name: "a", Value: variant.Int64(int64(j))},
			{Name: "b", Value: variant.String(fmt.Sprintf("s%d", j))},
		})
	}
	filesValues := [][]*variant.Value{
		values(9, obj),
		values(9, obj),
	}
	files := [][]byte{
		buildVariantFile(t, parquet.Group{"a": parquet.Int(64)}, filesValues[0]),
		buildVariantFile(t, nil, filesValues[1]),
	}
	var all []*variant.Value
	for _, v := range filesValues {
		all = append(all, v...)
	}

	target, err := parquet.ShreddedVariant(parquet.Group{"a": parquet.Int(64), "b": parquet.String()})
	if err != nil {
		t.Fatal(err)
	}
	schema := parquet.NewSchema("table", parquet.Group{
		"id":  parquet.Int(32),
		"var": parquet.Optional(target),
	})
	data := mergeVariantFiles(t, files, schema)
	assertVariantFile(t, data, all, "explicit schema")

	// Both fields must be typed in the merged file: the copy re-shredded
	// values from both the shredded and the unshredded source.
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	r, err := parquet.NewVariantReader(f.RowGroups()[0], "var")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	ca, cb := r.Path("a"), r.Path("b")
	if _, err := r.Next(len(all)); err != nil {
		t.Fatal(err)
	}
	for _, c := range []*parquet.VariantCursor{ca, cb} {
		for e, loc := range c.Locs() {
			if loc != variant.LocTyped {
				t.Errorf("entry %d: location %v, want typed", e, loc)
			}
		}
	}
}

// TestMergeRowGroupsVariantSameShredding merges files with identical
// shredding: the merged schema preserves the shredding and values stay
// typed through the copy.
func TestMergeRowGroupsVariantSameShredding(t *testing.T) {
	shred := parquet.Group{"a": parquet.Int(64)}
	mk := func(n int) []*variant.Value {
		out := make([]*variant.Value, n)
		for j := range out {
			out[j] = vptr(variant.MakeObject([]variant.Field{
				{Name: "a", Value: variant.Int64(int64(j))},
			}))
		}
		return out
	}
	valuesA, valuesB := mk(11), mk(6)
	files := [][]byte{
		buildVariantFile(t, shred, valuesA),
		buildVariantFile(t, shred, valuesB),
	}
	merged, err := parquet.MergeRowGroups(openRowGroups(t, files...))
	if err != nil {
		t.Fatal(err)
	}
	mergedVar := fieldByName(t, merged.Schema(), "var")
	if isUnshredded := len(mergedVar.Fields()) == 2; isUnshredded {
		t.Fatalf("merged schema lost the shredding:\n%s", merged.Schema())
	}

	data := mergeVariantFiles(t, files)
	assertVariantFile(t, data, slices.Concat(valuesA, valuesB), "same shredding")

	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	r, err := parquet.NewVariantReader(f.RowGroups()[0], "var")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	c := r.Path("a")
	n, err := r.Next(1000)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(valuesA)+len(valuesB) {
		t.Fatalf("read %d rows, want %d", n, len(valuesA)+len(valuesB))
	}
	for e, loc := range c.Locs() {
		if loc != variant.LocTyped {
			t.Errorf("row %d: location %v, want typed", e, loc)
		}
	}
}

// TestMergeRowGroupsVariantSorted exercises the sorted (heap) merge, which
// stays on the row-based path: variant values are re-shredded by the schema
// conversion while rows are interleaved by the sort key.
func TestMergeRowGroupsVariantSorted(t *testing.T) {
	type row struct {
		ID  int32      `parquet:"id"`
		Var rawVariant `parquet:"var,optional,variant"`
	}
	build := func(shred parquet.Node, ids []int32) ([]byte, map[int32]variant.Value) {
		variantNode := parquet.Variant()
		if shred != nil {
			shredded, err := parquet.ShreddedVariant(shred)
			if err != nil {
				t.Fatal(err)
			}
			variantNode = shredded
		}
		schema := parquet.NewSchema("table", parquet.Group{
			"id":  parquet.Int(32),
			"var": parquet.Optional(variantNode),
		})
		byID := make(map[int32]variant.Value, len(ids))
		rows := make([]row, len(ids))
		for i, id := range ids {
			v := variant.MakeObject([]variant.Field{
				{Name: "a", Value: variant.Int64(int64(id))},
				{Name: "extra", Value: variant.String(fmt.Sprintf("id-%d", id))},
			})
			byID[id] = v
			rows[i] = row{ID: id, Var: encodeRawVariant(v)}
		}
		buf := new(bytes.Buffer)
		w := parquet.NewGenericWriter[row](buf, schema,
			parquet.SortingWriterConfig(parquet.SortingColumns(parquet.Ascending("id"))))
		if _, err := w.Write(rows); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		return buf.Bytes(), byID
	}

	evens := []int32{0, 2, 4, 6, 8}
	odds := []int32{1, 3, 5, 7, 9}
	fileA, byIDA := build(parquet.Group{"a": parquet.Int(64)}, evens)
	fileB, byIDB := build(parquet.Group{"a": parquet.String()}, odds)

	merged, err := parquet.MergeRowGroups(openRowGroups(t, fileA, fileB),
		parquet.SortingRowGroupConfig(parquet.SortingColumns(parquet.Ascending("id"))))
	if err != nil {
		t.Fatal(err)
	}
	buf := new(bytes.Buffer)
	w := parquet.NewWriter(buf, merged.Schema())
	if _, err := w.WriteRowGroup(merged); err != nil {
		t.Fatalf("WriteRowGroup: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	got, present := readVariantColumnar(t, buf.Bytes(), 100, "var")
	if len(got) != 10 {
		t.Fatalf("read %d rows, want 10", len(got))
	}
	for i := range got {
		id := int32(i) // ids interleave to 0..9 after the sorted merge
		want, ok := byIDA[id]
		if !ok {
			want = byIDB[id]
		}
		if !present[i] {
			t.Errorf("row %d: null, want %#v", i, want.GoValue())
			continue
		}
		if !got[i].Equal(want) {
			t.Errorf("row %d mismatch:\n got: %#v\nwant: %#v", i, got[i].GoValue(), want.GoValue())
		}
	}
}

// TestMergeRowGroupsVariantExtraColumn merges sources whose non-variant
// columns differ (one file has an extra column), which the columnar fast
// path rejects; the row-based fallback must still re-shred correctly.
func TestMergeRowGroupsVariantExtraColumn(t *testing.T) {
	type rowA struct {
		ID  int32      `parquet:"id"`
		Var rawVariant `parquet:"var,optional,variant"`
	}
	type rowB struct {
		ID    int32      `parquet:"id"`
		Extra int64      `parquet:"extra"`
		Var   rawVariant `parquet:"var,optional,variant"`
	}
	shredded, err := parquet.ShreddedVariant(parquet.Group{"a": parquet.Int(64)})
	if err != nil {
		t.Fatal(err)
	}
	v1 := variant.MakeObject([]variant.Field{{Name: "a", Value: variant.Int64(1)}})
	v2 := variant.MakeObject([]variant.Field{{Name: "a", Value: variant.String("x")}})

	bufA := new(bytes.Buffer)
	wA := parquet.NewGenericWriter[rowA](bufA, parquet.NewSchema("table", parquet.Group{
		"id":  parquet.Int(32),
		"var": parquet.Optional(shredded),
	}))
	if _, err := wA.Write([]rowA{{ID: 0, Var: encodeRawVariant(v1)}}); err != nil {
		t.Fatal(err)
	}
	if err := wA.Close(); err != nil {
		t.Fatal(err)
	}

	bufB := new(bytes.Buffer)
	wB := parquet.NewGenericWriter[rowB](bufB, parquet.NewSchema("table", parquet.Group{
		"id":    parquet.Int(32),
		"extra": parquet.Int(64),
		"var":   parquet.Optional(parquet.Variant()),
	}))
	if _, err := wB.Write([]rowB{{ID: 1, Extra: 9, Var: encodeRawVariant(v2)}}); err != nil {
		t.Fatal(err)
	}
	if err := wB.Close(); err != nil {
		t.Fatal(err)
	}

	data := mergeVariantFiles(t, [][]byte{bufA.Bytes(), bufB.Bytes()})
	got, present := readVariantColumnar(t, data, 100, "var")
	want := []variant.Value{v1, v2}
	if len(got) != len(want) {
		t.Fatalf("read %d rows, want %d", len(got), len(want))
	}
	for i := range want {
		if !present[i] {
			t.Errorf("row %d: null, want %#v", i, want[i].GoValue())
			continue
		}
		if !got[i].Equal(want[i]) {
			t.Errorf("row %d mismatch:\n got: %#v\nwant: %#v", i, got[i].GoValue(), want[i].GoValue())
		}
	}
}

// TestMergeRowGroupsVariantRandomized runs the randomized schema/value
// matrix through the full merge pipeline: two files with independent random
// shredding merged into the schema MergeRowGroups derives.
func TestMergeRowGroupsVariantRandomized(t *testing.T) {
	const numSchemas = 24
	r := rand.New(rand.NewPCG(41, 82))
	for i := range numSchemas {
		t.Run(fmt.Sprintf("schema_%02d", i), func(t *testing.T) {
			var files [][]byte
			var all []*variant.Value
			for range 2 {
				var shred parquet.Node
				if r.IntN(4) != 0 {
					shred = randomShredNode(r, 0)
				}
				values := make([]*variant.Value, 5)
				for j := range values {
					if r.IntN(8) == 0 {
						continue
					}
					values[j] = vptr(randomVariant(r, 0))
				}
				files = append(files, buildVariantFile(t, shred, values))
				all = append(all, values...)
			}
			data := mergeVariantFiles(t, files)
			assertVariantFile(t, data, all, fmt.Sprintf("schema %d", i))
		})
	}
}

// BenchmarkMergeVariantRowGroups compares the columnar fast path taken by
// WriteRowGroup against the row-based fallback for an unsorted merge that
// re-shreds: the two source files shred the variant column differently and
// are merged into an explicit target shredding.
func BenchmarkMergeVariantRowGroups(b *testing.B) {
	shred := func(g parquet.Group) parquet.Node {
		n, err := parquet.ShreddedVariant(g)
		if err != nil {
			b.Fatal(err)
		}
		return n
	}
	type row struct {
		ID  int32      `parquet:"id"`
		Var rawVariant `parquet:"var,optional,variant"`
	}
	const numRows = 2000
	mkFile := func(variantNode parquet.Node) []byte {
		schema := parquet.NewSchema("table", parquet.Group{
			"id":  parquet.Int(32),
			"var": parquet.Optional(variantNode),
		})
		rows := make([]row, numRows)
		for i := range rows {
			rows[i] = row{ID: int32(i), Var: encodeRawVariant(variant.MakeObject([]variant.Field{
				{Name: "name", Value: variant.String("alice")},
				{Name: "age", Value: variant.Int64(int64(i))},
				{Name: "extra", Value: variant.Double(1.5)},
			}))}
		}
		buf := new(bytes.Buffer)
		w := parquet.NewGenericWriter[row](buf, schema)
		if _, err := w.Write(rows); err != nil {
			b.Fatal(err)
		}
		if err := w.Close(); err != nil {
			b.Fatal(err)
		}
		return buf.Bytes()
	}
	files := [][]byte{
		mkFile(shred(parquet.Group{"name": parquet.String(), "age": parquet.Int(64)})),
		mkFile(shred(parquet.Group{"name": parquet.String()})),
	}
	target := parquet.NewSchema("table", parquet.Group{
		"id":  parquet.Int(32),
		"var": parquet.Optional(shred(parquet.Group{"name": parquet.String(), "age": parquet.Int(64)})),
	})
	open := func() []parquet.RowGroup {
		var rowGroups []parquet.RowGroup
		for _, data := range files {
			f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
			if err != nil {
				b.Fatal(err)
			}
			rowGroups = append(rowGroups, f.RowGroups()...)
		}
		return rowGroups
	}

	b.Run("columnar", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			merged, err := parquet.MergeRowGroups(open(), target)
			if err != nil {
				b.Fatal(err)
			}
			w := parquet.NewWriter(discard{}, target)
			if _, err := w.WriteRowGroup(merged); err != nil {
				b.Fatal(err)
			}
			if err := w.Close(); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("rows", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			merged, err := parquet.MergeRowGroups(open(), target)
			if err != nil {
				b.Fatal(err)
			}
			w := parquet.NewWriter(discard{}, target)
			rows := merged.Rows()
			if _, err := w.ReadRowsFrom(rows); err != nil {
				b.Fatal(err)
			}
			if err := rows.Close(); err != nil {
				b.Fatal(err)
			}
			if err := w.Close(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }
