package parquet_test

import (
	"bytes"
	"fmt"
	"io"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/variant"
)

// The tests in this file cover variant columns flowing through the standard
// merge pipeline: MergeRowGroups + Writer.WriteRowGroup. Unsorted merges
// take the columnar copy fast path (variant_column_copy.go); sorted merges
// and schema shapes the fast path rejects fall back to row-based copying
// through the re-shredding schema conversion (convert_variant.go).

// openRowGroups opens the row groups of the given parquet files.
func openRowGroups(tb testing.TB, files ...[]byte) []parquet.RowGroup {
	tb.Helper()
	var rowGroups []parquet.RowGroup
	for i, data := range files {
		f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			tb.Fatalf("opening file %d: %v", i, err)
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
	files, all := buildMultiSchemaVariantSources(t, rand.New(rand.NewPCG(5, 25)), 7)

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
	assertVariantFieldsTyped(t, data, len(all), "a", "b")
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
	assertVariantFieldsTyped(t, data, len(valuesA)+len(valuesB), "a")
}

// TestMergeRowGroupsVariantSorted exercises the sorted (heap) merge, which
// stays on the row-based path: variant values are re-shredded by the schema
// conversion while rows are interleaved by the sort key.
func TestMergeRowGroupsVariantSorted(t *testing.T) {
	build := func(shred parquet.Node, ids []int32) ([]byte, map[int32]variant.Value) {
		byID := make(map[int32]variant.Value, len(ids))
		values := make([]*variant.Value, len(ids))
		for i, id := range ids {
			v := variant.MakeObject([]variant.Field{
				{Name: "a", Value: variant.Int64(int64(id))},
				{Name: "extra", Value: variant.String(fmt.Sprintf("id-%d", id))},
			})
			byID[id] = v
			values[i] = vptr(v)
		}
		data := buildVariantIDFile(t, shred, ids, values,
			parquet.SortingWriterConfig(parquet.SortingColumns(parquet.Ascending("id"))))
		return data, byID
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

	want := make([]*variant.Value, 10)
	for id := range int32(10) { // ids interleave to 0..9 after the sorted merge
		if v, ok := byIDA[id]; ok {
			want[id] = vptr(v)
		} else {
			want[id] = vptr(byIDB[id])
		}
	}
	assertVariantFile(t, buf.Bytes(), want, "sorted merge")
}

// TestMergeRowGroupsVariantSortedNonOverlapping merges two sorted files
// with disjoint key ranges and different variant shredding. The
// non-overlapping segments become independently writable sub-row-groups
// (sortedSegmentRowGroup), which the writer's verbatim-copy fast path once
// consumed chunk-level, silently reproducing each source's shredding
// instead of re-shredding to the merged schema; the copy paths must demote
// so every value survives.
func TestMergeRowGroupsVariantSortedNonOverlapping(t *testing.T) {
	build := func(shred parquet.Group, baseID int32, values []variant.Value) []byte {
		ids := make([]int32, len(values))
		ptrs := make([]*variant.Value, len(values))
		for i := range values {
			ids[i] = baseID + int32(i)
			ptrs[i] = &values[i]
		}
		return buildVariantIDFile(t, shred, ids, ptrs,
			parquet.SortingWriterConfig(parquet.SortingColumns(parquet.Ascending("id"))))
	}

	want := make([]*variant.Value, 10)
	lo := make([]variant.Value, 5)
	hi := make([]variant.Value, 5)
	for i := range 5 {
		lo[i] = variant.MakeObject([]variant.Field{{Name: "a", Value: variant.Int64(int64(i))}})
		want[i] = vptr(lo[i])
		hi[i] = variant.MakeObject([]variant.Field{{Name: "a", Value: variant.String(fmt.Sprintf("s%d", 5+i))}})
		want[5+i] = vptr(hi[i])
	}
	// Different shredding on each side forces the merged schema to the
	// unshredded fallback, so both sources carry variant conversions.
	fileLo := build(parquet.Group{"a": parquet.Int(64)}, 0, lo)
	fileHi := build(parquet.Group{"a": parquet.String()}, 5, hi)

	data := mergeVariantFiles(t, [][]byte{fileLo, fileHi},
		parquet.SortingRowGroupConfig(parquet.SortingColumns(parquet.Ascending("id"))))
	assertVariantFile(t, data, want, "sorted non-overlapping merge")
}

// TestWriteRowGroupVariantConverted writes a bare converted row group whose
// conversion re-shreds a variant column, using a nested-optional shape that
// the columnar variant fast path declines. WriteRowGroup once fell through
// to the chunk-level copy paths, which reproduced the source's shredding
// verbatim and made every value unreadable; they must demote to the
// row-based path.
func TestWriteRowGroupVariantConverted(t *testing.T) {
	type inner struct {
		Var rawVariant `parquet:"var,optional,variant"`
	}
	type row struct {
		ID   int32  `parquet:"id"`
		Meta *inner `parquet:"meta,optional"`
	}
	shredded, err := parquet.ShreddedVariant(parquet.Group{"a": parquet.Int(64)})
	if err != nil {
		t.Fatal(err)
	}
	sourceSchema := parquet.NewSchema("table", parquet.Group{
		"id": parquet.Int(32),
		"meta": parquet.Optional(parquet.Group{
			"var": parquet.Optional(shredded),
		}),
	})
	values := make([]variant.Value, 5)
	rows := make([]row, len(values))
	for i := range rows {
		values[i] = variant.MakeObject([]variant.Field{{Name: "a", Value: variant.Int64(int64(i))}})
		rows[i] = row{ID: int32(i), Meta: &inner{Var: encodeRawVariant(values[i])}}
	}
	src := new(bytes.Buffer)
	sw := parquet.NewGenericWriter[row](src, sourceSchema)
	if _, err := sw.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := sw.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := parquet.OpenFile(bytes.NewReader(src.Bytes()), int64(src.Len()))
	if err != nil {
		t.Fatal(err)
	}

	targetSchema := parquet.NewSchema("table", parquet.Group{
		"id": parquet.Int(32),
		"meta": parquet.Optional(parquet.Group{
			"var": parquet.Optional(parquet.Variant()),
		}),
	})
	conv, err := parquet.Convert(targetSchema, sourceSchema)
	if err != nil {
		t.Fatal(err)
	}
	converted := parquet.ConvertRowGroup(f.RowGroups()[0], conv)

	buf := new(bytes.Buffer)
	w := parquet.NewWriter(buf, targetSchema)
	if _, err := w.WriteRowGroup(converted); err != nil {
		t.Fatalf("WriteRowGroup: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	out, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}
	r := parquet.NewGenericReader[row](out)
	defer r.Close()
	got := make([]row, len(rows))
	if n, _ := r.Read(got); n != len(rows) {
		t.Fatalf("read %d rows, want %d", n, len(rows))
	}
	for i, g := range got {
		if g.Meta == nil {
			t.Errorf("row %d: meta is nil", i)
			continue
		}
		m, err := variant.DecodeMetadata(g.Meta.Var.Metadata)
		if err != nil {
			t.Errorf("row %d: metadata: %v", i, err)
			continue
		}
		val, err := variant.Decode(m, g.Meta.Var.Value)
		if err != nil {
			t.Errorf("row %d: value: %v", i, err)
			continue
		}
		if !val.Equal(values[i]) {
			t.Errorf("row %d: got %#v, want %#v", i, val.GoValue(), values[i].GoValue())
		}
	}
}

// TestMergeRowGroupsVariantExtraColumn merges sources whose non-variant
// columns differ (one file has an extra column), which the columnar fast
// path rejects; the row-based fallback must still re-shred correctly.
func TestMergeRowGroupsVariantExtraColumn(t *testing.T) {
	type rowB struct {
		ID    int32      `parquet:"id"`
		Extra int64      `parquet:"extra"`
		Var   rawVariant `parquet:"var,optional,variant"`
	}
	v1 := variant.MakeObject([]variant.Field{{Name: "a", Value: variant.Int64(1)}})
	v2 := variant.MakeObject([]variant.Field{{Name: "a", Value: variant.String("x")}})

	fileA := buildVariantIDFile(t, parquet.Group{"a": parquet.Int(64)}, []int32{0}, []*variant.Value{vptr(v1)})

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

	data := mergeVariantFiles(t, [][]byte{fileA, bufB.Bytes()})
	assertVariantFile(t, data, []*variant.Value{vptr(v1), vptr(v2)}, "extra column")
}

// TestMergeRowGroupsVariantNestedOptional merges files whose variant column
// sits below an optional group. The columnar fast path cannot express the
// distinction between "the enclosing group is null" and "the variant column
// is null" (both are missing rows to VariantReader), so these schemas must
// take the row-based path, which carries the levels through unchanged.
func TestMergeRowGroupsVariantNestedOptional(t *testing.T) {
	type inner struct {
		Var rawVariant `parquet:"var,optional,variant"`
	}
	type row struct {
		ID   int32  `parquet:"id"`
		Meta *inner `parquet:"meta,optional"`
	}
	build := func(shred parquet.Node) []byte {
		variantNode := parquet.Variant()
		if shred != nil {
			n, err := parquet.ShreddedVariant(shred)
			if err != nil {
				t.Fatal(err)
			}
			variantNode = n
		}
		schema := parquet.NewSchema("table", parquet.Group{
			"id": parquet.Int(32),
			"meta": parquet.Optional(parquet.Group{
				"var": parquet.Optional(variantNode),
			}),
		})
		rows := []row{
			{ID: 0, Meta: nil},      // enclosing group null
			{ID: 1, Meta: &inner{}}, // group present, variant null
			{ID: 2, Meta: &inner{Var: encodeRawVariant(variant.Int64(7))}},
		}
		buf := new(bytes.Buffer)
		w := parquet.NewGenericWriter[row](buf, schema)
		if _, err := w.Write(rows); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		return buf.Bytes()
	}

	data := mergeVariantFiles(t, [][]byte{
		build(parquet.Group{"a": parquet.Int(64)}),
		build(nil),
	})
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	r := parquet.NewGenericReader[row](f)
	defer r.Close()
	got := make([]row, 6)
	if n, _ := r.Read(got); n != 6 {
		t.Fatalf("read %d rows, want 6", n)
	}
	for i, g := range got {
		if wantNull := i%3 == 0; (g.Meta == nil) != wantNull {
			t.Errorf("row %d: meta==nil is %v, want %v", i, g.Meta == nil, wantNull)
		}
	}
	for _, i := range []int{2, 5} {
		m, err := variant.DecodeMetadata(got[i].Meta.Var.Metadata)
		if err != nil {
			t.Fatalf("row %d: metadata: %v", i, err)
		}
		val, err := variant.Decode(m, got[i].Meta.Var.Value)
		if err != nil {
			t.Fatalf("row %d: value: %v", i, err)
		}
		if !val.Equal(variant.Int64(7)) {
			t.Errorf("row %d: got %#v, want 7", i, val.GoValue())
		}
	}
}

// TestMergeRowGroupsVariantEmptyBytes merges a file holding a zero
// rawVariant: the plain unshredded write path stores it as present with
// empty metadata and value bytes, which both the re-shredding conversion
// and the columnar reader must read as variant null rather than erroring.
func TestMergeRowGroupsVariantEmptyBytes(t *testing.T) {
	type row struct {
		ID  int32      `parquet:"id"`
		Var rawVariant `parquet:"var,optional,variant"`
	}
	files := [][]byte{
		// A zero rawVariant behind an optional tag writes the variant group
		// as present with empty metadata and value byte arrays.
		func() []byte {
			schema := parquet.NewSchema("table", parquet.Group{
				"id":  parquet.Int(32),
				"var": parquet.Optional(parquet.Variant()),
			})
			buf := new(bytes.Buffer)
			w := parquet.NewGenericWriter[row](buf, schema)
			if _, err := w.Write([]row{{ID: 0}, {ID: 1, Var: encodeRawVariant(variant.Int64(3))}}); err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			return buf.Bytes()
		}(),
		buildVariantFile(t, parquet.Group{"a": parquet.Int(64)}, []*variant.Value{
			vptr(variant.MakeObject([]variant.Field{{Name: "a", Value: variant.Int64(4)}})),
		}),
	}
	data := mergeVariantFiles(t, files)
	assertVariantFile(t, data, []*variant.Value{
		vptr(variant.Null()), // the empty bytes read as variant null
		vptr(variant.Int64(3)),
		vptr(variant.MakeObject([]variant.Field{{Name: "a", Value: variant.Int64(4)}})),
	}, "empty bytes")
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
	mkFile := func(shred parquet.Group, numRows int) []byte {
		ids := make([]int32, numRows)
		values := make([]*variant.Value, numRows)
		for i := range values {
			ids[i] = int32(i)
			values[i] = vptr(variant.MakeObject([]variant.Field{
				{Name: "name", Value: variant.String("alice")},
				{Name: "age", Value: variant.Int64(int64(i))},
				{Name: "extra", Value: variant.Double(1.5)},
			}))
		}
		return buildVariantIDFile(b, shred, ids, values)
	}
	target := variantTestSchema(b, parquet.Group{"name": parquet.String(), "age": parquet.Int(64)})

	for _, numRows := range []int{2000, 50000} {
		files := [][]byte{
			mkFile(parquet.Group{"name": parquet.String(), "age": parquet.Int(64)}, numRows),
			mkFile(parquet.Group{"name": parquet.String()}, numRows),
		}

		b.Run(fmt.Sprintf("columnar/rows=%d", 2*numRows), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				merged, err := parquet.MergeRowGroups(openRowGroups(b, files...), target)
				if err != nil {
					b.Fatal(err)
				}
				w := parquet.NewWriter(io.Discard, target)
				if _, err := w.WriteRowGroup(merged); err != nil {
					b.Fatal(err)
				}
				if err := w.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run(fmt.Sprintf("rows/rows=%d", 2*numRows), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				merged, err := parquet.MergeRowGroups(openRowGroups(b, files...), target)
				if err != nil {
					b.Fatal(err)
				}
				w := parquet.NewWriter(io.Discard, target)
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
}
