package parquet

import (
	"bytes"
	"testing"
)

// TestVariantCopyPlan pins down when the columnar fast path of
// Writer.WriteRowGroup applies: schemas must match outside variant groups,
// and at least one variant column must need re-shredding.
func TestVariantCopyPlan(t *testing.T) {
	shred := func(g Group) Node {
		n, err := ShreddedVariant(g)
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	table := func(variantNode Node) *Schema {
		return NewSchema("table", Group{
			"id":  Int(32),
			"var": Optional(variantNode),
		})
	}
	shreddedA := table(shred(Group{"a": Int(64)}))
	shreddedB := table(shred(Group{"a": String()}))
	unshredded := table(Variant())

	t.Run("reshred", func(t *testing.T) {
		plan, ok := makeVariantCopyPlan(shreddedA, shreddedB)
		if !ok || !plan.needsReshred {
			t.Fatalf("ok=%v plan=%+v, want a re-shredding plan", ok, plan)
		}
		if len(plan.variantPaths) != 1 || len(plan.columnPairs) != 1 {
			t.Fatalf("plan=%+v, want one variant path and one column pair", plan)
		}
	})

	t.Run("same shredding does not reshred", func(t *testing.T) {
		plan, ok := makeVariantCopyPlan(shreddedA, shreddedA)
		if !ok || plan.needsReshred {
			t.Fatalf("ok=%v needsReshred=%v, want plan without re-shredding", ok, plan.needsReshred)
		}
	})

	t.Run("unshredded target", func(t *testing.T) {
		plan, ok := makeVariantCopyPlan(unshredded, shreddedA)
		if !ok || !plan.needsReshred {
			t.Fatalf("ok=%v, want a re-shredding plan", ok)
		}
	})

	t.Run("no variant column", func(t *testing.T) {
		s := NewSchema("table", Group{"id": Int(32)})
		if _, ok := makeVariantCopyPlan(s, s); ok {
			t.Fatal("plans without variant columns should not take the fast path")
		}
	})

	t.Run("non-variant columns differ", func(t *testing.T) {
		other := NewSchema("table", Group{
			"id":  Int(64), // different type
			"var": Optional(shred(Group{"a": Int(64)})),
		})
		if _, ok := makeVariantCopyPlan(shreddedA, other); ok {
			t.Fatal("differing non-variant columns should reject the fast path")
		}
	})

	t.Run("variant under repeated", func(t *testing.T) {
		s := NewSchema("table", Group{
			"events": Repeated(Group{"var": Optional(Variant())}),
		})
		if _, ok := makeVariantCopyPlan(s, s); ok {
			t.Fatal("variant under a repeated field should reject the fast path")
		}
	})

	t.Run("variant under optional group", func(t *testing.T) {
		// An optional ancestor's null rows are indistinguishable from
		// variant-null rows in the columnar readers, so the fast path must
		// leave these schemas to the row-based copy.
		s := NewSchema("table", Group{
			"meta": Optional(Group{"var": Optional(Variant())}),
		})
		if _, ok := makeVariantCopyPlan(s, s); ok {
			t.Fatal("variant under an optional group should reject the fast path")
		}
	})

	t.Run("variant under required group", func(t *testing.T) {
		s := func(v Node) *Schema {
			return NewSchema("table", Group{
				"meta": Group{"var": Optional(v)},
			})
		}
		plan, ok := makeVariantCopyPlan(s(Variant()), s(shred(Group{"a": Int(64)})))
		if !ok || !plan.needsReshred {
			t.Fatalf("ok=%v, want a re-shredding plan for a variant under a required group", ok)
		}
	})
}

// TestVariantCopySources checks the decomposition of merged row groups into
// copy sources.
func TestVariantCopySources(t *testing.T) {
	schema := NewSchema("table", Group{
		"id":  Int(32),
		"var": Optional(Variant()),
	})
	mkFile := func() RowGroup {
		buf := new(bytes.Buffer)
		w := NewGenericWriter[map[string]any](buf, schema)
		if _, err := w.Write([]map[string]any{
			{"id": int32(0), "var": map[string]any{"a": int64(1)}},
		}); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		f, err := OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		if err != nil {
			t.Fatal(err)
		}
		return f.RowGroups()[0]
	}
	a, b := mkFile(), mkFile()

	merged, err := MergeRowGroups([]RowGroup{a, b})
	if err != nil {
		t.Fatal(err)
	}
	sources, ok := variantCopySourcesOf(merged, nil)
	if !ok || len(sources) != 2 {
		t.Fatalf("ok=%v sources=%d, want 2 sources from an unsorted merge", ok, len(sources))
	}

	sorted, err := MergeRowGroups([]RowGroup{a, b},
		SortingRowGroupConfig(SortingColumns(Ascending("id"))))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := variantCopySourcesOf(sorted, nil); ok {
		t.Fatal("sorted merges must not take the columnar fast path")
	}
}
