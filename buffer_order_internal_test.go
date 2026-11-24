package parquet

import (
	"bytes"
	"io"
	"slices"
	"sort"
	"testing"

	"github.com/parquet-go/parquet-go/format"
)

type orderRow struct {
	ID int64 `parquet:"id"`
}

func TestSortingColumnsFollowEstablishedOrder(t *testing.T) {
	configured := []SortingColumn{Ascending("id")}
	option := SortingRowGroupConfig(SortingColumns(configured...))

	t.Run("generic", func(t *testing.T) {
		buf := NewGenericBuffer[orderRow](option)
		if got := buf.SortingColumns(); !EqualSortingColumns(got, configured) {
			t.Fatalf("empty buffer sorting columns = %v, want %v", got, configured)
		}
		if _, err := buf.Write([]orderRow{{ID: 1}}); err != nil {
			t.Fatal(err)
		}
		if got := buf.SortingColumns(); !EqualSortingColumns(got, configured) {
			t.Fatalf("single row sorting columns = %v, want %v", got, configured)
		}
		if _, err := buf.Write([]orderRow{{ID: 2}, {ID: 0}}); err != nil {
			t.Fatal(err)
		}
		if got := buf.SortingColumns(); len(got) != 0 {
			t.Fatalf("unsorted buffer sorting columns = %v, want none", got)
		}
		if got := writeOrderRowGroup(t, buf); len(got) != 0 {
			t.Fatalf("unsorted WriteRowGroup sorting columns = %+v, want none", got)
		}

		sort.Sort(buf)
		if got := buf.SortingColumns(); !EqualSortingColumns(got, configured) {
			t.Fatalf("sorted buffer sorting columns = %v, want %v", got, configured)
		}
		if got := writeOrderRowGroup(t, buf); !slices.Equal(got, []format.SortingColumn{{ColumnIdx: 0}}) {
			t.Fatalf("sorted WriteRowGroup sorting columns = %+v", got)
		}
		// A second observation must keep the cached order.
		if got := buf.SortingColumns(); !EqualSortingColumns(got, configured) {
			t.Fatalf("sorted buffer sorting columns after read = %v, want %v", got, configured)
		}

		if _, err := buf.Write([]orderRow{{ID: 9}}); err != nil {
			t.Fatal(err)
		}
		if got := buf.SortingColumns(); len(got) != 0 {
			t.Fatalf("sorting columns after append = %v, want none", got)
		}
		if got := writeOrderRowGroup(t, buf); len(got) != 0 {
			t.Fatalf("WriteRowGroup after append sorting columns = %+v, want none", got)
		}

		buf.Reset()
		if got := buf.SortingColumns(); !EqualSortingColumns(got, configured) {
			t.Fatalf("reset buffer sorting columns = %v, want %v", got, configured)
		}
	})

	t.Run("buffer", func(t *testing.T) {
		schema := SchemaOf(orderRow{})
		buf := NewBuffer(schema, option)
		rows := []orderRow{{ID: 2}, {ID: 0}, {ID: 1}}
		written := make([]Row, len(rows))
		for i := range rows {
			written[i] = schema.Deconstruct(nil, &rows[i])
		}
		if _, err := buf.WriteRows(written); err != nil {
			t.Fatal(err)
		}
		if got := buf.SortingColumns(); len(got) != 0 {
			t.Fatalf("unsorted buffer sorting columns = %v, want none", got)
		}
		sort.Stable(buf)
		if got := buf.SortingColumns(); !EqualSortingColumns(got, configured) {
			t.Fatalf("stable-sorted buffer sorting columns = %v, want %v", got, configured)
		}
		buf.Swap(0, 1)
		if got := buf.SortingColumns(); len(got) != 0 {
			t.Fatalf("sorting columns after Swap = %v, want none", got)
		}
	})

	t.Run("rowbuffer", func(t *testing.T) {
		buf := NewRowBuffer[orderRow](option)
		if _, err := buf.Write([]orderRow{{ID: 2}, {ID: 0}}); err != nil {
			t.Fatal(err)
		}
		if got := buf.SortingColumns(); len(got) != 0 {
			t.Fatalf("unsorted row buffer sorting columns = %v, want none", got)
		}
		if got := writeOrderRowGroup(t, buf); len(got) != 0 {
			t.Fatalf("unsorted row buffer WriteRowGroup sorting columns = %+v, want none", got)
		}
		sort.Sort(buf)
		if got := buf.SortingColumns(); !EqualSortingColumns(got, configured) {
			t.Fatalf("sorted row buffer sorting columns = %v, want %v", got, configured)
		}
		if got := writeOrderRowGroup(t, buf); !slices.Equal(got, []format.SortingColumn{{ColumnIdx: 0}}) {
			t.Fatalf("sorted row buffer WriteRowGroup sorting columns = %+v", got)
		}
		if _, err := buf.Write([]orderRow{{ID: 3}}); err != nil {
			t.Fatal(err)
		}
		if got := buf.SortingColumns(); len(got) != 0 {
			t.Fatalf("row buffer sorting columns after append = %v, want none", got)
		}
	})
}

func TestWriteRowGroupSortingColumnsFollowLeaf(t *testing.T) {
	type nested struct {
		Value int `parquet:"value"`
	}
	type row struct {
		Nested nested `parquet:"nested"`
	}
	configured := []SortingColumn{
		Ascending("missing"),
		Ascending("nested"),
		NullsFirst(Descending("nested", "value")),
		Ascending(),
	}
	rows := []row{{Nested: nested{Value: 1}}, {Nested: nested{Value: 2}}, {Nested: nested{Value: 0}}}
	wantLeaf := []format.SortingColumn{{ColumnIdx: 0, Descending: true, NullsFirst: true}}
	option := SortingRowGroupConfig(SortingColumns(configured...))

	t.Run("generic", func(t *testing.T) {
		buf := NewGenericBuffer[row](option)
		if _, err := buf.Write(rows); err != nil {
			t.Fatal(err)
		}
		if got := writeOrderRowGroup(t, buf); len(got) != 0 {
			t.Fatalf("unsorted multi-row sorting columns = %+v, want none", got)
		}
		sort.Sort(buf)
		if got := writeOrderRowGroup(t, buf); !slices.Equal(got, wantLeaf) {
			t.Fatalf("sorted multi-row sorting columns = %+v, want %+v", got, wantLeaf)
		}
	})

	t.Run("buffer", func(t *testing.T) {
		schema := SchemaOf(row{})
		buf := NewBuffer(schema, option)
		written := make([]Row, len(rows))
		for i := range rows {
			written[i] = schema.Deconstruct(nil, &rows[i])
		}
		if _, err := buf.WriteRows(written); err != nil {
			t.Fatal(err)
		}
		if got := writeOrderRowGroup(t, buf); len(got) != 0 {
			t.Fatalf("unsorted multi-row sorting columns = %+v, want none", got)
		}
		sort.Sort(buf)
		if got := writeOrderRowGroup(t, buf); !slices.Equal(got, wantLeaf) {
			t.Fatalf("sorted multi-row sorting columns = %+v, want %+v", got, wantLeaf)
		}
	})
}

func TestMergeRowGroupsUsesEstablishedBufferOrder(t *testing.T) {
	option := SortingRowGroupConfig(SortingColumns(Ascending("id")))
	newBuf := func(sorted bool, ids ...int64) *GenericBuffer[orderRow] {
		t.Helper()
		buf := NewGenericBuffer[orderRow](option)
		rows := make([]orderRow, len(ids))
		for i, id := range ids {
			rows[i] = orderRow{ID: id}
		}
		if _, err := buf.Write(rows); err != nil {
			t.Fatal(err)
		}
		if sorted {
			sort.Sort(buf)
		}
		return buf
	}

	t.Run("unsorted", func(t *testing.T) {
		merged, err := MergeRowGroups([]RowGroup{newBuf(false, 4, 1), newBuf(false, 3, 2)})
		if err != nil {
			t.Fatal(err)
		}
		if got := merged.SortingColumns(); len(got) != 0 {
			t.Fatalf("merged sorting columns = %v, want none", got)
		}
		got := readOrderIDs(t, merged)
		want := []int64{4, 1, 3, 2}
		if !slices.Equal(got, want) {
			t.Fatalf("merged ids = %v, want %v", got, want)
		}
	})

	t.Run("sorted", func(t *testing.T) {
		merged, err := MergeRowGroups([]RowGroup{newBuf(true, 1, 4), newBuf(true, 2, 3)})
		if err != nil {
			t.Fatal(err)
		}
		if got := merged.SortingColumns(); !EqualSortingColumns(got, []SortingColumn{Ascending("id")}) {
			t.Fatalf("merged sorting columns = %v, want ascending id", got)
		}
		got := readOrderIDs(t, merged)
		want := []int64{1, 2, 3, 4}
		if !slices.Equal(got, want) {
			t.Fatalf("merged ids = %v, want %v", got, want)
		}
		if cols := writeOrderRowGroup(t, merged); !slices.Equal(cols, []format.SortingColumn{{ColumnIdx: 0}}) {
			t.Fatalf("merged WriteRowGroup sorting columns = %+v", cols)
		}
	})
}

func TestSortingWriterRecordsSortingColumns(t *testing.T) {
	var output bytes.Buffer
	w := NewSortingWriter[orderRow](&output, 10, SortingWriterConfig(SortingColumns(Ascending("id"))))
	if _, err := w.Write([]orderRow{{ID: 3}, {ID: 1}, {ID: 2}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := OpenFile(bytes.NewReader(output.Bytes()), int64(output.Len()))
	if err != nil {
		t.Fatal(err)
	}
	want := []format.SortingColumn{{ColumnIdx: 0}}
	for i, group := range f.Metadata().RowGroups {
		if !slices.Equal(group.SortingColumns, want) {
			t.Fatalf("row group %d sorting columns = %+v, want %+v", i, group.SortingColumns, want)
		}
	}
	if got := readOrderIDs(t, f.RowGroups()[0]); !slices.Equal(got, []int64{1, 2, 3}) {
		t.Fatalf("ids = %v", got)
	}
}

func TestBufferWriteRowGroupKeepsConfiguredSortingCheck(t *testing.T) {
	option := SortingRowGroupConfig(SortingColumns(Ascending("id")))
	sorted := NewGenericBuffer[orderRow](option)
	if _, err := sorted.Write([]orderRow{{ID: 2}, {ID: 0}, {ID: 1}}); err != nil {
		t.Fatal(err)
	}
	sort.Sort(sorted)

	dst := NewGenericBuffer[orderRow](option)
	if _, err := dst.WriteRowGroup(sorted); err != nil {
		t.Fatal(err)
	}
	if got := dst.SortingColumns(); !EqualSortingColumns(got, []SortingColumn{Ascending("id")}) {
		t.Fatalf("buffer filled from sorted row group = %v, want ascending id", got)
	}

	unsorted := NewGenericBuffer[orderRow](option)
	if _, err := unsorted.Write([]orderRow{{ID: 2}, {ID: 0}}); err != nil {
		t.Fatal(err)
	}
	dst.Reset()
	if _, err := dst.WriteRowGroup(unsorted); err != ErrRowGroupSortingColumnsMismatch {
		t.Fatalf("WriteRowGroup(unsorted) = %v, want mismatch", err)
	}

	other := NewGenericBuffer[orderRow](SortingRowGroupConfig(SortingColumns(Descending("id"))))
	if _, err := other.Write([]orderRow{{ID: 1}}); err != nil {
		t.Fatal(err)
	}
	if _, err := dst.WriteRowGroup(other); err != ErrRowGroupSortingColumnsMismatch {
		t.Fatalf("WriteRowGroup(descending) = %v, want mismatch", err)
	}
}

func writeOrderRowGroup(t *testing.T, rowGroup RowGroup) []format.SortingColumn {
	t.Helper()
	var output bytes.Buffer
	w := NewWriter(&output, rowGroup.Schema())
	if _, err := w.WriteRowGroup(rowGroup); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := OpenFile(bytes.NewReader(output.Bytes()), int64(output.Len()))
	if err != nil {
		t.Fatal(err)
	}
	groups := f.Metadata().RowGroups
	if len(groups) != 1 {
		t.Fatalf("row groups = %d, want 1", len(groups))
	}
	return groups[0].SortingColumns
}

func readOrderIDs(t *testing.T, rowGroup RowGroup) []int64 {
	t.Helper()
	rows := make([]orderRow, rowGroup.NumRows())
	n, err := NewGenericRowGroupReader[orderRow](rowGroup).Read(rows)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if n != len(rows) {
		t.Fatalf("read %d rows, want %d", n, len(rows))
	}
	ids := make([]int64, n)
	for i := range rows {
		ids[i] = rows[i].ID
	}
	return ids
}
