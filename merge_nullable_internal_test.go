package parquet

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
	"testing"

	"github.com/parquet-go/parquet-go/encoding/thrift"
	"slices"
)

func nullableMergeFile[T any](t *testing.T, rows []T, columns []SortingColumn, version int, omitNullCounts bool) *File {
	t.Helper()
	var data bytes.Buffer
	pageSize := 4096
	if version == 1 {
		pageSize = 1
	}
	writer := NewGenericWriter[T](&data, PageBufferSize(pageSize), DataPageVersion(version), SortingWriterConfig(SortingColumns(columns...)))
	for _, row := range rows {
		if _, err := writer.Write([]T{row}); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	file, err := OpenFile(bytes.NewReader(data.Bytes()), int64(data.Len()))
	if err != nil {
		t.Fatal(err)
	}
	if version == 1 && len(rows) > 1 && len(file.ColumnIndexes()[0].NullPages) < 2 {
		t.Fatal("expected multiple data pages")
	}
	if omitNullCounts {
		// NullCounts is optional in Parquet; omit it from the serialized index.
		original := data.Bytes()
		footerStart := len(original) - 8 - int(binary.LittleEndian.Uint32(original[len(original)-8:]))
		body := slices.Clone(original[:footerStart])
		var protocol thrift.CompactProtocol
		for i := range file.metadata.RowGroups[0].Columns {
			chunk := &file.metadata.RowGroups[0].Columns[i]
			index := file.columnIndexes[i]
			index.NullCounts = nil
			encoded, err := thrift.Marshal(&protocol, &index)
			if err != nil {
				t.Fatal(err)
			}
			copy(body[chunk.ColumnIndexOffset:], encoded)
			chunk.ColumnIndexLength = int32(len(encoded))
		}
		footer, err := thrift.Marshal(&protocol, &file.metadata)
		if err != nil {
			t.Fatal(err)
		}
		body = append(body, footer...)
		body = binary.LittleEndian.AppendUint32(body, uint32(len(footer)))
		body = append(body, "PAR1"...)
		file, err = OpenFile(bytes.NewReader(body), int64(len(body)))
		if err != nil {
			t.Fatal(err)
		}
		for _, index := range file.ColumnIndexes() {
			if index.NullCounts != nil {
				t.Fatal("null counts were not omitted")
			}
		}
	}
	got, err := Read[T](file, file.Size())
	if err != nil || !reflect.DeepEqual(got, rows) {
		t.Fatalf("source roundtrip: got %v, want %v: %v", got, rows, err)
	}
	return file
}

func checkNullableMerge[T any](t *testing.T, groups []RowGroup, columns []SortingColumn, want []T) {
	t.Helper()
	merged, err := MergeRowGroups(groups, SortingRowGroupConfig(SortingColumns(columns...)))
	if err != nil {
		t.Fatal(err)
	}
	reader := NewGenericRowGroupReader[T](merged)
	defer reader.Close()
	got := make([]T, len(want))
	if n, err := reader.Read(got); n != len(want) {
		t.Fatalf("read %d rows: %v", n, err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestMergeRowGroupsNullableBounds(t *testing.T) {
	type record struct{ Key *int64 }
	row := func(value int64) record { return record{&value} }
	for _, version := range []int{1, 2} {
		for _, omit := range []bool{false, true} {
			for _, descending := range []bool{false, true} {
				for _, nullsFirst := range []bool{false, true} {
					t.Run(fmt.Sprintf("v%d/omit=%t/descending=%t/nullsFirst=%t", version, omit, descending, nullsFirst), func(t *testing.T) {
						col := Ascending("Key")
						a, b, want := []record{row(1)}, []record{row(5), row(6)}, []record{row(1), row(5), row(6)}
						if descending {
							col = Descending("Key")
							a, b, want = []record{row(10)}, []record{row(6), row(5)}, []record{row(10), row(6), row(5)}
						}
						if nullsFirst {
							if descending {
								a, want = []record{row(1)}, []record{row(6), row(5), row(1)}
							} else {
								a, want = []record{row(10)}, []record{row(5), row(6), row(10)}
							}
							col = NullsFirst(col)
							a = append([]record{{}}, a...)
							want = append([]record{{}}, want...)
						} else {
							a = append(a, record{})
							want = append(want, record{})
						}
						columns := []SortingColumn{col}
						fa := nullableMergeFile(t, a, columns, version, omit)
						fb := nullableMergeFile(t, b, columns, version, omit)
						checkNullableMerge(t, []RowGroup{fa.RowGroups()[0], fb.RowGroups()[0]}, columns, want)
					})
				}
			}
		}
	}
}

func TestMergeRowGroupsNullableSecondaryAndNestedKeys(t *testing.T) {
	type key struct{ Value *int64 }
	type record struct {
		First int64
		Key   *key
	}
	v1, v5, v6 := int64(1), int64(5), int64(6)
	a := []record{{1, nil}, {1, &key{&v1}}}
	b := []record{{1, &key{&v5}}, {1, &key{&v6}}}
	columns := []SortingColumn{Ascending("First"), NullsFirst(Ascending("Key", "Value"))}
	fa, fb := nullableMergeFile(t, a, columns, 2, false), nullableMergeFile(t, b, columns, 2, false)
	checkNullableMerge(t, []RowGroup{fa.RowGroups()[0], fb.RowGroups()[0]}, columns, append(a, b...))
}

func TestMergeRowGroupsNoNullBounds(t *testing.T) {
	t.Run("optional", func(t *testing.T) {
		type record struct{ Key *int64 }
		one, two := int64(1), int64(2)
		columns := []SortingColumn{Ascending("Key")}
		for _, omit := range []bool{false, true} {
			fa := nullableMergeFile(t, []record{{&one}}, columns, 2, omit)
			fb := nullableMergeFile(t, []record{{&two}}, columns, 2, omit)
			groups := []RowGroup{fa.RowGroups()[0], fb.RowGroups()[0]}
			segments := 0
			for range overlappingRowGroups(groups, fa.Schema(), columns, compareRowsFuncOf(fa.Schema(), columns)) {
				segments++
			}
			want := 2
			if omit {
				want = 1
			}
			if segments != want {
				t.Fatalf("omit=%t: got %d segments, want %d", omit, segments, want)
			}
			checkNullableMerge(t, groups, columns, []record{{&one}, {&two}})
		}
	})
	t.Run("required", func(t *testing.T) {
		type record struct{ Key int64 }
		columns := []SortingColumn{Ascending("Key")}
		fa := nullableMergeFile(t, []record{{1}}, columns, 2, true)
		fb := nullableMergeFile(t, []record{{2}}, columns, 2, true)
		segments := 0
		for range overlappingRowGroups([]RowGroup{fa.RowGroups()[0], fb.RowGroups()[0]}, fa.Schema(), columns, compareRowsFuncOf(fa.Schema(), columns)) {
			segments++
		}
		if segments != 2 {
			t.Fatalf("got %d segments, want 2", segments)
		}
	})
}

func TestSortingWriterNullableBounds(t *testing.T) {
	type record struct{ Key *int64 }
	v10, v5, v6 := int64(10), int64(5), int64(6)
	var data bytes.Buffer
	writer := NewSortingWriter[record](&data, 2, SortingWriterConfig(SortingColumns(NullsFirst(Ascending("Key")))))
	if _, err := writer.Write([]record{{nil}, {&v10}, {&v5}, {&v6}}); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := Read[record](bytes.NewReader(data.Bytes()), int64(data.Len()))
	if err != nil {
		t.Fatal(err)
	}
	want := []record{{nil}, {&v5}, {&v6}, {&v10}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}
