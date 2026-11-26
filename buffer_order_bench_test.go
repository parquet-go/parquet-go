package parquet

import "testing"

func BenchmarkBufferLenWithSortingColumns(b *testing.B) {
	benchConfiguredBufferLen(b, false)
}

func BenchmarkBufferNumRowsWithSortingColumns(b *testing.B) {
	benchConfiguredBufferLen(b, true)
}

func benchConfiguredBufferLen(b *testing.B, numRows bool) {
	schema := SchemaOf(orderRow{})
	buf := NewBuffer(schema, SortingRowGroupConfig(SortingColumns(Ascending("id"))))
	rows := make([]Row, 128)
	for i := range rows {
		row := orderRow{ID: int64(i)}
		rows[i] = schema.Deconstruct(nil, &row)
	}
	if _, err := buf.WriteRows(rows); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	if numRows {
		for b.Loop() {
			_ = buf.NumRows()
		}
		return
	}
	for b.Loop() {
		_ = buf.Len()
	}
}

func BenchmarkGenericBufferLenWithSortingColumns(b *testing.B) {
	buf := NewGenericBuffer[orderRow](SortingRowGroupConfig(SortingColumns(Ascending("id"))))
	rows := make([]orderRow, 128)
	for i := range rows {
		rows[i].ID = int64(i)
	}
	if _, err := buf.Write(rows); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for b.Loop() {
		_ = buf.Len()
	}
}

func BenchmarkRowBufferLenWithSortingColumns(b *testing.B) {
	buf := NewRowBuffer[orderRow](SortingRowGroupConfig(SortingColumns(Ascending("id"))))
	rows := make([]orderRow, 128)
	for i := range rows {
		rows[i].ID = int64(i)
	}
	if _, err := buf.Write(rows); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for b.Loop() {
		_ = buf.Len()
	}
}
