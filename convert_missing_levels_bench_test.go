package parquet_test

import (
	"io"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
)

// BenchmarkConvertRowGroupMissingFlatColumn reads a converted row group whose
// target adds a flat column that is not in the source. The unread source column
// is large so the cost of using it only as a level template shows up.
func BenchmarkConvertRowGroupMissingFlatColumn(b *testing.B) {
	type Source struct {
		Alpha string `parquet:"alpha"`
		Kept  int64  `parquet:"kept"`
	}
	type Target struct {
		Kept  int64  `parquet:"kept"`
		Extra string `parquet:"extra"`
	}

	const n = 8192
	src := make([]Source, n)
	payload := strings.Repeat("x", 1024)
	for i := range src {
		src[i].Alpha = payload
		src[i].Kept = int64(i)
	}
	buf := parquet.NewGenericBuffer[Source]()
	if _, err := buf.Write(src); err != nil {
		b.Fatal(err)
	}
	conv, err := parquet.Convert(parquet.SchemaOf(new(Target)), buf.Schema())
	if err != nil {
		b.Fatal(err)
	}

	out := make([]parquet.Row, 256)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		rows := parquet.ConvertRowGroup(buf, conv).Rows()
		for {
			n, err := rows.ReadRows(out)
			if n == 0 || err == io.EOF {
				break
			}
			if err != nil {
				rows.Close()
				b.Fatal(err)
			}
		}
		rows.Close()
	}
}
