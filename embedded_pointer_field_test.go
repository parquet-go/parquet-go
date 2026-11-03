package parquet_test

import (
	"bytes"
	"testing"

	"github.com/parquet-go/parquet-go"
)

type embeddedPointerFields struct {
	Count *int64  `parquet:"count,optional"`
	Name  *string `parquet:"name,optional"`
	Flag  *bool   `parquet:"flag,optional"`
}

type rowWithEmbeddedPointerFields struct {
	ID int64 `parquet:"id"`
	embeddedPointerFields
}

// A promoted pointer field must be written like a direct one: nil is NULL and
// a pointer to a zero value is that value.
func TestDeconstructEmbeddedPointerFields(t *testing.T) {
	zero, empty, no := int64(0), "", false
	rows := []rowWithEmbeddedPointerFields{
		{ID: 1, embeddedPointerFields: embeddedPointerFields{Count: &zero, Name: &empty, Flag: &no}},
		{ID: 2},
	}
	schema := parquet.SchemaOf(rows[0])

	for _, tc := range []struct {
		name  string
		write func(t *testing.T) []byte
	}{
		{"Deconstruct", func(t *testing.T) []byte {
			buf := new(bytes.Buffer)
			w := parquet.NewWriter(buf, schema)
			for i := range rows {
				if _, err := w.WriteRows([]parquet.Row{schema.Deconstruct(nil, &rows[i])}); err != nil {
					t.Fatal(err)
				}
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			return buf.Bytes()
		}},
		{"GenericWriter", func(t *testing.T) []byte {
			buf := new(bytes.Buffer)
			w := parquet.NewGenericWriter[rowWithEmbeddedPointerFields](buf)
			if _, err := w.Write(rows); err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			return buf.Bytes()
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := tc.write(t)
			r := parquet.NewReader(bytes.NewReader(data))
			got := make([]parquet.Row, 2)
			if n, _ := r.ReadRows(got); n != 2 {
				t.Fatalf("read %d rows, want 2", n)
			}
			// Columns: id, count, name, flag.
			for col := 1; col < 4; col++ {
				if v := got[0][col]; v.IsNull() {
					t.Errorf("row 1 column %d: a pointer to a zero value was written as NULL", col)
				}
				if v := got[1][col]; !v.IsNull() {
					t.Errorf("row 2 column %d: a nil pointer was written as %v, want NULL", col, v)
				}
			}
		})
	}
	if rows[1].Count != nil || rows[1].Name != nil || rows[1].Flag != nil {
		t.Errorf("writing mutated the input: %+v", rows[1].embeddedPointerFields)
	}
}
