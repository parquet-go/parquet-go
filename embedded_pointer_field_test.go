package parquet_test

import (
	"bytes"
	"io"
	"reflect"
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

type promotedPlace struct {
	City string `parquet:"city"`
}

type promotedPointerFields struct {
	Count *int64         `parquet:"count,optional"`
	Name  *string        `parquet:"name,optional"`
	Flag  *bool          `parquet:"flag,optional"`
	Place *promotedPlace `parquet:"place,optional"`
}

type rowWithPromotedPointerFields struct {
	ID int64 `parquet:"id"`
	promotedPointerFields
}

// A promoted pointer field must be written like a direct one: nil is NULL and
// a pointer to a zero value is that value.
func TestDeconstructEmbeddedPointerFields(t *testing.T) {
	schema := parquet.SchemaOf(rowWithEmbeddedPointerFields{})

	for _, tc := range []struct {
		name  string
		write func(t *testing.T, rows []rowWithEmbeddedPointerFields) []byte
	}{
		{
			name: "Deconstruct",
			write: func(t *testing.T, rows []rowWithEmbeddedPointerFields) []byte {
				t.Helper()
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
			},
		},
		{
			name: "GenericWriter",
			write: func(t *testing.T, rows []rowWithEmbeddedPointerFields) []byte {
				t.Helper()
				buf := new(bytes.Buffer)
				w := parquet.NewGenericWriter[rowWithEmbeddedPointerFields](buf)
				if _, err := w.Write(rows); err != nil {
					t.Fatal(err)
				}
				if err := w.Close(); err != nil {
					t.Fatal(err)
				}
				return buf.Bytes()
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Own rows for this case. The other writer must not observe
			// mutations this write leaves behind.
			zero, empty, no := int64(0), "", false
			rows := []rowWithEmbeddedPointerFields{
				{ID: 1, embeddedPointerFields: embeddedPointerFields{Count: &zero, Name: &empty, Flag: &no}},
				{ID: 2},
			}
			data := tc.write(t, rows)
			if rows[1].Count != nil || rows[1].Name != nil || rows[1].Flag != nil {
				t.Errorf("writing mutated the input: %+v", rows[1].embeddedPointerFields)
			}

			r := parquet.NewReader(bytes.NewReader(data))
			t.Cleanup(func() { r.Close() })
			got := make([]parquet.Row, len(rows))
			n, err := r.ReadRows(got)
			if err != nil && err != io.EOF {
				t.Fatalf("ReadRows: %v", err)
			}
			if n != len(rows) {
				t.Fatalf("read %d rows, want %d", n, len(rows))
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
}

// Reading promoted pointers back into Go values keeps nil distinct from a
// pointer to a zero value. That includes a promoted pointer to a struct: nil
// stays nil, and a pointer to a zero struct stays a pointer to that zero value.
func TestRoundTripEmbeddedPointerFields(t *testing.T) {
	tests := []struct {
		name string
		row  func() rowWithPromotedPointerFields
	}{
		{
			name: "nil embedded pointer",
			row: func() rowWithPromotedPointerFields {
				return rowWithPromotedPointerFields{ID: 1}
			},
		},
		{
			name: "pointer to zero value",
			row: func() rowWithPromotedPointerFields {
				zero, empty, no := int64(0), "", false
				return rowWithPromotedPointerFields{
					ID: 2,
					promotedPointerFields: promotedPointerFields{
						Count: &zero,
						Name:  &empty,
						Flag:  &no,
					},
				}
			},
		},
		{
			name: "promoted struct pointer",
			row: func() rowWithPromotedPointerFields {
				place := promotedPlace{}
				return rowWithPromotedPointerFields{
					ID:                    3,
					promotedPointerFields: promotedPointerFields{Place: &place},
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for _, write := range []struct {
				name  string
				write func(t *testing.T, rows []rowWithPromotedPointerFields) []byte
			}{
				{"Deconstruct", writePromotedDeconstruct},
				{"GenericWriter", writePromotedGeneric},
			} {
				t.Run(write.name, func(t *testing.T) {
					want := tc.row()
					rows := []rowWithPromotedPointerFields{tc.row()}
					data := write.write(t, rows)
					if !reflect.DeepEqual(rows[0], want) {
						t.Errorf("writing mutated the input:\nwant %#v\ngot  %#v", want, rows[0])
					}

					schema := parquet.SchemaOf(want)
					r := parquet.NewReader(bytes.NewReader(data))
					t.Cleanup(func() { r.Close() })
					gotRows := make([]parquet.Row, 1)
					n, err := r.ReadRows(gotRows)
					if err != nil && err != io.EOF {
						t.Fatalf("ReadRows: %v", err)
					}
					if n != 1 {
						t.Fatalf("ReadRows returned %d rows, want 1", n)
					}
					var got rowWithPromotedPointerFields
					if err := schema.Reconstruct(&got, gotRows[0]); err != nil {
						t.Fatalf("reconstruct: %v", err)
					}
					if !reflect.DeepEqual(got, want) {
						t.Errorf("round trip:\nwant %#v\ngot  %#v", want, got)
					}
				})
			}
		})
	}
}

func writePromotedDeconstruct(t *testing.T, rows []rowWithPromotedPointerFields) []byte {
	t.Helper()
	schema := parquet.SchemaOf(rows[0])
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
}

func writePromotedGeneric(t *testing.T, rows []rowWithPromotedPointerFields) []byte {
	t.Helper()
	buf := new(bytes.Buffer)
	w := parquet.NewGenericWriter[rowWithPromotedPointerFields](buf)
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}
