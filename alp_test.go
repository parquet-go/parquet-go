package parquet_test

import (
	"bytes"
	"math"
	"os"
	"slices"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
)

func TestReadALP(t *testing.T) {
	// https://github.com/apache/parquet-testing/blob/3acde70509f990c4ddfe34bf3ab05890657e766c/data/alp_extended.zstd.parquet
	// The PLAIN and ALP columns in this file contain identical values.
	type row struct {
		FloatPlain  *float32 `parquet:"float_plain"`
		DoublePlain *float64 `parquet:"double_plain"`
		Float32     *float32 `parquet:"float_alp_32"`
		Double32    *float64 `parquet:"double_alp_32"`
		Float1024   *float32 `parquet:"float_alp_1024"`
		Double1024  *float64 `parquet:"double_alp_1024"`
		Float4096   *float32 `parquet:"float_alp_4096"`
		Double4096  *float64 `parquet:"double_alp_4096"`
	}
	rows, err := parquet.ReadFile[row]("testdata/alp_extended.zstd.parquet")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 9032 {
		t.Fatalf("read %d rows from the ALP reference file, want 9032", len(rows))
	}
	for n, r := range rows {
		for i, f := range [...]*float32{r.Float32, r.Float1024, r.Float4096} {
			if (f == nil) != (r.FloatPlain == nil) || (f != nil && math.Float32bits(*f) != math.Float32bits(*r.FloatPlain)) {
				t.Fatalf("float mismatch on row %d, ALP column %d", n, i)
			}
		}
		for i, f := range [...]*float64{r.Double32, r.Double1024, r.Double4096} {
			if (f == nil) != (r.DoublePlain == nil) || (f != nil && math.Float64bits(*f) != math.Float64bits(*r.DoublePlain)) {
				t.Fatalf("double mismatch on row %d, ALP column %d", n, i)
			}
		}
	}
}

func TestWriteRowsWithALPFileSchema(t *testing.T) {
	data, err := os.ReadFile("testdata/alp_extended.zstd.parquet")
	if err != nil {
		t.Fatal(err)
	}
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	column := f.Root().Column("float_alp_32")
	if column == nil || column.Encoding().Encoding() != format.ALP {
		t.Fatal("expected an ALP column in the reference schema")
	}
	schema := parquet.NewSchema("data", parquet.Group{"value": column})
	type row struct {
		Value float32 `parquet:"value"`
	}
	want := []row{{Value: 1.25}, {Value: -0.5}}
	var output bytes.Buffer
	if err := parquet.Write(&output, want, schema); err != nil {
		t.Fatal(err)
	}
	rewritten, err := parquet.OpenFile(bytes.NewReader(output.Bytes()), int64(output.Len()))
	if err != nil {
		t.Fatal(err)
	}
	if enc := rewritten.Root().Column("value").Encoding(); enc != &parquet.ALP {
		t.Fatalf("rewritten column uses %v, want ALP", enc)
	}
	got, err := parquet.Read[row](bytes.NewReader(output.Bytes()), int64(output.Len()))
	if err != nil || !slices.Equal(got, want) {
		t.Fatalf("rewriting an ALP column: got %v, %v", got, err)
	}
}

func TestOptInALPWrite(t *testing.T) {
	type row struct {
		F float32
		D float64
	}
	rows := make([]row, 2049)
	for i := range rows {
		d := float64(i-1024) / 100
		rows[i] = row{float32(d), d}
	}
	rows[0] = row{math.Float32frombits(0xffc00042), math.Copysign(0, -1)}
	rows[1024] = row{math.SmallestNonzeroFloat32, math.Float64frombits(0x7ff0000000000042)}
	rows[2048] = row{float32(math.Inf(1)), math.Inf(-1)}
	var output bytes.Buffer
	if err := parquet.Write(&output, rows, parquet.DefaultEncodingFor(parquet.Float, &parquet.ALP), parquet.DefaultEncodingFor(parquet.Double, &parquet.ALP)); err != nil {
		t.Fatal(err)
	}
	f, err := parquet.OpenFile(bytes.NewReader(output.Bytes()), int64(output.Len()))
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"F", "D"} {
		if f.Root().Column(name).Encoding().Encoding() != format.ALP {
			t.Fatalf("expected ALP for %s", name)
		}
	}
	got, err := parquet.Read[row](bytes.NewReader(output.Bytes()), int64(output.Len()))
	if err != nil || len(got) != len(rows) {
		t.Fatalf("read %d rows: %v", len(got), err)
	}
	for i, r := range got {
		if math.Float32bits(r.F) != math.Float32bits(rows[i].F) || math.Float64bits(r.D) != math.Float64bits(rows[i].D) {
			t.Fatalf("row %d: bits differ", i)
		}
	}
}
