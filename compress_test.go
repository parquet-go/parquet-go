package parquet_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
	"github.com/parquet-go/parquet-go/compress/zstd"
)

type countingCodec struct {
	compress.Codec
	decodes int
}

func (c *countingCodec) Decode(dst, src []byte) ([]byte, error) {
	c.decodes++
	return c.Codec.Decode(dst, src)
}

func TestFileCompressionCodecs(t *testing.T) {
	type row struct {
		ID   int64  `parquet:"id"`
		Name string `parquet:"name"`
	}
	buf := new(bytes.Buffer)
	w := parquet.NewGenericWriter[row](buf, parquet.Compression(&parquet.Zstd))
	if _, err := w.Write([]row{{1, "a"}, {2, "b"}, {3, "c"}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	data := bytes.NewReader(buf.Bytes())

	own := &countingCodec{Codec: &zstd.Codec{}}
	snappy := &countingCodec{Codec: &parquet.Snappy}
	f, err := parquet.OpenFile(data, data.Size(), parquet.FileCompressionCodecs(snappy, own))
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range f.Root().Columns() {
		if c.Compression() != own {
			t.Errorf("column %s: codec %v, want the file's own zstd codec", c.Name(), c.Compression())
		}
	}
	r := parquet.NewGenericReader[row](f)
	rows := make([]row, 3)
	if n, err := r.Read(rows); n != 3 || (err != nil && err != io.EOF) {
		t.Fatalf("Read: got %d, %v", n, err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if own.decodes == 0 {
		t.Error("the file's own zstd codec decoded no page")
	}
	if snappy.decodes != 0 {
		t.Errorf("the snappy codec decoded %d pages of a zstd file", snappy.decodes)
	}

	// A file opened without the option keeps the shared codec.
	g, err := parquet.OpenFile(data, data.Size())
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range g.Root().Columns() {
		if c.Compression() != &parquet.Zstd {
			t.Errorf("column %s: codec %v, want parquet.Zstd", c.Name(), c.Compression())
		}
	}
}
