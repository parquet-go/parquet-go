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

func writeZstdFile(t *testing.T) *bytes.Reader {
	t.Helper()
	type row struct {
		ID int64 `parquet:"id"`
	}
	buf := new(bytes.Buffer)
	w := parquet.NewGenericWriter[row](buf, parquet.Compression(&parquet.Zstd))
	if _, err := w.Write([]row{{1}, {2}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return bytes.NewReader(buf.Bytes())
}

func TestFileCompressionCodecsReaderFileView(t *testing.T) {
	data := writeZstdFile(t)
	own := &countingCodec{Codec: &zstd.Codec{}}
	f, err := parquet.OpenFile(data, data.Size(), parquet.FileCompressionCodecs(own))
	if err != nil {
		t.Fatal(err)
	}
	r := parquet.NewReader(f)
	defer r.Close()
	view := r.File()
	for _, c := range view.Root().Columns() {
		if c.Compression() != own {
			t.Errorf("Reader.File().Root() column %s: codec %v, want the file's own zstd codec", c.Name(), c.Compression())
		}
	}

	// The row groups of the view read their pages with the file's codecs.
	pages := view.RowGroups()[0].ColumnChunks()[0].Pages()
	defer pages.Close()
	if _, err := pages.ReadPage(); err != nil {
		t.Fatal(err)
	}
	if own.decodes == 0 {
		t.Error("Reader.File().RowGroups(): the file's own zstd codec decoded no page")
	}
}

func TestFileCompressionCodecsCopiesTheSlice(t *testing.T) {
	own := &zstd.Codec{}
	codecs := []compress.Codec{own}
	opt := parquet.FileCompressionCodecs(codecs...)
	codecs[0] = &parquet.Snappy

	config := parquet.DefaultFileConfig()
	config.Apply(opt)
	if len(config.CompressionCodecs) != 1 || config.CompressionCodecs[0] != own {
		t.Errorf("codecs %v, want the codecs given to the option", config.CompressionCodecs)
	}
}

func TestFileCompressionCodecsWithoutCodecs(t *testing.T) {
	own := &zstd.Codec{}

	// Through Apply.
	applied := parquet.DefaultFileConfig()
	applied.Apply(parquet.FileCompressionCodecs(own), parquet.FileCompressionCodecs())
	if applied.CompressionCodecs != nil {
		t.Errorf("Apply: codecs %v, want none", applied.CompressionCodecs)
	}

	// Through ConfigureFile.
	configured := parquet.DefaultFileConfig()
	parquet.FileCompressionCodecs(own).ConfigureFile(configured)
	parquet.FileCompressionCodecs().ConfigureFile(configured)
	if configured.CompressionCodecs != nil {
		t.Errorf("ConfigureFile: codecs %v, want none", configured.CompressionCodecs)
	}

	// A file opened with the option and no codecs reads with the defaults.
	data := writeZstdFile(t)
	f, err := parquet.OpenFile(data, data.Size(), parquet.FileCompressionCodecs())
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range f.Root().Columns() {
		if c.Compression() != &parquet.Zstd {
			t.Errorf("column %s: codec %v, want parquet.Zstd", c.Name(), c.Compression())
		}
	}
}
