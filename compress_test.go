package parquet_test

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
	"github.com/parquet-go/parquet-go/compress/gzip"
	"github.com/parquet-go/parquet-go/compress/zstd"
	"github.com/parquet-go/parquet-go/format"
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

func writeGzipBloomFile(t *testing.T) *bytes.Reader {
	t.Helper()
	type row struct {
		Name string `parquet:"name"`
	}
	buf := new(bytes.Buffer)
	w := parquet.NewGenericWriter[row](buf,
		parquet.BloomFilters(parquet.SplitBlockFilter(10, "name")),
		parquet.BloomFilterCompression(&parquet.Gzip),
	)
	if _, err := w.Write([]row{{Name: "Alice"}, {Name: "Bob"}, {Name: "Charlie"}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return bytes.NewReader(buf.Bytes())
}

func bloomFilterOf(t *testing.T, data *bytes.Reader, opts ...parquet.FileOption) parquet.BloomFilter {
	t.Helper()
	f, err := parquet.OpenFile(data, data.Size(), opts...)
	if err != nil {
		t.Fatal(err)
	}
	bf := f.RowGroups()[0].ColumnChunks()[0].BloomFilter()
	if bf == nil {
		t.Fatal("expected a bloom filter")
	}
	return bf
}

func checkBloomNames(t *testing.T, bf parquet.BloomFilter) {
	t.Helper()
	for _, name := range []string{"Alice", "Bob", "Charlie"} {
		ok, err := bf.Check(parquet.ValueOf(name))
		if err != nil {
			t.Fatalf("Check(%q): %v", name, err)
		}
		if !ok {
			t.Errorf("Check(%q) = false, want true", name)
		}
	}
}

// errGzipCodec reports itself as gzip and refuses to decode. A bloom filter
// read that uses it must surface this error instead of the shared gzip codec.
type errGzipCodec struct{}

func (errGzipCodec) String() string { return "GZIP" }

func (errGzipCodec) CompressionCodec() format.CompressionCodec { return format.Gzip }

func (errGzipCodec) Encode(dst, src []byte) ([]byte, error) {
	return nil, errors.New("custom gzip encode")
}

func (errGzipCodec) Decode(dst, src []byte) ([]byte, error) {
	return nil, errors.New("custom gzip")
}

func TestFileCompressionCodecsBloomFilter(t *testing.T) {
	data := writeGzipBloomFile(t)
	names := func(t *testing.T, bf parquet.BloomFilter, own *countingCodec) {
		t.Helper()
		checkBloomNames(t, bf)
		if own.decodes != 1 {
			t.Errorf("gzip codec decoded %d times, want 1", own.decodes)
		}
	}

	t.Run("on open", func(t *testing.T) {
		own := &countingCodec{Codec: &gzip.Codec{}}
		names(t, bloomFilterOf(t, data, parquet.FileCompressionCodecs(own)), own)
	})

	t.Run("prefetched", func(t *testing.T) {
		own := &countingCodec{Codec: &gzip.Codec{}}
		bf := bloomFilterOf(t, data,
			parquet.PrefetchBloomFilters(true),
			parquet.FileCompressionCodecs(own),
		)
		names(t, bf, own)
	})

	t.Run("loaded later", func(t *testing.T) {
		own := &countingCodec{Codec: &gzip.Codec{}}
		bf := bloomFilterOf(t, data,
			parquet.SkipBloomFilters(true),
			parquet.FileCompressionCodecs(own),
		)
		names(t, bf, own)
	})

	t.Run("reader file view", func(t *testing.T) {
		own := &countingCodec{Codec: &gzip.Codec{}}
		f, err := parquet.OpenFile(data, data.Size(), parquet.FileCompressionCodecs(own))
		if err != nil {
			t.Fatal(err)
		}
		r := parquet.NewReader(f)
		defer r.Close()
		bf := r.File().RowGroups()[0].ColumnChunks()[0].BloomFilter()
		if bf == nil {
			t.Fatal("expected a bloom filter")
		}
		names(t, bf, own)
	})

	t.Run("file config", func(t *testing.T) {
		own := &countingCodec{Codec: &gzip.Codec{}}
		bf := bloomFilterOf(t, data, &parquet.FileConfig{
			CompressionCodecs: []compress.Codec{own},
		})
		names(t, bf, own)
	})

	t.Run("last gzip codec", func(t *testing.T) {
		first := &countingCodec{Codec: &gzip.Codec{}}
		last := &countingCodec{Codec: &gzip.Codec{}}
		bf := bloomFilterOf(t, data, parquet.FileCompressionCodecs(first, last))
		names(t, bf, last)
		if first.decodes != 0 {
			t.Errorf("first gzip codec decoded %d times, want 0", first.decodes)
		}
	})

	// A list with no gzip codec keeps the shared gzip codec.
	t.Run("fallback", func(t *testing.T) {
		snappy := &countingCodec{Codec: &parquet.Snappy}
		bf := bloomFilterOf(t, data, parquet.FileCompressionCodecs(snappy))
		checkBloomNames(t, bf)
		if snappy.decodes != 0 {
			t.Errorf("snappy codec decoded %d times, want 0", snappy.decodes)
		}
	})

	t.Run("no codecs", func(t *testing.T) {
		checkBloomNames(t, bloomFilterOf(t, data))
	})

	t.Run("broken gzip", func(t *testing.T) {
		bf := bloomFilterOf(t, data, parquet.FileCompressionCodecs(errGzipCodec{}))
		_, err := bf.Check(parquet.ValueOf("Alice"))
		if err == nil || !strings.Contains(err.Error(), "custom gzip") {
			t.Fatalf("Check: %v, want the file's gzip codec error", err)
		}
	})
}

func TestFileCompressionCodecsEncryptedBloomFilter(t *testing.T) {
	if !withTracebufLeakCheck(t) {
		return
	}
	footerKey := aes128Key(0xC2)
	cfg := &parquet.EncryptionConfig{
		FooterKey:       footerKey,
		EncryptedFooter: true,
		FileIdentifier:  []byte{3, 3, 3, 3, 4, 4, 4, 4},
	}
	rows := []encryptionTestRow{
		{Name: "alice", Value: 1},
		{Name: "bob", Value: 2},
	}
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[encryptionTestRow](&buf,
		parquet.WithEncryption(cfg),
		parquet.BloomFilters(parquet.SplitBlockFilter(10, "name")),
		parquet.BloomFilterCompression(&parquet.Gzip),
	)
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	data := buf.Bytes()
	own := &countingCodec{Codec: &gzip.Codec{}}
	snappy := &countingCodec{Codec: &parquet.Snappy}
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)),
		parquet.WithDecryption(&staticKeyRetriever{footerKey: footerKey}),
		parquet.FileCompressionCodecs(snappy, own),
	)
	if err != nil {
		t.Fatal(err)
	}
	bf := f.RowGroups()[0].ColumnChunks()[0].BloomFilter()
	if bf == nil {
		t.Fatal("expected a bloom filter")
	}
	if own.decodes != 1 {
		t.Errorf("gzip codec decoded %d times, want 1", own.decodes)
	}
	if snappy.decodes != 0 {
		t.Errorf("snappy codec decoded %d times, want 0", snappy.decodes)
	}
	ok, err := bf.Check(parquet.ValueOf("alice"))
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Error("Check(alice) = false, want true")
	}

	// A failing gzip codec is not replaced by the shared one. The encrypted
	// path drops a filter it cannot decompress.
	broken, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)),
		parquet.WithDecryption(&staticKeyRetriever{footerKey: footerKey}),
		parquet.FileCompressionCodecs(errGzipCodec{}),
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := broken.RowGroups()[0].ColumnChunks()[0].BloomFilter(); got != nil {
		t.Errorf("BloomFilter = %v, want nil when the file's gzip codec fails", got)
	}
}
