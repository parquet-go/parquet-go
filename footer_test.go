package parquet_test

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"
)

// footerTestFiles returns the paths of testdata files that open successfully
// with the default options.
func footerTestFiles(t *testing.T) []string {
	paths, err := filepath.Glob("testdata/*.parquet")
	if err != nil {
		t.Fatal(err)
	}
	files := make([]string, 0, len(paths))
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		if _, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data))); err != nil {
			continue
		}
		files = append(files, path)
	}
	return files
}

func marshalMetadata(t *testing.T, metadata *format.FileMetaData) []byte {
	t.Helper()
	b, err := thrift.Marshal(new(thrift.CompactProtocol), metadata)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// assertFilesEquivalent checks that two open files expose the same schema,
// metadata, row groups, and rows.
func assertFilesEquivalent(t *testing.T, base, withFooter *parquet.File) {
	t.Helper()

	if b, w := base.Schema().String(), withFooter.Schema().String(); b != w {
		t.Errorf("schema mismatch:\nbase: %s\nwith footer: %s", b, w)
	}
	if b, w := base.NumRows(), withFooter.NumRows(); b != w {
		t.Errorf("num rows mismatch: %d != %d", b, w)
	}
	if b, w := marshalMetadata(t, base.Metadata()), marshalMetadata(t, withFooter.Metadata()); !bytes.Equal(b, w) {
		t.Error("metadata mismatch after re-encoding")
	}
	if b, w := len(base.RowGroups()), len(withFooter.RowGroups()); b != w {
		t.Fatalf("row group count mismatch: %d != %d", b, w)
	}
	for _, kv := range base.Metadata().KeyValueMetadata {
		if v, ok := withFooter.Lookup(kv.Key); !ok || v != kv.Value {
			t.Errorf("lookup mismatch for key %q: %q (%t)", kv.Key, v, ok)
		}
	}

	baseRows := readAllFileRows(t, base)
	withFooterRows := readAllFileRows(t, withFooter)
	if !reflect.DeepEqual(baseRows, withFooterRows) {
		t.Error("rows mismatch between base open and open with footer")
	}
}

func readAllFileRows(t *testing.T, f *parquet.File) []parquet.Row {
	t.Helper()
	var rows []parquet.Row
	for _, rg := range f.RowGroups() {
		rr := rg.Rows()
		buf := make([]parquet.Row, 64)
		for {
			n, err := rr.ReadRows(buf)
			for _, row := range buf[:n] {
				rows = append(rows, row.Clone())
			}
			if err != nil {
				break
			}
		}
		rr.Close()
	}
	return rows
}

// TestOpenFileWithFooter checks that opening a file from a pre-read footer
// is equivalent to a regular open, for every testdata file.
func TestOpenFileWithFooter(t *testing.T) {
	for _, path := range footerTestFiles(t) {
		t.Run(filepath.Base(path), func(t *testing.T) {
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			r := bytes.NewReader(data)
			size := int64(len(data))

			base, err := parquet.OpenFile(r, size)
			if err != nil {
				t.Fatal(err)
			}
			footer, err := parquet.ReadFooter(r, size)
			if err != nil {
				t.Fatal(err)
			}
			withFooter, err := parquet.OpenFile(r, size, parquet.WithFooter(footer))
			if err != nil {
				t.Fatal(err)
			}
			assertFilesEquivalent(t, base, withFooter)
		})
	}
}

// TestDecodeFooter checks that DecodeFooter over cached footer bytes is
// equivalent to ReadFooter over the file.
func TestDecodeFooter(t *testing.T) {
	for _, path := range footerTestFiles(t) {
		t.Run(filepath.Base(path), func(t *testing.T) {
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			size := int64(len(data))
			footerSize := binary.LittleEndian.Uint32(data[size-8 : size-4])
			footerBytes := data[size-int64(footerSize)-8 : size]

			read, err := parquet.ReadFooter(bytes.NewReader(data), size)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := parquet.DecodeFooter(footerBytes)
			if err != nil {
				t.Fatal(err)
			}
			if b, w := marshalMetadata(t, read.Metadata()), marshalMetadata(t, decoded.Metadata()); !bytes.Equal(b, w) {
				t.Error("metadata mismatch between ReadFooter and DecodeFooter")
			}
			if b, w := read.Schema().String(), decoded.Schema().String(); b != w {
				t.Errorf("schema mismatch:\nread: %s\ndecoded: %s", b, w)
			}

			f, err := parquet.OpenFile(bytes.NewReader(data), size, parquet.WithFooter(decoded))
			if err != nil {
				t.Fatal(err)
			}
			if f.NumRows() != decoded.NumRows() {
				t.Errorf("num rows mismatch: %d != %d", f.NumRows(), decoded.NumRows())
			}
		})
	}
}

// TestDecodeFooterRejectsInvalidInput checks the input validation of
// DecodeFooter.
func TestDecodeFooterRejectsInvalidInput(t *testing.T) {
	if _, err := parquet.DecodeFooter([]byte("PAR1")); err == nil {
		t.Error("expected error for truncated input")
	}
	if _, err := parquet.DecodeFooter([]byte("\x00\x00\x00\x00XXXX")); err == nil {
		t.Error("expected error for invalid magic")
	}
	if _, err := parquet.DecodeFooter([]byte("\xff\x00\x00\x00PAR1")); err == nil {
		t.Error("expected error for footer size mismatch")
	}
}

// TestFooterSharedAcrossConcurrentOpens opens many files concurrently from
// one footer and verifies the footer is never mutated. Run with -race to
// catch unsynchronized writes.
func TestFooterSharedAcrossConcurrentOpens(t *testing.T) {
	data, err := os.ReadFile("testdata/alltypes_tiny_pages_plain.parquet")
	if err != nil {
		t.Fatal(err)
	}
	size := int64(len(data))
	footer, err := parquet.ReadFooter(bytes.NewReader(data), size)
	if err != nil {
		t.Fatal(err)
	}
	before := marshalMetadata(t, footer.Metadata())

	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := bytes.NewReader(data)
			f, err := parquet.OpenFile(r, size, parquet.WithFooter(footer))
			if err != nil {
				errs <- err
				return
			}
			_ = footer.Schema() // exercise concurrent lazy schema construction
			rows := f.RowGroups()[0].Rows()
			defer rows.Close()
			buf := make([]parquet.Row, 32)
			if _, err := rows.ReadRows(buf); err != nil && err != io.EOF {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	if after := marshalMetadata(t, footer.Metadata()); !bytes.Equal(before, after) {
		t.Error("footer metadata was mutated by concurrent opens")
	}
}

// TestOpenFileWithFooterPerformsNoReads checks the documented contract of
// WithFooter: combined with SkipPageIndex and SkipBloomFilters, OpenFile
// performs no I/O.
func TestOpenFileWithFooterPerformsNoReads(t *testing.T) {
	data, err := os.ReadFile("testdata/file.parquet")
	if err != nil {
		t.Fatal(err)
	}
	size := int64(len(data))
	footer, err := parquet.ReadFooter(bytes.NewReader(data), size)
	if err != nil {
		t.Fatal(err)
	}

	counting := &countingReaderAt{ra: bytes.NewReader(data)}
	if _, err := parquet.OpenFile(counting, size,
		parquet.WithFooter(footer),
		parquet.SkipPageIndex(true),
		parquet.SkipBloomFilters(true),
	); err != nil {
		t.Fatal(err)
	}
	if counting.reads != 0 {
		t.Errorf("OpenFile with footer performed %d reads, want 0", counting.reads)
	}
}

// TestFooterEncrypted checks ReadFooter/WithFooter against files with
// encrypted footers (PARE) and signed plaintext footers.
func TestFooterEncrypted(t *testing.T) {
	rows := []encryptionTestRow{
		{Name: "alpha", Value: 1},
		{Name: "beta", Value: 2},
		{Name: "gamma", Value: 3},
	}
	keys := &staticKeyRetriever{footerKey: aes128Key(0x11)}

	for _, test := range []struct {
		scenario        string
		encryptedFooter bool
	}{
		{scenario: "encrypted footer", encryptedFooter: true},
		{scenario: "signed plaintext footer", encryptedFooter: false},
	} {
		t.Run(test.scenario, func(t *testing.T) {
			data := writeEncrypted(t, rows, &parquet.EncryptionConfig{
				FooterKey:       keys.footerKey,
				EncryptedFooter: test.encryptedFooter,
			})
			size := int64(len(data))

			// Footer reads of encrypted files require a decryption config.
			if _, err := parquet.ReadFooter(bytes.NewReader(data), size); err == nil {
				t.Error("expected ReadFooter to fail without a DecryptionConfig")
			}

			footer, err := parquet.ReadFooter(bytes.NewReader(data), size, parquet.WithDecryption(keys))
			if err != nil {
				t.Fatal(err)
			}
			f, err := parquet.OpenFile(bytes.NewReader(data), size, parquet.WithFooter(footer))
			if err != nil {
				t.Fatal(err)
			}
			got := readTypedRows(t, f)
			if !reflect.DeepEqual(got, rows) {
				t.Errorf("rows mismatch: got %+v, want %+v", got, rows)
			}

			// DecodeFooter over the raw footer region must work as well.
			footerSize := binary.LittleEndian.Uint32(data[size-8 : size-4])
			footerBytes := data[size-int64(footerSize)-8 : size]
			decoded, err := parquet.DecodeFooter(footerBytes, parquet.WithDecryption(keys))
			if err != nil {
				t.Fatal(err)
			}
			f2, err := parquet.OpenFile(bytes.NewReader(data), size, parquet.WithFooter(decoded))
			if err != nil {
				t.Fatal(err)
			}
			if got := readTypedRows(t, f2); !reflect.DeepEqual(got, rows) {
				t.Errorf("rows mismatch after DecodeFooter: got %+v, want %+v", got, rows)
			}
		})
	}
}

func readTypedRows(t *testing.T, f *parquet.File) []encryptionTestRow {
	t.Helper()
	r := parquet.NewGenericReader[encryptionTestRow](f)
	defer r.Close()
	var out []encryptionTestRow
	buf := make([]encryptionTestRow, 16)
	for {
		n, err := r.Read(buf)
		out = append(out, buf[:n]...)
		if err == io.EOF {
			return out
		}
		if err != nil {
			t.Fatal(err)
		}
	}
}

// TestReadFooterValidation checks that ReadFooter applies the same
// validation as OpenFile.
func TestReadFooterValidation(t *testing.T) {
	data := []byte("not a parquet file")
	if _, err := parquet.ReadFooter(bytes.NewReader(data), int64(len(data))); err == nil {
		t.Error("expected error for invalid file")
	}
}
