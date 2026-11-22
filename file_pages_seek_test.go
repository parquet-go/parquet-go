package parquet_test

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/parquet-go/parquet-go"
)

const seekPageRows = 8

type seekPlainRow struct {
	V int64 `parquet:"v"`
}

type seekDictRow struct {
	V int64 `parquet:"v,dict"`
}

type seekTwoColRow struct {
	Plain int64 `parquet:"plain"`
	Dict  int64 `parquet:"dictcol,dict"`
}

type pageSeekCase struct {
	name  string
	reads int
	seeks []int64
}

// consecutivePageSeeks are the file-page SeekToRow sequences from
// TestFilePagesConsecutiveSeeks, plus a seek back onto the last page.
func consecutivePageSeeks() []pageSeekCase {
	return []pageSeekCase{
		{"next-page", 1, []int64{0, 3}},
		{"skip-page", 1, []int64{0, 5}},
		{"backward", 3, []int64{4, 1}},
		{"away-and-back", 1, []int64{5, 1}},
		{"replace-replay", 1, []int64{0, 5, 1}},
		{"same-page", 1, []int64{1, 0}},
		{"back-to-last", 4, []int64{1, 6}},
	}
}

// lazyPageSeeks adds sequences that still observe a dictionary column's
// data-page counter after an unindexed seek. That counter used to stay at 1,
// so the first indexed seek onto page 1 looked already positioned or served
// the wrong cached page. A later seek to a different page repositions and
// hides the bug, so these sequences end on page 1.
func lazyPageSeeks() []pageSeekCase {
	return append([]pageSeekCase{
		{"later-page", 0, []int64{3, 2}},
		{"later-page-after-read", 1, []int64{5, 3}},
	}, consecutivePageSeeks()...)
}

func writeSeekPages[T any](t *testing.T, rows []T, opts ...parquet.WriterOption) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[T](&buf, opts...)
	for i := 0; i < len(rows); i += 2 {
		if _, err := w.Write(rows[i : i+2]); err != nil {
			t.Fatal(err)
		}
		for _, column := range w.ColumnWriters() {
			if err := column.Flush(); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func seekPlainRows() []seekPlainRow {
	rows := make([]seekPlainRow, seekPageRows)
	for i := range rows {
		rows[i].V = int64(i)
	}
	return rows
}

func seekDictRows() []seekDictRow {
	rows := make([]seekDictRow, seekPageRows)
	for i := range rows {
		rows[i].V = int64(i)
	}
	return rows
}

func seekTwoColRows() []seekTwoColRow {
	rows := make([]seekTwoColRow, seekPageRows)
	for i := range rows {
		rows[i] = seekTwoColRow{Plain: int64(i), Dict: int64(i)}
	}
	return rows
}

func readRowsFrom(t *testing.T, pages parquet.Pages, row, numRows int64) {
	t.Helper()
	for row < numRows {
		page, err := pages.ReadPage()
		if err != nil {
			t.Fatalf("ReadPage at row %d: %v", row, err)
		}
		values := make([]parquet.Value, page.NumValues())
		n, err := readPageValues(page, values)
		parquet.Release(page)
		if n != len(values) || (err != nil && err != io.EOF) {
			t.Fatalf("ReadValues at row %d: n=%d err=%v", row, n, err)
		}
		for _, value := range values[:n] {
			if value.Int64() != row {
				t.Fatalf("row %d: got %d, want %d", row, value.Int64(), row)
			}
			row++
		}
	}
	page, err := pages.ReadPage()
	parquet.Release(page)
	if err != io.EOF {
		t.Fatalf("ReadPage after last row: got %v, want EOF", err)
	}
}

func readPageValues(page parquet.Page, values []parquet.Value) (int, error) {
	reader := page.Values()
	n := 0
	for n < len(values) {
		nn, err := reader.ReadValues(values[n:])
		n += nn
		if err != nil {
			return n, err
		}
		if nn == 0 {
			break
		}
	}
	return n, nil
}

func runConsecutivePageSeeks(t *testing.T, data []byte, opts []parquet.FileOption, lazy bool, cases []pageSeekCase) {
	t.Helper()
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)), opts...)
			if err != nil {
				t.Fatal(err)
			}
			for col, chunk := range f.RowGroups()[0].ColumnChunks() {
				t.Run(fmt.Sprintf("col%d", col), func(t *testing.T) {
					pages := chunk.Pages()
					defer pages.Close()
					if lazy {
						// Position before the offset index is available.
						if err := pages.SeekToRow(0); err != nil {
							t.Fatal(err)
						}
					}
					for range test.reads {
						page, err := pages.ReadPage()
						if err != nil {
							t.Fatal(err)
						}
						parquet.Release(page)
					}
					if lazy {
						if _, err := chunk.OffsetIndex(); err != nil {
							t.Fatal(err)
						}
					}
					for _, row := range test.seeks {
						if err := pages.SeekToRow(row); err != nil {
							t.Fatalf("SeekToRow(%d): %v", row, err)
						}
					}
					readRowsFrom(t, pages, test.seeks[len(test.seeks)-1], seekPageRows)
				})
			}
		})
	}
}

// TestFilePagesConsecutiveSeeksLazyIndex covers consecutive SeekToRow calls
// made after an offset index is loaded on demand.
func TestFilePagesConsecutiveSeeksLazyIndex(t *testing.T) {
	for _, version := range []int{1, 2} {
		data := writeSeekPages(t, seekTwoColRows(), parquet.DataPageVersion(version))
		for _, bufferSize := range []int{16, 4096} {
			t.Run(fmt.Sprintf("v%d/buffer%d", version, bufferSize), func(t *testing.T) {
				opts := []parquet.FileOption{
					parquet.SkipPageIndex(true),
					parquet.ReadBufferSize(bufferSize),
				}
				runConsecutivePageSeeks(t, data, opts, true, lazyPageSeeks())
			})
		}
	}
}

// TestFilePagesConsecutiveSeeksEncryptedDict covers consecutive SeekToRow
// calls on an encrypted dictionary column, with the offset index either
// loaded at open or loaded lazily after an unindexed seek.
func TestFilePagesConsecutiveSeeksEncryptedDict(t *testing.T) {
	footerKey := aes128Key(0xA6)
	cfg := &parquet.EncryptionConfig{
		FooterKey:       footerKey,
		EncryptedFooter: true,
		FileIdentifier:  []byte{8, 7, 6, 5, 4, 3, 2, 1},
	}
	keys := &staticKeyRetriever{footerKey: footerKey}
	for _, version := range []int{1, 2} {
		data := writeSeekPages(t, seekDictRows(), parquet.DataPageVersion(version), parquet.WithEncryption(cfg))
		for _, bufferSize := range []int{16, 4096} {
			for _, lazy := range []bool{false, true} {
				name := fmt.Sprintf("v%d/buffer%d", version, bufferSize)
				if lazy {
					name += "/lazy-index"
				}
				t.Run(name, func(t *testing.T) {
					opts := []parquet.FileOption{
						parquet.WithDecryption(keys),
						parquet.ReadBufferSize(bufferSize),
						parquet.SkipPageIndex(lazy),
					}
					cases := consecutivePageSeeks()
					if lazy {
						cases = lazyPageSeeks()
					}
					runConsecutivePageSeeks(t, data, opts, lazy, cases)
				})
			}
		}
	}
}

type recordedRead struct {
	off int64
	n   int
}

type recordingReaderAt struct {
	r     io.ReaderAt
	reads []recordedRead
}

func (r *recordingReaderAt) ReadAt(p []byte, off int64) (int, error) {
	n, err := r.r.ReadAt(p, off)
	r.reads = append(r.reads, recordedRead{off: off, n: n})
	return n, err
}

func readOverlaps(reads []recordedRead, start, size int64) bool {
	end := start + size
	for _, rd := range reads {
		if rd.n > 0 && rd.off < end && rd.off+int64(rd.n) > start {
			return true
		}
	}
	return false
}

// TestFilePagesSeekBackReusesCachedPage checks that seeking back onto a page
// that is already decoded does not fetch its bytes again, and that the
// following pages still return the original rows.
func TestFilePagesSeekBackReusesCachedPage(t *testing.T) {
	footerKey := aes128Key(0xA7)
	cfg := &parquet.EncryptionConfig{
		FooterKey:       footerKey,
		EncryptedFooter: true,
		FileIdentifier:  []byte{1, 1, 2, 2, 3, 3, 4, 4},
	}
	keys := &staticKeyRetriever{footerKey: footerKey}

	for _, kind := range []string{"plain", "dict", "encrypted-dict"} {
		for _, version := range []int{1, 2} {
			var data []byte
			var openOpts []parquet.FileOption
			switch kind {
			case "plain":
				data = writeSeekPages(t, seekPlainRows(), parquet.DataPageVersion(version))
			case "dict":
				data = writeSeekPages(t, seekDictRows(), parquet.DataPageVersion(version))
			default:
				data = writeSeekPages(t, seekDictRows(), parquet.DataPageVersion(version), parquet.WithEncryption(cfg))
				openOpts = append(openOpts, parquet.WithDecryption(keys))
			}
			for _, bufferSize := range []int{16, 4096} {
				for _, which := range []string{"within", "first", "middle", "last"} {
					t.Run(fmt.Sprintf("%s/v%d/buffer%d/%s", kind, version, bufferSize, which), func(t *testing.T) {
						rec := &recordingReaderAt{r: bytes.NewReader(data)}
						opts := append([]parquet.FileOption{parquet.ReadBufferSize(bufferSize)}, openOpts...)
						f, err := parquet.OpenFile(rec, int64(len(data)), opts...)
						if err != nil {
							t.Fatal(err)
						}
						chunk := f.RowGroups()[0].ColumnChunks()[0]
						idx, err := chunk.OffsetIndex()
						if err != nil {
							t.Fatal(err)
						}
						numPages := idx.NumPages()
						if numPages < 3 {
							t.Fatalf("num pages = %d, want at least 3", numPages)
						}
						page := 0
						prime := 1
						switch which {
						case "middle":
							page = 1
							prime = 2
						case "last":
							page = numPages - 1
							prime = numPages
						}
						pages := chunk.Pages()
						defer pages.Close()
						for range prime {
							got, err := pages.ReadPage()
							if err != nil {
								t.Fatal(err)
							}
							parquet.Release(got)
						}
						pageRows := seekPageRows - idx.FirstRowIndex(page)
						if page+1 < numPages {
							pageRows = idx.FirstRowIndex(page+1) - idx.FirstRowIndex(page)
						}
						if pageRows < 1 {
							t.Fatalf("page %d has %d rows", page, pageRows)
						}
						back := idx.FirstRowIndex(page)
						if pageRows > 1 {
							back++
						}
						away := idx.FirstRowIndex(numPages - 1)
						if page == numPages-1 {
							away = idx.FirstRowIndex(0)
						}
						rec.reads = nil
						if which != "within" {
							if err := pages.SeekToRow(away); err != nil {
								t.Fatalf("SeekToRow(%d): %v", away, err)
							}
						}
						if err := pages.SeekToRow(back); err != nil {
							t.Fatalf("SeekToRow(%d): %v", back, err)
						}
						got, err := pages.ReadPage()
						if err != nil {
							t.Fatalf("ReadPage at row %d: %v", back, err)
						}
						start, size := idx.Offset(page), idx.CompressedPageSize(page)
						if size <= 0 {
							t.Fatalf("page %d compressed size = %d", page, size)
						}
						if readOverlaps(rec.reads, start, size) {
							t.Fatalf("seeking back to cached page %d re-read [%d,%d): %+v", page, start, start+size, rec.reads)
						}
						values := make([]parquet.Value, got.NumValues())
						n, err := readPageValues(got, values)
						parquet.Release(got)
						if n != len(values) || (err != nil && err != io.EOF) {
							t.Fatalf("ReadValues at row %d: n=%d err=%v", back, n, err)
						}
						row := back
						for _, value := range values[:n] {
							if value.Int64() != row {
								t.Fatalf("row %d: got %d, want %d", row, value.Int64(), row)
							}
							row++
						}
						readRowsFrom(t, pages, row, seekPageRows)
					})
				}
			}
		}
	}
}

var benchmarkSeekBackRows int64

func BenchmarkFilePagesSeekToCachedPage(b *testing.B) {
	const rowsPerPage = 16384
	const numPages = 4
	type row struct {
		V int64 `parquet:"v,snappy"`
	}
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[row](&buf, parquet.PageBufferSize(8<<20))
	for page := range numPages {
		rows := make([]row, rowsPerPage)
		base := page * rowsPerPage
		for i := range rows {
			rows[i].V = int64(base + i)
		}
		if _, err := w.Write(rows); err != nil {
			b.Fatal(err)
		}
		for _, column := range w.ColumnWriters() {
			if err := column.Flush(); err != nil {
				b.Fatal(err)
			}
		}
	}
	if err := w.Close(); err != nil {
		b.Fatal(err)
	}
	data := bytes.Clone(buf.Bytes())
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		b.Fatal(err)
	}
	idx, err := f.RowGroups()[0].ColumnChunks()[0].OffsetIndex()
	if err != nil {
		b.Fatal(err)
	}
	if idx.NumPages() < 2 || idx.FirstRowIndex(1) < 4096 {
		b.Fatalf("page 0 has %d rows across %d pages, want a large first page", idx.FirstRowIndex(1), idx.NumPages())
	}
	pageRows := idx.FirstRowIndex(1)
	far := idx.FirstRowIndex(idx.NumPages() - 1)

	bench := func(b *testing.B, away bool) {
		pages := f.RowGroups()[0].ColumnChunks()[0].Pages()
		defer pages.Close()
		page, err := pages.ReadPage()
		if err != nil {
			b.Fatal(err)
		}
		parquet.Release(page)
		var row int64
		for b.Loop() {
			if away {
				if err := pages.SeekToRow(far); err != nil {
					b.Fatal(err)
				}
			}
			if err := pages.SeekToRow(row % pageRows); err != nil {
				b.Fatal(err)
			}
			row++
			page, err := pages.ReadPage()
			if err != nil {
				b.Fatal(err)
			}
			benchmarkSeekBackRows += page.NumRows()
			parquet.Release(page)
		}
	}

	b.Run("within", func(b *testing.B) { bench(b, false) })
	b.Run("away-and-back", func(b *testing.B) { bench(b, true) })
}
