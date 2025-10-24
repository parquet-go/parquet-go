package parquet_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
)

var testdataFiles []string

func init() {
	testdataFiles, _ = filepath.Glob("testdata/*.parquet")
}

func TestOpenFile(t *testing.T) {
	for _, path := range testdataFiles {
		t.Run(path, func(t *testing.T) {
			f, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()

			s, err := f.Stat()
			if err != nil {
				t.Fatal(err)
			}

			p, err := parquet.OpenFile(f, s.Size(),
				parquet.OptimisticRead(true),
				parquet.FileReadMode(parquet.ReadModeAsync),
			)
			if err != nil {
				t.Fatal(err)
			}

			if size := p.Size(); size != s.Size() {
				t.Errorf("file size mismatch: want=%d got=%d", s.Size(), size)
			}

			root := p.Root()
			b := new(strings.Builder)
			parquet.PrintSchema(b, root.Name(), root)
			t.Log(b)

			printColumns(t, p.Root(), "")
		})
	}
}

func TestOpenFileWithoutPageIndex(t *testing.T) {
	for _, path := range testdataFiles {
		t.Run(path, func(t *testing.T) {
			f, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()

			s, err := f.Stat()
			if err != nil {
				t.Fatal(err)
			}

			fileWithIndex, err := parquet.OpenFile(f, s.Size())
			if err != nil {
				t.Fatal(err)
			}
			fileWithoutIndex, err := parquet.OpenFile(f, s.Size(),
				parquet.SkipPageIndex(true))
			if err != nil {
				t.Fatal(err)
			}

			if size := fileWithoutIndex.Size(); size != s.Size() {
				t.Errorf("file size mismatch: want=%d got=%d", s.Size(), size)
			}

			for iRowGroup, rowGroup := range fileWithoutIndex.RowGroups() {
				for iChunk, chunk := range rowGroup.ColumnChunks() {
					chunkMeta := fileWithoutIndex.Metadata().RowGroups[iRowGroup].Columns[iChunk].MetaData

					preloadedColumnIndex, pErr := fileWithIndex.RowGroups()[iRowGroup].ColumnChunks()[iChunk].ColumnIndex()
					if errors.Is(pErr, parquet.ErrMissingColumnIndex) && chunkMeta.IndexPageOffset != 0 {
						t.Errorf("get column index for %s: %s", chunkMeta.PathInSchema[0], pErr)
					}
					columnIndex, err := chunk.ColumnIndex()
					if errors.Is(err, parquet.ErrMissingColumnIndex) && chunkMeta.IndexPageOffset != 0 {
						t.Errorf("get column index for %s: %s", chunkMeta.PathInSchema[0], err)
					}
					if !errors.Is(err, pErr) {
						t.Errorf("mismatch when opening file with and without index, chunk=%d, row group=%d", iChunk, iRowGroup)
					}
					if preloadedColumnIndex == nil && columnIndex != nil || preloadedColumnIndex != nil && columnIndex == nil {
						t.Errorf("mismatch when opening file with and without index, chunk=%d, row group=%d", iChunk, iRowGroup)
					}

					preloadedOffsetIndex, pErr := fileWithIndex.RowGroups()[iRowGroup].ColumnChunks()[iChunk].OffsetIndex()
					if errors.Is(pErr, parquet.ErrMissingOffsetIndex) && chunkMeta.IndexPageOffset != 0 {
						t.Errorf("get offset index for %s: %s", chunkMeta.PathInSchema[0], pErr)
					}
					offsetIndex, err := chunk.OffsetIndex()
					if errors.Is(err, parquet.ErrMissingOffsetIndex) && chunkMeta.IndexPageOffset != 0 {
						t.Errorf("get offset index for %s: %s", chunkMeta.PathInSchema[0], err)
					}
					if !errors.Is(err, pErr) {
						t.Errorf("mismatch when opening file with and without index, chunk=%d, row group=%d", iChunk, iRowGroup)
					}
					if preloadedOffsetIndex == nil && offsetIndex != nil || preloadedOffsetIndex != nil && offsetIndex == nil {
						t.Errorf("mismatch when opening file with and without index, chunk=%d, row group=%d", iChunk, iRowGroup)
					}
				}
			}
		})
	}
}

func printColumns(t *testing.T, col *parquet.Column, indent string) {
	if t.Failed() {
		return
	}

	path := strings.Join(col.Path(), ".")
	if col.Leaf() {
		t.Logf("%s%s %v %v", indent, path, col.Encoding(), col.Compression())
	} else {
		t.Logf("%s%s", indent, path)
	}
	indent += ". "

	buffer := make([]parquet.Value, 42)
	pages := col.Pages()
	defer pages.Close()
	for {
		p, err := pages.ReadPage()
		if err != nil {
			if err != io.EOF {
				t.Error(err)
			}
			break
		}

		values := p.Values()
		numValues := int64(0)
		nullCount := int64(0)

		for {
			n, err := values.ReadValues(buffer)
			for _, v := range buffer[:n] {
				if v.Column() != col.Index() {
					t.Errorf("value read from page of column %d says it belongs to column %d", col.Index(), v.Column())
					return
				}
				if v.IsNull() {
					nullCount++
				}
			}
			numValues += int64(n)
			if err != nil {
				if err != io.EOF {
					t.Error(err)
					return
				}
				break
			}
		}

		if numValues != p.NumValues() {
			t.Errorf("page of column %d declared %d values but %d were read", col.Index(), p.NumValues(), numValues)
			return
		}

		if nullCount != p.NumNulls() {
			t.Errorf("page of column %d declared %d nulls but %d were read", col.Index(), p.NumNulls(), nullCount)
			return
		}

		parquet.Release(p)
	}

	for _, child := range col.Columns() {
		printColumns(t, child, indent)
	}
}

func TestFileKeyValueMetadata(t *testing.T) {
	type Row struct {
		Name string
	}

	f, err := createParquetFile(
		makeRows([]Row{{Name: "A"}, {Name: "B"}, {Name: "C"}}),
		parquet.KeyValueMetadata("hello", "ignore this one"),
		parquet.KeyValueMetadata("hello", "world"),
		parquet.KeyValueMetadata("answer", "42"),
	)
	if err != nil {
		t.Fatal(err)
	}

	for _, want := range [][2]string{
		{"hello", "world"},
		{"answer", "42"},
	} {
		key, value := want[0], want[1]
		if found, ok := f.Lookup(key); !ok || found != value {
			t.Errorf("key/value metadata mismatch: want %q=%q but got %q=%q (found=%t)", key, value, key, found, ok)
		}
	}
}

func TestFileTypes(t *testing.T) {
	f, err := os.Open("testdata/data_index_bloom_encoding_stats.parquet")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	s, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}

	p, err := parquet.OpenFile(f, s.Size(),
		parquet.FileReadMode(parquet.ReadModeAsync),
	)
	if err != nil {
		t.Fatal(err)
	}

	for _, rowGroup := range p.RowGroups() {
		if _, ok := rowGroup.(*parquet.FileRowGroup); !ok {
			t.Fatalf("row group of parquet.File must be of type *parquet.FileRowGroup but got %T", rowGroup)
		}
		for _, columnChunk := range rowGroup.ColumnChunks() {
			fcc, ok := columnChunk.(*parquet.FileColumnChunk)
			if !ok {
				t.Fatalf("column chunk of parquet.File must be of type *parquet.FileColumnChunk but got %T", columnChunk)
			}
			min, max, ok := fcc.Bounds()
			if !ok {
				t.Error("column chunk is missing statistics")
			} else {
				if min.IsNull() {
					t.Error("column chunk has null min value")
				}
				if max.IsNull() {
					t.Error("column chunk has null max value")
				}
			}
		}
	}
}

func TestOpenFileOptimisticRead(t *testing.T) {
	f, err := os.Open("testdata/alltypes_tiny_pages_plain.parquet")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	s, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}

	r := &measuredReaderAt{reader: f}
	if _, err := parquet.OpenFile(r, s.Size(),
		parquet.OptimisticRead(true),
		parquet.SkipMagicBytes(true),
		parquet.ReadBufferSize(int(s.Size()/2)),
	); err != nil {
		t.Fatal(err)
	}

	if reads := r.reads.Load(); reads != 1 {
		t.Errorf("expected 1 read, got %d", reads)
	}
}

func TestIssue229(t *testing.T) {
	// https://github.com/grafana/tempo/blob/5cae77c9cf8da51e0db7c5556b19d305130ea9c4/tempodb/encoding/vparquet2/schema.go
	type Attribute struct {
		Key string `parquet:",snappy,dict"`

		// This is a bad design that leads to millions of null values. How can we fix this?
		Value       *string  `parquet:",dict,snappy,optional"`
		ValueInt    *int64   `parquet:",snappy,optional"`
		ValueDouble *float64 `parquet:",snappy,optional"`
		ValueBool   *bool    `parquet:",snappy,optional"`
		ValueKVList string   `parquet:",snappy,optional"`
		ValueArray  string   `parquet:",snappy,optional"`
	}

	type EventAttribute struct {
		Key   string `parquet:",snappy,dict"`
		Value []byte `parquet:",snappy"` // Was json-encoded data, is now proto encoded data
	}

	type Event struct {
		TimeUnixNano           uint64           `parquet:",delta"`
		Name                   string           `parquet:",snappy"`
		Attrs                  []EventAttribute `parquet:",list"`
		DroppedAttributesCount int32            `parquet:",snappy,delta"`
		Test                   string           `parquet:",snappy,dict,optional"` // Always empty for testing
	}

	type Span struct {
		// SpanID is []byte to save space. It doesn't need to be user
		// friendly like trace ID, and []byte is half the size of string.
		SpanID                 []byte      `parquet:","`
		ParentSpanID           []byte      `parquet:","`
		ParentID               int32       `parquet:",delta"` // can be zero for non-root spans, use IsRoot to check for root spans
		NestedSetLeft          int32       `parquet:",delta"` // doubles as numeric ID and is used to fill ParentID of child spans
		NestedSetRight         int32       `parquet:",delta"`
		Name                   string      `parquet:",snappy,dict"`
		Kind                   int         `parquet:",delta"`
		TraceState             string      `parquet:",snappy"`
		StartTimeUnixNano      uint64      `parquet:",delta"`
		DurationNano           uint64      `parquet:",delta"`
		StatusCode             int         `parquet:",delta"`
		StatusMessage          string      `parquet:",snappy"`
		Attrs                  []Attribute `parquet:",list"`
		DroppedAttributesCount int32       `parquet:",snappy"`
		Events                 []Event     `parquet:",list"`
		DroppedEventsCount     int32       `parquet:",snappy"`
		Links                  []byte      `parquet:",snappy"` // proto encoded []*v1_trace.Span_Link
		DroppedLinksCount      int32       `parquet:",snappy"`

		// Known attributes
		HttpMethod     *string `parquet:",snappy,optional,dict"`
		HttpUrl        *string `parquet:",snappy,optional,dict"`
		HttpStatusCode *int64  `parquet:",snappy,optional"`
	}

	type InstrumentationScope struct {
		Name    string `parquet:",snappy,dict"`
		Version string `parquet:",snappy,dict"`
	}

	type ScopeSpans struct {
		Scope InstrumentationScope `parquet:""`
		Spans []Span               `parquet:",list"`
	}

	type Resource struct {
		Attrs []Attribute `parquet:",list"`

		// Known attributes
		ServiceName      string  `parquet:",snappy,dict"`
		Cluster          *string `parquet:",snappy,optional,dict"`
		Namespace        *string `parquet:",snappy,optional,dict"`
		Pod              *string `parquet:",snappy,optional,dict"`
		Container        *string `parquet:",snappy,optional,dict"`
		K8sClusterName   *string `parquet:",snappy,optional,dict"`
		K8sNamespaceName *string `parquet:",snappy,optional,dict"`
		K8sPodName       *string `parquet:",snappy,optional,dict"`
		K8sContainerName *string `parquet:",snappy,optional,dict"`

		Test string `parquet:",snappy,dict,optional"` // Always empty for testing
	}

	type ResourceSpans struct {
		Resource   Resource     `parquet:""`
		ScopeSpans []ScopeSpans `parquet:"ss,list"`
	}

	type Trace struct {
		// TraceID is a byte slice as it helps maintain the sort order of traces within a parquet file
		TraceID       []byte          `parquet:""`
		ResourceSpans []ResourceSpans `parquet:"rs,list"`

		// TraceIDText is for better usability on downstream systems i.e: something other than Tempo is reading these files.
		// It will not be used as the primary traceID field within Tempo and is only helpful for debugging purposes.
		TraceIDText string `parquet:",snappy"`

		// Trace-level attributes for searching
		StartTimeUnixNano uint64 `parquet:",delta"`
		EndTimeUnixNano   uint64 `parquet:",delta"`
		DurationNano      uint64 `parquet:",delta"`
		RootServiceName   string `parquet:",dict"`
		RootSpanName      string `parquet:",dict"`
	}

	file, err := os.Open("testdata/issue229.parquet")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}

	pf, err := parquet.OpenFile(file, info.Size())
	if err != nil {
		t.Fatal(err)
	}

	r := parquet.NewReader(pf)

	if err := r.SeekToRow(3); err != nil {
		t.Fatal(err)
	}

	firstRows := []parquet.Row{{}}
	if n, err := r.ReadRows(firstRows); n != 1 {
		t.Fatalf("expected 1 row, got %d", n)
	} else if err != nil {
		t.Fatal(err)
	}
	tr := &Trace{}
	sch := parquet.SchemaOf(tr)
	sch.Reconstruct(tr, firstRows[0])

	if err := r.SeekToRow(8); err != nil {
		t.Fatal(err)
	}

	secondRows := []parquet.Row{{}}
	if n, err := r.ReadRows(secondRows); n != 1 {
		t.Fatalf("expected 1 row, got %d", n)
	} else if err != nil {
		t.Fatal(err)
	}
	tr = &Trace{}
	sch.Reconstruct(tr, secondRows[0])
}

func TestReadDictionaryPage(t *testing.T) {
	type A struct {
		Index string `parquet:",dict"`
	}

	b := new(bytes.Buffer)
	w := parquet.NewGenericWriter[A](b)

	// Create and write 1000 rows with index-based names
	const totalRows = 1000
	for i := range totalRows {
		_, err := w.Write([]A{{Index: fmt.Sprintf("%d", i)}})
		if err != nil {
			t.Fatal(err)
		}

		if (i+1)%37 == 0 { // force row group flush at purposefully unaligned rows
			if err := w.Flush(); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	pf, err := parquet.OpenFile(bytes.NewReader(b.Bytes()), int64(b.Len()), parquet.ReadBufferSize(2)) // read buffer size of 3 should expose any issues with buffer overread
	if err != nil {
		t.Fatal(err)
	}

	foundRows := 0
	for _, rg := range pf.RowGroups() {
		cc := rg.ColumnChunks()[0]
		pages := cc.Pages()

		filePages := pages.(*parquet.FilePages)
		dict, err := filePages.ReadDictionary()
		if err != nil {
			t.Fatal(err)
		}
		if dict == nil {
			t.Fatal("expected dictionary page to be available")
		}

		for {
			page, err := pages.ReadPage()
			if err != nil && err != io.EOF {
				t.Fatal(err)
			}
			if page == nil {
				break
			}

			vr := page.Values()
			values := make([]parquet.Value, 1)

			for {
				n, err := vr.ReadValues(values)
				if err != nil && err != io.EOF {
					t.Fatal(err)
				}
				if n == 0 {
					break
				}

				actual := values[0].String()
				expected := fmt.Sprintf("%d", foundRows)
				if actual != expected {
					t.Fatalf("expected value %s, got %s", expected, actual)
				}
				foundRows++
			}
		}

		pages.Close()
	}

	if foundRows != totalRows {
		t.Fatalf("expected %d rows, got %d", totalRows, foundRows)
	}
}

func TestCopyFilePages(t *testing.T) {
	type A struct {
		OptionalField *string
	}
	b := new(bytes.Buffer)
	w := parquet.NewGenericWriter[A](b)
	val := "test-val"
	if _, err := w.Write([]A{{OptionalField: &val}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	pf, err := parquet.OpenFile(bytes.NewReader(b.Bytes()), int64(b.Len()))
	if err != nil {
		t.Fatal(err)
	}
	pages := pf.RowGroups()[0].ColumnChunks()[0].Pages()
	if _, err := parquet.CopyPages(nopPageWriter{}, pages); err != nil {
		t.Fatal(err)
	}
	if err := pages.Close(); err != nil {
		t.Fatal(err)
	}
	// Sleep and run GC to trigger collection of buffer.
	time.Sleep(time.Second)
	runtime.GC()
}

func TestSeekToRowGeneral(t *testing.T) {
	type Row struct {
		A int `parquet:","`
		B int `parquet:",dict"`
	}

	// Create test data with 100 rows
	const numRows = 100

	// Create a parquet file in memory with a small page buffer to force many pages
	buf := new(bytes.Buffer)
	w := parquet.NewGenericWriter[Row](buf, parquet.PageBufferSize(128))

	// Write rows individually with frequent flushes to create multiple row groups
	for i := range numRows {
		if _, err := w.Write([]Row{{A: i, B: i}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Random rows to seek to
	randomRows := make([]int, 100)
	for i := range randomRows {
		randomRows[i] = rand.Intn(numRows)
	}
	t.Logf("Testing random rows: %v", randomRows)

	// values to read
	vals := make([]parquet.Value, 1)

	for _, col := range []int{0, 1} {
		for _, mode := range []parquet.ReadMode{parquet.ReadModeSync, parquet.ReadModeAsync} {
			t.Run(fmt.Sprintf("%s/col%d", readModeToString(mode), col), func(t *testing.T) {
				// Open parquet file for reading
				pf, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()), parquet.FileReadMode(mode))
				if err != nil {
					t.Fatal(err)
				}

				rgs := pf.RowGroups()
				if len(rgs) != 1 {
					t.Fatalf("expected exactly 1 row group, got %d", len(rgs))
				}

				// Test 1: SeekToRow forward through every row
				t.Run("forward", func(t *testing.T) {
					pages := rgs[0].ColumnChunks()[col].Pages()
					t.Cleanup(func() {
						_ = pages.Close()
					})

					for i := range numRows {
						err = pages.SeekToRow(int64(i))
						if err != nil {
							t.Fatalf("SeekToRow(%d) failed: %v", i, err)
						}

						pg1, err := pages.ReadPage()
						if err != nil {
							t.Fatalf("ReadPage failed at row %d: %v", i, err)
						}

						pg2, err := pages.ReadPage()
						if err != nil && !errors.Is(err, io.EOF) {
							t.Fatalf("ReadPage failed at row %d: %v", i, err)
						}
						parquet.Release(pg2)

						n, err := pg1.Values().ReadValues(vals)
						if err != nil && !errors.Is(err, io.EOF) {
							t.Fatalf("ReadValues failed at row %d: %v", i, err)
						}
						parquet.Release(pg1)

						if n != 1 {
							t.Fatalf("expected 1 value, got %d at position %d", n, i)
						}

						if vals[0].Int64() != int64(i) {
							t.Errorf("row %d: expected value %d, got %d", i, i, vals[0].Int64())
						}
					}
				})

				// Test 2: SeekToRow backward through every row
				t.Run("backward", func(t *testing.T) {
					pages := rgs[0].ColumnChunks()[col].Pages()
					t.Cleanup(func() {
						_ = pages.Close()
					})

					for i := numRows - 1; i >= 0; i-- {
						err = pages.SeekToRow(int64(i))
						if err != nil {
							t.Fatalf("SeekToRow(%d) failed: %v", i, err)
						}

						pg1, err := pages.ReadPage()
						if err != nil {
							t.Fatalf("ReadPage failed at row %d: %v", i, err)
						}

						pg2, err := pages.ReadPage()
						if err != nil && !errors.Is(err, io.EOF) {
							t.Fatalf("ReadPage failed at row %d: %v", i, err)
						}
						parquet.Release(pg2)

						n, err := pg1.Values().ReadValues(vals)
						if err != nil && !errors.Is(err, io.EOF) {
							t.Fatalf("ReadValues failed at row %d: %v", i, err)
						}
						parquet.Release(pg1)

						if n != 1 {
							t.Fatalf("expected 1 value, got %d at position %d", n, i)
						}

						if vals[0].Int64() != int64(i) {
							t.Errorf("row %d: expected value %d, got %d", i, i, vals[0].Int64())
						}
					}
				})

				// Test 3: SeekToRow to random rows
				t.Run("random", func(t *testing.T) {
					pages := rgs[0].ColumnChunks()[col].Pages()
					t.Cleanup(func() {
						_ = pages.Close()
					})

					for _, rowNum := range randomRows {
						err = pages.SeekToRow(int64(rowNum))
						if err != nil {
							t.Fatalf("SeekToRow(%d) failed: %v", rowNum, err)
						}

						pg1, err := pages.ReadPage()
						if err != nil {
							t.Fatalf("ReadPage failed at row %d: %v", rowNum, err)
						}

						pg2, err := pages.ReadPage()
						if err != nil && !errors.Is(err, io.EOF) {
							t.Fatalf("ReadPage failed at row %d: %v", rowNum, err)
						}
						parquet.Release(pg2)

						n, err := pg1.Values().ReadValues(vals)
						if err != nil && !errors.Is(err, io.EOF) {
							t.Fatalf("ReadValues failed at row %d: %v", rowNum, err)
						}
						parquet.Release(pg1)

						if n != 1 {
							t.Fatalf("expected 1 value, got %d at position %d", n, rowNum)
						}

						if vals[0].Int64() != int64(rowNum) {
							t.Errorf("row %d: expected value %d, got %d", rowNum, rowNum, vals[0].Int64())
						}
					}
				})
			})
		}
	}
}

type nopPageWriter struct{}

func (n nopPageWriter) WritePage(page parquet.Page) (int64, error) {
	return page.NumValues(), nil
}

type countingReaderAt struct {
	ra    io.ReaderAt
	reads int64
}

func (c *countingReaderAt) ReadAt(p []byte, off int64) (int, error) {
	atomic.AddInt64(&c.reads, 1)
	return c.ra.ReadAt(p, off)
}

type benchRow struct {
	K string   `parquet:",snappy,dict"`
	V []string `parquet:",snappy,list"`
}

// BenchmarkSeekThroughFile reads / seeks through the whole file while tracking the number of reads.
// Hypothesis: seeking through the whole file should always require equal or fewer reads than reading
// the whole file.
func BenchmarkSeekThroughFile(b *testing.B) {
	rnd := rand.New(rand.NewSource(1234))

	var numRows int64 = 10_000
	data := makeSeekBenchFile(b, numRows)

	steps := [6]int64{0, 7, 29, 61, 127, 251}
	var positions [6][]int64

	// precompute all seek positions for all steps
	for i, stepSize := range steps {
		positions[i] = make([]int64, 0, numRows/10)
		if stepSize > 0 {
			var pos int64
			for range numRows {
				jitter := rnd.Int63n(stepSize)
				pos += stepSize/2 + jitter
				positions[i] = append(positions[i], pos)
				if pos >= int64(numRows) {
					break
				}
			}
		}
	}

	// wrap bytes.Reader with counting ReaderAt and open file
	br := bytes.NewReader(data)
	ctr := &countingReaderAt{ra: br}

	for _, mode := range []parquet.ReadMode{parquet.ReadModeSync, parquet.ReadModeAsync} {
		b.Run(readModeToString(mode), func(b *testing.B) {
			opts := []parquet.FileOption{
				parquet.SkipBloomFilters(true),
				parquet.FileReadMode(mode),
				parquet.ReadBufferSize(90 * 1024), // roughly 1.5x the page size in the 'V' column
			}

			f, err := parquet.OpenFile(ctr, int64(len(data)), opts...)
			if err != nil {
				b.Fatalf("open file: %v", err)
			}

			// seek / read through the whole file using Reader
			b.Run("reader", func(b *testing.B) {
				row := make([]benchRow, 1)

				for i, stepSize := range steps {
					name := fmt.Sprintf("step size=%d", stepSize)
					if stepSize == 0 {
						name = "no-seek"
					}
					b.Run(name, func(b *testing.B) {
						atomic.StoreInt64(&ctr.reads, 0)

						// benchmark loop: read / seek through the whole file
						b.ResetTimer()
						for range b.N {
							r := parquet.NewGenericReader[benchRow](f)

							for j := range numRows {
								if stepSize > 0 { // if seek is zero, we don't seek at all (baseline)
									err = r.SeekToRow(positions[i][j])
									if err != nil {
										if errors.Is(err, io.EOF) {
											break
										}
										b.Fatalf("read row %d: %v", j, err)
									}
								}

								_, err = r.Read(row)
								if err != nil {
									if errors.Is(err, io.EOF) {
										break
									}
									b.Fatalf("read row %d: %v", j, err)
								}
							}
							_ = r.Close()
						}

						b.ReportMetric(float64(atomic.LoadInt64(&ctr.reads))/float64(b.N), "reads/op")
					})
				}
			})

			// seek / read through the whole column 1 using Pages
			b.Run("pages", func(b *testing.B) {
				for i, stepSize := range steps {
					name := fmt.Sprintf("step size=%d", stepSize)
					if stepSize == 0 {
						name = "no-seek"
					}
					b.Run(name, func(b *testing.B) {
						atomic.StoreInt64(&ctr.reads, 0)

						b.ResetTimer()
						for range b.N {
							// sequentially read all pages of column 1 (baseline)
							if stepSize == 0 {
								for _, rg := range f.RowGroups() {
									pages := rg.ColumnChunks()[1].Pages()
									for {
										_, err := pages.ReadPage()
										if err != nil {
											if errors.Is(err, io.EOF) {
												break
											}
											b.Fatalf("read page: %v", err)
										}
									}
									_ = pages.Close()
								}
								continue
							}

							// read column 1 seeking through rg pages
							var j int
							var rgOffset int64
							for _, rg := range f.RowGroups() {
								pages := rg.ColumnChunks()[1].Pages()

								for {
									pos := positions[i][j] - rgOffset
									if pos >= rg.NumRows() {
										_ = pages.Close()
										rgOffset += rg.NumRows()
										break
									}

									err = pages.SeekToRow(pos)
									if err != nil {
										b.Fatalf("seek page: %v", err)
									}

									_, err := pages.ReadPage()
									if err != nil {
										if errors.Is(err, io.EOF) {
											break
										}
										b.Fatalf("read page: %v", err)
									}
									j++
								}
							}
						}

						b.ReportMetric(float64(atomic.LoadInt64(&ctr.reads))/float64(b.N), "reads/op")
					})
				}
			})
		})
	}
}

func makeSeekBenchFile(t testing.TB, numRows int64) []byte {
	rnd := rand.New(rand.NewSource(1234))
	var buf bytes.Buffer

	w := parquet.NewGenericWriter[benchRow](&buf,
		parquet.MaxRowsPerRowGroup(numRows/3),
	)

	strs := make([]string, 500) // max cardinality
	for i := range strs {
		b := make([]byte, 50+rnd.Intn(50))
		for k := range b {
			b[k] = byte('a' + rnd.Intn(26))
		}
		strs[i] = string(b)
	}

	baseLen := 10
	for i := range numRows {
		if i != 0 && i%50 == 0 { // make pages with varying sizes
			baseLen = 1 + rnd.Intn(75)
		}
		vals := make([]string, baseLen+rnd.Intn(10))
		for j := range vals {
			vals[j] = strs[rnd.Intn(len(strs))]
		}

		_, err := w.Write([]benchRow{{K: fmt.Sprintf("key-%s", strs[rnd.Intn(len(strs))]), V: vals}})
		if err != nil {
			t.Fatalf("write row %d: %v", i, err)
		}
	}

	err := w.Close()
	if err != nil {
		t.Fatalf("close writer: %v", err)
	}

	return buf.Bytes()
}

func readModeToString(m parquet.ReadMode) string {
	switch m {
	case parquet.ReadModeSync:
		return "sync"
	case parquet.ReadModeAsync:
		return "async"
	default:
		return "unknown"
	}
}

// TestEmptyGroup tests reading and writing a struct with an empty group field
func TestEmptyGroup(t *testing.T) {
	type EmptyStruct struct {
		// This struct has no fields, creating an empty GROUP in parquet
	}

	type Record struct {
		ID    int         `parquet:"id"`
		Empty EmptyStruct `parquet:"empty"`
		Name  string      `parquet:"name"`
	}

	// Create some test data
	records := []Record{
		{ID: 1, Name: "Alice"},
		{ID: 2, Name: "Bob"},
		{ID: 3, Name: "Charlie"},
	}

	// Write to parquet
	buf := new(bytes.Buffer)
	writer := parquet.NewGenericWriter[Record](buf)

	n, err := writer.Write(records)
	if err != nil {
		t.Fatalf("failed to write records: %v", err)
	}
	if n != len(records) {
		t.Fatalf("expected to write %d records, wrote %d", len(records), n)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	t.Logf("Successfully wrote %d bytes", buf.Len())

	// Read back
	reader := parquet.NewGenericReader[Record](bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	readRecords := make([]Record, len(records))
	n, err = reader.Read(readRecords)
	// EOF is expected when all rows have been read
	if err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	if n != len(records) {
		t.Fatalf("expected to read %d records, read %d", len(records), n)
	}

	// Verify data
	for i := range readRecords {
		if readRecords[i].ID != records[i].ID {
			t.Errorf("record %d: expected ID=%d, got %d", i, records[i].ID, readRecords[i].ID)
		}
		if readRecords[i].Name != records[i].Name {
			t.Errorf("record %d: expected Name=%s, got %s", i, records[i].Name, readRecords[i].Name)
		}
	}

	t.Logf("Successfully read back %d records", n)
}

// TestOptionalEmptyGroup tests an optional empty group.
// Note: Optional empty groups cannot preserve nullability information
// since there are no columns to store definition levels.
func TestOptionalEmptyGroup(t *testing.T) {
	type EmptyStruct struct{}

	type Record struct {
		ID    int          `parquet:"id"`
		Empty *EmptyStruct `parquet:"empty,optional"`
		Name  string       `parquet:"name"`
	}

	records := []Record{
		{ID: 1, Empty: &EmptyStruct{}, Name: "Alice"},
		{ID: 2, Empty: nil, Name: "Bob"},
		{ID: 3, Empty: &EmptyStruct{}, Name: "Charlie"},
	}

	buf := new(bytes.Buffer)
	writer := parquet.NewGenericWriter[Record](buf)

	n, err := writer.Write(records)
	if err != nil {
		t.Fatalf("failed to write records: %v", err)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	reader := parquet.NewGenericReader[Record](bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	readRecords := make([]Record, len(records))
	n, err = reader.Read(readRecords)
	// EOF is expected when all rows have been read
	if err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	if n != len(records) {
		t.Fatalf("expected to read %d records, read %d", len(records), n)
	}

	// Verify basic data (ID and Name)
	for i := range readRecords {
		if readRecords[i].ID != records[i].ID {
			t.Errorf("record %d: expected ID=%d, got %d", i, records[i].ID, readRecords[i].ID)
		}
		if readRecords[i].Name != records[i].Name {
			t.Errorf("record %d: expected Name=%s, got %s", i, records[i].Name, readRecords[i].Name)
		}
		// Note: We cannot verify Empty field nullability because empty groups
		// have no columns to store definition levels
	}
}

// TestRepeatedEmptyGroup tests a repeated empty group.
// Note: Repeated empty groups cannot preserve array length information
// since there are no columns to store repetition levels.
func TestRepeatedEmptyGroup(t *testing.T) {
	type EmptyStruct struct{}

	type Record struct {
		ID      int           `parquet:"id"`
		Empties []EmptyStruct `parquet:"empties"`
		Name    string        `parquet:"name"`
	}

	records := []Record{
		{ID: 1, Empties: []EmptyStruct{{}}, Name: "Alice"},
		{ID: 2, Empties: []EmptyStruct{{}, {}}, Name: "Bob"},
		{ID: 3, Empties: nil, Name: "Charlie"},
	}

	buf := new(bytes.Buffer)
	writer := parquet.NewGenericWriter[Record](buf)

	n, err := writer.Write(records)
	if err != nil {
		t.Fatalf("failed to write records: %v", err)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	reader := parquet.NewGenericReader[Record](bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	readRecords := make([]Record, len(records))
	n, err = reader.Read(readRecords)
	// EOF is expected when all rows have been read
	if err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	if n != len(records) {
		t.Fatalf("expected to read %d records, read %d", len(records), n)
	}

	// Verify basic data (ID and Name)
	for i := range readRecords {
		if readRecords[i].ID != records[i].ID {
			t.Errorf("record %d: expected ID=%d, got %d", i, records[i].ID, readRecords[i].ID)
		}
		if readRecords[i].Name != records[i].Name {
			t.Errorf("record %d: expected Name=%s, got %s", i, records[i].Name, readRecords[i].Name)
		}
		// Note: We cannot verify Empties array length because empty groups
		// have no columns to store repetition levels
	}
}
