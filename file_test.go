package parquet_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

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

func TestSeekToRowReally(t *testing.T) {
	type A struct {
		IndexDict string `parquet:",dict"`
		//		IndexNoDict string `parquet:""`
	}

	b := new(bytes.Buffer)
	w := parquet.NewGenericWriter[A](b, parquet.PageBufferSize(10))

	// Create and write 1000 rows with index-based names
	const totalRows = 1000
	for i := range totalRows {
		val := fmt.Sprintf("%d", i)

		_, err := w.Write([]A{
			{
				IndexDict: val,
				//IndexNoDict: val,
			},
		})
		if err != nil {
			t.Fatal(err)
		}

		if i+1%3 == 0 {
			err := w.Flush()
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	pf, err := parquet.OpenFile(bytes.NewReader(b.Bytes()), int64(b.Len()), parquet.ReadBufferSize(3), parquet.SkipPageIndex(true)) // read buffer size of 3 should expose any issues with buffer overread
	if err != nil {
		t.Fatal(err)
	}

	seekTos := make([]int, 999)
	for i := range seekTos {
		seekTos[i] = i + 1
	}
	slices.Reverse(seekTos)
	rand.Shuffle(len(seekTos), func(i, j int) {
		seekTos[i], seekTos[j] = seekTos[j], seekTos[i]
	})

	// test dict column
	rg := pf.RowGroups()[0]
	cc := rg.ColumnChunks()[0]
	pages := cc.Pages()

	for _, seekTo := range seekTos {
		if err := pages.SeekToRow(int64(seekTo)); err != nil {
			t.Fatal(err)
		}

		page, err := pages.ReadPage()
		if err != nil && err != io.EOF {
			t.Fatal(err)
		}
		if page == nil {
			t.Fatalf("expected page, got nil: %d", seekTo)
		}

		vr := page.Values()
		values := make([]parquet.Value, 1)
		n, err := vr.ReadValues(values)
		if err != nil && err != io.EOF {
			t.Fatal(err)
		}
		if n != 1 {
			t.Fatalf("expected 1 value, got %d", n)
		}

		actual := values[0].String()
		expected := fmt.Sprintf("%d", seekTo)
		if actual != expected {
			t.Fatalf("expected %s, got %s", expected, actual)
		}
		parquet.Release(page)
	}
	pages.Close()

	// test row reader
	pf, err = parquet.OpenFile(bytes.NewReader(b.Bytes()), int64(b.Len()), parquet.ReadBufferSize(3)) // read buffer size of 3 should expose any issues with buffer overread
	if err != nil {
		t.Fatal(err)
	}

	reader := parquet.NewGenericReader[A](pf)
	for _, seekTo := range seekTos {
		if err := reader.SeekToRow(int64(seekTo)); err != nil {
			t.Fatal(err)
		}

		var n int
		rows := make([]A, 1)
		if n, err = reader.Read(rows); err != nil && err != io.EOF {
			t.Fatal(err)
		}
		if n != 1 {
			t.Fatalf("expected 1 row, got %d, seekTo: %d", n, seekTo)
		}

		actual := rows[0].IndexDict
		expected := fmt.Sprintf("%d", seekTo)
		if actual != expected {
			t.Fatalf("expected %s, got %s, seekTos: %v", expected, actual, seekTos)
		}

		/*		actual = rows[0].IndexNoDict
				expected = fmt.Sprintf("%d", seekTo)
				if actual != expected {
					t.Fatalf("expected %s, got %s", expected, actual)
				}*/
	}
	reader.Close()
}
