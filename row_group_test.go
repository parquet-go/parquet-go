package parquet_test

import (
	"bytes"
	"io"
	"os"
	"reflect"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
)

func sortedRowGroup(options []parquet.RowGroupOption, rows ...any) parquet.RowGroup {
	buf := parquet.NewBuffer(options...)
	for _, row := range rows {
		buf.Write(row)
	}
	sort.Stable(buf)
	return buf
}

type Person struct {
	Age       int
	FirstName utf8string
	LastName  utf8string
}

type LastNameOnly struct {
	LastName utf8string
}

func newPeopleBuffer(people []Person) parquet.RowGroup {
	buffer := parquet.NewBuffer()
	for i := range people {
		buffer.Write(&people[i])
	}
	return buffer
}

func newPeopleFile(people []Person) parquet.RowGroup {
	buffer := new(bytes.Buffer)
	writer := parquet.NewWriter(buffer)
	for i := range people {
		writer.Write(&people[i])
	}
	writer.Close()
	reader := bytes.NewReader(buffer.Bytes())
	f, err := parquet.OpenFile(reader, reader.Size())
	if err != nil {
		panic(err)
	}
	return f.RowGroups()[0]
}

func TestSeekToRow(t *testing.T) {
	for _, config := range []struct {
		name        string
		newRowGroup func([]Person) parquet.RowGroup
	}{
		{name: "buffer", newRowGroup: newPeopleBuffer},
		{name: "file", newRowGroup: newPeopleFile},
	} {
		t.Run(config.name, func(t *testing.T) { testSeekToRow(t, config.newRowGroup) })
	}
}

func testSeekToRow(t *testing.T, newRowGroup func([]Person) parquet.RowGroup) {
	err := quickCheck(func(people []Person) bool {
		if len(people) == 0 { // TODO: fix creation of empty parquet files
			return true
		}
		rowGroup := newRowGroup(people)
		rows := rowGroup.Rows()
		rbuf := make([]parquet.Row, 1)
		pers := Person{}
		schema := parquet.SchemaOf(&pers)
		defer rows.Close()

		for i := range people {
			if err := rows.SeekToRow(int64(i)); err != nil {
				t.Errorf("seeking to row %d: %+v", i, err)
				return false
			}
			if n, err := rows.ReadRows(rbuf); n != len(rbuf) {
				t.Errorf("reading row %d: %+v", i, err)
				return false
			}
			if err := schema.Reconstruct(&pers, rbuf[0]); err != nil {
				t.Errorf("deconstructing row %d: %+v", i, err)
				return false
			}
			if !reflect.DeepEqual(&pers, &people[i]) {
				t.Errorf("row %d mismatch", i)
				return false
			}
		}

		return true
	})
	if err != nil {
		t.Error(err)
	}
}

func selfRowGroup(rowGroup parquet.RowGroup) parquet.RowGroup {
	return rowGroup
}

func fileRowGroup(rowGroup parquet.RowGroup) parquet.RowGroup {
	buffer := new(bytes.Buffer)
	writer := parquet.NewWriter(buffer)
	if _, err := writer.WriteRowGroup(rowGroup); err != nil {
		panic(err)
	}
	if err := writer.Close(); err != nil {
		panic(err)
	}
	reader := bytes.NewReader(buffer.Bytes())
	f, err := parquet.OpenFile(reader, reader.Size())
	if err != nil {
		panic(err)
	}
	g := f.RowGroups()
	if len(g) > 0 {
		return g[0]
	}
	// There is a test checking for a panic when merging empty row groups. One of
	// the input is an empty row group which leads to this path.
	//
	// It is unnecessary to also return an empty row group here because the
	// behavior is triggered by custom row group implementation.
	//
	// buffer  scenario check  should be sufficient to cover for the issue.
	return nil
}

func TestWriteRowGroupClosesRows(t *testing.T) {
	var rows []*wrappedRows
	rg := wrappedRowGroup{
		RowGroup: newPeopleFile([]Person{{}}),
		rowsCallback: func(r parquet.Rows) parquet.Rows {
			wrapped := &wrappedRows{Rows: r}
			rows = append(rows, wrapped)
			return wrapped
		},
	}
	writer := parquet.NewWriter(io.Discard)
	if _, err := writer.WriteRowGroup(rg); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if !r.closed {
			t.Fatal("rows not closed")
		}
	}
}

type highLatencyReaderAt struct {
	reader  io.ReaderAt
	latency time.Duration
}

func (r *highLatencyReaderAt) ReadAt(p []byte, off int64) (n int, err error) {
	time.Sleep(r.latency)
	return r.reader.ReadAt(p, off)
}

type measuredReaderAt struct {
	reader      io.ReaderAt
	reads       atomic.Int64
	bytes       atomic.Int64
	inflight    atomic.Int64
	maxInflight atomic.Int64
}

func (r *measuredReaderAt) ReadAt(p []byte, off int64) (int, error) {
	r.reads.Add(1)
	inflight := r.inflight.Add(1)
	for {
		maxInflight := r.maxInflight.Load()
		if inflight < maxInflight {
			break
		}
		if r.maxInflight.CompareAndSwap(maxInflight, inflight) {
			break
		}
	}
	n, err := r.reader.ReadAt(p, off)
	r.inflight.Add(-1)
	r.bytes.Add(int64(n))
	return n, err
}

func BenchmarkReadModeAsync(b *testing.B) {
	f, err := os.ReadFile("testdata/file.parquet")
	if err != nil {
		b.Fatal(err)
	}

	r := &measuredReaderAt{
		reader: &highLatencyReaderAt{
			reader:  bytes.NewReader(f),
			latency: 10 * time.Millisecond,
		},
	}

	p, err := parquet.OpenFile(r, int64(len(f)),
		parquet.OptimisticRead(true),
		parquet.SkipMagicBytes(true),
		parquet.SkipPageIndex(true),
		parquet.SkipBloomFilters(true),
		parquet.FileReadMode(parquet.ReadModeAsync),
	)
	if err != nil {
		b.Fatal(err)
	}

	rowGroups := p.RowGroups()
	if len(rowGroups) != 1 {
		b.Fatalf("unexpected number of row groups: %d", len(rowGroups))
	}

	numRows := int64(0)
	rbuf := make([]parquet.Row, 100)
	rows := rowGroups[0].Rows()
	defer rows.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n, err := rows.ReadRows(rbuf)
		if err != nil {
			if err == io.EOF {
				err = rows.SeekToRow(0)
			}
			if err == nil {
				continue
			}
			b.Fatal(err)
		}
		numRows += int64(n)
		if err := rows.SeekToRow(numRows + 10); err != nil {
			b.Fatal(err)
		}
	}

	maxInflight := r.maxInflight.Load()
	if maxInflight <= 1 {
		b.Errorf("max inflight requests: %d", maxInflight)
	}

	b.ReportMetric(float64(numRows), "rows")
	b.ReportMetric(float64(r.reads.Load()), "reads")
	b.ReportMetric(float64(r.bytes.Load()), "bytes")
}
