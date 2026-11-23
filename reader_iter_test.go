package parquet_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"maps"
	"reflect"
	"testing"

	"github.com/parquet-go/parquet-go"
)

type iterRow struct {
	ID    int64
	Name  string
	Tags  []string
	Attrs map[string]int
}

func makeIterRows(n int) []iterRow {
	rows := make([]iterRow, n)
	for i := range rows {
		rows[i] = iterRow{
			ID:   int64(i),
			Name: fmt.Sprintf("name-%d", i),
			Tags: []string{fmt.Sprintf("t-%d", i), "shared"},
			Attrs: map[string]int{
				"k": i,
			},
		}
	}
	return rows
}

func writeIterFile(t *testing.T, rows []iterRow, opts ...parquet.WriterOption) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[iterRow](&buf, opts...)
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func openIterReader(t *testing.T, rows []iterRow, opts ...parquet.WriterOption) *parquet.GenericReader[iterRow] {
	t.Helper()
	b := writeIterFile(t, rows, opts...)
	return parquet.NewGenericReader[iterRow](bytes.NewReader(b))
}

func snapshotIterRow(row iterRow) iterRow {
	row.Tags = append([]string(nil), row.Tags...)
	if row.Attrs != nil {
		attrs := make(map[string]int, len(row.Attrs))
		maps.Copy(attrs, row.Attrs)
		row.Attrs = attrs
	}
	return row
}

func collectIter(r *parquet.GenericReader[iterRow], stop int) (rows []iterRow, err error) {
	n := 0
	for row, err := range r.Iter() {
		if err != nil {
			var zero iterRow
			if !reflect.DeepEqual(row, zero) {
				return rows, fmt.Errorf("error yielded with row %+v: %w", row, err)
			}
			return rows, err
		}
		rows = append(rows, row)
		n++
		if stop > 0 && n == stop {
			return rows, nil
		}
	}
	return rows, nil
}

func TestGenericReaderIter(t *testing.T) {
	want := makeIterRows(250)
	reader := openIterReader(t, want)
	defer reader.Close()

	got, err := collectIter(reader, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("rows mismatch:\nwant[0]=%+v\ngot[0]=%+v\nlen want=%d got=%d", want[0], got[0], len(want), len(got))
	}

	// A second pass is at EOF and yields nothing, including no io.EOF.
	got, err = collectIter(reader, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("second iteration yielded %d rows", len(got))
	}

	// The raw reader is still positioned at the end, not stuck on a buffer.
	if _, err := reader.ReadRows(make([]parquet.Row, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("ReadRows at end: %v", err)
	}
}

func TestGenericReaderIterEmpty(t *testing.T) {
	reader := openIterReader(t, nil)
	defer reader.Close()

	n := 0
	for _, err := range reader.Iter() {
		if err != nil {
			t.Fatal(err)
		}
		n++
	}
	if n != 0 {
		t.Fatalf("empty file yielded %d rows", n)
	}
	if reader.NumRows() != 0 {
		t.Fatalf("NumRows = %d, want 0", reader.NumRows())
	}
}

func TestGenericReaderIterMultiRowGroup(t *testing.T) {
	want := makeIterRows(250)
	b := writeIterFile(t, want, parquet.MaxRowsPerRowGroup(10))
	f, err := parquet.OpenFile(bytes.NewReader(b), int64(len(b)))
	if err != nil {
		t.Fatal(err)
	}
	if groups := len(f.RowGroups()); groups < 2 {
		t.Fatalf("row groups = %d, want more than one", groups)
	}

	reader := parquet.NewGenericReader[iterRow](f)
	defer reader.Close()
	got, err := collectIter(reader, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("multi row group mismatch: len want=%d got=%d", len(want), len(got))
	}
}

func TestGenericReaderIterEarlyBreak(t *testing.T) {
	want := makeIterRows(250)
	stops := []int{1, 10, 127, 128, 129, 200, 249}

	t.Run("resume-iter", func(t *testing.T) {
		for _, stop := range stops {
			t.Run(fmt.Sprintf("stop-%d", stop), func(t *testing.T) {
				reader := openIterReader(t, want, parquet.MaxRowsPerRowGroup(10))
				defer reader.Close()

				head, err := collectIter(reader, stop)
				if err != nil {
					t.Fatal(err)
				}
				tail, err := collectIter(reader, 0)
				if err != nil {
					t.Fatal(err)
				}
				got := append(head, tail...)
				if !reflect.DeepEqual(want, got) {
					t.Fatalf("stop %d: len head=%d tail=%d", stop, len(head), len(tail))
				}
			})
		}
	})

	t.Run("resume-read", func(t *testing.T) {
		reader := openIterReader(t, want, parquet.MaxRowsPerRowGroup(10))
		defer reader.Close()

		head, err := collectIter(reader, 50)
		if err != nil {
			t.Fatal(err)
		}
		var tail []iterRow
		buf := make([]iterRow, 40)
		for {
			n, err := reader.Read(buf)
			// Read reuses map storage in buf on the next call. Snapshot before that.
			for _, row := range buf[:n] {
				tail = append(tail, snapshotIterRow(row))
			}
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			if n == 0 {
				t.Fatal("Read made no progress")
			}
		}
		got := append(head, tail...)
		if !reflect.DeepEqual(want, got) {
			t.Fatalf("resume Read: len head=%d tail=%d", len(head), len(tail))
		}
	})

	t.Run("seek", func(t *testing.T) {
		reader := openIterReader(t, want)
		defer reader.Close()

		if _, err := collectIter(reader, 5); err != nil {
			t.Fatal(err)
		}
		if err := reader.SeekToRow(20); err != nil {
			t.Fatal(err)
		}
		got, err := collectIter(reader, 0)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(want[20:], got) {
			t.Fatalf("after SeekToRow(20): len=%d", len(got))
		}
	})

	t.Run("reset", func(t *testing.T) {
		reader := openIterReader(t, want)
		defer reader.Close()

		if _, err := collectIter(reader, 5); err != nil {
			t.Fatal(err)
		}
		reader.Reset()
		got, err := collectIter(reader, 0)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(want, got) {
			t.Fatalf("after Reset: len=%d", len(got))
		}
	})

	t.Run("failed-seek-keeps-buffer", func(t *testing.T) {
		buf := parquet.NewGenericBuffer[iterRow]()
		if _, err := buf.Write(want); err != nil {
			t.Fatal(err)
		}
		group := rowsFuncGroup{
			RowGroup: buf,
			wrap: func(rows parquet.Rows) parquet.Rows {
				return failSeekRows{Rows: rows}
			},
		}
		reader := parquet.NewGenericRowGroupReader[iterRow](group)
		defer reader.Close()

		const stop = 5
		head, err := collectIter(reader, stop)
		if err != nil {
			t.Fatal(err)
		}
		if err := reader.SeekToRow(0); !errors.Is(err, errSeekFailed) {
			t.Fatalf("SeekToRow: %v", err)
		}
		tail, err := collectIter(reader, 0)
		if err != nil {
			t.Fatal(err)
		}
		got := append(head, tail...)
		if !reflect.DeepEqual(want, got) {
			t.Fatalf("after failed seek: len head=%d tail=%d", len(head), len(tail))
		}
	})

	t.Run("close", func(t *testing.T) {
		reader := openIterReader(t, want)
		if _, err := collectIter(reader, 5); err != nil {
			t.Fatal(err)
		}
		if err := reader.Close(); err != nil {
			t.Fatal(err)
		}
		buf := make([]iterRow, 8)
		n, err := reader.Read(buf)
		if n != 0 || !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("Read after Close: n=%d err=%v", n, err)
		}
	})
}

func TestGenericReaderIterError(t *testing.T) {
	want := makeIterRows(200)
	buf := parquet.NewGenericBuffer[iterRow]()
	if _, err := buf.Write(want); err != nil {
		t.Fatal(err)
	}
	const failAt = 50
	group := rowsFuncGroup{
		RowGroup: buf,
		wrap: func(rows parquet.Rows) parquet.Rows {
			return &limitRows{Rows: rows, left: failAt, err: io.ErrUnexpectedEOF}
		},
	}

	t.Run("yield", func(t *testing.T) {
		reader := parquet.NewGenericRowGroupReader[iterRow](group)
		defer reader.Close()

		got, err := collectIter(reader, 0)
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("err = %v", err)
		}
		if !reflect.DeepEqual(want[:failAt], got) {
			t.Fatalf("rows before error: got %d, want %d", len(got), failAt)
		}
	})

	t.Run("break-then-resume", func(t *testing.T) {
		reader := parquet.NewGenericRowGroupReader[iterRow](group)
		defer reader.Close()

		head, err := collectIter(reader, 10)
		if err != nil {
			t.Fatal(err)
		}
		tail, err := collectIter(reader, 0)
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("err = %v", err)
		}
		got := append(head, tail...)
		if !reflect.DeepEqual(want[:failAt], got) {
			t.Fatalf("rows across break: got %d, want %d", len(got), failAt)
		}
	})

	t.Run("immediate", func(t *testing.T) {
		group := rowsFuncGroup{
			RowGroup: buf,
			wrap: func(rows parquet.Rows) parquet.Rows {
				return &limitRows{Rows: rows, left: 0, err: io.ErrUnexpectedEOF}
			},
		}
		reader := parquet.NewGenericRowGroupReader[iterRow](group)
		defer reader.Close()

		got, err := collectIter(reader, 0)
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("err = %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("got %d rows before an immediate error", len(got))
		}
	})
}

func TestGenericReaderIterOpenError(t *testing.T) {
	reader := parquet.NewGenericReader[iterRow](bytes.NewReader([]byte("not a parquet file")))
	n := 0
	var got error
	for _, err := range reader.Iter() {
		n++
		got = err
	}
	if n != 1 || got == nil {
		t.Fatalf("iterations=%d err=%v", n, got)
	}
}

func TestGenericReaderIterNoProgress(t *testing.T) {
	buf := parquet.NewGenericBuffer[iterRow]()
	if _, err := buf.Write(makeIterRows(1)); err != nil {
		t.Fatal(err)
	}
	group := rowsFuncGroup{
		RowGroup: buf,
		wrap: func(rows parquet.Rows) parquet.Rows {
			return zeroRows{Rows: rows}
		},
	}
	reader := parquet.NewGenericRowGroupReader[iterRow](group)
	defer reader.Close()

	n := 0
	var got error
	for _, err := range reader.Iter() {
		n++
		got = err
		if n > 1 {
			t.Fatal("iterator did not stop after a zero-length read")
		}
	}
	if n != 1 || !errors.Is(got, io.ErrNoProgress) {
		t.Fatalf("iterations=%d err=%v", n, got)
	}
}

func TestGenericReaderIterReadRowsWhileBuffered(t *testing.T) {
	want := makeIterRows(32)
	reader := openIterReader(t, want)
	defer reader.Close()

	head, err := collectIter(reader, 3)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reader.ReadRows(make([]parquet.Row, 4)); err == nil {
		t.Fatal("ReadRows consumed or skipped rows buffered by Iter")
	}

	got := append([]iterRow{}, head...)
	buf := make([]iterRow, 8)
	for {
		n, err := reader.Read(buf)
		for _, row := range buf[:n] {
			got = append(got, snapshotIterRow(row))
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("len=%d", len(got))
	}
	if _, err := reader.ReadRows(make([]parquet.Row, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("ReadRows after the buffer was consumed: %v", err)
	}
}

type rowsFuncGroup struct {
	parquet.RowGroup
	wrap func(parquet.Rows) parquet.Rows
}

func (g rowsFuncGroup) Rows() parquet.Rows {
	return g.wrap(g.RowGroup.Rows())
}

type limitRows struct {
	parquet.Rows
	left int
	err  error
}

func (r *limitRows) ReadRows(rows []parquet.Row) (int, error) {
	if r.left <= 0 {
		return 0, r.err
	}
	if len(rows) > r.left {
		rows = rows[:r.left]
	}
	n, err := r.Rows.ReadRows(rows)
	if n > r.left {
		n = r.left
	}
	r.left -= n
	if err != nil {
		return n, err
	}
	if r.left == 0 {
		return n, r.err
	}
	return n, nil
}

type zeroRows struct{ parquet.Rows }

func (zeroRows) ReadRows([]parquet.Row) (int, error) { return 0, nil }

var errSeekFailed = errors.New("seek failed")

type failSeekRows struct{ parquet.Rows }

func (failSeekRows) SeekToRow(int64) error { return errSeekFailed }

func ExampleGenericReader_Iter() {
	type Row struct {
		ID   int64  `parquet:"id"`
		Name string `parquet:"name"`
	}

	buf := new(bytes.Buffer)
	if err := parquet.Write(buf, []Row{
		{ID: 1, Name: "Alice"},
		{ID: 2, Name: "Bob"},
	}); err != nil {
		log.Fatal(err)
	}

	reader := parquet.NewGenericReader[Row](bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	for row, err := range reader.Iter() {
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%d: %s\n", row.ID, row.Name)
	}

	// Output:
	// 1: Alice
	// 2: Bob
}
