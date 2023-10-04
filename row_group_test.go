package parquet_test

import (
	"bytes"
	"io"
	"reflect"
	"sort"
	"testing"

	"github.com/parquet-go/parquet-go"
)

func sortedRowGroup(options []parquet.RowGroupOption, rows ...interface{}) parquet.RowGroup {
	buf := parquet.NewBuffer(options...)
	for _, row := range rows {
		buf.Write(row)
	}
	sort.Stable(buf)
	return buf
}

type Person struct {
	FirstName utf8string
	LastName  utf8string
	Age       int
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
			if _, err := rows.ReadRows(rbuf); err != nil {
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
