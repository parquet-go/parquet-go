package parquet_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/hexops/gotextdiff"
	"github.com/hexops/gotextdiff/myers"
	"github.com/hexops/gotextdiff/span"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
	"github.com/parquet-go/parquet-go/encoding"
	"github.com/parquet-go/parquet-go/internal/unsafecast"
)

const (
	v1 = 1
	v2 = 2
)

func BenchmarkGenericWriter(b *testing.B) {
	benchmarkGenericWriter[benchmarkRowType](b)
	benchmarkGenericWriter[booleanColumn](b)
	benchmarkGenericWriter[int32Column](b)
	benchmarkGenericWriter[int64Column](b)
	benchmarkGenericWriter[floatColumn](b)
	benchmarkGenericWriter[doubleColumn](b)
	benchmarkGenericWriter[byteArrayColumn](b)
	benchmarkGenericWriter[fixedLenByteArrayColumn](b)
	benchmarkGenericWriter[stringColumn](b)
	benchmarkGenericWriter[indexedStringColumn](b)
	benchmarkGenericWriter[uuidColumn](b)
	benchmarkGenericWriter[timeColumn](b)
	benchmarkGenericWriter[timeInMillisColumn](b)
	benchmarkGenericWriter[mapColumn](b)
	benchmarkGenericWriter[decimalColumn](b)
	benchmarkGenericWriter[contact](b)
	benchmarkGenericWriter[paddedBooleanColumn](b)
	benchmarkGenericWriter[optionalInt32Column](b)
	benchmarkGenericWriter[repeatedInt32Column](b)
}

func benchmarkGenericWriter[Row generator[Row]](b *testing.B) {
	var model Row
	b.Run(reflect.TypeOf(model).Name(), func(b *testing.B) {
		prng := rand.New(rand.NewSource(0))
		rows := make([]Row, benchmarkNumRows)
		for i := range rows {
			rows[i] = rows[i].generate(prng)
		}

		b.Run("go1.17", func(b *testing.B) {
			writer := parquet.NewWriter(io.Discard, parquet.SchemaOf(rows[0]))
			i := 0
			benchmarkRowsPerSecond(b, func() int {
				for range benchmarkRowsPerStep {
					if err := writer.Write(&rows[i]); err != nil {
						b.Fatal(err)
					}
				}

				i += benchmarkRowsPerStep
				i %= benchmarkNumRows

				if i == 0 {
					writer.Close()
					writer.Reset(io.Discard)
				}
				return benchmarkRowsPerStep
			})
		})

		b.Run("go1.18", func(b *testing.B) {
			writer := parquet.NewGenericWriter[Row](io.Discard)
			i := 0
			benchmarkRowsPerSecond(b, func() int {
				n, err := writer.Write(rows[i : i+benchmarkRowsPerStep])
				if err != nil {
					b.Fatal(err)
				}

				i += benchmarkRowsPerStep
				i %= benchmarkNumRows

				if i == 0 {
					writer.Close()
					writer.Reset(io.Discard)
				}
				return n
			})
		})
	})
}

func TestIssue249(t *testing.T) {
	type noExportedFields struct {
		a, b, c string
		x, y, z []int32
	}
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[*noExportedFields](&buf)
	_, err := w.Write([]*noExportedFields{
		{a: "a", b: "c", c: "c", x: []int32{0, 1, 2}},
		{a: "a", b: "c", c: "c", x: []int32{0, 1, 2}},
		{a: "a", b: "c", c: "c", x: []int32{0, 1, 2}},
	})
	if err == nil {
		t.Fatal("expecting Write to return an error, but it did not")
	}
	if !strings.Contains(err.Error(), "noExportedFields: it has no columns (maybe it has no exported fields)") {
		t.Fatalf("expecting Write to return an error describing that the input type has no columns; instead got: %v", err)
	}
}

func TestWriteRowsColumns(t *testing.T) {
	type simpleFlat struct {
		A, B, C int64
		S, T, U string
	}
	t.Run("simple", func(t *testing.T) {
		testWriteRowsColumns[simpleFlat](t)
	})
	type complexNested struct {
		A  []int64
		S  []string
		M  map[string]string
		R1 []struct {
			B  []int64
			T  []string
			R2 []simpleFlat
		}
	}
	t.Run("complex", func(t *testing.T) {
		testWriteRowsColumns[complexNested](t)
	})
}

func testWriteRowsColumns[T any](t *testing.T) {
	var zero T
	const numRows = 1000
	data := rowsOf(numRows, zero)
	rowSlice := asRowSlice[T](t, data)
	colSlice := transpose(rowSlice)
	testCases := []struct {
		name  string
		write func(*testing.T, *parquet.GenericWriter[T], [][]parquet.Value) (int, error)
		data  [][]parquet.Value
	}{
		{
			name: "WriteRows",
			write: func(_ *testing.T, w *parquet.GenericWriter[T], vals [][]parquet.Value) (int, error) {
				// annoying that we can't use normal type conversions between
				// []Row and [][]Value since they are structurally identical
				rows := unsafecast.Slice[parquet.Row](vals)
				return w.WriteRows(rows)
			},
			data: unsafecast.Slice[[]parquet.Value](rowSlice),
		},
		{
			name: "ColumnWriter.WriteRowValues",
			write: func(t *testing.T, w *parquet.GenericWriter[T], vals [][]parquet.Value) (int, error) {
				t.Helper()
				var numRows int
				for i, col := range vals {
					num, err := w.ColumnWriters()[i].WriteRowValues(col)
					if err != nil {
						return 0, err
					}
					if i == 0 {
						numRows = num
					} else if numRows != num {
						t.Errorf("column %d disagrees with number of rows written in column %d", i, 0)
					}
				}
				return numRows, nil
			},
			data: colSlice,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := parquet.NewGenericWriter[T](&buf)
			n, err := testCase.write(t, w, testCase.data)
			if err != nil {
				t.Fatal(err)
			}
			if n != numRows {
				t.Errorf("wrote %d rows, but expected to write %d", n, numRows)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			// Now read the data back and make sure it matches the input data.
			r := parquet.NewGenericReader[T](bytes.NewReader(buf.Bytes()))
			roundTripped := readAllRows(t, r, numRows)
			assertRowsEqualByRow(t, roundTripped, rowSlice)
		})
	}
}

func asRowSlice[T any](t testing.TB, data rows) []parquet.Row {
	t.Helper()

	typedRows := make([]T, len(data))
	for i := range data {
		typedRows[i] = data[i].(T)
	}

	var buf bytes.Buffer
	w := parquet.NewGenericWriter[T](&buf)
	n, err := w.Write(typedRows)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(typedRows) {
		t.Errorf("wrote %d rows, but expected to write %d", n, len(typedRows))
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r := parquet.NewGenericReader[T](bytes.NewReader(buf.Bytes()))
	return readAllRows(t, r, len(typedRows))
}

func readAllRows[T any](t testing.TB, r *parquet.GenericReader[T], numRows int) []parquet.Row {
	t.Helper()
	if r.NumRows() != int64(numRows) {
		t.Errorf("reader reports %d rows, but expected %d", r.NumRows(), numRows)
	}
	rows := make([]parquet.Row, numRows)
	n, err := r.ReadRows(rows)
	if n != numRows {
		t.Errorf("wrote %d rows, but expected to write %d", n, numRows)
	}
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	return rows
}

func transpose(rows []parquet.Row) [][]parquet.Value {
	var cols [][]parquet.Value
	for i, row := range rows {
		if i == 0 {
			var columnCount int
			row.Range(func(columnIndex int, columnValues []parquet.Value) bool {
				columnCount++
				return true
			})
			cols = make([][]parquet.Value, columnCount)
		}
		row.Range(func(columnIndex int, columnValues []parquet.Value) bool {
			cols[columnIndex] = append(cols[columnIndex], columnValues...)
			return true
		})
	}
	return cols
}

func TestIssueSegmentio272(t *testing.T) {
	type T2 struct {
		X string `parquet:",dict,optional"`
	}

	type T1 struct {
		TA *T2
		TB *T2
	}

	type T struct {
		T1 *T1
	}

	const nRows = 1

	row := T{
		T1: &T1{
			TA: &T2{
				X: "abc",
			},
		},
	}

	rows := make([]T, nRows)
	for i := range rows {
		rows[i] = row
	}

	b := new(bytes.Buffer)
	w := parquet.NewGenericWriter[T](b)

	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	f := bytes.NewReader(b.Bytes())
	r := parquet.NewGenericReader[T](f)

	parquetRows := make([]parquet.Row, nRows)
	n, err := r.ReadRows(parquetRows)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if n != nRows {
		t.Fatalf("wrong number of rows read: want=%d got=%d", nRows, n)
	}
	for _, r := range parquetRows {
		if d := r[0].DefinitionLevel(); d != 3 {
			t.Errorf("wrong definition level for column 0: %d", d)
		}
		if d := r[1].DefinitionLevel(); d != 1 {
			t.Errorf("wrong definition level for column 1: %d", d)
		}
	}
}

func TestIssueSegmentio279(t *testing.T) {
	type T2 struct {
		Id   int    `parquet:",plain,optional"`
		Name string `parquet:",plain,optional"`
	}

	type T1 struct {
		TA []*T2
	}

	type T struct {
		T1 *T1
	}

	const nRows = 1

	row := T{
		T1: &T1{
			TA: []*T2{
				{
					Id:   43,
					Name: "john",
				},
			},
		},
	}

	rows := make([]T, nRows)
	for i := range rows {
		rows[i] = row
	}

	b := new(bytes.Buffer)
	w := parquet.NewGenericWriter[T](b)

	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	f := bytes.NewReader(b.Bytes())
	r := parquet.NewGenericReader[T](f)

	parquetRows := make([]parquet.Row, nRows)
	n, err := r.ReadRows(parquetRows)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if n != nRows {
		t.Fatalf("wrong number of rows read: want=%d got=%d", nRows, n)
	}
	for _, r := range parquetRows {
		if d := r[0].DefinitionLevel(); d != 3 {
			t.Errorf("wrong definition level for column 0: %d", d)
		}
		if d := r[1].DefinitionLevel(); d != 3 {
			t.Errorf("wrong definition level for column 1: %d", d)
		}
	}
}

func TestIssueSegmentio302(t *testing.T) {
	tests := []struct {
		name string
		fn   func(t *testing.T)
	}{
		{
			name: "SimpleMap",
			fn: func(t *testing.T) {
				type M map[string]int

				type T struct {
					M M `parquet:","`
				}

				b := new(bytes.Buffer)
				_ = parquet.NewGenericWriter[T](b)

			},
		},

		{
			name: "MapWithValueTag",
			fn: func(t *testing.T) {
				type M map[string]int

				type T struct {
					M M `parquet:"," parquet-value:",zstd"`
				}

				b := new(bytes.Buffer)
				_ = parquet.NewGenericWriter[T](b)

			},
		},

		{
			name: "MapWithOptionalTag",
			fn: func(t *testing.T) {
				type M map[string]int

				type T struct {
					M M `parquet:",optional"`
				}

				b := new(bytes.Buffer)
				w := parquet.NewGenericWriter[T](b)
				expect := []T{
					{
						M: M{
							"Holden": 1,
							"Naomi":  2,
						},
					},
					{
						M: nil,
					},
					{
						M: M{
							"Naomi":  1,
							"Holden": 2,
						},
					},
				}
				_, err := w.Write(expect)
				if err != nil {
					t.Fatal(err)
				}
				if err = w.Close(); err != nil {
					t.Fatal(err)
				}

				bufReader := bytes.NewReader(b.Bytes())
				r := parquet.NewGenericReader[T](bufReader)
				values := make([]T, 3)
				_, err = r.Read(values)
				if !reflect.DeepEqual(expect, values) {
					t.Fatalf("values do not match.\n\texpect: %v\n\tactual: %v", expect, values)
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, test.fn)
	}
}

func TestIssueSegmentio347Writer(t *testing.T) {
	type TestType struct {
		Key int
	}

	b := new(bytes.Buffer)
	// instantiating with concrete type shouldn't panic
	_ = parquet.NewGenericWriter[TestType](b)

	// instantiating with schema and interface type parameter shouldn't panic
	schema := parquet.SchemaOf(TestType{})
	_ = parquet.NewGenericWriter[any](b, schema)

	defer func() {
		if r := recover(); r == nil {
			t.Errorf("instantiating generic buffer without schema and with interface " +
				"type parameter should panic")
		}
	}()
	_ = parquet.NewGenericWriter[any](b)
}

func TestIssueSegmentio375(t *testing.T) {
	type Row struct{ FirstName, LastName string }

	output := new(bytes.Buffer)
	writer := parquet.NewGenericWriter[Row](output, parquet.MaxRowsPerRowGroup(10))

	rows := make([]Row, 100)
	for i := range rows {
		rows[i] = Row{
			FirstName: "0123456789"[i%10 : i%10+1],
			LastName:  "foo",
		}
	}

	n, err := writer.Write(rows)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(rows) {
		t.Fatal("wrong number of rows written:", n)
	}

	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	f, err := parquet.OpenFile(bytes.NewReader(output.Bytes()), int64(output.Len()))
	if err != nil {
		t.Fatal(err)
	}

	rowGroups := f.RowGroups()
	if len(rowGroups) != 10 {
		t.Errorf("wrong number of row groups in parquet file: want=10 got=%d", len(rowGroups))
	}
}

func TestGenericSetKeyValueMetadata(t *testing.T) {
	testKey := "test-key"
	testValue := "test-value"

	type Row struct{ FirstName, LastName string }

	output := new(bytes.Buffer)
	writer := parquet.NewGenericWriter[Row](output, parquet.MaxRowsPerRowGroup(10))

	rows := []Row{
		{FirstName: "First", LastName: "Last"},
	}

	_, err := writer.Write(rows)
	if err != nil {
		t.Fatal(err)
	}

	writer.SetKeyValueMetadata(testKey, testValue)

	err = writer.Close()
	if err != nil {
		t.Fatal(err)
	}

	f, err := parquet.OpenFile(bytes.NewReader(output.Bytes()), int64(output.Len()))
	if err != nil {
		t.Fatal(err)
	}

	value, ok := f.Lookup(testKey)
	if !ok {
		t.Fatalf("key/value metadata should have included %q", testKey)
	}
	if value != testValue {
		t.Errorf("expected %q, got %q", testValue, value)
	}
}

func scanParquetFile(f *os.File) error {
	s, err := f.Stat()
	if err != nil {
		return err
	}

	p, err := parquet.OpenFile(f, s.Size())
	if err != nil {
		return err
	}

	return scanParquetValues(p.Root())
}

func scanParquetValues(col *parquet.Column) error {
	return forEachColumnValue(col, func(leaf *parquet.Column, value parquet.Value) error {
		fmt.Printf("%s > %+v\n", strings.Join(leaf.Path(), "."), value)
		return nil
	})
}

func generateParquetFile(rows rows, options ...parquet.WriterOption) (string, []byte, error) {
	tmp, err := os.CreateTemp("/tmp", "*.parquet")
	if err != nil {
		return "", nil, err
	}
	defer tmp.Close()
	path := tmp.Name()
	defer os.Remove(path)

	writerOptions := []parquet.WriterOption{
		parquet.PageBufferSize(20),
		parquet.DataPageStatistics(true),
	}
	writerOptions = append(writerOptions, options...)

	if err := writeParquetFile(tmp, rows, writerOptions...); err != nil {
		return "", nil, err
	}

	if err := scanParquetFile(tmp); err != nil {
		return "", nil, err
	}

	var outputParts [][]byte
	// Ideally, we could add the "cat" command here and validate each row in the parquet
	// file using the parquet CLI tool. However, it seems to have a number of bugs related
	// to reading repeated fields, so we cannot reliably do this validation for now.
	// See https://issues.apache.org/jira/projects/PARQUET/issues/PARQUET-2181 and others.
	for _, cmd := range []string{"meta", "pages"} {
		out, err := parquetCLI(cmd, path)
		if err != nil {
			return "", nil, err
		}
		outputParts = append(outputParts, out)
	}
	return path, bytes.Join(outputParts, []byte("")), nil
}

type firstAndLastName struct {
	FirstName string `parquet:"first_name,dict,zstd"`
	LastName  string `parquet:"last_name,delta,zstd"`
}

type timeseries struct {
	Name      string  `parquet:"name,dict"`
	Timestamp int64   `parquet:"timestamp,delta"`
	Value     float64 `parquet:"value"`
}

type timeseriesNoEncoding struct {
	Name      string  `parquet:"name"`
	Timestamp int64   `parquet:"timestamp"`
	Value     float64 `parquet:"value"`
}

type event struct {
	Name     string  `parquet:"name,dict"`
	Type     string  `parquet:"-"`
	Value    float64 `parquet:"value"`
	Category string  `parquet:"-"`
}

var writerTests = []struct {
	scenario           string
	version            int
	codec              compress.Codec
	defaultEncoding    encoding.Encoding
	defaultEncodingFor map[parquet.Kind]encoding.Encoding
	rows               []any
	dump               string
}{
	{
		scenario: "page v1 with dictionary encoding",
		version:  v1,
		rows: []any{
			&firstAndLastName{FirstName: "Han", LastName: "Solo"},
			&firstAndLastName{FirstName: "Leia", LastName: "Skywalker"},
			&firstAndLastName{FirstName: "Luke", LastName: "Skywalker"},
		},
		dump: `
File path:  {file-path}
Created by: github.com/parquet-go/parquet-go
Properties: (none)
Schema:
message firstAndLastName {
  required binary first_name (STRING);
  required binary last_name (STRING);
}


Row group 0:  count: 3  107.67 B records  start: 4  total(compressed): 323 B total(uncompressed):299 B
--------------------------------------------------------------------------------
            type      encodings count     avg size   nulls   min / max
first_name  BINARY    Z _ R     3         38.00 B            "Han" / "Luke"
last_name   BINARY    Z   D     3         69.67 B            "Skywalker" / "Solo"


Column: first_name
--------------------------------------------------------------------------------
  page   type  enc  count   avg size   size       rows     nulls   min / max
  0-D    dict  Z _  3       7.67 B     23 B
  0-1    data  Z R  3       2.33 B     7 B                         "Han" / "Luke"


Column: last_name
--------------------------------------------------------------------------------
  page   type  enc  count   avg size   size       rows     nulls   min / max
  0-0    data  Z D  2       28.00 B    56 B                        "Skywalker" / "Solo"
  0-1    data  Z D  1       19.00 B    19 B                        "Skywalker" / "Skywalker"

`,
	},

	{ // same as the previous test but uses page v2 where data pages aren't compressed
		scenario: "page v2 with dictionary encoding",
		version:  v2,
		rows: []any{
			&firstAndLastName{FirstName: "Han", LastName: "Solo"},
			&firstAndLastName{FirstName: "Leia", LastName: "Skywalker"},
			&firstAndLastName{FirstName: "Luke", LastName: "Skywalker"},
		},
		dump: `
File path:  {file-path}
Created by: github.com/parquet-go/parquet-go
Properties: (none)
Schema:
message firstAndLastName {
  required binary first_name (STRING);
  required binary last_name (STRING);
}


Row group 0:  count: 3  109.67 B records  start: 4  total(compressed): 329 B total(uncompressed):314 B
--------------------------------------------------------------------------------
            type      encodings count     avg size   nulls   min / max
first_name  BINARY    Z _ R     3         36.67 B            "Han" / "Luke"
last_name   BINARY    Z   D     3         73.00 B            "Skywalker" / "Solo"


Column: first_name
--------------------------------------------------------------------------------
  page   type  enc  count   avg size   size       rows     nulls   min / max
  0-D    dict  Z _  3       7.67 B     23 B
  0-1    data  _ R  3       2.33 B     7 B        3        0       "Han" / "Luke"


Column: last_name
--------------------------------------------------------------------------------
  page   type  enc  count   avg size   size       rows     nulls   min / max
  0-0    data  _ D  2       28.00 B    56 B       2        0       "Skywalker" / "Solo"
  0-1    data  _ D  1       19.00 B    19 B       1        0       "Skywalker" / "Skywalker"

`,
	},

	{
		scenario: "timeseries with delta encoding",
		version:  v2,
		codec:    &parquet.Gzip,
		rows: []any{
			timeseries{Name: "http_request_total", Timestamp: 1639444033, Value: 100},
			timeseries{Name: "http_request_total", Timestamp: 1639444058, Value: 0},
			timeseries{Name: "http_request_total", Timestamp: 1639444085, Value: 42},
			timeseries{Name: "http_request_total", Timestamp: 1639444093, Value: 1},
			timeseries{Name: "http_request_total", Timestamp: 1639444101, Value: 2},
			timeseries{Name: "http_request_total", Timestamp: 1639444108, Value: 5},
			timeseries{Name: "http_request_total", Timestamp: 1639444133, Value: 4},
			timeseries{Name: "http_request_total", Timestamp: 1639444137, Value: 5},
			timeseries{Name: "http_request_total", Timestamp: 1639444141, Value: 6},
			timeseries{Name: "http_request_total", Timestamp: 1639444144, Value: 10},
		},
		dump: `
File path:  {file-path}
Created by: github.com/parquet-go/parquet-go
Properties: (none)
Schema:
message timeseries {
  required binary name (STRING);
  required int64 timestamp (INTEGER(64,true));
  required double value;
}


Row group 0:  count: 10  121.70 B records  start: 4  total(compressed): 1.188 kB total(uncompressed):1.312 kB
--------------------------------------------------------------------------------
           type      encodings count     avg size   nulls   min / max
name       BINARY    G _ R     10        29.00 B            "http_request_total" / "http_request_total"
timestamp  INT64     G   D     10        46.70 B            "1639444033" / "1639444144"
value      DOUBLE    G   _     10        46.00 B            "-0.0" / "100.0"


Column: name
--------------------------------------------------------------------------------
  page   type  enc  count   avg size   size       rows     nulls   min / max
  0-D    dict  G _  1       22.00 B    22 B
  0-1    data  _ R  5       0.40 B     2 B        5        0       "http_request_total" / "http_request_total"
  0-2    data  _ R  5       0.40 B     2 B        5        0       "http_request_total" / "http_request_total"


Column: timestamp
--------------------------------------------------------------------------------
  page   type  enc  count   avg size   size       rows     nulls   min / max
  0-0    data  _ D  3       47.33 B    142 B      3        0       "1639444033" / "1639444085"
  0-1    data  _ D  3       47.33 B    142 B      3        0       "1639444093" / "1639444108"
  0-2    data  _ D  3       47.33 B    142 B      3        0       "1639444133" / "1639444141"
  0-3    data  _ D  1       9.00 B     9 B        1        0       "1639444144" / "1639444144"


Column: value
--------------------------------------------------------------------------------
  page   type  enc  count   avg size   size       rows     nulls   min / max
  0-0    data  _ _  3       8.00 B     24 B       3        0       "-0.0" / "100.0"
  0-1    data  _ _  3       8.00 B     24 B       3        0       "1.0" / "5.0"
  0-2    data  _ _  3       8.00 B     24 B       3        0       "4.0" / "6.0"
  0-3    data  _ _  1       8.00 B     8 B        1        0       "10.0" / "10.0"

`,
	},

	{
		scenario: "example from the twitter blog (v1)",
		version:  v1,
		rows: []any{
			AddressBook{
				Owner: "Julien Le Dem",
				OwnerPhoneNumbers: []string{
					"555 123 4567",
					"555 666 1337",
				},
				Contacts: []Contact{
					{
						Name:        "Dmitriy Ryaboy",
						PhoneNumber: "555 987 6543",
					},
					{
						Name: "Chris Aniszczyk",
					},
				},
			},
			AddressBook{
				Owner:             "A. Nonymous",
				OwnerPhoneNumbers: nil,
			},
		},
		dump: `
File path:  {file-path}
Created by: github.com/parquet-go/parquet-go
Properties: (none)
Schema:
message AddressBook {
  required binary owner (STRING);
  repeated binary ownerPhoneNumbers (STRING);
  repeated group contacts {
    required binary name (STRING);
    optional binary phoneNumber (STRING);
  }
}


Row group 0:  count: 2  384.00 B records  start: 4  total(compressed): 768 B total(uncompressed):691 B
--------------------------------------------------------------------------------
                      type      encodings count     avg size   nulls   min / max
owner                 BINARY    Z         2         70.00 B            "A. Nonymous" / "Julien Le Dem"
ownerPhoneNumbers     BINARY    G         3         80.33 B    1       "555 123 4567" / "555 666 1337"
contacts.name         BINARY    _         3         70.00 B    1       "Chris Aniszczyk" / "Dmitriy Ryaboy"
contacts.phoneNumber  BINARY    Z         3         59.00 B    2       "555 987 6543" / "555 987 6543"


Column: owner
--------------------------------------------------------------------------------
  page   type  enc  count   avg size   size       rows     nulls   min / max
  0-0    data  Z D  2       25.00 B    50 B                        "A. Nonymous" / "Julien Le Dem"


Column: ownerPhoneNumbers
--------------------------------------------------------------------------------
  page   type  enc  count   avg size   size       rows     nulls   min / max
  0-0    data  Z D  2       32.00 B    64 B                        "555 123 4567" / "555 666 1337"
  0-1    data  Z D  1       17.00 B    17 B                1


Column: contacts.name
--------------------------------------------------------------------------------
  page   type  enc  count   avg size   size       rows     nulls   min / max
  0-0    data  Z D  2       36.50 B    73 B                        "Chris Aniszczyk" / "Dmitriy Ryaboy"
  0-1    data  Z D  1       17.00 B    17 B                1


Column: contacts.phoneNumber
--------------------------------------------------------------------------------
  page   type  enc  count   avg size   size       rows     nulls   min / max
  0-0    data  Z D  2       16.50 B    33 B                1       "555 987 6543" / "555 987 6543"
  0-1    data  Z D  1       17.00 B    17 B                1

`,
	},

	{
		scenario: "example from the twitter blog (v2)",
		version:  v2,
		rows: []any{
			AddressBook{
				Owner: "Julien Le Dem",
				OwnerPhoneNumbers: []string{
					"555 123 4567",
					"555 666 1337",
				},
				Contacts: []Contact{
					{
						Name:        "Dmitriy Ryaboy",
						PhoneNumber: "555 987 6543",
					},
					{
						Name: "Chris Aniszczyk",
					},
				},
			},
			AddressBook{
				Owner:             "A. Nonymous",
				OwnerPhoneNumbers: nil,
			},
		},

		dump: `
File path:  {file-path}
Created by: github.com/parquet-go/parquet-go
Properties: (none)
Schema:
message AddressBook {
  required binary owner (STRING);
  repeated binary ownerPhoneNumbers (STRING);
  repeated group contacts {
    required binary name (STRING);
    optional binary phoneNumber (STRING);
  }
}


Row group 0:  count: 2  377.50 B records  start: 4  total(compressed): 755 B total(uncompressed):678 B
--------------------------------------------------------------------------------
                      type      encodings count     avg size   nulls   min / max
owner                 BINARY    Z         2         72.50 B            "A. Nonymous" / "Julien Le Dem"
ownerPhoneNumbers     BINARY    G         3         78.00 B    1       "555 123 4567" / "555 666 1337"
contacts.name         BINARY    _         3         68.00 B    1       "Chris Aniszczyk" / "Dmitriy Ryaboy"
contacts.phoneNumber  BINARY    Z         3         57.33 B    2       "555 987 6543" / "555 987 6543"


Column: owner
--------------------------------------------------------------------------------
  page   type  enc  count   avg size   size       rows     nulls   min / max
  0-0    data  _ D  2       25.00 B    50 B       2        0       "A. Nonymous" / "Julien Le Dem"


Column: ownerPhoneNumbers
--------------------------------------------------------------------------------
  page   type  enc  count   avg size   size       rows     nulls   min / max
  0-0    data  _ D  2       28.00 B    56 B       1        0       "555 123 4567" / "555 666 1337"
  0-1    data  _ D  1       9.00 B     9 B        1        1


Column: contacts.name
--------------------------------------------------------------------------------
  page   type  enc  count   avg size   size       rows     nulls   min / max
  0-0    data  _ D  2       32.50 B    65 B       1        0       "Chris Aniszczyk" / "Dmitriy Ryaboy"
  0-1    data  _ D  1       9.00 B     9 B        1        1


Column: contacts.phoneNumber
--------------------------------------------------------------------------------
  page   type  enc  count   avg size   size       rows     nulls   min / max
  0-0    data  _ D  2       12.50 B    25 B       1        1       "555 987 6543" / "555 987 6543"
  0-1    data  _ D  1       9.00 B     9 B        1        1

`,
	},
	{
		scenario: "omit `-` fields",
		version:  v1,
		rows: []any{
			&event{Name: "customer1", Type: "request", Value: 42.0},
			&event{Name: "customer2", Type: "access", Value: 1.0},
		},
		dump: `
File path:  {file-path}
Created by: github.com/parquet-go/parquet-go
Properties: (none)
Schema:
message event {
  required binary name (STRING);
  required double value;
}


Row group 0:  count: 2  100.00 B records  start: 4  total(compressed): 200 B total(uncompressed):200 B
--------------------------------------------------------------------------------
       type      encodings count     avg size   nulls   min / max
name   BINARY    _ _ R     2         59.50 B            "customer1" / "customer2"
value  DOUBLE    _   _     2         40.50 B            "1.0" / "42.0"


Column: name
--------------------------------------------------------------------------------
  page   type  enc  count   avg size   size       rows     nulls   min / max
  0-D    dict  _ _  2       13.00 B    26 B
  0-1    data  _ R  2       2.50 B     5 B                         "customer1" / "customer2"


Column: value
--------------------------------------------------------------------------------
  page   type  enc  count   avg size   size       rows     nulls   min / max
  0-0    data  _ _  2       8.00 B     16 B                        "1.0" / "42.0"

`,
	},

	{
		scenario: "no encoding (v2)",
		version:  v2,
		rows: []any{
			timeseriesNoEncoding{Name: "http_request_total", Timestamp: 1639444033, Value: 100},
		},
		dump: `
File path:  {file-path}
Created by: github.com/parquet-go/parquet-go
Properties: (none)
Schema:
message timeseriesNoEncoding {
  required binary name (STRING);
  required int64 timestamp (INTEGER(64,true));
  required double value;
}


Row group 0:  count: 1  289.00 B records  start: 4  total(compressed): 289 B total(uncompressed):289 B
--------------------------------------------------------------------------------
           type      encodings count     avg size   nulls   min / max
name       BINARY    _         1         133.00 B           "http_request_total" / "http_request_total"
timestamp  INT64     _   _     1         78.00 B            "1639444033" / "1639444033"
value      DOUBLE    _   _     1         78.00 B            "100.0" / "100.0"


Column: name
--------------------------------------------------------------------------------
  page   type  enc  count   avg size   size       rows     nulls   min / max
  0-0    data  _ D  1       23.00 B    23 B       1        0       "http_request_total" / "http_request_total"


Column: timestamp
--------------------------------------------------------------------------------
  page   type  enc  count   avg size   size       rows     nulls   min / max
  0-0    data  _ _  1       8.00 B     8 B        1        0       "1639444033" / "1639444033"


Column: value
--------------------------------------------------------------------------------
  page   type  enc  count   avg size   size       rows     nulls   min / max
  0-0    data  _ _  1       8.00 B     8 B        1        0       "100.0" / "100.0"

`,
	},

	{
		scenario: "default encoding (plain) (v2)",
		version:  v2,
		rows: []any{
			timeseriesNoEncoding{Name: "http_request_total", Timestamp: 1639444033, Value: 100},
		},
		defaultEncoding: &parquet.Plain,
		dump: `
File path:  {file-path}
Created by: github.com/parquet-go/parquet-go
Properties: (none)
Schema:
message timeseriesNoEncoding {
  required binary name (STRING);
  required int64 timestamp (INTEGER(64,true));
  required double value;
}


Row group 0:  count: 1  288.00 B records  start: 4  total(compressed): 288 B total(uncompressed):288 B
--------------------------------------------------------------------------------
           type      encodings count     avg size   nulls   min / max
name       BINARY    _   _     1         132.00 B           "http_request_total" / "http_request_total"
timestamp  INT64     _   _     1         78.00 B            "1639444033" / "1639444033"
value      DOUBLE    _   _     1         78.00 B            "100.0" / "100.0"


Column: name
--------------------------------------------------------------------------------
  page   type  enc  count   avg size   size       rows     nulls   min / max
  0-0    data  _ _  1       22.00 B    22 B       1        0       "http_request_total" / "http_request_total"


Column: timestamp
--------------------------------------------------------------------------------
  page   type  enc  count   avg size   size       rows     nulls   min / max
  0-0    data  _ _  1       8.00 B     8 B        1        0       "1639444033" / "1639444033"


Column: value
--------------------------------------------------------------------------------
  page   type  enc  count   avg size   size       rows     nulls   min / max
  0-0    data  _ _  1       8.00 B     8 B        1        0       "100.0" / "100.0"

`,
	},

	{
		scenario: "default encoding for (v2)",
		version:  v2,
		rows: []any{
			timeseriesNoEncoding{Name: "http_request_total", Timestamp: 1639444033, Value: 100},
		},
		defaultEncodingFor: map[parquet.Kind]encoding.Encoding{
			parquet.ByteArray: &parquet.DeltaByteArray,
			parquet.Int64:     &parquet.DeltaBinaryPacked,
			parquet.Double:    &parquet.RLEDictionary,
		},
		dump: `
File path:  {file-path}
Created by: github.com/parquet-go/parquet-go
Properties: (none)
Schema:
message timeseriesNoEncoding {
  required binary name (STRING);
  required int64 timestamp (INTEGER(64,true));
  required double value;
}


Row group 0:  count: 1  316.00 B records  start: 4  total(compressed): 316 B total(uncompressed):316 B
--------------------------------------------------------------------------------
           type      encodings count     avg size   nulls   min / max
name       BINARY    _   D     1         138.00 B           "http_request_total" / "http_request_total"
timestamp  INT64     _   D     1         79.00 B            "1639444033" / "1639444033"
value      DOUBLE    _ _ R     1         99.00 B            "100.0" / "100.0"


Column: name
--------------------------------------------------------------------------------
  page   type  enc  count   avg size   size       rows     nulls   min / max
  0-0    data  _ D  1       28.00 B    28 B       1        0       "http_request_total" / "http_request_total"


Column: timestamp
--------------------------------------------------------------------------------
  page   type  enc  count   avg size   size       rows     nulls   min / max
  0-0    data  _ D  1       9.00 B     9 B        1        0       "1639444033" / "1639444033"


Column: value
--------------------------------------------------------------------------------
  page   type  enc  count   avg size   size       rows     nulls   min / max
  0-D    dict  _ _  1       8.00 B     8 B
  0-1    data  _ R  1       2.00 B     2 B        1        0       "100.0" / "100.0"

`,
	},
}

// TestWriter uses the Apache parquet-cli tool to validate generated parquet files.
// On MacOS systems using brew, this can be installed with `brew install parquet-cli`.
// For more information on installing and running this tool, see:
// https://github.com/apache/parquet-java/blob/ef9929c130f8f2e24fca1c7b42b0742a4d9d5e61/parquet-cli/README.md
// This test expects the parquet-cli command to exist in the environment path as `parquet`
// and to require no additional arguments before the primary command. If you need to run
// it in some other way on your system, you can configure the environment variable
// `PARQUET_GO_TEST_CLI`.
func TestWriter(t *testing.T) {
	if !hasParquetCli() {
		t.Skip("Skipping TestWriter writerTests because parquet-cli is not installed in Github CI. FIXME.") // TODO
	}

	for _, test := range writerTests {
		dataPageVersion := test.version
		codec := test.codec
		rows := test.rows
		dump := test.dump
		defaultEncoding := test.defaultEncoding
		defaultEncodingFor := test.defaultEncodingFor

		t.Run(test.scenario, func(t *testing.T) {
			t.Parallel()

			writerOptions := []parquet.WriterOption{
				parquet.DataPageVersion(dataPageVersion),
				parquet.Compression(codec),
			}

			if defaultEncoding != nil {
				writerOptions = append(writerOptions,
					parquet.DefaultEncoding(defaultEncoding),
				)
			}
			for kind, enc := range defaultEncodingFor {
				writerOptions = append(writerOptions,
					parquet.DefaultEncodingFor(kind, enc),
				)
			}

			path, b, err := generateParquetFile(makeRows(rows),
				writerOptions...,
			)
			if err != nil {
				t.Logf("\n%s", string(b))
				t.Fatal(err)
			}

			// The CLI output includes the file-path of the parquet file. Because the test
			// uses a temp file, this value is not consistent between test runs and cannot
			// be hard-coded. Therefore, the expected value includes a placeholder value
			// and we replace it here.
			dump = strings.Replace(dump, "{file-path}", path, 1)
			if string(b) != dump {
				edits := myers.ComputeEdits(span.URIFromPath("want.txt"), dump, string(b))
				diff := fmt.Sprint(gotextdiff.ToUnified("want.txt", "got.txt", dump, edits))
				t.Errorf("\n%s", diff)
			}
		})
	}
}

func hasParquetCli() bool {
	// If PARQUET_GO_TEST_CLI is defined, always attempt to run the test. If it's defined
	// but the command cannot be called, the test itself should fail.
	if os.Getenv("PARQUET_GO_TEST_CLI") != "" {
		return true
	}
	_, err := exec.LookPath("parquet")
	return err == nil
}

func parquetCLI(cmd, path string) ([]byte, error) {
	execPath := "parquet"
	envCmd := os.Getenv("PARQUET_GO_TEST_CLI")
	var cmdArgs []string
	if envCmd != "" {
		envSplit := strings.Split(envCmd, " ")
		execPath = envSplit[0]
		cmdArgs = envSplit[1:]
	}
	cmdArgs = append(cmdArgs, cmd, path)
	p := exec.Command(execPath, cmdArgs...)

	output, err := p.CombinedOutput()
	if err != nil {
		return output, err
	}

	// parquet-cli has trailing spaces on some lines.
	lines := bytes.Split(output, []byte("\n"))

	for i, line := range lines {
		lines[i] = bytes.TrimRight(line, " ")
	}

	return bytes.Join(lines, []byte("\n")), nil
}

func TestWriterGenerateBloomFilters(t *testing.T) {
	type Person struct {
		FirstName utf8string `parquet:"first_name"`
		LastName  utf8string `parquet:"last_name"`
	}

	err := quickCheck(func(rows []Person) bool {
		if len(rows) == 0 { // TODO: support writing files with no rows
			return true
		}

		buffer := new(bytes.Buffer)
		writer := parquet.NewWriter(buffer,
			parquet.BloomFilters(
				parquet.SplitBlockFilter(10, "last_name"),
			),
		)
		for i := range rows {
			if err := writer.Write(&rows[i]); err != nil {
				t.Error(err)
				return false
			}
		}
		if err := writer.Close(); err != nil {
			t.Error(err)
			return false
		}

		reader := bytes.NewReader(buffer.Bytes())
		f, err := parquet.OpenFile(reader, reader.Size())
		if err != nil {
			t.Error(err)
			return false
		}
		rowGroup := f.RowGroups()[0]
		columns := rowGroup.ColumnChunks()
		firstName := columns[0]
		lastName := columns[1]

		if firstName.BloomFilter() != nil {
			t.Errorf(`"first_name" column has a bloom filter even though none were configured`)
			return false
		}

		bloomFilter := lastName.BloomFilter()
		if bloomFilter == nil {
			t.Error(`"last_name" column has no bloom filter despite being configured to have one`)
			return false
		}

		for i, row := range rows {
			if ok, err := bloomFilter.Check(parquet.ValueOf(row.LastName)); err != nil {
				t.Errorf("unexpected error checking bloom filter: %v", err)
				return false
			} else if !ok {
				t.Errorf("bloom filter does not contain value %q of row %d", row.LastName, i)
				return false
			}
		}

		return true
	})
	if err != nil {
		t.Error(err)
	}
}

func TestBloomFilterForDict(t *testing.T) {
	type testStruct struct {
		A string `parquet:"a,dict"`
	}

	schema := parquet.SchemaOf(&testStruct{})

	b := bytes.NewBuffer(nil)
	w := parquet.NewWriter(
		b,
		schema,
		parquet.BloomFilters(parquet.SplitBlockFilter(10, "a")),
	)

	err := w.Write(&testStruct{A: "test"})
	if err != nil {
		t.Fatal(err)
	}

	err = w.Close()
	if err != nil {
		t.Fatal(err)
	}

	f, err := parquet.OpenFile(bytes.NewReader(b.Bytes()), int64(b.Len()))
	if err != nil {
		t.Fatal(err)
	}

	ok, err := f.RowGroups()[0].ColumnChunks()[0].BloomFilter().Check(parquet.ValueOf("test"))
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Error("bloom filter should have contained 'test'")
	}
}

func TestWriterRepeatedUUIDDict(t *testing.T) {
	inputID := uuid.MustParse("123456ab-0000-0000-0000-000000000000")
	records := []struct {
		List []uuid.UUID `parquet:"list,dict"`
	}{{
		[]uuid.UUID{inputID},
	}}
	schema := parquet.SchemaOf(&records[0])
	b := bytes.NewBuffer(nil)
	w := parquet.NewWriter(b, schema)
	if err := w.Write(records[0]); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	f, err := parquet.OpenFile(bytes.NewReader(b.Bytes()), int64(b.Len()))
	if err != nil {
		t.Fatal(err)
	}

	rowbuf := make([]parquet.Row, 1)
	rows := f.RowGroups()[0].Rows()
	defer rows.Close()
	n, err := rows.ReadRows(rowbuf)
	if n == 0 {
		t.Fatalf("reading row from parquet file: %v", err)
	}
	if len(rowbuf[0]) != 1 {
		t.Errorf("expected 1 value in row, got %d", len(rowbuf[0]))
	}
	if !bytes.Equal(inputID[:], rowbuf[0][0].Bytes()) {
		t.Errorf("expected to get UUID %q back out, got %q", inputID, rowbuf[0][0].Bytes())
	}
}

func TestWriterResetWithBloomFilters(t *testing.T) {
	type Test struct {
		Value string `parquet:"value,dict"`
	}

	writer := parquet.NewWriter(new(bytes.Buffer),
		parquet.BloomFilters(
			parquet.SplitBlockFilter(10, "value"),
		),
	)

	if err := writer.Write(&Test{Value: "foo"}); err != nil {
		t.Fatal(err)
	}

	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	writer.Reset(new(bytes.Buffer))

	if err := writer.Write(&Test{Value: "bar"}); err != nil {
		t.Fatal(err)
	}

	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWriterMaxRowsPerRowGroup(t *testing.T) {
	output := new(bytes.Buffer)
	writer := parquet.NewWriter(output, parquet.MaxRowsPerRowGroup(10))

	for i := range 100 {
		err := writer.Write(struct{ FirstName, LastName string }{
			FirstName: "0123456789"[i%10 : i%10+1],
			LastName:  "foo",
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	f, err := parquet.OpenFile(bytes.NewReader(output.Bytes()), int64(output.Len()))
	if err != nil {
		t.Fatal(err)
	}

	rowGroups := f.RowGroups()
	if len(rowGroups) != 10 {
		t.Errorf("wrong number of row groups in parquet file: want=10 got=%d", len(rowGroups))
	}
}

func TestSetKeyValueMetadata(t *testing.T) {
	testKey := "test-key"
	testValue := "test-value"

	type testStruct struct {
		A string `parquet:"a,dict"`
	}

	schema := parquet.SchemaOf(&testStruct{})

	b := bytes.NewBuffer(nil)
	w := parquet.NewWriter(
		b,
		schema,
	)

	err := w.Write(&testStruct{A: "test"})
	if err != nil {
		t.Fatal(err)
	}

	w.SetKeyValueMetadata(testKey, testValue)

	err = w.Close()
	if err != nil {
		t.Fatal(err)
	}

	metadata := w.File().Metadata()
	if len(metadata.KeyValueMetadata) != 1 {
		t.Errorf("expected 1 key/value metadata, got %d", len(metadata.KeyValueMetadata))
	} else {
		if metadata.KeyValueMetadata[0].Key != testKey {
			t.Errorf("expected metadata key '%s', got '%s'", testKey, metadata.KeyValueMetadata[0].Key)
		}
		if metadata.KeyValueMetadata[0].Value != testValue {
			t.Errorf("expected metadata value '%s', got '%s'", testValue, metadata.KeyValueMetadata[0].Value)
		}
	}

	f, err := parquet.OpenFile(bytes.NewReader(b.Bytes()), int64(b.Len()))
	if err != nil {
		t.Fatal(err)
	}

	value, ok := f.Lookup(testKey)
	if !ok {
		t.Fatalf("key/value metadata should have included %q", testKey)
	}
	if value != testValue {
		t.Errorf("expected %q, got %q", testValue, value)
	}
}

func TestSetKeyValueMetadataOverwritesExisting(t *testing.T) {
	testKey := "test-key"
	testValue := "test-value"

	type testStruct struct {
		A string `parquet:"a,dict"`
	}

	schema := parquet.SchemaOf(&testStruct{})

	b := bytes.NewBuffer(nil)
	w := parquet.NewWriter(
		b,
		schema,
		parquet.KeyValueMetadata(testKey, "original-value"),
	)

	err := w.Write(&testStruct{A: "test"})
	if err != nil {
		t.Fatal(err)
	}

	w.SetKeyValueMetadata(testKey, testValue)

	err = w.Close()
	if err != nil {
		t.Fatal(err)
	}

	f, err := parquet.OpenFile(bytes.NewReader(b.Bytes()), int64(b.Len()))
	if err != nil {
		t.Fatal(err)
	}

	value, ok := f.Lookup(testKey)
	if !ok {
		t.Fatalf("key/value metadata should have included %q", testKey)
	}
	if value != testValue {
		t.Errorf("expected %q, got %q", testValue, value)
	}
}

func TestColumnMaxValueAndMinValue(t *testing.T) {
	type testStruct struct {
		A string `parquet:"a,plain"`
	}

	tests := make([]testStruct, 100)
	tests[0] = testStruct{A: ""}
	for i := 1; i < 100; i++ {
		tests[i] = testStruct{A: strconv.Itoa(i)}
	}
	schema := parquet.SchemaOf(&testStruct{})
	b := bytes.NewBuffer(nil)
	config := parquet.DefaultWriterConfig()
	config.PageBufferSize = 256
	w := parquet.NewGenericWriter[testStruct](b, schema, config)
	_, _ = w.Write(tests[0:50])
	_, _ = w.Write(tests[50:100])
	_ = w.Close()

	f, err := parquet.OpenFile(bytes.NewReader(b.Bytes()), int64(b.Len()))
	if err != nil {
		t.Fatal(err)
	}
	if len(f.RowGroups()) != 1 {
		t.Fatalf("wrong number of row groups in parquet file: want=1 got=%d", f.RowGroups())
	}
	statistics := f.Metadata().RowGroups[0].Columns[0].MetaData.Statistics
	if string(statistics.MinValue) != "" {
		t.Fatalf("wrong min value of row groups in parquet file: want=' '() got=%s", string(statistics.MinValue))
	}
	if string(statistics.MaxValue) != "99" {
		t.Fatalf("wrong max value of row groups in parquet file: want=99 got=%s", string(statistics.MaxValue))
	}
}

func TestColumnSkipPageBounds(t *testing.T) {
	type testStruct struct {
		A int `parquet:"a,plain"`
	}

	tests := make([]testStruct, 100)
	for i := range 100 {
		tests[i] = testStruct{A: i + 1}
	}
	schema := parquet.SchemaOf(&testStruct{})
	b := bytes.NewBuffer(nil)
	config := parquet.DefaultWriterConfig()
	config.PageBufferSize = 256
	w := parquet.NewGenericWriter[testStruct](b, schema, config, parquet.SkipPageBounds("a"))
	_, _ = w.Write(tests[0:50])
	_, _ = w.Write(tests[50:100])
	_ = w.Close()

	f, err := parquet.OpenFile(bytes.NewReader(b.Bytes()), int64(b.Len()))
	if err != nil {
		t.Fatal(err)
	}
	if len(f.RowGroups()) != 1 {
		t.Fatalf("wrong number of row groups in parquet file: want=1 got=%d", f.RowGroups())
	}
	statistics := f.Metadata().RowGroups[0].Columns[0].MetaData.Statistics
	if string(statistics.MinValue) != "" {
		t.Fatalf("wrong min value of row groups in parquet file: want='' got=%s", string(statistics.MinValue))
	}
	if string(statistics.MaxValue) != "" {
		t.Fatalf("wrong max value of row groups in parquet file: want='' got=%s", string(statistics.MaxValue))
	}
}

func TestIssueNotAllowedDefaultEncoding(t *testing.T) {
	const expectedPanic = "cannot use encoding DELTA_LENGTH_BYTE_ARRAY for kind BOOLEAN"

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("using default encoding not compatible with one of the primitive types should panic")
		}
		if r.(string) != expectedPanic {
			t.Fatalf(`panic should return "%s", but returned "%s"`, expectedPanic, r.(string))
		}
	}()

	type TestType struct {
		Key int
	}

	b := new(bytes.Buffer)
	_ = parquet.NewGenericWriter[TestType](b, parquet.DefaultEncoding(&parquet.DeltaLengthByteArray))
}

func TestIssueNotAllowedDefaultEncodingFor(t *testing.T) {
	const expectedPanic = "cannot use encoding DELTA_BINARY_PACKED for kind BYTE_ARRAY"

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("using a default encoding not compatible with a primitive type should panic")
		}
		if r.(string) != expectedPanic {
			t.Fatalf(`panic should return "%s", but returned "%s"`, expectedPanic, r.(string))
		}
	}()

	type TestType struct {
		Key int
	}

	b := new(bytes.Buffer)
	_ = parquet.NewGenericWriter[TestType](b,
		parquet.DefaultEncodingFor(parquet.ByteArray, &parquet.DeltaBinaryPacked),
	)
}
