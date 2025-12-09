package parquet_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/internal/quick"
)

func TestGenericReader(t *testing.T) {
	testGenericReader[booleanColumn](t)
	testGenericReader[int32Column](t)
	testGenericReader[int64Column](t)
	testGenericReader[int96Column](t)
	testGenericReader[floatColumn](t)
	testGenericReader[doubleColumn](t)
	testGenericReader[byteArrayColumn](t)
	testGenericReader[fixedLenByteArrayColumn](t)
	testGenericReader[stringColumn](t)
	testGenericReader[indexedStringColumn](t)
	testGenericReader[uuidColumn](t)
	testGenericReader[timeColumn](t)
	testGenericReader[timeInMillisColumn](t)
	testGenericReader[mapColumn](t)
	testGenericReader[decimalColumn](t)
	testGenericReader[addressBook](t)
	testGenericReader[contact](t)
	testGenericReader[listColumn2](t)
	testGenericReader[listColumn1](t)
	testGenericReader[listColumn0](t)
	testGenericReader[nestedListColumn1](t)
	testGenericReader[nestedListColumn](t)
	testGenericReader[*contact](t)
	testGenericReader[paddedBooleanColumn](t)
	testGenericReader[optionalInt32Column](t)
	testGenericReader[repeatedInt32Column](t)
}

func testGenericReader[Row any](t *testing.T) {
	var model Row
	t.Run(reflect.TypeOf(model).Name(), func(t *testing.T) {
		err := quickCheck(func(rows []Row) bool {
			if err := testGenericReaderRows(rows); err != nil {
				t.Error(err)
				return false
			}
			return true
		})
		if err != nil {
			t.Error(err)
		}
	})
}

func testGenericReaderRows[Row any](rows []Row) error {
	setNullPointers(rows)
	buffer := new(bytes.Buffer)
	writer := parquet.NewGenericWriter[Row](buffer)
	_, err := writer.Write(rows)
	if err != nil {
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}
	reader := parquet.NewGenericReader[Row](bytes.NewReader(buffer.Bytes()))
	result := make([]Row, len(rows))
	n, err := reader.Read(result)
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if n < len(rows) {
		return fmt.Errorf("not enough values were read: want=%d got=%d", len(rows), n)
	}
	if !reflect.DeepEqual(rows, result) {
		return fmt.Errorf("rows mismatch:\nwant: %+v\ngot: %+v", rows, result)
	}
	return nil
}

func TestIssueSegmentio400(t *testing.T) {
	type B struct {
		Name string
	}
	type A struct {
		B []B `parquet:",optional"`
	}

	b := new(bytes.Buffer)
	w := parquet.NewGenericWriter[A](b)
	expect := []A{
		{
			B: []B{
				{
					// 32 bytes random so we can see in the binary parquet if we
					// actually wrote the value
					Name: "9e7eb1f0-bbcc-43ec-bfad-a9fac1bb0feb",
				},
			},
		},
	}
	_, err := w.Write(expect)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r := parquet.NewGenericReader[A](bytes.NewReader(b.Bytes()))
	values := make([]A, 1)
	if n, err := r.Read(values); n != len(values) {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(expect, values) {
		t.Errorf("want %v got %v", expect, values)
	}
}

func TestReadMinPageSize(t *testing.T) {
	// NOTE: min page size is 307 for MyRow schema
	t.Run("test read less than min page size", func(t *testing.T) { testReadMinPageSize(128, t) })
	t.Run("test read equal to min page size", func(t *testing.T) { testReadMinPageSize(307, t) })
	t.Run("test read more than min page size", func(t *testing.T) { testReadMinPageSize(384, t) })
	// NOTE: num rows is 20,000
	t.Run("test read equal to num rows", func(t *testing.T) { testReadMinPageSize(20_000, t) })
	t.Run("test read more than num rows", func(t *testing.T) { testReadMinPageSize(25_000, t) })
}

func testReadMinPageSize(readSize int, t *testing.T) {
	type MyRow struct {
		ID    [16]byte `parquet:"id,delta,uuid"`
		File  string   `parquet:"file,dict,zstd"`
		Index int64    `parquet:"index,delta,zstd"`
	}

	numRows := 20_000
	maxPageBytes := 5000

	tmp, err := os.CreateTemp("/tmp", "*.parquet")
	if err != nil {
		t.Fatal("os.CreateTemp: ", err)
	}
	path := tmp.Name()
	defer os.Remove(path)
	t.Log("file:", path)

	// The page buffer size ensures we get multiple pages out of this example.
	w := parquet.NewGenericWriter[MyRow](tmp, parquet.PageBufferSize(maxPageBytes))
	// Need to write 1 row at a time here as writing many at once disregards PageBufferSize option.
	for i := range numRows {
		row := MyRow{
			ID:    [16]byte{15: byte(i)},
			File:  "hi" + fmt.Sprint(i),
			Index: int64(i),
		}
		_, err := w.Write([]MyRow{row})
		if err != nil {
			t.Fatal("w.Write: ", err)
		}
		// Flush writes rows as row group. 4 total (20k/5k) in this file.
		if (i+1)%maxPageBytes == 0 {
			err = w.Flush()
			if err != nil {
				t.Fatal("w.Flush: ", err)
			}
		}
	}
	err = w.Close()
	if err != nil {
		t.Fatal("w.Close: ", err)
	}
	err = tmp.Close()
	if err != nil {
		t.Fatal("tmp.Close: ", err)
	}

	file, err := os.Open(path)
	if err != nil {
		t.Fatal("os.Open", err)
	}
	reader := parquet.NewGenericReader[MyRow](file)
	read := int64(0)
	nRows := reader.NumRows()
	rows := make([]MyRow, 0, nRows)
	buf := make([]MyRow, readSize) // NOTE: min page size is 307 for MyRow schema

	for read < nRows {
		num, err := reader.Read(buf)
		read += int64(num)
		if err != nil && !errors.Is(err, io.EOF) {
			t.Fatal("Read:", err)
		}
		rows = append(rows, buf...)
	}

	if err := reader.Close(); err != nil {
		t.Fatal("Close", err)
	}

	if len(rows) < numRows {
		t.Fatalf("not enough values were read: want=%d got=%d", len(rows), numRows)
	}
	for i, row := range rows[:numRows] {
		id := [16]byte{15: byte(i)}
		file := "hi" + fmt.Sprint(i)
		index := int64(i)

		if row.ID != id || row.File != file || row.Index != index {
			t.Fatalf("rows mismatch at index: %d got: %+v", i, row)
		}
	}
}

func BenchmarkGenericReader(b *testing.B) {
	benchmarkGenericReader[benchmarkRowType](b)
	benchmarkGenericReader[booleanColumn](b)
	benchmarkGenericReader[int32Column](b)
	benchmarkGenericReader[int64Column](b)
	benchmarkGenericReader[floatColumn](b)
	benchmarkGenericReader[doubleColumn](b)
	benchmarkGenericReader[byteArrayColumn](b)
	benchmarkGenericReader[fixedLenByteArrayColumn](b)
	benchmarkGenericReader[stringColumn](b)
	benchmarkGenericReader[indexedStringColumn](b)
	benchmarkGenericReader[uuidColumn](b)
	benchmarkGenericReader[timeColumn](b)
	benchmarkGenericReader[timeInMillisColumn](b)
	benchmarkGenericReader[mapColumn](b)
	benchmarkGenericReader[decimalColumn](b)
	benchmarkGenericReader[contact](b)
	benchmarkGenericReader[paddedBooleanColumn](b)
	benchmarkGenericReader[optionalInt32Column](b)
}

func benchmarkGenericReader[Row generator[Row]](b *testing.B) {
	var model Row
	b.Run(reflect.TypeOf(model).Name(), func(b *testing.B) {
		prng := rand.New(rand.NewSource(0))
		rows := make([]Row, benchmarkNumRows)
		for i := range rows {
			rows[i] = rows[i].generate(prng)
		}

		rowbuf := make([]Row, benchmarkRowsPerStep)
		buffer := parquet.NewGenericBuffer[Row]()
		buffer.Write(rows)

		b.Run("go1.17", func(b *testing.B) {
			reader := parquet.NewRowGroupReader(buffer)
			benchmarkRowsPerSecond(b, func() int {
				for i := range rowbuf {
					if err := reader.Read(&rowbuf[i]); err != nil {
						if err != io.EOF {
							b.Fatal(err)
						} else {
							reader.Reset()
						}
					}
				}
				return len(rowbuf)
			})
		})

		b.Run("go1.18", func(b *testing.B) {
			reader := parquet.NewGenericRowGroupReader[Row](buffer)
			benchmarkRowsPerSecond(b, func() int {
				n, err := reader.Read(rowbuf)
				if err != nil {
					if err != io.EOF {
						b.Fatal(err)
					} else {
						reader.Reset()
					}
				}
				return n
			})
		})
	})
}

func rowsOf(numRows int, model any) rows {
	prng := rand.New(rand.NewSource(0))
	return randomRowsOf(prng, numRows, model)
}

func randomRowsOf(prng *rand.Rand, numRows int, model any) rows {
	typ := reflect.TypeOf(model)
	rows := make(rows, numRows)
	makeValue := quick.MakeValueFuncOf(typ)
	for i := range rows {
		v := reflect.New(typ).Elem()
		makeValue(v, prng)
		rows[i] = v.Interface()
	}
	return rows
}

var readerTests = []struct {
	scenario string
	model    any
}{
	{
		scenario: "BOOLEAN",
		model:    booleanColumn{},
	},

	{
		scenario: "INT32",
		model:    int32Column{},
	},

	{
		scenario: "INT64",
		model:    int64Column{},
	},

	{
		scenario: "INT96",
		model:    int96Column{},
	},

	{
		scenario: "FLOAT",
		model:    floatColumn{},
	},

	{
		scenario: "DOUBLE",
		model:    doubleColumn{},
	},

	{
		scenario: "BYTE_ARRAY",
		model:    byteArrayColumn{},
	},

	{
		scenario: "FIXED_LEN_BYTE_ARRAY",
		model:    fixedLenByteArrayColumn{},
	},

	{
		scenario: "STRING",
		model:    stringColumn{},
	},

	{
		scenario: "STRING (dict)",
		model:    indexedStringColumn{},
	},

	{
		scenario: "UUID",
		model:    uuidColumn{},
	},

	{
		scenario: "time.Time",
		model:    timeColumn{},
	},

	{
		scenario: "time.Time in ms",
		model:    timeInMillisColumn{},
	},

	{
		scenario: "DECIMAL",
		model:    decimalColumn{},
	},

	{
		scenario: "AddressBook",
		model:    addressBook{},
	},

	{
		scenario: "one optional level",
		model:    listColumn2{},
	},

	{
		scenario: "one repeated level",
		model:    listColumn1{},
	},

	{
		scenario: "two repeated levels",
		model:    listColumn0{},
	},

	{
		scenario: "three repeated levels",
		model:    listColumn0{},
	},

	{
		scenario: "nested lists",
		model:    nestedListColumn{},
	},

	{
		scenario: "key-value pairs",
		model: struct {
			KeyValuePairs map[utf8string]utf8string
		}{},
	},

	{
		scenario: "multiple key-value pairs",
		model: struct {
			KeyValuePairs0 map[utf8string]utf8string
			KeyValuePairs1 map[utf8string]utf8string
			KeyValuePairs2 map[utf8string]utf8string
		}{},
	},

	{
		scenario: "repeated key-value pairs",
		model: struct {
			RepeatedKeyValuePairs []map[utf8string]utf8string
		}{},
	},

	{
		scenario: "map of repeated values",
		model: struct {
			MapOfRepeated map[utf8string][]utf8string
		}{},
	},
}

func TestReader(t *testing.T) {
	buf := new(bytes.Buffer)
	file := bytes.NewReader(nil)

	for _, test := range readerTests {
		t.Run(test.scenario, func(t *testing.T) {
			const N = 42

			rowType := reflect.TypeOf(test.model)
			rowPtr := reflect.New(rowType)
			rowZero := reflect.Zero(rowType)
			rowValue := rowPtr.Elem()

			for n := 1; n < N; n++ {
				t.Run(fmt.Sprintf("N=%d", n), func(t *testing.T) {
					defer buf.Reset()
					rows := rowsOf(n, test.model)

					if err := writeParquetFileWithBuffer(buf, rows); err != nil {
						t.Fatal(err)
					}

					file.Reset(buf.Bytes())
					r := parquet.NewReader(file, parquet.SchemaOf(test.model))

					for i, v := range rows {
						if err := r.Read(rowPtr.Interface()); err != nil {
							t.Fatal(err)
						}
						if !reflect.DeepEqual(rowValue.Interface(), v) {
							t.Errorf("row mismatch at index %d\nwant = %+v\ngot  = %+v", i, v, rowValue.Interface())
						}
						rowValue.Set(rowZero)
					}

					if err := r.Read(rowPtr.Interface()); err != io.EOF {
						t.Errorf("expected EOF after reading all values but got: %v", err)
					}
				})
			}
		})
	}
}

func BenchmarkReaderReadType(b *testing.B) {
	buf := new(bytes.Buffer)
	file := bytes.NewReader(nil)

	for _, test := range readerTests {
		b.Run(test.scenario, func(b *testing.B) {
			defer buf.Reset()
			rows := rowsOf(benchmarkNumRows, test.model)

			if err := writeParquetFile(buf, rows); err != nil {
				b.Fatal(err)
			}
			file.Reset(buf.Bytes())
			f, err := parquet.OpenFile(file, file.Size())
			if err != nil {
				b.Fatal(err)
			}

			rowType := reflect.TypeOf(test.model)
			rowPtr := reflect.New(rowType)
			rowZero := reflect.Zero(rowType)
			rowValue := rowPtr.Elem()

			r := parquet.NewReader(f)
			p := rowPtr.Interface()

			benchmarkRowsPerSecond(b, func() (n int) {
				for range benchmarkRowsPerStep {
					if err := r.Read(p); err != nil {
						if err == io.EOF {
							r.Reset()
						} else {
							b.Fatal(err)
						}
					}
				}
				rowValue.Set(rowZero)
				return benchmarkRowsPerStep
			})

			b.SetBytes(int64(math.Ceil(benchmarkRowsPerStep * float64(file.Size()) / benchmarkNumRows)))
		})
	}
}

func BenchmarkReaderReadRow(b *testing.B) {
	buf := new(bytes.Buffer)
	file := bytes.NewReader(nil)

	for _, test := range readerTests {
		b.Run(test.scenario, func(b *testing.B) {
			defer buf.Reset()
			rows := rowsOf(benchmarkNumRows, test.model)

			if err := writeParquetFile(buf, rows); err != nil {
				b.Fatal(err)
			}
			file.Reset(buf.Bytes())
			f, err := parquet.OpenFile(file, file.Size())
			if err != nil {
				b.Fatal(err)
			}

			r := parquet.NewReader(f)
			rowbuf := make([]parquet.Row, benchmarkRowsPerStep)

			benchmarkRowsPerSecond(b, func() int {
				n, err := r.ReadRows(rowbuf)
				if err != nil {
					if err == io.EOF {
						r.Reset()
					} else {
						b.Fatal(err)
					}
				}
				return n
			})

			b.SetBytes(int64(math.Ceil(benchmarkRowsPerStep * float64(file.Size()) / benchmarkNumRows)))
		})
	}
}

func TestReaderReadSubset(t *testing.T) {
	// In this example we'll write 3 columns to the file - X, Y, and Z, but
	// we'll only read out the X and Y columns. Returns true if all writes
	// and reads were successful, and false otherwise.
	type Point3D struct{ X, Y, Z int64 }
	type Point2D struct{ X, Y int64 }

	err := quickCheck(func(points3D []Point3D) bool {
		if len(points3D) == 0 {
			return true
		}
		buf := new(bytes.Buffer)
		err := writeParquetFile(buf, makeRows(points3D))
		if err != nil {
			t.Error(err)
			return false
		}
		reader := parquet.NewReader(bytes.NewReader(buf.Bytes()))
		for i := 0; ; i++ {
			row := Point2D{}
			err := reader.Read(&row)
			if err != nil {
				if err == io.EOF && i == len(points3D) {
					break
				}
				t.Error(err)
				return false
			}
			if row != (Point2D{X: points3D[i].X, Y: points3D[i].Y}) {
				t.Errorf("points mismatch at row index %d: want=%v got=%v", i, points3D[i], row)
				return false
			}
		}
		return true
	})
	if err != nil {
		t.Error(err)
	}
}

func TestReaderSeekToRow(t *testing.T) {
	type rowType struct {
		Name utf8string `parquet:",dict"`
	}

	rows := rowsOf(10, rowType{})
	buf := new(bytes.Buffer)
	err := writeParquetFile(buf, rows)
	if err != nil {
		t.Fatal(err)
	}

	reader := parquet.NewReader(bytes.NewReader(buf.Bytes()))
	for i := range 10 {
		if err := reader.SeekToRow(int64(i)); err != nil {
			t.Fatalf("seek to row %d: %v", i, err)
		}

		row := new(rowType)
		err := reader.Read(row)
		if err != nil {
			t.Fatalf("reading row %d: %v", i, err)
		}

		if *row != rows[i] {
			t.Fatalf("row %d mismatch: got=%+v want=%+v", i, *row, rows[i])
		}
	}
}

func TestSeekToRowNoDict(t *testing.T) {
	type rowType struct {
		Name utf8string `parquet:","` // no dictionary encoding
	}

	// write samples to in-memory buffer
	buf := new(bytes.Buffer)
	schema := parquet.SchemaOf(new(rowType))
	w := parquet.NewWriter(buf, schema)
	sample := rowType{
		Name: "foo1",
	}
	// write two rows
	w.Write(sample)
	sample.Name = "foo2"
	w.Write(sample)
	w.Close()

	// create reader
	r := parquet.NewReader(bytes.NewReader(buf.Bytes()))

	// read second row
	r.SeekToRow(1)
	row := new(rowType)
	err := r.Read(row)
	if err != nil {
		t.Fatalf("reading row: %v", err)
	}
	// fmt.Println(&sample, row)
	if *row != sample {
		t.Fatalf("read != write")
	}
}

func TestSeekToRowReadAll(t *testing.T) {
	type rowType struct {
		Name utf8string `parquet:",dict"`
	}

	// write samples to in-memory buffer
	buf := new(bytes.Buffer)
	schema := parquet.SchemaOf(new(rowType))
	w := parquet.NewWriter(buf, schema)
	sample := rowType{
		Name: "foo1",
	}
	// write two rows
	w.Write(sample)
	sample.Name = "foo2"
	w.Write(sample)
	w.Close()

	// create reader
	r := parquet.NewReader(bytes.NewReader(buf.Bytes()))

	// read first row
	r.SeekToRow(0)
	row := new(rowType)
	err := r.Read(row)
	if err != nil {
		t.Fatalf("reading row: %v", err)
	}
	// read second row
	r.SeekToRow(1)
	row = new(rowType)
	err = r.Read(row)
	if err != nil {
		t.Fatalf("reading row: %v", err)
	}
	// fmt.Println(&sample, row)
	if *row != sample {
		t.Fatalf("read != write")
	}
}

func TestSeekToRowDictReadSecond(t *testing.T) {
	type rowType struct {
		Name utf8string `parquet:",dict"`
	}

	// write samples to in-memory buffer
	buf := new(bytes.Buffer)
	schema := parquet.SchemaOf(new(rowType))
	w := parquet.NewWriter(buf, schema)
	sample := rowType{
		Name: "foo1",
	}
	// write two rows
	w.Write(sample)
	sample.Name = "foo2"
	w.Write(sample)
	w.Close()

	// create reader
	r := parquet.NewReader(bytes.NewReader(buf.Bytes()))

	// read second row
	r.SeekToRow(1)
	row := new(rowType)
	err := r.Read(row)
	if err != nil {
		t.Fatalf("reading row: %v", err)
	}
	// fmt.Println(&sample, row)
	if *row != sample {
		t.Fatalf("read != write")
	}
}

func TestSeekToRowDictReadMultiplePages(t *testing.T) {
	type rowType struct {
		Name utf8string `parquet:",dict"`
	}

	// write samples to in-memory buffer
	buf := new(bytes.Buffer)
	schema := parquet.SchemaOf(new(rowType))
	w := parquet.NewWriter(buf, schema, &parquet.WriterConfig{
		PageBufferSize: 10,
	})
	sample := rowType{
		Name: "foo1",
	}

	// write enough rows to spill over a single page
	for range 10 {
		w.Write(sample)
	}
	sample.Name = "foo2"
	w.Write(sample)
	w.Close()

	// create reader
	r := parquet.NewReader(bytes.NewReader(buf.Bytes()))

	// read 11th row
	r.SeekToRow(10)
	row := new(rowType)
	err := r.Read(row)
	if err != nil {
		t.Fatalf("reading row: %v", err)
	}
	if *row != sample {
		t.Fatalf("read != write")
	}
}

func TestLookup(t *testing.T) {
	type rowType struct {
		Value int32
	}

	// Create test data with metadata
	buf := new(bytes.Buffer)
	schema := parquet.SchemaOf(new(rowType))
	w := parquet.NewWriter(buf, schema)
	w.SetKeyValueMetadata("key1", "value1")
	w.SetKeyValueMetadata("key2", "value2")
	w.SetKeyValueMetadata("empty", "")

	// Write a sample row
	if err := w.Write(rowType{Value: 42}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name     string
		key      string
		expected string
	}{
		{
			name:     "existing key 1",
			key:      "key1",
			expected: "value1",
		},
		{
			name:     "existing key 2",
			key:      "key2",
			expected: "value2",
		},
		{
			name:     "empty value",
			key:      "empty",
			expected: "",
		},
		{
			name:     "non-existent key",
			key:      "missing",
			expected: "",
		},
	}

	t.Run("Reader", func(t *testing.T) {
		r := parquet.NewReader(bytes.NewReader(buf.Bytes()))
		defer r.Close()

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				got, _ := r.File().Lookup(tt.key)
				if got != tt.expected {
					t.Errorf("Lookup(%q) = %q, want %q", tt.key, got, tt.expected)
				}
			})
		}
	})

	t.Run("GenericReader", func(t *testing.T) {
		r := parquet.NewGenericReader[rowType](bytes.NewReader(buf.Bytes()))
		defer r.Close()

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				got, _ := r.File().Lookup(tt.key)
				if got != tt.expected {
					t.Errorf("Lookup(%q) = %q, want %q", tt.key, got, tt.expected)
				}
			})
		}
	})
}

// struct for parquet-go/parquet-go
type OrderbookDepth struct {
	Timestamp  int64   `parquet:"timestamp"       json:"timestamp"`
	Instrument string  `parquet:"instrument_name" json:"instrument_name"`
	ChangeID   int64   `parquet:"change_id"       json:"change_id"`
	Bids       []Level `parquet:"bids,list"       json:"bids"`
	Asks       []Level `parquet:"asks,list"       json:"asks"`
}

type Level struct {
	Price  float64 `parquet:"price"  json:"price"`
	Amount float64 `parquet:"amount" json:"amount"`
}

func TestIssue206(t *testing.T) {
	f, err := os.Open("testdata/issue206.parquet")
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer f.Close()
	s, err := f.Stat()
	if err != nil {
		t.Fatalf("Failed to get file stats: %v", err)
	}

	rows, err := parquet.Read[OrderbookDepth](f, s.Size())
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}

	b, err := os.ReadFile("testdata/issue206.json")
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}

	var want []OrderbookDepth
	if err := json.Unmarshal(b, &want); err != nil {
		t.Fatalf("Failed to unmarshal JSON: %v", err)
	}

	if len(rows) != len(want) {
		t.Errorf("len(rows) != len(want): %d != %d", len(rows), len(want))
	}

	for i := range min(len(rows), len(want)) {
		g := rows[i]
		w := want[i]
		if g.Timestamp != w.Timestamp {
			t.Errorf("rows[%d].Timestamp != want[%d].Timestamp: %d != %d", i, i, g.Timestamp, w.Timestamp)
			break
		}
		if g.Instrument != w.Instrument {
			t.Errorf("rows[%d].Instrument != want[%d].Instrument: %q != %q", i, i, g.Instrument, w.Instrument)
			break
		}
		if g.ChangeID != w.ChangeID {
			t.Errorf("rows[%d].ChangeID != want[%d].ChangeID: %d != %d", i, i, g.ChangeID, w.ChangeID)
			break
		}
		if !slices.Equal(g.Bids, w.Bids) {
			t.Errorf("rows[%d].Bids != want[%d].Bids:\ngot:  %+v\nwant: %+v", i, i, g.Bids, w.Bids)
			break
		}
		if !slices.Equal(g.Asks, w.Asks) {
			t.Errorf("rows[%d].Asks != want[%d].Asks:\ngot:  %+v\nwant: %+v", i, i, g.Asks, w.Asks)
			break
		}
	}
}

func TestListElementsAcrossPages(t *testing.T) {
	// Test for https://github.com/parquet-go/parquet-go/issues/276
	// Each testdata/issue276_* parquet file has 5 rows.
	// Each row of the int64_array column has 13 int64 values.
	// One file is written with 4 values per page, the other has 9 per page.
	// The actual distribution of values per page is printed out by printColumnLayout.
	type Row struct {
		Id       string  `parquet:"id"`
		IntArray []int64 `parquet:"int64_array,optional,list"`
	}

	testFiles, _ := filepath.Glob("testdata/issue276_*.parquet")
	for _, testFile := range testFiles {
		t.Run(testFile, func(t *testing.T) {
			f, err := os.Open(testFile)
			if err != nil {
				t.Fatalf("Failed to open file: %v", err)
			}
			defer f.Close()

			// print layout of int64_array.list.element column/pages
			printColumnLayout(t, f)

			pr := parquet.NewGenericReader[*Row](f)
			expectedArrayLength := 13

			assertRow := func(t *testing.T, got *Row, idx int) {
				if got.Id != strconv.Itoa(idx) {
					t.Fatalf("ID mismatch: want=%d got=%s", idx, got.Id)
				}
				if len(got.IntArray) != expectedArrayLength {
					t.Fatalf("expected %d values, got %d", expectedArrayLength, len(got.IntArray))
				}
				if got.IntArray[0] != int64(idx) {
					t.Fatalf("expected value %d, got %d", idx, got.IntArray[idx])
				}

				if got.IntArray[expectedArrayLength-1] != int64(idx+expectedArrayLength-1) {
					t.Fatalf("expected last element to be %v, got %v", int64(idx+expectedArrayLength), got.IntArray[expectedArrayLength-1])
				}
			}

			for i := range int(pr.NumRows()) {
				for n := 1; n < int(pr.NumRows())-i; n += 1 {
					t.Run(fmt.Sprintf("seek %d, n=%d", i, n), func(t *testing.T) {
						if err := pr.SeekToRow(int64(i)); err != nil {
							t.Fatal(err)
						}
						rows := make([]*Row, n)
						nRead, err := pr.Read(rows)
						if nRead != n {
							t.Fatalf("expected to read %d rows", n)
						}
						if err != nil && err != io.EOF {
							t.Fatal(err)
						}
						for j := i; j < i+nRead; j++ {
							assertRow(t, rows[j-i], j)
						}
					})
				}
			}
		})
	}
}

func printColumnLayout(t *testing.T, f *os.File) {
	if t.Failed() {
		return
	}
	stats, _ := f.Stat()
	pqFile, err := parquet.OpenFile(f, stats.Size())
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	col := pqFile.Root()
	col = col.Column("int64_array").Column("list").Column("element")
	pages := col.Pages()
	defer pages.Close()
	pageCounter := 0
	for {
		pageCounter++
		p, err := pages.ReadPage()
		if err != nil {
			if err != io.EOF {
				t.Error(err)
			}
			break
		}
		t.Logf("page: %d, num rows in page: %d, num values in page: %d", pageCounter, p.NumRows(), p.NumValues())
		parquet.Release(p)
	}
}
