package parquet_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/big"
	"math/rand"
	"os"
	"os/exec"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/hexops/gotextdiff"
	"github.com/hexops/gotextdiff/myers"
	"github.com/hexops/gotextdiff/span"
	"github.com/parquet-go/bitpack/unsafecast"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
	"github.com/parquet-go/parquet-go/compress/zstd"
	"github.com/parquet-go/parquet-go/encoding"
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
			writer := parquet.NewWriter(io.Discard,
				parquet.SchemaOf(rows[0]),
				parquet.WriteBufferSize(0),
			)
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
			writer := parquet.NewGenericWriter[Row](io.Discard,
				parquet.WriteBufferSize(0),
			)
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

// TestIssue176DataPageV1WithDecimal tests the bug reported in issue #176
// where files written with data page version 1 and DECIMAL type cannot be read
// by Apache Parquet CLI tools.
func TestIssue176DataPageV1WithDecimal(t *testing.T) {
	// Create schema matching the issue description:
	// - DECIMAL(38, 0) on FIXED_LEN_BYTE_ARRAY(16)
	// - Optional field
	schema := parquet.NewSchema("test", parquet.Group{
		"A": parquet.Optional(parquet.Decimal(0, 38, parquet.FixedLenByteArrayType(16))),
	})

	// Write a test file with data page version 1 and zstd compression
	var buf bytes.Buffer
	writer := parquet.NewWriter(
		&buf,
		schema,
		parquet.DataPageVersion(1),
		parquet.Compression(&zstd.Codec{}),
	)

	// Create some test values - using big.Int for 38 precision decimal
	testValues := []struct {
		value *big.Int
	}{
		{value: big.NewInt(0)},
		{value: big.NewInt(123456789)},
		{value: big.NewInt(-987654321)},
		{value: new(big.Int).Exp(big.NewInt(10), big.NewInt(37), nil)}, // 10^37, near max for 38 precision
	}

	// Write rows - need to write raw values since we're using a custom schema
	for _, tv := range testValues {
		// Convert big.Int to 16-byte fixed length byte array
		var fixedBytes [16]byte
		valueBytes := tv.value.Bytes()

		// Copy to the end of the array (big-endian)
		if len(valueBytes) > 16 {
			t.Fatalf("value too large for 16 bytes: %v", tv.value)
		}
		copy(fixedBytes[16-len(valueBytes):], valueBytes)

		// Handle negative numbers (two's complement)
		if tv.value.Sign() < 0 {
			// For negative numbers, we need to set the leading bytes
			for i := range 16 - len(valueBytes) {
				fixedBytes[i] = 0xFF
			}
		}

		row := parquet.Row{
			parquet.ValueOf(fixedBytes[:]).Level(0, 1, 0),
		}

		if _, err := writer.WriteRows([]parquet.Row{row}); err != nil {
			t.Fatalf("WriteRows failed: %v", err)
		}
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Try to read the file back
	file := bytes.NewReader(buf.Bytes())
	reader := parquet.NewReader(file)
	defer reader.Close()

	// Read rows back
	rowNum := 0
	for {
		rows := make([]parquet.Row, 1)
		rows[0] = make(parquet.Row, 1)
		n, err := reader.ReadRows(rows)
		if err != nil && err != io.EOF {
			t.Fatalf("ReadRows error: %v", err)
		}
		rowNum += n
		if n == 0 || err != nil {
			break
		}
	}

	if rowNum != len(testValues) {
		t.Errorf("Expected to read %d rows, but got %d", len(testValues), rowNum)
	}

	t.Logf("Successfully wrote and read %d rows with data page v1", rowNum)
}

// TestIssue176NullValues tests writing null values with data page v1
func TestIssue176NullValues(t *testing.T) {
	schema := parquet.NewSchema("test", parquet.Group{
		"A": parquet.Optional(parquet.Decimal(0, 38, parquet.FixedLenByteArrayType(16))),
	})

	var buf bytes.Buffer
	writer := parquet.NewWriter(
		&buf,
		schema,
		parquet.DataPageVersion(1),
		parquet.Compression(&zstd.Codec{}),
	)

	// Write some null values
	for range 5 {
		row := parquet.Row{
			parquet.ValueOf(nil).Level(0, 0, 0), // null value
		}

		if _, err := writer.WriteRows([]parquet.Row{row}); err != nil {
			t.Fatalf("WriteRows failed: %v", err)
		}
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Try to read the file back
	file := bytes.NewReader(buf.Bytes())
	reader := parquet.NewReader(file)
	defer reader.Close()

	rowNum := 0
	for {
		rows := make([]parquet.Row, 1)
		rows[0] = make(parquet.Row, 1)
		n, err := reader.ReadRows(rows)
		if err != nil && err != io.EOF {
			t.Fatalf("ReadRows error: %v", err)
		}
		rowNum += n
		if n == 0 || err != nil {
			break
		}
	}

	if rowNum != 5 {
		t.Errorf("Expected to read 5 rows, but got %d", rowNum)
	}

	t.Logf("Successfully wrote and read %d null rows with data page v1", rowNum)
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
		{
			name: "ColumnWriter.WriteRowValues followed by Close",
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
					if w.ColumnWriters()[i].Close() != nil {
						t.Errorf("ColumnWriter[%d].Close() an error: %v", i, err)
					}
				}
				return numRows, nil
			},
			data: colSlice,
		},
		{
			name: "Parallel ColumnWriter.WriteRowValues",
			write: func(t *testing.T, w *parquet.GenericWriter[T], vals [][]parquet.Value) (int, error) {
				t.Helper()
				var (
					rowCounts []int
					errs      []error
					mu        sync.Mutex
					wg        sync.WaitGroup
				)
				for i, col := range vals {
					wg.Add(1)
					go func() {
						defer wg.Done()
						num, err := w.ColumnWriters()[i].WriteRowValues(col)
						if err != nil {
							mu.Lock()
							errs = append(errs, err)
							mu.Unlock()
							return
						}
						if w.ColumnWriters()[i].Close() != nil {
							t.Errorf("ColumnWriter[%d].Close() an error: %v", i, err)
						}
						mu.Lock()
						rowCounts = append(rowCounts, num)
						mu.Unlock()
					}()
				}
				wg.Wait()
				for i, n := range rowCounts[1:] {
					if n != rowCounts[0] {
						t.Errorf("column %d disagrees with number of rows written in column %d", i, 0)
					}
				}
				return rowCounts[0], errors.Join(errs...)
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

func TestIssue303_OptionalByteSlice(t *testing.T) {
	type Row struct {
		Buf []byte `parquet:"buf,optional"`
	}

	// Write a non-nil, non-empty []byte into an optional BYTE_ARRAY field
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[Row](&buf)
	in := []Row{{Buf: []byte("test")}}
	if _, err := w.Write(in); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Read back and verify it is not NULL and equals the original bytes
	r := parquet.NewGenericReader[Row](bytes.NewReader(buf.Bytes()))
	out := make([]Row, 1)
	if n, err := r.Read(out); n != len(out) {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}

	if got, want := string(out[0].Buf), "test"; got != want {
		t.Fatalf("optional []byte round-trip mismatch: got=%q want=%q", got, want)
	}
}

func TestIssue304(t *testing.T) {
	// Define schema with nested structure
	type Address struct {
		City    string `parquet:"city"`
		Country string `parquet:"country"`
	}

	type Person struct {
		Name    string  `parquet:"name"`
		Age     int32   `parquet:"age"`
		Address Address `parquet:"address"`
	}

	schema := parquet.SchemaOf(Person{})

	// Create a writer with the nested schema
	var buf bytes.Buffer
	writer := parquet.NewGenericWriter[any](&buf, schema)

	// Try to write using map[string]any with nested map[string]any
	// This is the scenario from the issue where dynamic/unknown schema
	// requires using map[string]any instead of predefined structs
	row := map[string]any{
		"name": "John Doe",
		"age":  int32(30),
		"address": map[string]any{
			"city":    "Paris",
			"country": "France",
		},
	}

	// This should reproduce the panic:
	// panic: reflect: call of reflect.Value.MapIndex on interface Value
	_, err := writer.Write([]any{row})
	if err != nil {
		t.Fatal(err)
	}

	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	// Read back and verify
	reader := parquet.NewReader(bytes.NewReader(buf.Bytes()))
	readRow := make(map[string]any)
	err = reader.Read(&readRow)
	if err != nil {
		t.Fatal(err)
	}

	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}

	// Verify the data
	if readRow["name"] != "John Doe" {
		t.Errorf("name mismatch: got %v, want John Doe", readRow["name"])
	}
	if readRow["age"] != int32(30) {
		t.Errorf("age mismatch: got %v, want 30", readRow["age"])
	}
	address, ok := readRow["address"].(map[string]any)
	if !ok {
		t.Fatalf("address is not map[string]any, got %T", readRow["address"])
	}
	if address["city"] != "Paris" {
		t.Errorf("city mismatch: got %v, want Paris", address["city"])
	}
	if address["country"] != "France" {
		t.Errorf("country mismatch: got %v, want France", address["country"])
	}
}

func TestIssue305(t *testing.T) {
	// Test for issue #305: Invalid Parquet File Generation with Empty Struct Maps
	// Structs containing map[string]struct{} fields (a common pattern for sets in Go)
	// should generate valid Parquet files with empty groups that have NumChildren
	// properly set to a non-null zero value.
	type RowType struct {
		Name           string
		FavoriteColors map[string]struct{}
	}

	// Create a writer
	var buf bytes.Buffer
	writer := parquet.NewGenericWriter[RowType](&buf)

	// Write data with map[string]struct{} (set pattern)
	rows := []RowType{
		{
			Name: "John Doe",
			FavoriteColors: map[string]struct{}{
				"Red":   {},
				"Green": {},
				"Blue":  {},
			},
		},
		{
			Name:           "Jane Smith",
			FavoriteColors: map[string]struct{}{"Yellow": {}},
		},
		{
			Name:           "Bob Johnson",
			FavoriteColors: map[string]struct{}{}, // empty map
		},
	}

	if _, err := writer.Write(rows); err != nil {
		t.Fatal(err)
	}

	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	// Read back and verify
	reader := parquet.NewGenericReader[RowType](bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	readRows := make([]RowType, len(rows))
	n, err := reader.Read(readRows)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}

	if n != len(rows) {
		t.Fatalf("expected to read %d rows, got %d", len(rows), n)
	}

	// Verify the data
	for i, expected := range rows {
		got := readRows[i]
		if got.Name != expected.Name {
			t.Errorf("row %d: name mismatch: got %q, want %q", i, got.Name, expected.Name)
		}
		if len(got.FavoriteColors) != len(expected.FavoriteColors) {
			t.Errorf("row %d: FavoriteColors length mismatch: got %d, want %d",
				i, len(got.FavoriteColors), len(expected.FavoriteColors))
		}
		for color := range expected.FavoriteColors {
			if _, ok := got.FavoriteColors[color]; !ok {
				t.Errorf("row %d: missing color %q in FavoriteColors", i, color)
			}
		}
	}

	// Verify the schema has NumChildren properly set for the empty struct group
	// The map value (struct{}) should be a group with NumChildren = 0 (not nil)
	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}

	schema := f.Schema()
	// Navigate to the map value node: FavoriteColors -> key_value -> value
	favoriteColorsField := schema.Fields()[1] // index 1 is FavoriteColors
	if favoriteColorsField.Name() != "FavoriteColors" {
		t.Fatalf("expected field FavoriteColors at index 1, got %s", favoriteColorsField.Name())
	}

	// In a MAP logical type, there should be a key_value group
	keyValueField := favoriteColorsField.Fields()[0]
	if keyValueField.Name() != "key_value" {
		t.Fatalf("expected key_value field, got %s", keyValueField.Name())
	}

	// The key_value group should have 2 children: key and value
	if len(keyValueField.Fields()) != 2 {
		t.Fatalf("expected key_value to have 2 fields, got %d", len(keyValueField.Fields()))
	}

	// The value field should be the empty struct (group with 0 children)
	valueField := keyValueField.Fields()[1]
	if valueField.Name() != "value" {
		t.Fatalf("expected value field, got %s", valueField.Name())
	}

	// The value should be a group (not a leaf)
	if valueField.Leaf() {
		t.Fatal("value field should be a group, not a leaf")
	}

	// The value should have 0 children (empty struct)
	if len(valueField.Fields()) != 0 {
		t.Fatalf("expected value to have 0 fields (empty struct), got %d", len(valueField.Fields()))
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


Row group 0:  count: 3  109.67 B records  start: 4  total(compressed): 329 B total(uncompressed):305 B
--------------------------------------------------------------------------------
            type      encodings count     avg size   nulls   min / max
first_name  BINARY    Z _ R     3         38.67 B    0       "Han" / "Luke"
last_name   BINARY    Z   D     3         71.00 B    0       "Skywalker" / "Solo"


Column: first_name
--------------------------------------------------------------------------------
  page   type  enc  count   avg size   size       rows     nulls   min / max
  0-D    dict  Z _  3       7.67 B     23 B
  0-1    data  Z R  3       2.33 B     7 B                 0       "Han" / "Luke"


Column: last_name
--------------------------------------------------------------------------------
  page   type  enc  count   avg size   size       rows     nulls   min / max
  0-0    data  Z D  2       28.00 B    56 B                0       "Skywalker" / "Solo"
  0-1    data  Z D  1       19.00 B    19 B                0       "Skywalker" / "Skywalker"

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


Row group 0:  count: 3  111.67 B records  start: 4  total(compressed): 335 B total(uncompressed):320 B
--------------------------------------------------------------------------------
            type      encodings count     avg size   nulls   min / max
first_name  BINARY    Z _ R     3         37.33 B    0       "Han" / "Luke"
last_name   BINARY    Z   D     3         74.33 B    0       "Skywalker" / "Solo"


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


Row group 0:  count: 10  123.70 B records  start: 4  total(compressed): 1.208 kB total(uncompressed):1.331 kB
--------------------------------------------------------------------------------
           type      encodings count     avg size   nulls   min / max
name       BINARY    G _ R     10        29.40 B    0       "http_request_total" / "http_request_total"
timestamp  INT64     G   D     10        47.50 B    0       "1639444033" / "1639444144"
value      DOUBLE    G   _     10        46.80 B    0       "-0.0" / "100.0"


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


Row group 0:  count: 2  387.00 B records  start: 4  total(compressed): 774 B total(uncompressed):697 B
--------------------------------------------------------------------------------
                      type      encodings count     avg size   nulls   min / max
owner                 BINARY    Z         2         71.00 B    0       "A. Nonymous" / "Julien Le Dem"
ownerPhoneNumbers     BINARY    G         3         81.00 B    1       "555 123 4567" / "555 666 1337"
contacts.name         BINARY    _         3         70.67 B    1       "Chris Aniszczyk" / "Dmitriy Ryaboy"
contacts.phoneNumber  BINARY    Z         3         59.00 B    2       "555 987 6543" / "555 987 6543"


Column: owner
--------------------------------------------------------------------------------
  page   type  enc  count   avg size   size       rows     nulls   min / max
  0-0    data  Z D  2       25.00 B    50 B                0       "A. Nonymous" / "Julien Le Dem"


Column: ownerPhoneNumbers
--------------------------------------------------------------------------------
  page   type  enc  count   avg size   size       rows     nulls   min / max
  0-0    data  Z D  2       32.00 B    64 B                0       "555 123 4567" / "555 666 1337"
  0-1    data  Z D  1       17.00 B    17 B                1


Column: contacts.name
--------------------------------------------------------------------------------
  page   type  enc  count   avg size   size       rows     nulls   min / max
  0-0    data  Z D  2       36.50 B    73 B                0       "Chris Aniszczyk" / "Dmitriy Ryaboy"
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


Row group 0:  count: 2  380.50 B records  start: 4  total(compressed): 761 B total(uncompressed):684 B
--------------------------------------------------------------------------------
                      type      encodings count     avg size   nulls   min / max
owner                 BINARY    Z         2         73.50 B    0       "A. Nonymous" / "Julien Le Dem"
ownerPhoneNumbers     BINARY    G         3         78.67 B    1       "555 123 4567" / "555 666 1337"
contacts.name         BINARY    _         3         68.67 B    1       "Chris Aniszczyk" / "Dmitriy Ryaboy"
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


Row group 0:  count: 2  102.00 B records  start: 4  total(compressed): 204 B total(uncompressed):204 B
--------------------------------------------------------------------------------
       type      encodings count     avg size   nulls   min / max
name   BINARY    _ _ R     2         60.50 B    0       "customer1" / "customer2"
value  DOUBLE    _   _     2         41.50 B    0       "1.0" / "42.0"


Column: name
--------------------------------------------------------------------------------
  page   type  enc  count   avg size   size       rows     nulls   min / max
  0-D    dict  _ _  2       13.00 B    26 B
  0-1    data  _ R  2       2.50 B     5 B                 0       "customer1" / "customer2"


Column: value
--------------------------------------------------------------------------------
  page   type  enc  count   avg size   size       rows     nulls   min / max
  0-0    data  _ _  2       8.00 B     16 B                0       "1.0" / "42.0"

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


Row group 0:  count: 1  295.00 B records  start: 4  total(compressed): 295 B total(uncompressed):295 B
--------------------------------------------------------------------------------
           type      encodings count     avg size   nulls   min / max
name       BINARY    _         1         135.00 B   0       "http_request_total" / "http_request_total"
timestamp  INT64     _   _     1         80.00 B    0       "1639444033" / "1639444033"
value      DOUBLE    _   _     1         80.00 B    0       "100.0" / "100.0"


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


Row group 0:  count: 1  294.00 B records  start: 4  total(compressed): 294 B total(uncompressed):294 B
--------------------------------------------------------------------------------
           type      encodings count     avg size   nulls   min / max
name       BINARY    _   _     1         134.00 B   0       "http_request_total" / "http_request_total"
timestamp  INT64     _   _     1         80.00 B    0       "1639444033" / "1639444033"
value      DOUBLE    _   _     1         80.00 B    0       "100.0" / "100.0"


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


Row group 0:  count: 1  322.00 B records  start: 4  total(compressed): 322 B total(uncompressed):322 B
--------------------------------------------------------------------------------
           type      encodings count     avg size   nulls   min / max
name       BINARY    _   D     1         140.00 B   0       "http_request_total" / "http_request_total"
timestamp  INT64     _   D     1         81.00 B    0       "1639444033" / "1639444033"
value      DOUBLE    _ _ R     1         101.00 B   0       "100.0" / "100.0"


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
		buffer := new(bytes.Buffer)
		writer := parquet.NewGenericWriter[Person](buffer,
			parquet.BloomFilters(
				parquet.SplitBlockFilter(10, "last_name"),
			),
		)
		if _, err := writer.Write(rows); err != nil {
			t.Error(err)
			return false
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

		// Handle empty file case
		if len(rows) == 0 {
			if len(f.RowGroups()) != 0 {
				t.Error("expected empty file to have no row groups")
				return false
			}
			return true
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

func TestMapKeySorting(t *testing.T) {
	// Test that map keys are sorted correctly when written to parquet files
	tests := []struct {
		name     string
		input    any
		validate func(t *testing.T, row parquet.Row)
	}{
		{
			name: "string keys",
			input: struct {
				StringMap map[string]int `parquet:"string_map"`
			}{
				StringMap: map[string]int{
					"z": 26,
					"a": 1,
					"m": 13,
					"c": 3,
					"x": 24,
				},
			},
			validate: func(t *testing.T, row parquet.Row) {
				var keys []string
				row.Range(func(columnIndex int, columnValues []parquet.Value) bool {
					if len(columnValues) == 0 {
						return true
					}
					if columnIndex%2 == 0 {
						key := columnValues[0].ByteArray()
						keys = append(keys, string(key))
					}
					return true
				})

				sortedKeys := make([]string, len(keys))
				copy(sortedKeys, keys)
				slices.Sort(sortedKeys)

				if !reflect.DeepEqual(keys, sortedKeys) {
					t.Errorf("string map keys not sorted, got: %v, want: %v", keys, sortedKeys)
				}
			},
		},
		{
			name: "int keys",
			input: struct {
				IntMap map[int]string `parquet:"int_map"`
			}{
				IntMap: map[int]string{
					5: "five",
					1: "one",
					3: "three",
					2: "two",
					4: "four",
				},
			},
			validate: func(t *testing.T, row parquet.Row) {
				var keys []int32
				row.Range(func(columnIndex int, columnValues []parquet.Value) bool {
					if len(columnValues) == 0 {
						return true
					}
					if columnIndex%2 == 0 {
						key := columnValues[0].Int32()
						keys = append(keys, key)
					}
					return true
				})

				sortedKeys := make([]int32, len(keys))
				copy(sortedKeys, keys)
				slices.Sort(sortedKeys)

				if !reflect.DeepEqual(keys, sortedKeys) {
					t.Errorf("int map keys not sorted, got: %v, want: %v", keys, sortedKeys)
				}
			},
		},
		{
			name: "float keys",
			input: struct {
				FloatMap map[float64]string `parquet:"float_map"`
			}{
				FloatMap: map[float64]string{
					5.5: "five point five",
					1.1: "one point one",
					3.3: "three point three",
					2.2: "two point two",
					4.4: "four point four",
				},
			},
			validate: func(t *testing.T, row parquet.Row) {
				var keys []float64
				row.Range(func(columnIndex int, columnValues []parquet.Value) bool {
					if len(columnValues) == 0 {
						return true
					}
					if columnIndex%2 == 0 {
						key := columnValues[0].Double()
						keys = append(keys, key)
					}
					return true
				})

				sortedKeys := make([]float64, len(keys))
				copy(sortedKeys, keys)
				slices.Sort(sortedKeys)

				if !reflect.DeepEqual(keys, sortedKeys) {
					t.Errorf("float map keys not sorted, got: %v, want: %v", keys, sortedKeys)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			buf := new(bytes.Buffer)
			w := parquet.NewWriter(buf, parquet.SchemaOf(tc.input))

			if err := w.Write(tc.input); err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}

			f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
			if err != nil {
				t.Fatal(err)
			}

			rows := f.RowGroups()[0].Rows()
			defer rows.Close()

			parquetRows := make([]parquet.Row, 1)
			_, err = rows.ReadRows(parquetRows)
			if err != nil && err != io.EOF {
				t.Fatal(err)
			}

			tc.validate(t, parquetRows[0])
		})
	}
}

func TestIssue275(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatal("require no panic")
		}
	}()

	type testStruct struct {
		A int `parquet:"value,plain"`
	}

	tests := make([]testStruct, 100)
	for i := range 100 {
		tests[i] = testStruct{A: i + 1}
	}

	b := bytes.NewBuffer(nil)
	w := parquet.NewGenericWriter[testStruct](b, parquet.MaxRowsPerRowGroup(10))

	if _, err := w.Write(tests[0:50]); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// test panic after reset writer
	b = bytes.NewBuffer(nil)
	w.Reset(b)

	if _, err := w.Write(tests[50:100]); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestColumnValidation(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatal("require no panic")
		}
	}()

	type DuplicateColumnsDiffTypes struct {
		Foo string `parquet:"foo"`
		Bar int    `parquet:"foo"`
	}

	b := bytes.NewBuffer(nil)

	failedWriter := parquet.NewGenericWriter[*DuplicateColumnsDiffTypes](b)
	data := []*DuplicateColumnsDiffTypes{
		{Foo: "1", Bar: 1},
	}
	if _, err := failedWriter.Write(data); err == nil {
		t.Fatal(err)
	}
}

func TestConcurrentRowGroupWriter(t *testing.T) {
	type Row struct {
		ID   int
		Name string
	}

	t.Run("single row group", func(t *testing.T) {
		buf := new(bytes.Buffer)
		writer := parquet.NewGenericWriter[Row](buf, parquet.MaxRowsPerRowGroup(100))

		rg := writer.BeginRowGroup()

		rows := []parquet.Row{
			{parquet.ValueOf(1).Level(0, 0, 0), parquet.ValueOf("test").Level(0, 0, 1)},
		}
		n, err := rg.WriteRows(rows)
		if err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Fatalf("expected 1 row written, got %d", n)
		}

		if _, err := rg.Commit(); err != nil {
			t.Fatal(err)
		}

		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}

		f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		if err != nil {
			t.Fatal(err)
		}

		if len(f.RowGroups()) != 1 {
			t.Fatalf("expected 1 row group, got %d", len(f.RowGroups()))
		}

		if f.NumRows() != 1 {
			t.Fatalf("expected 1 row, got %d", f.NumRows())
		}
	})

	t.Run("multiple row groups in order", func(t *testing.T) {
		buf := new(bytes.Buffer)
		writer := parquet.NewGenericWriter[Row](buf, parquet.MaxRowsPerRowGroup(10))

		const numGroups = 5
		rgs := make([]*parquet.ConcurrentRowGroupWriter, numGroups)

		// Create row groups sequentially
		for i := range rgs {
			rg := writer.BeginRowGroup()
			rgs[i] = rg

			// Write data to this row group
			for j := range 10 {
				rows := []parquet.Row{
					{parquet.ValueOf(i*10+j).Level(0, 0, 0), parquet.ValueOf(fmt.Sprintf("row_%d_%d", i, j)).Level(0, 0, 1)},
				}
				if _, err := rg.WriteRows(rows); err != nil {
					t.Fatal(err)
				}
			}
		}

		// Commit in order
		for _, rg := range rgs {
			if _, err := rg.Commit(); err != nil {
				t.Fatal(err)
			}
		}

		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}

		f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		if err != nil {
			t.Fatal(err)
		}

		if len(f.RowGroups()) != numGroups {
			t.Fatalf("expected %d row groups, got %d", numGroups, len(f.RowGroups()))
		}

		if f.NumRows() != 50 {
			t.Fatalf("expected 50 rows, got %d", f.NumRows())
		}

		// Verify data
		reader := parquet.NewGenericReader[Row](f)
		for i := range 50 {
			rows := []Row{{}}
			n, err := reader.Read(rows)
			if err != nil && err != io.EOF {
				t.Fatal(err)
			}
			if n > 0 && rows[0].ID != i {
				t.Errorf("row %d: expected ID %d, got %d", i, i, rows[0].ID)
			}
		}
	})

	t.Run("parallel row group writing", func(t *testing.T) {
		buf := new(bytes.Buffer)
		writer := parquet.NewGenericWriter[Row](buf, parquet.MaxRowsPerRowGroup(10))

		const numGroups = 5
		rgs := make([]*parquet.ConcurrentRowGroupWriter, numGroups)

		// Create all row groups
		for i := range rgs {
			rg := writer.BeginRowGroup()
			rgs[i] = rg
		}

		// Write to them in parallel
		var wg sync.WaitGroup
		errs := make([]error, numGroups)
		for i := range rgs {
			wg.Add(1)
			go func(index int, rg *parquet.ConcurrentRowGroupWriter) {
				defer wg.Done()
				for j := range 10 {
					rows := []parquet.Row{
						{parquet.ValueOf(index*10+j).Level(0, 0, 0), parquet.ValueOf(fmt.Sprintf("row_%d_%d", index, j)).Level(0, 0, 1)},
					}
					if _, err := rg.WriteRows(rows); err != nil {
						errs[index] = err
						return
					}
				}
			}(i, rgs[i])
		}
		wg.Wait()

		// Check for errors
		for i, err := range errs {
			if err != nil {
				t.Fatalf("row group %d failed: %v", i, err)
			}
		}

		// Commit in order
		for _, rg := range rgs {
			if _, err := rg.Commit(); err != nil {
				t.Fatal(err)
			}
		}

		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}

		f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		if err != nil {
			t.Fatal(err)
		}

		if len(f.RowGroups()) != numGroups {
			t.Fatalf("expected %d row groups, got %d", numGroups, len(f.RowGroups()))
		}

		if f.NumRows() != 50 {
			t.Fatalf("expected 50 rows, got %d", f.NumRows())
		}

		// Verify all data is present (order within row groups should be maintained)
		reader := parquet.NewGenericReader[Row](f)
		rowsSeen := make(map[int]bool)
		for range 50 {
			rows := []Row{{}}
			n, err := reader.Read(rows)
			if err != nil && err != io.EOF {
				t.Fatal(err)
			}
			if n > 0 {
				if rows[0].ID < 0 || rows[0].ID >= 50 {
					t.Errorf("unexpected ID: %d", rows[0].ID)
				}
				rowsSeen[rows[0].ID] = true
			}
		}
		if len(rowsSeen) != 50 {
			t.Errorf("expected 50 unique rows, got %d", len(rowsSeen))
		}
	})

	t.Run("commit with pending rows", func(t *testing.T) {
		buf := new(bytes.Buffer)
		writer := parquet.NewGenericWriter[Row](buf, parquet.MaxRowsPerRowGroup(10))

		// Write some rows to the main writer
		if _, err := writer.Write([]Row{{ID: 1, Name: "main1"}}); err != nil {
			t.Fatal(err)
		}

		// Create and write to a row group
		rg := writer.BeginRowGroup()

		rows := []parquet.Row{
			{parquet.ValueOf(2).Level(0, 0, 0), parquet.ValueOf("rg1").Level(0, 0, 1)},
		}
		if _, err := rg.WriteRows(rows); err != nil {
			t.Fatal(err)
		}

		// Commit should flush pending rows first
		if _, err := rg.Commit(); err != nil {
			t.Fatal(err)
		}

		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}

		f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		if err != nil {
			t.Fatal(err)
		}

		// Should have 2 row groups (one from Flush, one from Commit)
		if len(f.RowGroups()) != 2 {
			t.Fatalf("expected 2 row groups, got %d", len(f.RowGroups()))
		}

		if f.NumRows() != 2 {
			t.Fatalf("expected 2 rows, got %d", f.NumRows())
		}
	})
}

func TestConcurrentRowGroupWriterWithColumnWriters(t *testing.T) {
	type Row struct {
		ID   int    `parquet:"id"`
		Name string `parquet:"name"`
	}

	t.Run("single row group with column writers", func(t *testing.T) {
		buf := new(bytes.Buffer)
		writer := parquet.NewGenericWriter[Row](buf)

		rg := writer.BeginRowGroup()
		cols := rg.ColumnWriters()

		if len(cols) != 2 {
			t.Fatalf("expected 2 column writers, got %d", len(cols))
		}

		// Write values directly to columns
		idValues := []parquet.Value{
			parquet.Int32Value(1).Level(0, 0, 0),
			parquet.Int32Value(2).Level(0, 0, 0),
			parquet.Int32Value(3).Level(0, 0, 0),
		}
		nameValues := []parquet.Value{
			parquet.ByteArrayValue([]byte("Alice")).Level(0, 0, 1),
			parquet.ByteArrayValue([]byte("Bob")).Level(0, 0, 1),
			parquet.ByteArrayValue([]byte("Charlie")).Level(0, 0, 1),
		}

		if _, err := cols[0].WriteRowValues(idValues); err != nil {
			t.Fatal(err)
		}
		if _, err := cols[1].WriteRowValues(nameValues); err != nil {
			t.Fatal(err)
		}

		if _, err := rg.Commit(); err != nil {
			t.Fatal(err)
		}

		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}

		// Verify the written data
		reader := parquet.NewGenericReader[Row](bytes.NewReader(buf.Bytes()))
		defer reader.Close()

		rows := make([]Row, 3)
		n, err := reader.Read(rows)
		if err != nil && err != io.EOF {
			t.Fatal(err)
		}
		if n != 3 {
			t.Fatalf("expected to read 3 rows, got %d", n)
		}

		expected := []Row{
			{ID: 1, Name: "Alice"},
			{ID: 2, Name: "Bob"},
			{ID: 3, Name: "Charlie"},
		}

		if !reflect.DeepEqual(rows, expected) {
			t.Fatalf("expected %+v, got %+v", expected, rows)
		}
	})

	t.Run("parallel row groups with column writers", func(t *testing.T) {
		buf := new(bytes.Buffer)
		writer := parquet.NewGenericWriter[Row](buf)

		const numGroups = 3
		rgs := make([]*parquet.ConcurrentRowGroupWriter, numGroups)

		// Create row groups
		for i := range rgs {
			rgs[i] = writer.BeginRowGroup()
		}

		// Write to them in parallel using column writers
		var wg sync.WaitGroup
		for i := range rgs {
			wg.Add(1)
			go func(index int, rg *parquet.ConcurrentRowGroupWriter) {
				defer wg.Done()

				cols := rg.ColumnWriters()
				if len(cols) != 2 {
					t.Errorf("expected 2 column writers, got %d", len(cols))
					return
				}

				// Each row group writes 2 rows with different IDs
				startID := index * 2
				idValues := []parquet.Value{
					parquet.Int32Value(int32(startID)).Level(0, 0, 0),
					parquet.Int32Value(int32(startID+1)).Level(0, 0, 0),
				}
				nameValues := []parquet.Value{
					parquet.ByteArrayValue(fmt.Appendf(nil, "Name%d", startID)).Level(0, 0, 1),
					parquet.ByteArrayValue(fmt.Appendf(nil, "Name%d", startID+1)).Level(0, 0, 1),
				}

				if _, err := cols[0].WriteRowValues(idValues); err != nil {
					t.Errorf("error writing ID values: %v", err)
					return
				}
				if _, err := cols[1].WriteRowValues(nameValues); err != nil {
					t.Errorf("error writing name values: %v", err)
					return
				}
			}(i, rgs[i])
		}
		wg.Wait()

		// Commit in order
		for _, rg := range rgs {
			if _, err := rg.Commit(); err != nil {
				t.Fatal(err)
			}
		}

		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}

		// Verify the written data
		reader := parquet.NewGenericReader[Row](bytes.NewReader(buf.Bytes()))
		defer reader.Close()

		rows := make([]Row, 6)
		n, err := reader.Read(rows)
		if err != nil && err != io.EOF {
			t.Fatal(err)
		}
		if n != 6 {
			t.Fatalf("expected to read 6 rows, got %d", n)
		}

		// Data should be in the order of row group commits
		expected := []Row{
			{ID: 0, Name: "Name0"},
			{ID: 1, Name: "Name1"},
			{ID: 2, Name: "Name2"},
			{ID: 3, Name: "Name3"},
			{ID: 4, Name: "Name4"},
			{ID: 5, Name: "Name5"},
		}

		if !reflect.DeepEqual(rows, expected) {
			t.Fatalf("expected %+v, got %+v", expected, rows)
		}
	})

	t.Run("mixing WriteRows and ColumnWriters", func(t *testing.T) {
		buf := new(bytes.Buffer)
		writer := parquet.NewGenericWriter[Row](buf)

		rg1 := writer.BeginRowGroup()
		rg2 := writer.BeginRowGroup()

		// First row group uses WriteRows
		rows1 := []parquet.Row{
			{parquet.Int32Value(1).Level(0, 0, 0), parquet.ByteArrayValue([]byte("Alice")).Level(0, 0, 1)},
			{parquet.Int32Value(2).Level(0, 0, 0), parquet.ByteArrayValue([]byte("Bob")).Level(0, 0, 1)},
		}
		if _, err := rg1.WriteRows(rows1); err != nil {
			t.Fatal(err)
		}

		// Second row group uses ColumnWriters
		cols := rg2.ColumnWriters()
		idValues := []parquet.Value{
			parquet.Int32Value(3).Level(0, 0, 0),
			parquet.Int32Value(4).Level(0, 0, 0),
		}
		nameValues := []parquet.Value{
			parquet.ByteArrayValue([]byte("Charlie")).Level(0, 0, 1),
			parquet.ByteArrayValue([]byte("Dave")).Level(0, 0, 1),
		}
		if _, err := cols[0].WriteRowValues(idValues); err != nil {
			t.Fatal(err)
		}
		if _, err := cols[1].WriteRowValues(nameValues); err != nil {
			t.Fatal(err)
		}

		// Commit both
		if _, err := rg1.Commit(); err != nil {
			t.Fatal(err)
		}
		if _, err := rg2.Commit(); err != nil {
			t.Fatal(err)
		}

		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}

		// Verify the written data
		f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		if err != nil {
			t.Fatal(err)
		}

		if f.NumRows() != 4 {
			t.Fatalf("expected 4 rows, got %d", f.NumRows())
		}

		if len(f.RowGroups()) != 2 {
			t.Fatalf("expected 2 row groups, got %d", len(f.RowGroups()))
		}

		reader := parquet.NewGenericReader[Row](bytes.NewReader(buf.Bytes()))
		defer reader.Close()

		rows := make([]Row, 4)
		n, err := reader.Read(rows)
		if err != nil && err != io.EOF {
			t.Fatal(err)
		}
		if n != 4 {
			t.Fatalf("expected to read 4 rows, got %d", n)
		}

		expected := []Row{
			{ID: 1, Name: "Alice"},
			{ID: 2, Name: "Bob"},
			{ID: 3, Name: "Charlie"},
			{ID: 4, Name: "Dave"},
		}

		if !reflect.DeepEqual(rows, expected) {
			t.Fatalf("expected %+v, got %+v", expected, rows)
		}
	})
}

func TestDictionaryMaxBytes(t *testing.T) {
	// Test that dictionary encoding switches to PLAIN when size limit is exceeded
	type Record struct {
		Value string `parquet:"value,dict"`
	}

	// Create records with unique strings to grow the dictionary
	numRecords := 1000
	records := make([]Record, numRecords)
	for i := range numRecords {
		// Each string is ~50 bytes, so 1000 records = ~50KB
		records[i].Value = fmt.Sprintf("unique_string_value_%04d_with_padding_to_make_it_longer", i)
	}

	// Test with a very small dictionary limit (1KB) to trigger the fallback
	buf := new(bytes.Buffer)
	writer := parquet.NewGenericWriter[Record](
		buf,
		parquet.DictionaryMaxBytes(1024), // 1KB limit
	)

	// Write records - should trigger dictionary to PLAIN conversion
	if _, err := writer.Write(records); err != nil {
		t.Fatalf("failed to write records: %v", err)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	// Verify we can read the data back correctly
	reader := parquet.NewGenericReader[Record](bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	readRecords := make([]Record, numRecords)
	n, err := reader.Read(readRecords)
	if err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}
	if n != numRecords {
		t.Fatalf("expected to read %d records, got %d", numRecords, n)
	}

	// Verify data integrity
	for i := range numRecords {
		if readRecords[i].Value != records[i].Value {
			t.Errorf("record %d: expected %q, got %q", i, records[i].Value, readRecords[i].Value)
		}
	}
}

func TestDictionaryMaxBytesUnlimited(t *testing.T) {
	// Test that unlimited (default) dictionary size works correctly
	type Record struct {
		Value string `parquet:"value,dict"`
	}

	records := make([]Record, 100)
	for i := range 100 {
		records[i].Value = fmt.Sprintf("value_%d", i)
	}

	buf := new(bytes.Buffer)
	// No DictionaryMaxBytes option = unlimited
	writer := parquet.NewGenericWriter[Record](buf)

	if _, err := writer.Write(records); err != nil {
		t.Fatalf("failed to write records: %v", err)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	// Verify readability
	reader := parquet.NewGenericReader[Record](bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	readRecords := make([]Record, 100)
	n, err := reader.Read(readRecords)
	if err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}
	if n != 100 {
		t.Fatalf("expected to read 100 records, got %d", n)
	}
}

func TestDictionaryMaxBytesPerColumn(t *testing.T) {
	// Test that dictionary limit is per-column, not global
	type Record struct {
		SmallColumn string `parquet:"small_column,dict"`
		LargeColumn string `parquet:"large_column,dict"`
	}

	numRecords := 500
	records := make([]Record, numRecords)
	for i := range numRecords {
		// Small column has small strings (stays under limit)
		records[i].SmallColumn = fmt.Sprintf("s%d", i%10) // Only 10 unique values
		// Large column has large unique strings (exceeds limit)
		records[i].LargeColumn = fmt.Sprintf("large_unique_string_value_%04d_with_lots_of_padding", i)
	}

	buf := new(bytes.Buffer)
	// Set limit to 2KB - small column should stay dict-encoded, large should switch to PLAIN
	writer := parquet.NewGenericWriter[Record](
		buf,
		parquet.DictionaryMaxBytes(2048),
	)

	if _, err := writer.Write(records); err != nil {
		t.Fatalf("failed to write records: %v", err)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	// Verify we can read all data back correctly
	reader := parquet.NewGenericReader[Record](bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	readRecords := make([]Record, numRecords)
	n, err := reader.Read(readRecords)
	if err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}
	if n != numRecords {
		t.Fatalf("expected to read %d records, got %d", numRecords, n)
	}

	// Verify data integrity
	for i := range numRecords {
		if readRecords[i].SmallColumn != records[i].SmallColumn {
			t.Errorf("record %d small_column: expected %q, got %q", i, records[i].SmallColumn, readRecords[i].SmallColumn)
		}
		if readRecords[i].LargeColumn != records[i].LargeColumn {
			t.Errorf("record %d large_column: expected %q, got %q", i, records[i].LargeColumn, readRecords[i].LargeColumn)
		}
	}
}

func TestDictionaryMaxBytesMultipleRowGroups(t *testing.T) {
	// Test that dictionary encoding works across multiple writes
	// (simulating multiple row groups being written)
	type Record struct {
		Value string `parquet:"value,dict"`
	}

	buf := new(bytes.Buffer)
	writer := parquet.NewGenericWriter[Record](
		buf,
		parquet.DictionaryMaxBytes(1024), // Small limit to trigger fallback
	)

	// Write in two batches - this tests that if the first batch triggers
	// dictionary-to-plain conversion, subsequent writes still work
	batch1 := make([]Record, 100)
	for i := range 100 {
		// Large unique strings that will exceed dictionary limit
		batch1[i].Value = fmt.Sprintf("batch_1_unique_value_%04d_with_lots_of_padding", i)
	}

	if _, err := writer.Write(batch1); err != nil {
		t.Fatalf("failed to write batch 1: %v", err)
	}

	batch2 := make([]Record, 100)
	for i := range 100 {
		// More large unique strings
		batch2[i].Value = fmt.Sprintf("batch_2_unique_value_%04d_with_lots_of_padding", i)
	}

	if _, err := writer.Write(batch2); err != nil {
		t.Fatalf("failed to write batch 2: %v", err)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	// Verify all data can be read back correctly
	reader := parquet.NewGenericReader[Record](bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	totalRecords := 200
	readRecords := make([]Record, totalRecords)
	n, err := reader.Read(readRecords)
	if err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}
	if n != totalRecords {
		t.Fatalf("expected to read %d records, got %d", totalRecords, n)
	}

	// Verify data integrity
	for i := range 100 {
		expected := fmt.Sprintf("batch_1_unique_value_%04d_with_lots_of_padding", i)
		if readRecords[i].Value != expected {
			t.Errorf("record %d: expected %q, got %q", i, expected, readRecords[i].Value)
		}
	}
	for i := range 100 {
		expected := fmt.Sprintf("batch_2_unique_value_%04d_with_lots_of_padding", i)
		if readRecords[100+i].Value != expected {
			t.Errorf("record %d: expected %q, got %q", 100+i, expected, readRecords[100+i].Value)
		}
	}
}

// TestIssue185 reproduces the issue reported in
// https://github.com/parquet-go/parquet-go/issues/185
// where rewriting a parquet file with nested types using GenericWriter[any]
// causes a panic in the reflection layer.
func TestIssue185(t *testing.T) {
	type Address struct {
		City string
	}

	type NestedType struct {
		Name    string
		Address Address
	}

	// Step 1: Create source file with nested types
	sourceData := []NestedType{
		{Name: "Alice", Address: Address{City: "New York"}},
		{Name: "Bob", Address: Address{City: "Los Angeles"}},
		{Name: "Charlie", Address: Address{City: "Chicago"}},
	}

	var sourceBuffer bytes.Buffer
	sourceWriter := parquet.NewGenericWriter[NestedType](&sourceBuffer)
	_, err := sourceWriter.Write(sourceData)
	if err != nil {
		t.Fatalf("failed to write source data: %v", err)
	}
	if err := sourceWriter.Close(); err != nil {
		t.Fatalf("failed to close source writer: %v", err)
	}

	// Step 2: Read the file as []any and extract schema
	sourceFile, err := parquet.OpenFile(bytes.NewReader(sourceBuffer.Bytes()), int64(sourceBuffer.Len()))
	if err != nil {
		t.Fatalf("failed to open source file: %v", err)
	}

	schema := sourceFile.Schema()

	// Read rows as any
	reader := parquet.NewGenericReader[any](bytes.NewReader(sourceBuffer.Bytes()))
	defer reader.Close()

	rows := make([]any, len(sourceData))
	n, err := reader.Read(rows)
	if err != nil && err != io.EOF {
		t.Fatalf("failed to read rows: %v", err)
	}
	if n != len(sourceData) {
		t.Fatalf("expected to read %d rows, got %d", len(sourceData), n)
	}

	// Step 3: Write rows (as maps) to a new file using the extracted schema
	var destBuffer bytes.Buffer
	destWriter := parquet.NewGenericWriter[any](&destBuffer, schema)
	defer destWriter.Close()

	_, err = destWriter.Write(rows)
	if err != nil {
		t.Fatalf("failed to write rows to destination: %v", err)
	}

	if err := destWriter.Close(); err != nil {
		t.Fatalf("failed to close destination writer: %v", err)
	}

	// Verify the data can be read back correctly
	verifyReader := parquet.NewGenericReader[NestedType](bytes.NewReader(destBuffer.Bytes()))
	defer verifyReader.Close()

	verifyRows := make([]NestedType, len(sourceData))
	n, err = verifyReader.Read(verifyRows)
	if err != nil && err != io.EOF {
		t.Fatalf("failed to read verification rows: %v", err)
	}
	if n != len(sourceData) {
		t.Fatalf("expected to read %d verification rows, got %d", len(sourceData), n)
	}

	// Verify data integrity
	for i := range sourceData {
		if verifyRows[i] != sourceData[i] {
			t.Errorf("row %d: expected %+v, got %+v", i, sourceData[i], verifyRows[i])
		}
	}
}

// TestIssue316 reproduces the issue reported in
// https://github.com/parquet-go/parquet-go/issues/316
// where nested maps with pointer struct values cause a panic when some of
// the inner maps are nil.
func TestIssue316(t *testing.T) {
	// The issue involves multilevel Go structs containing nested maps where
	// the inner map values are pointer types. When a pointer value in the
	// outer map points to a struct with a nil inner map, the code panics
	// with "invalid memory address or nil pointer dereference" when calling
	// m.Len() on the nil map.

	type LevelStats struct {
		Avg            float64
		Price          float64
		CheapestPerLvl float64
		Count          int64
	}

	type SecStats struct {
		Levels         map[string]*LevelStats
		CheapestPerSec float64
	}

	type Summary struct {
		Sections map[string]*SecStats
	}

	// Create test data where some inner maps are nil
	rows := []Summary{
		{
			Sections: map[string]*SecStats{
				"section1": {
					Levels: map[string]*LevelStats{
						"level1": {Avg: 1.0, Price: 10.0, CheapestPerLvl: 5.0, Count: 100},
						"level2": {Avg: 2.0, Price: 20.0, CheapestPerLvl: 15.0, Count: 200},
					},
					CheapestPerSec: 5.0,
				},
				"section2": {
					Levels:         nil, // This nil map should not cause a panic
					CheapestPerSec: 3.0,
				},
			},
		},
		{
			Sections: nil, // Top-level nil map
		},
		{
			Sections: map[string]*SecStats{
				"section3": nil, // nil pointer value in map
			},
		},
	}

	var buf bytes.Buffer
	writer := parquet.NewGenericWriter[Summary](&buf)

	_, err := writer.Write(rows)
	if err != nil {
		t.Fatalf("failed to write rows: %v", err)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	// Read back and verify basic structure
	reader := parquet.NewGenericReader[Summary](bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	readRows := make([]Summary, len(rows))
	n, err := reader.Read(readRows)
	if err != nil && err != io.EOF {
		t.Fatalf("failed to read rows: %v", err)
	}

	if n != len(rows) {
		t.Fatalf("expected to read %d rows, got %d", len(rows), n)
	}

	// Verify the first row which has fully populated data
	if len(readRows[0].Sections) != 2 {
		t.Errorf("row 0: expected 2 sections, got %d", len(readRows[0].Sections))
	}
	if s1, ok := readRows[0].Sections["section1"]; ok {
		if len(s1.Levels) != 2 {
			t.Errorf("row 0 section1: expected 2 levels, got %d", len(s1.Levels))
		}
		if s1.CheapestPerSec != 5.0 {
			t.Errorf("row 0 section1: expected CheapestPerSec=5.0, got %f", s1.CheapestPerSec)
		}
	} else {
		t.Error("row 0: missing section1")
	}

	// Verify section2 with nil Levels map
	if s2, ok := readRows[0].Sections["section2"]; ok {
		if s2.CheapestPerSec != 3.0 {
			t.Errorf("row 0 section2: expected CheapestPerSec=3.0, got %f", s2.CheapestPerSec)
		}
	} else {
		t.Error("row 0: missing section2")
	}

	// Verify the second row with nil Sections
	if readRows[1].Sections != nil && len(readRows[1].Sections) != 0 {
		t.Errorf("row 1: expected nil or empty Sections, got %d elements", len(readRows[1].Sections))
	}
}

// TestIssue118 reproduces the issue reported in
// https://github.com/parquet-go/parquet-go/issues/118
// The issue was that []*string (slice of pointers to strings) was not
// properly supported, causing a panic when writing.
func TestIssue118(t *testing.T) {
	type Row struct {
		Name    string    `parquet:"name"`
		Classes []*string `parquet:"classes,list"`
	}

	class1 := "math"
	class2 := "science"

	rows := []Row{
		{Name: "Alice", Classes: []*string{&class1, &class2}},
		{Name: "Bob", Classes: []*string{&class1, nil}}, // includes nil
		{Name: "Charlie", Classes: []*string{}},         // empty slice
		{Name: "Diana", Classes: nil},                   // nil slice
	}

	var buf bytes.Buffer
	w := parquet.NewGenericWriter[Row](&buf)
	if _, err := w.Write(rows); err != nil {
		t.Fatalf("failed to write rows: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	// Read back and verify
	r := parquet.NewGenericReader[Row](bytes.NewReader(buf.Bytes()))
	out := make([]Row, len(rows))
	n, err := r.Read(out)
	if err != nil && err != io.EOF {
		t.Fatalf("failed to read rows: %v", err)
	}
	if n != len(rows) {
		t.Fatalf("expected %d rows, got %d", len(rows), n)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("failed to close reader: %v", err)
	}

	// Verify Alice
	if out[0].Name != "Alice" {
		t.Errorf("row 0 name: got %q, want %q", out[0].Name, "Alice")
	}
	if len(out[0].Classes) != 2 {
		t.Errorf("row 0 classes length: got %d, want 2", len(out[0].Classes))
	} else {
		if out[0].Classes[0] == nil || *out[0].Classes[0] != "math" {
			t.Errorf("row 0 classes[0]: got %v, want math", out[0].Classes[0])
		}
		if out[0].Classes[1] == nil || *out[0].Classes[1] != "science" {
			t.Errorf("row 0 classes[1]: got %v, want science", out[0].Classes[1])
		}
	}

	// Verify Bob (has nil element)
	if out[1].Name != "Bob" {
		t.Errorf("row 1 name: got %q, want %q", out[1].Name, "Bob")
	}
	if len(out[1].Classes) != 2 {
		t.Errorf("row 1 classes length: got %d, want 2", len(out[1].Classes))
	} else {
		if out[1].Classes[0] == nil || *out[1].Classes[0] != "math" {
			t.Errorf("row 1 classes[0]: got %v, want math", out[1].Classes[0])
		}
		if out[1].Classes[1] != nil {
			t.Errorf("row 1 classes[1]: got %v, want nil", out[1].Classes[1])
		}
	}

	// Verify Charlie (empty slice)
	if out[2].Name != "Charlie" {
		t.Errorf("row 2 name: got %q, want %q", out[2].Name, "Charlie")
	}
	if len(out[2].Classes) != 0 {
		t.Errorf("row 2 classes length: got %d, want 0", len(out[2].Classes))
	}

	// Verify Diana (nil slice - note: parquet doesn't distinguish between nil and empty slice)
	if out[3].Name != "Diana" {
		t.Errorf("row 3 name: got %q, want %q", out[3].Name, "Diana")
	}
	if len(out[3].Classes) != 0 {
		t.Errorf("row 3 classes length: got %d, want 0", len(out[3].Classes))
	}
}
