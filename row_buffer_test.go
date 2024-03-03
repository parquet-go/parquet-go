package parquet_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"reflect"
	"sort"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/encoding"
)

func TestRowBuffer(t *testing.T) {
	testRowBuffer[booleanColumn](t)
	testRowBuffer[int32Column](t)
	testRowBuffer[int64Column](t)
	testRowBuffer[int96Column](t)
	testRowBuffer[floatColumn](t)
	testRowBuffer[doubleColumn](t)
	testRowBuffer[byteArrayColumn](t)
	testRowBuffer[fixedLenByteArrayColumn](t)
	testRowBuffer[stringColumn](t)
	testRowBuffer[indexedStringColumn](t)
	testRowBuffer[uuidColumn](t)
	testRowBuffer[timeColumn](t)
	testRowBuffer[timeInMillisColumn](t)
	testRowBuffer[mapColumn](t)
	testRowBuffer[decimalColumn](t)
	testRowBuffer[addressBook](t)
	testRowBuffer[contact](t)
	testRowBuffer[listColumn2](t)
	testRowBuffer[listColumn1](t)
	testRowBuffer[listColumn0](t)
	testRowBuffer[nestedListColumn1](t)
	testRowBuffer[nestedListColumn](t)
	testRowBuffer[*contact](t)
	testRowBuffer[paddedBooleanColumn](t)
	testRowBuffer[optionalInt32Column](t)
	testRowBuffer[repeatedInt32Column](t)

	for _, test := range bufferTests {
		t.Run(test.scenario, func(t *testing.T) {
			for _, mod := range [...]struct {
				scenario string
				function func(parquet.Node) parquet.Node
			}{
				{scenario: "optional", function: parquet.Optional},
				{scenario: "repeated", function: parquet.Repeated},
				{scenario: "required", function: parquet.Required},
			} {
				t.Run(mod.scenario, func(t *testing.T) {
					for _, ordering := range [...]struct {
						scenario string
						sorting  parquet.SortingColumn
						sortFunc func(parquet.Type, []parquet.Value)
					}{
						{scenario: "unordered", sorting: nil, sortFunc: unordered},
						{scenario: "ascending", sorting: parquet.Ascending("data"), sortFunc: ascending},
						{scenario: "descending", sorting: parquet.Descending("data"), sortFunc: descending},
					} {
						t.Run(ordering.scenario, func(t *testing.T) {
							schema := parquet.NewSchema("test", parquet.Group{
								"data": mod.function(parquet.Leaf(test.typ)),
							})

							options := []parquet.RowGroupOption{
								schema,
							}

							if ordering.sorting != nil {
								options = append(options,
									parquet.SortingRowGroupConfig(
										parquet.SortingColumns(ordering.sorting),
									),
								)
							}

							content := new(bytes.Buffer)
							buffer := parquet.NewRowBuffer[any](options...)

							for _, values := range test.values {
								t.Run("", func(t *testing.T) {
									defer content.Reset()
									defer buffer.Reset()
									fields := schema.Fields()
									testRowBufferAny(t, fields[0], buffer, &parquet.Plain, values, ordering.sortFunc)
								})
							}
						})
					}
				})
			}
		})
	}
}

func testRowBuffer[Row any](t *testing.T) {
	var model Row
	t.Run(reflect.TypeOf(model).Name(), func(t *testing.T) {
		err := quickCheck(func(rows []Row) bool {
			if len(rows) == 0 {
				return true // TODO: fix support for parquet files with zero rows
			}
			if err := testRowBufferRows(rows); err != nil {
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

func testRowBufferRows[Row any](rows []Row) error {
	setNullPointers(rows)
	buffer := parquet.NewRowBuffer[Row]()
	_, err := buffer.Write(rows)
	if err != nil {
		return err
	}
	reader := parquet.NewGenericRowGroupReader[Row](buffer)
	result := make([]Row, len(rows))
	n, err := reader.Read(result)
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if n < len(rows) {
		return fmt.Errorf("not enough values were read: want=%d got=%d", len(rows), n)
	}
	if !reflect.DeepEqual(rows, result) {
		return fmt.Errorf("rows mismatch:\nwant: %#v\ngot:  %#v", rows, result)
	}
	return nil
}

func testRowBufferAny(t *testing.T, node parquet.Node, buffer *parquet.RowBuffer[any], encoding encoding.Encoding, values []any, sortFunc sortFunc) {
	repetitionLevel := 0
	definitionLevel := 0
	if !node.Required() {
		definitionLevel = 1
	}

	minValue := parquet.Value{}
	maxValue := parquet.Value{}
	batch := make([]parquet.Value, len(values))
	for i := range values {
		batch[i] = parquet.ValueOf(values[i]).Level(repetitionLevel, definitionLevel, 0)
	}

	for i := range batch {
		_, err := buffer.WriteRows([]parquet.Row{batch[i : i+1]})
		if err != nil {
			t.Fatalf("writing value to row group: %v", err)
		}
	}

	numRows := buffer.NumRows()
	if numRows != int64(len(batch)) {
		t.Fatalf("number of rows mismatch: want=%d got=%d", len(batch), numRows)
	}

	typ := node.Type()
	for _, value := range batch {
		if minValue.IsNull() || typ.Compare(value, minValue) < 0 {
			minValue = value
		}
		if maxValue.IsNull() || typ.Compare(value, maxValue) > 0 {
			maxValue = value
		}
	}

	sortFunc(typ, batch)
	sort.Sort(buffer)

	pages := buffer.ColumnChunks()[0].Pages()
	page, err := pages.ReadPage()
	defer pages.Close()

	if err == io.EOF {
		if numRows != 0 {
			t.Fatalf("no pages found in row buffer despite having %d rows", numRows)
		} else {
			return
		}
	}

	numValues := page.NumValues()
	if numValues != int64(len(batch)) {
		t.Fatalf("number of values mistmatch: want=%d got=%d", len(batch), numValues)
	}

	numNulls := page.NumNulls()
	if numNulls != 0 {
		t.Fatalf("number of nulls mismatch: want=0 got=%d", numNulls)
	}

	min, max, hasBounds := page.Bounds()
	if !hasBounds && numRows > 0 {
		t.Fatal("page bounds are missing")
	}
	if !parquet.Equal(min, minValue) {
		t.Fatalf("min value mismatch: want=%v got=%v", minValue, min)
	}
	if !parquet.Equal(max, maxValue) {
		t.Fatalf("max value mismatch: want=%v got=%v", maxValue, max)
	}

	// We write a single value per row, so num values = num rows for all pages
	// including repeated ones, which makes it OK to slice the pages using the
	// number of values as a proxy for the row indexes.
	halfValues := numValues / 2

	for _, test := range [...]struct {
		scenario string
		values   []parquet.Value
		reader   parquet.ValueReader
	}{
		{"page", batch, page.Values()},
		{"head", batch[:halfValues], page.Slice(0, halfValues).Values()},
		{"tail", batch[halfValues:], page.Slice(halfValues, numValues).Values()},
	} {
		v := [1]parquet.Value{}
		i := 0

		for {
			n, err := test.reader.ReadValues(v[:])
			if n > 0 {
				if n != 1 {
					t.Fatalf("reading value from %q reader returned the wrong count: want=1 got=%d", test.scenario, n)
				}
				if i < len(test.values) {
					if !parquet.Equal(v[0], test.values[i]) {
						t.Fatalf("%q value at index %d mismatches: want=%v got=%v", test.scenario, i, test.values[i], v[0])
					}
				}
				i++
			}
			if err != nil {
				if err == io.EOF {
					break
				}
				t.Fatalf("reading value from %q reader: %v", test.scenario, err)
			}
		}

		if i != len(test.values) {
			t.Errorf("wrong number of values read from %q reader: want=%d got=%d", test.scenario, len(test.values), i)
		}
	}
}

func BenchmarkSortRowBuffer(b *testing.B) {
	type Row struct {
		I0 int64
		I1 int64
		I2 int64
		I3 int64
		I4 int64
		I5 int64
		I6 int64
		I7 int64
		I8 int64
		I9 int64
		ID [16]byte
	}

	buf := parquet.NewRowBuffer[Row](
		parquet.SortingRowGroupConfig(
			parquet.SortingColumns(
				parquet.Ascending("ID"),
			),
		),
	)

	rows := make([]Row, 10e3)
	prng := rand.New(rand.NewSource(0))

	for i := range rows {
		binary.LittleEndian.PutUint64(rows[i].ID[:8], uint64(i))
		binary.LittleEndian.PutUint64(rows[i].ID[8:], ^uint64(i))
	}

	buf.Write(rows)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for j := 0; j < 10; j++ {
			buf.Swap(prng.Intn(len(rows)), prng.Intn(len(rows)))
		}

		sort.Sort(buf)
	}
}

func BenchmarkMergeRowBuffers(b *testing.B) {
	type Row struct {
		ID int64 `parquet:"id"`
	}

	const (
		numBuffers       = 100
		numRowsPerBuffer = 10e3
	)

	rows := [numBuffers][numRowsPerBuffer]Row{}
	nextID := int64(0)
	for i := 0; i < numRowsPerBuffer; i++ {
		for j := 0; j < numBuffers; j++ {
			rows[j][i].ID = nextID
			nextID++
		}
	}

	options := []parquet.RowGroupOption{
		parquet.SortingRowGroupConfig(
			parquet.SortingColumns(
				parquet.Ascending("id"),
			),
		),
	}

	rowGroups := make([]parquet.RowGroup, numBuffers)
	for i := range rowGroups {
		buffer := parquet.NewRowBuffer[Row](options...)
		buffer.Write(rows[i][:])
		rowGroups[i] = buffer
	}

	merge, err := parquet.MergeRowGroups(rowGroups, options...)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		rows := merge.Rows()
		_, err := parquet.CopyRows(discardRows{}, rows)
		rows.Close()
		if err != nil {
			b.Fatal(err)
		}
	}
}

type discardRows struct{}

func (discardRows) WriteRows(rows []parquet.Row) (int, error) {
	return len(rows), nil
}
