package parquet_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/encoding"
)

func TestGenericBuffer(t *testing.T) {
	testGenericBuffer[booleanColumn](t)
	testGenericBuffer[int8Column](t)
	testGenericBuffer[int16Column](t)
	testGenericBuffer[int32Column](t)
	testGenericBuffer[uint8Column](t)
	testGenericBuffer[uint16Column](t)
	testGenericBuffer[int64Column](t)
	testGenericBuffer[int96Column](t)
	testGenericBuffer[floatColumn](t)
	testGenericBuffer[doubleColumn](t)
	testGenericBuffer[byteArrayColumn](t)
	testGenericBuffer[fixedLenByteArrayColumn](t)
	testGenericBuffer[stringColumn](t)
	testGenericBuffer[indexedStringColumn](t)
	testGenericBuffer[uuidColumn](t)
	testGenericBuffer[timeColumn](t)
	testGenericBuffer[timeInMillisColumn](t)
	testGenericBuffer[mapColumn](t)
	testGenericBuffer[decimalColumn](t)
	testGenericBuffer[addressBook](t)
	testGenericBuffer[contact](t)
	testGenericBuffer[listColumn2](t)
	testGenericBuffer[listColumn1](t)
	testGenericBuffer[listColumn0](t)
	testGenericBuffer[nestedListColumn1](t)
	testGenericBuffer[nestedListColumn](t)
	testGenericBuffer[*contact](t)
	testGenericBuffer[paddedBooleanColumn](t)
	testGenericBuffer[optionalInt32Column](t)
	testGenericBuffer[repeatedInt32Column](t)
}

func testGenericBuffer[Row any](t *testing.T) {
	var model Row
	t.Run(reflect.TypeOf(model).Name(), func(t *testing.T) {
		err := quickCheck(func(rows []Row) bool {
			if err := testGenericBufferRows(rows); err != nil {
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

func testGenericBufferRows[Row any](rows []Row) error {
	setNullPointers(rows)
	buffer := parquet.NewGenericBuffer[Row]()
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

func setNullPointers[Row any](rows []Row) {
	if len(rows) > 0 && reflect.TypeOf(rows[0]).Kind() == reflect.Pointer {
		for i := range rows {
			v := reflect.ValueOf(&rows[i]).Elem()
			if v.IsNil() {
				v.Set(reflect.New(v.Type().Elem()))
			}
		}
	}
}

type generator[T any] interface {
	generate(*rand.Rand) T
}

func BenchmarkGenericBuffer(b *testing.B) {
	benchmarkGenericBuffer[benchmarkRowType](b)
	benchmarkGenericBuffer[booleanColumn](b)
	benchmarkGenericBuffer[int8Column](b)
	benchmarkGenericBuffer[int16Column](b)
	benchmarkGenericBuffer[int32Column](b)
	benchmarkGenericBuffer[uint8Column](b)
	benchmarkGenericBuffer[uint16Column](b)
	benchmarkGenericBuffer[int64Column](b)
	benchmarkGenericBuffer[floatColumn](b)
	benchmarkGenericBuffer[doubleColumn](b)
	benchmarkGenericBuffer[byteArrayColumn](b)
	benchmarkGenericBuffer[fixedLenByteArrayColumn](b)
	benchmarkGenericBuffer[stringColumn](b)
	benchmarkGenericBuffer[indexedStringColumn](b)
	benchmarkGenericBuffer[uuidColumn](b)
	benchmarkGenericBuffer[timeColumn](b)
	benchmarkGenericBuffer[timeInMillisColumn](b)
	benchmarkGenericBuffer[mapColumn](b)
	benchmarkGenericBuffer[decimalColumn](b)
	benchmarkGenericBuffer[contact](b)
	benchmarkGenericBuffer[paddedBooleanColumn](b)
	benchmarkGenericBuffer[optionalInt32Column](b)
	benchmarkGenericBuffer[repeatedInt32Column](b)
}

func benchmarkGenericBuffer[Row generator[Row]](b *testing.B) {
	var model Row
	b.Run(reflect.TypeOf(model).Name(), func(b *testing.B) {
		prng := rand.New(rand.NewSource(0))
		rows := make([]Row, benchmarkNumRows)
		for i := range rows {
			rows[i] = rows[i].generate(prng)
		}

		b.Run("go1.17", func(b *testing.B) {
			buffer := parquet.NewBuffer(parquet.SchemaOf(rows[0]))
			i := 0
			benchmarkRowsPerSecond(b, func() int {
				for range benchmarkRowsPerStep {
					if err := buffer.Write(&rows[i]); err != nil {
						b.Fatal(err)
					}
				}

				i += benchmarkRowsPerStep
				i %= benchmarkNumRows

				if i == 0 {
					buffer.Reset()
				}
				return benchmarkRowsPerStep
			})
		})

		b.Run("go1.18", func(b *testing.B) {
			buffer := parquet.NewGenericBuffer[Row]()
			i := 0
			benchmarkRowsPerSecond(b, func() int {
				n, err := buffer.Write(rows[i : i+benchmarkRowsPerStep])
				if err != nil {
					b.Fatal(err)
				}

				i += benchmarkRowsPerStep
				i %= benchmarkNumRows

				if i == 0 {
					buffer.Reset()
				}
				return n
			})
		})
	})
}

func TestIssueSegmentio327(t *testing.T) {
	t.Run("untagged nested lists should panic", func(t *testing.T) {
		type testType struct {
			ListOfLists [][]int
		}

		defer func() {
			if r := recover(); r == nil {
				t.Errorf("Nested lists without the list tag should panic")
			}
		}()

		_ = parquet.NewGenericBuffer[testType]()
	})
}

func TestIssueSegmentio346(t *testing.T) {
	type TestType struct {
		Key int
	}

	schema := parquet.SchemaOf(TestType{})
	buffer := parquet.NewGenericBuffer[any](schema)

	data := make([]any, 1)
	data[0] = TestType{Key: 0}
	_, _ = buffer.Write(data)
}

func TestIssueSegmentio347(t *testing.T) {
	type TestType struct {
		Key int
	}

	// instantiating with concrete type shouldn't panic
	_ = parquet.NewGenericBuffer[TestType]()

	// instantiating with schema and interface type parameter shouldn't panic
	schema := parquet.SchemaOf(TestType{})
	_ = parquet.NewGenericBuffer[any](schema)

	defer func() {
		if r := recover(); r == nil {
			t.Errorf("instantiating generic buffer without schema and with interface " +
				"type parameter should panic")
		}
	}()
	_ = parquet.NewGenericBuffer[any]()
}

func BenchmarkSortGenericBuffer(b *testing.B) {
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

	buffer := parquet.NewGenericBuffer[Row](
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

	buffer.Write(rows)

	for b.Loop() {
		for range 10 {
			buffer.Swap(prng.Intn(len(rows)), prng.Intn(len(rows)))
		}

		sort.Sort(buffer)
	}
}

var bufferTests = [...]struct {
	scenario string
	typ      parquet.Type
	values   [][]any
}{
	{
		scenario: "boolean",
		typ:      parquet.BooleanType,
		values: [][]any{
			{},
			{false},
			{true},
			{
				false, true, false, false, true, true,
				false, false, false, true, false, true,
			},
		},
	},

	{
		scenario: "int32",
		typ:      parquet.Int32Type,
		values: [][]any{
			{},
			{int32(0)},
			{int32(1)},
			{
				int32(1), int32(2), int32(3), int32(4), int32(5), int32(6),
				int32(math.MaxInt8), int32(math.MaxInt16), int32(math.MaxInt32),
				int32(7), int32(9), int32(9), int32(0),
			},
		},
	},

	{
		scenario: "int64",
		typ:      parquet.Int64Type,
		values: [][]any{
			{},
			{int64(0)},
			{int64(1)},
			{
				int64(1), int64(2), int64(3), int64(4), int64(5), int64(6),
				int64(math.MaxInt8), int64(math.MaxInt16), int64(math.MaxInt64), int64(7),
				int64(9), int64(9), int64(0),
			},
		},
	},

	{
		scenario: "float",
		typ:      parquet.FloatType,
		values: [][]any{
			{},
			{float32(0)},
			{float32(1)},
			{
				float32(1), float32(2), float32(3), float32(4), float32(5), float32(6),
				float32(0.5), float32(math.SmallestNonzeroFloat32), float32(math.MaxFloat32), float32(7),
				float32(9), float32(9), float32(0),
			},
		},
	},

	{
		scenario: "double",
		typ:      parquet.DoubleType,
		values: [][]any{
			{},
			{float64(0)},
			{float64(1)},
			{
				float64(1), float64(2), float64(3), float64(4), float64(5), float64(6),
				float64(0.5), float64(math.SmallestNonzeroFloat64), float64(math.MaxFloat64), float64(7),
				float64(9), float64(9), float64(0),
			},
		},
	},

	{
		scenario: "string",
		typ:      parquet.ByteArrayType,
		values: [][]any{
			{},
			{""},
			{"Hello World!"},
			{
				"ABCDEFG", "HIJKLMN", "OPQRSTU", "VWXZY01", "2345678",
				"90!@#$%", "^&*()_+", "Hello World!", "Answer=42", "ABCEDFG",
				"HIJKLMN", "OPQRSTU", "VWXYZ",
			},
		},
	},

	{
		scenario: "fixed length byte array",
		typ:      parquet.FixedLenByteArrayType(10),
		values: [][]any{
			{},
			{[10]byte{}},
			{[10]byte{0: 1}},
			{
				[10]byte{0: 0}, [10]byte{0: 2}, [10]byte{0: 1}, [10]byte{0: 4}, [10]byte{0: 3},
				[10]byte{0: 6}, [10]byte{0: 5}, [10]byte{0: 8}, [10]byte{0: 7}, [10]byte{0: 10},
				[10]byte{0: 11}, [10]byte{0: 12}, [10]byte{9: 0xFF},
			},
		},
	},

	{
		scenario: "uuid",
		typ:      parquet.UUID().Type(),
		values: [][]any{
			{},
			{[16]byte{}},
			{[16]byte{0: 1}},
			{
				[16]byte{0: 0}, [16]byte{0: 2}, [16]byte{0: 1}, [16]byte{0: 4}, [16]byte{0: 3},
				[16]byte{0: 6}, [16]byte{0: 5}, [16]byte{0: 8}, [16]byte{0: 7}, [16]byte{0: 10},
				[16]byte{0: 11}, [16]byte{0: 12}, [16]byte{15: 0xFF},
			},
		},
	},

	{
		scenario: "uint32",
		typ:      parquet.Uint(32).Type(),
		values: [][]any{
			{},
			{uint32(0)},
			{uint32(1)},
			{
				uint32(1), uint32(2), uint32(3), uint32(4), uint32(5), uint32(6),
				uint32(math.MaxInt8), uint32(math.MaxInt16), uint32(math.MaxUint32), uint32(7),
				uint32(9), uint32(9), uint32(0),
			},
		},
	},

	{
		scenario: "uint64",
		typ:      parquet.Uint(64).Type(),
		values: [][]any{
			{},
			{uint64(0)},
			{uint64(1)},
			{
				uint64(1), uint64(2), uint64(3), uint64(4), uint64(5), uint64(6),
				uint64(math.MaxInt8), uint64(math.MaxInt16), uint64(math.MaxUint64),
				uint64(7), uint64(9), uint64(9), uint64(0),
			},
		},
	},
}

func TestBuffer(t *testing.T) {
	for _, test := range bufferTests {
		t.Run(test.scenario, func(t *testing.T) {
			for _, config := range [...]struct {
				scenario string
				typ      parquet.Type
			}{
				{scenario: "plain", typ: test.typ},
				{scenario: "indexed", typ: test.typ.NewDictionary(0, 0, test.typ.NewValues(nil, nil)).Type()},
			} {
				t.Run(config.scenario, func(t *testing.T) {
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
										"data": mod.function(parquet.Leaf(config.typ)),
									})

									options := []parquet.RowGroupOption{
										schema,
										parquet.ColumnBufferCapacity(100),
									}
									if ordering.sorting != nil {
										options = append(options,
											parquet.SortingRowGroupConfig(
												parquet.SortingColumns(ordering.sorting),
											),
										)
									}

									content := new(bytes.Buffer)
									buffer := parquet.NewBuffer(options...)

									for _, values := range test.values {
										t.Run("", func(t *testing.T) {
											defer content.Reset()
											defer buffer.Reset()
											fields := schema.Fields()
											testBuffer(t, fields[0], buffer, &parquet.Plain, values, ordering.sortFunc)
										})
									}
								})
							}
						})
					}
				})
			}
		})
	}
}

type sortFunc func(parquet.Type, []parquet.Value)

func unordered(typ parquet.Type, values []parquet.Value) {}

func ascending(typ parquet.Type, values []parquet.Value) {
	slices.SortFunc(values, func(a, b parquet.Value) int { return typ.Compare(a, b) })
}

func descending(typ parquet.Type, values []parquet.Value) {
	ascending(typ, values)
	slices.Reverse(values)
}

func testBuffer(t *testing.T, node parquet.Node, buffer *parquet.Buffer, encoding encoding.Encoding, values []any, sortFunc sortFunc) {
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

	page := buffer.ColumnBuffers()[0].Page()
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

scenarios:
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
					t.Errorf("reading value from %q reader returned the wrong count: want=1 got=%d", test.scenario, n)
					continue scenarios
				}
				if i < len(test.values) {
					if !parquet.Equal(v[0], test.values[i]) {
						t.Errorf("%q value at index %d mismatches: want=%v got=%v", test.scenario, i, test.values[i], v[0])
						continue scenarios
					}
				}
				i++
			}
			if err != nil {
				if err != io.EOF {
					t.Errorf("reading value from %q reader: %v", test.scenario, err)
				}
				break
			}
		}

		if i != len(test.values) {
			t.Errorf("wrong number of values read from %q reader: want=%d got=%d", test.scenario, len(test.values), i)
		}
	}
}

func TestBufferGenerateBloomFilters(t *testing.T) {
	type Point3D struct {
		X float64
		Y float64
		Z float64
	}

	f := func(rows []Point3D) bool {
		output := new(bytes.Buffer)
		buffer := parquet.NewGenericBuffer[Point3D]()
		writer := parquet.NewGenericWriter[Point3D](output,
			parquet.BloomFilters(
				parquet.SplitBlockFilter(10, "X"),
				parquet.SplitBlockFilter(10, "Y"),
				parquet.SplitBlockFilter(10, "Z"),
			),
		)
		buffer.Write(rows)
		_, err := copyRowsAndClose(writer, buffer.Rows())
		if err != nil {
			t.Error(err)
			return false
		}
		if err := writer.Close(); err != nil {
			t.Error(err)
			return false
		}

		reader := bytes.NewReader(output.Bytes())
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
		x := columns[0]
		y := columns[1]
		z := columns[2]

		for i, col := range []parquet.ColumnChunk{x, y, z} {
			if col.BloomFilter() == nil {
				t.Errorf("column %d has no bloom filter despite being configured to have one", i)
				return false
			}
		}

		fx := x.BloomFilter()
		fy := y.BloomFilter()
		fz := z.BloomFilter()

		test := func(f parquet.BloomFilter, v float64) bool {
			if ok, err := f.Check(parquet.ValueOf(v)); err != nil {
				t.Errorf("unexpected error checking bloom filter: %v", err)
				return false
			} else if !ok {
				t.Errorf("bloom filter does not contain value %g", v)
				return false
			}
			return true
		}

		for _, row := range rows {
			if !test(fx, row.X) || !test(fy, row.Y) || !test(fz, row.Z) {
				return false
			}
		}

		return true
	}

	if err := quickCheck(f); err != nil {
		t.Error(err)
	}
}

func TestBufferRoundtripNestedRepeated(t *testing.T) {
	type C struct {
		D int
	}
	type B struct {
		C []C
	}
	type A struct {
		B []B
	}

	// Write enough objects to exceed first page
	buffer := parquet.NewBuffer()
	var objs []A
	for i := range 6 {
		o := A{[]B{{[]C{
			{i},
			{i},
		}}}}
		buffer.Write(&o)
		objs = append(objs, o)
	}

	buf := new(bytes.Buffer)
	w := parquet.NewWriter(buf, parquet.PageBufferSize(100))
	w.WriteRowGroup(buffer)
	w.Flush()
	w.Close()

	file := bytes.NewReader(buf.Bytes())
	r := parquet.NewReader(file)
	for i := 0; ; i++ {
		o := new(A)
		err := r.Read(o)
		if errors.Is(err, io.EOF) {
			if i < len(objs) {
				t.Errorf("too few rows were read: %d<%d", i, len(objs))
			}
			break
		}
		if !reflect.DeepEqual(*o, objs[i]) {
			t.Errorf("points mismatch at row index %d: want=%v got=%v", i, objs[i], o)
		}
	}
}

func TestBufferRoundtripNestedRepeatedPointer(t *testing.T) {
	type C struct {
		D *int
	}
	type B struct {
		C []C
	}
	type A struct {
		B []B
	}

	// Write enough objects to exceed first page
	buffer := parquet.NewBuffer()
	var objs []A
	for i := range 6 {
		j := i
		o := A{[]B{{[]C{
			{&j},
			{nil},
		}}}}
		buffer.Write(&o)
		objs = append(objs, o)
	}

	buf := new(bytes.Buffer)
	w := parquet.NewWriter(buf, parquet.PageBufferSize(100))
	w.WriteRowGroup(buffer)
	w.Flush()
	w.Close()

	file := bytes.NewReader(buf.Bytes())
	r := parquet.NewReader(file)
	for i := 0; ; i++ {
		o := new(A)
		err := r.Read(o)
		if err == io.EOF {
			break
		}
		if !reflect.DeepEqual(*o, objs[i]) {
			t.Errorf("points mismatch at row index %d: want=%v got=%v", i, objs[i], o)
		}
	}
}

func TestRoundtripNestedRepeatedBytes(t *testing.T) {
	type B struct {
		C []byte
	}
	type A struct {
		A string
		B []B
	}

	var objs []A
	for i := range 2 {
		o := A{
			"test" + strconv.Itoa(i),
			[]B{
				{[]byte{byte(i)}},
			},
		}
		objs = append(objs, o)
	}

	buf := new(bytes.Buffer)
	w := parquet.NewWriter(buf, parquet.PageBufferSize(100))
	for _, o := range objs {
		w.Write(&o)
	}
	w.Close()

	file := bytes.NewReader(buf.Bytes())

	r := parquet.NewReader(file)
	for i := 0; ; i++ {
		o := new(A)
		err := r.Read(o)
		if errors.Is(err, io.EOF) {
			if i < len(objs) {
				t.Errorf("too few rows were read: %d<%d", i, len(objs))
			}
			break
		}
		if !reflect.DeepEqual(*o, objs[i]) {
			t.Errorf("points mismatch at row index %d: want=%v got=%v", i, objs[i], o)
		}
	}
}

func TestBufferSeekToRow(t *testing.T) {
	type B struct {
		I int
		C []string
	}
	type A struct {
		B []B
	}

	buffer := parquet.NewBuffer()
	var objs []A
	for i := range 2 {
		o := A{
			B: []B{
				{I: i, C: []string{"foo", strconv.Itoa(i)}},
				{I: i + 1, C: []string{"bar", strconv.Itoa(i + 1)}},
			},
		}
		buffer.Write(&o)
		objs = append(objs, o)
	}

	buf := new(bytes.Buffer)
	w := parquet.NewWriter(buf)
	w.WriteRowGroup(buffer)
	w.Flush()
	w.Close()

	file := bytes.NewReader(buf.Bytes())
	r := parquet.NewReader(file)

	i := 1
	o := new(A)
	if err := r.SeekToRow(int64(i)); err != nil {
		t.Fatal(err)
	}
	if err := r.Read(o); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(*o, objs[i]) {
		t.Errorf("points mismatch at row index %d: want=%v got=%v", i, objs[i], o)
	}
}

type TestStruct struct {
	A *string `parquet:"a,optional,dict"`
}

func TestOptionalDictWriteRowGroup(t *testing.T) {
	s := parquet.SchemaOf(&TestStruct{})

	str1 := "test1"
	str2 := "test2"
	records := []*TestStruct{
		{A: nil},
		{A: &str1},
		{A: nil},
		{A: &str2},
		{A: nil},
	}

	buf := parquet.NewBuffer(s)
	for _, rec := range records {
		row := s.Deconstruct(nil, rec)
		_, err := buf.WriteRows([]parquet.Row{row})
		if err != nil {
			t.Fatal(err)
		}
	}

	b := bytes.NewBuffer(nil)
	w := parquet.NewWriter(b)
	_, err := w.WriteRowGroup(buf)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestNullsSortFirst(t *testing.T) {
	s := parquet.SchemaOf(&TestStruct{})

	str1 := "test1"
	str2 := "test2"
	records := []*TestStruct{
		{A: &str1},
		{A: nil},
		{A: &str2},
	}
	buffer := parquet.NewBuffer(
		s,
		parquet.SortingRowGroupConfig(parquet.SortingColumns(parquet.NullsFirst(parquet.Ascending(s.Columns()[0][0])))),
	)
	for _, rec := range records {
		row := s.Deconstruct(nil, rec)
		_, err := buffer.WriteRows([]parquet.Row{row})
		if err != nil {
			t.Fatal(err)
		}
	}

	for range 2 {
		sort.Sort(buffer)

		rows := buffer.Rows()
		rowBuf := make([]parquet.Row, len(records))
		if n, err := rows.ReadRows(rowBuf); n != len(records) {
			t.Fatal(err)
		}

		resultRecords := make([]TestStruct, len(records))
		for i, r := range rowBuf {
			if err := s.Reconstruct(&resultRecords[i], r); err != nil {
				t.Fatal(err)
			}
		}

		rows.Close()

		want := []TestStruct{{A: nil}, {A: &str1}, {A: &str2}}
		if !reflect.DeepEqual(resultRecords, want) {
			t.Fatalf("sorted rows mismatch: got %v, want %v", resultRecords, want)
		}
	}
}

func TestBufferSortNulls(t *testing.T) {
	type row struct {
		Key *int64
	}
	negative, zero, positive := int64(-1), int64(0), int64(1)
	for _, test := range []struct {
		name    string
		sorting parquet.SortingColumn
		want    []row
	}{
		{"ascending/nulls-last", parquet.Ascending("Key"), []row{{&negative}, {&zero}, {&zero}, {&positive}, {}, {}}},
		{"ascending/nulls-first", parquet.NullsFirst(parquet.Ascending("Key")), []row{{}, {}, {&negative}, {&zero}, {&zero}, {&positive}}},
		{"descending/nulls-last", parquet.Descending("Key"), []row{{&positive}, {&zero}, {&zero}, {&negative}, {}, {}}},
		{"descending/nulls-first", parquet.NullsFirst(parquet.Descending("Key")), []row{{}, {}, {&positive}, {&zero}, {&zero}, {&negative}}},
	} {
		for _, values := range []struct {
			name  string
			input []row
			want  []row
		}{
			{"mixed", []row{{&zero}, {}, {&negative}, {&positive}, {}, {&zero}}, test.want},
			{"equal", []row{{&zero}, {&zero}}, []row{{&zero}, {&zero}}},
			{"all-null", []row{{}, {}}, []row{{}, {}}},
			{"empty", []row{}, []row{}},
		} {
			for _, generic := range []bool{false, true} {
				option := parquet.SortingRowGroupConfig(parquet.SortingColumns(test.sorting))
				var buffer interface {
					parquet.RowGroup
					sort.Interface
				}
				if generic {
					b := parquet.NewGenericBuffer[row](option)
					if _, err := b.Write(values.input); err != nil {
						t.Fatal(err)
					}
					buffer = b
				} else {
					b := parquet.NewBuffer(parquet.SchemaOf(row{}), option)
					rows := make([]parquet.Row, len(values.input))
					for i := range rows {
						rows[i] = b.Schema().Deconstruct(nil, &values.input[i])
					}
					if _, err := b.WriteRows(rows); err != nil {
						t.Fatal(err)
					}
					buffer = b
				}
				sort.Sort(buffer)
				reader := parquet.NewGenericRowGroupReader[row](buffer)
				defer reader.Close()
				got := make([]row, len(values.input))
				n, err := reader.Read(got)
				if err != nil && !errors.Is(err, io.EOF) {
					t.Fatal(err)
				}
				if n != len(got) {
					t.Fatalf("read %d rows, want %d", n, len(got))
				}
				if !reflect.DeepEqual(got, values.want) {
					t.Errorf("sorted rows mismatch (%s/%s/generic=%t): got %v, want %v", test.name, values.name, generic, got, values.want)
				}
			}
		}
	}
}

func TestBufferSortRepeatedNulls(t *testing.T) {
	// An empty repeated value is a null at the leaf: its definition level is
	// below the maximum. Descending must keep that null placement.
	type row struct {
		Key []int64
	}
	negative, zero, positive := []int64{-1}, []int64{0}, []int64{1}
	empty := []int64{}
	for _, test := range []struct {
		name    string
		sorting parquet.SortingColumn
		want    []row
	}{
		{"ascending/nulls-last", parquet.Ascending("Key"), []row{{negative}, {zero}, {zero}, {positive}, {empty}, {empty}}},
		{"ascending/nulls-first", parquet.NullsFirst(parquet.Ascending("Key")), []row{{empty}, {empty}, {negative}, {zero}, {zero}, {positive}}},
		{"descending/nulls-last", parquet.Descending("Key"), []row{{positive}, {zero}, {zero}, {negative}, {empty}, {empty}}},
		{"descending/nulls-first", parquet.NullsFirst(parquet.Descending("Key")), []row{{empty}, {empty}, {positive}, {zero}, {zero}, {negative}}},
	} {
		for _, values := range []struct {
			name  string
			input []row
			want  []row
		}{
			{"mixed", []row{{Key: zero}, {}, {Key: negative}, {Key: positive}, {}, {Key: zero}}, test.want},
			{"equal", []row{{Key: zero}, {Key: zero}}, []row{{Key: zero}, {Key: zero}}},
			{"all-null", []row{{}, {}}, []row{{Key: empty}, {Key: empty}}},
			{"empty", []row{}, []row{}},
		} {
			for _, generic := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/generic=%t", test.name, values.name, generic), func(t *testing.T) {
					option := parquet.SortingRowGroupConfig(parquet.SortingColumns(test.sorting))
					var buffer interface {
						parquet.RowGroup
						sort.Interface
					}
					if generic {
						b := parquet.NewGenericBuffer[row](option)
						if _, err := b.Write(values.input); err != nil {
							t.Fatal(err)
						}
						buffer = b
					} else {
						b := parquet.NewBuffer(parquet.SchemaOf(row{}), option)
						rows := make([]parquet.Row, len(values.input))
						for i := range rows {
							rows[i] = b.Schema().Deconstruct(nil, &values.input[i])
						}
						if _, err := b.WriteRows(rows); err != nil {
							t.Fatal(err)
						}
						buffer = b
					}
					for cycle := 1; cycle <= 2; cycle++ {
						sort.Sort(buffer)
						reader := parquet.NewGenericRowGroupReader[row](buffer)
						got := make([]row, len(values.input))
						n, err := reader.Read(got)
						reader.Close()
						if err != nil && !errors.Is(err, io.EOF) {
							t.Fatal(err)
						}
						if n != len(got) {
							t.Fatalf("read %d rows, want %d", n, len(got))
						}
						if !reflect.DeepEqual(got, values.want) {
							t.Fatalf("sorted rows mismatch after sort/read %d: got %#v, want %#v", cycle, got, values.want)
						}
					}
				})
			}
		}
	}
}

func TestBufferSortRepeatedPrefix(t *testing.T) {
	type row struct{ Key []int64 }
	input := []row{{Key: []int64{0}}, {Key: []int64{0, 0}}}
	schema := parquet.SchemaOf(row{})
	rows := make([]parquet.Row, len(input))
	for i := range input {
		rows[i] = schema.Deconstruct(nil, &input[i])
	}
	for _, test := range []struct {
		name    string
		columns []parquet.SortingColumn
		want    bool
	}{
		{"default", nil, false},
		{"ascending", []parquet.SortingColumn{parquet.Ascending("Key")}, true},
		{"ascending/nulls-first", []parquet.SortingColumn{parquet.NullsFirst(parquet.Ascending("Key"))}, true},
		{"descending", []parquet.SortingColumn{parquet.Descending("Key")}, false},
		{"descending/nulls-first", []parquet.SortingColumn{parquet.NullsFirst(parquet.Descending("Key"))}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			option := parquet.SortingRowGroupConfig(parquet.SortingColumns(test.columns...))
			buffer := parquet.NewBuffer(schema, option)
			if _, err := buffer.WriteRows(rows); err != nil {
				t.Fatal(err)
			}
			generic := parquet.NewGenericBuffer[row](option)
			if _, err := generic.Write(input); err != nil {
				t.Fatal(err)
			}
			rowBuffer := parquet.NewRowBuffer[row](option)
			if _, err := rowBuffer.Write(input); err != nil {
				t.Fatal(err)
			}
			compare := schema.Comparator(test.columns...)
			wantReverse := len(test.columns) > 0 && !test.want
			if (compare(rows[0], rows[1]) < 0) != test.want || (compare(rows[1], rows[0]) < 0) != wantReverse {
				t.Error("schema comparator prefix order disagrees with sorting direction")
			}
			for name, b := range map[string]sort.Interface{
				"Buffer": buffer, "GenericBuffer": generic, "RowBuffer": rowBuffer,
			} {
				if b.Less(0, 1) != test.want || b.Less(1, 0) != wantReverse {
					t.Errorf("%s prefix order: Less(0,1)=%t Less(1,0)=%t, want %t and %t", name, b.Less(0, 1), b.Less(1, 0), test.want, wantReverse)
				}
			}
		})
	}
}

func TestBufferSortNullsAfterRead(t *testing.T) {
	type row struct {
		Key *int64
	}
	zero, one := int64(0), int64(1)
	for _, test := range []struct {
		name    string
		sorting parquet.SortingColumn
		input   []row
		want    []row
	}{
		{"descending", parquet.NullsFirst(parquet.Descending("Key")), []row{{}, {&zero}, {&one}}, []row{{}, {&one}, {&zero}}},
		{"ascending", parquet.NullsFirst(parquet.Ascending("Key")), []row{{}, {&one}, {&zero}}, []row{{}, {&zero}, {&one}}},
	} {
		for _, generic := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/generic=%t", test.name, generic), func(t *testing.T) {
				option := parquet.SortingRowGroupConfig(parquet.SortingColumns(test.sorting))
				var buffer interface {
					parquet.RowGroup
					sort.Interface
				}
				if generic {
					b := parquet.NewGenericBuffer[row](option)
					if _, err := b.Write(test.input); err != nil {
						t.Fatal(err)
					}
					buffer = b
				} else {
					b := parquet.NewBuffer(parquet.SchemaOf(row{}), option)
					rows := make([]parquet.Row, len(test.input))
					for i := range rows {
						rows[i] = b.Schema().Deconstruct(nil, &test.input[i])
					}
					if _, err := b.WriteRows(rows); err != nil {
						t.Fatal(err)
					}
					buffer = b
				}
				for i := range 2 {
					func() {
						defer func() {
							if err := recover(); err != nil {
								t.Fatalf("sort/read %d panicked: %v", i+1, err)
							}
						}()
						sort.Sort(buffer)
						reader := parquet.NewGenericRowGroupReader[row](buffer)
						defer reader.Close()
						got := make([]row, len(test.input))
						n, err := reader.Read(got)
						if err != nil && !errors.Is(err, io.EOF) {
							t.Fatal(err)
						}
						if n != len(got) {
							t.Fatalf("read %d rows, want %d", n, len(got))
						}
						if !reflect.DeepEqual(got, test.want) {
							t.Fatalf("sorted rows mismatch after sort/read %d: got %v, want %v", i+1, got, test.want)
						}
					}()
				}
			})
		}
	}
}

func generateBenchmarkBufferRows(n int) (*parquet.Schema, []parquet.Row) {
	model := new(benchmarkRowType)
	schema := parquet.SchemaOf(model)
	prng := rand.New(rand.NewSource(0))
	rows := make([]parquet.Row, n)

	for i := range rows {
		io.ReadFull(prng, model.ID[:])
		model.Value = prng.Float64()
		rows[i] = make(parquet.Row, 0, 2)
		rows[i] = schema.Deconstruct(rows[i], model)
	}

	return schema, rows
}

func BenchmarkBufferReadRows100x(b *testing.B) {
	schema, rows := generateBenchmarkBufferRows(benchmarkNumRows)
	buffer := parquet.NewBuffer(schema)

	for i := 0; i < len(rows); i += benchmarkRowsPerStep {
		j := i + benchmarkRowsPerStep
		if _, err := buffer.WriteRows(rows[i:j]); err != nil {
			b.Fatal(err)
		}
	}

	bufferRows := buffer.Rows()
	defer bufferRows.Close()

	benchmarkRowsPerSecond(b, func() int {
		n, err := bufferRows.ReadRows(rows[:benchmarkRowsPerStep])
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = bufferRows.SeekToRow(0)
			}
			if err != nil {
				b.Fatal(err)
			}
		}
		return n
	})
}

func BenchmarkBufferWriteRows100x(b *testing.B) {
	schema, rows := generateBenchmarkBufferRows(benchmarkNumRows)
	buffer := parquet.NewBuffer(schema)

	i := 0
	benchmarkRowsPerSecond(b, func() int {
		n, err := buffer.WriteRows(rows[i : i+benchmarkRowsPerStep])
		if err != nil {
			b.Fatal(err)
		}

		i += benchmarkRowsPerStep
		i %= benchmarkNumRows

		if i == 0 {
			buffer.Reset()
		}
		return n
	})
}

func TestBufferSortIgnoresInvalidColumn(t *testing.T) {
	type SortTestRow struct {
		FirstName string
		LastName  string
	}

	buffer := parquet.NewGenericBuffer[SortTestRow](
		parquet.SortingRowGroupConfig(
			parquet.SortingColumns(
				parquet.Ascending("LastName"),
				parquet.Ascending("NonExistent"),
				parquet.Ascending("FirstName"),
			),
		),
	)

	inputRows := []SortTestRow{
		{FirstName: "Han", LastName: "Solo"},
		{FirstName: "Luke", LastName: "Skywalker"},
		{FirstName: "Anakin", LastName: "Skywalker"},
		{FirstName: "Leia", LastName: "Organa"},
	}

	_, err := buffer.Write(inputRows)
	if err != nil {
		t.Fatalf("Failed to write rows: %v", err)
	}

	sort.Sort(buffer)

	reader := parquet.NewGenericRowGroupReader[SortTestRow](buffer)
	defer reader.Close()
	outputRows := make([]SortTestRow, len(inputRows))
	n, err := reader.Read(outputRows)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("Failed to read rows back: %v", err)
	}
	if n != len(inputRows) {
		t.Fatalf("Read incorrect number of rows: got %d, want %d", n, len(inputRows))
	}

	expectedRows := []SortTestRow{
		{FirstName: "Leia", LastName: "Organa"},
		{FirstName: "Anakin", LastName: "Skywalker"},
		{FirstName: "Luke", LastName: "Skywalker"},
		{FirstName: "Han", LastName: "Solo"},
	}

	if !reflect.DeepEqual(outputRows, expectedRows) {
		t.Errorf("Sorted rows mismatch:\nWant: %#v\nGot:  %#v", expectedRows, outputRows)
	}
}

func TestGenericBufferAppendAfterRead(t *testing.T) {
	type row struct {
		ID    int64
		Value string
	}
	buffer := parquet.NewGenericBuffer[row](parquet.SortingRowGroupConfig(
		parquet.SortingColumns(parquet.Ascending("ID")),
	))
	batches := [][]row{
		{{2, "initial long string"}, {0, "a"}},
		{{3, "appended null"}, {1, "appended low"}},
		{{5, ""}, {4, "last"}},
	}
	for range 2 {
		buffer.Reset()
		var want []row
		for _, batch := range batches {
			if _, err := buffer.Write(batch); err != nil {
				t.Fatal(err)
			}
			want = append(want, batch...)
			slices.SortFunc(want, func(a, b row) int { return int(a.ID - b.ID) })
			sort.Sort(buffer)
			reader := parquet.NewGenericRowGroupReader[row](buffer)
			got := make([]row, len(want))
			n, err := reader.Read(got)
			reader.Close()
			if err != nil && err != io.EOF {
				t.Fatal(err)
			}
			if n != len(want) || !reflect.DeepEqual(got, want) {
				t.Fatalf("rows mismatch: got %#v, want %#v", got[:n], want)
			}
		}
	}
}

func BenchmarkSortGenericBufferStrings(b *testing.B) {
	type row struct {
		ID    int64
		Value string
	}
	for _, test := range []struct {
		name       string
		emptyEvery int
	}{
		{"NoEmpty", 0},
		{"FewEmpty", 100},
		{"ManyEmpty", 2},
	} {
		b.Run(test.name, func(b *testing.B) {
			rows := make([]row, 10000)
			for i := range rows {
				rows[i] = row{ID: int64(i), Value: "abcdefghij"}
				if test.emptyEvery != 0 && i%test.emptyEvery == 0 {
					rows[i].Value = ""
				}
			}
			buffer := parquet.NewGenericBuffer[row](parquet.SortingRowGroupConfig(
				parquet.SortingColumns(parquet.Ascending("ID")),
			))
			defer buffer.Reset()
			if _, err := buffer.Write(rows); err != nil {
				b.Fatal(err)
			}
			prng := rand.New(rand.NewSource(0))
			for b.Loop() {
				for range 10 {
					buffer.Swap(prng.Intn(len(rows)), prng.Intn(len(rows)))
				}
				sort.Sort(buffer)
			}
		})
	}
}
