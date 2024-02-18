package parquet_test

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/deprecated"
	"github.com/parquet-go/parquet-go/internal/quick"
)

const (
	benchmarkNumRows     = 10_000
	benchmarkRowsPerStep = 1000
)

func ExampleReadFile() {
	type Row struct {
		ID   int64  `parquet:"id"`
		Name string `parquet:"name,zstd"`
	}

	ExampleWriteFile()

	rows, err := parquet.ReadFile[Row]("/tmp/file.parquet")
	if err != nil {
		log.Fatal(err)
	}

	for _, row := range rows {
		fmt.Printf("%d: %q\n", row.ID, row.Name)
	}

	// Output:
	// 0: "Bob"
	// 1: "Alice"
	// 2: "Franky"
}

func ExampleWriteFile() {
	type Row struct {
		ID   int64  `parquet:"id"`
		Name string `parquet:"name,zstd"`
	}

	if err := parquet.WriteFile("/tmp/file.parquet", []Row{
		{ID: 0, Name: "Bob"},
		{ID: 1, Name: "Alice"},
		{ID: 2, Name: "Franky"},
	}); err != nil {
		log.Fatal(err)
	}

	// Output:
}

func ExampleRead_any() {
	type Row struct{ FirstName, LastName string }

	buf := new(bytes.Buffer)
	err := parquet.Write(buf, []Row{
		{FirstName: "Luke", LastName: "Skywalker"},
		{FirstName: "Han", LastName: "Solo"},
		{FirstName: "R2", LastName: "D2"},
	})
	if err != nil {
		log.Fatal(err)
	}

	file := bytes.NewReader(buf.Bytes())

	rows, err := parquet.Read[any](file, file.Size())
	if err != nil {
		log.Fatal(err)
	}

	for _, row := range rows {
		fmt.Printf("%q\n", row)
	}

	// Output:
	// map["FirstName":"Luke" "LastName":"Skywalker"]
	// map["FirstName":"Han" "LastName":"Solo"]
	// map["FirstName":"R2" "LastName":"D2"]
}

func ExampleWrite_any() {
	schema := parquet.SchemaOf(struct {
		FirstName string
		LastName  string
	}{})

	buf := new(bytes.Buffer)
	err := parquet.Write[any](
		buf,
		[]any{
			map[string]string{"FirstName": "Luke", "LastName": "Skywalker"},
			map[string]string{"FirstName": "Han", "LastName": "Solo"},
			map[string]string{"FirstName": "R2", "LastName": "D2"},
		},
		schema,
	)
	if err != nil {
		log.Fatal(err)
	}

	file := bytes.NewReader(buf.Bytes())

	rows, err := parquet.Read[any](file, file.Size())
	if err != nil {
		log.Fatal(err)
	}

	for _, row := range rows {
		fmt.Printf("%q\n", row)
	}

	// Output:
	// map["FirstName":"Luke" "LastName":"Skywalker"]
	// map["FirstName":"Han" "LastName":"Solo"]
	// map["FirstName":"R2" "LastName":"D2"]
}

func ExampleSearch() {
	type Row struct{ FirstName, LastName string }

	buf := new(bytes.Buffer)
	// The column being searched should be sorted to avoid a full scan of the
	// column. See the section of the readme on sorting for how to sort on
	// insertion into the parquet file using parquet.SortingColumns
	rows := []Row{
		{FirstName: "C", LastName: "3PO"},
		{FirstName: "Han", LastName: "Solo"},
		{FirstName: "Leia", LastName: "Organa"},
		{FirstName: "Luke", LastName: "Skywalker"},
		{FirstName: "R2", LastName: "D2"},
	}
	// The tiny page buffer size ensures we get multiple pages out of the example above.
	w := parquet.NewGenericWriter[Row](buf, parquet.PageBufferSize(12), parquet.WriteBufferSize(0))
	// Need to write 1 row at a time here as writing many at once disregards PageBufferSize option.
	for _, row := range rows {
		_, err := w.Write([]Row{row})
		if err != nil {
			log.Fatal(err)
		}
	}
	err := w.Close()
	if err != nil {
		log.Fatal(err)
	}

	reader := bytes.NewReader(buf.Bytes())
	file, err := parquet.OpenFile(reader, reader.Size())
	if err != nil {
		log.Fatal(err)
	}

	// Search is scoped to a single RowGroup/ColumnChunk
	rowGroup := file.RowGroups()[0]
	firstNameColChunk := rowGroup.ColumnChunks()[0]

	columnIndex, err := firstNameColChunk.ColumnIndex()
	if err != nil {
		log.Fatal(err)
	}
	found := parquet.Search(columnIndex, parquet.ValueOf("Luke"), parquet.ByteArrayType)
	offsetIndex, _ := firstNameColChunk.OffsetIndex()
	fmt.Printf("numPages: %d\n", offsetIndex.NumPages())
	fmt.Printf("result found in page: %d\n", found)
	if found < offsetIndex.NumPages() {
		r := parquet.NewGenericReader[Row](file)
		defer r.Close()
		// Seek to the first row in the page the result was found
		r.SeekToRow(offsetIndex.FirstRowIndex(found))
		result := make([]Row, 2)
		_, _ = r.Read(result)
		// Leia is in index 0 for the page.
		for _, row := range result {
			if row.FirstName == "Luke" {
				fmt.Printf("%q\n", row)
			}
		}
	}

	// Output:
	// numPages: 3
	// result found in page: 1
	// {"Luke" "Skywalker"}
}

func TestIssue360(t *testing.T) {
	type TestType struct {
		Key []int
	}

	schema := parquet.SchemaOf(TestType{})
	buffer := parquet.NewGenericBuffer[any](schema)

	data := make([]any, 1)
	data[0] = TestType{Key: []int{1}}
	_, err := buffer.Write(data)
	if err != nil {
		fmt.Println("Exiting with error: ", err)
		return
	}

	var out bytes.Buffer
	writer := parquet.NewGenericWriter[any](&out, schema)

	_, err = parquet.CopyRows(writer, buffer.Rows())
	if err != nil {
		fmt.Println("Exiting with error: ", err)
		return
	}
	writer.Close()

	br := bytes.NewReader(out.Bytes())
	rows, _ := parquet.Read[any](br, br.Size())

	expect := []any{
		map[string]any{
			"Key": []any{
				int64(1),
			},
		},
	}

	assertRowsEqual(t, expect, rows)
}

func TestIssue362ParquetReadFromGenericReaders(t *testing.T) {
	path := "testdata/dms_test_table_LOAD00000001.parquet"
	fp, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer fp.Close()

	r1 := parquet.NewGenericReader[any](fp)
	rows1 := make([]any, r1.NumRows())
	_, err = r1.Read(rows1)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}

	r2 := parquet.NewGenericReader[any](fp)
	rows2 := make([]any, r2.NumRows())
	_, err = r2.Read(rows2)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
}

func TestIssue362ParquetReadFile(t *testing.T) {
	rows1, err := parquet.ReadFile[any]("testdata/dms_test_table_LOAD00000001.parquet")
	if err != nil {
		t.Fatal(err)
	}

	rows2, err := parquet.ReadFile[any]("testdata/dms_test_table_LOAD00000001.parquet")
	if err != nil {
		t.Fatal(err)
	}

	assertRowsEqual(t, rows1, rows2)
}

func TestIssue368(t *testing.T) {
	f, err := os.Open("testdata/issue368.parquet")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}

	pf, err := parquet.OpenFile(f, info.Size())
	if err != nil {
		t.Fatal(err)
	}

	reader := parquet.NewGenericReader[any](pf)
	defer reader.Close()

	trs := make([]any, 1)
	for {
		_, err := reader.Read(trs)
		if err != nil {
			break
		}
	}
}

func TestIssue377(t *testing.T) {
	type People struct {
		Name string
		Age  int
	}

	type Nested struct {
		P  []People
		F  string
		GF string
	}
	row1 := Nested{P: []People{
		{
			Name: "Bob",
			Age:  10,
		}}}
	ods := []Nested{
		row1,
	}
	buf := new(bytes.Buffer)
	w := parquet.NewGenericWriter[Nested](buf)
	_, err := w.Write(ods)
	if err != nil {
		t.Fatal("write error: ", err)
	}
	w.Close()

	file := bytes.NewReader(buf.Bytes())
	rows, err := parquet.Read[Nested](file, file.Size())
	if err != nil {
		t.Fatal("read error: ", err)
	}

	assertRowsEqual(t, rows, ods)
}

func TestIssue423(t *testing.T) {
	type Inner struct {
		Value string `parquet:","`
	}
	type Outer struct {
		Label string  `parquet:","`
		Inner Inner   `parquet:",json"`
		Slice []Inner `parquet:",json"`
		// This is the only tricky situation. Because we're delegating to json Marshaler/Unmarshaler
		// We use the json tags for optionality.
		Ptr *Inner `json:",omitempty" parquet:",json"`

		// This tests BC behavior that slices of bytes and json strings still get written/read in a BC way.
		String        string                     `parquet:",json"`
		Bytes         []byte                     `parquet:",json"`
		MapOfStructPb map[string]*structpb.Value `parquet:",json"`
		StructPB      *structpb.Value            `parquet:",json"`
	}

	writeRows := []Outer{
		{
			Label: "welp",
			Inner: Inner{
				Value: "this is a string",
			},
			Slice: []Inner{
				{
					Value: "in a slice",
				},
			},
			Ptr:    nil,
			String: `{"hello":"world"}`,
			Bytes:  []byte(`{"goodbye":"world"}`),
			MapOfStructPb: map[string]*structpb.Value{
				"answer": structpb.NewNumberValue(42.00),
			},
			StructPB: structpb.NewBoolValue(true),
		},
		{
			Label: "foxes",
			Inner: Inner{
				Value: "the quick brown fox jumped over the yellow lazy dog.",
			},
			Slice: []Inner{
				{
					Value: "in a slice",
				},
			},
			Ptr: &Inner{
				Value: "not nil",
			},
			String: `{"hello":"world"}`,
			Bytes:  []byte(`{"goodbye":"world"}`),
			MapOfStructPb: map[string]*structpb.Value{
				"doubleAnswer": structpb.NewNumberValue(84.00),
			},
			StructPB: structpb.NewBoolValue(false),
		},
	}

	schema := parquet.SchemaOf(new(Outer))
	fmt.Println(schema.String())
	buf := new(bytes.Buffer)
	w := parquet.NewGenericWriter[Outer](buf, schema)
	_, err := w.Write(writeRows)
	if err != nil {
		t.Fatal("write error: ", err)
	}
	w.Close()

	file := bytes.NewReader(buf.Bytes())
	readRows, err := parquet.Read[Outer](file, file.Size())
	if err != nil {
		t.Fatal("read error: ", err)
	}

	assertRowsEqual(t, writeRows, readRows)
}

func TestReadFileGenericMultipleRowGroupsMultiplePages(t *testing.T) {
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
	for i := 0; i < numRows; i++ {
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

	rows, err := parquet.ReadFile[MyRow](path)
	if err != nil {
		t.Fatal("parquet.ReadFile: ", err)
	}

	if len(rows) != numRows {
		t.Fatalf("not enough values were read: want=%d got=%d", len(rows), numRows)
	}
	for i, row := range rows {
		id := [16]byte{15: byte(i)}
		file := "hi" + fmt.Sprint(i)
		index := int64(i)

		if row.ID != id || row.File != file || row.Index != index {
			t.Fatalf("rows mismatch at index: %d got: %+v", i, row)
		}
	}
}

func assertRowsEqual[T any](t *testing.T, rows1, rows2 []T) {
	if !reflect.DeepEqual(rows1, rows2) {
		t.Error("rows mismatch")

		t.Log("want:")
		logRows(t, rows1)

		t.Log("got:")
		logRows(t, rows2)
	}
}

func logRows[T any](t *testing.T, rows []T) {
	for _, row := range rows {
		t.Logf(". %#v\n", row)
	}
}

func TestNestedPointer(t *testing.T) {
	type InnerStruct struct {
		InnerField string
	}

	type SliceElement struct {
		Inner *InnerStruct
	}

	type Outer struct {
		Slice []*SliceElement
	}
	value := "inner-string"
	in := &Outer{
		Slice: []*SliceElement{
			{
				Inner: &InnerStruct{
					InnerField: value,
				},
			},
		},
	}

	var f bytes.Buffer

	pw := parquet.NewGenericWriter[*Outer](&f)
	_, err := pw.Write([]*Outer{in})
	if err != nil {
		t.Fatal(err)
	}

	err = pw.Close()
	if err != nil {
		t.Fatal(err)
	}

	pr := parquet.NewGenericReader[*Outer](bytes.NewReader(f.Bytes()))

	out := make([]*Outer, 1)
	_, err = pr.Read(out)
	if err != nil {
		t.Fatal(err)
	}
	pr.Close()
	if want, got := value, out[0].Slice[0].Inner.InnerField; want != got {
		t.Error("failed to set inner field pointer")
	}
}

type benchmarkRowType struct {
	ID    [16]byte `parquet:"id,uuid"`
	Value float64  `parquet:"value"`
}

func (row benchmarkRowType) generate(prng *rand.Rand) benchmarkRowType {
	prng.Read(row.ID[:])
	row.Value = prng.Float64()
	return row
}

type paddedBooleanColumn struct {
	Value bool
	_     [3]byte
}

func (row paddedBooleanColumn) generate(prng *rand.Rand) paddedBooleanColumn {
	return paddedBooleanColumn{Value: prng.Int()%2 == 0}
}

type booleanColumn struct {
	Value bool
}

func (row booleanColumn) generate(prng *rand.Rand) booleanColumn {
	return booleanColumn{Value: prng.Int()%2 == 0}
}

type int32Column struct {
	Value int32 `parquet:",delta"`
}

func (row int32Column) generate(prng *rand.Rand) int32Column {
	return int32Column{Value: prng.Int31n(100)}
}

type int64Column struct {
	Value int64 `parquet:",delta"`
}

func (row int64Column) generate(prng *rand.Rand) int64Column {
	return int64Column{Value: prng.Int63n(100)}
}

type int96Column struct {
	Value deprecated.Int96
}

func (row int96Column) generate(prng *rand.Rand) int96Column {
	row.Value[0] = prng.Uint32()
	row.Value[1] = prng.Uint32()
	row.Value[2] = prng.Uint32()
	return row
}

type floatColumn struct {
	Value float32
}

func (row floatColumn) generate(prng *rand.Rand) floatColumn {
	return floatColumn{Value: prng.Float32()}
}

type doubleColumn struct {
	Value float64
}

func (row doubleColumn) generate(prng *rand.Rand) doubleColumn {
	return doubleColumn{Value: prng.Float64()}
}

type byteArrayColumn struct {
	Value []byte
}

func (row byteArrayColumn) generate(prng *rand.Rand) byteArrayColumn {
	row.Value = make([]byte, prng.Intn(10))
	prng.Read(row.Value)
	return row
}

type fixedLenByteArrayColumn struct {
	Value [10]byte
}

func (row fixedLenByteArrayColumn) generate(prng *rand.Rand) fixedLenByteArrayColumn {
	prng.Read(row.Value[:])
	return row
}

type stringColumn struct {
	Value string
}

func (row stringColumn) generate(prng *rand.Rand) stringColumn {
	return stringColumn{Value: generateString(prng, 10)}
}

type indexedStringColumn struct {
	Value string `parquet:",dict"`
}

func (row indexedStringColumn) generate(prng *rand.Rand) indexedStringColumn {
	return indexedStringColumn{Value: generateString(prng, 10)}
}

type uuidColumn struct {
	Value uuid.UUID `parquet:",delta"`
}

func (row uuidColumn) generate(prng *rand.Rand) uuidColumn {
	prng.Read(row.Value[:])
	return row
}

type timeColumn struct {
	Value time.Time
}

func (row timeColumn) generate(prng *rand.Rand) timeColumn {
	t := time.Unix(0, prng.Int63()).UTC()
	return timeColumn{Value: t}
}

type timeInMillisColumn struct {
	Value time.Time `parquet:",timestamp(millisecond)"`
}

func (row timeInMillisColumn) generate(prng *rand.Rand) timeInMillisColumn {
	t := time.Unix(0, prng.Int63()).UTC()
	return timeInMillisColumn{Value: t}
}

type decimalColumn struct {
	Value int64 `parquet:",decimal(0:3)"`
}

func (row decimalColumn) generate(prng *rand.Rand) decimalColumn {
	return decimalColumn{Value: prng.Int63()}
}

type mapColumn struct {
	Value map[utf8string]int
}

func (row mapColumn) generate(prng *rand.Rand) mapColumn {
	n := prng.Intn(10)
	row.Value = make(map[utf8string]int, n)
	for i := 0; i < n; i++ {
		row.Value[utf8string(generateString(prng, 8))] = prng.Intn(100)
	}
	return row
}

type addressBook struct {
	Owner             utf8string   `parquet:",plain"`
	OwnerPhoneNumbers []utf8string `parquet:",plain"`
	Contacts          []contact
}

type contact struct {
	Name        utf8string `parquet:",plain"`
	PhoneNumber utf8string `parquet:",plain"`
}

func (row contact) generate(prng *rand.Rand) contact {
	return contact{
		Name:        utf8string(generateString(prng, 16)),
		PhoneNumber: utf8string(generateString(prng, 10)),
	}
}

type optionalInt32Column struct {
	Value int32 `parquet:",optional"`
}

func (row optionalInt32Column) generate(prng *rand.Rand) optionalInt32Column {
	return optionalInt32Column{Value: prng.Int31n(100)}
}

type repeatedInt32Column struct {
	Values []int32
}

func (row repeatedInt32Column) generate(prng *rand.Rand) repeatedInt32Column {
	row.Values = make([]int32, prng.Intn(10))
	for i := range row.Values {
		row.Values[i] = prng.Int31n(10)
	}
	return row
}

type listColumn2 struct {
	Value utf8string `parquet:",optional"`
}

type listColumn1 struct {
	List2 []listColumn2 `parquet:",list"`
}

type listColumn0 struct {
	List1 []listColumn1 `parquet:",list"`
}

type nestedListColumn1 struct {
	Level3 []utf8string `parquet:"level3"`
}

type nestedListColumn struct {
	Level1 []nestedListColumn1 `parquet:"level1"`
	Level2 []utf8string        `parquet:"level2"`
}

type utf8string string

func (utf8string) Generate(rand *rand.Rand, size int) reflect.Value {
	const characters = "abcdefghijklmnopqrstuvwxyz1234567890"
	const maxSize = 10
	if size > maxSize {
		size = maxSize
	}
	n := rand.Intn(size)
	b := make([]byte, n)
	for i := range b {
		b[i] = characters[rand.Intn(len(characters))]
	}
	return reflect.ValueOf(utf8string(b))
}

type Contact struct {
	Name        string `parquet:"name"`
	PhoneNumber string `parquet:"phoneNumber,optional,zstd"`
}

type AddressBook struct {
	Owner             string    `parquet:"owner,zstd"`
	OwnerPhoneNumbers []string  `parquet:"ownerPhoneNumbers,gzip"`
	Contacts          []Contact `parquet:"contacts"`
}

func forEachLeafColumn(col *parquet.Column, do func(*parquet.Column) error) error {
	children := col.Columns()

	if len(children) == 0 {
		return do(col)
	}

	for _, child := range children {
		if err := forEachLeafColumn(child, do); err != nil {
			return err
		}
	}

	return nil
}

func forEachPage(pages parquet.PageReader, do func(parquet.Page) error) error {
	doAndReleasePage := func(page parquet.Page) error {
		defer parquet.Release(page)
		return do(page)
	}

	for {
		p, err := pages.ReadPage()
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return err
		}
		if err := doAndReleasePage(p); err != nil {
			return err
		}
	}
}

func forEachValue(values parquet.ValueReader, do func(parquet.Value) error) error {
	buffer := [3]parquet.Value{}
	for {
		n, err := values.ReadValues(buffer[:])
		for _, v := range buffer[:n] {
			if err := do(v); err != nil {
				return err
			}
		}
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return err
		}
	}
}

func forEachColumnPage(col *parquet.Column, do func(*parquet.Column, parquet.Page) error) error {
	return forEachLeafColumn(col, func(leaf *parquet.Column) error {
		pages := leaf.Pages()
		defer pages.Close()
		return forEachPage(pages, func(page parquet.Page) error { return do(leaf, page) })
	})
}

func forEachColumnValue(col *parquet.Column, do func(*parquet.Column, parquet.Value) error) error {
	return forEachColumnPage(col, func(leaf *parquet.Column, page parquet.Page) error {
		return forEachValue(page.Values(), func(value parquet.Value) error { return do(leaf, value) })
	})
}

func forEachColumnChunk(file *parquet.File, do func(*parquet.Column, parquet.ColumnChunk) error) error {
	return forEachLeafColumn(file.Root(), func(leaf *parquet.Column) error {
		for _, rowGroup := range file.RowGroups() {
			if err := do(leaf, rowGroup.ColumnChunks()[leaf.Index()]); err != nil {
				return err
			}
		}
		return nil
	})
}

func createParquetFile(rows rows, options ...parquet.WriterOption) (*parquet.File, error) {
	buffer := new(bytes.Buffer)

	if err := writeParquetFile(buffer, rows, options...); err != nil {
		return nil, err
	}

	reader := bytes.NewReader(buffer.Bytes())
	return parquet.OpenFile(reader, reader.Size())
}

func writeParquetFile(w io.Writer, rows rows, options ...parquet.WriterOption) error {
	writer := parquet.NewWriter(w, options...)

	for _, row := range rows {
		if err := writer.Write(row); err != nil {
			return err
		}
	}

	return writer.Close()
}

func writeParquetFileWithBuffer(w io.Writer, rows rows, options ...parquet.WriterOption) error {
	buffer := parquet.NewBuffer()
	for _, row := range rows {
		if err := buffer.Write(row); err != nil {
			return err
		}
	}

	writer := parquet.NewWriter(w, options...)
	numRows, err := copyRowsAndClose(writer, buffer.Rows())
	if err != nil {
		return err
	}
	if numRows != int64(len(rows)) {
		return fmt.Errorf("wrong number of rows written from buffer to file: want=%d got=%d", len(rows), numRows)
	}
	return writer.Close()
}

type rows []interface{}

func makeRows(any interface{}) rows {
	if v, ok := any.([]interface{}); ok {
		return rows(v)
	}
	value := reflect.ValueOf(any)
	slice := make([]interface{}, value.Len())
	for i := range slice {
		slice[i] = value.Index(i).Interface()
	}
	return rows(slice)
}

func randValueFuncOf(t parquet.Type) func(*rand.Rand) parquet.Value {
	switch k := t.Kind(); k {
	case parquet.Boolean:
		return func(r *rand.Rand) parquet.Value {
			return parquet.ValueOf(r.Float64() < 0.5)
		}

	case parquet.Int32:
		return func(r *rand.Rand) parquet.Value {
			return parquet.ValueOf(r.Int31())
		}

	case parquet.Int64:
		return func(r *rand.Rand) parquet.Value {
			return parquet.ValueOf(r.Int63())
		}

	case parquet.Int96:
		return func(r *rand.Rand) parquet.Value {
			return parquet.ValueOf(deprecated.Int96{
				0: r.Uint32(),
				1: r.Uint32(),
				2: r.Uint32(),
			})
		}

	case parquet.Float:
		return func(r *rand.Rand) parquet.Value {
			return parquet.ValueOf(r.Float32())
		}

	case parquet.Double:
		return func(r *rand.Rand) parquet.Value {
			return parquet.ValueOf(r.Float64())
		}

	case parquet.ByteArray:
		return func(r *rand.Rand) parquet.Value {
			n := r.Intn(49) + 1
			b := make([]byte, n)
			const characters = "1234567890qwertyuiopasdfghjklzxcvbnm "
			for i := range b {
				b[i] = characters[r.Intn(len(characters))]
			}
			return parquet.ValueOf(b)
		}

	case parquet.FixedLenByteArray:
		arrayType := reflect.ArrayOf(t.Length(), reflect.TypeOf(byte(0)))
		return func(r *rand.Rand) parquet.Value {
			b := make([]byte, arrayType.Len())
			r.Read(b)
			v := reflect.New(arrayType).Elem()
			reflect.Copy(v, reflect.ValueOf(b))
			return parquet.ValueOf(v.Interface())
		}

	default:
		panic("NOT IMPLEMENTED")
	}
}

func copyRowsAndClose(w parquet.RowWriter, r parquet.Rows) (int64, error) {
	defer r.Close()
	return parquet.CopyRows(w, r)
}

func benchmarkRowsPerSecond(b *testing.B, f func() int) {
	b.ResetTimer()
	start := time.Now()
	numRows := int64(0)

	for i := 0; i < b.N; i++ {
		n := f()
		numRows += int64(n)
	}

	seconds := time.Since(start).Seconds()
	b.ReportMetric(float64(numRows)/seconds, "row/s")
}

func generateString(r *rand.Rand, n int) string {
	const characters = "1234567890qwertyuiopasdfghjklzxcvbnm"
	b := new(strings.Builder)
	for i := 0; i < n; i++ {
		b.WriteByte(characters[r.Intn(len(characters))])
	}
	return b.String()
}

var quickCheckConfig = quick.Config{
	Sizes: []int{
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
		10, 20, 30, 40, 50, 123,
		4096 + 1,
	},
}

func quickCheck(f interface{}) error {
	return quickCheckConfig.Check(f)
}

func TestParquetAnyValueConversions(t *testing.T) {
	// This test runs conversions to/from any values with edge case schemas.

	type obj = map[string]any
	type arr = []any

	for _, test := range []struct {
		name           string
		input          any
		explicitOutput any // Only set if we expect a difference from the input
		schema         parquet.Group
	}{
		{
			name: "simple strings",
			input: obj{
				"A": "foo",
				"B": "bar",
				"C": "baz",
			},
			schema: parquet.Group{
				"A": parquet.String(),
				"B": parquet.String(),
				"C": parquet.String(),
			},
		},
		{
			name: "simple strings with nil",
			input: obj{
				"A": "foo",
				"B": (*string)(nil),
				"C": nil,
			},
			explicitOutput: obj{
				"A": "foo",
				"B": "",
				"C": "",
			},
			schema: parquet.Group{
				"A": parquet.String(),
				"B": parquet.String(),
				"C": parquet.String(),
			},
		},
		{
			name: "simple groups with nil",
			input: obj{
				"A": obj{
					"AA": "foo",
				},
				"B": nil,
				"C": (*obj)(nil),
				"D": obj{
					"DA": "bar",
				},
			},
			explicitOutput: obj{
				"A": obj{
					"AA": "foo",
				},
				"B": obj{
					"BA": "",
				},
				"C": obj{
					"CA": "",
				},
				"D": obj{
					"DA": "bar",
				},
			},
			schema: parquet.Group{
				"A": parquet.Group{
					"AA": parquet.String(),
				},
				"B": parquet.Group{
					"BA": parquet.String(),
				},
				"C": parquet.Group{
					"CA": parquet.String(),
				},
				"D": parquet.Group{
					"DA": parquet.String(),
				},
			},
		},
		{
			name: "simple values",
			input: obj{
				"A": "foo",
				"B": int64(5),
				"C": 0.5,
			},
			schema: parquet.Group{
				"A": parquet.String(),
				"B": parquet.Int(64),
				"C": parquet.Leaf(parquet.DoubleType),
			},
		},
		{
			name: "repeated values",
			input: obj{
				"A": arr{"foo", "bar", "baz"},
				"B": arr{int64(5), int64(6)},
				"C": arr{0.5},
			},
			schema: parquet.Group{
				"A": parquet.Repeated(parquet.String()),
				"B": parquet.Repeated(parquet.Int(64)),
				"C": parquet.Repeated(parquet.Leaf(parquet.DoubleType)),
			},
		},
		{
			name: "nested groups",
			input: obj{
				"A": obj{
					"B": obj{
						"C": "here we are",
					},
				},
			},
			schema: parquet.Group{
				"A": parquet.Group{
					"B": parquet.Group{
						"C": parquet.String(),
					},
				},
			},
		},
		{
			name: "nested repeated groups",
			input: obj{
				"A": arr{
					obj{
						"B": arr{
							obj{"C": arr{"first", "second"}},
							obj{"C": arr{"third", "fourth"}},
							obj{"C": arr{"fifth"}},
						},
					},
					obj{
						"B": arr{
							obj{"C": arr{"sixth"}},
						},
					},
				},
			},
			schema: parquet.Group{
				"A": parquet.Repeated(parquet.Group{
					"B": parquet.Repeated(parquet.Group{
						"C": parquet.Repeated(parquet.String()),
					}),
				}),
			},
		},
		{
			name: "optional values",
			input: obj{
				"A": "foo",
				"B": nil,
				"C": "baz",
			},
			schema: parquet.Group{
				"A": parquet.Optional(parquet.String()),
				"B": parquet.Optional(parquet.String()),
				"C": parquet.Optional(parquet.String()),
			},
		},
		{
			name: "nested optional groups",
			input: obj{
				"A": obj{
					"B": obj{
						"C": "here we are",
					},
					"D": nil,
				},
			},
			schema: parquet.Group{
				"A": parquet.Group{
					"B": parquet.Optional(parquet.Group{
						"C": parquet.String(),
					}),
					"D": parquet.Optional(parquet.Group{
						"E": parquet.String(),
					}),
				},
			},
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			var buf bytes.Buffer
			pWtr := parquet.NewGenericWriter[any](&buf, parquet.NewSchema("", test.schema))
			if _, err := pWtr.Write([]any{test.input}); err != nil {
				t.Fatal(err)
			}
			if err := pWtr.Close(); err != nil {
				t.Fatal(err)
			}

			pRdr := parquet.NewGenericReader[any](bytes.NewReader(buf.Bytes()))
			outRows := make([]any, 1)
			if _, err := pRdr.Read(outRows); err != nil {
				t.Fatal(err)
			}
			if err := pRdr.Close(); err != nil {
				t.Fatal(err)
			}

			expected := test.input
			if test.explicitOutput != nil {
				expected = test.explicitOutput
			}

			if value1, value2 := expected, outRows[0]; !reflect.DeepEqual(value1, value2) {
				t.Errorf("value mismatch: want=%+v got=%+v", value1, value2)
			}
		})
	}
}
