package parquet_test

import (
	"bytes"
	"io"
	"reflect"
	"testing"

	"github.com/google/uuid"
	"github.com/parquet-go/bitpack/unsafecast"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/deprecated"
	"github.com/parquet-go/parquet-go/encoding/plain"
)

func TestPage(t *testing.T) {
	t.Run("BOOLEAN", testPageBoolean)
	t.Run("INT32", testPageInt32)
	t.Run("INT64", testPageInt64)
	t.Run("INT96", testPageInt96)
	t.Run("FLOAT", testPageFloat)
	t.Run("DOUBLE", testPageDouble)
	t.Run("BYTE_ARRAY", testPageByteArray)
	t.Run("FIXED_LEN_BYTE_ARRAY", testPageFixedLenByteArray)
	t.Run("UUID", testPageUUID)
}

func testPageBoolean(t *testing.T) {
	schema := parquet.SchemaOf(struct{ Value bool }{})

	t.Run("parquet", func(t *testing.T) {
		testPage(t, schema, pageTest{
			write: func(w parquet.ValueWriter) (any, error) {
				values := make([]bool, 50_000)
				for i := range values {
					values[i] = i%2 == 0
				}
				n, err := w.(parquet.BooleanWriter).WriteBooleans(values)
				return values[:n], err
			},

			read: func(r parquet.ValueReader) (any, error) {
				values := make([]bool, 50_000)
				n, err := r.(parquet.BooleanReader).ReadBooleans(values)
				return values[:n], err
			},
		})
	})
}

func testPageInt32(t *testing.T) {
	schema := parquet.SchemaOf(struct{ Value int32 }{})

	t.Run("io", func(t *testing.T) {
		testBufferPage(t, schema, pageTest{
			write: func(w parquet.ValueWriter) (any, error) {
				values := []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
				n, err := w.(io.Writer).Write(unsafecast.Slice[byte](values))
				return values[:n/4], err
			},

			read: func(r parquet.ValueReader) (any, error) {
				values := make([]int32, 10)
				n, err := r.(io.Reader).Read(unsafecast.Slice[byte](values))
				return values[:n/4], err
			},
		})
	})

	t.Run("parquet", func(t *testing.T) {
		testPage(t, schema, pageTest{
			write: func(w parquet.ValueWriter) (any, error) {
				values := []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
				n, err := w.(parquet.Int32Writer).WriteInt32s(values)
				return values[:n], err
			},

			read: func(r parquet.ValueReader) (any, error) {
				values := make([]int32, 10)
				n, err := r.(parquet.Int32Reader).ReadInt32s(values)
				return values[:n], err
			},
		})
	})
}

func testPageInt64(t *testing.T) {
	schema := parquet.SchemaOf(struct{ Value int64 }{})

	t.Run("io", func(t *testing.T) {
		testBufferPage(t, schema, pageTest{
			write: func(w parquet.ValueWriter) (any, error) {
				values := []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
				n, err := w.(io.Writer).Write(unsafecast.Slice[byte](values))
				return values[:n/8], err
			},

			read: func(r parquet.ValueReader) (any, error) {
				values := make([]int64, 10)
				n, err := r.(io.Reader).Read(unsafecast.Slice[byte](values))
				return values[:n/8], err
			},
		})
	})

	t.Run("parquet", func(t *testing.T) {
		testPage(t, schema, pageTest{
			write: func(w parquet.ValueWriter) (any, error) {
				values := []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
				n, err := w.(parquet.Int64Writer).WriteInt64s(values)
				return values[:n], err
			},

			read: func(r parquet.ValueReader) (any, error) {
				values := make([]int64, 10)
				n, err := r.(parquet.Int64Reader).ReadInt64s(values)
				return values[:n], err
			},
		})
	})
}

func testPageInt96(t *testing.T) {
	schema := parquet.SchemaOf(struct{ Value deprecated.Int96 }{})

	t.Run("io", func(t *testing.T) {
		testBufferPage(t, schema, pageTest{
			write: func(w parquet.ValueWriter) (any, error) {
				values := []deprecated.Int96{{0: 0}, {0: 1}, {0: 2}}
				n, err := w.(io.Writer).Write(unsafecast.Slice[byte](values))
				return values[:n/12], err
			},

			read: func(r parquet.ValueReader) (any, error) {
				values := make([]deprecated.Int96, 3)
				n, err := r.(io.Reader).Read(unsafecast.Slice[byte](values))
				return values[:n/12], err
			},
		})
	})

	t.Run("parquet", func(t *testing.T) {
		testPage(t, schema, pageTest{
			write: func(w parquet.ValueWriter) (any, error) {
				values := []deprecated.Int96{{0: 0}, {0: 1}, {0: 2}}
				n, err := w.(parquet.Int96Writer).WriteInt96s(values)
				return values[:n], err
			},

			read: func(r parquet.ValueReader) (any, error) {
				values := make([]deprecated.Int96, 3)
				n, err := r.(parquet.Int96Reader).ReadInt96s(values)
				return values[:n], err
			},
		})
	})
}

func testPageFloat(t *testing.T) {
	schema := parquet.SchemaOf(struct{ Value float32 }{})

	t.Run("io", func(t *testing.T) {
		testBufferPage(t, schema, pageTest{
			write: func(w parquet.ValueWriter) (any, error) {
				values := []float32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
				n, err := w.(io.Writer).Write(unsafecast.Slice[byte](values))
				return values[:n/4], err
			},

			read: func(r parquet.ValueReader) (any, error) {
				values := make([]float32, 10)
				n, err := r.(io.Reader).Read(unsafecast.Slice[byte](values))
				return values[:n/4], err
			},
		})
	})

	t.Run("parquet", func(t *testing.T) {
		testPage(t, schema, pageTest{
			write: func(w parquet.ValueWriter) (any, error) {
				values := []float32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
				n, err := w.(parquet.FloatWriter).WriteFloats(values)
				return values[:n], err
			},

			read: func(r parquet.ValueReader) (any, error) {
				values := make([]float32, 10)
				n, err := r.(parquet.FloatReader).ReadFloats(values)
				return values[:n], err
			},
		})
	})
}

func testPageDouble(t *testing.T) {
	schema := parquet.SchemaOf(struct{ Value float64 }{})

	t.Run("io", func(t *testing.T) {
		testBufferPage(t, schema, pageTest{
			write: func(w parquet.ValueWriter) (any, error) {
				values := []float64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
				n, err := w.(io.Writer).Write(unsafecast.Slice[byte](values))
				return values[:n/8], err
			},

			read: func(r parquet.ValueReader) (any, error) {
				values := make([]float64, 10)
				n, err := r.(io.Reader).Read(unsafecast.Slice[byte](values))
				return values[:n/8], err
			},
		})
	})

	t.Run("parquet", func(t *testing.T) {
		testPage(t, schema, pageTest{
			write: func(w parquet.ValueWriter) (any, error) {
				values := []float64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
				n, err := w.(parquet.DoubleWriter).WriteDoubles(values)
				return values[:n], err
			},

			read: func(r parquet.ValueReader) (any, error) {
				values := make([]float64, 10)
				n, err := r.(parquet.DoubleReader).ReadDoubles(values)
				return values[:n], err
			},
		})
	})
}

func testPageByteArray(t *testing.T) {
	schema := parquet.SchemaOf(struct{ Value []byte }{})

	t.Run("io", func(t *testing.T) {
		testBufferPage(t, schema, pageTest{
			write: func(w parquet.ValueWriter) (any, error) {
				values := []byte{}
				values = plain.AppendByteArray(values, []byte("A"))
				values = plain.AppendByteArray(values, []byte("B"))
				values = plain.AppendByteArray(values, []byte("C"))
				n, err := w.(io.Writer).Write(values)
				return values[:n], err
			},

			read: func(r parquet.ValueReader) (any, error) {
				values := make([]byte, 3+3*plain.ByteArrayLengthSize)
				n, err := r.(io.Reader).Read(values)
				return values[:n], err
			},
		})
	})

	t.Run("parquet", func(t *testing.T) {
		testPage(t, schema, pageTest{
			write: func(w parquet.ValueWriter) (any, error) {
				values := []byte{}
				values = plain.AppendByteArray(values, []byte("A"))
				values = plain.AppendByteArray(values, []byte("B"))
				values = plain.AppendByteArray(values, []byte("C"))
				_, err := w.(parquet.ByteArrayWriter).WriteByteArrays(values)
				return values, err
			},

			read: func(r parquet.ValueReader) (any, error) {
				values := make([]byte, 3+3*plain.ByteArrayLengthSize)
				n, err := r.(parquet.ByteArrayReader).ReadByteArrays(values)
				return values[:n+n*plain.ByteArrayLengthSize], err
			},
		})
	})
}

func testPageFixedLenByteArray(t *testing.T) {
	schema := parquet.SchemaOf(struct{ Value [3]byte }{})

	t.Run("io", func(t *testing.T) {
		testBufferPage(t, schema, pageTest{
			write: func(w parquet.ValueWriter) (any, error) {
				values := []byte("123456789")
				n, err := w.(io.Writer).Write(values)
				return values[:n], err
			},

			read: func(r parquet.ValueReader) (any, error) {
				values := make([]byte, 3*3)
				n, err := r.(io.Reader).Read(values)
				return values[:n], err
			},
		})
	})

	t.Run("parquet", func(t *testing.T) {
		testPage(t, schema, pageTest{
			write: func(w parquet.ValueWriter) (any, error) {
				values := []byte("123456789")
				n, err := w.(parquet.FixedLenByteArrayWriter).WriteFixedLenByteArrays(values)
				return values[:3*n], err
			},

			read: func(r parquet.ValueReader) (any, error) {
				values := make([]byte, 3*3)
				n, err := r.(parquet.FixedLenByteArrayReader).ReadFixedLenByteArrays(values)
				return values[:3*n], err
			},
		})
	})
}

func testPageUUID(t *testing.T) {
	schema := parquet.NewSchema("test", parquet.Group{
		"Value": parquet.UUID(),
	})

	t.Run("be128", func(t *testing.T) {
		testPage(t, schema, pageTest{
			write: func(w parquet.ValueWriter) (any, error) {
				values := [][16]byte{
					{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F},
					{0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F},
					{0xFF, 0xEE, 0xDD, 0xCC, 0xBB, 0xAA, 0x99, 0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11, 0x00},
				}
				n, err := w.(parquet.BE128Writer).WriteBE128s(values)
				return values[:n], err
			},

			read: func(r parquet.ValueReader) (any, error) {
				values := make([][16]byte, 3)
				n, err := r.(parquet.BE128Reader).ReadBE128s(values)
				return values[:n], err
			},
		})
	})

	t.Run("uuid", func(t *testing.T) {
		testPage(t, schema, pageTest{
			write: func(w parquet.ValueWriter) (any, error) {
				values := []uuid.UUID{
					uuid.MustParse("00010203-0405-0607-0809-0a0b0c0d0e0f"),
					uuid.MustParse("10111213-1415-1617-1819-1a1b1c1d1e1f"),
					uuid.MustParse("ffeeddcc-bbaa-9988-7766-554433221100"),
				}
				n, err := w.(parquet.UUIDWriter).WriteUUIDs(values)
				return values[:n], err
			},

			read: func(r parquet.ValueReader) (any, error) {
				values := make([]uuid.UUID, 3)
				n, err := r.(parquet.UUIDReader).ReadUUIDs(values)
				return values[:n], err
			},
		})
	})
}

type pageTest struct {
	write func(parquet.ValueWriter) (any, error)
	read  func(parquet.ValueReader) (any, error)
}

func testPage(t *testing.T, schema *parquet.Schema, test pageTest) {
	t.Run("buffer", func(t *testing.T) { testBufferPage(t, schema, test) })
	t.Run("file", func(t *testing.T) { testFilePage(t, schema, test) })
}

func testBufferPage(t *testing.T, schema *parquet.Schema, test pageTest) {
	buffer := parquet.NewBuffer(schema)
	column := buffer.ColumnBuffers()[0]

	w, err := test.write(column)
	if err != nil {
		t.Fatal("writing page values:", err)
	}

	r, err := test.read(column.Page().Values())
	if err != io.EOF {
		t.Errorf("expected io.EOF after reading all values but got %v", err)
	}
	if !reflect.DeepEqual(w, r) {
		t.Errorf("wrong values read from the page: got=%+v want=%+v", r, w)
	}
}

func testFilePage(t *testing.T, schema *parquet.Schema, test pageTest) {
	buffer := parquet.NewBuffer(schema)
	column := buffer.ColumnBuffers()[0]

	w, err := test.write(column)
	if err != nil {
		t.Fatal("writing page values:", err)
	}

	output := new(bytes.Buffer)
	writer := parquet.NewWriter(output)
	n, err := writer.WriteRowGroup(buffer)
	if err != nil {
		t.Fatal("writing parquet file:", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal("writing parquet file:", err)
	}
	if n != buffer.NumRows() {
		t.Fatalf("number of rows written mismatch: got=%d want=%d", n, buffer.NumRows())
	}

	reader := bytes.NewReader(output.Bytes())
	f, err := parquet.OpenFile(reader, reader.Size())
	if err != nil {
		t.Fatal("opening parquet file:", err)
	}

	pages := f.RowGroups()[0].ColumnChunks()[0].Pages()
	defer pages.Close()

	p, err := pages.ReadPage()
	if err != nil {
		t.Fatal("reading parquet page:", err)
	}
	defer parquet.Release(p)

	values := p.Values()
	r, err := test.read(values)
	if err != io.EOF && err != nil {
		t.Errorf("expected io.EOF after reading all values but got %v", err)
	}
	if !reflect.DeepEqual(w, r) {
		t.Errorf("wrong values read from the page: got=%+v want=%+v", r, w)
	}
	if r, err := test.read(values); reflect.ValueOf(r).Len() != 0 || err != io.EOF {
		t.Errorf("expected no data and io.EOF after reading all values but got %d and %v", r, err)
	}
}

type testStruct struct {
	Value *string
}

func TestOptionalPageTrailingNulls(t *testing.T) {
	schema := parquet.SchemaOf(&testStruct{})
	buffer := parquet.NewBuffer(schema)

	str := "test"
	rows := []testStruct{{
		Value: nil,
	}, {
		Value: &str,
	}, {
		Value: nil,
	}}

	for _, row := range rows {
		_, err := buffer.WriteRows([]parquet.Row{schema.Deconstruct(nil, row)})
		if err != nil {
			t.Fatal("writing row:", err)
		}
	}

	resultRows := make([]parquet.Row, 0, len(rows))
	bufferRows := make([]parquet.Row, 10)
	reader := buffer.Rows()
	defer reader.Close()
	for {
		n, err := reader.ReadRows(bufferRows)
		resultRows = append(resultRows, bufferRows[:n]...)
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal("reading rows:", err)
		}
	}

	if len(resultRows) != len(rows) {
		t.Errorf("wrong number of rows read: got=%d want=%d", len(resultRows), len(rows))
	}
}

func TestOptionalPagePreserveIndex(t *testing.T) {
	schema := parquet.SchemaOf(&testStruct{})
	buffer := parquet.NewBuffer(schema)

	_, err := buffer.WriteRows([]parquet.Row{
		schema.Deconstruct(nil, &testStruct{Value: nil}),
	})
	if err != nil {
		t.Fatal("writing row:", err)
	}

	rows := buffer.Rows()
	defer rows.Close()

	rowbuf := make([]parquet.Row, 2)

	n, err := rows.ReadRows(rowbuf)
	if err != nil && err != io.EOF {
		t.Fatal("reading rows:", err)
	}
	if n != 1 {
		t.Fatal("wrong number of rows returned:", n)
	}
	if rowbuf[0][0].Column() != 0 {
		t.Errorf("wrong index: got=%d want=%d", rowbuf[0][0].Column(), 0)
	}

	n, err = rows.ReadRows(rowbuf)
	if err != io.EOF {
		t.Fatal("reading EOF:", err)
	}
	if n != 0 {
		t.Fatal("expected no more rows after EOF:", n)
	}
}

func TestRepeatedPageTrailingNulls(t *testing.T) {
	type testStruct struct {
		A []string `parquet:"a"`
	}

	s := parquet.SchemaOf(&testStruct{})

	records := []*testStruct{
		{A: nil},
		{A: []string{"test"}},
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

	rows := make([]parquet.Row, len(records)+1)
	reader := buf.Rows()
	defer reader.Close()

	n, err := reader.ReadRows(rows)
	if err != nil && err != io.EOF {
		t.Fatal("reading rows:", err)
	}

	if n != len(records) {
		t.Errorf("wrong number of rows read: got=%d want=%d", n, len(records))
	}
}

func TestReslicingBooleanPage(t *testing.T) {
	type testStruct struct {
		B bool `parquet:"b"`
	}

	numValues := 100
	expected := []*testStruct{}
	for i := range numValues {
		expected = append(expected, &testStruct{B: i%2 == 0})
	}

	buf := new(bytes.Buffer)
	writer := parquet.NewGenericWriter[*testStruct](buf)
	_, err := writer.Write(expected)
	if err != nil {
		t.Fatal(err)
	}
	err = writer.Close()
	if err != nil {
		t.Fatal(err)
	}

	reader := bytes.NewReader(buf.Bytes())
	pf, err := parquet.OpenFile(reader, reader.Size())
	if err != nil {
		t.Fatal(err)
	}

	// grab the page we wrote above
	rg := pf.RowGroups()[0]
	cc := rg.ColumnChunks()
	pgs := cc[0].Pages()
	defer pgs.Close()

	pg, err := pgs.ReadPage()
	if err != nil {
		t.Fatal(err)
	}
	defer parquet.Release(pg)

	// continue reslicing and reading the values
	sliceEvery := 3
	for i := 0; i < numValues-1; i += sliceEvery {
		vs := make([]parquet.Value, numValues)

		low := int64(sliceEvery)
		high := int64(numValues - i)

		if low >= high {
			break
		}

		// slice the page
		pg = pg.Slice(low, high)
		defer parquet.Release(pg)

		v := pg.Values()
		v.ReadValues(vs)

		// and the expected values with the same low/high
		expected = expected[low:high]

		// confirm values match
		for n, exp := range expected {
			if exp.B != vs[n].Boolean() {
				t.Fatalf("unexpected value: %v at pos: %d", vs[n], n)
			}
			n++
		}
	}
}
