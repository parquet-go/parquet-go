package parquet_test

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/parquet-go/parquet-go"
)

func TestBufferReadValuesAt(t *testing.T) {
	type element struct{ Value *int32 }
	type row struct {
		Optional *int32
		Repeated []element
	}
	schema := parquet.SchemaOf(new(row))
	for _, api := range []string{"Buffer", "GenericBuffer"} {
		for column, name := range []string{"optional", "repeated"} {
			t.Run(api+"/"+name, func(t *testing.T) {
				for size := 1; size <= 6; size++ {
					for mask := 0; mask < 1<<size; mask++ {
						rows := make([]row, size)
						var want []parquet.Value
						for i := range rows {
							if mask&(1<<i) != 0 {
								value := int32(11 * (i + 1))
								rows[i].Optional = &value
							}
							rows[i].Repeated = []element{{rows[i].Optional}, {rows[i].Optional}}
							for _, v := range schema.Deconstruct(nil, &rows[i]) {
								if v.Column() == column {
									want = append(want, v)
								}
							}
						}
						var columns []parquet.ColumnBuffer
						if api == "Buffer" {
							buf := parquet.NewBuffer(schema)
							for i := range rows {
								if err := buf.Write(&rows[i]); err != nil {
									t.Fatal(err)
								}
							}
							columns = buf.ColumnBuffers()
						} else {
							buf := parquet.NewGenericBuffer[row]()
							if _, err := buf.Write(rows); err != nil {
								t.Fatal(err)
							}
							columns = buf.ColumnBuffers()
						}
						for offset := 0; offset < len(want); offset++ {
							for length := 0; length <= len(want)+1; length++ {
								sentinel := parquet.Int32Value(-1).Level(0, 0, column)
								values := make([]parquet.Value, length+1)
								for i := range values {
									values[i] = sentinel
								}
								var n int
								var err error
								func() {
									defer func() {
										if p := recover(); p != nil {
											t.Fatalf("size=%d mask=%b offset=%d length=%d: ReadValuesAt panicked: %v", size, mask, offset, length, p)
										}
									}()
									n, err = columns[column].ReadValuesAt(values[:length], int64(offset))
								}()
								count := min(length, len(want)-offset)
								if n != count || err != nil {
									t.Fatalf("size=%d mask=%b offset=%d length=%d: read (%d, %v), want (%d, nil)", size, mask, offset, length, n, err, count)
								}
								for i, got := range values {
									expected := sentinel
									if i < count {
										expected = want[offset+i]
									}
									if !reflect.DeepEqual(got, expected) {
										t.Fatalf("size=%d mask=%b offset=%d length=%d value[%d]: got %#v, want %#v", size, mask, offset, length, i, got, expected)
									}
								}
							}
						}
					}
				}
			})
		}
	}
}

func TestOptionalReadValuesAtShapes(t *testing.T) {
	type inner struct {
		Name *string
	}
	type row struct {
		Nested *inner
		Flag   *bool
		Name   *string `parquet:"name,dict,optional"`
		Fixed  *[8]byte
	}
	present := "here"
	empty := ""
	fixed := [8]byte{1, 2, 3, 4, 5, 6, 7, 8}
	flag := true
	rows := []row{
		{Nested: &inner{Name: &present}, Flag: &flag, Name: &present, Fixed: &fixed},
		{},
		{Nested: &inner{}, Name: &empty},
		{Nested: &inner{Name: &present}, Flag: &flag, Name: &present, Fixed: &fixed},
	}
	schema := parquet.SchemaOf(new(row))
	buf := parquet.NewGenericBuffer[row]()
	if _, err := buf.Write(rows); err != nil {
		t.Fatal(err)
	}
	columns := buf.ColumnBuffers()
	for column := range columns {
		var want []parquet.Value
		for i := range rows {
			for _, v := range schema.Deconstruct(nil, &rows[i]) {
				if v.Column() == column {
					want = append(want, v)
				}
			}
		}
		got := make([]parquet.Value, len(want))
		n, err := columns[column].ReadValuesAt(got, 0)
		if err != nil || n != len(want) {
			t.Fatalf("column %d: read (%d,%v), want %d", column, n, err, len(want))
		}
		for i := range got {
			if !sameReadValue(got[i], want[i]) {
				t.Fatalf("column %d value %d\ngot  %#v\nwant %#v", column, i, got[i], want[i])
			}
		}
		if len(want) < 2 {
			continue
		}
		tail := make([]parquet.Value, len(want)-1)
		n, err = columns[column].ReadValuesAt(tail, 1)
		if err != nil || n != len(tail) {
			t.Fatalf("column %d tail: n=%d err=%v", column, n, err)
		}
		for i := range tail {
			if !sameReadValue(tail[i], want[1+i]) {
				t.Fatalf("column %d tail value %d\ngot  %#v\nwant %#v", column, i, tail[i], want[1+i])
			}
		}
	}
}

func sameReadValue(a, b parquet.Value) bool {
	if a.Kind() != b.Kind() || a.DefinitionLevel() != b.DefinitionLevel() || a.RepetitionLevel() != b.RepetitionLevel() || a.Column() != b.Column() || a.IsNull() != b.IsNull() {
		return false
	}
	if a.IsNull() {
		return true
	}
	switch a.Kind() {
	case parquet.Boolean:
		return a.Boolean() == b.Boolean()
	case parquet.Int32:
		return a.Int32() == b.Int32()
	case parquet.Int64:
		return a.Int64() == b.Int64()
	case parquet.Float:
		return a.Float() == b.Float()
	case parquet.Double:
		return a.Double() == b.Double()
	case parquet.ByteArray, parquet.FixedLenByteArray:
		return bytes.Equal(a.ByteArray(), b.ByteArray())
	default:
		return reflect.DeepEqual(a, b)
	}
}

func BenchmarkBufferReadValuesAt(b *testing.B) {
	type row struct {
		Optional *int32
		Repeated []int32
	}
	rows := make([]row, 100_000)
	for i := range rows {
		value := int32(i)
		rows[i] = row{Optional: &value, Repeated: []int32{value}}
	}
	buf := parquet.NewGenericBuffer[row]()
	if _, err := buf.Write(rows); err != nil {
		b.Fatal(err)
	}
	for column, name := range []string{"optional", "repeated"} {
		for _, mode := range []string{"whole-column", "tail"} {
			b.Run(name+"/"+mode, func(b *testing.B) {
				col := buf.ColumnBuffers()[column]
				length := int(col.NumValues())
				if mode == "tail" {
					length = 1024
				}
				values := make([]parquet.Value, length)
				offset := col.NumValues() - int64(length)
				b.ReportAllocs()
				for b.Loop() {
					if n, err := col.ReadValuesAt(values, offset); n != len(values) || err != nil {
						b.Fatalf("read (%d,%v)", n, err)
					}
				}
			})
		}
	}
}
