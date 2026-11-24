package parquet_test

import (
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
