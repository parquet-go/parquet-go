package parquet_test

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
)

var dictionaryTypes = [...]parquet.Type{
	parquet.BooleanType,
	parquet.Int32Type,
	parquet.Int64Type,
	parquet.Int96Type,
	parquet.FloatType,
	parquet.DoubleType,
	parquet.ByteArrayType,
	parquet.FixedLenByteArrayType(10),
	parquet.FixedLenByteArrayType(16),
	parquet.Uint(32).Type(),
	parquet.Uint(64).Type(),
}

func TestDictionary(t *testing.T) {
	for _, typ := range dictionaryTypes {
		for _, numValues := range []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 17, 1e2, 1e3, 1e4} {
			t.Run(fmt.Sprintf("%s/N=%d", typ, numValues), func(t *testing.T) {
				testDictionary(t, typ, numValues)
			})
		}
	}
}

func testDictionary(t *testing.T, typ parquet.Type, numValues int) {
	const columnIndex = 1

	dict := typ.NewDictionary(columnIndex, 0, typ.NewValues(nil, nil))
	values := make([]parquet.Value, numValues)
	indexes := make([]int32, numValues)
	lookups := make([]parquet.Value, numValues)

	f := randValueFuncOf(typ)
	r := rand.New(rand.NewSource(int64(numValues)))

	for i := range values {
		values[i] = f(r)
		values[i] = values[i].Level(0, 0, columnIndex)
	}

	mapping := make(map[int32]parquet.Value, numValues)

	for i := 0; i < numValues; {
		j := i + ((numValues-i)/2 + 1)
		if j > numValues {
			j = numValues
		}

		dict.Insert(indexes[i:j], values[i:j])

		for k, v := range values[i:j] {
			mapping[indexes[i+k]] = v
		}

		for _, index := range indexes[i:j] {
			if index < 0 || index >= int32(dict.Len()) {
				t.Fatalf("index out of bounds: %d", index)
			}
		}

		// second insert is a no-op since all the values are already in the dictionary
		lastDictLen := dict.Len()
		dict.Insert(indexes[i:j], values[i:j])

		if dict.Len() != lastDictLen {
			for k, index := range indexes[i:j] {
				if index >= int32(len(mapping)) {
					t.Log(values[i+k])
				}
			}

			t.Fatalf("%d values were inserted on the second pass", dict.Len()-len(mapping))
		}

		r.Shuffle(j-i, func(a, b int) {
			indexes[a+i], indexes[b+i] = indexes[b+i], indexes[a+i]
		})

		dict.Lookup(indexes[i:j], lookups[i:j])

		for lookupIndex, valueIndex := range indexes[i:j] {
			want := mapping[valueIndex]
			got := lookups[lookupIndex+i]

			if !parquet.DeepEqual(want, got) {
				t.Fatalf("wrong value looked up at index %d: want=%#v got=%#v", valueIndex, want, got)
			}
		}

		minValue := values[i]
		maxValue := values[i]

		for _, value := range values[i+1 : j] {
			switch {
			case typ.Compare(value, minValue) < 0:
				minValue = value
			case typ.Compare(value, maxValue) > 0:
				maxValue = value
			}
		}

		lowerBound, upperBound := dict.Bounds(indexes[i:j])
		if !parquet.DeepEqual(lowerBound, minValue) {
			t.Errorf("wrong lower bound between indexes %d and %d: want=%#v got=%#v", i, j, minValue, lowerBound)
		}
		if !parquet.DeepEqual(upperBound, maxValue) {
			t.Errorf("wrong upper bound between indexes %d and %d: want=%#v got=%#v", i, j, maxValue, upperBound)
		}

		i = j
	}

	for i := range lookups {
		lookups[i] = parquet.Value{}
	}

	dict.Lookup(indexes, lookups)

	for lookupIndex, valueIndex := range indexes {
		want := mapping[valueIndex]
		got := lookups[lookupIndex]

		if !parquet.Equal(want, got) {
			t.Fatalf("wrong value looked up at index %d: want=%+v got=%+v", valueIndex, want, got)
		}
	}
}

func BenchmarkDictionary(b *testing.B) {
	tests := []struct {
		scenario string
		init     func(parquet.Dictionary, []int32, []parquet.Value)
		test     func(parquet.Dictionary, []int32, []parquet.Value)
	}{
		{
			scenario: "Bounds",
			init:     parquet.Dictionary.Insert,
			test: func(dict parquet.Dictionary, indexes []int32, _ []parquet.Value) {
				dict.Bounds(indexes)
			},
		},

		{
			scenario: "Insert",
			test:     parquet.Dictionary.Insert,
		},

		{
			scenario: "Lookup",
			init:     parquet.Dictionary.Insert,
			test:     parquet.Dictionary.Lookup,
		},
	}

	for i, test := range tests {
		b.Run(test.scenario, func(b *testing.B) {
			for j, typ := range dictionaryTypes {
				for _, numValues := range []int{1e2, 1e3, 1e4, 1e5, 1e6} {
					buf := typ.NewValues(make([]byte, 0, 4*numValues), nil)
					dict := typ.NewDictionary(0, 0, buf)
					values := make([]parquet.Value, numValues)

					f := randValueFuncOf(typ)
					r := rand.New(rand.NewSource(int64(i * j * numValues)))

					for i := range values {
						values[i] = f(r)
					}

					indexes := make([]int32, len(values))
					if test.init != nil {
						test.init(dict, indexes, values)
					}

					b.Run(fmt.Sprintf("%s/N=%d", typ, numValues), func(b *testing.B) {
						start := time.Now()

						for i := 0; i < b.N; i++ {
							test.test(dict, indexes, values)
						}

						seconds := time.Since(start).Seconds()
						b.ReportMetric(float64(numValues*b.N)/seconds, "value/s")
					})
				}
			}
		})
	}
}

func TestIssue312(t *testing.T) {
	node := parquet.String()
	node = parquet.Encoded(node, &parquet.RLEDictionary)
	g := parquet.Group{}
	g["mystring"] = node
	schema := parquet.NewSchema("test", g)

	rows := []parquet.Row{[]parquet.Value{parquet.ValueOf("hello").Level(0, 0, 0)}}

	var storage bytes.Buffer

	tests := []struct {
		name        string
		getRowGroup func(t *testing.T) parquet.RowGroup
	}{
		{
			name: "Writer",
			getRowGroup: func(t *testing.T) parquet.RowGroup {
				t.Helper()

				w := parquet.NewWriter(&storage, schema)
				_, err := w.WriteRows(rows)
				if err != nil {
					t.Fatal(err)
				}
				if err := w.Close(); err != nil {
					t.Fatal(err)
				}

				r := bytes.NewReader(storage.Bytes())
				f, err := parquet.OpenFile(r, int64(storage.Len()))
				if err != nil {
					t.Fatal(err)
				}
				return f.RowGroups()[0]
			},
		},
		{
			name: "Buffer",
			getRowGroup: func(t *testing.T) parquet.RowGroup {
				t.Helper()

				b := parquet.NewBuffer(schema)
				_, err := b.WriteRows(rows)
				if err != nil {
					t.Fatal(err)
				}
				return b
			},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			rowGroup := testCase.getRowGroup(t)

			chunk := rowGroup.ColumnChunks()[0]
			idx := chunk.ColumnIndex()
			val := idx.MinValue(0)
			columnType := chunk.Type()
			values := columnType.NewValues(val.Bytes(), []uint32{0, uint32(len(val.Bytes()))})

			// This test ensures that the dictionary type created by column
			// chunks of parquet readers and buffers are the same. We want the
			// column chunk type to be the actual value type, even when the
			// schema uses a dictionary encoding.
			//
			// https://github.com/segmentio/parquet-go/issues/312
			_ = columnType.NewDictionary(0, 1, values)
		})
	}
}
