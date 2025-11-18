package parquet_test

import (
	"bytes"
	"io"
	"math/rand"
	"reflect"
	"testing"

	"github.com/parquet-go/parquet-go"
)

func BenchmarkColumnChunkScan(b *testing.B) {
	benchmarkColumnChunkScan[benchmarkRowType](b)
	benchmarkColumnChunkScan[booleanColumn](b)
	benchmarkColumnChunkScan[int32Column](b)
	benchmarkColumnChunkScan[int64Column](b)
	benchmarkColumnChunkScan[floatColumn](b)
	benchmarkColumnChunkScan[doubleColumn](b)
	benchmarkColumnChunkScan[byteArrayColumn](b)
	benchmarkColumnChunkScan[fixedLenByteArrayColumn](b)
	benchmarkColumnChunkScan[stringColumn](b)
	benchmarkColumnChunkScan[indexedStringColumn](b)
	benchmarkColumnChunkScan[uuidColumn](b)
	benchmarkColumnChunkScan[timeColumn](b)
	benchmarkColumnChunkScan[timeInMillisColumn](b)
	benchmarkColumnChunkScan[mapColumn](b)
	benchmarkColumnChunkScan[decimalColumn](b)
	benchmarkColumnChunkScan[contact](b)
	benchmarkColumnChunkScan[paddedBooleanColumn](b)
	benchmarkColumnChunkScan[optionalInt32Column](b)
}

func benchmarkColumnChunkScan[Row generator[Row]](b *testing.B) {
	var model Row
	b.Run(reflect.TypeOf(model).Name(), func(b *testing.B) {
		prng := rand.New(rand.NewSource(0))
		rows := make([]Row, benchmarkNumRows)
		for i := range rows {
			rows[i] = rows[i].generate(prng)
		}

		buf := bytes.Buffer{}
		writer := parquet.NewGenericWriter[Row](&buf)
		_, err := writer.Write(rows)
		if err != nil {
			panic(err)
		}
		err = writer.Close()
		if err != nil {
			panic(err)
		}

		f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		if err != nil {
			panic(err)
		}

		cc := f.RowGroups()[0].ColumnChunks()[0]
		vbuf := make([]parquet.Value, 1024)
		count := 0
		b.ResetTimer()
		for b.Loop() {
			pages := cc.Pages()
			for {
				page, err := pages.ReadPage()
				if err == io.EOF {
					break
				}
				if err != nil {
					panic(err)
				}

				values := page.Values()
				for {
					n, err := values.ReadValues(vbuf)
					count += n
					if n < len(vbuf) || err != nil {
						break
					}
				}
				parquet.Release(page)
			}
			pages.Close()
		}
		b.ReportMetric(float64(count)/b.Elapsed().Seconds(), "values/s")
	})
}
