//go:build !race

package parquet_test

import (
	"bytes"
	"io"
	"math/rand"
	"reflect"
	"runtime"
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

func TestColumnChunkAllocs(t *testing.T) {
	testColumnChunkScan[benchmarkRowType](t)
	testColumnChunkScan[booleanColumn](t)
	testColumnChunkScan[int32Column](t)
	testColumnChunkScan[int64Column](t)
	testColumnChunkScan[floatColumn](t)
	testColumnChunkScan[doubleColumn](t)
	testColumnChunkScan[byteArrayColumn](t)
	testColumnChunkScan[fixedLenByteArrayColumn](t)
	testColumnChunkScan[stringColumn](t)
	testColumnChunkScan[indexedStringColumn](t)
	testColumnChunkScan[uuidColumn](t)
	testColumnChunkScan[timeColumn](t)
	testColumnChunkScan[timeInMillisColumn](t)
	testColumnChunkScan[mapColumn](t)
	testColumnChunkScan[decimalColumn](t)
	testColumnChunkScan[contact](t)
	testColumnChunkScan[paddedBooleanColumn](t)
	testColumnChunkScan[optionalInt32Column](t)
}

func testColumnChunkScan[Row generator[Row]](t *testing.T) {
	var (
		model   Row
		numRows = benchmarkNumRows * 5
		runs    = 10
	)

	t.Run(reflect.TypeOf(model).Name(), func(t *testing.T) {
		prng := rand.New(rand.NewSource(0))
		rows := make([]Row, numRows)
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
		offsetIndex, err := cc.OffsetIndex()
		if err != nil {
			panic(err)
		}

		numPages := offsetIndex.NumPages()
		dictSize := 0

		allocPerRun := allocBytesPerRun(runs, func() {
			pages := cc.Pages()
			defer pages.Close()
			for {
				page, err := pages.ReadPage()
				if err == io.EOF {
					break
				}
				if err != nil {
					panic(err)
				}
				if dict := page.Dictionary(); dict != nil {
					dictSize = int(dict.Size())
				}
				parquet.Release(page)
			}
		})

		// Non-dictionary alloc averaged across runs and pages.
		// For now, dictionary buffers are allocated fresh per row group
		// and there is one per run in this test.
		// The remaining alloc consists of format.PageHeader, bufferedPage, FilePages, etc.
		avgAllocPerPage := (allocPerRun - uint64(dictSize*runs)) / uint64(numPages)

		if int(avgAllocPerPage) > 1000 {
			t.Errorf("avg alloc per page should be under 1KB, got %d", avgAllocPerPage)
		}
	})
}

func allocBytesPerRun(runs int, f func()) (avg uint64) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))

	// Warm up the function
	f()

	// Measure the starting statistics
	var memstats runtime.MemStats
	runtime.ReadMemStats(&memstats)
	start := memstats.TotalAlloc

	// Run the function the specified number of times
	for range runs {
		f()
	}

	// Read the final statistics
	runtime.ReadMemStats(&memstats)

	// Average the mallocs over the runs (not counting the warm-up).
	// We are forced to return a float64 because the API is silly, but do
	// the division as integers so we can ask if AllocsPerRun()==1
	// instead of AllocsPerRun()<2.
	return (memstats.TotalAlloc - start) / uint64(runs)
}
