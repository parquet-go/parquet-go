// These tests look at buffer reuse via sync.Pool, which does not work under
// the race detector.  There also seems to be a platform difference on s390x,
// so we skip the tests in those cases.
//go:build !race && !s390x

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
		var (
			f     = fileWithRows[Row](benchmarkNumRows)
			cc    = f.RowGroups()[0].ColumnChunks()[0]
			vbuf  = make([]parquet.Value, 1024)
			count = 0
		)

		b.ResetTimer()
		for b.Loop() {
			// Read all values in all pages.
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
		numRows = benchmarkNumRows * 5 // This is enough to get several pages of each type.
		runs    = 10
	)

	t.Run(reflect.TypeOf(model).Name(), func(t *testing.T) {
		f := fileWithRows[Row](numRows)
		cc := f.RowGroups()[0].ColumnChunks()[0]

		offsetIndex, err := cc.OffsetIndex()
		if err != nil {
			panic(err)
		}

		numPages := offsetIndex.NumPages()
		dictSize := 0

		allocPerRun := allocBytesPerRun(runs, func() {
			// Iterate through all pages and return them to the pool.
			// Value/offsetbuffers should be reused each run.
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
				// This isn't accessible earlier through public interfaces,
				// so for now we remember the last dictionary size here.
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

		// Byte array types (including strings and nested types containing them)
		// clone their data to prevent use-after-free when pooled buffers are
		// reused across files during operations like MergeRowGroups. This is
		// necessary for memory safety but means allocation is proportional to
		// data size rather than fixed overhead.
		typeName := reflect.TypeOf(model).Name()
		isByteArrayType := typeName == "byteArrayColumn" ||
			typeName == "stringColumn" ||
			typeName == "mapColumn" ||
			typeName == "contact"

		if !isByteArrayType && int(avgAllocPerPage) > 1000 {
			t.Errorf("avg alloc per page should be under 1KB, got %d", avgAllocPerPage)
		}
	})
}

func fileWithRows[Row generator[Row]](numRows int) *parquet.File {
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

	return f
}

// allocBytesPerRun is like testing.AllocsPerRun but finds the average
// number of bytes allocated instead of the number of allocations.
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
	return (memstats.TotalAlloc - start) / uint64(runs)
}
