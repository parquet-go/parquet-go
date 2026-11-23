package parquet

import (
	"fmt"
	"io"
	"testing"
	"time"
)

type iterBenchInt struct {
	ID    int64 `parquet:",delta"`
	Value int64 `parquet:",delta"`
}

type iterBenchString struct {
	Value string
}

func BenchmarkGenericReaderIter(b *testing.B) {
	benchmarkGenericReaderIter(b, "int64", makeIterBenchInts(10_000))
	benchmarkGenericReaderIter(b, "string", makeIterBenchStrings(10_000))
}

func makeIterBenchInts(n int) []iterBenchInt {
	rows := make([]iterBenchInt, n)
	for i := range rows {
		rows[i] = iterBenchInt{ID: int64(i), Value: int64(i * 3)}
	}
	return rows
}

func makeIterBenchStrings(n int) []iterBenchString {
	rows := make([]iterBenchString, n)
	for i := range rows {
		rows[i] = iterBenchString{Value: fmt.Sprintf("row-%d", i)}
	}
	return rows
}

func benchmarkGenericReaderIter[T any](b *testing.B, name string, rows []T) {
	b.Helper()
	buffer := NewGenericBuffer[T]()
	if _, err := buffer.Write(rows); err != nil {
		b.Fatal(err)
	}

	scan := func(b *testing.B, read func(*GenericReader[T]) (int, error)) {
		b.Helper()
		reader := NewGenericRowGroupReader[T](buffer)
		b.ReportAllocs()
		b.ResetTimer()
		start := time.Now()
		var total int64
		for b.Loop() {
			n, err := read(reader)
			if err != nil {
				b.Fatal(err)
			}
			total += int64(n)
			reader.Reset()
		}
		b.ReportMetric(float64(total)/time.Since(start).Seconds(), "row/s")
	}

	b.Run(name+"/Read", func(b *testing.B) {
		buf := make([]T, genericReaderIterBatch)
		scan(b, func(reader *GenericReader[T]) (int, error) {
			return readAll(reader, buf)
		})
	})

	b.Run(name+"/Read1000", func(b *testing.B) {
		buf := make([]T, 1000)
		scan(b, func(reader *GenericReader[T]) (int, error) {
			return readAll(reader, buf)
		})
	})

	b.Run(name+"/Iter", func(b *testing.B) {
		scan(b, func(reader *GenericReader[T]) (int, error) {
			n := 0
			for _, err := range reader.Iter() {
				if err != nil {
					return n, err
				}
				n++
			}
			return n, nil
		})
	})
}

func readAll[T any](reader *GenericReader[T], buf []T) (int, error) {
	n := 0
	for {
		rn, err := reader.Read(buf)
		n += rn
		if err != nil {
			if err == io.EOF {
				return n, nil
			}
			return n, err
		}
		if rn == 0 {
			return n, io.ErrNoProgress
		}
	}
}
