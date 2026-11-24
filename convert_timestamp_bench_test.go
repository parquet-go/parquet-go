package parquet_test

import (
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
)

func BenchmarkConvertTimestampMillisToNanos(b *testing.B) {
	benchmarkConvertTimestamp(b, parquet.Millisecond, parquet.Nanosecond, 1_670_888_613_123)
}

func BenchmarkConvertTimestampMillisToMicros(b *testing.B) {
	benchmarkConvertTimestamp(b, parquet.Millisecond, parquet.Microsecond, 1_670_888_613_123)
}

func BenchmarkConvertTimestampMicrosToMillis(b *testing.B) {
	benchmarkConvertTimestamp(b, parquet.Microsecond, parquet.Millisecond, 1_670_888_613_123_456)
}

func BenchmarkConvertTimestampToDate(b *testing.B) {
	from := parquet.Timestamp(parquet.Millisecond).Type()
	to := parquet.Date().Type()
	values := timestampBenchValues(1_670_888_613_123)
	benchmarkConvertValues(b, to, from, values)
}

func BenchmarkAssignTimeMicros(b *testing.B) {
	typ := parquet.Time(parquet.Microsecond).Type()
	values := timestampBenchValues(12 * time.Hour.Microseconds())
	var d time.Duration
	dst := reflect.ValueOf(&d).Elem()
	b.ReportAllocs()
	var i int
	for b.Loop() {
		if err := typ.AssignValue(dst, values[i]); err != nil {
			b.Fatal(err)
		}
		i++
		if i == len(values) {
			i = 0
		}
	}
	timestampBenchSink = parquet.Int64Value(int64(d))
}

func BenchmarkConvertTimestampRows(b *testing.B) {
	type source struct {
		T time.Time `parquet:"t,timestamp(millisecond)"`
	}
	type target struct {
		T time.Time `parquet:"t,timestamp(microsecond)"`
	}

	const n = 4096
	rows := make([]source, n)
	base := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := range rows {
		rows[i].T = base.Add(time.Duration(i) * time.Second)
	}
	buf := parquet.NewGenericBuffer[source]()
	if _, err := buf.Write(rows); err != nil {
		b.Fatal(err)
	}
	conv, err := parquet.Convert(parquet.SchemaOf(new(target)), buf.Schema())
	if err != nil {
		b.Fatal(err)
	}

	out := make([]parquet.Row, 256)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		reader := parquet.ConvertRowGroup(buf, conv).Rows()
		for {
			n, err := reader.ReadRows(out)
			if n == 0 || err == io.EOF {
				break
			}
			if err != nil {
				reader.Close()
				b.Fatal(err)
			}
		}
		reader.Close()
	}
}

func benchmarkConvertTimestamp(b *testing.B, fromUnit, toUnit parquet.TimeUnit, seed int64) {
	b.Helper()
	from := parquet.Timestamp(fromUnit).Type()
	to := parquet.Timestamp(toUnit).Type()
	benchmarkConvertValues(b, to, from, timestampBenchValues(seed))
}

var timestampBenchSink parquet.Value

func benchmarkConvertValues(b *testing.B, to, from parquet.Type, values []parquet.Value) {
	b.Helper()
	b.ReportAllocs()
	var i int
	for b.Loop() {
		v, err := to.ConvertValue(values[i], from)
		if err != nil {
			b.Fatal(err)
		}
		timestampBenchSink = v
		i++
		if i == len(values) {
			i = 0
		}
	}
}

func timestampBenchValues(seed int64) []parquet.Value {
	values := make([]parquet.Value, 1024)
	for i := range values {
		values[i] = parquet.Int64Value(seed + int64(i)*17)
	}
	return values
}
