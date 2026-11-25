package parquet

import "testing"

func BenchmarkConvertDecimalInt32ToFixedLen(b *testing.B) {
	from := Decimal(2, 9, Int32Type).Type()
	to := Decimal(2, 38, FixedLenByteArrayType(16)).Type()
	values := make([]Value, 1024)
	for i := range values {
		values[i] = Int32Value(int32(i*17-1000)).Level(0, 1, 0)
	}
	benchmarkDecimalConvert(b, to, from, values)
}

func BenchmarkConvertDecimalInt64ToByteArray(b *testing.B) {
	from := Decimal(2, 18, Int64Type).Type()
	to := Decimal(2, 38, ByteArrayType).Type()
	values := make([]Value, 1024)
	for i := range values {
		values[i] = Int64Value(int64(i*17-1000)).Level(0, 1, 0)
	}
	benchmarkDecimalConvert(b, to, from, values)
}

func BenchmarkConvertDecimalFixedLenWiden(b *testing.B) {
	from := Decimal(2, 9, FixedLenByteArrayType(4)).Type()
	to := Decimal(2, 38, FixedLenByteArrayType(16)).Type()
	values := make([]Value, 1024)
	for i := range values {
		v := int32(i*17 - 1000)
		buf := []byte{byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}
		values[i] = FixedLenByteArrayValue(buf).Level(0, 1, 0)
	}
	benchmarkDecimalConvert(b, to, from, values)
}

func BenchmarkConvertDecimalFixedLenNarrow(b *testing.B) {
	from := Decimal(2, 38, FixedLenByteArrayType(16)).Type()
	to := Decimal(2, 18, FixedLenByteArrayType(8)).Type()
	values := make([]Value, 1024)
	for i := range values {
		var buf [16]byte
		putBigEndianInt64(buf[:], int64(i*17-1000))
		values[i] = FixedLenByteArrayValue(append([]byte(nil), buf[:]...)).Level(0, 1, 0)
	}
	benchmarkDecimalConvert(b, to, from, values)
}

func BenchmarkConvertDecimalFixedLenToInt64(b *testing.B) {
	from := Decimal(2, 38, FixedLenByteArrayType(16)).Type()
	to := Decimal(2, 18, Int64Type).Type()
	values := make([]Value, 1024)
	for i := range values {
		var buf [16]byte
		putBigEndianInt64(buf[:], int64(i*17-1000))
		values[i] = FixedLenByteArrayValue(append([]byte(nil), buf[:]...)).Level(0, 1, 0)
	}
	benchmarkDecimalConvert(b, to, from, values)
}

func benchmarkDecimalConvert(b *testing.B, to, from Type, values []Value) {
	b.Helper()
	b.ReportAllocs()
	var i int
	for b.Loop() {
		v, err := to.ConvertValue(values[i], from)
		if err != nil {
			b.Fatal(err)
		}
		decimalConvertSink = v
		i++
		if i == len(values) {
			i = 0
		}
	}
}

var decimalConvertSink Value
