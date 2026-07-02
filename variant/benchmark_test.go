package variant

import (
	"testing"
)

func BenchmarkMarshalInt64Slice(b *testing.B) {
	slice := make([]int64, 1000)
	for i := range slice {
		slice[i] = int64(i)
	}

	b.ReportAllocs()
	for b.Loop() {
		meta, val, err := Marshal(slice)
		if err != nil {
			b.Fatal(err)
		}
		_ = meta
		_ = val
	}
}

func BenchmarkEncodePrimitive(b *testing.B) {
	v := Int64(42)
	var builder MetadataBuilder

	b.ReportAllocs()
	for b.Loop() {
		_ = Encode(&builder, v)
	}
}

func BenchmarkEncodeNested(b *testing.B) {
	fields := []Field{
		{Name: "a", Value: Int64(1)},
		{Name: "b", Value: String("hello")},
	}
	obj := MakeObject(fields)
	elements := make([]Value, 10)
	for i := range elements {
		elements[i] = obj
	}
	arr := MakeArray(elements)

	var builder MetadataBuilder

	b.ReportAllocs()
	for b.Loop() {
		_ = Encode(&builder, arr)
	}
}
