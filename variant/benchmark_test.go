package variant

import (
	"fmt"
	"strings"
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

func BenchmarkMarshalRepresentativeShapes(b *testing.B) {
	benchmarks := []struct {
		name  string
		input any
	}{
		{"Int64Slice", benchmarkInt64Slice(1000)},
		{"StringSlice", benchmarkStringSlice(1000)},
		{"MixedSlice", benchmarkMixedSlice(300)},
		{"NestedMap", benchmarkNestedMap(100)},
		{"LargeObject", benchmarkLargeObject(300)},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				meta, val, err := Marshal(bm.input)
				if err != nil {
					b.Fatal(err)
				}
				_ = meta
				_ = val
			}
		})
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

func benchmarkInt64Slice(n int) []int64 {
	out := make([]int64, n)
	for i := range out {
		out[i] = int64(i)
	}
	return out
}

func benchmarkStringSlice(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = strings.Repeat("x", 40)
	}
	return out
}

func benchmarkMixedSlice(n int) []any {
	out := make([]any, n)
	for i := range out {
		out[i] = map[string]any{
			"i":  int64(i),
			"s":  strings.Repeat("y", i%80),
			"ok": i%2 == 0,
		}
	}
	return out
}

func benchmarkNestedMap(n int) map[string]any {
	out := make(map[string]any, n)
	for i := range n {
		out[fmt.Sprintf("row_%03d", i)] = []any{
			int64(i),
			strings.Repeat("z", 20),
			map[string]any{"nested": i%3 == 0},
		}
	}
	return out
}

func benchmarkLargeObject(n int) map[string]any {
	out := make(map[string]any, n)
	for i := range n {
		out[fmt.Sprintf("k%03d", i)] = strings.Repeat("x", 300)
	}
	return out
}
