package thriftdecode

import (
	"bytes"
	"testing"

	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"
)

// Test data generators

func makePageLocation(offset int64, size int32, firstRow int64) format.PageLocation {
	return format.PageLocation{
		Offset:             offset,
		CompressedPageSize: size,
		FirstRowIndex:      firstRow,
	}
}

func makeOffsetIndex(numPages int) format.OffsetIndex {
	pages := make([]format.PageLocation, numPages)
	for i := range pages {
		pages[i] = makePageLocation(int64(i*1000), int32(i*100+50), int64(i*500))
	}
	return format.OffsetIndex{
		PageLocations: pages,
	}
}

func makeColumnIndex(numPages int) format.ColumnIndex {
	nullPages := make([]bool, numPages)
	minValues := make([][]byte, numPages)
	maxValues := make([][]byte, numPages)
	nullCounts := make([]int64, numPages)

	for i := range numPages {
		nullPages[i] = i%5 == 0 // every 5th page is null
		minValues[i] = []byte{byte(i), byte(i + 1), byte(i + 2), byte(i + 3)}
		maxValues[i] = []byte{byte(i + 10), byte(i + 11), byte(i + 12), byte(i + 13)}
		nullCounts[i] = int64(i * 10)
	}

	return format.ColumnIndex{
		NullPages:     nullPages,
		MinValues:     minValues,
		MaxValues:     maxValues,
		BoundaryOrder: format.Ascending,
		NullCounts:    nullCounts,
	}
}

// Helper to encode a value using the standard thrift encoder
func encodeThrift(v any) []byte {
	protocol := &thrift.CompactProtocol{}
	data, err := thrift.Marshal(protocol, v)
	if err != nil {
		panic(err)
	}
	return data
}

// Unit Tests

func TestDecodePageLocation(t *testing.T) {
	original := makePageLocation(12345, 678, 9012)
	data := encodeThrift(&original)

	var decoded format.PageLocation
	err := DecodePageLocation(data, &decoded)
	if err != nil {
		t.Fatalf("DecodePageLocation failed: %v", err)
	}

	if decoded.Offset != original.Offset {
		t.Errorf("Offset: got %d, want %d", decoded.Offset, original.Offset)
	}
	if decoded.CompressedPageSize != original.CompressedPageSize {
		t.Errorf("CompressedPageSize: got %d, want %d", decoded.CompressedPageSize, original.CompressedPageSize)
	}
	if decoded.FirstRowIndex != original.FirstRowIndex {
		t.Errorf("FirstRowIndex: got %d, want %d", decoded.FirstRowIndex, original.FirstRowIndex)
	}
}

func TestDecodeOffsetIndex(t *testing.T) {
	original := makeOffsetIndex(10)
	data := encodeThrift(&original)

	var decoded format.OffsetIndex
	err := DecodeOffsetIndex(data, &decoded)
	if err != nil {
		t.Fatalf("DecodeOffsetIndex failed: %v", err)
	}

	if len(decoded.PageLocations) != len(original.PageLocations) {
		t.Fatalf("PageLocations length: got %d, want %d", len(decoded.PageLocations), len(original.PageLocations))
	}

	for i := range original.PageLocations {
		if decoded.PageLocations[i] != original.PageLocations[i] {
			t.Errorf("PageLocations[%d]: got %+v, want %+v", i, decoded.PageLocations[i], original.PageLocations[i])
		}
	}
}

func TestDecodeColumnIndex(t *testing.T) {
	original := makeColumnIndex(10)
	data := encodeThrift(&original)

	var decoded format.ColumnIndex
	err := DecodeColumnIndex(data, &decoded)
	if err != nil {
		t.Fatalf("DecodeColumnIndex failed: %v", err)
	}

	if len(decoded.NullPages) != len(original.NullPages) {
		t.Fatalf("NullPages length: got %d, want %d", len(decoded.NullPages), len(original.NullPages))
	}
	for i := range original.NullPages {
		if decoded.NullPages[i] != original.NullPages[i] {
			t.Errorf("NullPages[%d]: got %v, want %v", i, decoded.NullPages[i], original.NullPages[i])
		}
	}

	if len(decoded.MinValues) != len(original.MinValues) {
		t.Fatalf("MinValues length: got %d, want %d", len(decoded.MinValues), len(original.MinValues))
	}
	for i := range original.MinValues {
		if !bytes.Equal(decoded.MinValues[i], original.MinValues[i]) {
			t.Errorf("MinValues[%d]: got %v, want %v", i, decoded.MinValues[i], original.MinValues[i])
		}
	}

	if len(decoded.MaxValues) != len(original.MaxValues) {
		t.Fatalf("MaxValues length: got %d, want %d", len(decoded.MaxValues), len(original.MaxValues))
	}
	for i := range original.MaxValues {
		if !bytes.Equal(decoded.MaxValues[i], original.MaxValues[i]) {
			t.Errorf("MaxValues[%d]: got %v, want %v", i, decoded.MaxValues[i], original.MaxValues[i])
		}
	}

	if decoded.BoundaryOrder != original.BoundaryOrder {
		t.Errorf("BoundaryOrder: got %d, want %d", decoded.BoundaryOrder, original.BoundaryOrder)
	}

	if len(decoded.NullCounts) != len(original.NullCounts) {
		t.Fatalf("NullCounts length: got %d, want %d", len(decoded.NullCounts), len(original.NullCounts))
	}
	for i := range original.NullCounts {
		if decoded.NullCounts[i] != original.NullCounts[i] {
			t.Errorf("NullCounts[%d]: got %d, want %d", i, decoded.NullCounts[i], original.NullCounts[i])
		}
	}
}

func TestDecodeColumnIndexWithHistograms(t *testing.T) {
	original := makeColumnIndex(5)
	// Add histograms
	original.RepetitionLevelHistogram = []int64{100, 200, 300, 400, 500}
	original.DefinitionLevelHistogram = []int64{10, 20, 30, 40, 50}

	data := encodeThrift(&original)

	var decoded format.ColumnIndex
	err := DecodeColumnIndex(data, &decoded)
	if err != nil {
		t.Fatalf("DecodeColumnIndex failed: %v", err)
	}

	if len(decoded.RepetitionLevelHistogram) != len(original.RepetitionLevelHistogram) {
		t.Fatalf("RepetitionLevelHistogram length: got %d, want %d",
			len(decoded.RepetitionLevelHistogram), len(original.RepetitionLevelHistogram))
	}
	for i := range original.RepetitionLevelHistogram {
		if decoded.RepetitionLevelHistogram[i] != original.RepetitionLevelHistogram[i] {
			t.Errorf("RepetitionLevelHistogram[%d]: got %d, want %d",
				i, decoded.RepetitionLevelHistogram[i], original.RepetitionLevelHistogram[i])
		}
	}

	if len(decoded.DefinitionLevelHistogram) != len(original.DefinitionLevelHistogram) {
		t.Fatalf("DefinitionLevelHistogram length: got %d, want %d",
			len(decoded.DefinitionLevelHistogram), len(original.DefinitionLevelHistogram))
	}
	for i := range original.DefinitionLevelHistogram {
		if decoded.DefinitionLevelHistogram[i] != original.DefinitionLevelHistogram[i] {
			t.Errorf("DefinitionLevelHistogram[%d]: got %d, want %d",
				i, decoded.DefinitionLevelHistogram[i], original.DefinitionLevelHistogram[i])
		}
	}
}

// Benchmarks

func BenchmarkDecodePageLocation_Thrift(b *testing.B) {
	original := makePageLocation(12345, 678, 9012)
	data := encodeThrift(&original)
	protocol := &thrift.CompactProtocol{}

	b.ReportAllocs()

	for b.Loop() {
		var decoded format.PageLocation
		if err := thrift.Unmarshal(protocol, data, &decoded); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodePageLocation_Optimized(b *testing.B) {
	original := makePageLocation(12345, 678, 9012)
	data := encodeThrift(&original)

	b.ReportAllocs()

	for b.Loop() {
		var decoded format.PageLocation
		if err := DecodePageLocation(data, &decoded); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeOffsetIndex_Thrift(b *testing.B) {
	original := makeOffsetIndex(100)
	data := encodeThrift(&original)
	protocol := &thrift.CompactProtocol{}

	b.ReportAllocs()

	for b.Loop() {
		var decoded format.OffsetIndex
		if err := thrift.Unmarshal(protocol, data, &decoded); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeOffsetIndex_Optimized(b *testing.B) {
	original := makeOffsetIndex(100)
	data := encodeThrift(&original)

	b.ReportAllocs()

	for b.Loop() {
		var decoded format.OffsetIndex
		if err := DecodeOffsetIndex(data, &decoded); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeColumnIndex_Thrift(b *testing.B) {
	original := makeColumnIndex(100)
	data := encodeThrift(&original)
	protocol := &thrift.CompactProtocol{}

	b.ReportAllocs()

	for b.Loop() {
		var decoded format.ColumnIndex
		if err := thrift.Unmarshal(protocol, data, &decoded); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeColumnIndex_Optimized(b *testing.B) {
	original := makeColumnIndex(100)
	data := encodeThrift(&original)

	b.ReportAllocs()

	for b.Loop() {
		var decoded format.ColumnIndex
		if err := DecodeColumnIndex(data, &decoded); err != nil {
			b.Fatal(err)
		}
	}
}
