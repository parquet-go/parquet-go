package format_test

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"
)

func TestMarshalUnmarshalSchemaMetadata(t *testing.T) {
	protocol := &thrift.CompactProtocol{}
	metadata := &format.FileMetaData{
		Version: 1,
		Schema: []format.SchemaElement{
			{
				Name: "hello",
			},
		},
		RowGroups: []format.RowGroup{},
	}

	b, err := thrift.Marshal(protocol, metadata)
	if err != nil {
		t.Fatal(err)
	}

	decoded := &format.FileMetaData{}
	if err := thrift.Unmarshal(protocol, b, &decoded); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(metadata, decoded) {
		t.Error("values mismatch:")
		t.Logf("expected:\n%#v", metadata)
		t.Logf("found:\n%#v", decoded)
	}
}

func TestMarshalStatisticsIncludeZeroNullCount(t *testing.T) {
	protocol := &thrift.CompactProtocol{}
	statistics := &format.Statistics{
		NullCount: 0,
	}
	marshalled, err := thrift.Marshal(protocol, statistics)
	if err != nil {
		t.Fatal(err)
	}

	// [54, 0, 0] is the encoded representation because
	// 54 = 0011 0110 where 0011 is 3 for the field delta and 0110 is 6 for the int64 type
	// 0 is the value
	// 0 is the stop marker
	if !bytes.Equal(marshalled, []byte{54, 0, 0}) {
		t.Fatal("marshalled statistics does not match expected value")
	}
}

// TestWriteZeroStatisticsFields verifies that Statistics fields with writezero
// are serialized even when containing only NullCount=0. Without writezero on
// the parent Statistics field, the entire struct would be skipped as "zero"
// because reflect.IsZero returns true for Statistics{NullCount: 0}.
//
// Statistics{NullCount:0} serializes to bytes [54, 0, 0] (field 3, varint 0, stop).
// We verify these bytes appear in the parent struct's serialization.
func TestWriteZeroStatisticsFields(t *testing.T) {
	protocol := &thrift.CompactProtocol{}
	// Statistics{NullCount: 0} encodes to: field delta=3, type=i64, value=0, stop
	statsBytes := []byte{54, 0, 0}

	t.Run("ColumnMetaData.Statistics", func(t *testing.T) {
		input := &format.ColumnMetaData{
			Type:                  format.Int64,
			Encoding:              []format.Encoding{format.Plain},
			PathInSchema:          []string{"col"},
			Codec:                 format.Uncompressed,
			NumValues:             1,
			TotalUncompressedSize: 8,
			TotalCompressedSize:   8,
			DataPageOffset:        0,
			Statistics:            format.Statistics{NullCount: 0},
		}
		b, err := thrift.Marshal(protocol, input)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Contains(b, statsBytes) {
			t.Errorf("Statistics{NullCount:0} not found in serialized bytes: %v", b)
		}
	})

	t.Run("DataPageHeader.Statistics", func(t *testing.T) {
		input := &format.DataPageHeader{
			NumValues:               1,
			Encoding:                format.Plain,
			DefinitionLevelEncoding: format.RLE,
			RepetitionLevelEncoding: format.RLE,
			Statistics:              format.Statistics{NullCount: 0},
		}
		b, err := thrift.Marshal(protocol, input)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Contains(b, statsBytes) {
			t.Errorf("Statistics{NullCount:0} not found in serialized bytes: %v", b)
		}
	})

	t.Run("DataPageHeaderV2.Statistics", func(t *testing.T) {
		input := &format.DataPageHeaderV2{
			NumValues:                  1,
			NumNulls:                   0,
			NumRows:                    1,
			Encoding:                   format.Plain,
			DefinitionLevelsByteLength: 0,
			RepetitionLevelsByteLength: 0,
			Statistics:                 format.Statistics{NullCount: 0},
		}
		b, err := thrift.Marshal(protocol, input)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Contains(b, statsBytes) {
			t.Errorf("Statistics{NullCount:0} not found in serialized bytes: %v", b)
		}
	})

	t.Run("ColumnIndex.NullCounts", func(t *testing.T) {
		input := &format.ColumnIndex{
			NullPages:     []bool{false},
			MinValues:     [][]byte{{1}},
			MaxValues:     [][]byte{{2}},
			BoundaryOrder: format.Ascending,
			NullCounts:    []int64{0},
		}
		b, err := thrift.Marshal(protocol, input)
		if err != nil {
			t.Fatal(err)
		}
		output := &format.ColumnIndex{}
		if err := thrift.Unmarshal(protocol, b, output); err != nil {
			t.Fatal(err)
		}
		// NullCounts []int64{0} should survive round-trip (not be nil)
		if output.NullCounts == nil {
			t.Error("NullCounts was not serialized (got nil after round-trip)")
		}
		if len(output.NullCounts) != 1 || output.NullCounts[0] != 0 {
			t.Errorf("NullCounts = %v, want [0]", output.NullCounts)
		}
	})
}

// TestWriterZeroRowGroupOrdinal verifies that the Ordinal field in RowGroup is
// serialized for the first row group whose value is zero. Without writezero,
// RowGroup{Ordinal:0} the serialized bytes [68, 0] will be omitted.
func TestWriteZeroRowGroupOrdinal(t *testing.T) {
	protocol := &thrift.CompactProtocol{}
	rg := &format.RowGroup{
		Ordinal: 0,
	}
	b, err := thrift.Marshal(protocol, rg)
	if err != nil {
		t.Fatal(err)
	}

	// | Bytes | Pos | Field	     | Value |
	// |-------|-----|---------------|-------|
	// | 25 12 |  1  | Columns       |  []   |
	// | 22  0 |  2  | TotalByteSize |   0   |
	// | 22  0 |  3  | NumRows	     |   0   |
	// | 68  0 |  7  | Ordinal	     |   0   |
	if !bytes.Equal(b, []byte{25, 12, 22, 0, 22, 0, 68, 0, 0}) {
		t.Errorf("RowGroup{Ordinal:0} not found in serialized bytes: %v", b)
	}
}

// testFiles contains parquet files ordered by footer complexity
var testFiles = []string{
	"list_columns.parquet",              // Simple baseline
	"alltypes_tiny_pages_plain.parquet", // Multiple types
	"file.parquet",                      // Medium nested complexity
	"issue368.parquet",                  // Deep nesting
	"trace.snappy.parquet",              // Complex maps+lists (~30 columns)
	"nested_structs.rust.parquet",       // Wide schema, many parallel nested groups
}

// footerCache stores pre-loaded footer bytes for each test file
var footerCache = make(map[string][]byte)

func init() {
	for _, name := range testFiles {
		path := filepath.Join("..", "testdata", name)
		data, err := extractFooterBytes(path)
		if err != nil {
			// Files might not exist in all test contexts
			continue
		}
		footerCache[name] = data
	}
}

// extractFooterBytes extracts the raw footer bytes from a parquet file.
// This follows the pattern from file.go:102-115.
func extractFooterBytes(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Get file size
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := fi.Size()

	// Read last 8 bytes (4-byte footer size + 4-byte magic "PAR1")
	trailer := make([]byte, 8)
	if _, err := f.ReadAt(trailer, size-8); err != nil {
		return nil, err
	}

	// Validate magic
	if string(trailer[4:]) != "PAR1" {
		return nil, os.ErrInvalid
	}

	// Extract footer size and read footer data
	footerSize := int64(binary.LittleEndian.Uint32(trailer[:4]))
	footerData := make([]byte, footerSize)
	if _, err := f.ReadAt(footerData, size-footerSize-8); err != nil {
		return nil, err
	}

	return footerData, nil
}

// BenchmarkDecodeFooter benchmarks footer decoding with real parquet files.
// This measures the CPU and memory cost of decoding FileMetaData from actual files.
func BenchmarkDecodeFooter(b *testing.B) {
	if len(footerCache) == 0 {
		b.Skip("no test files available")
	}

	for _, name := range testFiles {
		footerData, ok := footerCache[name]
		if !ok {
			continue
		}

		b.Run(name, func(b *testing.B) {
			// Decode once to get metadata stats for reporting
			var metadata format.FileMetaData
			protocol := &thrift.CompactProtocol{}
			if err := thrift.Unmarshal(protocol, footerData, &metadata); err != nil {
				b.Fatal(err)
			}

			// Report custom metrics
			schemaElements := len(metadata.Schema)
			rowGroups := len(metadata.RowGroups)
			columns := 0
			for _, rg := range metadata.RowGroups {
				columns += len(rg.Columns)
			}

			b.ReportMetric(float64(schemaElements), "schema_elements/op")
			b.ReportMetric(float64(rowGroups), "row_groups/op")
			b.ReportMetric(float64(columns), "columns/op")

			b.SetBytes(int64(len(footerData)))
			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				var decoded format.FileMetaData
				if err := thrift.Unmarshal(protocol, footerData, &decoded); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkDecodeFooterLarge focuses on the largest/most complex footers
// for detailed profiling. Use with -cpuprofile or -memprofile.
func BenchmarkDecodeFooterLarge(b *testing.B) {
	largeFiles := []string{
		"trace.snappy.parquet",
		"nested_structs.rust.parquet",
	}

	for _, name := range largeFiles {
		footerData, ok := footerCache[name]
		if !ok {
			continue
		}

		b.Run(name, func(b *testing.B) {
			protocol := &thrift.CompactProtocol{}
			b.SetBytes(int64(len(footerData)))
			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				var decoded format.FileMetaData
				if err := thrift.Unmarshal(protocol, footerData, &decoded); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkDecodeFooterReuse benchmarks footer decoding with struct reuse.
// This demonstrates the slice reuse optimization - reusing the same FileMetaData
// struct across multiple decodes to reduce allocations.
func BenchmarkDecodeFooterReuse(b *testing.B) {
	largeFiles := []string{
		"trace.snappy.parquet",
		"nested_structs.rust.parquet",
	}

	for _, name := range largeFiles {
		footerData, ok := footerCache[name]
		if !ok {
			continue
		}

		b.Run(name, func(b *testing.B) {
			protocol := &thrift.CompactProtocol{}
			var decoded format.FileMetaData

			// Prime the struct with one decode to allocate slices
			if err := thrift.Unmarshal(protocol, footerData, &decoded); err != nil {
				b.Fatal(err)
			}

			b.SetBytes(int64(len(footerData)))
			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				// Reuse the same struct - slices should be reused
				if err := thrift.Unmarshal(protocol, footerData, &decoded); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
