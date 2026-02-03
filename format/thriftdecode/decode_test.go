package thriftdecode

import (
	"bytes"
	"fmt"
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

// FileMetaData test helpers

func makeSchemaElement(name string, isGroup bool) format.SchemaElement {
	se := format.SchemaElement{
		Name: name,
	}
	if isGroup {
		numChildren := int32(2)
		se.NumChildren = &numChildren
		rt := format.Required
		se.RepetitionType = &rt
	} else {
		t := format.Int64
		se.Type = &t
		rt := format.Optional
		se.RepetitionType = &rt
		// Add a logical type
		se.LogicalType = &format.LogicalType{
			Timestamp: &format.TimestampType{
				IsAdjustedToUTC: true,
				Unit: format.TimeUnit{
					Micros: &format.MicroSeconds{},
				},
			},
		}
	}
	return se
}

func makeColumnMetaData(path []string, numValues int64) format.ColumnMetaData {
	return format.ColumnMetaData{
		Type:                  format.Int64,
		Encoding:              []format.Encoding{format.Plain, format.RLE},
		PathInSchema:          path,
		Codec:                 format.Snappy,
		NumValues:             numValues,
		TotalUncompressedSize: numValues * 8,
		TotalCompressedSize:   numValues * 6,
		DataPageOffset:        1000,
		Statistics: format.Statistics{
			NullCount:     10,
			DistinctCount: 100,
			MinValue:      []byte{0, 0, 0, 0, 0, 0, 0, 1},
			MaxValue:      []byte{0, 0, 0, 0, 0, 0, 0, 100},
		},
	}
}

func makeColumnChunk(path []string, numValues int64) format.ColumnChunk {
	return format.ColumnChunk{
		FileOffset:        4,
		MetaData:          makeColumnMetaData(path, numValues),
		OffsetIndexOffset: 5000,
		OffsetIndexLength: 100,
		ColumnIndexOffset: 6000,
		ColumnIndexLength: 200,
	}
}

func makeRowGroup(numColumns int, numRows int64) format.RowGroup {
	columns := make([]format.ColumnChunk, numColumns)
	for i := range columns {
		columns[i] = makeColumnChunk([]string{"root", fmt.Sprintf("col%d", i)}, numRows)
	}
	return format.RowGroup{
		Columns:             columns,
		TotalByteSize:       numRows * int64(numColumns) * 8,
		NumRows:             numRows,
		TotalCompressedSize: numRows * int64(numColumns) * 6,
		Ordinal:             0,
	}
}

func makeFileMetaData(numRowGroups, numColumns int, numRowsPerGroup int64) format.FileMetaData {
	// Create schema: root + numColumns leaf columns
	schema := make([]format.SchemaElement, 1+numColumns)
	schema[0] = makeSchemaElement("root", true)
	numChildren := int32(numColumns)
	schema[0].NumChildren = &numChildren
	for i := 1; i <= numColumns; i++ {
		schema[i] = makeSchemaElement(fmt.Sprintf("col%d", i-1), false)
	}

	rowGroups := make([]format.RowGroup, numRowGroups)
	for i := range rowGroups {
		rowGroups[i] = makeRowGroup(numColumns, numRowsPerGroup)
		rowGroups[i].Ordinal = int16(i)
	}

	return format.FileMetaData{
		Version:   2,
		Schema:    schema,
		NumRows:   numRowsPerGroup * int64(numRowGroups),
		RowGroups: rowGroups,
		KeyValueMetadata: []format.KeyValue{
			{Key: "created_by", Value: "test"},
			{Key: "version", Value: "1.0"},
		},
		CreatedBy: "parquet-go test",
		ColumnOrders: []format.ColumnOrder{
			{TypeOrder: &format.TypeDefinedOrder{}},
		},
	}
}

func TestDecodeFileMetaData(t *testing.T) {
	original := makeFileMetaData(2, 3, 1000)
	data := encodeThrift(&original)

	var decoded format.FileMetaData
	err := DecodeFileMetaData(data, &decoded)
	if err != nil {
		t.Fatalf("DecodeFileMetaData failed: %v", err)
	}

	// Check basic fields
	if decoded.Version != original.Version {
		t.Errorf("Version: got %d, want %d", decoded.Version, original.Version)
	}
	if decoded.NumRows != original.NumRows {
		t.Errorf("NumRows: got %d, want %d", decoded.NumRows, original.NumRows)
	}
	if decoded.CreatedBy != original.CreatedBy {
		t.Errorf("CreatedBy: got %q, want %q", decoded.CreatedBy, original.CreatedBy)
	}

	// Check schema
	if len(decoded.Schema) != len(original.Schema) {
		t.Fatalf("Schema length: got %d, want %d", len(decoded.Schema), len(original.Schema))
	}
	for i := range original.Schema {
		if decoded.Schema[i].Name != original.Schema[i].Name {
			t.Errorf("Schema[%d].Name: got %q, want %q", i, decoded.Schema[i].Name, original.Schema[i].Name)
		}
	}

	// Check row groups
	if len(decoded.RowGroups) != len(original.RowGroups) {
		t.Fatalf("RowGroups length: got %d, want %d", len(decoded.RowGroups), len(original.RowGroups))
	}
	for i := range original.RowGroups {
		if decoded.RowGroups[i].NumRows != original.RowGroups[i].NumRows {
			t.Errorf("RowGroups[%d].NumRows: got %d, want %d", i, decoded.RowGroups[i].NumRows, original.RowGroups[i].NumRows)
		}
		if len(decoded.RowGroups[i].Columns) != len(original.RowGroups[i].Columns) {
			t.Errorf("RowGroups[%d].Columns length: got %d, want %d", i,
				len(decoded.RowGroups[i].Columns), len(original.RowGroups[i].Columns))
		}
	}

	// Check key-value metadata
	if len(decoded.KeyValueMetadata) != len(original.KeyValueMetadata) {
		t.Fatalf("KeyValueMetadata length: got %d, want %d",
			len(decoded.KeyValueMetadata), len(original.KeyValueMetadata))
	}
	for i := range original.KeyValueMetadata {
		if decoded.KeyValueMetadata[i].Key != original.KeyValueMetadata[i].Key {
			t.Errorf("KeyValueMetadata[%d].Key: got %q, want %q", i,
				decoded.KeyValueMetadata[i].Key, original.KeyValueMetadata[i].Key)
		}
		if decoded.KeyValueMetadata[i].Value != original.KeyValueMetadata[i].Value {
			t.Errorf("KeyValueMetadata[%d].Value: got %q, want %q", i,
				decoded.KeyValueMetadata[i].Value, original.KeyValueMetadata[i].Value)
		}
	}
}

func TestDecodeSchemaElementWithLogicalTypes(t *testing.T) {
	testCases := []struct {
		name        string
		logicalType *format.LogicalType
	}{
		{
			name: "String",
			logicalType: &format.LogicalType{
				UTF8: &format.StringType{},
			},
		},
		{
			name: "Decimal",
			logicalType: &format.LogicalType{
				Decimal: &format.DecimalType{Scale: 2, Precision: 10},
			},
		},
		{
			name: "Timestamp",
			logicalType: &format.LogicalType{
				Timestamp: &format.TimestampType{
					IsAdjustedToUTC: true,
					Unit:            format.TimeUnit{Micros: &format.MicroSeconds{}},
				},
			},
		},
		{
			name: "Time",
			logicalType: &format.LogicalType{
				Time: &format.TimeType{
					IsAdjustedToUTC: false,
					Unit:            format.TimeUnit{Millis: &format.MilliSeconds{}},
				},
			},
		},
		{
			name: "Integer",
			logicalType: &format.LogicalType{
				Integer: &format.IntType{BitWidth: 32, IsSigned: true},
			},
		},
		{
			name: "Date",
			logicalType: &format.LogicalType{
				Date: &format.DateType{},
			},
		},
		{
			name: "UUID",
			logicalType: &format.LogicalType{
				UUID: &format.UUIDType{},
			},
		},
		{
			name: "JSON",
			logicalType: &format.LogicalType{
				Json: &format.JsonType{},
			},
		},
		{
			name: "Map",
			logicalType: &format.LogicalType{
				Map: &format.MapType{},
			},
		},
		{
			name: "List",
			logicalType: &format.LogicalType{
				List: &format.ListType{},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			typ := format.Int64
			rt := format.Optional
			original := format.SchemaElement{
				Type:           &typ,
				RepetitionType: &rt,
				Name:           "test_field",
				LogicalType:    tc.logicalType,
			}
			data := encodeThrift(&original)

			var decoded format.SchemaElement
			err := (&buffer{data: data}).decodeSchemaElement(&decoded)
			if err != nil {
				t.Fatalf("decodeSchemaElement failed: %v", err)
			}

			if decoded.Name != original.Name {
				t.Errorf("Name: got %q, want %q", decoded.Name, original.Name)
			}
			if decoded.LogicalType == nil {
				t.Fatal("LogicalType is nil")
			}
		})
	}
}

func TestDecodeColumnMetaData(t *testing.T) {
	original := makeColumnMetaData([]string{"root", "col1"}, 1000)
	data := encodeThrift(&original)

	var decoded format.ColumnMetaData
	err := (&buffer{data: data}).decodeColumnMetaData(&decoded)
	if err != nil {
		t.Fatalf("decodeColumnMetaData failed: %v", err)
	}

	if decoded.Type != original.Type {
		t.Errorf("Type: got %d, want %d", decoded.Type, original.Type)
	}
	if decoded.NumValues != original.NumValues {
		t.Errorf("NumValues: got %d, want %d", decoded.NumValues, original.NumValues)
	}
	if decoded.Codec != original.Codec {
		t.Errorf("Codec: got %d, want %d", decoded.Codec, original.Codec)
	}
	if len(decoded.PathInSchema) != len(original.PathInSchema) {
		t.Errorf("PathInSchema length: got %d, want %d", len(decoded.PathInSchema), len(original.PathInSchema))
	}
	if decoded.Statistics.NullCount != original.Statistics.NullCount {
		t.Errorf("Statistics.NullCount: got %d, want %d", decoded.Statistics.NullCount, original.Statistics.NullCount)
	}
}

// Benchmarks for FileMetaData

func BenchmarkDecodeFileMetaData_Thrift(b *testing.B) {
	original := makeFileMetaData(10, 20, 100000) // 10 row groups, 20 columns
	data := encodeThrift(&original)
	protocol := &thrift.CompactProtocol{}

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		var decoded format.FileMetaData
		if err := thrift.Unmarshal(protocol, data, &decoded); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeFileMetaData_Optimized(b *testing.B) {
	original := makeFileMetaData(10, 20, 100000) // 10 row groups, 20 columns
	data := encodeThrift(&original)

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		var decoded format.FileMetaData
		if err := DecodeFileMetaData(data, &decoded); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeFileMetaData_Large_Thrift(b *testing.B) {
	// Simulates a larger file with many row groups and columns
	original := makeFileMetaData(100, 50, 1000000) // 100 row groups, 50 columns
	data := encodeThrift(&original)
	protocol := &thrift.CompactProtocol{}

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		var decoded format.FileMetaData
		if err := thrift.Unmarshal(protocol, data, &decoded); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeFileMetaData_Large_Optimized(b *testing.B) {
	// Simulates a larger file with many row groups and columns
	original := makeFileMetaData(100, 50, 1000000) // 100 row groups, 50 columns
	data := encodeThrift(&original)

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		var decoded format.FileMetaData
		if err := DecodeFileMetaData(data, &decoded); err != nil {
			b.Fatal(err)
		}
	}
}

// PageHeader test helpers

func makeDataPageHeader(numValues int32) *format.DataPageHeader {
	return &format.DataPageHeader{
		NumValues:               numValues,
		Encoding:                format.Plain,
		DefinitionLevelEncoding: format.RLE,
		RepetitionLevelEncoding: format.RLE,
		Statistics: format.Statistics{
			NullCount:     10,
			DistinctCount: 100,
			MinValue:      []byte{1, 2, 3, 4},
			MaxValue:      []byte{5, 6, 7, 8},
		},
	}
}

func makeDataPageHeaderV2(numValues, numNulls, numRows int32) *format.DataPageHeaderV2 {
	isCompressed := true
	return &format.DataPageHeaderV2{
		NumValues:                  numValues,
		NumNulls:                   numNulls,
		NumRows:                    numRows,
		Encoding:                   format.Plain,
		DefinitionLevelsByteLength: 100,
		RepetitionLevelsByteLength: 50,
		IsCompressed:               &isCompressed,
		Statistics: format.Statistics{
			NullCount: int64(numNulls),
			MinValue:  []byte{1, 2, 3, 4},
			MaxValue:  []byte{5, 6, 7, 8},
		},
	}
}

func makeDictionaryPageHeader(numValues int32) *format.DictionaryPageHeader {
	return &format.DictionaryPageHeader{
		NumValues: numValues,
		Encoding:  format.Plain,
		IsSorted:  true,
	}
}

func makePageHeader(pageType format.PageType) format.PageHeader {
	h := format.PageHeader{
		Type:                 pageType,
		UncompressedPageSize: 4096,
		CompressedPageSize:   2048,
		CRC:                  12345678,
	}
	switch pageType {
	case format.DataPage:
		h.DataPageHeader = makeDataPageHeader(1000)
	case format.DataPageV2:
		h.DataPageHeaderV2 = makeDataPageHeaderV2(1000, 10, 100)
	case format.DictionaryPage:
		h.DictionaryPageHeader = makeDictionaryPageHeader(500)
	}
	return h
}

func TestDecodePageHeader_DataPage(t *testing.T) {
	original := makePageHeader(format.DataPage)
	data := encodeThrift(&original)

	decoder := NewPageHeaderDecoder(bytes.NewReader(data))
	var decoded format.PageHeader
	if err := decoder.Decode(&decoded); err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.Type != original.Type {
		t.Errorf("Type: got %d, want %d", decoded.Type, original.Type)
	}
	if decoded.UncompressedPageSize != original.UncompressedPageSize {
		t.Errorf("UncompressedPageSize: got %d, want %d", decoded.UncompressedPageSize, original.UncompressedPageSize)
	}
	if decoded.CompressedPageSize != original.CompressedPageSize {
		t.Errorf("CompressedPageSize: got %d, want %d", decoded.CompressedPageSize, original.CompressedPageSize)
	}
	if decoded.CRC != original.CRC {
		t.Errorf("CRC: got %d, want %d", decoded.CRC, original.CRC)
	}
	if decoded.DataPageHeader == nil {
		t.Fatal("DataPageHeader is nil")
	}
	if decoded.DataPageHeader.NumValues != original.DataPageHeader.NumValues {
		t.Errorf("DataPageHeader.NumValues: got %d, want %d",
			decoded.DataPageHeader.NumValues, original.DataPageHeader.NumValues)
	}
	if decoded.DataPageHeader.Encoding != original.DataPageHeader.Encoding {
		t.Errorf("DataPageHeader.Encoding: got %d, want %d",
			decoded.DataPageHeader.Encoding, original.DataPageHeader.Encoding)
	}
}

func TestDecodePageHeader_DataPageV2(t *testing.T) {
	original := makePageHeader(format.DataPageV2)
	data := encodeThrift(&original)

	decoder := NewPageHeaderDecoder(bytes.NewReader(data))
	var decoded format.PageHeader
	if err := decoder.Decode(&decoded); err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.Type != original.Type {
		t.Errorf("Type: got %d, want %d", decoded.Type, original.Type)
	}
	if decoded.DataPageHeaderV2 == nil {
		t.Fatal("DataPageHeaderV2 is nil")
	}
	if decoded.DataPageHeaderV2.NumValues != original.DataPageHeaderV2.NumValues {
		t.Errorf("DataPageHeaderV2.NumValues: got %d, want %d",
			decoded.DataPageHeaderV2.NumValues, original.DataPageHeaderV2.NumValues)
	}
	if decoded.DataPageHeaderV2.NumRows != original.DataPageHeaderV2.NumRows {
		t.Errorf("DataPageHeaderV2.NumRows: got %d, want %d",
			decoded.DataPageHeaderV2.NumRows, original.DataPageHeaderV2.NumRows)
	}
	if decoded.DataPageHeaderV2.IsCompressed == nil || *decoded.DataPageHeaderV2.IsCompressed != *original.DataPageHeaderV2.IsCompressed {
		t.Errorf("DataPageHeaderV2.IsCompressed: got %v, want %v",
			decoded.DataPageHeaderV2.IsCompressed, original.DataPageHeaderV2.IsCompressed)
	}
}

func TestDecodePageHeader_DictionaryPage(t *testing.T) {
	original := makePageHeader(format.DictionaryPage)
	data := encodeThrift(&original)

	decoder := NewPageHeaderDecoder(bytes.NewReader(data))
	var decoded format.PageHeader
	if err := decoder.Decode(&decoded); err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.Type != original.Type {
		t.Errorf("Type: got %d, want %d", decoded.Type, original.Type)
	}
	if decoded.DictionaryPageHeader == nil {
		t.Fatal("DictionaryPageHeader is nil")
	}
	if decoded.DictionaryPageHeader.NumValues != original.DictionaryPageHeader.NumValues {
		t.Errorf("DictionaryPageHeader.NumValues: got %d, want %d",
			decoded.DictionaryPageHeader.NumValues, original.DictionaryPageHeader.NumValues)
	}
	if decoded.DictionaryPageHeader.IsSorted != original.DictionaryPageHeader.IsSorted {
		t.Errorf("DictionaryPageHeader.IsSorted: got %v, want %v",
			decoded.DictionaryPageHeader.IsSorted, original.DictionaryPageHeader.IsSorted)
	}
}

func BenchmarkDecodePageHeader_Thrift(b *testing.B) {
	original := makePageHeader(format.DataPage)
	data := encodeThrift(&original)
	protocol := &thrift.CompactProtocol{}
	reader := bytes.NewReader(data)

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		reader.Reset(data)
		var decoded format.PageHeader
		if err := thrift.Unmarshal(protocol, data, &decoded); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodePageHeader_Optimized(b *testing.B) {
	original := makePageHeader(format.DataPage)
	data := encodeThrift(&original)
	reader := bytes.NewReader(data)
	decoder := NewPageHeaderDecoder(reader)

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		reader.Reset(data)
		decoder.Reset(reader)
		var decoded format.PageHeader
		if err := decoder.Decode(&decoded); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodePageHeader_DataPageV2_Thrift(b *testing.B) {
	original := makePageHeader(format.DataPageV2)
	data := encodeThrift(&original)
	protocol := &thrift.CompactProtocol{}
	reader := bytes.NewReader(data)

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		reader.Reset(data)
		var decoded format.PageHeader
		if err := thrift.Unmarshal(protocol, data, &decoded); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodePageHeader_DataPageV2_Optimized(b *testing.B) {
	original := makePageHeader(format.DataPageV2)
	data := encodeThrift(&original)
	reader := bytes.NewReader(data)
	decoder := NewPageHeaderDecoder(reader)

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		reader.Reset(data)
		decoder.Reset(reader)
		var decoded format.PageHeader
		if err := decoder.Decode(&decoded); err != nil {
			b.Fatal(err)
		}
	}
}

// BloomFilterHeader tests

func makeBloomFilterHeader() format.BloomFilterHeader {
	return format.BloomFilterHeader{
		NumBytes: 1024,
		Algorithm: format.BloomFilterAlgorithm{
			Block: &format.SplitBlockAlgorithm{},
		},
		Hash: format.BloomFilterHash{
			XxHash: &format.XxHash{},
		},
		Compression: format.BloomFilterCompression{
			Uncompressed: &format.BloomFilterUncompressed{},
		},
	}
}

func TestDecodeBloomFilterHeader(t *testing.T) {
	original := makeBloomFilterHeader()
	data := encodeThrift(&original)

	decoder := NewBloomFilterHeaderDecoder(bytes.NewReader(data))
	var decoded format.BloomFilterHeader
	if err := decoder.Decode(&decoded); err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.NumBytes != original.NumBytes {
		t.Errorf("NumBytes: got %d, want %d", decoded.NumBytes, original.NumBytes)
	}
	if decoded.Algorithm.Block == nil {
		t.Error("Algorithm.Block is nil")
	}
	if decoded.Hash.XxHash == nil {
		t.Error("Hash.XxHash is nil")
	}
	if decoded.Compression.Uncompressed == nil {
		t.Error("Compression.Uncompressed is nil")
	}
}

func BenchmarkDecodeBloomFilterHeader_Thrift(b *testing.B) {
	original := makeBloomFilterHeader()
	data := encodeThrift(&original)
	protocol := &thrift.CompactProtocol{}
	reader := bytes.NewReader(data)

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		reader.Reset(data)
		var decoded format.BloomFilterHeader
		if err := thrift.Unmarshal(protocol, data, &decoded); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeBloomFilterHeader_Optimized(b *testing.B) {
	original := makeBloomFilterHeader()
	data := encodeThrift(&original)
	reader := bytes.NewReader(data)
	decoder := NewBloomFilterHeaderDecoder(reader)

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		reader.Reset(data)
		decoder.Reset(reader)
		var decoded format.BloomFilterHeader
		if err := decoder.Decode(&decoded); err != nil {
			b.Fatal(err)
		}
	}
}
