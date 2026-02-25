package thrift_test

import (
	"testing"

	"github.com/parquet-go/parquet-go/deprecated"
	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"
)

// BenchmarkDecodeSchemaElement benchmarks decoding of SchemaElement.
// SchemaElement is decoded for every column in the schema, making it high impact.
func BenchmarkDecodeSchemaElement(b *testing.B) {
	elem := format.SchemaElement{
		Type:           thrift.New(format.Int64),
		TypeLength:     thrift.New[int32](0),
		RepetitionType: thrift.New(format.Optional),
		Name:           "test_column",
		NumChildren:    thrift.New[int32](0),
		ConvertedType:  thrift.New(deprecated.Int64),
		Scale:          thrift.New[int32](0),
		Precision:      thrift.New[int32](18),
		FieldID:        1,
		LogicalType: thrift.Null[format.LogicalType]{
			V: format.LogicalType{
				Integer: &format.IntType{
					BitWidth: 64,
					IsSigned: true,
				},
			},
			Valid: true,
		},
	}

	protocol := &thrift.CompactProtocol{}
	data, err := thrift.Marshal(protocol, elem)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()

	for b.Loop() {
		var decoded format.SchemaElement
		if err := thrift.Unmarshal(protocol, data, &decoded); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDecodePageHeader benchmarks decoding of PageHeader.
// PageHeader is decoded for every page, making it high impact.
func BenchmarkDecodePageHeader(b *testing.B) {
	header := format.PageHeader{
		Type:                 format.DataPage,
		UncompressedPageSize: 4096,
		CompressedPageSize:   2048,
		CRC:                  12345,
		DataPageHeader: thrift.Null[format.DataPageHeader]{
			V: format.DataPageHeader{
				NumValues:               1000,
				Encoding:                format.Plain,
				DefinitionLevelEncoding: format.RLE,
				RepetitionLevelEncoding: format.RLE,
				Statistics: format.Statistics{
					NullCount:     10,
					DistinctCount: 100,
					MinValue:      []byte{0, 0, 0, 0},
					MaxValue:      []byte{255, 255, 255, 255},
				},
			},
			Valid: true,
		},
	}

	protocol := &thrift.CompactProtocol{}
	data, err := thrift.Marshal(protocol, header)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()

	for b.Loop() {
		var decoded format.PageHeader
		if err := thrift.Unmarshal(protocol, data, &decoded); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDecodePageHeaderV2 benchmarks decoding of PageHeader with DataPageHeaderV2.
func BenchmarkDecodePageHeaderV2(b *testing.B) {
	header := format.PageHeader{
		Type:                 format.DataPageV2,
		UncompressedPageSize: 4096,
		CompressedPageSize:   2048,
		CRC:                  12345,
		DataPageHeaderV2: thrift.Null[format.DataPageHeaderV2]{
			V: format.DataPageHeaderV2{
				NumValues:                  1000,
				NumNulls:                   10,
				NumRows:                    990,
				Encoding:                   format.Plain,
				DefinitionLevelsByteLength: 100,
				RepetitionLevelsByteLength: 100,
				IsCompressed:               thrift.New(true),
				Statistics: format.Statistics{
					NullCount:     10,
					DistinctCount: 100,
					MinValue:      []byte{0, 0, 0, 0},
					MaxValue:      []byte{255, 255, 255, 255},
				},
			},
			Valid: true,
		},
	}

	protocol := &thrift.CompactProtocol{}
	data, err := thrift.Marshal(protocol, header)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()

	for b.Loop() {
		var decoded format.PageHeader
		if err := thrift.Unmarshal(protocol, data, &decoded); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDecodeFileMetaData benchmarks decoding of FileMetaData.
// This exercises full metadata decoding including schema and row groups.
func BenchmarkDecodeFileMetaData(b *testing.B) {
	metadata := format.FileMetaData{
		Version: 2,
		Schema: []format.SchemaElement{
			{Name: "root", NumChildren: thrift.New[int32](2)},
			{
				Type:           thrift.New(format.Int64),
				RepetitionType: thrift.New(format.Required),
				Name:           "id",
				FieldID:        1,
			},
			{
				Type:           thrift.New(format.Int64),
				RepetitionType: thrift.New(format.Required),
				Name:           "value",
				FieldID:        2,
			},
		},
		NumRows: 10000,
		RowGroups: []format.RowGroup{
			{
				Columns: []format.ColumnChunk{
					{
						FileOffset: 100,
						MetaData: format.ColumnMetaData{
							Type:                  format.Int64,
							Encoding:              []format.Encoding{format.Plain, format.RLE},
							PathInSchema:          []string{"id"},
							Codec:                 format.Snappy,
							NumValues:             5000,
							TotalUncompressedSize: 40000,
							TotalCompressedSize:   20000,
							DataPageOffset:        100,
						},
					},
					{
						FileOffset: 20100,
						MetaData: format.ColumnMetaData{
							Type:                  format.Int64,
							Encoding:              []format.Encoding{format.Plain, format.RLE},
							PathInSchema:          []string{"value"},
							Codec:                 format.Snappy,
							NumValues:             5000,
							TotalUncompressedSize: 40000,
							TotalCompressedSize:   20000,
							DataPageOffset:        20100,
						},
					},
				},
				TotalByteSize: 40000,
				NumRows:       5000,
			},
			{
				Columns: []format.ColumnChunk{
					{
						FileOffset: 40100,
						MetaData: format.ColumnMetaData{
							Type:                  format.Int64,
							Encoding:              []format.Encoding{format.Plain, format.RLE},
							PathInSchema:          []string{"id"},
							Codec:                 format.Snappy,
							NumValues:             5000,
							TotalUncompressedSize: 40000,
							TotalCompressedSize:   20000,
							DataPageOffset:        40100,
						},
					},
					{
						FileOffset: 60100,
						MetaData: format.ColumnMetaData{
							Type:                  format.Int64,
							Encoding:              []format.Encoding{format.Plain, format.RLE},
							PathInSchema:          []string{"value"},
							Codec:                 format.Snappy,
							NumValues:             5000,
							TotalUncompressedSize: 40000,
							TotalCompressedSize:   20000,
							DataPageOffset:        60100,
						},
					},
				},
				TotalByteSize: 40000,
				NumRows:       5000,
			},
		},
		CreatedBy: "parquet-go test",
	}

	protocol := &thrift.CompactProtocol{}
	data, err := thrift.Marshal(protocol, metadata)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()

	for b.Loop() {
		var decoded format.FileMetaData
		if err := thrift.Unmarshal(protocol, data, &decoded); err != nil {
			b.Fatal(err)
		}
	}
}
