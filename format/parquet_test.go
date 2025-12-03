package format_test

import (
	"bytes"
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
