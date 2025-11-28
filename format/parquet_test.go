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
