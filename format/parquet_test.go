package format_test

import (
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
