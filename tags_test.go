package parquet

import (
	"reflect"
	"testing"
)

func TestFromStructTag(t *testing.T) {
	tests := []struct {
		structTag reflect.StructTag
		expected  parquetTags
	}{
		{
			structTag: reflect.StructTag("parquet:\"-,\""),
			expected: parquetTags{
				parquet: "-,",
			},
		},
		{
			structTag: reflect.StructTag("parquet:\"name,optional\""),
			expected: parquetTags{
				parquet: "name,optional",
			},
		},
		{
			structTag: reflect.StructTag("parquet:\",\" parquet-key:\",timestamp\""),
			expected: parquetTags{
				parquet:    ",",
				parquetKey: ",timestamp",
			},
		},
		{
			structTag: reflect.StructTag("parquet:\",optional\" parquet-value:\",json\" parquet-key:\",timestamp(microsecond)\""),
			expected: parquetTags{
				parquet:      ",optional",
				parquetValue: ",json",
				parquetKey:   ",timestamp(microsecond)",
			},
		},
		{
			structTag: reflect.StructTag("parquet:\"outer,id(1),list\" parquet-element:\",id(2)\""),
			expected: parquetTags{
				parquet:        "outer,id(1),list",
				parquetElement: ",id(2)",
			},
		},
	}

	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			actual := fromStructTag(test.structTag)
			if actual != test.expected {
				t.Errorf("expected %v, got %v", test.expected, actual)
			}
		})
	}
}

func TestGetNodeTags(t *testing.T) {
	tests := []struct {
		input       parquetTags
		getNodeTags func(parquetTags) parquetTags
		expected    parquetTags
	}{
		{
			input: parquetTags{
				parquet: ",optional",
			},
			getNodeTags: func(p parquetTags) parquetTags {
				return p.getMapKeyNodeTags()
			},
			expected: parquetTags{
				parquet: "",
			},
		},
		{
			input: parquetTags{
				parquet:    ",optional",
				parquetKey: ",timestamp",
			},
			getNodeTags: func(p parquetTags) parquetTags {
				return p.getMapKeyNodeTags()
			},
			expected: parquetTags{
				parquet: ",timestamp",
			},
		},
		{
			input: parquetTags{
				parquet:      ",optional",
				parquetValue: ",json",
			},
			getNodeTags: func(p parquetTags) parquetTags {
				return p.getMapValueNodeTags()
			},
			expected: parquetTags{
				parquet: ",json",
			},
		},
		{
			input: parquetTags{
				parquet:        ",id(1),list",
				parquetElement: ",id(2)",
			},
			getNodeTags: func(p parquetTags) parquetTags {
				return p.getListElementNodeTags()
			},
			expected: parquetTags{
				parquet: ",id(2)",
			},
		},
	}

	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			actual := test.getNodeTags(test.input)
			if actual != test.expected {
				t.Errorf("expected %v, got %v", test.expected, actual)
			}
		})
	}
}

func TestProtoFieldNameFromTag(t *testing.T) {
	tests := []struct {
		name      string
		structTag reflect.StructTag
		expected  string
	}{
		{
			name:      "standard protobuf tag",
			structTag: reflect.StructTag(`protobuf:"bytes,1,opt,name=user_id,proto3"`),
			expected:  "user_id",
		},
		{
			name:      "protobuf tag with json field",
			structTag: reflect.StructTag(`protobuf:"bytes,2,opt,name=first_name,json=firstName,proto3"`),
			expected:  "first_name",
		},
		{
			name:      "protobuf tag with varint type",
			structTag: reflect.StructTag(`protobuf:"varint,3,opt,name=age,proto3"`),
			expected:  "age",
		},
		{
			name:      "no protobuf tag",
			structTag: reflect.StructTag(`json:"user_id"`),
			expected:  "",
		},
		{
			name:      "empty protobuf tag",
			structTag: reflect.StructTag(`protobuf:""`),
			expected:  "",
		},
		{
			name:      "protobuf tag without name field",
			structTag: reflect.StructTag(`protobuf:"bytes,1,opt,proto3"`),
			expected:  "",
		},
		{
			name:      "protobuf tag with parquet tag (parquet takes precedence via schema logic)",
			structTag: reflect.StructTag(`protobuf:"bytes,1,opt,name=proto_name,proto3" parquet:"parquet_name"`),
			expected:  "proto_name",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := protoFieldNameFromTag(test.structTag)
			if actual != test.expected {
				t.Errorf("expected %q, got %q", test.expected, actual)
			}
		})
	}
}
