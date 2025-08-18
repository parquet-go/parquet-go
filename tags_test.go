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
