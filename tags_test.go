package parquet

import (
	"reflect"
	"testing"
)

func TestFromStructTag(t *testing.T) {
	tests := []struct {
		structTag reflect.StructTag
		expected  ParquetTags
	}{
		{
			structTag: reflect.StructTag("parquet:\"-,\""),
			expected: ParquetTags{
				Parquet: "-,",
			},
		},
		{
			structTag: reflect.StructTag("parquet:\"name,optional\""),
			expected: ParquetTags{
				Parquet: "name,optional",
			},
		},
		{
			structTag: reflect.StructTag("parquet:\",\" parquet-key:\",timestamp\""),
			expected: ParquetTags{
				Parquet:    ",",
				ParquetKey: ",timestamp",
			},
		},
		{
			structTag: reflect.StructTag("parquet:\",optional\" parquet-value:\",json\" parquet-key:\",timestamp(microsecond)\""),
			expected: ParquetTags{
				Parquet:      ",optional",
				ParquetValue: ",json",
				ParquetKey:   ",timestamp(microsecond)",
			},
		},
		{
			structTag: reflect.StructTag("parquet:\"outer,id(1),list\" parquet-element:\",id(2)\""),
			expected: ParquetTags{
				Parquet:        "outer,id(1),list",
				ParquetElement: ",id(2)",
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
		input       ParquetTags
		getNodeTags func(ParquetTags) ParquetTags
		expected    ParquetTags
	}{
		{
			input: ParquetTags{
				Parquet: ",optional",
			},
			getNodeTags: func(p ParquetTags) ParquetTags {
				return p.getMapKeyNodeTags()
			},
			expected: ParquetTags{
				Parquet: "",
			},
		},
		{
			input: ParquetTags{
				Parquet:    ",optional",
				ParquetKey: ",timestamp",
			},
			getNodeTags: func(p ParquetTags) ParquetTags {
				return p.getMapKeyNodeTags()
			},
			expected: ParquetTags{
				Parquet: ",timestamp",
			},
		},
		{
			input: ParquetTags{
				Parquet:      ",optional",
				ParquetValue: ",json",
			},
			getNodeTags: func(p ParquetTags) ParquetTags {
				return p.getMapValueNodeTags()
			},
			expected: ParquetTags{
				Parquet: ",json",
			},
		},
		{
			input: ParquetTags{
				Parquet:        ",id(1),list",
				ParquetElement: ",id(2)",
			},
			getNodeTags: func(p ParquetTags) ParquetTags {
				return p.getListElementNodeTags()
			},
			expected: ParquetTags{
				Parquet: ",id(2)",
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
