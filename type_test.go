package parquet_test

import (
	"testing"

	"github.com/parquet-go/parquet-go"
)

func TestLogicalTypesEqual(t *testing.T) {
	tests := []struct {
		a, b parquet.Node
	}{
		{parquet.Leaf(parquet.Int32Type), parquet.Int(32)},
		{parquet.Leaf(parquet.Int64Type), parquet.Int(64)},
	}

	for _, test := range tests {
		eq := parquet.EqualNodes(test.a, test.b)
		if !eq {
			t.Errorf("expected %v to be equal to %v", test.a, test.b)
		}
	}
}
