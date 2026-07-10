package parquet

import (
	"errors"
	"testing"

	"github.com/parquet-go/parquet-go/encoding"
)

// corruptPage simulates a data page whose definition levels declare more
// non-null values than the value section decoded, as produced by corrupt
// BYTE_ARRAY, dictionary-encoded, or data page v2 pages that the generic
// page decoding layer does not cross-check.
type corruptPage struct {
	Page
	defs []byte
	data encoding.Values
}

func (p *corruptPage) DefinitionLevels() []byte { return p.defs }
func (p *corruptPage) RepetitionLevels() []byte { return nil }
func (p *corruptPage) Dictionary() Dictionary   { return nil }
func (p *corruptPage) Data() encoding.Values    { return p.data }

// TestVariantLeafReaderCorruptPage verifies that setPage rejects pages whose
// decoded value count falls short of the definition levels, instead of
// letting appendValue index past the decoded data.
func TestVariantLeafReaderCorruptPage(t *testing.T) {
	defs := []byte{1, 1, 1, 1, 1, 1, 1, 1, 1, 1} // 10 non-null slots

	tests := []struct {
		name string
		kind Kind
		data encoding.Values
	}{
		{name: "int32", kind: Int32, data: encoding.Int32Values(make([]int32, 5))},
		{name: "byte_array", kind: ByteArray, data: encoding.ByteArrayValues(nil, []uint32{0, 0, 0, 0, 0, 0})},
		{name: "byte_array_empty", kind: ByteArray, data: encoding.ByteArrayValues(nil, nil)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			l := &variantLeafReader{reader: &VariantReader{}, kind: test.kind, maxDef: 1}
			err := l.setPage(&corruptPage{defs: defs, data: test.data})
			if !errors.Is(err, ErrCorrupted) {
				t.Fatalf("setPage = %v, want ErrCorrupted", err)
			}
		})
	}

	// A page with the declared number of values must pass.
	l := &variantLeafReader{reader: &VariantReader{}, kind: Int32, maxDef: 1}
	if err := l.setPage(&corruptPage{defs: defs, data: encoding.Int32Values(make([]int32, 10))}); err != nil {
		t.Fatalf("setPage on valid page: %v", err)
	}
}
