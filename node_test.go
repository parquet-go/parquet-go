package parquet

import (
	"reflect"
	"testing"

	"github.com/parquet-go/parquet-go/compress"
	"github.com/parquet-go/parquet-go/encoding"
)

type mockNode struct {
	id          int
	str         string
	typ         func() Type
	optional    bool
	repeated    bool
	required    bool
	leaf        bool
	fields      func() []Field
	enc         func() encoding.Encoding
	compression func() compress.Codec
	goType      func() reflect.Type
}

func (m *mockNode) ID() int                     { return m.id }
func (m *mockNode) String() string              { return m.str }
func (m *mockNode) Type() Type                  { return m.typ() }
func (m *mockNode) Optional() bool              { return m.optional }
func (m *mockNode) Repeated() bool              { return m.repeated }
func (m *mockNode) Required() bool              { return m.required }
func (m *mockNode) Leaf() bool                  { return m.leaf }
func (m *mockNode) Fields() []Field             { return m.fields() }
func (m *mockNode) Encoding() encoding.Encoding { return m.enc() }
func (m *mockNode) Compression() compress.Codec { return m.compression() }
func (m *mockNode) GoType() reflect.Type        { return m.goType() }

func TestEncodingOf(t *testing.T) {
	testCases := []struct {
		name             string
		node             Node
		defaultEncodings map[Kind]encoding.Encoding
		expectedEncoding encoding.Encoding
	}{
		{
			name: "node with encoding, no default",
			node: &mockNode{
				enc: func() encoding.Encoding { return &DeltaBinaryPacked },
				typ: func() Type { return Int32Type },
			},
			defaultEncodings: nil,
			expectedEncoding: &DeltaBinaryPacked,
		},
		{
			name: "node with encoding, different default",
			node: &mockNode{
				enc: func() encoding.Encoding { return &DeltaBinaryPacked },
				typ: func() Type { return Int32Type },
			},
			defaultEncodings: map[Kind]encoding.Encoding{
				Int32: &Plain,
			},
			expectedEncoding: &DeltaBinaryPacked,
		},
		{
			name: "node without encoding, with default",
			node: &mockNode{
				enc: func() encoding.Encoding { return nil },
				typ: func() Type { return ByteArrayType },
			},
			defaultEncodings: map[Kind]encoding.Encoding{
				ByteArray: &DeltaByteArray,
			},
			expectedEncoding: &DeltaByteArray,
		},
		{
			name: "node without encoding, no default, not ByteArray",
			node: &mockNode{
				enc: func() encoding.Encoding { return nil },
				typ: func() Type { return Int32Type },
			},
			defaultEncodings: nil,
			expectedEncoding: &Plain,
		},
		{
			name: "node without encoding, no default, ByteArray",
			node: &mockNode{
				enc: func() encoding.Encoding { return nil },
				typ: func() Type { return ByteArrayType },
			},
			defaultEncodings: nil,
			expectedEncoding: &DeltaLengthByteArray,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			enc := encodingOf(tc.node, tc.defaultEncodings)
			if enc == nil {
				t.Error("encodingOf returned nil")
			}
			if enc.String() != tc.expectedEncoding.String() {
				t.Errorf("encodingOf returned %s, but expected %s",
					enc.String(), tc.expectedEncoding.String())
			}
		})
	}
}
