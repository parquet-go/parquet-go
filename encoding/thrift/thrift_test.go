package thrift_test

import (
	"bytes"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go/encoding/thrift"
)

var marshalTestValues = [...]struct {
	scenario string
	values   []any
}{
	{
		scenario: "bool",
		values:   []any{false, true},
	},

	{
		scenario: "int",
		values: []any{
			int(0),
			int(-1),
			int(1),
		},
	},

	{
		scenario: "int8",
		values: []any{
			int8(0),
			int8(-1),
			int8(1),
			int8(math.MinInt8),
			int8(math.MaxInt8),
		},
	},

	{
		scenario: "int16",
		values: []any{
			int16(0),
			int16(-1),
			int16(1),
			int16(math.MinInt16),
			int16(math.MaxInt16),
		},
	},

	{
		scenario: "int32",
		values: []any{
			int32(0),
			int32(-1),
			int32(1),
			int32(math.MinInt32),
			int32(math.MaxInt32),
		},
	},

	{
		scenario: "int64",
		values: []any{
			int64(0),
			int64(-1),
			int64(1),
			int64(math.MinInt64),
			int64(math.MaxInt64),
		},
	},

	{
		scenario: "string",
		values: []any{
			"",
			"A",
			"1234567890",
			strings.Repeat("qwertyuiop", 100),
		},
	},

	{
		scenario: "[]byte",
		values: []any{
			[]byte(""),
			[]byte("A"),
			[]byte("1234567890"),
			bytes.Repeat([]byte("qwertyuiop"), 100),
		},
	},

	{
		scenario: "[]string",
		values: []any{
			[]string{},
			[]string{"A"},
			[]string{"hello", "world", "!!!"},
			[]string{"0", "1", "3", "4", "5", "6", "7", "8", "9"},
		},
	},

	{
		scenario: "map[string]int",
		values: []any{
			map[string]int{},
			map[string]int{"A": 1},
			map[string]int{"hello": 1, "world": 2, "answer": 42},
		},
	},

	{
		scenario: "map[int64]struct{}",
		values: []any{
			map[int64]struct{}{},
			map[int64]struct{}{0: {}, 1: {}, 2: {}},
		},
	},

	{
		scenario: "[]map[string]struct{}",
		values: []any{
			[]map[string]struct{}{},
			[]map[string]struct{}{{}, {"A": {}, "B": {}, "C": {}}},
		},
	},

	{
		scenario: "struct{}",
		values:   []any{struct{}{}},
	},

	{
		scenario: "Point2D",
		values: []any{
			Point2D{},
			Point2D{X: 1},
			Point2D{Y: 2},
			Point2D{X: 3, Y: 4},
		},
	},

	{
		scenario: "RecursiveStruct",
		values: []any{
			RecursiveStruct{},
			RecursiveStruct{Value: "hello"},
			RecursiveStruct{Value: "hello", Next: &RecursiveStruct{}},
			RecursiveStruct{Value: "hello", Next: &RecursiveStruct{Value: "world", Test: newBool(true)}},
		},
	},

	{
		scenario: "StructWithEnum",
		values: []any{
			StructWithEnum{},
			StructWithEnum{Enum: 1},
			StructWithEnum{Enum: 2},
		},
	},

	{
		scenario: "StructWithPointToPointerToBool",
		values: []any{
			StructWithPointerToPointerToBool{
				Test: newBoolPtr(true),
			},
		},
	},

	{
		scenario: "StructWithEmbeddedStrutPointerWithPointerToPointer",
		values: []any{
			StructWithEmbeddedStrutPointerWithPointerToPointer{
				StructWithPointerToPointerToBool: &StructWithPointerToPointerToBool{
					Test: newBoolPtr(true),
				},
			},
		},
	},

	{
		scenario: "Union",
		values: []any{
			Union{},
			Union{Value: &UnionBool{V: true}},
			Union{Value: &UnionInt{V: 42}},
			Union{Value: &UnionString{V: "hello world!"}},
			Union{Value: &UnionEmpty{}},
		},
	},
}

type Point2D struct {
	X float64 `thrift:"1,required"`
	Y float64 `thrift:"2,required"`
}

type RecursiveStruct struct {
	Value string           `thrift:"1"`
	Next  *RecursiveStruct `thrift:"2"`
	Test  *bool            `thrift:"3"`
}

type StructWithEnum struct {
	Enum int8 `thrift:"1,enum"`
}

type StructWithPointerToPointerToBool struct {
	Test **bool `thrift:"1"`
}

type StructWithEmbeddedStrutPointerWithPointerToPointer struct {
	*StructWithPointerToPointerToBool
}

// UnionValue is the member interface of Union. The FieldID method that
// thrift.UnionMember requires is what closes the interface: bare types cannot
// satisfy it by accident.
type UnionValue interface {
	thrift.UnionMember
}

type Union struct {
	Value UnionValue
}

var unionMembers = []thrift.UnionMember{
	(*UnionBool)(nil),
	(*UnionInt)(nil),
	(*UnionString)(nil),
	(*UnionEmpty)(nil),
}

func (*Union) UnionMembers() []thrift.UnionMember { return unionMembers }

type UnionBool struct {
	V bool `thrift:"1"`
}

type UnionInt struct {
	V int64 `thrift:"1"`
}

type UnionString struct {
	V string `thrift:"1"`
}

// UnionEmpty is zero-sized, so the codec hands out an interned instance rather
// than allocating one per decode.
type UnionEmpty struct{}

func (*UnionBool) FieldID() int16   { return 1 }
func (*UnionInt) FieldID() int16    { return 2 }
func (*UnionString) FieldID() int16 { return 3 }
func (*UnionEmpty) FieldID() int16  { return 4 }

func newBool(b bool) *bool       { return &b }
func newInt(i int) *int          { return &i }
func newString(s string) *string { return &s }

func newBoolPtr(b bool) **bool {
	p := newBool(b)
	return &p
}

func TestMarshalUnmarshal(t *testing.T) {
	for _, p := range protocols {
		t.Run(p.name, func(t *testing.T) { testMarshalUnmarshal(t, p.proto) })
	}
}

func testMarshalUnmarshal(t *testing.T, p thrift.Protocol) {
	for _, test := range marshalTestValues {
		t.Run(test.scenario, func(t *testing.T) {
			for _, value := range test.values {
				b, err := thrift.Marshal(p, value)
				if err != nil {
					t.Fatal("marshal:", err)
				}

				v := reflect.New(reflect.TypeOf(value))
				if err := thrift.Unmarshal(p, b, v.Interface()); err != nil {
					t.Fatal("unmarshal:", err)
				}

				if result := v.Elem().Interface(); !reflect.DeepEqual(value, result) {
					t.Errorf("value mismatch:\nwant: %#v\ngot:  %#v", value, result)
				}
			}
		})
	}
}

func BenchmarkMarshal(b *testing.B) {
	for _, p := range protocols {
		b.Run(p.name, func(b *testing.B) { benchmarkMarshal(b, p.proto) })
	}
}

type BenchmarkEncodeType struct {
	Name     string               `thrift:"1"`
	Question string               `thrift:"2"`
	Answer   string               `thrift:"3"`
	Sub      *BenchmarkEncodeType `thrift:"4"`
}

func benchmarkMarshal(b *testing.B, p thrift.Protocol) {
	buf := new(bytes.Buffer)
	enc := thrift.NewEncoder(p.NewWriter(buf))
	val := &BenchmarkEncodeType{
		Name:     "Luke",
		Question: "How are you?",
		Answer:   "42",
		Sub: &BenchmarkEncodeType{
			Name:     "Leia",
			Question: "?",
			Answer:   "whatever",
		},
	}

	for b.Loop() {
		buf.Reset()
		enc.Encode(val)
	}

	b.SetBytes(int64(buf.Len()))
}

func BenchmarkUnmarshal(b *testing.B) {
	for _, p := range protocols {
		b.Run(p.name, func(b *testing.B) { benchmarkUnmarshal(b, p.proto) })
	}
}

type BenchmarkDecodeType struct {
	Name     string               `thrift:"1"`
	Question string               `thrift:"2"`
	Answer   string               `thrift:"3"`
	Sub      *BenchmarkDecodeType `thrift:"4"`
}

func benchmarkUnmarshal(b *testing.B, p thrift.Protocol) {
	buf, _ := thrift.Marshal(p, &BenchmarkDecodeType{
		Name:     "Luke",
		Question: "How are you?",
		Answer:   "42",
		Sub: &BenchmarkDecodeType{
			Name:     "Leia",
			Question: "?",
			Answer:   "whatever",
		},
	})

	rb := bytes.NewReader(nil)
	dec := thrift.NewDecoder(p.NewReader(rb))
	val := &BenchmarkDecodeType{}

	for b.Loop() {
		rb.Reset(buf)
		dec.Decode(val)
	}

	b.SetBytes(int64(len(buf)))
}
