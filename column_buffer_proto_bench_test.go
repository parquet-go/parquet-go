//go:build !purego

package parquet

import (
	"fmt"
	"io"
	"reflect"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// protoFallbackFieldCount is the number of present optional scalars on the
// fallback write path measured below. Each unconditional reflect.New was one
// allocation.
const protoFallbackFieldCount = 8

// BenchmarkWriteProtoFallback measures writing one proto row through the
// fallback writer, where the schema is not the schema of the Go type.
// NonZero values used to be wrapped just like zeros.
func BenchmarkWriteProtoFallback(b *testing.B) {
	schema := optionalInt32Schema(protoFallbackFieldCount)
	for _, tc := range []struct {
		name  string
		value func(int) int32
	}{
		{name: "NonZero", value: func(i int) int32 { return int32(i + 1) }},
		{name: "Zero", value: func(int) int32 { return 0 }},
	} {
		b.Run(tc.name, func(b *testing.B) {
			row, err := optionalInt32Message(protoFallbackFieldCount, tc.value)
			if err != nil {
				b.Fatal(err)
			}
			rows := []proto.Message{row}
			w := NewGenericWriter[proto.Message](io.Discard, schema)
			if _, err := w.Write(rows); err != nil {
				b.Fatal(err)
			}
			w.Reset(io.Discard)

			b.ReportAllocs()
			for b.Loop() {
				if _, err := w.Write(rows); err != nil {
					b.Fatal(err)
				}
				w.Reset(io.Discard)
			}
		})
	}
}

// Present non-zero scalars must not pay for the pointer allocated to keep a
// zero from being written as null. Zeros still need that pointer.
func TestProtoFallbackNonZeroSkipsNullWrap(t *testing.T) {
	schema := optionalInt32Schema(protoFallbackFieldCount)
	nonzero, err := optionalInt32Message(protoFallbackFieldCount, func(i int) int32 { return int32(i + 1) })
	if err != nil {
		t.Fatal(err)
	}
	zero, err := optionalInt32Message(protoFallbackFieldCount, func(int) int32 { return 0 })
	if err != nil {
		t.Fatal(err)
	}
	unset, err := optionalInt32Message(protoFallbackFieldCount, nil)
	if err != nil {
		t.Fatal(err)
	}

	got := writtenColumnStrings(t, schema, []proto.Message{zero, nonzero, unset})
	want := make(map[string][]string, protoFallbackFieldCount)
	for i := range protoFallbackFieldCount {
		want[fmt.Sprintf("f%d", i)] = []string{"0", fmt.Sprint(i + 1), "<null>"}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v, want %#v", got, want)
	}

	nonzeroAllocs := allocsPerProtoWrite(t, schema, nonzero)
	zeroAllocs := allocsPerProtoWrite(t, schema, zero)
	if zeroAllocs-nonzeroAllocs < protoFallbackFieldCount {
		t.Fatalf("non-zero present fields allocate like zeros: zero %v, non-zero %v", zeroAllocs, nonzeroAllocs)
	}
}

func allocsPerProtoWrite(t *testing.T, schema *Schema, row proto.Message) float64 {
	t.Helper()
	rows := []proto.Message{row}
	w := NewGenericWriter[proto.Message](io.Discard, schema)
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	w.Reset(io.Discard)
	return testing.AllocsPerRun(100, func() {
		if _, err := w.Write(rows); err != nil {
			t.Fatal(err)
		}
		w.Reset(io.Discard)
	})
}

func optionalInt32Schema(n int) *Schema {
	group := make(Group, n)
	for i := range n {
		group[fmt.Sprintf("f%d", i)] = Optional(Int(32))
	}
	return NewSchema("row", group)
}

func optionalInt32Message(n int, value func(int) int32) (proto.Message, error) {
	fields := make([]*descriptorpb.FieldDescriptorProto, n)
	for i := range fields {
		fields[i] = &descriptorpb.FieldDescriptorProto{
			Name:   new(fmt.Sprintf("f%d", i)),
			Number: new(int32(i + 1)),
			Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
			Type:   descriptorpb.FieldDescriptorProto_TYPE_INT32.Enum(),
		}
	}
	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name:    new("optional_int32.proto"),
		Package: new("bench"),
		Syntax:  new("proto2"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name:  new("Row"),
			Field: fields,
		}},
	}, nil)
	if err != nil {
		return nil, err
	}
	md := file.Messages().Get(0)
	msg := dynamicpb.NewMessage(md)
	if value == nil {
		return msg, nil
	}
	for i := range n {
		msg.Set(md.Fields().Get(i), protoreflect.ValueOfInt32(value(i)))
	}
	return msg, nil
}
