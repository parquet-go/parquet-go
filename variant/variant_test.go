package variant

import (
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestPrimitiveRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		val  Value
	}{
		{"Null", Null()},
		{"True", Bool(true)},
		{"False", Bool(false)},
		{"Int8", Int8(-42)},
		{"Int16", Int16(-1234)},
		{"Int32", Int32(123456)},
		{"Int64", Int64(1234567890123)},
		{"Float", Float(3.14)},
		{"Double", Double(2.718281828)},
		{"ShortString", String("hello")},
		{"EmptyString", String("")},
		{"LongString", String(strings.Repeat("x", 64))},
		{"MaxShortString", String(strings.Repeat("a", 63))},
		{"Binary", Binary([]byte{0xDE, 0xAD, 0xBE, 0xEF})},
		{"EmptyBinary", Binary([]byte{})},
		{"Date", Date(19000)},
		{"Timestamp", Timestamp(1700000000000000)},
		{"TimestampNTZ", TimestampNTZ(1700000000000000)},
		{"Time", Time(43200000000)},
		{"UUID", UUID(uuid.MustParse("550e8400-e29b-41d4-a716-446655440000"))},
		{"Decimal4", Decimal4(12345, 2)},
		{"Decimal8", Decimal8(123456789012, 4)},
		{"Decimal16", Decimal16([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}, 6)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var b MetadataBuilder
			encoded := Encode(&b, tt.val)
			meta, metaBytes := b.Build()

			// Also test that we can decode the metadata
			decodedMeta, err := DecodeMetadata(metaBytes)
			if err != nil {
				t.Fatalf("DecodeMetadata: %v", err)
			}
			_ = decodedMeta

			decoded, err := Decode(meta, encoded)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}

			if !decoded.Equal(tt.val) {
				t.Errorf("round-trip failed: got %v, want %v", decoded.GoValue(), tt.val.GoValue())
			}
		})
	}
}

func TestObjectRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		val  Value
	}{
		{
			"SimpleObject",
			MakeObject([]Field{
				{Name: "name", Value: String("Alice")},
				{Name: "age", Value: Int32(30)},
			}),
		},
		{
			"NestedObject",
			MakeObject([]Field{
				{Name: "user", Value: MakeObject([]Field{
					{Name: "id", Value: Int64(1)},
					{Name: "email", Value: String("alice@example.com")},
				})},
				{Name: "active", Value: Bool(true)},
			}),
		},
		{
			"UnicodeKeys",
			MakeObject([]Field{
				{Name: "名前", Value: String("太郎")},
				{Name: "年齢", Value: Int32(25)},
			}),
		},
		{
			"EmptyObject",
			MakeObject([]Field{}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var b MetadataBuilder
			encoded := Encode(&b, tt.val)
			meta, metaBytes := b.Build()

			decodedMeta, err := DecodeMetadata(metaBytes)
			if err != nil {
				t.Fatalf("DecodeMetadata: %v", err)
			}

			decoded, err := Decode(decodedMeta, encoded)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}

			// Objects may be reordered during encode (sorted by name).
			// Compare as Go values (maps).
			_ = meta
			got := decoded.GoValue()
			want := tt.val.GoValue()
			if !mapsEqual(got, want) {
				t.Errorf("round-trip failed:\ngot:  %v\nwant: %v", got, want)
			}
		})
	}
}

func TestArrayRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		val  Value
	}{
		{
			"SimpleArray",
			MakeArray([]Value{Int32(1), Int32(2), Int32(3)}),
		},
		{
			"MixedArray",
			MakeArray([]Value{String("hello"), Int64(42), Bool(true), Null()}),
		},
		{
			"NestedArray",
			MakeArray([]Value{
				MakeArray([]Value{Int32(1), Int32(2)}),
				MakeArray([]Value{Int32(3), Int32(4)}),
			}),
		},
		{
			"EmptyArray",
			MakeArray([]Value{}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var b MetadataBuilder
			encoded := Encode(&b, tt.val)
			meta, _ := b.Build()

			decoded, err := Decode(meta, encoded)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}

			if !decoded.Equal(tt.val) {
				t.Errorf("round-trip failed: got %v, want %v", decoded.GoValue(), tt.val.GoValue())
			}
		})
	}
}

func TestMetadataDictionary(t *testing.T) {
	t.Run("InsertionOrder", func(t *testing.T) {
		var b MetadataBuilder
		b.Add("charlie")
		b.Add("alice")
		b.Add("bob")

		meta, metaBytes := b.Build()
		if meta.Sorted {
			t.Fatal("expected unsorted metadata (insertion order)")
		}

		decoded, err := DecodeMetadata(metaBytes)
		if err != nil {
			t.Fatalf("DecodeMetadata: %v", err)
		}

		expected := []string{"charlie", "alice", "bob"}
		for i, s := range decoded.Strings {
			if s != expected[i] {
				t.Errorf("string[%d] = %q, want %q", i, s, expected[i])
			}
		}
	})

	t.Run("SortedWhenAlreadySorted", func(t *testing.T) {
		var b MetadataBuilder
		b.Add("alice")
		b.Add("bob")
		b.Add("charlie")

		meta, _ := b.Build()
		if !meta.Sorted {
			t.Fatal("expected sorted metadata when strings are added in order")
		}
	})

	t.Run("Deduplication", func(t *testing.T) {
		var b MetadataBuilder
		idx1 := b.Add("hello")
		idx2 := b.Add("world")
		idx3 := b.Add("hello")
		if idx1 != idx3 {
			t.Errorf("expected same index for duplicate: %d != %d", idx1, idx3)
		}
		if idx1 == idx2 {
			t.Error("expected different indices for different strings")
		}
	})

	t.Run("LargeDictionary", func(t *testing.T) {
		var b MetadataBuilder
		// Add enough strings to require 2-byte offsets
		for i := range 300 {
			b.Add(strings.Repeat("x", 100) + string(rune(i+'A')))
		}
		meta, metaBytes := b.Build()

		decoded, err := DecodeMetadata(metaBytes)
		if err != nil {
			t.Fatalf("DecodeMetadata: %v", err)
		}
		if len(decoded.Strings) != 300 {
			t.Errorf("expected 300 strings, got %d", len(decoded.Strings))
		}
		_ = meta
	})

	t.Run("Reset", func(t *testing.T) {
		var b MetadataBuilder
		b.Add("hello")
		b.Reset()
		idx := b.Add("world")
		if idx != 0 {
			t.Errorf("expected index 0 after reset, got %d", idx)
		}
	})
}

func TestShortStringBoundary(t *testing.T) {
	// Short strings can be at most 63 bytes
	for _, length := range []int{0, 1, 62, 63, 64, 100} {
		s := strings.Repeat("a", length)
		val := String(s)

		var b MetadataBuilder
		encoded := Encode(&b, val)
		meta, _ := b.Build()

		decoded, err := Decode(meta, encoded)
		if err != nil {
			t.Fatalf("length %d: Decode: %v", length, err)
		}

		// Check encoding type
		basic := BasicType(encoded[0] & 0x03)
		if length <= 63 {
			if basic != BasicShortString {
				t.Errorf("length %d: expected short string encoding, got basic=%d", length, basic)
			}
		} else {
			if basic != BasicPrimitive {
				t.Errorf("length %d: expected primitive string encoding, got basic=%d", length, basic)
			}
		}

		if decoded.Str() != s {
			t.Errorf("length %d: got %q, want %q", length, decoded.Str(), s)
		}
	}
}

func TestLargeObjectOffsets(t *testing.T) {
	// Create an object with values large enough to need multi-byte offsets
	fields := make([]Field, 10)
	for i := range fields {
		// Each value is a large binary blob
		fields[i] = Field{
			Name:  string(rune('a' + i)),
			Value: Binary(make([]byte, 10000)),
		}
	}
	val := MakeObject(fields)

	var b MetadataBuilder
	encoded := Encode(&b, val)
	meta, _ := b.Build()

	decoded, err := Decode(meta, encoded)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	got := decoded.GoValue()
	want := val.GoValue()
	if !mapsEqual(got, want) {
		t.Error("round-trip of large object failed")
	}
}

// mapsEqual compares two Go values recursively.
func mapsEqual(a, b any) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	am, aOk := a.(map[string]any)
	bm, bOk := b.(map[string]any)
	if aOk && bOk {
		if len(am) != len(bm) {
			return false
		}
		for k, av := range am {
			bv, ok := bm[k]
			if !ok || !mapsEqual(av, bv) {
				return false
			}
		}
		return true
	}
	as, aOk := a.([]any)
	bs, bOk := b.([]any)
	if aOk && bOk {
		if len(as) != len(bs) {
			return false
		}
		for i := range as {
			if !mapsEqual(as[i], bs[i]) {
				return false
			}
		}
		return true
	}

	aBytes, aOk := a.([]byte)
	bBytes, bOk := b.([]byte)
	if aOk && bOk {
		if len(aBytes) != len(bBytes) {
			return false
		}
		for i := range aBytes {
			if aBytes[i] != bBytes[i] {
				return false
			}
		}
		return true
	}

	return a == b
}

// TestPrimitiveTypeWireValues pins primitive type header bytes against the spec.
// Objects and arrays are excluded — their headers encode structural metadata,
// not a fixed type_info. See TestObjectRoundTrip and TestArrayRoundTrip.
func TestPrimitiveTypeWireValues(t *testing.T) {
	tests := []struct {
		name          string
		val           Value
		wantTypeInfo  byte // bits [7:2] of the header byte per spec
		wantBasicType byte // bits [1:0] of the header byte per spec
	}{
		{"null", Null(), 0, 0},
		{"true", Bool(true), 1, 0},
		{"false", Bool(false), 2, 0},
		{"int8", Int8(1), 3, 0},
		{"int16", Int16(1), 4, 0},
		{"int32", Int32(1), 5, 0},
		{"int64", Int64(1), 6, 0},
		{"double", Double(1.0), 7, 0},
		{"decimal4", Decimal4(1, 0), 8, 0},
		{"decimal8", Decimal8(1, 0), 9, 0},
		{"date", Date(1), 11, 0},
		{"float", Float(1.0), 14, 0},
		{"binary", Binary([]byte{1}), 15, 0},
		{"string_short", String("hi"), 2, 1},                    // short string: basic_type=1, type_info=len(2)
		{"string_long", String(strings.Repeat("x", 64)), 16, 0}, // long string: basic_type=0, type_info=16
		{"uuid", UUID([16]byte{}), 20, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var b MetadataBuilder
			encoded := Encode(&b, tt.val)
			if len(encoded) == 0 {
				t.Fatal("encoded value is empty")
			}
			header := encoded[0]
			gotBasicType := header & 0x03
			gotTypeInfo := (header >> 2) & 0x3F

			if gotBasicType != tt.wantBasicType {
				t.Errorf("basic_type: got %d, want %d", gotBasicType, tt.wantBasicType)
			}
			if gotTypeInfo != tt.wantTypeInfo {
				t.Errorf("type_info: got %d, want %d (header=0x%02x)", gotTypeInfo, tt.wantTypeInfo, header)
			}
		})
	}
}
