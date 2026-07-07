package variant

import (
	"bytes"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
)

// TestMarshalMatchesValueOf pins the Go-to-variant type mapping and the
// invariant that the package's two conversion paths implement it
// identically: Marshal encodes through the streaming encoder
// (encodeReflect), while ValueOf builds a Value tree (goToVariantReflect)
// that callers such as the shredded variant writer encode later. If the two
// ever map a Go type differently, the same Go value would produce different
// variant data depending on whether the column is shredded.
//
// Each case declares the expected Value; ValueOf must produce it, and
// Marshal must produce byte-identical output to encoding it. Multi-key maps
// are exercised separately in TestMarshalMatchesValueOfMaps: Go map
// iteration order makes dictionary ids nondeterministic on both paths, so
// byte equality does not hold for them.
func TestMarshalMatchesValueOf(t *testing.T) {
	type tagged struct {
		A int32  `variant:"renamed"`
		B string `json:"js,omitempty"`
		C []byte `variant:"-"`
		d bool
	}
	_ = tagged{}.d // unexported fields must be skipped, not error
	type derived16 [16]byte
	type namedByte byte
	type namedElem16 [16]namedByte

	u := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")
	subMicro := time.Date(2024, 3, 1, 12, 0, 0, 123456789, time.UTC)
	wholeMicro := time.Date(2024, 3, 1, 12, 0, 0, 123456000, time.UTC)
	// Sub-microsecond precision outside the nanosecond timestamp range
	// (years 1678-2261) must fall back to microseconds rather than
	// overflow UnixNano.
	beforeNanoRange := time.Date(1677, 12, 31, 0, 0, 0, 1, time.UTC)
	afterNanoRange := time.Date(2262, 1, 1, 0, 0, 0, 1, time.UTC)
	inNanoRangeLow := time.Date(1678, 1, 1, 0, 0, 0, 1, time.UTC)
	inNanoRangeHigh := time.Date(2261, 12, 31, 0, 0, 0, 1, time.UTC)

	i := 7
	longString := string(bytes.Repeat([]byte("a"), 100))
	values := map[string]struct {
		input any
		want  Value
	}{
		"nil":         {nil, Null()},
		"nil_pointer": {(*int)(nil), Null()},
		"pointer":     {&i, Int64(7)},
		"bool":        {true, Bool(true)},
		"int8":        {int8(-1), Int8(-1)},
		"int16":       {int16(-2), Int16(-2)},
		"int32":       {int32(-3), Int32(-3)},
		"int64":       {int64(-4), Int64(-4)},
		"int":         {5, Int64(5)},
		// Unsigned types widen to the next signed type that holds them.
		"uint8":        {uint8(200), Int16(200)},
		"uint16":       {uint16(60000), Int32(60000)},
		"uint32":       {uint32(4e9), Int64(4e9)},
		"uint64":       {uint64(math.MaxInt64), Int64(math.MaxInt64)},
		"float32":      {float32(1.5), Float(1.5)},
		"float64":      {2.5, Double(2.5)},
		"string_short": {"hello", String("hello")},
		"string_long":  {longString, String(longString)},
		"bytes":        {[]byte{1, 2, 3}, Binary([]byte{1, 2, 3})},
		// [16]byte arrays (including uuid.UUID and types derived from it)
		// map to the UUID primitive, matching the FIXED_LEN_BYTE_ARRAY(16)
		// semantics of shredded UUID columns; other arrays stay arrays.
		"uuid":           {u, UUID(u)},
		"byte_array_16":  {[16]byte(u), UUID(u)},
		"derived_16":     {derived16(u), UUID(u)},
		"named_elem_16":  {namedElem16{0x55, 0x0e, 0x84}, UUID(uuid.UUID{0x55, 0x0e, 0x84})},
		"array_non_byte": {[3]int32{1, 2, 3}, MakeArray([]Value{Int32(1), Int32(2), Int32(3)})},
		// time.Time maps to UTC-adjusted timestamps: nanosecond precision
		// only when the value has sub-microsecond components and fits the
		// nanosecond range, microsecond precision otherwise.
		"time_sub_micro":    {subMicro, TimestampNanos(subMicro.UnixNano())},
		"time_whole_micro":  {wholeMicro, Timestamp(wholeMicro.UnixMicro())},
		"time_before_range": {beforeNanoRange, Timestamp(beforeNanoRange.UnixMicro())},
		"time_after_range":  {afterNanoRange, Timestamp(afterNanoRange.UnixMicro())},
		"time_range_low":    {inNanoRangeLow, TimestampNanos(inNanoRangeLow.UnixNano())},
		"time_range_high":   {inNanoRangeHigh, TimestampNanos(inNanoRangeHigh.UnixNano())},
		"slice": {[]any{int32(1), "two", nil}, MakeArray([]Value{
			Int32(1), String("two"), Null(),
		})},
		"slice_empty": {[]string{}, MakeArray(nil)},
		"map_nil":     {map[string]any(nil), Null()},
		"map_empty":   {map[string]any{}, MakeObject(nil)},
		"map_single":  {map[string]any{"k": int64(9)}, MakeObject([]Field{{Name: "k", Value: Int64(9)}})},
		// Struct fields honor variant and json tags; "-" and unexported
		// fields are dropped.
		"struct": {tagged{A: 42, B: "b", C: []byte("dropped")}, MakeObject([]Field{
			{Name: "renamed", Value: Int32(42)},
			{Name: "js", Value: String("b")},
		})},
		"struct_nested": {struct {
			Inner tagged
			List  []int64
		}{Inner: tagged{A: 1}, List: []int64{1, 2}}, MakeObject([]Field{
			{Name: "Inner", Value: MakeObject([]Field{
				{Name: "renamed", Value: Int32(1)},
				{Name: "js", Value: String("")},
			})},
			{Name: "List", Value: MakeArray([]Value{Int64(1), Int64(2)})},
		})},
	}

	for name, tt := range values {
		t.Run(name, func(t *testing.T) {
			v, err := ValueOf(tt.input)
			if err != nil {
				t.Fatalf("ValueOf: %v", err)
			}
			if !v.Equal(tt.want) {
				t.Fatalf("ValueOf mapping:\n got: %#v\nwant: %#v", v.GoValue(), tt.want.GoValue())
			}

			gotMeta, gotValue, err := Marshal(tt.input)
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			var b MetadataBuilder
			wantValue := Encode(&b, v)
			_, wantMeta := b.Build()

			if !bytes.Equal(gotValue, wantValue) {
				t.Errorf("value bytes differ:\nMarshal:         %x\nEncode(ValueOf): %x", gotValue, wantValue)
			}
			if !bytes.Equal(gotMeta, wantMeta) {
				t.Errorf("metadata bytes differ:\nMarshal:         %x\nEncode(ValueOf): %x", gotMeta, wantMeta)
			}
		})
	}
}

// TestMarshalMatchesValueOfErrors verifies the two paths reject the same
// inputs: a value only one path errors on would make shredded and
// unshredded writes of the same row diverge.
func TestMarshalMatchesValueOfErrors(t *testing.T) {
	values := map[string]any{
		"uint64_overflow":    uint64(1 << 63),
		"uint_overflow_map":  map[string]any{"k": uint64(1 << 63)},
		"unsupported_type":   make(chan int),
		"non_string_map_key": map[int]any{1: "v"},
		"nested_unsupported": []any{[]any{complex(1, 2)}},
	}
	for name, input := range values {
		t.Run(name, func(t *testing.T) {
			if _, _, err := Marshal(input); err == nil {
				t.Errorf("Marshal accepted %T", input)
			}
			if _, err := ValueOf(input); err == nil {
				t.Errorf("ValueOf accepted %T", input)
			}
		})
	}
}

// TestMarshalMatchesValueOfMaps compares the two paths structurally for
// multi-key maps, where dictionary id assignment depends on map iteration
// order and byte equality does not hold.
func TestMarshalMatchesValueOfMaps(t *testing.T) {
	input := map[string]any{
		"z": int64(1),
		"a": "s",
		"m": map[string]any{"x": nil, "y": []any{int8(3)}},
	}

	gotMeta, gotValue, err := Marshal(input)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	m, err := DecodeMetadata(gotMeta)
	if err != nil {
		t.Fatalf("DecodeMetadata: %v", err)
	}
	got, err := Decode(m, gotValue)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	want, err := ValueOf(input)
	if err != nil {
		t.Fatalf("ValueOf: %v", err)
	}
	if !got.Equal(want) {
		t.Errorf("Marshal and ValueOf disagree:\n got: %#v\nwant: %#v", got.GoValue(), want.GoValue())
	}
}
