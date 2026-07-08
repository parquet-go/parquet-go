package variant

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestMarshalUnmarshalPrimitives(t *testing.T) {
	tests := []struct {
		name  string
		input any
	}{
		{"nil", nil},
		{"true", true},
		{"false", false},
		{"int8", int8(-10)},
		{"int16", int16(-1000)},
		{"int32", int32(100000)},
		{"int64", int64(1234567890123)},
		// int marshals as Int64, tested separately in TestMarshalUnmarshalInt
		{"float32", float32(3.14)},
		{"float64", float64(2.718281828)},
		{"string", "hello world"},
		{"bytes", []byte{1, 2, 3}},
		{"uuid", uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			meta, val, err := Marshal(tt.input)
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}

			got, err := Unmarshal(meta, val)
			if err != nil {
				t.Fatalf("Unmarshal: %v", err)
			}

			if !mapsEqual(got, tt.input) {
				t.Errorf("round-trip: got %v (%T), want %v (%T)", got, got, tt.input, tt.input)
			}
		})
	}
}

func TestMarshalUnmarshalInt(t *testing.T) {
	// int is marshaled as Int64, so unmarshal gives int64
	meta, val, err := Marshal(42)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	got, err := Unmarshal(meta, val)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if got != int64(42) {
		t.Errorf("got %v (%T), want int64(42)", got, got)
	}
}

// TestMarshalUnmarshalUnsignedInts verifies that unsigned integers marshal
// to the narrowest signed variant type that can hold their value, since the
// variant format has no unsigned types.
func TestMarshalUnmarshalUnsignedInts(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected any
	}{
		{"uint8", uint8(200), int16(200)},
		{"uint16", uint16(60000), int32(60000)},
		{"uint32", uint32(4000000000), int64(4000000000)},
		{"uint64_small", uint64(1000), int64(1000)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			meta, val, err := Marshal(tt.input)
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}

			got, err := Unmarshal(meta, val)
			if err != nil {
				t.Fatalf("Unmarshal: %v", err)
			}

			if got != tt.expected {
				t.Errorf("got %v (%T), want %v (%T)", got, got, tt.expected, tt.expected)
			}
		})
	}
}

func TestMarshalUnmarshalMap(t *testing.T) {
	input := map[string]any{
		"name": "Alice",
		"age":  int32(30),
		"tags": []any{"admin", "user"},
	}

	meta, val, err := Marshal(input)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	got, err := Unmarshal(meta, val)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if !mapsEqual(got, input) {
		t.Errorf("round-trip: got %v, want %v", got, input)
	}
}

func TestMarshalUnmarshalSlice(t *testing.T) {
	input := []any{int32(1), "two", true, nil}

	meta, val, err := Marshal(input)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	got, err := Unmarshal(meta, val)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if !mapsEqual(got, input) {
		t.Errorf("round-trip: got %v, want %v", got, input)
	}
}

// TestMarshalStruct verifies that struct fields map to object fields named
// by their `variant` or `json` tags, that unexported fields are omitted,
// and that nested structs become nested objects.
func TestMarshalStruct(t *testing.T) {
	type Address struct {
		Street string `json:"street"`
		City   string `json:"city"`
	}
	type Person struct {
		Name    string  `variant:"name"`
		Age     int32   `json:"age"`
		Address Address `variant:"address"`
		hidden  int     //nolint:unused
	}

	input := Person{
		Name: "Bob",
		Age:  25,
		Address: Address{
			Street: "123 Main St",
			City:   "Springfield",
		},
	}

	meta, val, err := Marshal(input)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	got, err := Unmarshal(meta, val)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	expected := map[string]any{
		"name": "Bob",
		"age":  int32(25),
		"address": map[string]any{
			"street": "123 Main St",
			"city":   "Springfield",
		},
	}

	if !mapsEqual(got, expected) {
		t.Errorf("struct marshal: got %v, want %v", got, expected)
	}
}

func TestMarshalStructSkipField(t *testing.T) {
	type S struct {
		Visible string `json:"visible"`
		Hidden  string `json:"-"`
	}

	input := S{Visible: "yes", Hidden: "no"}
	meta, val, err := Marshal(input)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	got, err := Unmarshal(meta, val)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	m, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("expected map, got %T", got)
	}
	if _, exists := m["Hidden"]; exists {
		t.Error("hidden field should not be present")
	}
	if m["visible"] != "yes" {
		t.Errorf("visible = %v, want yes", m["visible"])
	}
}

// TestMarshalTimeTime verifies that time.Time maps to timestamp primitives.
// time.Time has no exported fields, so the reflection-based marshaler used
// to encode it as an empty object — silent data loss. Instants with whole
// microseconds map to timestamp(MICROS, UTC) (type 12); sub-microsecond
// instants in the nanos-representable range map to timestamp(NANOS, UTC)
// (type 18).
func TestMarshalTimeTime(t *testing.T) {
	tests := []struct {
		name     string
		input    time.Time
		wantType PrimitiveType
	}{
		{
			name:     "microsecond precision",
			input:    time.Date(2025, 4, 16, 12, 34, 56, 780_000_000, time.UTC),
			wantType: PrimitiveTimestamp,
		},
		{
			name:     "nanosecond precision",
			input:    time.Date(2024, 11, 7, 12, 33, 54, 123_456_789, time.UTC),
			wantType: PrimitiveTimestampNanos,
		},
		{
			name:     "non-UTC zone keeps the instant",
			input:    time.Date(2025, 4, 16, 12, 34, 56, 0, time.FixedZone("EDT", -4*3600)),
			wantType: PrimitiveTimestamp,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			meta, val, err := Marshal(tt.input)
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			m, err := DecodeMetadata(meta)
			if err != nil {
				t.Fatalf("DecodeMetadata: %v", err)
			}
			decoded, err := Decode(m, val)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if decoded.Type() != tt.wantType {
				t.Fatalf("primitive type = %d, want %d", decoded.Type(), tt.wantType)
			}
			got, ok := decoded.GoValue().(time.Time)
			if !ok {
				t.Fatalf("GoValue() = %T, want time.Time", decoded.GoValue())
			}
			if !got.Equal(tt.input) {
				t.Errorf("round-trip instant = %v, want %v", got, tt.input)
			}
		})
	}
}

func TestMarshalNested(t *testing.T) {
	input := map[string]any{
		"outer": map[string]any{
			"inner": map[string]any{
				"value": int32(42),
			},
		},
	}

	meta, val, err := Marshal(input)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	got, err := Unmarshal(meta, val)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if !mapsEqual(got, input) {
		t.Errorf("nested round-trip: got %v, want %v", got, input)
	}
}
