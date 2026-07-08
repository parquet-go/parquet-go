package variant

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/google/uuid"
)

// TestReplayDifferential checks that replaying encoded bytes as events
// produces the same value as decoding them: for every test value, the
// events Replay emits into a Builder must round-trip to the original.
func TestReplayDifferential(t *testing.T) {
	values := builderTestValues()
	r := rand.New(rand.NewPCG(19, 23))
	for range 50 {
		values = append(values, randomBuilderValue(r, 0))
	}

	for i, v := range values {
		t.Run(fmt.Sprintf("value_%02d", i), func(t *testing.T) {
			var mb MetadataBuilder
			encoded := Encode(&mb, v)
			m, err := DecodeMetadata(mb.AppendTo(nil))
			if err != nil {
				t.Fatalf("decoding metadata: %v", err)
			}

			var b Builder
			if err := Replay(&b, m, encoded); err != nil {
				t.Fatalf("Replay: %v", err)
			}
			metadata, value, err := b.Finish()
			if err != nil {
				t.Fatalf("Finish: %v", err)
			}
			m2, err := DecodeMetadata(metadata)
			if err != nil {
				t.Fatalf("decoding replayed metadata: %v", err)
			}
			got, err := Decode(m2, value)
			if err != nil {
				t.Fatalf("decoding replayed value: %v", err)
			}
			if !got.Equal(v) {
				t.Fatalf("replay mismatch:\n got: %#v\nwant: %#v", got.GoValue(), v.GoValue())
			}
		})
	}
}

// TestReplayObjectFields checks the field filtering and the non-object
// result.
func TestReplayObjectFields(t *testing.T) {
	v := MakeObject([]Field{
		{Name: "a", Value: Int64(1)},
		{Name: "b", Value: String("skipped")},
		{Name: "c", Value: MakeArray([]Value{Bool(true)})},
	})
	var mb MetadataBuilder
	encoded := Encode(&mb, v)
	m, err := DecodeMetadata(mb.AppendTo(nil))
	if err != nil {
		t.Fatal(err)
	}

	var b Builder
	b.BeginObject()
	ok, err := ReplayObjectFields(&b, m, encoded, func(name string) bool { return name == "b" })
	if err != nil {
		t.Fatalf("ReplayObjectFields: %v", err)
	}
	if !ok {
		t.Fatal("ReplayObjectFields reported a non-object for an object value")
	}
	b.EndObject()
	metadata, value, err := b.Finish()
	if err != nil {
		t.Fatal(err)
	}
	m2, err := DecodeMetadata(metadata)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Decode(m2, value)
	if err != nil {
		t.Fatal(err)
	}
	want := MakeObject([]Field{
		{Name: "a", Value: Int64(1)},
		{Name: "c", Value: MakeArray([]Value{Bool(true)})},
	})
	if !got.Equal(want) {
		t.Fatalf("got %#v, want %#v", got.GoValue(), want.GoValue())
	}

	// Non-object values replay nothing and report false.
	scalar := Encode(&mb, Int64(7))
	var b2 Builder
	ok, err = ReplayObjectFields(&b2, m, scalar, nil)
	if err != nil {
		t.Fatalf("ReplayObjectFields on scalar: %v", err)
	}
	if ok {
		t.Fatal("ReplayObjectFields reported an object for a scalar value")
	}
}

// TestReplayValidationAgreesWithDecode mutates valid encodings and checks
// that Replay and Decode agree on whether the bytes are valid: Replay must
// not accept input Decode rejects, and vice versa, so the streaming path
// keeps the decoder's validation guarantees. Duplicate-field and UTF-8
// validation are included via the mutations.
func TestReplayValidationAgreesWithDecode(t *testing.T) {
	values := builderTestValues()
	r := rand.New(rand.NewPCG(29, 31))
	for range 30 {
		values = append(values, randomBuilderValue(r, 0))
	}

	check := func(t *testing.T, m Metadata, data []byte) {
		t.Helper()
		_, decodeErr := Decode(m, data)
		var b Builder
		replayErr := Replay(&b, m, data)
		if (decodeErr == nil) != (replayErr == nil) {
			t.Fatalf("validation disagreement on % x:\n decode: %v\n replay: %v", data, decodeErr, replayErr)
		}
	}

	for i, v := range values {
		t.Run(fmt.Sprintf("value_%02d", i), func(t *testing.T) {
			var mb MetadataBuilder
			encoded := Encode(&mb, v)
			m, err := DecodeMetadata(mb.AppendTo(nil))
			if err != nil {
				t.Fatal(err)
			}
			check(t, m, encoded)
			// Truncations.
			for n := range len(encoded) {
				check(t, m, encoded[:n])
			}
			// Single-byte corruptions.
			for range 64 {
				mutated := slices.Clone(encoded)
				mutated[r.IntN(len(mutated))] ^= byte(1 + r.IntN(255))
				check(t, m, mutated)
			}
		})
	}
}

// TestReplayAllocations checks that replaying a container of scalars emits
// no allocations: this is what lets the columnar variant copy move residual
// values without a per-row decode cost.
func TestReplayAllocations(t *testing.T) {
	v := MakeObject([]Field{
		{Name: "a", Value: Int64(42)},
		{Name: "b", Value: String("some string value")},
		{Name: "c", Value: MakeArray([]Value{Double(1.5), Bool(false), UUID(uuid.MustParse("00112233-4455-6677-8899-aabbccddeeff"))})},
	})
	var mb MetadataBuilder
	encoded := Encode(&mb, v)
	m, err := DecodeMetadata(mb.AppendTo(nil))
	if err != nil {
		t.Fatal(err)
	}
	var w discardValueWriter
	if err := Replay(&w, m, encoded); err != nil {
		t.Fatal(err)
	}
	allocs := testing.AllocsPerRun(100, func() {
		if err := Replay(&w, m, encoded); err != nil {
			t.Fatal(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("Replay allocated %v times per run, want 0", allocs)
	}
}

// discardValueWriter is a no-op ValueBuilder for allocation measurements.
type discardValueWriter struct{}

func (discardValueWriter) Null()                    {}
func (discardValueWriter) Bool(bool)                {}
func (discardValueWriter) Int8(int8)                {}
func (discardValueWriter) Int16(int16)              {}
func (discardValueWriter) Int32(int32)              {}
func (discardValueWriter) Int64(int64)              {}
func (discardValueWriter) Float(float32)            {}
func (discardValueWriter) Double(float64)           {}
func (discardValueWriter) String(string)            {}
func (discardValueWriter) Binary([]byte)            {}
func (discardValueWriter) Date(int32)               {}
func (discardValueWriter) Time(int64)               {}
func (discardValueWriter) Timestamp(int64)          {}
func (discardValueWriter) TimestampNTZ(int64)       {}
func (discardValueWriter) TimestampNanos(int64)     {}
func (discardValueWriter) TimestampNTZNanos(int64)  {}
func (discardValueWriter) UUID(uuid.UUID)           {}
func (discardValueWriter) Decimal4(int32, byte)     {}
func (discardValueWriter) Decimal8(int64, byte)     {}
func (discardValueWriter) Decimal16([16]byte, byte) {}
func (discardValueWriter) BeginObject()             {}
func (discardValueWriter) Field(string)             {}
func (discardValueWriter) EndObject()               {}
func (discardValueWriter) BeginArray()              {}
func (discardValueWriter) EndArray()                {}
func (discardValueWriter) Err() error               { return nil }
