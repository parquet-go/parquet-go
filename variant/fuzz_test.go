package variant

import (
	"encoding/binary"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
)

// FuzzDecodeMetadata verifies that DecodeMetadata never panics or succeeds
// with out-of-range results on arbitrary input.
func FuzzDecodeMetadata(f *testing.F) {
	f.Add([]byte{0x01, 0x00, 0x00})
	f.Add([]byte{})
	if paths, err := filepath.Glob(filepath.Join("testdata", "spec", "*.metadata")); err == nil {
		for _, p := range paths {
			if data, err := os.ReadFile(p); err == nil {
				f.Add(data)
			}
		}
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		m, err := DecodeMetadata(data)
		if err != nil {
			return
		}
		for i := range m.Strings {
			if _, err := m.Lookup(i); err != nil {
				t.Fatalf("Lookup(%d) failed on successfully decoded metadata: %v", i, err)
			}
		}
	})
}

// FuzzDecode verifies that Decode never panics on arbitrary value bytes, and
// that every value it accepts survives re-encoding unchanged. Values are
// decoded against both an empty dictionary and a small populated one, so
// field ID resolution failures are exercised alongside successful lookups.
func FuzzDecode(f *testing.F) {
	f.Add([]byte{0x00})
	f.Add([]byte{})
	if paths, err := filepath.Glob(filepath.Join("testdata", "spec", "*.value")); err == nil {
		for _, p := range paths {
			if data, err := os.ReadFile(p); err == nil {
				f.Add(data)
			}
		}
	}
	dicts := []Metadata{{}, {Strings: []string{"a", "b", "c"}}}
	f.Fuzz(func(t *testing.T, data []byte) {
		for _, meta := range dicts {
			v, err := Decode(meta, data)
			if err != nil {
				continue
			}
			var b MetadataBuilder
			encoded := Encode(&b, v)
			m2, _ := b.Build()
			v2, err := Decode(m2, encoded)
			if err != nil {
				t.Fatalf("re-encoded value failed to decode: %v", err)
			}
			if !v.Equal(v2) {
				t.Fatalf("round-trip mismatch:\n got: %#v\nwant: %#v", v2.GoValue(), v.GoValue())
			}
		}
	})
}

// arbitraryValue builds a Value tree from fuzz input, consuming bytes from
// data as it goes and returning the remainder. It can produce every
// primitive type plus nested objects and arrays. Generated values are
// always legal to encode: strings are forced to valid UTF-8 and object
// field names are deduplicated, since the decoder rejects both.
func arbitraryValue(data []byte, depth int) (Value, []byte) {
	take := func(n int) []byte {
		b := make([]byte, n)
		n = min(n, len(data))
		copy(b, data[:n])
		data = data[n:]
		return b
	}
	u32 := func() uint32 { return binary.LittleEndian.Uint32(take(4)) }
	u64 := func() uint64 { return binary.LittleEndian.Uint64(take(8)) }
	str := func(n int) string { return strings.ToValidUTF8(string(take(n)), "?") }

	if len(data) == 0 {
		return Null(), nil
	}
	tag := data[0]
	data = data[1:]

	// Beyond maxDepth only primitive kinds are generated, so trees stay
	// small regardless of input.
	const maxDepth, primitiveKinds = 6, 21
	kinds := byte(primitiveKinds + 2)
	if depth >= maxDepth {
		kinds = primitiveKinds
	}
	switch tag % kinds {
	case 0:
		return Null(), data
	case 1:
		return Bool(true), data
	case 2:
		return Bool(false), data
	case 3:
		return Int8(int8(take(1)[0])), data
	case 4:
		return Int16(int16(binary.LittleEndian.Uint16(take(2)))), data
	case 5:
		return Int32(int32(u32())), data
	case 6:
		return Int64(int64(u64())), data
	case 7:
		return Float(math.Float32frombits(u32())), data
	case 8:
		return Double(math.Float64frombits(u64())), data
	case 9:
		return Decimal4(int32(u32()), take(1)[0]), data
	case 10:
		return Decimal8(int64(u64()), take(1)[0]), data
	case 11:
		var d [16]byte
		copy(d[:], take(16))
		return Decimal16(d, take(1)[0]), data
	case 12:
		return Date(int32(u32())), data
	case 13:
		return Timestamp(int64(u64())), data
	case 14:
		return TimestampNTZ(int64(u64())), data
	case 15:
		return TimestampNanos(int64(u64())), data
	case 16:
		return TimestampNTZNanos(int64(u64())), data
	case 17:
		return Time(int64(u64())), data
	case 18:
		var u uuid.UUID
		copy(u[:], take(16))
		return UUID(u), data
	case 19:
		// Lengths up to 95 cross the 64-byte short string boundary.
		return String(str(int(take(1)[0] % 96))), data
	case 20:
		return Binary(take(int(take(1)[0] % 32))), data
	case 21:
		elems := make([]Value, take(1)[0]%5)
		for i := range elems {
			elems[i], data = arbitraryValue(data, depth+1)
		}
		return MakeArray(elems), data
	default:
		n := int(take(1)[0] % 5)
		fields := make([]Field, 0, n)
		seen := make(map[string]bool, n)
		for range n {
			name := str(int(take(1)[0] % 8))
			var v Value
			v, data = arbitraryValue(data, depth+1)
			if !seen[name] {
				seen[name] = true
				fields = append(fields, Field{Name: name, Value: v})
			}
		}
		return MakeObject(fields), data
	}
}

// FuzzEncodeRoundTrip drives the encoder with arbitrary structured values
// rather than arbitrary bytes. The decoder-driven fuzzers above only reach
// Encode with values the decoder already accepted; this target covers
// encoder inputs they rarely produce, such as deeply nested structures,
// NaN payloads, and every primitive type. Any generated value must encode,
// decode, and compare equal.
func FuzzEncodeRoundTrip(f *testing.F) {
	f.Add([]byte{})
	// An object {a: 42, b: [int64]} followed by trailing bytes.
	f.Add([]byte{22, 3, 1, 'a', 3, 42, 1, 'b', 21, 2, 6, 1, 2, 3, 4, 5, 6, 7, 8})
	f.Fuzz(func(t *testing.T, data []byte) {
		v, _ := arbitraryValue(data, 0)
		var b MetadataBuilder
		encoded := Encode(&b, v)
		m, _ := b.Build()
		decoded, err := Decode(m, encoded)
		if err != nil {
			t.Fatalf("decoding encoded value failed: %v", err)
		}
		if !decoded.Equal(v) {
			t.Fatalf("round-trip mismatch:\n got: %#v\nwant: %#v", decoded.GoValue(), v.GoValue())
		}
	})
}

// FuzzMetadataValueRoundTrip decodes a (metadata, value) pair and, on
// success, re-encodes and compares for equality.
func FuzzMetadataValueRoundTrip(f *testing.F) {
	names := []string{"object_nested", "array_nested", "primitive_int8"}
	for _, name := range names {
		meta, err1 := os.ReadFile(filepath.Join("testdata", "spec", name+".metadata"))
		val, err2 := os.ReadFile(filepath.Join("testdata", "spec", name+".value"))
		if err1 == nil && err2 == nil {
			f.Add(meta, val)
		}
	}
	f.Fuzz(func(t *testing.T, metaBytes, valueBytes []byte) {
		m, err := DecodeMetadata(metaBytes)
		if err != nil {
			return
		}
		v, err := Decode(m, valueBytes)
		if err != nil {
			return
		}
		var b MetadataBuilder
		encoded := Encode(&b, v)
		m2, _ := b.Build()
		v2, err := Decode(m2, encoded)
		if err != nil {
			t.Fatalf("re-encoded value failed to decode: %v", err)
		}
		if !v.Equal(v2) {
			t.Fatalf("round-trip mismatch:\n got: %#v\nwant: %#v", v2.GoValue(), v.GoValue())
		}
	})
}
