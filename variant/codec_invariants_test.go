package variant

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"sort"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/google/uuid"
)

// This file checks every encoding the package produces against an
// independent reimplementation of the spec's value and metadata grammars
// (see verifyValue and verifyMetadata), rather than against the package's
// own decoder. Round-trip tests cannot catch bugs where encoder and decoder
// agree on a non-spec layout — that is how the object header bit swap and
// the metadata offset-size misplacement survived until differential testing
// against other engines. The corpus spans every primitive type and the
// structural boundaries where the wire layout changes shape: 1/2/3/4-byte
// offsets, 1/2-byte field IDs, small/large element counts, and short/long
// strings.
//
// Each corpus entry is also checked for the properties the grammar implies:
//
//   - decodeValue consumes exactly the encoded length (values are
//     self-delimiting, so trailing bytes must be ignored);
//   - every strict prefix of the encoding fails to decode (no bounds check
//     can be skipped without accepting corrupt input);
//   - the decoded value compares Equal to the original.

// specValueSize returns the data size (excluding the header byte) that the
// spec's primitive table assigns to type_info id. Variable-length types
// (string, binary) return -1. Taken from the "Value type" table in
// VariantEncoding.md, independently of primitiveSize in types.go.
func specValueSize(id byte) (int, bool) {
	sizes := map[byte]int{
		0: 0, 1: 0, 2: 0, // null, true, false
		3: 1, 4: 2, 5: 4, 6: 8, // int8..int64
		7: 8,               // double
		8: 5, 9: 9, 10: 17, // decimal4/8/16 (1 scale byte + value)
		11: 4,        // date
		12: 8, 13: 8, // timestamp micros, ntz micros
		14: 4,          // float
		15: -1, 16: -1, // binary, string
		17: 8,        // time ntz micros
		18: 8, 19: 8, // timestamp nanos, ntz nanos
		20: 16, // uuid
	}
	n, ok := sizes[id]
	return n, ok
}

// verifyValue walks an encoded value per the spec's value grammar and
// returns the number of bytes it occupies. It fails the test if any header
// field, offset table, or size disagrees with the grammar, or if the
// encoding is not canonical (unsorted object fields, non-minimal offset or
// field-id widths, is_large set when unnecessary).
func verifyValue(t *testing.T, m Metadata, data []byte, path string) int {
	t.Helper()
	if len(data) == 0 {
		t.Fatalf("%s: empty value", path)
	}
	header := data[0]
	basic := BasicType(header & 0x03)
	info := header >> 2

	switch basic {
	case BasicPrimitive:
		size, ok := specValueSize(info)
		if !ok {
			t.Fatalf("%s: unknown primitive type id %d", path, info)
		}
		if size >= 0 {
			if len(data) < 1+size {
				t.Fatalf("%s: primitive %d needs %d bytes, have %d", path, info, 1+size, len(data))
			}
			return 1 + size
		}
		// string (16) and binary (15): 4-byte little-endian length prefix.
		if len(data) < 5 {
			t.Fatalf("%s: variable-length primitive %d missing length", path, info)
		}
		length := int(binary.LittleEndian.Uint32(data[1:5]))
		if len(data) < 5+length {
			t.Fatalf("%s: primitive %d length %d exceeds data", path, info, length)
		}
		if PrimitiveType(info) == PrimitiveString && !utf8.Valid(data[5:5+length]) {
			t.Fatalf("%s: string primitive is not valid UTF-8", path)
		}
		return 5 + length

	case BasicShortString:
		length := int(info)
		if len(data) < 1+length {
			t.Fatalf("%s: short string length %d exceeds data", path, length)
		}
		if !utf8.Valid(data[1 : 1+length]) {
			t.Fatalf("%s: short string is not valid UTF-8", path)
		}
		return 1 + length

	case BasicObject:
		// VariantEncoding.md object_header grammar:
		//   object_header: is_large << 4 | field_id_size_minus_one << 2 |
		//                  field_offset_size_minus_one
		offSz := int(info&0x03) + 1
		idSz := int((info>>2)&0x03) + 1
		isLarge := (info >> 4) & 0x01

		pos := 1
		var n int
		if isLarge == 1 {
			n = int(binary.LittleEndian.Uint32(data[pos : pos+4]))
			pos += 4
		} else {
			n = int(data[pos])
			pos++
		}
		if (n > 255) != (isLarge == 1) {
			t.Fatalf("%s: object is_large=%d inconsistent with %d elements", path, isLarge, n)
		}

		names := make([]string, n)
		maxID := 0
		for i := range n {
			id := readLE(data[pos : pos+idSz])
			pos += idSz
			name, err := m.Lookup(id)
			if err != nil {
				t.Fatalf("%s: object field %d: %v", path, i, err)
			}
			names[i] = name
			maxID = max(maxID, id)
		}
		// VariantEncoding.md: "The field ids and field offsets must be
		// listed in the order of the sorted field names."
		if !sort.StringsAreSorted(names) {
			t.Fatalf("%s: object field names not sorted: %q", path, names)
		}
		for i := 1; i < n; i++ {
			if names[i] == names[i-1] {
				t.Fatalf("%s: duplicate object field %q", path, names[i])
			}
		}

		offsets := make([]int, n+1)
		for i := range offsets {
			offsets[i] = readLE(data[pos : pos+offSz])
			pos += offSz
		}
		if offsets[0] != 0 {
			t.Fatalf("%s: object first offset = %d, want 0", path, offsets[0])
		}

		region := data[pos:]
		if len(region) < offsets[n] {
			t.Fatalf("%s: object value region %d exceeds data %d", path, offsets[n], len(region))
		}
		for i := range n {
			if offsets[i+1] < offsets[i] {
				t.Fatalf("%s: object offsets not monotonic at %d", path, i)
			}
			got := verifyValue(t, m, region[offsets[i]:offsets[n]], fmt.Sprintf("%s.%s", path, names[i]))
			if got != offsets[i+1]-offsets[i] {
				t.Fatalf("%s.%s: field occupies %d bytes, offsets say %d", path, names[i], got, offsets[i+1]-offsets[i])
			}
		}

		// Canonical output uses minimal widths: field_offset_size fits the
		// value region size, field_id_size fits the largest field id.
		if wantOff := offsetSize(offsetSizeCode(offsets[n])); offSz != wantOff {
			t.Fatalf("%s: object offset size %d, minimal is %d for region %d", path, offSz, wantOff, offsets[n])
		}
		if wantID := offsetSize(offsetSizeCode(maxID)); n > 0 && idSz != wantID {
			t.Fatalf("%s: object field id size %d, minimal is %d for max id %d", path, idSz, wantID, maxID)
		}
		return pos + offsets[n]

	case BasicArray:
		// VariantEncoding.md array_header grammar:
		//   array_header: is_large << 2 | field_offset_size_minus_one
		offSz := int(info&0x03) + 1
		isLarge := (info >> 2) & 0x01

		pos := 1
		var n int
		if isLarge == 1 {
			n = int(binary.LittleEndian.Uint32(data[pos : pos+4]))
			pos += 4
		} else {
			n = int(data[pos])
			pos++
		}
		if (n > 255) != (isLarge == 1) {
			t.Fatalf("%s: array is_large=%d inconsistent with %d elements", path, isLarge, n)
		}

		offsets := make([]int, n+1)
		for i := range offsets {
			offsets[i] = readLE(data[pos : pos+offSz])
			pos += offSz
		}
		if offsets[0] != 0 {
			t.Fatalf("%s: array first offset = %d, want 0", path, offsets[0])
		}
		region := data[pos:]
		if len(region) < offsets[n] {
			t.Fatalf("%s: array value region %d exceeds data %d", path, offsets[n], len(region))
		}
		for i := range n {
			if offsets[i+1] < offsets[i] {
				t.Fatalf("%s: array offsets not monotonic at %d", path, i)
			}
			got := verifyValue(t, m, region[offsets[i]:offsets[n]], fmt.Sprintf("%s[%d]", path, i))
			if got != offsets[i+1]-offsets[i] {
				t.Fatalf("%s[%d]: element occupies %d bytes, offsets say %d", path, i, got, offsets[i+1]-offsets[i])
			}
		}
		if wantOff := offsetSize(offsetSizeCode(offsets[n])); offSz != wantOff {
			t.Fatalf("%s: array offset size %d, minimal is %d for region %d", path, offSz, wantOff, offsets[n])
		}
		return pos + offsets[n]
	}
	panic("unreachable")
}

// verifyMetadata checks an encoded metadata dictionary against the spec's
// metadata grammar:
//
//	header: 1 byte (<version> | <sorted_strings> << 4 | (<offset_size_minus_one> << 6))
func verifyMetadata(t *testing.T, raw []byte, wantStrings []string) {
	t.Helper()
	header := raw[0]
	if v := header & 0x0F; v != 1 {
		t.Fatalf("metadata version = %d, want 1", v)
	}
	sortedBit := (header >> 4) & 0x01
	offSz := int((header>>6)&0x03) + 1

	pos := 1
	n := readLE(raw[pos : pos+offSz])
	pos += offSz
	if n != len(wantStrings) {
		t.Fatalf("metadata dictionary_size = %d, want %d", n, len(wantStrings))
	}
	offsets := make([]int, n+1)
	for i := range offsets {
		offsets[i] = readLE(raw[pos : pos+offSz])
		pos += offSz
	}
	if offsets[0] != 0 {
		t.Fatalf("metadata first offset = %d, want 0", offsets[0])
	}
	stringData := raw[pos:]
	if len(stringData) != offsets[n] {
		t.Fatalf("metadata string data length %d, offsets say %d", len(stringData), offsets[n])
	}
	for i := range n {
		if offsets[i+1] < offsets[i] {
			t.Fatalf("metadata offsets not monotonic at %d", i)
		}
		s := string(stringData[offsets[i]:offsets[i+1]])
		if s != wantStrings[i] {
			t.Fatalf("metadata string %d = %q, want %q", i, s, wantStrings[i])
		}
		if !utf8.ValidString(s) {
			t.Fatalf("metadata string %d is not valid UTF-8", i)
		}
	}
	if sorted := sort.StringsAreSorted(wantStrings); (sortedBit == 1) != sorted {
		t.Fatalf("metadata sorted bit = %d, strings sorted = %v", sortedBit, sorted)
	}
	// The offset size must accommodate both dictionary_size and the largest
	// offset; canonical output uses the minimal size that does.
	if want := offsetSize(offsetSizeCode(max(offsets[n], n))); offSz != want {
		t.Fatalf("metadata offset size %d, minimal is %d", offSz, want)
	}
}

func readLE(b []byte) int {
	v := 0
	for i := len(b) - 1; i >= 0; i-- {
		v = v<<8 | int(b[i])
	}
	return v
}

// checkCodecInvariants encodes v and runs the full battery of grammar and
// decoding checks described in the file comment.
func checkCodecInvariants(t *testing.T, v Value) {
	t.Helper()
	var b MetadataBuilder
	encoded := Encode(&b, v)
	m, metaBytes := b.Build()

	verifyMetadata(t, metaBytes, m.Strings)
	m2, err := DecodeMetadata(metaBytes)
	if err != nil {
		t.Fatalf("DecodeMetadata: %v", err)
	}
	if len(m2.Strings) != len(m.Strings) {
		t.Fatalf("metadata round-trip: %d strings, want %d", len(m2.Strings), len(m.Strings))
	}
	for i := range m.Strings {
		if m2.Strings[i] != m.Strings[i] {
			t.Fatalf("metadata round-trip string %d = %q, want %q", i, m2.Strings[i], m.Strings[i])
		}
	}
	if want := sort.StringsAreSorted(m.Strings); m2.Sorted != want {
		t.Fatalf("metadata round-trip Sorted = %v, want %v", m2.Sorted, want)
	}
	for i := 0; i < len(metaBytes); i += prefixStep(len(metaBytes)) {
		if _, err := DecodeMetadata(metaBytes[:i]); err == nil {
			t.Fatalf("DecodeMetadata accepted truncated input [:%d] of %d bytes", i, len(metaBytes))
		}
	}

	// The wire type id of a primitive must match the constructor's type;
	// round-trip tests cannot pin this because a swapped id in both encoder
	// and decoder stays self-consistent. Strings are exempt: the encoder
	// legitimately folds short strings into the short-string basic type.
	if v.Basic() == BasicPrimitive && v.Type() != PrimitiveString {
		if got := PrimitiveType(encoded[0] >> 2); got != v.Type() {
			t.Fatalf("wire type id = %d, want %d", got, v.Type())
		}
	}

	if got := verifyValue(t, m, encoded, "$"); got != len(encoded) {
		t.Fatalf("grammar walk consumed %d bytes, encoding is %d", got, len(encoded))
	}

	// The exact-size buffer must decode: every length check has to accept
	// input with no slack after the value.
	decoded, err := Decode(m, encoded)
	if err != nil {
		t.Fatalf("Decode of exact-size input: %v", err)
	}
	if !decoded.Equal(v) || !v.Equal(decoded) {
		t.Fatalf("round-trip mismatch:\n got: %#v\nwant: %#v", decoded.GoValue(), v.GoValue())
	}

	// Values are self-delimiting: decodeValue must consume exactly the
	// encoded length and ignore trailing bytes.
	withTrailer := append(slices.Clone(encoded), 0xDE, 0xAD, 0xBE, 0xEF)
	decoded, consumed, err := decodeValue(m, withTrailer)
	if err != nil {
		t.Fatalf("decodeValue with trailing bytes: %v", err)
	}
	if consumed != len(encoded) {
		t.Fatalf("decodeValue consumed %d bytes, encoding is %d", consumed, len(encoded))
	}
	if !decoded.Equal(v) {
		t.Fatalf("round-trip mismatch with trailing bytes:\n got: %#v\nwant: %#v", decoded.GoValue(), v.GoValue())
	}

	// Every strict prefix of a canonical encoding must fail to decode:
	// each byte is load-bearing, so a decoder that accepts a prefix has
	// skipped a bounds check. Large encodings sample prefixes to keep the
	// quadratic scan bounded.
	for i := 0; i < len(encoded); i += prefixStep(len(encoded)) {
		if _, err := Decode(m, encoded[:i]); err == nil {
			t.Fatalf("Decode accepted truncated input [:%d] of %d bytes", i, len(encoded))
		}
	}
}

func prefixStep(n int) int {
	return max(1, n/512)
}

// corpusValues returns deterministic values covering every primitive type
// and every structural boundary of the wire format.
func corpusValues() map[string]Value {
	long := strings.Repeat("x", 300)
	corpus := map[string]Value{
		"null":             Null(),
		"true":             Bool(true),
		"false":            Bool(false),
		"int8_min":         Int8(math.MinInt8),
		"int8_max":         Int8(math.MaxInt8),
		"int16_min":        Int16(math.MinInt16),
		"int32_min":        Int32(math.MinInt32),
		"int64_min":        Int64(math.MinInt64),
		"int64_max":        Int64(math.MaxInt64),
		"float_nan":        Float(float32(math.NaN())),
		"double_inf":       Double(math.Inf(1)),
		"double_neg":       Double(-math.MaxFloat64),
		"decimal4":         Decimal4(-999999999, 9),
		"decimal8":         Decimal8(math.MaxInt64, 38),
		"decimal16":        Decimal16([16]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F}, 0),
		"date":             Date(-719162),
		"timestamp":        Timestamp(math.MinInt64),
		"timestamp_ntz":    TimestampNTZ(1),
		"timestamp_nanos":  TimestampNanos(math.MaxInt64),
		"timestamp_ntz_ns": TimestampNTZNanos(-1),
		"time":             Time(86399999999),
		"uuid":             UUID(uuid.MustParse("f24f9b64-81fa-49d1-b74e-8c09a6e31c56")),
		"string_empty":     String(""),
		"string_63":        String(strings.Repeat("a", 63)),
		"string_64":        String(strings.Repeat("a", 64)),
		"string_multibyte": String(strings.Repeat("é", 31) + "e"), // 63 bytes with a multibyte rune at the boundary
		"binary_empty":     Binary(nil),
		"binary_1":         Binary([]byte{0}),
		"array_empty":      MakeArray(nil),
		"array_one":        MakeArray([]Value{Null()}),
		"object_empty":     MakeObject(nil),
		"object_one":       MakeObject([]Field{{Name: "a", Value: Int8(1)}}),
		"object_unsorted_input": MakeObject([]Field{
			{Name: "z", Value: Int8(1)},
			{Name: "a", Value: Int8(2)},
			{Name: "m", Value: String(long)},
		}),
		"nested": MakeObject([]Field{
			{Name: "arr", Value: MakeArray([]Value{
				MakeObject([]Field{{Name: "deep", Value: MakeArray([]Value{String("s"), Null()})}}),
				Binary(make([]byte, 300)),
			})},
			{Name: "s", Value: String(long)},
		}),
	}

	// Element counts crossing the is_large boundary (255 → 256).
	elems := make([]Value, 256)
	for i := range elems {
		elems[i] = Int8(int8(i))
	}
	corpus["array_255"] = MakeArray(elems[:255])
	corpus["array_256"] = MakeArray(elems)

	fields := make([]Field, 256)
	for i := range fields {
		fields[i] = Field{Name: fmt.Sprintf("f%03d", i), Value: Int8(int8(i))}
	}
	corpus["object_255"] = MakeObject(fields[:255])
	corpus["object_256"] = MakeObject(fields)

	// Value regions crossing offset-size boundaries: 1→2 bytes at 255/256,
	// 2→3 bytes at 65535/65536.
	for _, size := range []int{255, 256, 65535, 65536} {
		pad := size - 5 // 5 bytes of binary primitive overhead (header + length)
		corpus[fmt.Sprintf("array_region_%d", size)] = MakeArray([]Value{Binary(make([]byte, pad))})
		corpus[fmt.Sprintf("object_region_%d", size)] = MakeObject([]Field{{Name: "a", Value: Binary(make([]byte, pad))}})
	}

	// Field ids crossing the 1-byte boundary: an object using dictionary
	// entries above id 255 must switch to 2-byte field ids.
	bigDict := make([]Field, 300)
	for i := range bigDict {
		bigDict[i] = Field{Name: fmt.Sprintf("k%03d", i), Value: Null()}
	}
	corpus["object_field_ids_2byte"] = MakeObject(bigDict)

	// Metadata offset-size boundaries: total dictionary string bytes
	// crossing 255/256 and 65535/65536.
	for _, size := range []int{255, 256, 65535, 65536} {
		corpus[fmt.Sprintf("metadata_region_%d", size)] = MakeObject([]Field{
			{Name: strings.Repeat("k", size-1), Value: Null()},
			{Name: "v", Value: Null()},
		})
	}

	return corpus
}

// TestCodecGrammarInvariants runs the invariant battery over the
// deterministic boundary corpus.
func TestCodecGrammarInvariants(t *testing.T) {
	for name, v := range corpusValues() {
		t.Run(name, func(t *testing.T) {
			checkCodecInvariants(t, v)
		})
	}
}

// TestDecodeNonCanonicalInputs pins the leniency half of the Decode
// contract: input that is non-canonical but unambiguous under the spec
// grammar must decode. The encoder never produces these layouts, so they
// only get coverage here.
func TestDecodeNonCanonicalInputs(t *testing.T) {
	tests := []struct {
		name  string
		meta  Metadata
		value []byte
		want  Value
	}{
		{
			// The spec allows the long string form for any length; the
			// encoder only uses it above 63 bytes.
			name:  "long-form empty string",
			value: []byte{0x40, 0x00, 0x00, 0x00, 0x00},
			want:  String(""),
		},
		{
			name:  "long-form short string",
			value: []byte{0x40, 0x02, 0x00, 0x00, 0x00, 'h', 'i'},
			want:  String("hi"),
		},
		{
			// is_large with a small element count is wasteful but legal.
			// Array header: basic=3, offset_size_minus_one=0, is_large=1
			// (bit 4): 0x13. 4-byte num_elements=1, offsets {0,1}, null.
			name:  "is_large array with one element",
			value: []byte{0x13, 0x01, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00},
			want:  MakeArray([]Value{Null()}),
		},
		{
			// Object header: basic=2, field_offset_size_minus_one=0,
			// field_id_size_minus_one=0, is_large=1 (bit 6): 0x42.
			name:  "is_large object with one field",
			meta:  Metadata{Strings: []string{"a"}},
			value: []byte{0x42, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00},
			want:  MakeObject([]Field{{Name: "a", Value: Null()}}),
		},
		{
			// 4-byte offsets for a tiny array: legal, never emitted.
			// Array header: basic=3, offset_size_minus_one=3: 0x0F.
			name:  "oversized offsets",
			value: []byte{0x0F, 0x01, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00},
			want:  MakeArray([]Value{Null()}),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Decode(tt.meta, tt.value)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if !got.Equal(tt.want) {
				t.Errorf("decoded value:\n got: %#v\nwant: %#v", got.GoValue(), tt.want.GoValue())
			}
		})
	}
}

// TestCodecGrammarInvariantsRandomized runs the same battery over values
// from the fuzz generator, seeded deterministically so failures reproduce.
func TestCodecGrammarInvariantsRandomized(t *testing.T) {
	rng := rand.New(rand.NewPCG(0x9E3779B9, 0x7F4A7C15))
	for i := range 300 {
		raw := make([]byte, 200)
		for j := range raw {
			raw[j] = byte(rng.Uint32())
		}
		v, _ := arbitraryValue(raw, 0)
		t.Run(fmt.Sprintf("seed_%03d", i), func(t *testing.T) {
			checkCodecInvariants(t, v)
		})
	}
}
