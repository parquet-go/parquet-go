package variant

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// The testdata/spec directory contains the canonical Variant binary encoding
// examples from https://github.com/apache/parquet-testing (the variant/
// directory, vendored at commit 1a2a75127be06fc0123f03ebd36c966f7beda27d).
// Each case is a pair of files: <name>.metadata holds the binary contents of
// the metadata column, and <name>.value holds the binary contents of the
// value column. Expected values are documented in data_dictionary.json and
// in the regen.py generator script from that repository.

func mustLoadSpecCase(t testing.TB, name string) (metadata, value []byte) {
	t.Helper()
	metadata, err := os.ReadFile(filepath.Join("testdata", "spec", name+".metadata"))
	if err != nil {
		t.Fatalf("reading metadata: %v", err)
	}
	value, err = os.ReadFile(filepath.Join("testdata", "spec", name+".value"))
	if err != nil {
		t.Fatalf("reading value: %v", err)
	}
	return metadata, value
}

func decimal16FromInt64(v int64) [16]byte {
	var b [16]byte
	binary.LittleEndian.PutUint64(b[:8], uint64(v))
	if v < 0 {
		for i := 8; i < 16; i++ {
			b[i] = 0xFF
		}
	}
	return b
}

// specExpectedValues returns the expected decoded variant.Value for every
// case in testdata/spec. Values were derived from the regen.py script in
// apache/parquet-testing (Spark 4.0) and verified against hex dumps of the
// binary files.
func specExpectedValues() map[string]Value {
	mustParse := func(layout, s string) time.Time {
		ts, err := time.Parse(layout, s)
		if err != nil {
			panic(err)
		}
		return ts
	}

	// regen.py inserts '2025-04-16T12:34:56.78'::Timestamp with a session
	// time zone of -04:00 (per data_dictionary.json), so the instant is
	// 16:34:56.78 UTC.
	tsMicros := mustParse(time.RFC3339, "2025-04-16T12:34:56.78-04:00").UnixMicro()
	tsNTZMicros := mustParse(time.RFC3339, "2025-04-16T12:34:56.78Z").UnixMicro()
	tsNanos := mustParse(time.RFC3339, "2024-11-07T12:33:54.123456789Z").UnixNano()
	dateDays := int32(mustParse(time.DateOnly, "2025-04-16").Unix() / 86400)
	// 12:33:54.123456 since midnight, in microseconds.
	timeMicros := int64((12*3600+33*60+54))*1_000_000 + 123456

	return map[string]Value{
		"primitive_null":          Null(),
		"primitive_boolean_true":  Bool(true),
		"primitive_boolean_false": Bool(false),
		"primitive_int8":          Int8(42),
		"primitive_int16":         Int16(1234),
		"primitive_int32":         Int32(123456),
		"primitive_int64":         Int64(1234567890123456789),
		"primitive_float":         Float(1234567890.1234),
		"primitive_double":        Double(1234567890.1234),
		"primitive_decimal4":      Decimal4(1234, 2),
		"primitive_decimal8":      Decimal8(1234567890, 2),
		"primitive_decimal16":     Decimal16(decimal16FromInt64(1234567891234567890), 2),
		"primitive_date":          Date(dateDays),
		"primitive_timestamp":     Timestamp(tsMicros),
		"primitive_timestampntz":  TimestampNTZ(tsNTZMicros),
		"primitive_time":          Time(timeMicros),
		// Nanosecond-precision timestamps (primitive type IDs 18 and 19).
		"primitive_timestamp_nanos":    TimestampNanos(tsNanos),
		"primitive_timestampntz_nanos": TimestampNTZNanos(tsNanos),
		"primitive_uuid":               UUID(uuid.MustParse("f24f9b64-81fa-49d1-b74e-8c09a6e31c56")),
		"primitive_binary":             Binary([]byte{0x03, 0x13, 0x37, 0xde, 0xad, 0xbe, 0xef, 0xca, 0xfe}),
		"primitive_string":             String("This string is longer than 64 bytes and therefore does not fit in a short_string and it also includes several non ascii characters such as 🐢, 💖, ♥️, 🎣 and 🤦!!"),
		"long_string":                  String("This string is for sure and certainly longer than 64 bytes and it also includes several non ascii characters such as 🐢, 💖, ♥️, 🎣 and 🤦!!"),
		"short_string":                 String("Less than 64 bytes (❤️ with utf8)"),

		"object_empty": MakeObject(nil),
		// Note: Spark's parse_json maps JSON numbers with a fractional part
		// to decimals, and integers to the narrowest int type.
		"object_primitive": MakeObject([]Field{
			{Name: "int_field", Value: Int8(1)},
			{Name: "double_field", Value: Decimal4(123456789, 8)},
			{Name: "boolean_true_field", Value: Bool(true)},
			{Name: "boolean_false_field", Value: Bool(false)},
			{Name: "string_field", Value: String("Apache Parquet")},
			{Name: "null_field", Value: Null()},
			{Name: "timestamp_field", Value: String("2025-04-16T12:34:56.78")},
		}),
		"object_nested": MakeObject([]Field{
			{Name: "id", Value: Int8(1)},
			{Name: "species", Value: MakeObject([]Field{
				{Name: "name", Value: String("lava monster")},
				{Name: "population", Value: Int16(6789)},
			})},
			{Name: "observation", Value: MakeObject([]Field{
				{Name: "time", Value: String("12:34:56")},
				{Name: "location", Value: String("In the Volcano")},
				{Name: "value", Value: MakeObject([]Field{
					{Name: "temperature", Value: Int8(123)},
					{Name: "humidity", Value: Int16(456)},
				})},
			})},
		}),

		"array_empty":     MakeArray(nil),
		"array_primitive": MakeArray([]Value{Int8(2), Int8(1), Int8(5), Int8(9)}),
		"array_nested": MakeArray([]Value{
			MakeObject([]Field{
				{Name: "id", Value: Int8(1)},
				{Name: "thing", Value: MakeObject([]Field{
					{Name: "names", Value: MakeArray([]Value{
						String("Contrarian"), String("Spider"),
					})},
				})},
			}),
			Null(),
			MakeObject([]Field{
				{Name: "id", Value: Int8(2)},
				{Name: "names", Value: MakeArray([]Value{
					String("Apple"), String("Ray"), Null(),
				})},
				{Name: "type", Value: String("if")},
			}),
		}),
	}
}

// TestSpecConformanceDecode decodes each canonical binary case from
// apache/parquet-testing and verifies the result matches the documented
// expected value exactly (including the primitive type ID). Every vendored
// case must have an expected value: a file without one fails the test rather
// than being silently skipped.
func TestSpecConformanceDecode(t *testing.T) {
	expected := specExpectedValues()

	paths, err := filepath.Glob(filepath.Join("testdata", "spec", "*.value"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("globbing testdata/spec: %v (%d files)", err, len(paths))
	}
	for _, p := range paths {
		name := strings.TrimSuffix(filepath.Base(p), ".value")
		if _, ok := expected[name]; !ok {
			t.Errorf("testdata/spec/%s has no expected value in specExpectedValues", name)
		}
	}

	for name, want := range expected {
		t.Run(name, func(t *testing.T) {
			metadataBytes, valueBytes := mustLoadSpecCase(t, name)

			m, err := DecodeMetadata(metadataBytes)
			if err != nil {
				t.Fatalf("DecodeMetadata: %v", err)
			}
			got, err := Decode(m, valueBytes)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if !got.Equal(want) {
				t.Errorf("decoded value mismatch:\n got: %#v\nwant: %#v", got.GoValue(), want.GoValue())
			}
		})
	}
}

// TestSpecConformanceRoundTrip decodes each canonical binary case, re-encodes
// it with this package's encoder, decodes the re-encoded bytes, and verifies
// the value survives unchanged. This validates the encoder against
// spec-generated data for every type.
func TestSpecConformanceRoundTrip(t *testing.T) {
	for name := range specExpectedValues() {
		t.Run(name, func(t *testing.T) {
			metadataBytes, valueBytes := mustLoadSpecCase(t, name)

			m, err := DecodeMetadata(metadataBytes)
			if err != nil {
				t.Fatalf("DecodeMetadata: %v", err)
			}
			original, err := Decode(m, valueBytes)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}

			var b MetadataBuilder
			reencoded := Encode(&b, original)
			m2, _ := b.Build()

			decoded, err := Decode(m2, reencoded)
			if err != nil {
				t.Fatalf("Decode(re-encoded): %v", err)
			}
			if !original.Equal(decoded) {
				t.Errorf("round-trip mismatch:\n got: %#v\nwant: %#v", decoded.GoValue(), original.GoValue())
			}
		})
	}
}

// TestSpecConformanceMetadataRoundTrip verifies that re-encoding the decoded
// metadata produces a dictionary that can resolve all the same strings.
func TestSpecConformanceMetadataRoundTrip(t *testing.T) {
	for name := range specExpectedValues() {
		t.Run(name, func(t *testing.T) {
			metadataBytes, _ := mustLoadSpecCase(t, name)

			m, err := DecodeMetadata(metadataBytes)
			if err != nil {
				t.Fatalf("DecodeMetadata: %v", err)
			}

			var b MetadataBuilder
			for _, s := range m.Strings {
				b.Add(s)
			}
			_, encoded := b.Build()

			m2, err := DecodeMetadata(encoded)
			if err != nil {
				t.Fatalf("DecodeMetadata(re-encoded): %v", err)
			}
			if len(m.Strings) != len(m2.Strings) {
				t.Fatalf("dictionary size mismatch: got %d want %d", len(m2.Strings), len(m.Strings))
			}
			for i := range m.Strings {
				if m.Strings[i] != m2.Strings[i] {
					t.Errorf("dictionary entry %d mismatch: got %q want %q", i, m2.Strings[i], m.Strings[i])
				}
			}
		})
	}
}

// BenchmarkDecode decodes every case in the spec corpus per iteration,
// covering all primitive types plus nested objects and arrays. Validation
// added to the decode path (UTF-8 checks, size checks) shows up here.
func BenchmarkDecode(b *testing.B) {
	type specCase struct {
		meta  Metadata
		value []byte
	}
	var cases []specCase
	for name := range specExpectedValues() {
		metadataBytes, valueBytes := mustLoadSpecCase(b, name)
		m, err := DecodeMetadata(metadataBytes)
		if err != nil {
			b.Fatalf("DecodeMetadata(%s): %v", name, err)
		}
		cases = append(cases, specCase{meta: m, value: valueBytes})
	}
	b.ReportAllocs()
	for b.Loop() {
		for _, c := range cases {
			if _, err := Decode(c.meta, c.value); err != nil {
				b.Fatal(err)
			}
		}
	}
}

// BenchmarkEncode encodes every decoded case in the spec corpus per
// iteration.
func BenchmarkEncode(b *testing.B) {
	var values []Value
	for name := range specExpectedValues() {
		metadataBytes, valueBytes := mustLoadSpecCase(b, name)
		m, err := DecodeMetadata(metadataBytes)
		if err != nil {
			b.Fatalf("DecodeMetadata(%s): %v", name, err)
		}
		v, err := Decode(m, valueBytes)
		if err != nil {
			b.Fatalf("Decode(%s): %v", name, err)
		}
		values = append(values, v)
	}
	b.ReportAllocs()
	for b.Loop() {
		for _, v := range values {
			var mb MetadataBuilder
			Encode(&mb, v)
		}
	}
}
