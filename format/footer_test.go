package format_test

import (
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"testing"

	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"
)

// loadAllFooters extracts the raw footer bytes of every readable plain
// parquet file under ../testdata, keyed by file name.
func loadAllFooters(t testing.TB) map[string][]byte {
	paths, err := filepath.Glob(filepath.Join("..", "testdata", "*.parquet"))
	if err != nil {
		t.Fatal(err)
	}
	footers := make(map[string][]byte)
	for _, path := range paths {
		// Skips encrypted (PARE) and malformed files.
		if footer, err := extractFooterBytes(path); err == nil {
			footers[filepath.Base(path)] = footer
		}
	}
	if len(footers) == 0 {
		t.Skip("no parquet testdata files found")
	}
	return footers
}

func sortedNames(footers map[string][]byte) []string {
	names := make([]string, 0, len(footers))
	for name := range footers {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func decodeFresh(t testing.TB, footer []byte) *format.FileMetaData {
	protocol := &thrift.CompactProtocol{}
	metadata := new(format.FileMetaData)
	if err := thrift.Unmarshal(protocol, footer, metadata); err != nil {
		t.Fatal(err)
	}
	return metadata
}

// TestFooterDecoderMatchesUnmarshal checks that FooterDecoder produces the
// same result as a fresh thrift.Unmarshal for every testdata footer, and
// that it consumes the entire input.
func TestFooterDecoderMatchesUnmarshal(t *testing.T) {
	footers := loadAllFooters(t)
	decoder := new(format.FooterDecoder)

	for _, name := range sortedNames(footers) {
		footer := footers[name]
		decoded, n, err := decoder.Decode(footer)
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		if n != len(footer) {
			t.Errorf("%s: consumed %d bytes, want %d", name, n, len(footer))
		}
		if fresh := decodeFresh(t, footer); !equalMetadata(decoded, fresh) {
			t.Errorf("%s: decoded metadata differs from fresh decode", name)
		}
	}
}

// TestFooterDecoderReuse decodes every ordered pair of testdata footers
// through a single FooterDecoder and checks the second result against a
// fresh decode. This catches any state from the first decode leaking into
// the second (i.e. fields missed by the Reset methods).
func TestFooterDecoderReuse(t *testing.T) {
	footers := loadAllFooters(t)
	names := sortedNames(footers)

	for _, first := range names {
		t.Run(first, func(t *testing.T) {
			for _, second := range names {
				decoder := new(format.FooterDecoder)
				if _, _, err := decoder.Decode(footers[first]); err != nil {
					t.Fatalf("decoding %s: %v", first, err)
				}
				decoded, _, err := decoder.Decode(footers[second])
				if err != nil {
					t.Fatalf("decoding %s after %s: %v", second, first, err)
				}
				if fresh := decodeFresh(t, footers[second]); !equalMetadata(decoded, fresh) {
					t.Errorf("decoding %s after %s: metadata differs from fresh decode", second, first)
				}
			}
		})
	}
}

// TestFooterDecoderInputOwnership checks that the decoder copies its input:
// clobbering the caller's buffer after Decode must not corrupt the result.
func TestFooterDecoderInputOwnership(t *testing.T) {
	footers := loadAllFooters(t)
	decoder := new(format.FooterDecoder)

	for _, name := range sortedNames(footers) {
		footer := slices.Clone(footers[name])
		decoded, _, err := decoder.Decode(footer)
		if err != nil {
			t.Fatal(err)
		}
		for i := range footer {
			footer[i] = 0xff
		}
		if fresh := decodeFresh(t, footers[name]); !equalMetadata(decoded, fresh) {
			t.Errorf("%s: metadata corrupted after mutating the input buffer", name)
		}
	}
}

// TestFooterDecoderZeroAllocs checks the zero-allocation guarantee: after a
// warmup decode, repeated decodes of the same footer must not allocate.
func TestFooterDecoderZeroAllocs(t *testing.T) {
	footers := loadAllFooters(t)

	for _, name := range sortedNames(footers) {
		footer := footers[name]
		decoder := new(format.FooterDecoder)
		if _, _, err := decoder.Decode(footer); err != nil {
			t.Fatal(err)
		}
		allocs := testing.AllocsPerRun(10, func() {
			if _, _, err := decoder.Decode(footer); err != nil {
				t.Fatal(err)
			}
		})
		if allocs != 0 {
			t.Errorf("%s: %v allocs per decode, want 0", name, allocs)
		}
	}
}

func BenchmarkFooterDecoder(b *testing.B) {
	footers := loadAllFooters(b)

	for _, name := range []string{"trace.snappy.parquet", "nested_structs.rust.parquet", "alltypes_tiny_pages_plain.parquet"} {
		footer, ok := footers[name]
		if !ok {
			continue
		}
		b.Run(name, func(b *testing.B) {
			decoder := new(format.FooterDecoder)
			b.SetBytes(int64(len(footer)))
			b.ReportAllocs()
			for b.Loop() {
				if _, _, err := decoder.Decode(footer); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// equalMetadata compares two FileMetaData values, treating nil and empty
// slices as equal. The distinction is unavoidable when reusing decode
// targets: truncated slices retain their backing arrays where a fresh
// decode would leave the field nil.
func equalMetadata(a, b *format.FileMetaData) bool {
	return equalValue(reflect.ValueOf(a).Elem(), reflect.ValueOf(b).Elem())
}

// isNullType reports whether t has the shape of thrift.Null[T].
func isNullType(t reflect.Type) bool {
	return t.NumField() == 2 && t.Field(0).Name == "V" && t.Field(1).Name == "Valid"
}

func equalValue(a, b reflect.Value) bool {
	if a.Type() != b.Type() {
		return false
	}
	switch a.Kind() {
	case reflect.Pointer, reflect.Interface:
		if a.IsNil() || b.IsNil() {
			return a.IsNil() == b.IsNil()
		}
		return equalValue(a.Elem(), b.Elem())
	case reflect.Slice:
		if a.Len() != b.Len() {
			return false
		}
		for i := range a.Len() {
			if !equalValue(a.Index(i), b.Index(i)) {
				return false
			}
		}
		return true
	case reflect.Struct:
		// thrift.Null[T] values compare by validity first: when both are
		// invalid, the inner value is unobservable and may contain stale
		// data retained for reuse by Reset.
		if isNullType(a.Type()) {
			av, bv := a.FieldByName("Valid"), b.FieldByName("Valid")
			if av.Bool() != bv.Bool() {
				return false
			}
			if !av.Bool() {
				return true
			}
		}
		for i := range a.NumField() {
			if !equalValue(a.Field(i), b.Field(i)) {
				return false
			}
		}
		return true
	default:
		return a.Interface() == b.Interface()
	}
}
