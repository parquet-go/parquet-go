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

// maximalFileMetaData returns a FileMetaData with every optional field
// populated, including the ones no testdata footer exercises (sorting
// columns, non-zero ordinals, file paths, crypto metadata, geospatial
// statistics, geometry/geography logical types, ...). It anchors the reuse
// tests below to the full type tree instead of whatever testdata contains.
func maximalFileMetaData() *format.FileMetaData {
	return &format.FileMetaData{
		Version: 2,
		Schema: thrift.Slice[format.SchemaElement]{
			{
				Name:        "root",
				NumChildren: thrift.New(int32(2)),
			},
			{
				Type:           thrift.New(format.ByteArray),
				RepetitionType: thrift.New(format.Optional),
				Name:           "geometry",
				FieldID:        7,
				LogicalType: format.LogicalType{
					Value: &format.GeometryType{CRS: "EPSG:4326"},
				},
			},
			{
				Type:           thrift.New(format.ByteArray),
				TypeLength:     thrift.New(int32(16)),
				RepetitionType: thrift.New(format.Required),
				Name:           "geography",
				Scale:          thrift.New(int32(2)),
				Precision:      thrift.New(int32(10)),
				LogicalType: format.LogicalType{
					Value: &format.GeographyType{CRS: "OGC:CRS84", Algorithm: 1},
				},
			},
		},
		NumRows: 3,
		RowGroups: thrift.Slice[format.RowGroup]{
			{
				Columns: thrift.Slice[format.ColumnChunk]{
					{
						FilePath:   "part-0001.parquet",
						FileOffset: 4,
						MetaData: format.ColumnMetaData{
							Type:                  format.ByteArray,
							Encoding:              thrift.Slice[format.Encoding]{format.Plain},
							PathInSchema:          thrift.Slice[string]{"geometry"},
							Codec:                 format.Snappy,
							NumValues:             3,
							TotalUncompressedSize: 100,
							TotalCompressedSize:   80,
							KeyValueMetadata:      []format.KeyValue{{Key: "ck", Value: "cv"}},
							DataPageOffset:        4,
							IndexPageOffset:       8,
							DictionaryPageOffset:  12,
							Statistics: format.Statistics{
								Max: []byte("z"), Min: []byte("a"),
								NullCount: 1, DistinctCount: 2,
								MaxValue: []byte("z"), MinValue: []byte("a"),
							},
							EncodingStats: []format.PageEncodingStats{
								{PageType: format.DataPage, Encoding: format.Plain, Count: 1},
							},
							BloomFilterOffset: 16,
							BloomFilterLength: 32,
							SizeStatistics: format.SizeStatistics{
								UnencodedByteArrayDataBytes: 7,
								RepetitionLevelHistogram:    []int64{1, 2},
								DefinitionLevelHistogram:    []int64{3, 4},
							},
							GeospatialStatistics: format.GeospatialStatistics{
								BBox: format.BoundingBox{
									XMin: 1, XMax: 2, YMin: 3, YMax: 4,
									ZMin: thrift.New(5.0), ZMax: thrift.New(6.0),
								},
								GeoSpatialTypes: []int32{1},
							},
						},
						OffsetIndexOffset: 20,
						OffsetIndexLength: 4,
						ColumnIndexOffset: 24,
						ColumnIndexLength: 4,
						CryptoMetadata: format.ColumnCryptoMetaData{
							Value: &format.EncryptionWithColumnKey{
								PathInSchema: []string{"geometry"},
								KeyMetadata:  []byte("key-meta"),
							},
						},
						EncryptedColumnMetadata: []byte("encrypted-blob"),
					},
				},
				TotalByteSize: 100,
				NumRows:       3,
				SortingColumns: []format.SortingColumn{
					{ColumnIdx: 0, Descending: true, NullsFirst: true},
				},
				FileOffset:          4,
				TotalCompressedSize: 80,
				Ordinal:             5,
			},
		},
		KeyValueMetadata: []format.KeyValue{{Key: "k", Value: "v"}},
		CreatedBy:        "synthetic-test",
		ColumnOrders: []format.ColumnOrder{
			{Value: &format.TypeDefinedOrder{}},
			{Value: &format.TypeDefinedOrder{}},
		},
		EncryptionAlgorithm: format.EncryptionAlgorithm{
			Value: &format.AesGcmV1{
				AadPrefix:       []byte("prefix"),
				AadFileUnique:   []byte("unique"),
				SupplyAadPrefix: true,
			},
		},
		FooterSigningKeyMetadata: []byte("signing-key"),
	}
}

// The minimal* structs mirror the thrift field IDs of FileMetaData but
// declare only the required fields, producing an input where every optional
// field is genuinely absent. Marshaling a format.FileMetaData cannot produce
// such an input for fields with writezero tags (e.g. RowGroup.Ordinal).
type minimalColumnMetaData struct {
	Type                  int32    `thrift:"1,required"`
	Encoding              []int32  `thrift:"2,required"`
	PathInSchema          []string `thrift:"3,required"`
	Codec                 int32    `thrift:"4,required"`
	NumValues             int64    `thrift:"5,required"`
	TotalUncompressedSize int64    `thrift:"6,required"`
	TotalCompressedSize   int64    `thrift:"7,required"`
	DataPageOffset        int64    `thrift:"9,required"`
}

type minimalColumnChunk struct {
	FileOffset int64                 `thrift:"2,required"`
	MetaData   minimalColumnMetaData `thrift:"3,optional"`
}

type minimalRowGroup struct {
	Columns       []minimalColumnChunk `thrift:"1,required"`
	TotalByteSize int64                `thrift:"2,required"`
	NumRows       int64                `thrift:"3,required"`
}

type minimalSchemaElement struct {
	Type        int32  `thrift:"1,optional"`
	Name        string `thrift:"4,required"`
	NumChildren int32  `thrift:"5,optional"`
}

type minimalFileMetaData struct {
	Version   int32                  `thrift:"1,required"`
	Schema    []minimalSchemaElement `thrift:"2,required"`
	NumRows   int64                  `thrift:"3,required"`
	RowGroups []minimalRowGroup      `thrift:"4,required"`
}

// TestFooterDecoderReuseSynthetic decodes a footer with every optional field
// populated and a footer with every optional field absent through the same
// FooterDecoder, in both orders, and checks the second result against a
// fresh decode. Unlike TestFooterDecoderReuse this does not depend on what
// the testdata footers happen to contain, so it covers the Reset methods of
// fields absent from all testdata (ordinals, sorting columns, file paths,
// crypto metadata, geospatial types, ...).
func TestFooterDecoderReuseSynthetic(t *testing.T) {
	protocol := &thrift.CompactProtocol{}

	maximal, err := thrift.Marshal(protocol, maximalFileMetaData())
	if err != nil {
		t.Fatal(err)
	}
	minimal, err := thrift.Marshal(protocol, &minimalFileMetaData{
		Version: 1,
		Schema: []minimalSchemaElement{
			{Name: "root", NumChildren: 1},
			{Type: int32(format.ByteArray), Name: "geometry"},
		},
		NumRows: 1,
		RowGroups: []minimalRowGroup{{
			Columns: []minimalColumnChunk{{
				FileOffset: 4,
				MetaData: minimalColumnMetaData{
					Type:                  int32(format.ByteArray),
					Encoding:              []int32{int32(format.Plain)},
					PathInSchema:          []string{"geometry"},
					NumValues:             1,
					TotalUncompressedSize: 10,
					TotalCompressedSize:   8,
					DataPageOffset:        4,
				},
			}},
			TotalByteSize: 10,
			NumRows:       1,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}

	footers := map[string][]byte{"maximal": maximal, "minimal": minimal}
	for first, firstFooter := range footers {
		for second, secondFooter := range footers {
			decoder := new(format.FooterDecoder)
			if _, _, err := decoder.Decode(firstFooter); err != nil {
				t.Fatalf("decoding %s: %v", first, err)
			}
			decoded, _, err := decoder.Decode(secondFooter)
			if err != nil {
				t.Fatalf("decoding %s after %s: %v", second, first, err)
			}
			if fresh := decodeFresh(t, secondFooter); !equalMetadata(decoded, fresh) {
				t.Errorf("decoding %s after %s: metadata differs from fresh decode", second, first)
			}
		}
	}
}

// TestSchemaElementResetClearsGeospatialMembers pins the CRS-clearing
// behavior of SchemaElement.Reset directly: retained Geometry/Geography
// union members must not keep strings that alias the previous decode's
// input buffer.
func TestSchemaElementResetClearsGeospatialMembers(t *testing.T) {
	s := format.SchemaElement{
		Name:        "geometry",
		LogicalType: format.LogicalType{Value: &format.GeometryType{CRS: "EPSG:4326"}},
	}
	s.Reset()
	if g, ok := s.LogicalType.Value.(*format.GeometryType); ok && *g != (format.GeometryType{}) {
		t.Errorf("Geometry member not cleared by Reset: %+v", *g)
	}

	s = format.SchemaElement{
		Name:        "geography",
		LogicalType: format.LogicalType{Value: &format.GeographyType{CRS: "OGC:CRS84", Algorithm: 1}},
	}
	s.Reset()
	if g, ok := s.LogicalType.Value.(*format.GeographyType); ok && *g != (format.GeographyType{}) {
		t.Errorf("Geography member not cleared by Reset: %+v", *g)
	}
}

// TestResetClearsCryptoUnionMembers pins the buffer-aliasing scrub of the
// two retained crypto unions, mirroring the CRS handling above: a retained
// EncryptionAlgorithm or CryptoMetadata member must not keep byte slices or
// strings that alias the previous decode's input buffer.
func TestResetClearsCryptoUnionMembers(t *testing.T) {
	m := format.FileMetaData{
		EncryptionAlgorithm: format.EncryptionAlgorithm{
			Value: &format.AesGcmV1{
				AadPrefix:       []byte("prefix"),
				AadFileUnique:   []byte("unique"),
				SupplyAadPrefix: true,
			},
		},
	}
	m.Reset()
	if v, ok := m.EncryptionAlgorithm.Value.(*format.AesGcmV1); !ok {
		t.Errorf("AesGcmV1 member allocation not retained by Reset")
	} else if v.AadPrefix != nil || v.AadFileUnique != nil || v.SupplyAadPrefix {
		t.Errorf("AesGcmV1 member not cleared by Reset: %+v", *v)
	}

	m = format.FileMetaData{
		EncryptionAlgorithm: format.EncryptionAlgorithm{
			Value: &format.AesGcmCtrV1{AadPrefix: []byte("prefix")},
		},
	}
	m.Reset()
	if v, ok := m.EncryptionAlgorithm.Value.(*format.AesGcmCtrV1); !ok {
		t.Errorf("AesGcmCtrV1 member allocation not retained by Reset")
	} else if v.AadPrefix != nil || v.AadFileUnique != nil || v.SupplyAadPrefix {
		t.Errorf("AesGcmCtrV1 member not cleared by Reset: %+v", *v)
	}

	c := format.ColumnChunk{
		CryptoMetadata: format.ColumnCryptoMetaData{
			Value: &format.EncryptionWithColumnKey{
				PathInSchema: thrift.Slice[string]{"a", "b"},
				KeyMetadata:  []byte("key"),
			},
		},
	}
	c.Reset()
	if v, ok := c.CryptoMetadata.Value.(*format.EncryptionWithColumnKey); !ok {
		t.Errorf("EncryptionWithColumnKey member allocation not retained by Reset")
	} else if v.PathInSchema != nil || v.KeyMetadata != nil {
		t.Errorf("EncryptionWithColumnKey member not cleared by Reset: %+v", *v)
	}
}

// TestFooterDecoderTrailingBytes checks the documented tolerance for
// trailing bytes: signed plaintext footers carry a 28-byte signature after
// the thrift payload, which Decode must not treat as an error, reporting the
// consumed byte count instead.
func TestFooterDecoderTrailingBytes(t *testing.T) {
	footers := loadAllFooters(t)
	decoder := new(format.FooterDecoder)

	for _, name := range sortedNames(footers) {
		footer := footers[name]
		signed := make([]byte, 0, len(footer)+28)
		signed = append(signed, footer...)
		for range 28 {
			signed = append(signed, 0xa5)
		}
		decoded, n, err := decoder.Decode(signed)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if n != len(footer) {
			t.Errorf("%s: consumed %d bytes, want %d", name, n, len(footer))
		}
		if fresh := decodeFresh(t, footer); !equalMetadata(decoded, fresh) {
			t.Errorf("%s: metadata differs from fresh decode", name)
		}
	}
}

// TestFooterDecoderErrorRecovery checks that a failed decode does not
// poison the decoder: decoding a valid footer after an invalid input must
// produce the same result as a fresh decode.
func TestFooterDecoderErrorRecovery(t *testing.T) {
	footers := loadAllFooters(t)
	decoder := new(format.FooterDecoder)

	// Truncated inputs fail partway through decoding, leaving partially
	// populated metadata that the next Decode must fully reset.
	for _, name := range sortedNames(footers) {
		footer := footers[name]
		if _, _, err := decoder.Decode(footer[:len(footer)/2]); err == nil {
			t.Fatalf("%s: expected error decoding truncated footer", name)
		}
		decoded, _, err := decoder.Decode(footer)
		if err != nil {
			t.Fatalf("%s: decoding after a failed decode: %v", name, err)
		}
		if fresh := decodeFresh(t, footer); !equalMetadata(decoded, fresh) {
			t.Errorf("%s: metadata differs from fresh decode after a failed decode", name)
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
