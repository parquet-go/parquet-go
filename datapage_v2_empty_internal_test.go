package parquet

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/parquet-go/parquet-go/compress"
	"github.com/parquet-go/parquet-go/encoding/rle"
	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"
)

// datapage_v2_empty_datapage.snappy.parquet and page_v2_empty_compressed.parquet
// are interoperability fixtures from apache/parquet-testing at
// 0e00127a431f83388d38eebb22cf644d06e86a15.
func TestDataPageV2EmptyDataPageFixtures(t *testing.T) {
	t.Run("datapage_v2_empty_datapage.snappy.parquet", func(t *testing.T) {
		type Row struct {
			Value *float32 `parquet:"value"`
		}
		rows, err := ReadFile[Row]("./testdata/datapage_v2_empty_datapage.snappy.parquet")
		if err != nil {
			t.Fatalf("ReadFile: %v", err)
		}
		if len(rows) != 1 {
			t.Fatalf("got %d rows, want 1", len(rows))
		}
		if rows[0].Value != nil {
			t.Fatalf("got %v, want nil", *rows[0].Value)
		}
	})

	t.Run("page_v2_empty_compressed.parquet", func(t *testing.T) {
		type Row struct {
			IntegerColumn *int32 `parquet:"integer_column"`
		}
		rows, err := ReadFile[Row]("./testdata/page_v2_empty_compressed.parquet")
		if err != nil {
			t.Fatalf("ReadFile: %v", err)
		}
		if len(rows) != 10 {
			t.Fatalf("got %d rows, want 10", len(rows))
		}
		for i, row := range rows {
			if row.IntegerColumn != nil {
				t.Fatalf("row %d: got %v, want nil", i, *row.IntegerColumn)
			}
		}
	})
}

func TestDataPageV2EmptyDataSection(t *testing.T) {
	codecs := []compress.Codec{
		&Snappy,
		&Gzip,
		&Brotli,
		&Zstd,
		&Lz4Raw,
	}

	encodeLevels := func(t *testing.T, bitWidth int, levels []byte) []byte {
		t.Helper()
		enc := rle.Encoding{BitWidth: bitWidth}
		out, err := enc.EncodeLevels(nil, levels)
		if err != nil {
			t.Fatal(err)
		}
		return out
	}

	readAllValues := func(t *testing.T, page Page) []Value {
		t.Helper()
		vr := page.Values()
		var out []Value
		buf := make([]Value, 8)
		for {
			n, err := vr.ReadValues(buf)
			out = append(out, buf[:n]...)
			if err == io.EOF {
				return out
			}
			if err != nil {
				t.Fatalf("ReadValues: %v", err)
			}
		}
	}

	for _, codec := range codecs {
		t.Run(codec.String(), func(t *testing.T) {
			t.Run("optional all nulls", func(t *testing.T) {
				defLevels := encodeLevels(t, 1, []byte{0, 0, 0, 0})
				col := &Column{
					typ:                FloatType,
					schema:             &format.SchemaElement{Name: "v"},
					compression:        codec,
					maxDefinitionLevel: 1,
				}
				hdr := DataPageHeaderV2{header: &format.DataPageHeaderV2{
					NumValues:                  4,
					NumNulls:                   4,
					NumRows:                    4,
					Encoding:                   format.Plain,
					DefinitionLevelsByteLength: int32(len(defLevels)),
					IsCompressed:               thrift.New(true),
				}}
				page, err := col.decodeDataPageV2(hdr, newBuffer(defLevels), nil, int32(len(defLevels)))
				if err != nil {
					t.Fatalf("decodeDataPageV2: %v", err)
				}
				defer Release(page)

				if page.NumValues() != 4 || page.NumNulls() != 4 || page.NumRows() != 4 {
					t.Fatalf("counts = (%d, %d, %d), want (4, 4, 4)", page.NumValues(), page.NumNulls(), page.NumRows())
				}
				vals := readAllValues(t, page)
				if len(vals) != 4 {
					t.Fatalf("got %d values, want 4", len(vals))
				}
				for i, v := range vals {
					if !v.IsNull() {
						t.Fatalf("value %d = %v, want null", i, v)
					}
				}
			})

			t.Run("repeated optional all nulls via DecodeDataPageV2", func(t *testing.T) {
				repLevels := encodeLevels(t, 1, []byte{0, 0, 0})
				defLevels := encodeLevels(t, 2, []byte{0, 1, 0})
				raw := append(append([]byte(nil), repLevels...), defLevels...)

				col := &Column{
					typ:                Int32Type,
					schema:             &format.SchemaElement{Name: "v"},
					compression:        codec,
					maxRepetitionLevel: 1,
					maxDefinitionLevel: 2,
				}
				hdr := DataPageHeaderV2{header: &format.DataPageHeaderV2{
					NumValues:                  3,
					NumNulls:                   3,
					NumRows:                    3,
					Encoding:                   format.Plain,
					RepetitionLevelsByteLength: int32(len(repLevels)),
					DefinitionLevelsByteLength: int32(len(defLevels)),
					IsCompressed:               thrift.New(true),
				}}
				page, err := col.DecodeDataPageV2(hdr, raw, nil)
				if err != nil {
					t.Fatalf("DecodeDataPageV2: %v", err)
				}
				defer Release(page)

				vals := readAllValues(t, page)
				if len(vals) != 3 {
					t.Fatalf("got %d values, want 3", len(vals))
				}
				for i, v := range vals {
					if !v.IsNull() {
						t.Fatalf("value %d = %v, want null", i, v)
					}
				}
			})

			t.Run("zero values empty page", func(t *testing.T) {
				col := &Column{
					typ:         Int64Type,
					schema:      &format.SchemaElement{Name: "v"},
					compression: codec,
				}
				hdr := DataPageHeaderV2{header: &format.DataPageHeaderV2{
					NumValues:    0,
					NumNulls:     0,
					NumRows:      0,
					Encoding:     format.Plain,
					IsCompressed: thrift.New(true),
				}}
				page, err := col.decodeDataPageV2(hdr, newBuffer[byte](nil), nil, 0)
				if err != nil {
					t.Fatalf("decodeDataPageV2: %v", err)
				}
				defer Release(page)

				if page.NumValues() != 0 || page.NumNulls() != 0 {
					t.Fatalf("counts = (%d, %d), want (0, 0)", page.NumValues(), page.NumNulls())
				}
			})

			t.Run("non-empty compressed all-null roundtrip", func(t *testing.T) {
				type Row struct {
					Plain *int32  `parquet:"plain"`
					Dict  *string `parquet:"dict"`
					Delta *int64  `parquet:"delta"`
				}
				schema := NewSchema("Row", Group{
					"plain": Optional(Int(32)),
					"dict":  Optional(Encoded(String(), &RLEDictionary)),
					"delta": Optional(Encoded(Int(64), &DeltaBinaryPacked)),
				})
				var buf bytes.Buffer
				w := NewGenericWriter[Row](&buf, schema, DataPageVersion(2), Compression(codec))
				in := []Row{{}, {}, {}}
				if _, err := w.Write(in); err != nil {
					t.Fatalf("Write: %v", err)
				}
				if err := w.Close(); err != nil {
					t.Fatalf("Close: %v", err)
				}

				r := NewGenericReader[Row](bytes.NewReader(buf.Bytes()))
				defer r.Close()
				out := make([]Row, len(in))
				n, err := r.Read(out)
				if err != nil && err != io.EOF {
					t.Fatalf("Read: %v", err)
				}
				if n != len(in) {
					t.Fatalf("read %d rows, want %d", n, len(in))
				}
				for i, row := range out {
					if row.Plain != nil || row.Dict != nil || row.Delta != nil {
						t.Fatalf("row %d = %+v, want all nil", i, row)
					}
				}
			})
		})
	}

	// An empty data section that claims non-null values must be rejected before
	// invoking any codec, covering codecs that fail on 0-byte input (Snappy,
	// Brotli), return an empty slice (Zstd, Lz4Raw), panic on EOF (Gzip), or
	// skip decompression (Uncompressed).
	for _, codec := range append([]compress.Codec{&Uncompressed}, codecs...) {
		t.Run(codec.String()+"/truncated non-null dictionary page", func(t *testing.T) {
			col := &Column{
				typ:         ByteArrayType,
				schema:      &format.SchemaElement{Name: "s"},
				compression: codec,
			}
			dict := newTestByteArrayDictionary("a", "b")
			hdr := DataPageHeaderV2{header: &format.DataPageHeaderV2{
				NumValues:    3,
				NumNulls:     0,
				NumRows:      3,
				Encoding:     format.RLEDictionary,
				IsCompressed: thrift.New(codec != &Uncompressed),
			}}
			page, err := col.decodeDataPageV2(hdr, newBuffer[byte](nil), dict, 3)
			if page != nil {
				Release(page)
			}
			if !errors.Is(err, ErrCorrupted) {
				t.Fatalf("got %v, want error wrapping ErrCorrupted", err)
			}
		})
	}

	t.Run("num_nulls exceeds num_values", func(t *testing.T) {
		col := &Column{
			typ:         ByteArrayType,
			schema:      &format.SchemaElement{Name: "s"},
			compression: &Zstd,
		}
		dict := newTestByteArrayDictionary("a", "b")
		hdr := DataPageHeaderV2{header: &format.DataPageHeaderV2{
			NumValues:    1,
			NumNulls:     2,
			NumRows:      1,
			Encoding:     format.RLEDictionary,
			IsCompressed: thrift.New(true),
		}}
		page, err := col.decodeDataPageV2(hdr, newBuffer[byte](nil), dict, 3)
		if page != nil {
			Release(page)
		}
		if !errors.Is(err, ErrCorrupted) {
			t.Fatalf("got %v, want error wrapping ErrCorrupted", err)
		}
	})
}
