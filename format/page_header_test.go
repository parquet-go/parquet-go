package format_test

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"
)

// pageHeaderFixtures covers the page header shapes seen in real files, in an
// order that exercises reuse hazards: a header with statistics followed by
// one without (stale statistics must not survive), and different sub-header
// arms following one another (a stale arm must not remain valid).
var pageHeaderFixtures = []format.PageHeader{
	{
		Type:                 format.DataPage,
		UncompressedPageSize: 4096,
		CompressedPageSize:   2048,
		CRC:                  12345,
		DataPageHeader: thrift.Null[format.DataPageHeader]{
			V: format.DataPageHeader{
				NumValues:               1000,
				Encoding:                format.Plain,
				DefinitionLevelEncoding: format.RLE,
				RepetitionLevelEncoding: format.RLE,
				Statistics: format.Statistics{
					NullCount:     10,
					DistinctCount: 100,
					MinValue:      []byte{1, 2, 3, 4},
					MaxValue:      []byte{250, 251, 252, 253},
				},
			},
			Valid: true,
		},
	},
	{
		Type:                 format.DataPage,
		UncompressedPageSize: 512,
		CompressedPageSize:   256,
		DataPageHeader: thrift.Null[format.DataPageHeader]{
			V: format.DataPageHeader{
				NumValues:               8,
				Encoding:                format.RLEDictionary,
				DefinitionLevelEncoding: format.RLE,
				RepetitionLevelEncoding: format.RLE,
			},
			Valid: true,
		},
	},
	{
		Type:                 format.DictionaryPage,
		UncompressedPageSize: 128,
		CompressedPageSize:   128,
		DictionaryPageHeader: thrift.Null[format.DictionaryPageHeader]{
			V: format.DictionaryPageHeader{
				NumValues: 16,
				Encoding:  format.Plain,
				IsSorted:  true,
			},
			Valid: true,
		},
	},
	{
		Type:                 format.DataPageV2,
		UncompressedPageSize: 8192,
		CompressedPageSize:   4096,
		DataPageHeaderV2: thrift.Null[format.DataPageHeaderV2]{
			V: format.DataPageHeaderV2{
				NumValues:                  2000,
				NumNulls:                   20,
				NumRows:                    1980,
				Encoding:                   format.DeltaBinaryPacked,
				DefinitionLevelsByteLength: 100,
				RepetitionLevelsByteLength: 50,
				IsCompressed:               thrift.New(true),
				Statistics: format.Statistics{
					NullCount: 20,
					MinValue:  []byte{9},
					MaxValue:  []byte{200, 201},
				},
			},
			Valid: true,
		},
	},
}

// TestPageHeaderDecodeReuse checks that decoding a sequence of page headers
// into a single reused header (Reset between decodes, as FilePages does)
// produces the same values as decoding each header into a fresh struct.
func TestPageHeaderDecodeReuse(t *testing.T) {
	protocol := &thrift.CompactProtocol{}

	rd := bytes.NewReader(nil)
	decoder := thrift.NewDecoder(protocol.NewReader(rd))
	reused := new(format.PageHeader)

	for i, fixture := range pageHeaderFixtures {
		data, err := thrift.Marshal(protocol, fixture)
		if err != nil {
			t.Fatalf("fixture %d: marshal: %v", i, err)
		}

		fresh := new(format.PageHeader)
		if err := thrift.Unmarshal(protocol, data, fresh); err != nil {
			t.Fatalf("fixture %d: unmarshal into fresh header: %v", i, err)
		}

		rd.Reset(data)
		reused.Reset()
		if err := decoder.Decode(reused); err != nil {
			t.Fatalf("fixture %d: decode into reused header: %v", i, err)
		}

		if !reflect.DeepEqual(normalizePageHeader(reused), normalizePageHeader(fresh)) {
			t.Errorf("fixture %d: reused decode differs from fresh decode:\nreused: %+v\nfresh:  %+v", i, reused, fresh)
		}
	}
}

// TestPageHeaderDecodeReuseZeroAlloc checks that once the reused header has
// grown to the shape of the input, decoding does not allocate.
func TestPageHeaderDecodeReuseZeroAlloc(t *testing.T) {
	protocol := &thrift.CompactProtocol{}

	inputs := make([][]byte, len(pageHeaderFixtures))
	for i, fixture := range pageHeaderFixtures {
		data, err := thrift.Marshal(protocol, fixture)
		if err != nil {
			t.Fatalf("fixture %d: marshal: %v", i, err)
		}
		inputs[i] = data
	}

	rd := bytes.NewReader(nil)
	decoder := thrift.NewDecoder(protocol.NewReader(rd))
	reused := new(format.PageHeader)

	allocs := testing.AllocsPerRun(100, func() {
		for _, data := range inputs {
			rd.Reset(data)
			reused.Reset()
			if err := decoder.Decode(reused); err != nil {
				t.Fatal(err)
			}
		}
	})
	if allocs != 0 {
		t.Errorf("expected zero allocations per decode cycle, got %g", allocs)
	}
}

// normalizePageHeader nils zero-length byte slices in the statistics so that
// headers decoded into a reused struct (whose Reset truncates the slices,
// leaving them empty but non-nil) compare equal to headers decoded into a
// fresh struct (where absent fields are left nil). The two states are
// semantically equivalent.
func normalizePageHeader(h *format.PageHeader) *format.PageHeader {
	c := *h
	c.DataPageHeader.V.Statistics = normalizeStatistics(c.DataPageHeader.V.Statistics)
	c.DataPageHeaderV2.V.Statistics = normalizeStatistics(c.DataPageHeaderV2.V.Statistics)
	return &c
}

func normalizeStatistics(s format.Statistics) format.Statistics {
	nilIfEmpty := func(b []byte) []byte {
		if len(b) == 0 {
			return nil
		}
		return b
	}
	s.Max = nilIfEmpty(s.Max)
	s.Min = nilIfEmpty(s.Min)
	s.MaxValue = nilIfEmpty(s.MaxValue)
	s.MinValue = nilIfEmpty(s.MinValue)
	return s
}
