package parquet

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"
)

type schemeRow struct {
	Name  string `parquet:"name,dict"`
	Value int64  `parquet:"value"`
}

type schemeKeys struct {
	footer  []byte
	columns map[string][]byte
}

func (k schemeKeys) FooterKey([]byte) ([]byte, error) { return k.footer, nil }

func (k schemeKeys) ColumnKey(path []string, _ []byte) ([]byte, error) {
	if key, ok := k.columns[strings.Join(path, ".")]; ok {
		return key, nil
	}
	return k.footer, nil
}

func TestLegacyFixtureRequiresLegacyAAD(t *testing.T) {
	footerKey := bytes.Repeat([]byte{0x11}, 16)
	nameKey := bytes.Repeat([]byte{0x22}, 16)
	keys := schemeKeys{footer: footerKey, columns: map[string][]byte{"name": nameKey}}
	for _, path := range []string{
		"testdata/encryption/legacy-encrypted-footer.parquet",
		"testdata/encryption/legacy-plaintext-footer.parquet",
	} {
		t.Run(path, func(t *testing.T) {
			f := openSchemeFile(t, path, keys)
			if !f.legacyModules.Load() {
				t.Fatal("file written before the module-type fix did not authenticate with the legacy AAD")
			}
			// value is encrypted with the footer key. Its column index must
			// reject the spec module id and accept the id this package used to write.
			chunk := &f.metadata.RowGroups[0].Columns[1]
			env := readFileRange(t, f, chunk.ColumnIndexOffset, int64(chunk.ColumnIndexLength))
			if _, err := decryptModule(footerKey, makeAAD(f.aadPrefix, f.fileUnique, columnIndexModule, 0, 1), env); err == nil {
				t.Fatal("spec column-index AAD decrypted a legacy module")
			}
			if _, err := decryptModule(footerKey, makeAAD(f.aadPrefix, f.fileUnique, legacyColumnIndexModule, 0, 1), env); err != nil {
				t.Fatalf("legacy column-index AAD: %v", err)
			}
		})
	}
}

func TestWriterEncryptionFollowsSpec(t *testing.T) {
	footerKey := bytes.Repeat([]byte{0x31}, 16)
	nameKey := bytes.Repeat([]byte{0x32}, 16)
	rows := []schemeRow{
		{Name: "alpha", Value: 1},
		{Name: "beta", Value: 2},
		{Name: "alpha", Value: 3},
		{Name: "gamma", Value: 4},
	}
	var buf bytes.Buffer
	w := NewGenericWriter[schemeRow](&buf,
		WithEncryption(&EncryptionConfig{
			FooterKey: footerKey,
			ColumnKeys: map[string][]byte{
				"name": nameKey,
			},
			EncryptedFooter: false,
			FileIdentifier:  []byte{9, 8, 7, 6, 5, 4, 3, 2},
			AadPrefix:       []byte("spec"),
		}),
		BloomFilters(SplitBlockFilter(10, "name")),
	)
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	data := buf.Bytes()
	keys := schemeKeys{footer: footerKey, columns: map[string][]byte{"name": nameKey}}
	f := openSchemeBytes(t, data, keys)
	if f.legacyModules.Load() {
		t.Fatal("file written by this package authenticated with the legacy AAD")
	}
	got := make([]schemeRow, 0, len(rows))
	r := NewGenericReader[schemeRow](f)
	scratch := make([]schemeRow, len(rows))
	for {
		n, err := r.Read(scratch)
		got = append(got, scratch[:n]...)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	r.Close()
	if len(got) != len(rows) {
		t.Fatalf("read %d rows, wrote %d", len(got), len(rows))
	}
	for i := range rows {
		if got[i] != rows[i] {
			t.Fatalf("row %d = %+v, want %+v", i, got[i], rows[i])
		}
	}

	name := &f.metadata.RowGroups[0].Columns[0]
	env := readFileRange(t, f, name.ColumnIndexOffset, int64(name.ColumnIndexLength))
	if _, err := decryptModule(nameKey, makeAAD(f.aadPrefix, f.fileUnique, columnIndexModule, 0, 0), env); err != nil {
		t.Fatalf("spec column index: %v", err)
	}
	if _, err := decryptModule(nameKey, makeAAD(f.aadPrefix, f.fileUnique, legacyColumnIndexModule, 0, 0), env); err == nil {
		t.Fatal("legacy column-index module id also authenticated")
	}

	if name.MetaData.DictionaryPageOffset == 0 || name.MetaData.DataPageOffset <= name.MetaData.DictionaryPageOffset {
		t.Fatalf("dictionary page offset %d data page offset %d", name.MetaData.DictionaryPageOffset, name.MetaData.DataPageOffset)
	}
	dict := readFileRange(t, f, name.MetaData.DictionaryPageOffset, name.MetaData.DataPageOffset-name.MetaData.DictionaryPageOffset)
	if _, err := decryptModule(nameKey, makeAAD(f.aadPrefix, f.fileUnique, dictPageHeaderModule, 0, 0), dict); err != nil {
		t.Fatalf("spec dictionary page header: %v", err)
	}
	if _, err := decryptModule(nameKey, makeAAD(f.aadPrefix, f.fileUnique, dictPageHeaderModule, 0, 0, 0), dict); err == nil {
		t.Fatal("dictionary page header authenticated with a page ordinal")
	}
	body := dict[4+binary.LittleEndian.Uint32(dict[:4]):]
	if _, err := decryptModule(nameKey, makeAAD(f.aadPrefix, f.fileUnique, dictPageBodyModule, 0, 0), body); err != nil {
		t.Fatalf("spec dictionary page: %v", err)
	}
	if _, err := decryptModule(nameKey, makeAAD(f.aadPrefix, f.fileUnique, legacyDictPageBodyModule, 0, 0, 0), body); err == nil {
		t.Fatal("legacy dictionary page module id also authenticated")
	}

	dataHdr := readFileRange(t, f, name.MetaData.DataPageOffset, 256)
	if _, err := decryptModule(nameKey, makeAAD(f.aadPrefix, f.fileUnique, dataPageHeaderModule, 0, 0, 0), dataHdr); err != nil {
		t.Fatalf("spec data page header: %v", err)
	}
	if _, err := decryptModule(nameKey, makeAAD(f.aadPrefix, f.fileUnique, legacyDataPageHeaderModule, 0, 0, 0), dataHdr); err == nil {
		t.Fatal("legacy data page header module id also authenticated")
	}

	if name.MetaData.BloomFilterOffset == 0 {
		t.Fatal("missing bloom filter")
	}
	bloom := readFileRange(t, f, name.MetaData.BloomFilterOffset, int64(name.MetaData.BloomFilterLength))
	if _, err := decryptModule(nameKey, makeAAD(f.aadPrefix, f.fileUnique, bloomFilterHdrModule, 0, 0), bloom); err != nil {
		t.Fatalf("spec bloom filter header: %v", err)
	}
	if _, err := decryptModule(nameKey, makeAAD(f.aadPrefix, f.fileUnique, legacyBloomFilterHdrModule, 0, 0), bloom); err == nil {
		t.Fatal("legacy bloom filter header module id also authenticated")
	}
	bitsOff := 4 + binary.LittleEndian.Uint32(bloom[:4])
	if _, err := decryptModule(nameKey, makeAAD(f.aadPrefix, f.fileUnique, bloomFilterBitsModule, 0, 0), bloom[bitsOff:]); err != nil {
		t.Fatalf("spec bloom filter bitset: %v", err)
	}
	if _, err := decryptModule(nameKey, makeAAD(f.aadPrefix, f.fileUnique, legacyBloomFilterBitsModule, 0, 0), bloom[bitsOff:]); err == nil {
		t.Fatal("legacy bloom filter bitset module id also authenticated")
	}

	plain, sig := plaintextFooterAndSignature(t, data)
	aad := makeAAD(f.aadPrefix, f.fileUnique, footerModule)
	nonce, tag := sig[:encNonceSize], sig[encNonceSize:]
	ok, err := footerTagMatches(footerKey, nonce, plain, aad, tag)
	if err != nil || !ok {
		t.Fatalf("spec footer signature: ok=%v err=%v", ok, err)
	}
	legacyAAD := slices.Concat(aad, plain)
	ok, err = footerTagMatches(footerKey, nonce, nil, legacyAAD, tag)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("legacy footer signature also matched")
	}

	var enc bytes.Buffer
	ew := NewGenericWriter[schemeRow](&enc, WithEncryption(&EncryptionConfig{
		FooterKey:       footerKey,
		EncryptedFooter: true,
		FileIdentifier:  []byte{1, 1, 1, 1, 2, 2, 2, 2},
	}))
	if _, err := ew.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := ew.Close(); err != nil {
		t.Fatal(err)
	}
	ef := openSchemeBytes(t, enc.Bytes(), schemeKeys{footer: footerKey})
	if n := encryptedFooterPadding(t, enc.Bytes(), footerKey, ef.aadPrefix, ef.fileUnique); n != 0 {
		t.Fatalf("encrypted footer plaintext has %d trailing bytes", n)
	}
}

func TestEncryptionPlaintextFooterMetadataReuse(t *testing.T) {
	keys := schemeKeys{
		footer:  bytes.Repeat([]byte{0x31}, 16),
		columns: map[string][]byte{"name": bytes.Repeat([]byte{0x32}, 16)},
	}
	var buf bytes.Buffer
	w := NewGenericWriter[schemeRow](&buf,
		WithEncryption(&EncryptionConfig{
			FooterKey:  keys.footer,
			ColumnKeys: keys.columns,
		}),
		Compression(&Gzip),
	)
	rows := []schemeRow{{Name: "alpha", Value: 1}, {Name: "beta", Value: 2}}
	for file := range 3 {
		if file > 0 {
			buf.Reset()
			w.Reset(&buf)
		}
		if _, err := w.Write(rows); err != nil {
			t.Fatal(err)
		}
		if err := w.Flush(); err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write(rows[:1]); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}

		plain, _ := plaintextFooterAndSignature(t, buf.Bytes())
		var footer format.FileMetaData
		if err := thrift.Unmarshal(&thrift.CompactProtocol{}, plain, &footer); err != nil {
			t.Fatal(err)
		}
		for i, rg := range footer.RowGroups {
			for j, c := range rg.Columns {
				if !reflect.DeepEqual(c.MetaData, format.ColumnMetaData{}) || len(c.EncryptedColumnMetadata) == 0 {
					t.Errorf("file %d row group %d column %d: plaintext metadata not hidden", file, i, j)
				}
			}
		}

		f := openSchemeBytes(t, buf.Bytes(), keys)
		if len(f.Metadata().RowGroups) != 2 {
			t.Fatalf("row groups = %d, want 2", len(f.Metadata().RowGroups))
		}
		for i, rg := range f.Metadata().RowGroups {
			for j, want := range []struct {
				typ      format.Type
				path     []string
				encoding []format.Encoding
			}{
				{format.ByteArray, []string{"name"}, []format.Encoding{format.Plain, format.RLEDictionary}},
				{format.Int64, []string{"value"}, []format.Encoding{format.Plain}},
			} {
				m := rg.Columns[j].MetaData
				if m.Type != want.typ || m.Codec != format.Gzip || !slices.Equal(m.PathInSchema, want.path) || !slices.Equal(m.Encoding, want.encoding) {
					t.Errorf("file %d row group %d column %d: metadata Type=%v Codec=%v PathInSchema=%v Encoding=%v, want %v GZIP %v %v",
						file, i, j, m.Type, m.Codec, m.PathInSchema, m.Encoding, want.typ, want.path, want.encoding)
				}
			}
		}
	}
}

func TestPlaintextFooterDeferredBloomFilters(t *testing.T) {
	footerKey := bytes.Repeat([]byte{0x41}, 16)
	nameKey := bytes.Repeat([]byte{0x42}, 16)
	keys := schemeKeys{footer: footerKey, columns: map[string][]byte{"name": nameKey}}
	cfg := &EncryptionConfig{
		FooterKey:       footerKey,
		ColumnKeys:      keys.columns,
		EncryptedFooter: false,
		FileIdentifier:  []byte{1, 2, 3, 4, 5, 6, 7, 8},
		AadPrefix:       []byte("bloom"),
	}
	// Each value is larger than the page buffer and rows are written one at a
	// time, so each row becomes its own page. The row groups then record
	// different encoding-stat counts. A metadata copy that still shares the
	// column writer's slice fails the comparison with the reference file.
	long := func(tag string, n int64) bloomSchemeRow {
		return bloomSchemeRow{Name: strings.Repeat(tag, 40), Value: n}
	}
	groups := [][]bloomSchemeRow{
		{long("a", 1), long("b", 2)},
		{long("c", 3), long("d", 4), long("e", 5)},
	}
	columnKeys := [][]byte{nameKey, footerKey}
	pageSize := PageBufferSize(32)

	deferred := writeSchemeBloomFile(t, cfg, groups, pageSize, DeferBloomFiltersWithBuffers(NewBufferPool()))
	reference := writeSchemeBloomFile(t, cfg, groups, pageSize)

	deferredMeta := decryptPlaintextFooterColumnMetadata(t, deferred, cfg, columnKeys)
	referenceMeta := decryptPlaintextFooterColumnMetadata(t, reference, cfg, columnKeys)
	if len(deferredMeta) != len(groups) || len(referenceMeta) != len(groups) {
		t.Fatalf("row groups: deferred %d reference %d, want %d", len(deferredMeta), len(referenceMeta), len(groups))
	}

	if slices.Equal(referenceMeta[0][0].EncodingStats, referenceMeta[1][0].EncodingStats) {
		t.Fatalf("row groups recorded the same encoding stats: %+v", referenceMeta[0][0].EncodingStats)
	}

	var bloomEnd int64
	for i, rg := range deferredMeta {
		for j, meta := range rg {
			ref := referenceMeta[i][j]
			if meta.BloomFilterOffset == 0 || meta.BloomFilterLength <= 0 {
				t.Errorf("row group %d column %d: decrypted bloom offset %d length %d", i, j, meta.BloomFilterOffset, meta.BloomFilterLength)
			}
			if meta.BloomFilterLength != ref.BloomFilterLength {
				t.Errorf("row group %d column %d: bloom length %d, reference %d", i, j, meta.BloomFilterLength, ref.BloomFilterLength)
			}
			if bloomEnd == 0 {
				bloomEnd = meta.BloomFilterOffset
			}
			if meta.BloomFilterOffset != bloomEnd {
				t.Errorf("row group %d column %d: bloom offset %d, want %d", i, j, meta.BloomFilterOffset, bloomEnd)
			}
			bloomEnd = meta.BloomFilterOffset + int64(meta.BloomFilterLength)

			if meta.Type != ref.Type || meta.Codec != ref.Codec || meta.NumValues != int64(len(groups[i])) ||
				meta.TotalUncompressedSize != ref.TotalUncompressedSize ||
				!slices.Equal(meta.PathInSchema, ref.PathInSchema) || !slices.Equal(meta.Encoding, ref.Encoding) ||
				!slices.Equal(meta.EncodingStats, ref.EncodingStats) || meta.DataPageOffset == 0 ||
				meta.DictionaryPageOffset != 0 ||
				meta.Statistics.NullCount != ref.Statistics.NullCount ||
				!bytes.Equal(meta.Statistics.Min, ref.Statistics.Min) || !bytes.Equal(meta.Statistics.Max, ref.Statistics.Max) ||
				!bytes.Equal(meta.Statistics.MinValue, ref.Statistics.MinValue) || !bytes.Equal(meta.Statistics.MaxValue, ref.Statistics.MaxValue) {
				t.Errorf("row group %d column %d: got type=%v codec=%v values=%d uncompressed=%d path=%v enc=%v stats=%+v min=%q max=%q; ref type=%v codec=%v values=%d uncompressed=%d path=%v enc=%v stats=%+v min=%q max=%q",
					i, j,
					meta.Type, meta.Codec, meta.NumValues, meta.TotalUncompressedSize, meta.PathInSchema, meta.Encoding, meta.EncodingStats, meta.Statistics.MinValue, meta.Statistics.MaxValue,
					ref.Type, ref.Codec, ref.NumValues, ref.TotalUncompressedSize, ref.PathInSchema, ref.Encoding, ref.EncodingStats, ref.Statistics.MinValue, ref.Statistics.MaxValue)
			}
		}
	}

	f := openSchemeBytes(t, deferred, keys)
	if len(f.Metadata().RowGroups) != len(groups) {
		t.Fatalf("reader row groups = %d, want %d", len(f.Metadata().RowGroups), len(groups))
	}
	for i, rg := range f.Metadata().RowGroups {
		for j := range rg.Columns {
			got := rg.Columns[j].MetaData
			want := deferredMeta[i][j]
			if got.BloomFilterOffset != want.BloomFilterOffset || got.BloomFilterLength != want.BloomFilterLength {
				t.Errorf("row group %d column %d: reader bloom offset %d length %d, decrypted blob %d %d",
					i, j, got.BloomFilterOffset, got.BloomFilterLength, want.BloomFilterOffset, want.BloomFilterLength)
			}
		}
		chunks := f.RowGroups()[i].ColumnChunks()
		checks := []struct {
			column int
			value  Value
			want   bool
		}{
			{0, ValueOf(groups[i][0].Name), true},
			{0, ValueOf(groups[1-i][0].Name), false},
			{1, ValueOf(groups[i][0].Value), true},
			{1, ValueOf(groups[1-i][0].Value), false},
		}
		for _, check := range checks {
			bf := chunks[check.column].BloomFilter()
			if bf == nil {
				t.Errorf("row group %d column %d: missing bloom filter", i, check.column)
				continue
			}
			ok, err := bf.Check(check.value)
			if err != nil {
				t.Errorf("row group %d column %d: Check(%v): %v", i, check.column, check.value, err)
				continue
			}
			if ok != check.want {
				t.Errorf("row group %d column %d: Check(%v) = %v, want %v", i, check.column, check.value, ok, check.want)
			}
		}
	}
}

type bloomSchemeRow struct {
	Name  string `parquet:"name"`
	Value int64  `parquet:"value"`
}

func writeSchemeBloomFile(t *testing.T, cfg *EncryptionConfig, groups [][]bloomSchemeRow, opts ...WriterOption) []byte {
	t.Helper()
	var buf bytes.Buffer
	options := make([]WriterOption, 0, 2+len(opts))
	options = append(options,
		WithEncryption(cfg),
		BloomFilters(SplitBlockFilter(10, "name"), SplitBlockFilter(10, "value")),
	)
	options = append(options, opts...)
	w := NewGenericWriter[bloomSchemeRow](&buf, options...)
	for i, rows := range groups {
		for _, row := range rows {
			if _, err := w.Write([]bloomSchemeRow{row}); err != nil {
				t.Fatal(err)
			}
		}
		if i+1 < len(groups) {
			if err := w.Flush(); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// decryptPlaintextFooterColumnMetadata reads column metadata from the encrypted
// blobs only. Unmarshalling into a fresh value ignores plaintext fields that a
// decode into the footer struct would leave in place.
func decryptPlaintextFooterColumnMetadata(t *testing.T, data []byte, cfg *EncryptionConfig, columnKeys [][]byte) [][]format.ColumnMetaData {
	t.Helper()
	plain, _ := plaintextFooterAndSignature(t, data)
	var footer format.FileMetaData
	if err := thrift.Unmarshal(&thrift.CompactProtocol{}, plain, &footer); err != nil {
		t.Fatal(err)
	}
	out := make([][]format.ColumnMetaData, len(footer.RowGroups))
	for i, rg := range footer.RowGroups {
		if rg.Ordinal != int16(i) {
			t.Fatalf("row group %d ordinal = %d", i, rg.Ordinal)
		}
		if len(rg.Columns) != len(columnKeys) {
			t.Fatalf("row group %d columns = %d, want %d", i, len(rg.Columns), len(columnKeys))
		}
		out[i] = make([]format.ColumnMetaData, len(rg.Columns))
		for j, c := range rg.Columns {
			if !reflect.DeepEqual(c.MetaData, format.ColumnMetaData{}) || len(c.EncryptedColumnMetadata) == 0 {
				t.Fatalf("row group %d column %d: plaintext metadata %+v encrypted length %d", i, j, c.MetaData, len(c.EncryptedColumnMetadata))
			}
			aad := makeAAD(cfg.AadPrefix, cfg.FileIdentifier, columnMetaDataModule, rg.Ordinal, int16(j))
			encoded, err := decryptModule(columnKeys[j], aad, c.EncryptedColumnMetadata)
			if err != nil {
				t.Fatalf("row group %d column %d: decrypt column metadata: %v", i, j, err)
			}
			if err := thrift.Unmarshal(&thrift.CompactProtocol{}, encoded, &out[i][j]); err != nil {
				t.Fatalf("row group %d column %d: decode column metadata: %v", i, j, err)
			}
		}
	}
	return out
}

func encryptedFooterPadding(t *testing.T, data, key, prefix, unique []byte) int {
	t.Helper()
	footerSize := binary.LittleEndian.Uint32(data[len(data)-8 : len(data)-4])
	footer := data[len(data)-8-int(footerSize) : len(data)-8]
	var crypto format.FileCryptoMetaData
	pr := (&thrift.CompactProtocol{}).NewReaderFromBytes(footer)
	if err := thrift.NewDecoder(pr).Decode(&crypto); err != nil {
		t.Fatal(err)
	}
	plain, err := decryptModule(key, makeAAD(prefix, unique, footerModule), footer[pr.BytesRead():])
	if err != nil {
		t.Fatal(err)
	}
	r := (&thrift.CompactProtocol{}).NewReaderFromBytes(plain)
	var meta format.FileMetaData
	if err := thrift.NewDecoder(r).Decode(&meta); err != nil {
		t.Fatal(err)
	}
	return len(plain) - r.BytesRead()
}

func plaintextFooterAndSignature(t *testing.T, data []byte) (plain, sig []byte) {
	t.Helper()
	footerSize := binary.LittleEndian.Uint32(data[len(data)-8 : len(data)-4])
	footer := data[len(data)-8-int(footerSize) : len(data)-8]
	var meta format.FileMetaData
	pr := (&thrift.CompactProtocol{}).NewReaderFromBytes(footer)
	if err := thrift.NewDecoder(pr).Decode(&meta); err != nil {
		t.Fatal(err)
	}
	if got := len(footer) - pr.BytesRead(); got != encNonceSize+encTagSize {
		t.Fatalf("footer signature length = %d", got)
	}
	return footer[:pr.BytesRead()], footer[pr.BytesRead():]
}

func openSchemeFile(t *testing.T, path string, keys schemeKeys) *File {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return openSchemeBytes(t, data, keys)
}

func openSchemeBytes(t *testing.T, data []byte, keys schemeKeys) *File {
	t.Helper()
	f, err := OpenFile(bytes.NewReader(data), int64(len(data)), WithDecryption(keys))
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func readFileRange(t *testing.T, f *File, off, n int64) []byte {
	t.Helper()
	buf := make([]byte, n)
	if _, err := f.ReadAt(buf, off); err != nil && err != io.EOF {
		t.Fatalf("read at %d len %d: %v", off, n, err)
	}
	return buf
}
