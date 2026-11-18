package parquet

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
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
	legacyAAD := append(append([]byte{}, aad...), plain...)
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
