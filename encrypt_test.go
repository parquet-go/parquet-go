package parquet_test

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
)

// staticKeyRetriever is a test-only KeyRetriever backed by static key maps.
type staticKeyRetriever struct {
	footerKey  []byte
	columnKeys map[string][]byte // dot-joined path -> AES key
}

func (s *staticKeyRetriever) FooterKey(_ []byte) ([]byte, error) {
	return s.footerKey, nil
}

func (s *staticKeyRetriever) ColumnKey(path []string, _ []byte) ([]byte, error) {
	k := strings.Join(path, ".")
	if key, ok := s.columnKeys[k]; ok {
		return key, nil
	}
	return s.footerKey, nil
}

// encryptionTestRow is a simple row type used across encryption tests.
type encryptionTestRow struct {
	Name  string `parquet:"name"`
	Value int64  `parquet:"value"`
}

// aes128Key returns a 16-byte AES key with the given fill byte.
func aes128Key(b byte) []byte {
	key := make([]byte, 16)
	for i := range key {
		key[i] = b
	}
	return key
}

// writeEncrypted writes rows to a buffer with the given EncryptionConfig and returns the bytes.
func writeEncrypted(t *testing.T, rows []encryptionTestRow, cfg *parquet.EncryptionConfig) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[encryptionTestRow](&buf, parquet.WithEncryption(cfg))
	if _, err := w.Write(rows); err != nil {
		t.Fatalf("write rows: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	return buf.Bytes()
}

// readDecrypted opens the parquet data with the given KeyRetriever and reads all rows.
func readDecrypted(t *testing.T, data []byte, keys parquet.KeyRetriever) []encryptionTestRow {
	t.Helper()
	f, err := parquet.OpenFile(
		bytes.NewReader(data),
		int64(len(data)),
		parquet.WithDecryption(keys),
	)
	if err != nil {
		t.Fatalf("open file: %v", err)
	}
	r := parquet.NewGenericReader[encryptionTestRow](f)
	defer r.Close()

	var out []encryptionTestRow
	buf := make([]encryptionTestRow, 16)
	for {
		n, err := r.Read(buf)
		out = append(out, buf[:n]...)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("read rows: %v", err)
		}
	}
	return out
}

var testRows = []encryptionTestRow{
	{Name: "alice", Value: 1},
	{Name: "bob", Value: 2},
	{Name: "carol", Value: 3},
}

// TestEncryptionRoundTripEncryptedFooter tests write+read with the encrypted-footer ("PARE") mode.
func TestEncryptionRoundTripEncryptedFooter(t *testing.T) {
	footerKey := aes128Key(0xAA)
	cfg := &parquet.EncryptionConfig{
		FooterKey:       footerKey,
		EncryptedFooter: true,
		FileIdentifier:  []byte{1, 2, 3, 4, 5, 6, 7, 8},
	}
	data := writeEncrypted(t, testRows, cfg)

	keys := &staticKeyRetriever{footerKey: footerKey}
	got := readDecrypted(t, data, keys)
	assertRowsEqual(t, testRows, got)
}

// TestEncryptionRoundTripPlaintextFooter tests write+read with the plaintext-footer-with-signature mode.
func TestEncryptionRoundTripPlaintextFooter(t *testing.T) {
	footerKey := aes128Key(0xBB)
	cfg := &parquet.EncryptionConfig{
		FooterKey:       footerKey,
		EncryptedFooter: false,
		FileIdentifier:  []byte{9, 10, 11, 12, 13, 14, 15, 16},
	}
	data := writeEncrypted(t, testRows, cfg)

	keys := &staticKeyRetriever{footerKey: footerKey}
	got := readDecrypted(t, data, keys)
	assertRowsEqual(t, testRows, got)
}

// TestEncryptionPerColumnKeys tests that per-column keys encrypt/decrypt correctly.
func TestEncryptionPerColumnKeys(t *testing.T) {
	footerKey := aes128Key(0xCC)
	nameKey := aes128Key(0xDD)
	cfg := &parquet.EncryptionConfig{
		FooterKey: footerKey,
		ColumnKeys: map[string][]byte{
			"name": nameKey,
		},
		EncryptedFooter: true,
		FileIdentifier:  []byte{17, 18, 19, 20, 21, 22, 23, 24},
	}
	data := writeEncrypted(t, testRows, cfg)

	keys := &staticKeyRetriever{
		footerKey: footerKey,
		columnKeys: map[string][]byte{
			"name": nameKey,
		},
	}
	got := readDecrypted(t, data, keys)
	assertRowsEqual(t, testRows, got)
}

// TestEncryptionNoKeyReturnsError verifies that opening an encrypted file without
// providing a DecryptionConfig returns an error rather than silently wrong data.
func TestEncryptionNoKeyReturnsError(t *testing.T) {
	footerKey := aes128Key(0xEE)
	cfg := &parquet.EncryptionConfig{
		FooterKey:       footerKey,
		EncryptedFooter: true,
		FileIdentifier:  []byte{25, 26, 27, 28, 29, 30, 31, 32},
	}
	data := writeEncrypted(t, testRows, cfg)

	// Open without any decryption config — must return an error.
	_, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err == nil {
		t.Fatal("expected error opening encrypted file without decryption config, got nil")
	}
}

// TestEncryptionFooterSignatureTamper verifies that tampering with the footer bytes of
// a plaintext-footer-with-signature file is detected on open.
func TestEncryptionFooterSignatureTamper(t *testing.T) {
	footerKey := aes128Key(0xFF)
	cfg := &parquet.EncryptionConfig{
		FooterKey:       footerKey,
		EncryptedFooter: false,
		FileIdentifier:  []byte{33, 34, 35, 36, 37, 38, 39, 40},
	}
	data := writeEncrypted(t, testRows, cfg)

	// Flip a byte in the footer region (last ~100 bytes before the magic trailer).
	corrupted := bytes.Clone(data)
	// The last 8 bytes are [4-byte footer size][PAR1 magic]. The 28-byte signature
	// immediately precedes that, and the thrift footer is before the signature.
	// Flip a byte 40 bytes from the end (well inside the footer+signature region).
	if len(corrupted) < 50 {
		t.Fatal("test data too short to corrupt footer region")
	}
	corrupted[len(corrupted)-40] ^= 0xFF

	keys := &staticKeyRetriever{footerKey: footerKey}
	_, err := parquet.OpenFile(bytes.NewReader(corrupted), int64(len(corrupted)), parquet.WithDecryption(keys))
	if err == nil {
		t.Fatal("expected error opening file with corrupted footer, got nil")
	}
}

// TestEncryptionCTRAlgorithmRejected verifies that requesting AES_GCM_CTR_V1
// (not yet implemented) causes the writer to panic immediately rather than
// silently emitting a GCM-encrypted file labelled as CTR.
func TestEncryptionCTRAlgorithmRejected(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic when AES_GCM_CTR_V1 is requested, got none")
		}
	}()
	cfg := &parquet.EncryptionConfig{
		FooterKey: aes128Key(0x11),
		Algorithm: parquet.AES_GCM_CTR_V1,
	}
	var buf bytes.Buffer
	_ = parquet.NewGenericWriter[encryptionTestRow](&buf, parquet.WithEncryption(cfg))
}

// TestEncryptionSeekToRow verifies that SeekToRow works correctly on encrypted
// columns with an offset index.  The bug: dataPageOrd was not updated after a
// seek, so the AAD for subsequent page reads used the wrong page ordinal,
// causing AES-GCM authentication failures.
func TestEncryptionSeekToRow(t *testing.T) {
	footerKey := aes128Key(0xAB)
	cfg := &parquet.EncryptionConfig{
		FooterKey:       footerKey,
		EncryptedFooter: true,
		FileIdentifier:  []byte{41, 42, 43, 44, 45, 46, 47, 48},
	}

	// Write enough rows to span multiple pages (force small page buffer).
	const numRows = 200
	rows := make([]encryptionTestRow, numRows)
	for i := range rows {
		rows[i] = encryptionTestRow{Name: strings.Repeat("x", 20), Value: int64(i)}
	}

	var buf bytes.Buffer
	w := parquet.NewGenericWriter[encryptionTestRow](&buf,
		parquet.WithEncryption(cfg),
		parquet.PageBufferSize(512), // small pages → many pages per column
	)
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	data := buf.Bytes()
	keys := &staticKeyRetriever{footerKey: footerKey}
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)), parquet.WithDecryption(keys))
	if err != nil {
		t.Fatalf("open file: %v", err)
	}

	// Seek forward into the file (not page 0) and read a single row.
	const targetRow = 150
	r := parquet.NewGenericReader[encryptionTestRow](f)
	defer r.Close()

	if err := r.SeekToRow(targetRow); err != nil {
		t.Fatalf("SeekToRow(%d): %v", targetRow, err)
	}
	result := make([]encryptionTestRow, 1)
	if n, err := r.Read(result); n != 1 || (err != nil && !errors.Is(err, io.EOF)) {
		t.Fatalf("Read after seek: n=%d err=%v", n, err)
	}
	if result[0].Value != int64(targetRow) {
		t.Errorf("SeekToRow(%d): got row with Value=%d, want %d", targetRow, result[0].Value, targetRow)
	}
}

// TestEncryptionBloomFilter verifies that bloom filters written on encrypted
// columns can be loaded and queried after decryption.  The bug: the read path
// tried to thrift-decode the encrypted bloom filter header as plaintext,
// causing an error whenever SkipBloomFilters=false (the default).
func TestEncryptionBloomFilter(t *testing.T) {
	footerKey := aes128Key(0xBC)
	cfg := &parquet.EncryptionConfig{
		FooterKey:       footerKey,
		EncryptedFooter: true,
		FileIdentifier:  []byte{49, 50, 51, 52, 53, 54, 55, 56},
	}

	rows := []encryptionTestRow{
		{Name: "alice", Value: 1},
		{Name: "bob", Value: 2},
		{Name: "carol", Value: 3},
	}

	var buf bytes.Buffer
	w := parquet.NewGenericWriter[encryptionTestRow](&buf,
		parquet.WithEncryption(cfg),
		parquet.BloomFilters(parquet.SplitBlockFilter(10, "name")),
	)
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	data := buf.Bytes()
	keys := &staticKeyRetriever{footerKey: footerKey}
	// Opening the file pre-loads bloom filters by default (SkipBloomFilters=false).
	// Before the fix this would fail when trying to thrift-decode the encrypted header.
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)), parquet.WithDecryption(keys))
	if err != nil {
		t.Fatalf("open file with encrypted bloom filter: %v", err)
	}

	rgs := f.RowGroups()
	if len(rgs) == 0 {
		t.Fatal("no row groups")
	}
	chunks := rgs[0].ColumnChunks()

	// Find the "name" column (index 0, alphabetical order).
	var nameChunk parquet.ColumnChunk
	for _, cc := range chunks {
		if cc.Column() == 0 {
			nameChunk = cc
			break
		}
	}
	if nameChunk == nil {
		t.Fatal("could not find name column chunk")
	}

	bf := nameChunk.BloomFilter()
	if bf == nil {
		t.Fatal("expected bloom filter on name column, got nil")
	}

	// "alice" should be present; "dave" should not.
	present, err := bf.Check(parquet.ValueOf("alice"))
	if err != nil {
		t.Fatalf("bloom filter Check(alice): %v", err)
	}
	if !present {
		t.Error("bloom filter should report 'alice' as present")
	}

	absent, err := bf.Check(parquet.ValueOf("dave"))
	if err != nil {
		t.Fatalf("bloom filter Check(dave): %v", err)
	}
	if absent {
		t.Error("bloom filter should not report 'dave' as present")
	}
}
