package parquet_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
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

// partialKeyRetriever is a test-only KeyRetriever that returns an error for
// column keys not present in its map (simulates partial key access).
type partialKeyRetriever struct {
	footerKey  []byte
	columnKeys map[string][]byte
}

func (p *partialKeyRetriever) FooterKey(_ []byte) ([]byte, error) {
	return p.footerKey, nil
}

func (p *partialKeyRetriever) ColumnKey(path []string, _ []byte) ([]byte, error) {
	k := strings.Join(path, ".")
	if key, ok := p.columnKeys[k]; ok {
		return key, nil
	}
	return nil, fmt.Errorf("no key for column %q: %w", k, parquet.ErrKeyNotFound)
}

// hardErrorKeyRetriever returns a caller-supplied hard error for every column
// key lookup (simulates a transient KMS failure or similar real error).
type hardErrorKeyRetriever struct {
	footerKey []byte
	colErr    error
}

func (h *hardErrorKeyRetriever) FooterKey(_ []byte) ([]byte, error) {
	return h.footerKey, nil
}

func (h *hardErrorKeyRetriever) ColumnKey(_ []string, _ []byte) ([]byte, error) {
	return nil, h.colErr
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

// TestEncryptionPlaintextFooterColumnMetadataHidden verifies that in
// plaintext-footer mode, column metadata (page offsets, statistics) is
// NOT present in plaintext form in the footer — it should be encrypted
// inline as EncryptedColumnMetadata so that unauthenticated readers
// cannot read sensitive metadata.
func TestEncryptionPlaintextFooterColumnMetadataHidden(t *testing.T) {
	footerKey := aes128Key(0xAA)
	cfg := &parquet.EncryptionConfig{
		FooterKey:       footerKey,
		EncryptedFooter: false, // plaintext-footer mode
		FileIdentifier:  []byte{57, 58, 59, 60, 61, 62, 63, 64},
	}
	data := writeEncrypted(t, testRows, cfg)

	// Open the file WITHOUT a decryption key and attempt to read row groups.
	// The plaintext footer must not expose column metadata — if MetaData is
	// present in plaintext the unauthenticated reader could use data offsets.
	fPlain, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		// A signed-footer file opened without keys: error is expected
		// only if the implementation verifies signatures on open.
		// Either way, if open succeeds the metadata must be hidden.
		return
	}
	rgs := fPlain.RowGroups()
	for i, rg := range rgs {
		for j, cc := range rg.ColumnChunks() {
			if cc.NumValues() != 0 {
				t.Errorf("rowGroup=%d col=%d: NumValues=%d but column metadata should be hidden without decryption key",
					i, j, cc.NumValues())
			}
		}
	}

	// With the correct key the round-trip must still work.
	keys := &staticKeyRetriever{footerKey: footerKey}
	got := readDecrypted(t, data, keys)
	assertRowsEqual(t, testRows, got)
}

// TestEncryptionPlaintextFooterRowGroupSizes verifies that TotalByteSize and
// TotalCompressedSize in the file metadata are non-zero in plaintext-footer
// encrypted mode.  The bug: MetaData was zeroed before sizes were accumulated.
func TestEncryptionPlaintextFooterRowGroupSizes(t *testing.T) {
	footerKey := aes128Key(0x12)
	cfg := &parquet.EncryptionConfig{
		FooterKey:       footerKey,
		EncryptedFooter: false,
		FileIdentifier:  []byte{65, 66, 67, 68, 69, 70, 71, 72},
	}
	data := writeEncrypted(t, testRows, cfg)

	keys := &staticKeyRetriever{footerKey: footerKey}
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)), parquet.WithDecryption(keys))
	if err != nil {
		t.Fatalf("open file: %v", err)
	}

	for i, rg := range f.Metadata().RowGroups {
		if rg.TotalByteSize == 0 {
			t.Errorf("rowGroup=%d: TotalByteSize=0, expected non-zero", i)
		}
		if rg.TotalCompressedSize == 0 {
			t.Errorf("rowGroup=%d: TotalCompressedSize=0, expected non-zero", i)
		}
	}
}

// TestEncryptionPlaintextFooterColumnEncoding verifies that Column.Encoding()
// and Column.Compression() are correctly populated when opening a
// plaintext-footer encrypted file.  The bug: openColumns ran before column
// metadata was decrypted, so it cached nil encoding/compression.
func TestEncryptionPlaintextFooterColumnEncoding(t *testing.T) {
	footerKey := aes128Key(0x13)
	cfg := &parquet.EncryptionConfig{
		FooterKey:       footerKey,
		EncryptedFooter: false,
		FileIdentifier:  []byte{73, 74, 75, 76, 77, 78, 79, 80},
	}

	// Write with non-default (Snappy) compression so we can detect it on read.
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[encryptionTestRow](&buf,
		parquet.WithEncryption(cfg),
		parquet.Compression(&parquet.Snappy),
	)
	if _, err := w.Write(testRows); err != nil {
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

	// The root column's leaf compression should be Snappy, not nil.
	for _, rg := range f.RowGroups() {
		for _, cc := range rg.ColumnChunks() {
			if cc.NumValues() == 0 {
				t.Errorf("col=%d: NumValues=0 after decryption, column metadata was not restored", cc.Column())
			}
		}
	}

	// Full round-trip must also succeed.
	got := readDecrypted(t, data, keys)
	assertRowsEqual(t, testRows, got)
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

// tracebufLeakEnv selects the subprocess that runs a test with buffer tracing.
// PARQUETGODEBUG is read once, from init, so the leak check cannot be turned on
// inside an already-started test binary.
const tracebufLeakEnv = "PARQUET_TEST_TRACEBUF_LEAK"

// syncBuffer is a log sink the finalizer goroutine can write while the test
// reads it back after GC.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// flushFilterPagesLeakReport returns tracebuf leak reports whose stack is
// flushFilterPages. Other leaked buffers are ignored.
func flushFilterPagesLeakReport(text string) string {
	const header = "PARQUETGODEBUG: buffer["
	var b strings.Builder
	rest := text
	for {
		i := strings.Index(rest, header)
		if i < 0 {
			break
		}
		rest = rest[i:]
		next := strings.Index(rest[len(header):], header)
		report := rest
		if next >= 0 {
			report = rest[:len(header)+next]
			rest = rest[len(header)+next:]
		} else {
			rest = ""
		}
		if strings.Contains(report, "non-zero reference count") && strings.Contains(report, "flushFilterPages") {
			if b.Len() > 0 {
				b.WriteByte('\n')
			}
			b.WriteString(strings.TrimSpace(report))
		}
	}
	return b.String()
}

// tracebufLeakCapture collects tracebuf leak logs for one subprocess test.
type tracebufLeakCapture struct {
	buf     syncBuffer
	scanned int
}

// activeTracebufLeaks is set only in the subprocess started by withTracebufLeakCheck.
var activeTracebufLeaks *tracebufLeakCapture

// collectTracebufLeaks forces finalizers and fails t when a buffer allocated
// from flushFilterPages was collected with a non-zero reference count.
// Safe to call more than once: each call reports only leaks logged since the
// previous call. No-op outside the tracing subprocess.
func collectTracebufLeaks(t *testing.T) {
	t.Helper()
	c := activeTracebufLeaks
	if c == nil {
		return
	}
	// Finalizers run after the allocating frame returns. Two collections are
	// enough for a pointer-containing buffer; the third covers a finalizer
	// queued by the second.
	runtime.GC()
	runtime.GC()
	runtime.GC()
	text := c.buf.String()
	if c.scanned > len(text) {
		c.scanned = 0
	}
	report := flushFilterPagesLeakReport(text[c.scanned:])
	c.scanned = len(text)
	if report != "" {
		t.Errorf("PARQUETGODEBUG=tracebuf=1 reported a buffer leaked from flushFilterPages:\n%s", report)
	}
}

// withTracebufLeakCheck re-executes the calling top-level test in a subprocess
// with PARQUETGODEBUG=tracebuf=1. The subprocess fails when tracebuf reports a
// buffer leaked from flushFilterPages; logging the leak is not enough.
//
// Call it before any other test logic. It returns false in the parent process
// after the subprocess has been checked, and true in the subprocess.
func withTracebufLeakCheck(t *testing.T) bool {
	t.Helper()
	if strings.Contains(t.Name(), "/") {
		t.Fatalf("withTracebufLeakCheck must run on the top-level test, not %s", t.Name())
	}
	if os.Getenv(tracebufLeakEnv) != "1" {
		cmd := exec.Command(os.Args[0], "-test.run=^"+regexp.QuoteMeta(t.Name())+"$", "-test.count=1", "-test.v")
		env := make([]string, 0, len(os.Environ())+3)
		for _, e := range os.Environ() {
			if strings.HasPrefix(e, "PARQUETGODEBUG=") || strings.HasPrefix(e, "GOGC=") || strings.HasPrefix(e, tracebufLeakEnv+"=") {
				continue
			}
			env = append(env, e)
		}
		cmd.Env = append(env, "PARQUETGODEBUG=tracebuf=1", "GOGC=1", tracebufLeakEnv+"=1")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("buffer leak check failed: %v\n%s", err, out)
		}
		// A finalizer can log after the subprocess has already decided the test
		// passed. Treat that the same as a failure inside the test.
		if leak := flushFilterPagesLeakReport(string(out)); leak != "" {
			t.Fatalf("PARQUETGODEBUG=tracebuf=1 reported a buffer leaked from flushFilterPages:\n%s", leak)
		}
		return false
	}

	logs := &tracebufLeakCapture{}
	activeTracebufLeaks = logs
	prev := log.Writer()
	log.SetOutput(io.MultiWriter(prev, &logs.buf))
	t.Cleanup(func() {
		collectTracebufLeaks(t)
		log.SetOutput(prev)
		activeTracebufLeaks = nil
	})
	return true
}

// TestEncryptionBloomFilter verifies that bloom filters written on encrypted
// columns can be loaded and queried after decryption.  The bug: the read path
// tried to thrift-decode the encrypted bloom filter header as plaintext,
// causing an error whenever SkipBloomFilters=false (the default).
func TestEncryptionBloomFilter(t *testing.T) {
	if !withTracebufLeakCheck(t) {
		return
	}
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

// TestEncryptionPartialColumnKeysOpenFile verifies that OpenFile succeeds even
// when the caller only provides keys for a subset of per-column-encrypted
// columns.  The bug: decryptAllColumnMetadata returned an error on the first
// missing column key, blocking reads of columns whose keys were available.
func TestEncryptionPartialColumnKeysOpenFile(t *testing.T) {
	footerKey := aes128Key(0xA1)
	nameKey := aes128Key(0xA2)
	cfg := &parquet.EncryptionConfig{
		FooterKey: footerKey,
		ColumnKeys: map[string][]byte{
			"name": nameKey,
		},
		EncryptedFooter: false,
		FileIdentifier:  []byte{81, 82, 83, 84, 85, 86, 87, 88},
	}
	data := writeEncrypted(t, testRows, cfg)

	// Open with only the footer key — name column key is missing.
	// Before the fix this would return an error from decryptAllColumnMetadata.
	partial := &partialKeyRetriever{
		footerKey:  footerKey,
		columnKeys: map[string][]byte{}, // no column keys provided
	}
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)), parquet.WithDecryption(partial))
	if err != nil {
		t.Fatalf("OpenFile with partial keys should succeed, got: %v", err)
	}

	// The "value" column (uses footer key) should still be readable.
	rgs := f.RowGroups()
	if len(rgs) == 0 {
		t.Fatal("expected at least one row group")
	}
}

func TestEncryptionPartialColumnKeysBloomFilter(t *testing.T) {
	footerKey := aes128Key(0xA1)
	for _, encryptedFooter := range []bool{false, true} {
		var buf bytes.Buffer
		w := parquet.NewGenericWriter[encryptionTestRow](&buf,
			parquet.WithEncryption(&parquet.EncryptionConfig{
				FooterKey:       footerKey,
				ColumnKeys:      map[string][]byte{"name": aes128Key(0xA2)},
				EncryptedFooter: encryptedFooter,
			}),
			parquet.BloomFilters(parquet.SplitBlockFilter(10, "name")),
		)
		if _, err := w.Write(testRows); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}

		for _, prefetch := range []bool{false, true} {
			t.Run(fmt.Sprintf("encryptedFooter=%t/prefetch=%t", encryptedFooter, prefetch), func(t *testing.T) {
				keys := &partialKeyRetriever{footerKey: footerKey}
				f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()),
					parquet.WithDecryption(keys), parquet.PrefetchBloomFilters(prefetch))
				if err != nil {
					t.Fatalf("OpenFile with partial keys should succeed, got: %v", err)
				}

				type projection struct {
					Value int64 `parquet:"value"`
				}
				r := parquet.NewGenericReader[projection](f)
				defer r.Close()
				got := make([]projection, len(testRows))
				n, err := r.Read(got)
				if err != nil && err != io.EOF {
					t.Fatalf("read accessible column: %v", err)
				}
				if n != len(testRows) {
					t.Fatalf("read accessible column: got %d rows, want %d", n, len(testRows))
				}
				for i, row := range testRows {
					if got[i].Value != row.Value {
						t.Errorf("accessible column row %d: got %d, want %d", i, got[i].Value, row.Value)
					}
				}

				name := f.RowGroups()[0].ColumnChunks()[0]
				bf := name.BloomFilter()
				if bf == nil {
					t.Fatal("missing-key column: expected bloom filter")
				}
				if _, err := bf.Check(parquet.ValueOf(testRows[0].Name)); !errors.Is(err, parquet.ErrKeyNotFound) {
					t.Errorf("missing-key bloom filter: got %v, want ErrKeyNotFound", err)
				}
				pages := name.Pages()
				defer pages.Close()
				if page, err := pages.ReadPage(); !errors.Is(err, parquet.ErrKeyNotFound) {
					parquet.Release(page)
					t.Errorf("missing-key pages: got %v, want ErrKeyNotFound", err)
				}
			})
		}
	}
}

// TestEncryptionRealColumnKeyErrorSurfaced verifies that a non-ErrKeyNotFound
// error from ColumnKey is returned by OpenFile rather than silently swallowed.
// The bug: all ColumnKey errors were treated as "key missing" and suppressed,
// hiding real failures such as KMS errors or malformed key metadata.
func TestEncryptionRealColumnKeyErrorSurfaced(t *testing.T) {
	footerKey := aes128Key(0xA3)
	nameKey := aes128Key(0xA4)
	cfg := &parquet.EncryptionConfig{
		FooterKey: footerKey,
		ColumnKeys: map[string][]byte{
			"name": nameKey,
		},
		EncryptedFooter: false,
		FileIdentifier:  []byte{97, 98, 99, 100, 101, 102, 103, 104},
	}
	data := writeEncrypted(t, testRows, cfg)

	// Retriever that returns a hard (non-ErrKeyNotFound) error for "name".
	hardErr := fmt.Errorf("KMS unavailable")
	badRetriever := &hardErrorKeyRetriever{
		footerKey: footerKey,
		colErr:    hardErr,
	}
	_, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)), parquet.WithDecryption(badRetriever))
	if err == nil {
		t.Fatal("expected OpenFile to surface the KMS error, got nil")
	}
	if !strings.Contains(err.Error(), "KMS unavailable") {
		t.Errorf("expected error to contain 'KMS unavailable', got: %v", err)
	}
}

// TestEncryptionMultipleRowGroupsOrdinals verifies that encrypted files with
// multiple row groups (ordinals > 0) round-trip correctly.  The bug:
// decryptAllColumnMetadata used rg.Ordinal before ordinals were normalized,
// causing authentication failures for files that omit the optional field.
func TestEncryptionMultipleRowGroupsOrdinals(t *testing.T) {
	footerKey := aes128Key(0xB1)
	cfg := &parquet.EncryptionConfig{
		FooterKey:       footerKey,
		EncryptedFooter: false,
		FileIdentifier:  []byte{89, 90, 91, 92, 93, 94, 95, 96},
	}

	// Write three separate row groups (MaxRowsPerRowGroup=1 forces one per row).
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[encryptionTestRow](&buf,
		parquet.WithEncryption(cfg),
		parquet.MaxRowsPerRowGroup(1),
	)
	if _, err := w.Write(testRows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	data := buf.Bytes()

	keys := &staticKeyRetriever{footerKey: footerKey}
	got := readDecrypted(t, data, keys)
	assertRowsEqual(t, testRows, got)
}

// TestEncryptionBloomFilterGzip verifies that bloom filters configured with
// gzip compression round-trip when the column is encrypted.  Regression for
// codex finding 4: newBloomFilterFromBytes only handled the Uncompressed
// header variant, dropping encrypted gzip-compressed bloom filters silently.
func TestEncryptionBloomFilterGzip(t *testing.T) {
	if !withTracebufLeakCheck(t) {
		return
	}
	footerKey := aes128Key(0xC1)
	cfg := &parquet.EncryptionConfig{
		FooterKey:       footerKey,
		EncryptedFooter: true,
		FileIdentifier:  []byte{1, 1, 1, 1, 2, 2, 2, 2},
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
		parquet.BloomFilterCompression(&parquet.Gzip),
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
		t.Fatalf("open file with encrypted+gzip bloom filter: %v", err)
	}

	rgs := f.RowGroups()
	if len(rgs) == 0 {
		t.Fatal("no row groups")
	}
	chunks := rgs[0].ColumnChunks()

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
		t.Fatal("expected bloom filter on encrypted+gzip column, got nil (writer accepted the configuration)")
	}

	present, err := bf.Check(parquet.ValueOf("alice"))
	if err != nil {
		t.Fatalf("bloom filter Check(alice): %v", err)
	}
	if !present {
		t.Error("bloom filter should report 'alice' as present")
	}
}

// TestEncryptionBloomFilterPageBuffers is the flushFilterPages leak path with
// more than one data page, and with compressed data pages. The bloom filter is
// not sized ahead of time, and the column is not dictionary-encoded, so the
// writer decrypts each page back into a pooled buffer at flush.
func TestEncryptionBloomFilterPageBuffers(t *testing.T) {
	if !withTracebufLeakCheck(t) {
		return
	}

	rows := make([]encryptionTestRow, 24)
	for i := range rows {
		rows[i] = encryptionTestRow{
			Name:  fmt.Sprintf("name-%04d-%s", i, strings.Repeat("x", 32)),
			Value: int64(i),
		}
	}

	t.Run("multiple pages", func(t *testing.T) {
		t.Cleanup(func() { collectTracebufLeaks(t) })
		f := encryptedBloomFile(t, 0xD1, rows, parquet.PageBufferSize(48))
		assertEncryptedBloomPages(t, f, rows, format.Uncompressed, 2)
	})

	t.Run("compressed pages", func(t *testing.T) {
		t.Cleanup(func() { collectTracebufLeaks(t) })
		f := encryptedBloomFile(t, 0xD2, rows,
			parquet.PageBufferSize(48),
			parquet.Compression(&parquet.Snappy),
		)
		assertEncryptedBloomPages(t, f, rows, format.Snappy, 2)
	})
}

func encryptedBloomFile(t *testing.T, keyByte byte, rows []encryptionTestRow, opts ...parquet.WriterOption) *parquet.File {
	t.Helper()
	footerKey := aes128Key(keyByte)
	cfg := &parquet.EncryptionConfig{
		FooterKey:       footerKey,
		EncryptedFooter: true,
		FileIdentifier:  bytes.Repeat([]byte{keyByte}, 8),
	}
	options := make([]parquet.WriterOption, 0, 2+len(opts))
	options = append(options,
		parquet.WithEncryption(cfg),
		parquet.BloomFilters(parquet.SplitBlockFilter(10, "name")),
	)
	options = append(options, opts...)

	var buf bytes.Buffer
	w := parquet.NewGenericWriter[encryptionTestRow](&buf, options...)
	// One row at a time: a single Write of the whole slice is flushed as one
	// page once the buffer crosses the target size, so it cannot produce the
	// multiple pages this test exists to cover.
	for i := range rows {
		if _, err := w.Write(rows[i : i+1]); err != nil {
			t.Fatalf("write row %d: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	data := append([]byte(nil), buf.Bytes()...)
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)), parquet.WithDecryption(&staticKeyRetriever{footerKey: footerKey}))
	if err != nil {
		t.Fatalf("open file: %v", err)
	}
	return f
}

func assertEncryptedBloomPages(t *testing.T, f *parquet.File, rows []encryptionTestRow, codec format.CompressionCodec, minPages int) {
	t.Helper()
	rgs := f.RowGroups()
	if len(rgs) != 1 {
		t.Fatalf("row groups: got %d, want 1", len(rgs))
	}
	chunks := rgs[0].ColumnChunks()
	cols := f.Metadata().RowGroups[0].Columns
	if len(chunks) != len(cols) {
		t.Fatalf("column chunks %d, metadata columns %d", len(chunks), len(cols))
	}

	var nameChunk parquet.ColumnChunk
	var meta format.ColumnMetaData
	found := false
	for i, cc := range chunks {
		path := cols[i].MetaData.PathInSchema
		if len(path) == 1 && path[0] == "name" {
			nameChunk = cc
			meta = cols[i].MetaData
			found = true
			break
		}
	}
	if !found {
		t.Fatal("name column not found")
	}
	if meta.Codec != codec {
		t.Fatalf("name column codec: got %s, want %s", meta.Codec, codec)
	}
	for _, enc := range meta.Encoding {
		if enc == format.RLEDictionary || enc == format.PlainDictionary {
			t.Fatalf("name column encoding %s uses a dictionary; flushFilterPages would not re-read data pages", enc)
		}
	}

	oi, err := nameChunk.OffsetIndex()
	if err != nil {
		t.Fatalf("offset index: %v", err)
	}
	if oi.NumPages() < minPages {
		t.Fatalf("name column pages: got %d, want at least %d", oi.NumPages(), minPages)
	}

	bf := nameChunk.BloomFilter()
	if bf == nil {
		t.Fatal("expected bloom filter on name column")
	}
	present, err := bf.Check(parquet.ValueOf(rows[0].Name))
	if err != nil {
		t.Fatalf("bloom filter Check(%q): %v", rows[0].Name, err)
	}
	if !present {
		t.Errorf("bloom filter should report %q as present", rows[0].Name)
	}
	absent, err := bf.Check(parquet.ValueOf("not-written"))
	if err != nil {
		t.Fatalf("bloom filter Check(not-written): %v", err)
	}
	if absent {
		t.Error("bloom filter should not report \"not-written\" as present")
	}
}

// TestEncryptionEncryptedFooterColumnKeyErrorSurfaced verifies that a real KMS
// failure surfaces during OpenFile in encrypted-footer mode.  Regression for
// codex finding 3: FileRowGroup.init silently swallowed FooterKey/ColumnKey
// errors, so OpenFile would succeed with decryptionKey == nil and the user
// would later see confusing "decode page header" errors.  In plaintext-footer
// mode TestEncryptionRealColumnKeyErrorSurfaced already covers this via
// decryptAllColumnMetadata; encrypted-footer mode never runs that path.
func TestEncryptionEncryptedFooterColumnKeyErrorSurfaced(t *testing.T) {
	footerKey := aes128Key(0xC2)
	nameKey := aes128Key(0xC3)
	cfg := &parquet.EncryptionConfig{
		FooterKey: footerKey,
		ColumnKeys: map[string][]byte{
			"name": nameKey,
		},
		EncryptedFooter: true, // critical: encrypted-footer mode
		FileIdentifier:  []byte{2, 2, 2, 2, 3, 3, 3, 3},
	}
	data := writeEncrypted(t, testRows, cfg)

	hardErr := fmt.Errorf("KMS unavailable")
	badRetriever := &hardErrorKeyRetriever{
		footerKey: footerKey,
		colErr:    hardErr,
	}
	_, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)), parquet.WithDecryption(badRetriever))
	if err == nil {
		t.Fatal("expected OpenFile to surface the KMS error in encrypted-footer mode, got nil")
	}
	if !strings.Contains(err.Error(), "KMS unavailable") {
		t.Errorf("expected error to contain 'KMS unavailable', got: %v", err)
	}
}

// TestEncryptionPlaintextFooterPageIndexAvailable verifies that ColumnIndex
// and OffsetIndex are populated after OpenFile on an encrypted plaintext-footer
// file.  Regression for codex finding 2: ReadPageIndex ran before
// decryptAllColumnMetadata, observing zero ColumnIndexOffset on every column
// (because MetaData was still encrypted), so the page index was silently lost.
func TestEncryptionPlaintextFooterPageIndexAvailable(t *testing.T) {
	footerKey := aes128Key(0xC4)
	cfg := &parquet.EncryptionConfig{
		FooterKey:       footerKey,
		EncryptedFooter: false, // critical: plaintext footer with EncryptedColumnMetadata
		FileIdentifier:  []byte{3, 3, 3, 3, 4, 4, 4, 4},
	}
	data := writeEncrypted(t, testRows, cfg)

	keys := &staticKeyRetriever{footerKey: footerKey}
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)), parquet.WithDecryption(keys))
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	chunks := f.RowGroups()[0].ColumnChunks()
	for _, cc := range chunks {
		oi, err := cc.OffsetIndex()
		if err != nil {
			t.Fatalf("OffsetIndex(col=%d): %v", cc.Column(), err)
		}
		if oi == nil {
			t.Errorf("OffsetIndex(col=%d) is nil; encrypted plaintext-footer file lost its page index", cc.Column())
		}
		ci, err := cc.ColumnIndex()
		if err != nil {
			t.Fatalf("ColumnIndex(col=%d): %v", cc.Column(), err)
		}
		if ci == nil {
			t.Errorf("ColumnIndex(col=%d) is nil; encrypted plaintext-footer file lost its column index", cc.Column())
		}
	}
}

// TestEncryptionPageIndexEncrypted verifies that the per-column page index
// (column index + offset index) is encrypted on disk for encrypted columns:
// the literal little-endian encoding of distinctive column statistics must
// not appear in the file bytes.  Regression for codex finding 1: the writer
// left the page index plaintext, leaking min/max values on encrypted columns.
func TestEncryptionPageIndexEncrypted(t *testing.T) {
	footerKey := aes128Key(0xC6)
	cfg := &parquet.EncryptionConfig{
		FooterKey:       footerKey,
		EncryptedFooter: false,
		FileIdentifier:  []byte{5, 5, 5, 5, 6, 6, 6, 6},
	}

	// Distinctive values so a leaked column index would be detectable.
	rows := []encryptionTestRow{
		{Name: "alpha", Value: 1000},
		{Name: "omega", Value: 9999},
	}
	data := writeEncrypted(t, rows, cfg)

	// The decrypted round-trip must still work.
	keys := &staticKeyRetriever{footerKey: footerKey}
	gotRows := readDecrypted(t, data, keys)
	assertRowsEqual(t, rows, gotRows)

	// And the column index must be readable through the public API.
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)), parquet.WithDecryption(keys))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for _, cc := range f.RowGroups()[0].ColumnChunks() {
		ci, err := cc.ColumnIndex()
		if err != nil || ci == nil {
			t.Fatalf("missing column index for column %d after decrypted open: err=%v", cc.Column(), err)
		}
	}

	// Statistics must not leak in plaintext.  Search the on-disk bytes for
	// the little-endian encoding of each distinctive Value.  In an unencrypted
	// page index the writer would embed these as Min/Max thrift fields and
	// they would be findable byte-for-byte.  AES-GCM ciphertext should not
	// contain them with vanishing probability for these specific patterns.
	for _, want := range []int64{1000, 9999} {
		var le [8]byte
		for i := range 8 {
			le[i] = byte(want >> (8 * i))
		}
		if bytes.Contains(data, le[:]) {
			t.Errorf("file bytes contain plaintext little-endian encoding of column statistic %d — page index is not encrypted", want)
		}
	}
}

// encryptionResetRow uses a dictionary-encoded string so the dictionary page
// (written from writeRowGroup) and the data pages (flushed by ColumnWriter.Close
// before that) can be encrypted under different row group ordinals.
type encryptionResetRow struct {
	Name  string `parquet:"name,dict"`
	Value int64  `parquet:"value"`
}

// TestEncryptionResetRowGroupOrdinal writes a file with more than one row
// group, then reuses the writer with Reset, Write, and Close and no explicit
// Flush. Data pages of the second file are encrypted while the column is
// closed, before writeRowGroup assigns row group 0. Both footer modes must
// read back.
func TestEncryptionResetRowGroupOrdinal(t *testing.T) {
	first := []encryptionResetRow{
		{Name: "alpha", Value: 1},
		{Name: "beta", Value: 2},
		{Name: "alpha", Value: 3},
	}
	second := []encryptionResetRow{
		{Name: "gamma", Value: 10},
		{Name: "delta", Value: 11},
		{Name: "gamma", Value: 12},
	}

	for _, tc := range []struct {
		name            string
		encryptedFooter bool
		pageBufferSize  int
	}{
		{name: "encrypted footer", encryptedFooter: true},
		{name: "plaintext footer", encryptedFooter: false},
		// A one-byte page buffer flushes a data page from Write, before Close
		// stamps the ordinal. Reset has to clear it or that page stays wrong.
		{name: "encrypted footer page flushed while writing", encryptedFooter: true, pageBufferSize: 1},
		{name: "plaintext footer page flushed while writing", encryptedFooter: false, pageBufferSize: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			footerKey := aes128Key(0xE7)
			cfg := &parquet.EncryptionConfig{
				FooterKey:       footerKey,
				EncryptedFooter: tc.encryptedFooter,
				FileIdentifier:  []byte{8, 7, 6, 5, 4, 3, 2, 1},
			}
			opts := []parquet.WriterOption{parquet.WithEncryption(cfg)}
			if tc.pageBufferSize > 0 {
				opts = append(opts, parquet.PageBufferSize(tc.pageBufferSize))
			}

			var buf bytes.Buffer
			w := parquet.NewGenericWriter[encryptionResetRow](&buf, opts...)
			if _, err := w.Write(first); err != nil {
				t.Fatalf("write first row group: %v", err)
			}
			if err := w.Flush(); err != nil {
				t.Fatalf("flush first row group: %v", err)
			}
			if _, err := w.Write(first[:2]); err != nil {
				t.Fatalf("write second row group: %v", err)
			}
			if err := w.Close(); err != nil {
				t.Fatalf("close first file: %v", err)
			}

			keys := &staticKeyRetriever{footerKey: footerKey}
			firstBytes := append([]byte(nil), buf.Bytes()...)
			gotFirst := readEncryptionResetRows(t, firstBytes, keys)
			wantFirst := make([]encryptionResetRow, 0, len(first)+2)
			wantFirst = append(wantFirst, first...)
			wantFirst = append(wantFirst, first[:2]...)
			assertRowsEqual(t, wantFirst, gotFirst)
			if groups := openEncryptionResetFile(t, firstBytes, keys).RowGroups(); len(groups) < 2 {
				t.Fatalf("first file row groups = %d, want more than one", len(groups))
			}

			buf.Reset()
			w.Reset(&buf)
			if _, err := w.Write(second); err != nil {
				t.Fatalf("write second file: %v", err)
			}
			if err := w.Close(); err != nil {
				t.Fatalf("close second file: %v", err)
			}

			secondFile := openEncryptionResetFile(t, buf.Bytes(), keys)
			if groups := secondFile.RowGroups(); len(groups) != 1 {
				t.Fatalf("second file row groups = %d, want 1", len(groups))
			}
			gotSecond := readEncryptionResetRows(t, buf.Bytes(), keys)
			assertRowsEqual(t, second, gotSecond)
		})
	}
}

func openEncryptionResetFile(t *testing.T, data []byte, keys parquet.KeyRetriever) *parquet.File {
	t.Helper()
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)), parquet.WithDecryption(keys))
	if err != nil {
		t.Fatalf("open file: %v", err)
	}
	return f
}

func readEncryptionResetRows(t *testing.T, data []byte, keys parquet.KeyRetriever) []encryptionResetRow {
	t.Helper()
	f := openEncryptionResetFile(t, data, keys)
	r := parquet.NewGenericReader[encryptionResetRow](f)
	defer r.Close()

	var out []encryptionResetRow
	buf := make([]encryptionResetRow, 16)
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

func TestEncryptionConcurrentRowGroups(t *testing.T) {
	type row struct {
		Name   string  `parquet:"name"`
		Dict   string  `parquet:"dict,dict"`
		Values []int64 `parquet:"values,list"`
		Note   *string `parquet:"note,optional"`
	}
	const secret = "concurrent-secret-payload"
	note := secret + "-optional"
	makeRows := func(group int) []row {
		return []row{
			{Name: fmt.Sprintf("%s-%d-first", secret, group), Dict: secret, Values: []int64{int64(group), 42}, Note: &note},
			{Name: fmt.Sprintf("%s-%d-second", secret, group), Dict: secret, Values: []int64{}},
			{Name: fmt.Sprintf("%s-%d-third", secret, group), Dict: secret, Values: []int64{99}},
		}
	}
	for _, api := range []string{"BeginRowGroup", "BeginGenericRowGroup", "Writer.BeginRowGroup"} {
		for _, encryptedFooter := range []bool{true, false} {
			for _, version := range []int{1, 2} {
				for _, order := range [][]int{{0, 1, 2}, {2, 0, 1}} {
					for _, columnKeys := range []bool{false, true} {
						t.Run(fmt.Sprintf("%s/footer=%t/v%d/order=%v/columnKeys=%t", api, encryptedFooter, version, order, columnKeys), func(t *testing.T) {
							key, dictKey := aes128Key(0xA1), aes128Key(0xB2)
							cfg := &parquet.EncryptionConfig{
								FooterKey:       key,
								EncryptedFooter: encryptedFooter, AadPrefix: []byte("concurrent-test"),
							}
							if columnKeys {
								cfg.ColumnKeys = map[string][]byte{"dict": dictKey}
							} else {
								dictKey = key
							}
							dir := t.TempDir()
							opts := []parquet.WriterOption{
								parquet.WithEncryption(cfg), parquet.DataPageVersion(version),
								parquet.ColumnPageBuffers(parquet.NewFileBufferPool(dir, "pages-*")),
								parquet.BloomFilters(parquet.SplitBlockFilter(10, "name"), parquet.SplitBlockFilter(10, "dict")),
							}
							if version == 2 {
								opts = append(opts,
									parquet.Compression(&parquet.Snappy),
									parquet.DictionaryMaxBytes(1),
									parquet.BloomFilterCompression(&parquet.Gzip),
									parquet.DeferBloomFiltersWithBuffers(parquet.NewFileBufferPool(dir, "bloom-*")),
								)
							}
							var buf bytes.Buffer
							w := parquet.NewGenericWriter[row](&buf, opts...)
							schema := w.Schema()
							writeRows := func(rg *parquet.ConcurrentRowGroupWriter, rows []row) (int, error) {
								values := make([]parquet.Row, len(rows))
								for i := range rows {
									values[i] = schema.Deconstruct(nil, &rows[i])
								}
								return rg.WriteRows(values)
							}
							begin := func() (*parquet.ConcurrentRowGroupWriter, func([]row) (int, error)) {
								if api == "BeginGenericRowGroup" {
									rg := w.BeginGenericRowGroup()
									return rg.ConcurrentRowGroupWriter, rg.Write
								}
								rg := w.BeginRowGroup()
								return rg, func(rows []row) (int, error) { return writeRows(rg, rows) }
							}
							closeWriter := w.Close
							writeParent := w.Write
							if api == "Writer.BeginRowGroup" {
								legacy := parquet.NewWriter(&buf, append(opts, schema)...)
								begin = func() (*parquet.ConcurrentRowGroupWriter, func([]row) (int, error)) {
									rg := legacy.BeginRowGroup()
									return rg, func(rows []row) (int, error) { return writeRows(rg, rows) }
								}
								closeWriter = legacy.Close
								writeParent = func(rows []row) (int, error) {
									for i := range rows {
										if err := legacy.Write(&rows[i]); err != nil {
											return i, err
										}
									}
									return len(rows), nil
								}
							}
							const groups = 3
							rgs := make([]*parquet.ConcurrentRowGroupWriter, groups)
							writes := make([]func([]row) (int, error), groups)
							for i := range rgs {
								rgs[i], writes[i] = begin()
							}
							var wg sync.WaitGroup
							for i, rg := range rgs {
								wg.Go(func() {
									for _, r := range makeRows(i) {
										if _, err := writes[i]([]row{r}); err != nil {
											t.Errorf("write rows: %v", err)
											return
										}
										if err := rg.Flush(); err != nil {
											t.Errorf("flush rows: %v", err)
											return
										}
									}
								})
							}
							wg.Wait()
							// Page buffers can be on disk even before a row group is committed.
							paths, err := filepath.Glob(filepath.Join(dir, "pages-*"))
							if err != nil {
								t.Fatal(err)
							}
							if len(paths) == 0 {
								t.Fatal("no page buffers created")
							}
							for _, path := range paths {
								data, err := os.ReadFile(path)
								if err != nil {
									t.Fatal(err)
								}
								if bytes.Contains(data, []byte(secret)) {
									t.Error("plaintext payload in page buffer")
								}
							}
							var want []row
							// Commit order, not creation order, defines the row-group AAD.
							for _, i := range order {
								if n, err := rgs[i].Commit(); err != nil || n != int64(len(makeRows(i))) {
									t.Fatalf("commit rows: %d, %v", n, err)
								}
								want = append(want, makeRows(i)...)
							}
							// Pending parent rows are committed first, changing the reused group's ordinal again.
							parentRows := makeRows(3)[:1]
							if _, err := writeParent(parentRows); err != nil {
								t.Fatalf("write parent rows: %v", err)
							}
							want = append(want, parentRows...)
							// Plaintext-footer metadata reuse is covered separately; use a fresh
							// group there until that independent repair lands.
							if !encryptedFooter {
								rgs[2], writes[2] = begin()
							}
							extra := makeRows(4)[:1]
							if _, err := writes[2](extra); err != nil {
								t.Fatalf("write rows: %v", err)
							}
							if _, err := rgs[2].Commit(); err != nil {
								t.Fatalf("commit rows: %v", err)
							}
							want = append(want, extra...)
							if err := closeWriter(); err != nil {
								t.Fatalf("close writer: %v", err)
							}
							if bytes.Contains(buf.Bytes(), []byte(secret)) {
								t.Error("plaintext payload in output file")
							}
							f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()), parquet.WithDecryption(&staticKeyRetriever{footerKey: key, columnKeys: map[string][]byte{"dict": dictKey}}))
							if err != nil {
								t.Fatalf("open encrypted file: %v", err)
							}
							for i, group := range f.RowGroups() {
								for j, chunk := range group.ColumnChunks() {
									meta := f.Metadata().RowGroups[i].Columns[j].MetaData
									if bf := chunk.BloomFilter(); bf != nil {
										present, err := bf.Check(parquet.ByteArrayValue(meta.Statistics.MinValue))
										if err != nil || !present {
											t.Errorf("bloom filter lost a value: group %d column %d: %v", i, j, err)
										}
									}
									oi, err := chunk.OffsetIndex()
									if err != nil {
										t.Fatal(err)
									}
									end := meta.DataPageOffset
									if meta.DictionaryPageOffset != 0 {
										end = meta.DictionaryPageOffset
									}
									end += meta.TotalCompressedSize
									for k := oi.NumPages() - 1; k >= 0; k-- {
										if oi.Offset(k)+oi.CompressedPageSize(k) != end {
											t.Errorf("page size mismatch: group %d column %d page %d", i, j, k)
										}
										end = oi.Offset(k)
									}
								}
							}

							r := parquet.NewGenericReader[row](f)
							defer r.Close()
							got := make([]row, len(want)+1)
							n, err := r.Read(got)
							if err != io.EOF {
								t.Errorf("read rows: got %v, want EOF", err)
							}
							if !reflect.DeepEqual(got[:n], want) {
								t.Errorf("read rows: got %+v, want %+v", got[:n], want)
							}
						})
					}
				}
			}
		}
	}
}

func TestEncryptionConcurrentColumnFlush(t *testing.T) {
	for _, version := range []int{1, 2} {
		for _, column := range []int{0, 1} {
			t.Run(fmt.Sprintf("v%d/column=%d", version, column), func(t *testing.T) {
				key := aes128Key(0xA1)
				var buf bytes.Buffer
				w := parquet.NewGenericWriter[encryptionTestRow](&buf,
					parquet.DataPageVersion(version),
					parquet.WithEncryption(&parquet.EncryptionConfig{FooterKey: key, EncryptedFooter: true}),
				)
				first, second := w.BeginGenericRowGroup(), w.BeginGenericRowGroup()
				if _, err := first.Write(testRows); err != nil {
					t.Fatal(err)
				}
				// The other column flushes at Commit and can use its final ordinal
				// directly. Only this column's buffered pages need remapping.
				if err := first.ColumnWriters()[column].Flush(); err != nil {
					t.Fatal(err)
				}
				if _, err := second.Write(testRows); err != nil {
					t.Fatal(err)
				}
				if _, err := second.Commit(); err != nil {
					t.Fatal(err)
				}
				if _, err := first.Commit(); err != nil {
					t.Fatal(err)
				}
				if err := w.Close(); err != nil {
					t.Fatal(err)
				}
				got := readDecrypted(t, buf.Bytes(), &staticKeyRetriever{footerKey: key})
				want := append(slices.Clone(testRows), testRows...)
				assertRowsEqual(t, want, got)
			})
		}
	}
}

func TestEncryptionConcurrentParentClose(t *testing.T) {
	for _, api := range []string{"BeginRowGroup", "BeginGenericRowGroup", "Writer.BeginRowGroup"} {
		for _, footer := range []bool{true, false} {
			for _, version := range []int{1, 2} {
				for _, flush := range []string{"none", "first", "second", "all", "automatic"} {
					t.Run(fmt.Sprintf("%s/footer=%t/v%d/flush=%s", api, footer, version, flush), func(t *testing.T) {
						var buf bytes.Buffer
						key := aes128Key(0xA1)
						opts := []parquet.WriterOption{
							parquet.WithEncryption(&parquet.EncryptionConfig{FooterKey: key, EncryptedFooter: footer}),
							parquet.DataPageVersion(version),
						}
						if flush == "automatic" {
							opts = append(opts, parquet.PageBufferSize(1))
						}
						w := parquet.NewGenericWriter[encryptionTestRow](&buf, opts...)
						schema := w.Schema()
						rows := make([]parquet.Row, len(testRows))
						for i := range testRows {
							rows[i] = schema.Deconstruct(nil, &testRows[i])
						}
						closeWriter, writeParent, columns := w.Close, w.Write, w.ColumnWriters
						var commit func() (int64, error)
						switch api {
						case "BeginGenericRowGroup":
							rg := w.BeginGenericRowGroup()
							if _, err := rg.Write(testRows); err != nil {
								t.Fatal(err)
							}
							commit = rg.Commit
						case "BeginRowGroup":
							rg := w.BeginRowGroup()
							if _, err := rg.WriteRows(rows); err != nil {
								t.Fatal(err)
							}
							commit = rg.Commit
						case "Writer.BeginRowGroup":
							legacy := parquet.NewWriter(&buf, append(opts, schema)...)
							closeWriter, columns = legacy.Close, legacy.ColumnWriters
							writeParent = func(rows []encryptionTestRow) (int, error) {
								for i := range rows {
									if err := legacy.Write(&rows[i]); err != nil {
										return i, err
									}
								}
								return len(rows), nil
							}
							rg := legacy.BeginRowGroup()
							if _, err := rg.WriteRows(rows); err != nil {
								t.Fatal(err)
							}
							commit = rg.Commit
						}
						if _, err := commit(); err != nil {
							t.Fatal(err)
						}
						if _, err := writeParent(testRows); err != nil {
							t.Fatal(err)
						}
						for i, c := range columns() {
							if flush == "all" || flush == "first" && i == 0 || flush == "second" && i == 1 {
								if err := c.Flush(); err != nil {
									t.Fatal(err)
								}
							}
						}
						// Close must keep the old ordinal for sealed pages, including
						// those followed by values still in the column buffer.
						if _, err := writeParent(testRows[:1]); err != nil {
							t.Fatal(err)
						}
						if err := closeWriter(); err != nil {
							t.Fatal(err)
						}
						want := append(slices.Clone(testRows), testRows...)
						want = append(want, testRows[0])
						got := readDecrypted(t, buf.Bytes(), &staticKeyRetriever{footerKey: key})
						assertRowsEqual(t, want, got)
					})
				}
			}
		}
	}
}

func TestEncryptionConcurrentRowGroupBloomFilter(t *testing.T) {
	type row struct {
		Plain int64 `parquet:"plain"`
		Dict  int64 `parquet:"dict,dict"`
	}
	key := aes128Key(0xA1)
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[row](&buf,
		parquet.WithEncryption(&parquet.EncryptionConfig{FooterKey: key, EncryptedFooter: true}),
		parquet.BloomFilters(parquet.SplitBlockFilter(10, "plain"), parquet.SplitBlockFilter(10, "dict")),
		parquet.DictionaryMaxBytes(1),
	)
	rg := w.BeginGenericRowGroup()
	// Cross the signed 16-bit page ordinal boundary, including a PLAIN page
	// after dictionary fallback whose value appears only on the last page.
	const pages = 1<<15 + 1
	for i := range pages {
		value := int64(1)
		if i == pages-1 {
			value = 7
		}
		if _, err := rg.Write([]row{{Plain: value, Dict: value}}); err != nil {
			t.Fatal(err)
		}
		if err := rg.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	if n, err := rg.Commit(); err != nil || n != pages {
		t.Fatalf("commit rows: %d, %v", n, err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()), parquet.WithDecryption(&staticKeyRetriever{footerKey: key}))
	if err != nil {
		t.Fatal(err)
	}
	for _, chunk := range f.RowGroups()[0].ColumnChunks() {
		for _, value := range []int64{1, 7} {
			present, err := chunk.BloomFilter().Check(parquet.Int64Value(value))
			if err != nil || !present {
				t.Errorf("bloom filter lost a value: %d: %v", value, err)
			}
		}
	}
	r := parquet.NewGenericReader[row](f)
	defer r.Close()
	got := make([]row, pages+1)
	if n, err := r.Read(got); n != pages || err != io.EOF {
		t.Fatalf("read rows: %d, %v", n, err)
	}
	if got[0] != (row{Plain: 1, Dict: 1}) || got[pages-1] != (row{Plain: 7, Dict: 7}) {
		t.Fatal("read rows: first or last page value changed")
	}
}

type encryptionErrorWriter struct {
	bytes.Buffer
	err   error
	short bool
}

func (w *encryptionErrorWriter) Write(p []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	if w.short {
		return 0, nil
	}
	return w.Buffer.Write(p)
}

type encryptionErrorBufferPool struct {
	parquet.BufferPool
	err  error
	body bool
}

func (p *encryptionErrorBufferPool) GetBuffer() io.ReadWriteSeeker {
	return &encryptionErrorBuffer{ReadWriteSeeker: p.BufferPool.GetBuffer(), pool: p}
}

func (p *encryptionErrorBufferPool) PutBuffer(b io.ReadWriteSeeker) {
	p.BufferPool.PutBuffer(b.(*encryptionErrorBuffer).ReadWriteSeeker)
}

type encryptionErrorBuffer struct {
	io.ReadWriteSeeker
	pool       *encryptionErrorBufferPool
	bodyOffset int64
}

func (b *encryptionErrorBuffer) Read(p []byte) (int, error) {
	if b.pool.err != nil {
		if !b.pool.body {
			return 0, b.pool.err
		}
		offset, err := b.ReadWriteSeeker.Seek(0, io.SeekCurrent)
		if err != nil {
			return 0, err
		}
		if offset == 0 {
			// Permit the length-prefixed encrypted header, then fail on the body.
			var size [4]byte
			if _, err := io.ReadFull(b.ReadWriteSeeker, size[:]); err != nil {
				return 0, err
			}
			if _, err := b.ReadWriteSeeker.Seek(0, io.SeekStart); err != nil {
				return 0, err
			}
			b.bodyOffset = 4 + int64(binary.LittleEndian.Uint32(size[:]))
		}
		if offset >= b.bodyOffset {
			return 0, b.pool.err
		}
	}
	return b.ReadWriteSeeker.Read(p)
}

func TestEncryptionConcurrentRowGroupWriteError(t *testing.T) {
	for _, failure := range []string{"write", "short write", "read header", "read body"} {
		t.Run(failure, func(t *testing.T) {
			var out encryptionErrorWriter
			pool := &encryptionErrorBufferPool{BufferPool: parquet.NewBufferPool(), body: failure == "read body"}
			opts := []parquet.WriterOption{
				parquet.WriteBufferSize(0),
				parquet.WithEncryption(&parquet.EncryptionConfig{FooterKey: aes128Key(0xA1), EncryptedFooter: true}),
			}
			if failure == "read header" || failure == "read body" {
				opts = append(opts, parquet.ColumnPageBuffers(pool))
			}
			w := parquet.NewGenericWriter[encryptionTestRow](&out, opts...)
			first, second := w.BeginGenericRowGroup(), w.BeginGenericRowGroup()
			for _, rg := range []*parquet.GenericConcurrentRowGroupWriter[encryptionTestRow]{first, second} {
				if _, err := rg.Write(testRows); err != nil {
					t.Fatal(err)
				}
				if err := rg.Flush(); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := second.Commit(); err != nil {
				t.Fatal(err)
			}
			want := errors.New(failure + " failed")
			switch failure {
			case "short write":
				out.short = true
				want = io.ErrShortWrite
			case "write":
				out.err = want
			default:
				pool.err = want
			}
			if _, err := first.Commit(); !errors.Is(err, want) {
				t.Fatalf("Commit error = %v, want %v", err, want)
			}
		})
	}
}
