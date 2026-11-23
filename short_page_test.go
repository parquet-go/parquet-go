package parquet_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"runtime"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
)

// TestShortDataPageRejectsPooledLeftovers is the regression for issue #624.
//
// A data page whose payload decodes fewer values than the header's num_values
// used to be accepted. The page was then sliced up to the declared count, and
// the bytes past the decoded length were whatever an earlier page had left in
// a reused buffer. Reading that page returned those leftover values and no
// error.
func TestShortDataPageRejectsPooledLeftovers(t *testing.T) {
	// sync.Pool hands buffers back to the same P. One P makes the reuse that
	// this test is looking for reliable.
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))

	const declared = 24

	write := func(v bool, n int) []byte {
		t.Helper()
		return writeRLEBooleans(t, repeatBool(v, n))
	}

	// Fill pooled decode buffers with 0xFF from pages of true, and return
	// them. A later short page must not read those bytes back.
	for _, n := range []int{declared, declared, 4096, declared} {
		if _, err := readBooleans(t, write(true, n)); err != nil {
			t.Fatalf("reading %d true values: %v", n, err)
		}
	}

	original := write(false, declared)
	got, err := readBooleans(t, original)
	if err != nil {
		t.Fatalf("reading an intact page of %d false values: %v", declared, err)
	}
	if len(got) != declared || countTrue(got) != 0 {
		t.Fatalf("intact page: got %d values, %d true, want %d false", len(got), countTrue(got), declared)
	}

	// The intact read returned its buffers. Prime the pool again so a short
	// read that reaches past the decoded length observes true bits, not zeros.
	for _, n := range []int{4096, declared} {
		if _, err := readBooleans(t, write(true, n)); err != nil {
			t.Fatalf("re-reading %d true values: %v", n, err)
		}
	}

	patched, runLen := shortenBooleanRun(t, append([]byte(nil), original...), declared)
	t.Logf("patched the run to %d values (page declares %d)", runLen, declared)

	got, err = readBooleans(t, patched)
	if err == nil {
		t.Fatalf("short page returned %d values, %d of them true, and no error", len(got), countTrue(got))
	}
	if !errors.Is(err, parquet.ErrCorrupted) {
		t.Fatalf("got %v, want an error wrapping ErrCorrupted", err)
	}
	if !strings.Contains(err.Error(), "holds only") {
		t.Fatalf("got %v, want an error reporting the decoded length", err)
	}
	if len(got) != 0 {
		t.Fatalf("short page returned %d values (%d true) along with %v", len(got), countTrue(got), err)
	}
}

func writeRLEBooleans(t *testing.T, vals []bool) []byte {
	t.Helper()
	schema := parquet.NewSchema("t", parquet.Group{
		"b": parquet.Encoded(parquet.Leaf(parquet.BooleanType), &parquet.RLE),
	})
	var buf bytes.Buffer
	w := parquet.NewWriter(&buf, schema, parquet.Compression(&parquet.Uncompressed))
	for _, v := range vals {
		if _, err := w.WriteRows([]parquet.Row{{parquet.BooleanValue(v).Level(0, 0, 0)}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func readBooleans(t *testing.T, data []byte) ([]bool, error) {
	t.Helper()
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	r := parquet.NewReader(f)
	defer r.Close()

	rows := make([]parquet.Row, 64)
	var out []bool
	for {
		n, err := r.ReadRows(rows)
		for _, row := range rows[:n] {
			if len(row) != 1 {
				t.Fatalf("row width %d, want 1", len(row))
			}
			out = append(out, row[0].Boolean())
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return out, nil
			}
			return out, err
		}
	}
}

// shortenBooleanRun rewrites the single RLE run of an all-false required
// BOOLEAN page so the payload decodes fewer values than the header declares.
// The page byte length stays the same. The page CRC is rewritten in place,
// picking a run length whose zigzag encoding is the same size as the old one.
func shortenBooleanRun(t *testing.T, file []byte, declared int) ([]byte, int) {
	t.Helper()
	if declared <= 1 || declared >= 128 {
		t.Fatalf("declared = %d, want 2..127 so the run header is one byte", declared)
	}
	pattern := []byte{2, 0, 0, 0, byte(declared << 1), 0x00}
	idx := bytes.Index(file, pattern)
	if idx < 0 {
		t.Fatalf("RLE run of %d false values not found", declared)
	}
	const pageLen = 6
	zz := func(v uint32) []byte {
		s := int32(v)
		return binary.AppendUvarint(nil, uint64(uint32((s<<1)^(s>>31))))
	}
	oldCRC := zz(crc32.ChecksumIEEE(file[idx : idx+pageLen]))
	at := bytes.LastIndex(file[:idx], oldCRC)
	if at < 0 {
		t.Fatal("page checksum not found in the page header")
	}
	for n := 1; n < declared; n++ {
		page := append([]byte(nil), file[idx:idx+pageLen]...)
		page[4] = byte(n << 1)
		newCRC := zz(crc32.ChecksumIEEE(page))
		if len(newCRC) != len(oldCRC) {
			continue
		}
		copy(file[idx:], page)
		copy(file[at:], newCRC)
		return file, n
	}
	t.Fatal("no shorter run kept the checksum the same size")
	return nil, 0
}

func repeatBool(v bool, n int) []bool {
	vals := make([]bool, n)
	for i := range vals {
		vals[i] = v
	}
	return vals
}

func countTrue(vals []bool) int {
	n := 0
	for _, v := range vals {
		if v {
			n++
		}
	}
	return n
}
