package parquet_test

import (
	"bytes"
	"encoding/base64"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
)

// validDictionaryFile is a tiny valid Parquet file (pyarrow 25.0.1: one string
// column "s", rows "a" and "b", PLAIN_DICTIONARY, no compression, data page
// v1). TestDictionaryEncodedDataPageRejectsOutOfRangeIndex flips its data-page
// bit-width byte so the same bytes decode to indexes outside the dictionary.
const validDictionaryFileBase64 = "UEFSMRUEFRQVFEwVBBUAEgAAAQAAAGEBAAAAYhUAFRIVEiwVBBUQFQYVBhwAAAACAAAABAEBAwIVBBksNQAYBnNjaGVtYRUCABUM" +
	"JQIYAXMlAEwcAAAAFgQZHBkcJgAcFQwZNQAGEBkYAXMVABYEFmgWaCY4JggpLBUEFQAVAgAVABUQFQIAPBYEGQYZJgAEAAAAFmgW" +
	"BCYIFmgAGRwYDEFSUk9XOnNjaGVtYRigAS8vLy8vM0FBQUFBUUFBQUFBQUFLQUF3QUJnQUZBQWdBQ2dBQUFBQUJCQUFNQUFBQUNB" +
	"QUlBQUFBQkFBSUFBQUFCQUFBQUFFQUFBQVVBQUFBRUFBVUFBZ0FCZ0FIQUF3QUFBQVFBQkFBQUFBQUFBRUZFQUFBQUJnQUFBQUVB" +
	"QUFBQUFBQUFBRUFBQUJ6QUFBQUJBQUVBQVFBQUFBQUFBQUEAGCBwYXJxdWV0LWNwcC1hcnJvdyB2ZXJzaW9uIDI1LjAuMRkcHAAA" +
	"AEABAABQQVIx" +
	""

func mustDecodeDictionaryFile(t *testing.T, s string) []byte {
	t.Helper()
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func readAllDictionaryRows(f *parquet.File) error {
	buf := make([]parquet.Row, 64)
	for _, rg := range f.RowGroups() {
		rows := rg.Rows()
		for {
			_, err := rows.ReadRows(buf)
			if err != nil {
				rows.Close()
				if errors.Is(err, io.EOF) {
					break
				}
				return err
			}
		}
	}
	return nil
}

// TestDictionaryEncodedDataPageRejectsOutOfRangeIndex is the regression from
// issue #613 (credit @cms103). Dictionary.Lookup panics when an index is
// negative or past the dictionary. The RLE/bit-packing hybrid stream of
// dictionary indexes starts with a one-byte bit width; this file's two-entry
// dictionary uses a width of 1. Flipping that byte to 2 decodes the same
// trailing bytes as indexes that no longer fit, which used to panic in
// byteArrayDictionary.Lookup and now returns an error wrapping ErrCorrupted.
func TestDictionaryEncodedDataPageRejectsOutOfRangeIndex(t *testing.T) {
	seed := mustDecodeDictionaryFile(t, validDictionaryFileBase64)

	f, err := parquet.OpenFile(bytes.NewReader(seed), int64(len(seed)))
	if err != nil {
		t.Fatalf("seed file should open cleanly: %v", err)
	}
	if err := readAllDictionaryRows(f); err != nil {
		t.Fatalf("seed file should read cleanly: %v", err)
	}

	// Offset 53 is the data page bit-width byte. Values other than 2 (up to
	// 32) also decode out of range; 2 is the smallest width that does.
	corrupt := append([]byte(nil), seed...)
	const bitWidthOffset = 53
	if corrupt[bitWidthOffset] != 1 {
		t.Fatalf("seed file layout changed: byte at offset %d is %#x, want 0x01", bitWidthOffset, corrupt[bitWidthOffset])
	}
	corrupt[bitWidthOffset] = 2

	f, err = parquet.OpenFile(bytes.NewReader(corrupt), int64(len(corrupt)))
	if err != nil {
		if !errors.Is(err, parquet.ErrCorrupted) {
			t.Fatalf("corrupted file rejected at open with %v, want an error wrapping ErrCorrupted", err)
		}
		return
	}

	err = readAllDictionaryRows(f)
	if err == nil {
		t.Fatal("reading a data page whose dictionary index is out of range should fail")
	}
	if !errors.Is(err, parquet.ErrCorrupted) {
		t.Fatalf("got %v, want an error wrapping ErrCorrupted", err)
	}
	if !strings.Contains(err.Error(), "out of bounds") && !strings.Contains(err.Error(), "out of range") {
		t.Errorf("got error %v, want one mentioning the index being out of range", err)
	}
}
