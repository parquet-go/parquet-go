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
// v1). TestDictionaryEncodedDataPageRejectsOutOfRangeIndex replaces its
// packed run with an RLE run containing an index outside the dictionary.
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
// negative or past the dictionary. The run below is structurally valid but
// repeats index 2 into a two-entry dictionary. It must return ErrCorrupted,
// not reach byteArrayDictionary.Lookup.
func TestDictionaryEncodedDataPageRejectsOutOfRangeIndex(t *testing.T) {
	seed := mustDecodeDictionaryFile(t, validDictionaryFileBase64)

	f, err := parquet.OpenFile(bytes.NewReader(seed), int64(len(seed)))
	if err != nil {
		t.Fatalf("seed file should open cleanly: %v", err)
	}
	if err := readAllDictionaryRows(f); err != nil {
		t.Fatalf("seed file should read cleanly: %v", err)
	}

	corrupt := bytes.Clone(seed)
	const indexStreamOffset = 53
	indexes := corrupt[indexStreamOffset : indexStreamOffset+3]
	if !bytes.Equal(indexes, []byte{1, 3, 2}) {
		t.Fatalf("seed file layout changed: got index stream %x", indexes)
	}
	// Two repeats of index 2, with the same encoded length as the seed.
	// The RLE value occupies a whole byte even when the width is one bit.
	indexes[1] = 4

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
