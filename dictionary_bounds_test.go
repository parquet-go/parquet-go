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

// validDictionaryFile is a tiny, valid Parquet file (written by pyarrow
// 25.0.1: one string column "s", two rows "a" and "b", PLAIN_DICTIONARY
// encoded, no compression, data page v1). It is used as the seed for
// TestDictionaryEncodedDataPageRejectsOutOfRangeIndex below: flipping a
// single byte of it produces a well-formed-looking file whose data page
// disagrees with its dictionary.
const validDictionaryFileBase64 = "UEFSMRUEFRQVFEwVBBUAEgAAAQAAAGEBAAAAYhUAFRIVEiwVBBUQFQYVBhwAAAACAAAABAEBAwIVBBksNQAYBnNjaGVtYRUCABUM" +
	"JQIYAXMlAEwcAAAAFgQZHBkcJgAcFQwZNQAGEBkYAXMVABYEFmgWaCY4JggpLBUEFQAVAgAVABUQFQIAPBYEGQYZJgAEAAAAFmgW" +
	"BCYIFmgAGRwYDEFSUk9XOnNjaGVtYRigAS8vLy8vM0FBQUFBUUFBQUFBQUFLQUF3QUJnQUZBQWdBQ2dBQUFBQUJCQUFNQUFBQUNB" +
	"QUlBQUFBQkFBSUFBQUFCQUFBQUFFQUFBQVVBQUFBRUFBVUFBZ0FCZ0FIQUF3QUFBQVFBQkFBQUFBQUFBRUZFQUFBQUJnQUFBQUVB" +
	"QUFBQUFBQUFBRUFBQUJ6QUFBQUJBQUVBQVFBQUFBQUFBQUEAGCBwYXJxdWV0LWNwcC1hcnJvdyB2ZXJzaW9uIDI1LjAuMRkcHAAA" +
	"AEABAABQQVIx" +
	""

func mustDecodeBase64(t *testing.T, s string) []byte {
	t.Helper()
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// readAllRowsIgnoringEOF reads every row of every row group of f, returning the first
// non-io.EOF error encountered (if any).
func readAllRowsIgnoringEOF(f *parquet.File) error {
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

// TestDictionaryEncodedDataPageRejectsOutOfRangeIndex is a regression test
// for a panic reading a Parquet file whose dictionary-encoded data page
// contains an index that is out of range for its dictionary.
//
// Every implementation of Dictionary.Lookup / Dictionary.Index (for every
// physical type, and for each of the generic, amd64 and purego code paths)
// is documented to panic if an index is negative or >= the dictionary's
// length -- see the Dictionary.Lookup doc comment in dictionary.go. That
// precondition is never checked before untrusted bytes from a data page
// reach it: decodeDataPage in column.go decodes a data page's RLE/bit-
// packing hybrid stream straight into the []int32 indexes an indexedPage
// holds, with no validation against the dictionary it was built for.
//
// A single corrupted byte is enough to trigger it. The RLE/bit-packing
// hybrid encoding used for dictionary indexes begins with a one-byte bit
// width (see the Parquet format's Encodings.md): a data page written for a
// two-entry dictionary uses a bit width of 1 (indexes 0 and 1 fit in one
// bit). If that leading byte is corrupted to claim a wider bit width, the
// same trailing bytes decode to numerically larger indexes -- some of
// which are no longer valid indexes into the (unchanged) two-entry
// dictionary -- and parquet-go panics instead of returning an error:
//
//	panic: runtime error: index out of range [3] with length 3
//
//	github.com/parquet-go/parquet-go.(*byteArrayPage).index(...)
//		page_byte_array.go:67
//	github.com/parquet-go/parquet-go.(*byteArrayDictionary).lookupString(...)
//		dictionary_purego.go:43
//	github.com/parquet-go/parquet-go.(*byteArrayDictionary).Lookup(...)
//		dictionary_byte_array.go:97
//	github.com/parquet-go/parquet-go.(*indexedPageValues).ReadValues(...)
//		dictionary.go:237
//	github.com/parquet-go/parquet-go.(*optionalPageValues).ReadValues(...)
//		page_optional.go:95
//	github.com/parquet-go/parquet-go.(*columnChunkValueReader).ReadValues(...)
//		column_chunk.go:138
//	github.com/parquet-go/parquet-go.(*rowGroupRows).ReadRows(...)
//		row_group.go:315
//
// A file's dictionary and data pages are two separate sections that a
// reader has no way to cross-validate other than by decoding the indexes
// and checking them, so this is not limited to a bit-width corruption:
// deliberately or accidentally malformed Parquet input can put an
// out-of-range index in a data page in several ways (this test's seed file,
// generated separately by randomly mutating a larger multi-column file and
// keeping only crashers, also produced instances via a corrupted
// SchemaElement.repetition_type in the footer changing a MAP's key_value
// group from REPEATED to REQUIRED, which shifts how many repetition-level
// bytes the reader expects before the index stream). Any application that
// calls parquet.OpenFile or parquet.NewGenericReader on a file it does not
// fully trust (uploaded by a user, fetched over the network, read from a
// shared bucket) can be crashed by this: a Parquet file is largely
// self-describing bytes on disk, not something typically re-validated
// against an external schema before it's read.
func TestDictionaryEncodedDataPageRejectsOutOfRangeIndex(t *testing.T) {
	seed := mustDecodeBase64(t, validDictionaryFileBase64)

	// Sanity check: the seed file is valid and reads back its two rows.
	f, err := parquet.OpenFile(bytes.NewReader(seed), int64(len(seed)))
	if err != nil {
		t.Fatalf("seed file should open cleanly: %v", err)
	}
	if err := readAllRowsIgnoringEOF(f); err != nil {
		t.Fatalf("seed file should read cleanly: %v", err)
	}

	// Flip the data page's bit-width byte (offset 53) from 1 to 2. Every
	// other value tried here (up to 32) reproduces the same panic; 2 is
	// the smallest and clearest ("one dictionary index needs one more bit
	// than the dictionary actually has").
	corrupt := append([]byte(nil), seed...)
	const bitWidthOffset = 53
	if corrupt[bitWidthOffset] != 1 {
		t.Fatalf("seed file layout changed: byte at offset %d is %#x, want 0x01 (the expected bit-width byte); this test's offset needs updating", bitWidthOffset, corrupt[bitWidthOffset])
	}
	corrupt[bitWidthOffset] = 2

	f, err = parquet.OpenFile(bytes.NewReader(corrupt), int64(len(corrupt)))
	if err != nil {
		// Rejecting it while opening (before any row is read) would also
		// be an acceptable outcome; this seed happens to open fine and
		// fail only once its data page is decoded.
		t.Logf("corrupted file rejected at open: %v (acceptable)", err)
		return
	}

	err = readAllRowsIgnoringEOF(f)
	if err == nil {
		t.Fatal("reading a data page whose dictionary index is out of range should fail, not succeed")
	}
	if !strings.Contains(err.Error(), "out of bounds") && !strings.Contains(err.Error(), "out of range") {
		t.Errorf("got error %v, want one mentioning the index being out of range/bounds", err)
	}
	// The real assertion is implicit: reaching this line at all means
	// readAllRowsIgnoringEOF returned an error instead of the runtime panicking.
}
