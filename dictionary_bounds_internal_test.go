package parquet

import (
	"errors"
	"io"
	"math"
	"testing"

	"github.com/parquet-go/parquet-go/encoding"
	"github.com/parquet-go/parquet-go/format"
)

func TestCheckDictionaryIndexBounds(t *testing.T) {
	checks := []struct {
		name    string
		indexes []int32
		num     int
		dictLen int
		bad     bool
	}{
		{name: "nothing kept", indexes: []int32{99}, num: 0, dictLen: 1},
		{name: "tail dropped", indexes: []int32{0, 1, 99}, num: 2, dictLen: 2},
		{name: "tail kept", indexes: []int32{0, 1, 99}, num: 3, dictLen: 2, bad: true},
		{name: "zero fill empty dictionary", indexes: nil, num: 3, dictLen: 0, bad: true},
		{name: "zero fill", indexes: []int32{1}, num: 4, dictLen: 2},
		{name: "short prefix out of range", indexes: []int32{5}, num: 4, dictLen: 2, bad: true},
		{name: "negative", indexes: []int32{-1, 0}, num: 2, dictLen: 2, bad: true},
		{name: "max int32 equals dictionary length", indexes: []int32{math.MaxInt32}, num: 1, dictLen: math.MaxInt32, bad: true},
		{name: "max int32 inside a longer dictionary", indexes: []int32{math.MaxInt32}, num: 1, dictLen: math.MaxInt32 + 1},
		{name: "negative inside a longer dictionary", indexes: []int32{-1}, num: 1, dictLen: math.MaxInt32 + 1, bad: true},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			err := checkDictionaryIndexBounds(check.indexes, check.num, check.dictLen)
			if !check.bad {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if !errors.Is(err, ErrCorrupted) {
				t.Fatalf("got %v, want an error wrapping ErrCorrupted", err)
			}
		})
	}
}

func TestDictionaryIndexBounds(t *testing.T) {
	col := &Column{typ: ByteArrayType, schema: &format.SchemaElement{Name: "s"}}
	dict := newTestByteArrayDictionary("a", "b")

	t.Run("in range", func(t *testing.T) {
		page := decodeDictionaryPage(t, col, dict, format.RLEDictionary, 4, []int32{0, 1, 0, 1})
		defer Release(page)
		if got, want := readPageStrings(t, page), []string{"a", "b", "a", "b"}; !stringSlicesEqual(got, want) {
			t.Fatalf("values = %q, want %q", got, want)
		}
	})

	t.Run("plain dictionary encoding", func(t *testing.T) {
		page := decodeDictionaryPage(t, col, dict, format.PlainDictionary, 2, []int32{1, 0})
		defer Release(page)
		if got, want := readPageStrings(t, page), []string{"b", "a"}; !stringSlicesEqual(got, want) {
			t.Fatalf("values = %q, want %q", got, want)
		}
	})

	t.Run("data page v2", func(t *testing.T) {
		encoded := encodeDictionaryIndexes(t, []int32{0, 1, 7})
		page, err := col.decodeDataPageV2(DataPageHeaderV2{header: &format.DataPageHeaderV2{
			NumValues: 3,
			NumRows:   3,
			Encoding:  format.RLEDictionary,
		}}, newBuffer(encoded), dict, -1)
		requireDictionaryIndexError(t, page, err)
	})

	t.Run("kept index out of range", func(t *testing.T) {
		page, err := decodeDictionaryPageResult(col, dict, format.RLEDictionary, 4, encodeDictionaryIndexes(t, []int32{0, 1, 0, 2}))
		requireDictionaryIndexError(t, page, err)
	})

	t.Run("negative index", func(t *testing.T) {
		page, err := decodeDictionaryPageResult(col, dict, format.RLEDictionary, 1, encodeDictionaryIndexes(t, []int32{-1}))
		requireDictionaryIndexError(t, page, err)
	})

	// Bit width does not mask an RLE value down to that width. A one-bit page
	// can still carry an index the dictionary does not have.
	t.Run("unmasked rle value", func(t *testing.T) {
		page, err := decodeDictionaryPageResult(col, dict, format.RLEDictionary, 1, []byte{1, 2, 5})
		requireDictionaryIndexError(t, page, err)
	})

	t.Run("no dictionary", func(t *testing.T) {
		page, err := decodeDictionaryPageResult(col, nil, format.RLEDictionary, 2, encodeDictionaryIndexes(t, []int32{0, 1}))
		if page != nil {
			Release(page)
		}
		if !errors.Is(err, ErrCorrupted) {
			t.Fatalf("got %v, want an error wrapping ErrCorrupted", err)
		}
	})

	// newIndexedPage zero-fills when the decoder returns fewer indexes than
	// numValues. Index 0 into an empty dictionary panics in Lookup.
	t.Run("short decode empty dictionary", func(t *testing.T) {
		empty := ByteArrayType.NewDictionary(0, 0, encoding.ByteArrayValues(nil, nil))
		const numValues = 4
		data := []byte{0} // bit width 0, no payload: decodes to nothing
		decoded, err := RLEDictionary.DecodeInt32(nil, data)
		if err != nil {
			t.Fatal(err)
		}
		if len(decoded) >= numValues {
			t.Fatalf("decoded %d indexes, want a short decode below %d", len(decoded), numValues)
		}
		page, err := decodeDictionaryPageResult(col, empty, format.RLEDictionary, numValues, data)
		requireDictionaryIndexError(t, page, err)
	})

	// The same zero fill is a valid index when the dictionary is not empty.
	t.Run("short decode in range", func(t *testing.T) {
		const numValues = 5
		data := encodeDictionaryIndexes(t, []int32{0, 1})
		decoded, err := RLEDictionary.DecodeInt32(nil, data)
		if err != nil {
			t.Fatal(err)
		}
		if len(decoded) >= numValues {
			t.Fatalf("decoded %d indexes, want a short decode below %d", len(decoded), numValues)
		}
		page, err := decodeDictionaryPageResult(col, dict, format.RLEDictionary, numValues, data)
		page = mustDictionaryPage(t, page, err)
		defer Release(page)
		if got, want := readPageStrings(t, page), []string{"a", "b", "a", "a", "a"}; !stringSlicesEqual(got, want) {
			t.Fatalf("values = %q, want %q", got, want)
		}
	})

	// Bit-packed groups are multiples of 8. The page keeps numValues of them
	// and drops the tail, so an out-of-range index past that point is not an
	// error.
	t.Run("out of range tail dropped", func(t *testing.T) {
		const numValues = 3
		indexes := []int32{0, 1, 0, 100, 100, 100, 100, 100}
		data := encodeDictionaryIndexes(t, indexes)
		decoded, err := RLEDictionary.DecodeInt32(nil, data)
		if err != nil {
			t.Fatal(err)
		}
		if len(decoded) <= numValues {
			t.Fatalf("decoded %d indexes, want a tail past %d", len(decoded), numValues)
		}
		for _, index := range decoded[numValues:] {
			if int(index) < dict.Len() {
				t.Fatalf("tail index %d is inside the dictionary; the encoder did not leave an out-of-range tail", index)
			}
		}
		page, err := decodeDictionaryPageResult(col, dict, format.RLEDictionary, numValues, data)
		page = mustDictionaryPage(t, page, err)
		defer Release(page)
		if got, want := readPageStrings(t, page), []string{"a", "b", "a"}; !stringSlicesEqual(got, want) {
			t.Fatalf("values = %q, want %q", got, want)
		}
	})
}

func newTestByteArrayDictionary(values ...string) Dictionary {
	var buf []byte
	offsets := []uint32{0}
	for _, value := range values {
		buf = append(buf, value...)
		offsets = append(offsets, uint32(len(buf)))
	}
	return ByteArrayType.NewDictionary(0, len(values), encoding.ByteArrayValues(buf, offsets))
}

func encodeDictionaryIndexes(t *testing.T, indexes []int32) []byte {
	t.Helper()
	encoded, err := RLEDictionary.EncodeInt32(nil, indexes)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func decodeDictionaryPage(t *testing.T, col *Column, dict Dictionary, enc format.Encoding, numValues int, indexes []int32) Page {
	t.Helper()
	page, err := decodeDictionaryPageResult(col, dict, enc, numValues, encodeDictionaryIndexes(t, indexes))
	return mustDictionaryPage(t, page, err)
}

func decodeDictionaryPageResult(col *Column, dict Dictionary, enc format.Encoding, numValues int, data []byte) (Page, error) {
	return col.decodeDataPageV1(DataPageHeaderV1{header: &format.DataPageHeader{
		NumValues:               int32(numValues),
		Encoding:                enc,
		DefinitionLevelEncoding: format.RLE,
		RepetitionLevelEncoding: format.RLE,
	}}, newBuffer(data), dict, -1)
}

func mustDictionaryPage(t *testing.T, page Page, err error) Page {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	return page
}

func requireDictionaryIndexError(t *testing.T, page Page, err error) {
	t.Helper()
	if err == nil {
		defer Release(page)
		// Missing the check, Lookup panics on the bad index.
		readPageStrings(t, page)
		t.Fatal("expected an out-of-range dictionary index error")
	}
	if page != nil {
		Release(page)
	}
	if !errors.Is(err, ErrCorrupted) {
		t.Fatalf("got %v, want an error wrapping ErrCorrupted", err)
	}
}

func readPageStrings(t *testing.T, page Page) []string {
	t.Helper()
	values := make([]Value, page.NumValues())
	n, err := page.Values().ReadValues(values)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	got := make([]string, n)
	for i, value := range values[:n] {
		got[i] = string(value.ByteArray())
	}
	return got
}

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
