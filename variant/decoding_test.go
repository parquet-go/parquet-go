package variant

import (
	"strings"
	"testing"
)

// TestDecodeRejectsCorruptSizes verifies that inputs declaring more elements
// than the data can possibly hold are rejected before any allocation is
// sized from them. Element counts and dictionary sizes are attacker
// controlled; decoding a 5-byte value must never allocate gigabytes (found
// by fuzzing).
func TestDecodeRejectsCorruptSizes(t *testing.T) {
	tests := []struct {
		name    string
		meta    Metadata
		value   []byte
		wantErr string
	}{
		{
			// Object header with is_large=1 declaring 2^31-1 fields,
			// followed by no field data at all.
			name:    "object element count exceeds input",
			value:   []byte{0x42, 0xFF, 0xFF, 0xFF, 0x7F},
			wantErr: "element count",
		},
		{
			// Array header with is_large=1 declaring 2^31-1 elements.
			name:    "array element count exceeds input",
			value:   []byte{0x13, 0xFF, 0xFF, 0xFF, 0x7F},
			wantErr: "element count",
		},
		{
			// Object with two fields that both reference dictionary entry 0
			// ("a"): the spec requires field names to be unique within an
			// object. Layout: header, num_elements=2, field_ids{0,0},
			// offsets{0,1,2}, values{null,null}.
			name:    "duplicate object field names",
			meta:    Metadata{Strings: []string{"a"}},
			value:   []byte{0x02, 0x02, 0x00, 0x00, 0x00, 0x01, 0x02, 0x00, 0x00},
			wantErr: "duplicate object field",
		},
		{
			// Fields named {b, a, b}: the duplicate is not adjacent, so it
			// is only caught by the map-based check that kicks in when the
			// field names violate the spec's lexicographic order. Layout:
			// header, num_elements=3, field_ids{1,0,1}, offsets{0,1,2,3},
			// values{null,null,null}.
			name:    "duplicate object field names out of order",
			meta:    Metadata{Strings: []string{"a", "b"}},
			value:   []byte{0x02, 0x03, 0x01, 0x00, 0x01, 0x00, 0x01, 0x02, 0x03, 0x00, 0x00, 0x00},
			wantErr: "duplicate object field",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Decode(tt.meta, tt.value)
			if err == nil {
				t.Fatalf("Decode succeeded, want error containing %q", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Decode error = %q, want error containing %q", err, tt.wantErr)
			}
		})
	}
}

// TestDecodeUnsortedObjectFields verifies that objects whose field names are
// not in lexicographic order still decode: the spec requires writers to sort,
// but readers stay lenient as long as the names are unique.
func TestDecodeUnsortedObjectFields(t *testing.T) {
	// Fields named {b, a}. Layout: header, num_elements=2, field_ids{1,0},
	// offsets{0,1,2}, values{null,null}.
	meta := Metadata{Strings: []string{"a", "b"}}
	value := []byte{0x02, 0x02, 0x01, 0x00, 0x00, 0x01, 0x02, 0x00, 0x00}

	got, err := Decode(meta, value)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	want := MakeObject([]Field{
		{Name: "a", Value: Null()},
		{Name: "b", Value: Null()},
	})
	if !got.Equal(want) {
		t.Errorf("decoded value mismatch:\n got: %#v\nwant: %#v", got.GoValue(), want.GoValue())
	}
}

// TestDecodeMetadataRejectsCorruptSizes verifies that a metadata header
// declaring a dictionary larger than the input is rejected before the
// offset slice is allocated.
func TestDecodeMetadataRejectsCorruptSizes(t *testing.T) {
	// Version 1, 1-byte offsets, dictionary_size=200, then nothing.
	_, err := DecodeMetadata([]byte{0x01, 0xC8})
	if err == nil {
		t.Fatal("DecodeMetadata succeeded, want dictionary size error")
	}
	if !strings.Contains(err.Error(), "dictionary size") {
		t.Fatalf("DecodeMetadata error = %q, want dictionary size error", err)
	}
}

// TestDecodeRejectsInvalidUTF8 verifies that strings are validated on
// decode: the spec requires variant strings and metadata dictionary entries
// to be valid UTF-8. 0xFF can never appear in UTF-8 text.
func TestDecodeRejectsInvalidUTF8(t *testing.T) {
	t.Run("short string", func(t *testing.T) {
		// Short-string header with length 1, followed by 0xFF.
		if _, err := Decode(Metadata{}, []byte{0x05, 0xFF}); err == nil {
			t.Fatal("Decode succeeded, want invalid UTF-8 error")
		}
	})
	t.Run("string primitive", func(t *testing.T) {
		// String primitive (type 16) with 4-byte length 1, followed by 0xFF.
		if _, err := Decode(Metadata{}, []byte{0x40, 0x01, 0x00, 0x00, 0x00, 0xFF}); err == nil {
			t.Fatal("Decode succeeded, want invalid UTF-8 error")
		}
	})
	t.Run("metadata dictionary entry", func(t *testing.T) {
		// Dictionary with one 1-byte string 0xFF.
		if _, err := DecodeMetadata([]byte{0x01, 0x01, 0x00, 0x01, 0xFF}); err == nil {
			t.Fatal("DecodeMetadata succeeded, want invalid UTF-8 error")
		}
	})
}
