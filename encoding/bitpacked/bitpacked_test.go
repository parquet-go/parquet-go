package bitpacked_test

import (
	"testing"

	"github.com/parquet-go/parquet-go/encoding/bitpacked"
	"github.com/parquet-go/parquet-go/encoding/fuzz"
)

func FuzzEncodeLevels(f *testing.F) {
	fuzz.EncodeLevels(f, &bitpacked.Encoding{BitWidth: 8})
}

// TestEncodeLevelsKnownBytes verifies the encoder and decoder against byte
// vectors derived directly from the Parquet BIT_PACKED spec (MSB-first
// packing).  These are not roundtrip tests: the expected bytes are computed
// independently from the implementation.
func TestEncodeLevelsKnownBytes(t *testing.T) {
	tests := []struct {
		name     string
		bitWidth int
		values   []uint8
		// wantBytes is the exact byte sequence mandated by the Parquet spec.
		// BIT_PACKED packs bits MSB-first: the first value's MSB occupies bit 7
		// of byte 0, subsequent values follow contiguously toward LSB.
		wantBytes []byte
	}{
		{
			// Spec example (Parquet Encodings page): values 0–7 with bitWidth=3.
			// Binary stream: 000 001 010 011 100 101 110 111
			//   Byte 0: 00000101 = 0x05
			//   Byte 1: 00111001 = 0x39
			//   Byte 2: 01110111 = 0x77
			name:      "spec-example-bitwidth3",
			bitWidth:  3,
			values:    []uint8{0, 1, 2, 3, 4, 5, 6, 7},
			wantBytes: []byte{0x05, 0x39, 0x77},
		},
		{
			// bitWidth=1: values packed one bit each, MSB first.
			// Binary stream: 0 1 1 0 1 0 1 1 = 01101011 = 0x6B
			name:      "bitwidth1",
			bitWidth:  1,
			values:    []uint8{0, 1, 1, 0, 1, 0, 1, 1},
			wantBytes: []byte{0x6B},
		},
		{
			// bitWidth=2: values packed two bits each, MSB first.
			// Binary stream: 00 01 10 11 00 01 10 11 = 00011011 00011011
			//   Byte 0: 0x1B, Byte 1: 0x1B
			name:      "bitwidth2",
			bitWidth:  2,
			values:    []uint8{0, 1, 2, 3, 0, 1, 2, 3},
			wantBytes: []byte{0x1B, 0x1B},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			enc := &bitpacked.Encoding{BitWidth: tc.bitWidth}

			// Encode and compare against known bytes.
			gotBytes, err := enc.EncodeLevels(nil, tc.values)
			if err != nil {
				t.Fatalf("EncodeLevels: %v", err)
			}
			if len(gotBytes) != len(tc.wantBytes) {
				t.Fatalf("EncodeLevels: got %d bytes, want %d\n  got:  %#v\n  want: %#v",
					len(gotBytes), len(tc.wantBytes), gotBytes, tc.wantBytes)
			}
			for i, b := range tc.wantBytes {
				if gotBytes[i] != b {
					t.Fatalf("EncodeLevels byte[%d]: got 0x%02X, want 0x%02X\n  got:  %#v\n  want: %#v",
						i, gotBytes[i], b, gotBytes, tc.wantBytes)
				}
			}

			// Decode the known bytes and compare against original values.
			gotValues, err := enc.DecodeLevels(nil, tc.wantBytes)
			if err != nil {
				t.Fatalf("DecodeLevels: %v", err)
			}
			// DecodeLevels may return extra values for padding bits; trim to
			// the number of original values before comparing.
			if len(gotValues) < len(tc.values) {
				t.Fatalf("DecodeLevels: got %d values, want at least %d", len(gotValues), len(tc.values))
			}
			gotValues = gotValues[:len(tc.values)]
			for i, v := range tc.values {
				if gotValues[i] != v {
					t.Fatalf("DecodeLevels value[%d]: got %d, want %d\n  got:  %v\n  want: %v",
						i, gotValues[i], v, gotValues, tc.values)
				}
			}
		})
	}
}
