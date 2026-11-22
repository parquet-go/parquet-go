package alp

import (
	"testing"

	"github.com/parquet-go/bitpack"
)

func TestUnpackWithLookahead(t *testing.T) {
	// Each byte holds two 4-bit deltas. The remaining bytes belong to the page.
	page := [3 + bitpack.PaddingInt64]byte{0x10, 0x32, 0x54}
	packed := page[:3]

	var ints32 [6]int32
	unpack(ints32[:], packed, 4, bitpack.PaddingInt32)
	if want := [6]int32{0, 1, 2, 3, 4, 5}; ints32 != want {
		t.Errorf("32-bit deltas = %v, want %v", ints32, want)
	}
	var ints64 [6]int64
	unpack(ints64[:], packed, 4, bitpack.PaddingInt64)
	if want := [6]int64{0, 1, 2, 3, 4, 5}; ints64 != want {
		t.Errorf("64-bit deltas = %v, want %v", ints64, want)
	}
}
