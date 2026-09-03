package parquet

import (
	"math/bits"
	"math/rand"
	"testing"

	"github.com/parquet-go/parquet-go/encoding/rle"
)

// encodeTestLevels encodes levels using the RLE/bit-packed hybrid encoding
// with the bit width derived from maxLevel, like the parquet-go writer does.
func encodeTestLevels(t testing.TB, levels []byte, maxLevel byte) []byte {
	t.Helper()
	enc := rle.Encoding{BitWidth: bits.Len8(maxLevel)}
	data, err := enc.EncodeLevels(nil, levels)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// makeFixedLevels returns the repetition and definition levels of numRows
// lists of length n with no nulls.
func makeFixedLevels(numRows, n int, maxDef byte) (rep, def []byte) {
	rep = make([]byte, numRows*n)
	def = make([]byte, numRows*n)
	for i := range rep {
		if i%n != 0 {
			rep[i] = 1
		}
		def[i] = maxDef
	}
	return rep, def
}

func TestDetectFixedList(t *testing.T) {
	for _, maxDef := range []byte{1, 2, 3} {
		for _, n := range []int{1, 2, 3, 4, 5, 7, 8, 9, 15, 16, 17, 24, 64, 100, 768} {
			for _, numRows := range []int{1, 2, 3, 7, 8, 100} {
				rep, def := makeFixedLevels(numRows, n, maxDef)
				repData := encodeTestLevels(t, rep, 1)
				defData := encodeTestLevels(t, def, maxDef)
				numValues := numRows * n

				got, ok := detectFixedList(repData, defData, numValues, maxDef)
				if !ok || got != n {
					t.Errorf("maxDef=%d n=%d numRows=%d: got (%d,%t), want (%d,true)", maxDef, n, numRows, got, ok, n)
				}
			}
		}
	}
}

func TestDetectFixedListRejects(t *testing.T) {
	const maxDef = 1

	encode := func(rep, def []byte) (repData, defData []byte) {
		return encodeTestLevels(t, rep, 1), encodeTestLevels(t, def, maxDef)
	}

	t.Run("variable lengths", func(t *testing.T) {
		rep := []byte{0, 1, 1, 0, 1, 0, 1, 1, 1}
		def := []byte{1, 1, 1, 1, 1, 1, 1, 1, 1}
		repData, defData := encode(rep, def)
		if _, ok := detectFixedList(repData, defData, len(rep), maxDef); ok {
			t.Error("detected fixed list in variable-length stream")
		}
	})

	t.Run("last list differs", func(t *testing.T) {
		numRows, n := 100, 3
		rep, def := makeFixedLevels(numRows, n, maxDef)
		rep = append(rep, 1) // extend the last list by one element
		def = append(def, maxDef)
		repData, defData := encode(rep, def)
		if _, ok := detectFixedList(repData, defData, len(rep), maxDef); ok {
			t.Error("detected fixed list when last list is longer")
		}
	})

	t.Run("last list shorter", func(t *testing.T) {
		numRows, n := 100, 3
		rep, def := makeFixedLevels(numRows, n, maxDef)
		rep = rep[:len(rep)-1] // truncate the last list
		def = def[:len(def)-1]
		repData, defData := encode(rep, def)
		if _, ok := detectFixedList(repData, defData, len(rep), maxDef); ok {
			t.Error("detected fixed list when last list is truncated")
		}
	})

	t.Run("null element", func(t *testing.T) {
		const maxDef = 2
		numRows, n := 100, 4
		rep, def := makeFixedLevels(numRows, n, maxDef)
		def[57] = 1 // null element in the middle
		repData := encodeTestLevels(t, rep, 1)
		defData := encodeTestLevels(t, def, maxDef)
		if _, ok := detectFixedList(repData, defData, len(rep), maxDef); ok {
			t.Error("detected fixed list with a null element")
		}
	})

	t.Run("empty list", func(t *testing.T) {
		// Lists of length 4 with an empty list in the middle: the empty
		// list contributes one value slot with def level 0.
		rep := []byte{0, 1, 1, 1, 0, 0, 1, 1, 1}
		def := []byte{1, 1, 1, 1, 0, 1, 1, 1, 1}
		repData, defData := encode(rep, def)
		if _, ok := detectFixedList(repData, defData, len(rep), maxDef); ok {
			t.Error("detected fixed list with an empty list")
		}
	})

	t.Run("page starts mid-row", func(t *testing.T) {
		rep := []byte{1, 1, 0, 1, 1, 0, 1, 1}
		def := []byte{1, 1, 1, 1, 1, 1, 1, 1}
		repData, defData := encode(rep, def)
		if _, ok := detectFixedList(repData, defData, len(rep), maxDef); ok {
			t.Error("detected fixed list in page starting mid-row")
		}
	})

	t.Run("empty page", func(t *testing.T) {
		if _, ok := detectFixedList(nil, nil, 0, maxDef); ok {
			t.Error("detected fixed list in empty page")
		}
	})

	t.Run("def stream too short", func(t *testing.T) {
		numRows, n := 10, 3
		rep, def := makeFixedLevels(numRows, n, maxDef)
		repData := encodeTestLevels(t, rep, 1)
		defData := encodeTestLevels(t, def[:len(def)-5], maxDef)
		if _, ok := detectFixedList(repData, defData, len(rep), maxDef); ok {
			t.Error("detected fixed list with truncated definition levels")
		}
	})
}

// TestDetectFixedListRandomized cross-checks the detector against ground
// truth computed from the decoded levels, on randomly generated pages.
func TestDetectFixedListRandomized(t *testing.T) {
	prng := rand.New(rand.NewSource(0))

	for range 2000 {
		maxDef := byte(1 + prng.Intn(3))
		numRows := 1 + prng.Intn(50)
		fixed := prng.Intn(2) == 0
		n := 1 + prng.Intn(20)
		withNulls := !fixed && prng.Intn(2) == 0

		var rep, def []byte
		for range numRows {
			length := n
			if !fixed {
				length = 1 + prng.Intn(20)
			}
			for j := range length {
				if j == 0 {
					rep = append(rep, 0)
				} else {
					rep = append(rep, 1)
				}
				d := maxDef
				if withNulls && prng.Intn(10) == 0 {
					d = byte(prng.Intn(int(maxDef)))
				}
				def = append(def, d)
			}
		}

		// Ground truth: fixed length iff all rows have the same length and
		// every definition level is maxDef.
		truthN := -1
		truthOK := true
		rowLen := 0
		for i := range rep {
			if rep[i] == 0 && i > 0 {
				if truthN == -1 {
					truthN = rowLen
				} else if rowLen != truthN {
					truthOK = false
				}
				rowLen = 0
			}
			rowLen++
		}
		if truthN == -1 {
			truthN = rowLen
		} else if rowLen != truthN {
			truthOK = false
		}
		for _, d := range def {
			if d != maxDef {
				truthOK = false
			}
		}

		repData := encodeTestLevels(t, rep, 1)
		defData := encodeTestLevels(t, def, maxDef)
		gotN, gotOK := detectFixedList(repData, defData, len(rep), maxDef)

		if gotOK != truthOK || (gotOK && gotN != truthN) {
			t.Fatalf("numRows=%d rep=%v def=%v: got (%d,%t), want (%d,%t)",
				numRows, rep, def, gotN, gotOK, truthN, truthOK)
		}
	}
}
