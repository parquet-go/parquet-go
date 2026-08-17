package parquet

import (
	"encoding/binary"
	"math/bits"
)

// fixedListFastPathEnabled controls whether data page decoding attempts to
// detect pages containing only lists of the same length with no null values
// by inspecting the encoded repetition and definition level streams. When a
// page passes detection, the level streams are never decoded and the page is
// represented by a fixedRepeatedPage, which computes row boundaries
// arithmetically.
//
// The detection verifies the entire encoded streams, so enabling the fast
// path never changes the values, repetition levels, or definition levels
// observed by readers. The variable exists so tests and benchmarks can
// compare both code paths.
var fixedListFastPathEnabled = true

// maxDetectableRunLength bounds the run lengths accepted by the detector.
// The bound guards the arithmetic on run lengths (e.g. count*8 for
// bit-packed runs) against overflow, including on 32-bit platforms, and is
// still far beyond the length of any valid run in a real page.
const maxDetectableRunLength = 1 << 24

// detectFixedList inspects the encoded (RLE/bit-packed hybrid) repetition and
// definition level streams of a data page holding numValues values, and
// reports whether the page contains only complete lists of the same length
// with no null values.
//
// It returns the common list length and true when:
//
//   - every definition level equals maxDefinitionLevel (no null or empty
//     lists, no null elements), and
//   - the repetition levels encode the periodic pattern 0,1,1,...,1 with a
//     constant distance n between record boundaries, and
//   - numValues is a multiple of n (the last list is complete).
//
// The caller must only use this for columns with maxRepetitionLevel == 1
// (the repetition level stream is then encoded with a bit width of 1).
func detectFixedList(repData, defData []byte, numValues int, maxDefinitionLevel byte) (listLength int, ok bool) {
	if numValues <= 0 || maxDefinitionLevel == 0 {
		return 0, false
	}
	if !encodedLevelsAllEqual(defData, numValues, maxDefinitionLevel) {
		return 0, false
	}
	n, ok := firstRepetitionPeriod(repData, numValues)
	if !ok || n <= 0 || numValues%n != 0 {
		return 0, false
	}
	if !verifyRepetitionPeriod(repData, numValues, n) {
		return 0, false
	}
	return n, true
}

// encodedLevelsAllEqual reports whether the encoded level stream src decodes
// to (at least) numValues values which are all equal to value. Values encoded
// beyond numValues are ignored, mirroring the behavior of decodeLevels which
// truncates the decoded levels to numValues.
//
// The stream is assumed to be encoded with a bit width of bits.Len8(value),
// which is how lookupLevelEncoding selects the level encoding.
func encodedLevelsAllEqual(src []byte, numValues int, value byte) bool {
	bitWidth := uint(bits.Len8(value))
	// Byte pattern of a bit-packed group where every value equals `value`.
	// Only computable when values do not straddle byte boundaries.
	var packed byte
	packedOK := 8%bitWidth == 0
	if packedOK {
		for i := uint(0); i < 8; i += bitWidth {
			packed |= value << i
		}
	}

	pos := 0
	i := 0
	for pos < numValues {
		if i >= len(src) {
			return false
		}
		u, k := binary.Uvarint(src[i:])
		if k <= 0 || (u>>1) > maxDetectableRunLength {
			return false
		}
		i += k
		count := int(u >> 1)
		if count == 0 {
			continue
		}
		if (u & 1) != 0 {
			// Bit-packed run of count groups of 8 values.
			if !packedOK {
				return false
			}
			runBytes := count * int(bitWidth)
			if i+runBytes > len(src) {
				return false
			}
			checkValues := min(count*8, numValues-pos)
			checkBits := checkValues * int(bitWidth)
			for j := range checkBits / 8 {
				if src[i+j] != packed {
					return false
				}
			}
			if rem := checkBits % 8; rem != 0 {
				mask := byte(1<<rem) - 1
				if (src[i+checkBits/8]^packed)&mask != 0 {
					return false
				}
			}
			pos += checkValues
			i += runBytes
		} else {
			// RLE run of count values equal to the next byte.
			if i >= len(src) {
				return false
			}
			if src[i] != value {
				return false
			}
			i++
			pos += count
		}
	}
	return true
}

// firstRepetitionPeriod scans the encoded repetition level stream (bit width
// 1) for the positions of the first two zero levels and returns the distance
// between them, which is the length of the first list in the page.
//
// It returns false if the first repetition level is not zero (the page starts
// in the middle of a row) or if the stream is malformed. If the page contains
// a single record, the returned period is numValues.
func firstRepetitionPeriod(src []byte, numValues int) (int, bool) {
	pos := 0
	sawFirstZero := false
	i := 0
	for pos < numValues {
		if i >= len(src) {
			return 0, false
		}
		u, k := binary.Uvarint(src[i:])
		if k <= 0 || (u>>1) > maxDetectableRunLength {
			return 0, false
		}
		i += k
		count := int(u >> 1)
		if count == 0 {
			continue
		}
		if (u & 1) != 0 {
			// Bit-packed run of count bytes holding count*8 levels,
			// least significant bit first.
			if i+count > len(src) {
				return 0, false
			}
			for j := range count {
				if pos+8*j >= numValues {
					break
				}
				b := src[i+j]
				for b != 0xFF {
					z := bits.TrailingZeros8(^b)
					q := pos + 8*j + z
					if q >= numValues {
						break
					}
					if !sawFirstZero {
						if q != 0 {
							return 0, false
						}
						sawFirstZero = true
					} else {
						return q, true
					}
					b |= 1 << z
				}
			}
			pos += count * 8
			i += count
		} else {
			if i >= len(src) {
				return 0, false
			}
			w := src[i]
			i++
			switch w {
			case 0:
				if !sawFirstZero {
					if pos != 0 {
						return 0, false
					}
					sawFirstZero = true
					if count > 1 && numValues > 1 {
						return 1, true
					}
				} else {
					return pos, true
				}
			case 1:
				if !sawFirstZero {
					// The first repetition level is not zero.
					return 0, false
				}
			default:
				return 0, false
			}
			pos += count
		}
	}
	if sawFirstZero {
		// Only one record boundary: the page holds a single list.
		return numValues, true
	}
	return 0, false
}

// verifyRepetitionPeriod reports whether the encoded repetition level stream
// (bit width 1) decodes, for the first numValues values, to the exact
// periodic pattern where level i is 0 when i%n == 0 and 1 otherwise.
//
// RLE runs are verified in O(1); bit-packed runs are compared byte-wise
// against the expected periodic byte pattern.
func verifyRepetitionPeriod(src []byte, numValues, n int) bool {
	// For short periods the whole stream is bit-packed; precompute the
	// expected byte for each starting phase to avoid recomputing it.
	var stampTable [8]byte
	useStamp := n <= 8
	if useStamp {
		for r := range n {
			stampTable[r] = expectedRepByte(r, n)
		}
	}

	pos := 0
	i := 0
	for pos < numValues {
		if i >= len(src) {
			return false
		}
		u, k := binary.Uvarint(src[i:])
		if k <= 0 || (u>>1) > maxDetectableRunLength {
			return false
		}
		i += k
		count := int(u >> 1)
		if count == 0 {
			continue
		}
		if (u & 1) != 0 {
			// Bit-packed run of count bytes holding count*8 levels.
			if i+count > len(src) {
				return false
			}
			numBytes := count
			if maxBytes := (numValues - pos + 7) / 8; numBytes > maxBytes {
				numBytes = maxBytes
			}
			r, step := pos%n, 8%n
			for j := range numBytes {
				var want byte
				if useStamp {
					want = stampTable[r]
					if r += step; r >= n {
						r -= n
					}
				} else {
					want = expectedRepByte(pos+8*j, n)
				}
				if got := src[i+j]; got != want {
					// The last byte may contain padding bits beyond
					// numValues whose content is unspecified.
					if rem := numValues - (pos + 8*j); rem < 8 {
						mask := byte(1<<rem) - 1
						if (got^want)&mask == 0 {
							continue
						}
					}
					return false
				}
			}
			pos += count * 8
			i += count
		} else {
			if i >= len(src) {
				return false
			}
			w := src[i]
			i++
			end := min(pos+count, numValues)
			switch w {
			case 0:
				// Every level in [pos,end) is a record boundary: only
				// allowed for single-element lists, or a single boundary
				// landing on a multiple of n.
				if n != 1 && (pos%n != 0 || end-pos > 1) {
					return false
				}
			case 1:
				// No record boundary may fall within [pos,end).
				next := pos
				if r := pos % n; r != 0 {
					next = pos + (n - r)
				}
				if next < end {
					return false
				}
			default:
				return false
			}
			pos += count
		}
	}
	return true
}

// expectedRepByte returns the byte holding the bit-packed (bit width 1)
// repetition levels for positions pos..pos+7 of the periodic pattern with
// record boundaries at every multiple of n: bit j is 0 when (pos+j)%n == 0.
func expectedRepByte(pos, n int) byte {
	b := byte(0xFF)
	j := 0
	if r := pos % n; r != 0 {
		j = n - r
	}
	for ; j < 8; j += n {
		b &^= 1 << j
	}
	return b
}
