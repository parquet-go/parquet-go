// Package alp implements reading Adaptive Lossless floating-Point pages.
package alp

import (
	"encoding/binary"
	"fmt"
	"math"
	"slices"

	"github.com/parquet-go/bitpack"
	"github.com/parquet-go/bitpack/unsafecast"
	"github.com/parquet-go/parquet-go/encoding"
	"github.com/parquet-go/parquet-go/format"
)

// Encoding decodes ALP values as specified in apache/parquet-format's AlpEncoding.md.
type Encoding struct{ encoding.NotSupported }

func (*Encoding) String() string            { return "ALP" }
func (*Encoding) Encoding() format.Encoding { return format.ALP }

func (*Encoding) DecodeFloat(dst []float32, src []byte) ([]float32, error) {
	return decode[float32, int32](dst, src, 4, 10)
}

func (*Encoding) DecodeDouble(dst []float64, src []byte) ([]float64, error) {
	return decode[float64, int64](dst, src, 8, 18)
}

func invalid(reason string) error {
	return fmt.Errorf("ALP: %s: %w", reason, encoding.ErrInvalidArgument)
}

func decode[F float32 | float64, I int32 | int64](dst []F, src []byte, wordSize, maxExponent int) ([]F, error) {
	dst = dst[:0]
	if len(src) < 7 {
		return dst, invalid("short page header")
	}
	if src[0] != 0 || src[1] != 0 {
		return dst, fmt.Errorf("ALP: mode %d, integer encoding %d: %w", src[0], src[1], encoding.ErrNotSupported)
	}
	if src[2] < 3 || src[2] > 15 {
		return dst, invalid("invalid vector size")
	}
	count := int(int32(binary.LittleEndian.Uint32(src[3:])))
	if count < 0 {
		return dst, invalid("negative element count")
	}
	if count > math.MaxInt/wordSize {
		return dst, invalid("decoded page size overflows int")
	}
	body := src[7:len(src):len(src)]
	if count == 0 {
		if len(body) != 0 {
			return dst, invalid("data after empty page")
		}
		return dst, nil
	}
	vectorSize := 1 << src[2]
	vectors := (count-1)/vectorSize + 1
	if vectors > len(body)/4 {
		return dst, invalid("short offsets table")
	}
	start := 4 * vectors
	if uint64(binary.LittleEndian.Uint32(body)) != uint64(start) {
		return dst, invalid("invalid first vector offset")
	}
	for i := range vectors {
		end := len(body)
		if i+1 < vectors {
			offset := binary.LittleEndian.Uint32(body[4*(i+1):])
			if uint64(offset) > uint64(len(body)) || uint64(offset) < uint64(start) {
				return dst, invalid("invalid vector offset")
			}
			end = int(offset)
		}
		n := min(count, vectorSize)
		var err error
		dst, err = decodeVector[F, I](dst, body[start:end], n, wordSize, maxExponent)
		if err != nil {
			return dst, fmt.Errorf("vector %d: %w", i, err)
		}
		count -= n
		start = end
	}
	return dst, nil
}

func word[I int32 | int64](buf []byte, size int) I {
	if size == 4 {
		return I(binary.LittleEndian.Uint32(buf))
	}
	return I(binary.LittleEndian.Uint64(buf))
}

func decodeVector[F float32 | float64, I int32 | int64](dst []F, src []byte, count, wordSize, maxExponent int) ([]F, error) {
	headerSize := 5 + wordSize
	if len(src) < headerSize {
		return dst, invalid("short vector header")
	}
	exponent, factor := int(src[0]), int(src[1])
	if exponent > maxExponent || factor > exponent {
		return dst, invalid("invalid decimal scale")
	}
	exceptions := int(binary.LittleEndian.Uint16(src[2:]))
	frame := word[I](src[4:], wordSize)
	width := int(src[headerSize-1])
	if width > 8*wordSize || exceptions > count {
		return dst, invalid("invalid bit width or exception count")
	}
	packedSize := (count*width + 7) / 8
	if len(src)-headerSize != packedSize+exceptions*(2+wordSize) {
		return dst, invalid("inconsistent vector size")
	}
	packed := src[headerSize : headerSize+packedSize]
	positions := src[headerSize+packedSize:][:2*exceptions]
	raw := src[headerSize+packedSize+len(positions):]

	dst = slices.Grow(dst, count)
	values := dst[len(dst) : len(dst)+count]
	words := unsafecast.Slice[I](values)
	// Use separate multiplications in the destination precision, as required by ALP.
	powers := [...]F{1, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18}
	inverses := [...]F{1, 1e-1, 1e-2, 1e-3, 1e-4, 1e-5, 1e-6, 1e-7, 1e-8, 1e-9, 1e-10, 1e-11, 1e-12, 1e-13, 1e-14, 1e-15, 1e-16, 1e-17, 1e-18}
	mul, inv := powers[factor], inverses[exponent]
	if width == 0 {
		value := F(F(frame)*mul) * inv
		for i := range values {
			values[i] = value
		}
	} else {
		padding := bitpack.PaddingInt32
		if wordSize == 8 {
			padding = bitpack.PaddingInt64
		}
		unpack(words, packed, width, padding)
		for i, delta := range words {
			// Addition wraps at the full int32 or int64 width before conversion to float.
			values[i] = F(F(frame+delta)*mul) * inv
		}
	}
	for i := range exceptions {
		idx := int(binary.LittleEndian.Uint16(positions[2*i:]))
		if idx >= count {
			return dst, invalid("exception position outside vector")
		}
		// Copy IEEE bits directly so signed zero and NaN payloads survive.
		words[idx] = word[I](raw[i*wordSize:], wordSize)
	}
	return dst[:len(dst)+count], nil
}

// unpack supplies the lookahead bytes required by bitpack.Unpack. At the end
// of a page, copy only the tail into a padded buffer. Eight values occupy
// exactly width bytes, so the split always falls on a byte boundary.
func unpack[I int32 | int64](dst []I, src []byte, width, padding int) {
	if cap(src)-len(src) >= padding {
		bitpack.Unpack(dst, src[:len(src)+padding], uint(width))
		return
	}
	groups := max(0, len(src)-padding) / width
	if groups > 0 {
		bitpack.Unpack(dst[:8*groups], src, uint(width))
	}
	// The tail has fewer than padding + width bytes, with width at most 64.
	var tail [2*bitpack.PaddingInt64 + 64]byte
	copy(tail[:], src[groups*width:])
	bitpack.Unpack(dst[8*groups:], tail[:], uint(width))
}
