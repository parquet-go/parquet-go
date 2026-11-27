package alp

import (
	"encoding/binary"
	"math"
	"math/bits"

	"github.com/parquet-go/bitpack"
	"github.com/parquet-go/bitpack/unsafecast"
)

func (*Encoding) EncodeFloat(dst []byte, src []float32) ([]byte, error) {
	return encode[float32, int32](dst, src, 4, 10)
}

func (*Encoding) EncodeDouble(dst []byte, src []float64) ([]byte, error) {
	return encode[float64, int64](dst, src, 8, 18)
}

const vectorSize = 1024

func encode[F float32 | float64, I int32 | int64](dst []byte, src []F, size, maxExp int) ([]byte, error) {
	if len(src) > math.MaxInt32 {
		return dst[:0], invalid("element count exceeds int32")
	}
	n := len(src) / vectorSize
	if len(src)%vectorSize != 0 {
		n++
	}
	dst = append(dst[:0], 0, 0, 10)
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(src)))
	dst = append(dst, make([]byte, 4*n)...)
	for i := range n {
		count := min(vectorSize, len(src))
		values := src[:count]
		src = src[count:]
		offset := len(dst) - 7
		if uint64(offset) > math.MaxInt32 {
			return dst[:0], invalid("vector offset exceeds int32")
		}
		binary.LittleEndian.PutUint32(dst[7+4*i:], uint32(offset))
		dst = encodeVector[F, I](dst, values, size, maxExp)
	}
	return dst, nil
}

var decimalPowers = [...]float64{1, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18}
var decimalInverses = [...]float64{1, 1e-1, 1e-2, 1e-3, 1e-4, 1e-5, 1e-6, 1e-7, 1e-8, 1e-9, 1e-10, 1e-11, 1e-12, 1e-13, 1e-14, 1e-15, 1e-16, 1e-17, 1e-18}

// Only pack integers that the decoder will reconstruct exactly. Negative
// zero and non-finite values always use the raw IEEE exception stream.
func scaled[F float32 | float64, I int32 | int64](v F, exp int, limit float64) (I, bool) {
	n := math.Round(float64(v * F(decimalPowers[exp])))
	if !(n >= -limit && n < limit) || (v == 0 && math.Signbit(float64(v))) {
		return 0, false
	}
	i := I(n)
	return i, F(i)*F(decimalInverses[exp]) == v
}

func encodeVector[F float32 | float64, I int32 | int64](dst []byte, src []F, size, maxExp int) []byte {
	limit := math.Ldexp(1, 8*size-1)
	// Estimate packed bits and exception costs on an evenly spaced sample.
	// A zero factor is sufficient for every vector this writer emits.
	exp, best := 0, math.MaxInt
	step := max(1, len(src)/32)
	for e := 0; e <= maxExp; e++ {
		var lo, hi I
		count, exceptions := 0, 0
		for j := 0; j < len(src); j += step {
			v, ok := scaled[F, I](src[j], e, limit)
			if ok {
				if count == exceptions {
					lo, hi = v, v
				}
				lo, hi = min(lo, v), max(hi, v)
			} else {
				exceptions++
			}
			count++
		}
		cost := count*bits.Len64(uint64(hi)-uint64(lo)) + exceptions*(size+2)*8
		if cost < best {
			exp, best = e, cost
		}
	}

	var words [vectorSize]I
	var positions [vectorSize]uint16
	var lo, hi I
	exceptions := 0
	for j, v := range src {
		n, ok := scaled[F, I](v, exp, limit)
		if !ok {
			positions[exceptions] = uint16(j)
			exceptions++
			continue
		}
		if j == exceptions {
			lo, hi = n, n
		}
		lo, hi = min(lo, n), max(hi, n)
		words[j] = n
	}
	width := bits.Len64(uint64(hi) - uint64(lo))
	// With noisy IEEE data a few outlying integers may force full-width
	// deltas as well as raw exceptions. Cap the vector at the all-raw cost.
	if (len(src)*width+7)/8 > (len(src)-exceptions)*(size+2) {
		lo, width, exceptions = 0, 0, len(src)
		for j := range src {
			positions[j] = uint16(j)
		}
	} else if width != 0 {
		for j := range src {
			words[j] -= lo
		}
		for _, j := range positions[:exceptions] {
			words[j] = 0
		}
	}
	dst = append(dst, byte(exp), 0)
	dst = binary.LittleEndian.AppendUint16(dst, uint16(exceptions))
	dst = appendWord(dst, lo, size)
	dst = append(dst, byte(width))
	if width > 0 {
		start := len(dst)
		dst = append(dst, make([]byte, (len(src)*width+7)/8)...)
		bitpack.Pack(dst[start:], words[:len(src)], uint(width))
	}
	raw := unsafecast.Slice[I](src)
	for _, j := range positions[:exceptions] {
		dst = binary.LittleEndian.AppendUint16(dst, j)
	}
	for _, j := range positions[:exceptions] {
		dst = appendWord(dst, raw[j], size)
	}
	return dst
}

func appendWord[I int32 | int64](dst []byte, v I, size int) []byte {
	if size == 4 {
		return binary.LittleEndian.AppendUint32(dst, uint32(v))
	}
	return binary.LittleEndian.AppendUint64(dst, uint64(v))
}
