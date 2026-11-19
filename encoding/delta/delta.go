package delta

import (
	"fmt"
	"math"

	"github.com/parquet-go/bitpack/unsafecast"
	"github.com/parquet-go/parquet-go/internal/memory"
)

type int32Buffer struct {
	values []int32
}

func (buf *int32Buffer) resize(size int) {
	if cap(buf.values) < size {
		buf.values = make([]int32, size, 2*size)
	} else {
		buf.values = buf.values[:size]
	}
}

func (buf *int32Buffer) decode(src []byte) ([]byte, error) {
	values, remain, err := decodeInt32(unsafecast.Slice[byte](buf.values[:0]), src)
	buf.values = unsafecast.Slice[int32](values)
	return remain, err
}

// sumNonNegative returns the sum of values when every value is non-negative
// and the sum is within limit. Otherwise it returns 0. limit is the input
// length for encodings whose decoded payload is a subset of that input, so a
// corrupt length list cannot request a buffer larger than the page.
func sumNonNegative(values []int32, limit int) int {
	if limit < 0 {
		return 0
	}
	var sum int64
	for _, v := range values {
		if v < 0 {
			return 0
		}
		sum += int64(v)
	}
	if sum > int64(limit) || sum > int64(math.MaxInt) {
		return 0
	}
	return int(sum)
}

// reconstructedByteArraySize is the decoded size of a DELTA_BYTE_ARRAY page.
// srcLen is the suffix payload, the bytes left after both length lists.
// Prefix bytes repeat earlier values and may expand the page; a length list
// that is negative, inconsistent, or overflows does not name a buffer size.
func reconstructedByteArraySize(prefix, suffix []int32, srcLen int) int {
	if len(prefix) != len(suffix) || srcLen < 0 {
		return 0
	}
	var total int64
	var suffixTotal int64
	var last int64
	src := int64(srcLen)
	for i := range prefix {
		p := int64(prefix[i])
		n := int64(suffix[i])
		if p < 0 || n < 0 || p > last {
			return 0
		}
		if n > src-suffixTotal {
			return 0
		}
		suffixTotal += n
		total += p + n
		last = p + n
	}
	if total > int64(math.MaxInt) {
		return 0
	}
	return int(total)
}

var (
	int32BufferPool memory.Pool[int32Buffer]
)

func getInt32Buffer() *int32Buffer {
	return int32BufferPool.Get(
		func() *int32Buffer {
			return &int32Buffer{
				values: make([]int32, 0, 1024),
			}
		},
		func(b *int32Buffer) { b.values = b.values[:0] },
	)
}

func putInt32Buffer(b *int32Buffer) {
	int32BufferPool.Put(b)
}

func resizeNoMemclr(buf []byte, size int) []byte {
	if cap(buf) < size {
		return grow(buf, size)
	}
	return buf[:size]
}

func resize(buf []byte, size int) []byte {
	if cap(buf) < size {
		return grow(buf, size)
	}
	if size > len(buf) {
		clear := buf[len(buf):size]
		for i := range clear {
			clear[i] = 0
		}
	}
	return buf[:size]
}

func grow(buf []byte, size int) []byte {
	newCap := max(2*cap(buf), size)
	newBuf := make([]byte, size, newCap)
	copy(newBuf, buf)
	return newBuf
}

func errPrefixAndSuffixLengthMismatch(prefixLength, suffixLength int) error {
	return fmt.Errorf("length of prefix and suffix mismatch: %d != %d", prefixLength, suffixLength)
}

func errInvalidNegativeValueLength(length int) error {
	return fmt.Errorf("invalid negative value length: %d", length)
}

func errInvalidNegativePrefixLength(length int) error {
	return fmt.Errorf("invalid negative prefix length: %d", length)
}

func errValueLengthOutOfBounds(length, maxLength int) error {
	return fmt.Errorf("value length is larger than the input size: %d > %d", length, maxLength)
}

func errPrefixLengthOutOfBounds(length, maxLength int) error {
	return fmt.Errorf("prefix length %d is larger than the last value of size %d", length, maxLength)
}
