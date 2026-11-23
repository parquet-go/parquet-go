// Package rle implements internal helpers for Parquet's hybrid RLE encoding.
package rle

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/parquet-go/bitpack"
)

// Int32Runs walks an RLE/bit-packed stream, with a position inside its current run.
// It does not own the encoded bytes and can be copied to save a position.
type Int32Runs struct {
	src       []byte
	next      int
	remaining int64
	word      int32
	packed    []byte
	index     int64
	width     uint
}

func NewInt32Runs(src []byte, width uint) (Int32Runs, error) {
	if width > 32 {
		return Int32Runs{}, fmt.Errorf("cannot decode INT32 with invalid bit-width=%d", width)
	}
	return Int32Runs{src: src, width: width}, nil
}

// Next loads the next nonempty run; io.EOF denotes a clean end of stream.
func (c *Int32Runs) Next() error {
	for c.next < len(c.src) {
		u, n := binary.Uvarint(c.src[c.next:])
		if n == 0 {
			return fmt.Errorf("decoding run-length block header: %w", io.ErrUnexpectedEOF)
		}
		if n < 0 {
			return fmt.Errorf("overflow after decoding %d/%d bytes of run-length block header", c.next-n, len(c.src))
		}
		c.next += n
		count := u >> 1
		if count == 0 {
			continue
		}
		if count > math.MaxInt32 {
			return fmt.Errorf("decoded run-length block cannot have more than %d values", math.MaxInt32)
		}
		c.index, c.word, c.packed = 0, 0, nil
		if u&1 != 0 {
			size := count * uint64(c.width)
			if size > uint64(len(c.src)-c.next) {
				return fmt.Errorf("decoding bit-packed block of %d values: %w", 8*count, io.ErrUnexpectedEOF)
			}
			end := c.next + int(size)
			if c.width > 0 {
				c.packed = c.src[c.next:end]
			}
			c.next, c.remaining = end, int64(count)*8
		} else {
			end := c.next + bitpack.ByteCount(c.width)
			if end > len(c.src) {
				return fmt.Errorf("decoding run-length block of %d values: %w", count, io.ErrUnexpectedEOF)
			}
			var word [4]byte
			copy(word[:], c.src[c.next:end])
			c.word = int32(binary.LittleEndian.Uint32(word[:]))
			c.next, c.remaining = end, int64(count)
		}
		return nil
	}
	return io.EOF
}

func (c *Int32Runs) Remaining() int64 { return c.remaining }

func (c *Int32Runs) Repeated() (int32, bool) { return c.word, c.packed == nil }

func (c *Int32Runs) Skip(n int64) {
	c.index += n
	c.remaining -= n
}

// Unpack consumes len(dst) values from the current packed run.
func (c *Int32Runs) Unpack(dst []int32) {
	index := c.index
	c.Skip(int64(len(dst)))
	for len(dst) > 0 {
		group, offset := index/8, int(index%8)
		in := c.packed[int(group)*int(c.width):]
		if offset == 0 && cap(in) >= bitpack.PaddingInt32 {
			n := min(len(dst)/8, (cap(in)-bitpack.PaddingInt32)/int(c.width)) * 8
			if n > 0 {
				bitpack.Unpack(dst[:n], in[:cap(in)], c.width)
				dst = dst[n:]
				index += int64(n)
				continue
			}
		}
		// Partial groups and groups without SIMD padding use fixed scratch.
		var packed [32 + bitpack.PaddingInt32]byte
		var ids [8]int32
		copy(packed[:], in[:int(c.width)])
		bitpack.Unpack(ids[:], packed[:], c.width)
		n := copy(dst, ids[offset:])
		dst = dst[n:]
		index += int64(n)
	}
}
