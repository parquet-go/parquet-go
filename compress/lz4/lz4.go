// Package lz4 implements the LZ4_RAW parquet compression codec.
package lz4

import (
	"math"

	"github.com/parquet-go/parquet-go/format"
	"github.com/pierrec/lz4/v4"
)

type Level = lz4.CompressionLevel

const (
	Fastest = lz4.CompressionLevel(99)
	Fast    = lz4.Fast
	Level1  = lz4.Level1
	Level2  = lz4.Level2
	Level3  = lz4.Level3
	Level4  = lz4.Level4
	Level5  = lz4.Level5
	Level6  = lz4.Level6
	Level7  = lz4.Level7
	Level8  = lz4.Level8
	Level9  = lz4.Level9
)

const (
	DefaultLevel = Fast
)

type Codec struct {
	Level Level
}

func (c *Codec) String() string {
	return "LZ4_RAW"
}

func (c *Codec) CompressionCodec() format.CompressionCodec {
	return format.Lz4Raw
}

func (c *Codec) Encode(dst, src []byte) ([]byte, error) {
	dst = reserveAtLeast(dst, lz4.CompressBlockBound(len(src)))

	var (
		n   int
		err error
	)
	if c.Level == Fastest {
		n, err = lz4.CompressBlock(src, dst, nil)
	} else {
		n, err = lz4.CompressBlockHC(src, dst, c.Level, nil, nil)
	}
	return dst[:n], err
}

func (c *Codec) Decode(dst, src []byte) ([]byte, error) {
	// A zero-length buffer is not an LZ4 block. The empty block is one
	// 0x00 token. pierrec/lz4 reports success for a zero-length source,
	// which would never reach the bound below.
	if len(src) == 0 {
		return nil, lz4.ErrInvalidSourceShortBuffer
	}
	// Use the capacity of dst if any, otherwise guess a 3x compression ratio.
	if cap(dst) == 0 {
		dst = make([]byte, 3*len(src))
	}
	dst = dst[:cap(dst)]

	for {
		n, err := lz4.UncompressBlock(src, dst)
		if err == nil {
			return dst[:n], nil
		}
		// UncompressBlock returns one error both when dst is short and when
		// the block is corrupt. Grow only up to the most output that block
		// could produce.
		limit := maxDecodedLen(len(src))
		if len(dst) >= limit {
			return nil, err
		}
		next := len(dst) * 2
		if next > limit || next <= len(dst) {
			next = limit
		}
		dst = make([]byte, next)
	}
}

// maxDecodedLen is the most output bytes pierrec/lz4 can produce from an
// input of n bytes.
//
// A match length lives in the token's low nibble and, when that nibble is
// 15, in extra bytes. An extra byte of 255 adds 255 and continues, so one
// input byte adds at most 255 output bytes. With no dictionary the match
// offset has to land on a byte already decoded, which costs one literal,
// plus a token and a two-byte offset. The longest block this decoder accepts
// is a single sequence:
//
//	token 0x1F, one literal, offset 1, then bytes of 0xFF and a final byte 254
//
// For n >= 4 that sequence is n bytes long and decodes to 255*n-1001 bytes.
// At n == 4 there is no extra length byte: one literal and a match of 18 is
// 19 bytes, which is the same formula. Inputs shorter than 4 bytes cannot
// hold a match and produce at most n-1 bytes.
//
// The LZ4 block spec lets a decoder reject blocks that do not end in five
// literals. pierrec/lz4 does not, and parquet has to decode the blocks it
// accepts.
//
// https://github.com/lz4/lz4/blob/dev/doc/lz4_Block_format.md
func maxDecodedLen(n int) int {
	if n < 4 {
		if n < 2 {
			return 0
		}
		return n - 1
	}
	// 255*n-1001, saturated so the multiplication cannot overflow int.
	if n > math.MaxInt/255 {
		return math.MaxInt
	}
	return 255*n - 1001
}

func reserveAtLeast(b []byte, n int) []byte {
	if cap(b) < n {
		b = make([]byte, n)
	} else {
		b = b[:cap(b)]
	}
	return b
}
