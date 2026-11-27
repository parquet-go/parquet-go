package parquet

import (
	"encoding/binary"
	"fmt"
	"math"
	"slices"

	"github.com/parquet-go/parquet-go/compress"
	"github.com/parquet-go/parquet-go/compress/brotli"
	"github.com/parquet-go/parquet-go/compress/gzip"
	"github.com/parquet-go/parquet-go/compress/lz4"
	"github.com/parquet-go/parquet-go/compress/snappy"
	"github.com/parquet-go/parquet-go/compress/uncompressed"
	"github.com/parquet-go/parquet-go/compress/zstd"
	"github.com/parquet-go/parquet-go/format"
	pierrec "github.com/pierrec/lz4/v4"
)

var (
	// Uncompressed is a parquet compression codec representing uncompressed
	// pages.
	Uncompressed uncompressed.Codec

	// Snappy is the SNAPPY parquet compression codec.
	Snappy snappy.Codec

	// Gzip is the GZIP parquet compression codec.
	Gzip = gzip.Codec{
		Level: gzip.DefaultCompression,
	}

	// Brotli is the BROTLI parquet compression codec.
	Brotli = brotli.Codec{
		Quality: brotli.DefaultQuality,
		LGWin:   brotli.DefaultLGWin,
	}

	// Zstd is the ZSTD parquet compression codec.
	Zstd = zstd.Codec{
		Level: zstd.DefaultLevel,
	}

	// Lz4Raw is the LZ4_RAW parquet compression codec.
	Lz4Raw = lz4.Codec{
		Level: lz4.DefaultLevel,
	}

	// deprecatedLZ4 is the codec of the deprecated LZ4 parquet compression
	// format. It only supports decoding pages: files written before LZ4_RAW
	// existed must keep opening, but the compression library that introduced
	// the codec framed pages with an undocumented scheme, which made it
	// non-interoperable and led to its deprecation. Writers must use LZ4_RAW
	// instead.
	deprecatedLZ4 lz4ReadCodec

	// Table of compression codecs indexed by their code in the parquet format.
	compressionCodecs = [...]compress.Codec{
		format.Uncompressed: &Uncompressed,
		format.Snappy:       &Snappy,
		format.Gzip:         &Gzip,
		format.Brotli:       &Brotli,
		format.Zstd:         &Zstd,
		format.Lz4:          &deprecatedLZ4,
		format.Lz4Raw:       &Lz4Raw,
	}
)

// LookupCompressionCodec returns the compression codec associated with the
// given code.
//
// The function never returns nil. If the encoding is not supported,
// an "unsupported" codec is returned.
func LookupCompressionCodec(codec format.CompressionCodec) compress.Codec {
	if codec >= 0 && int(codec) < len(compressionCodecs) {
		if c := compressionCodecs[codec]; c != nil {
			return c
		}
	}
	return &unsupported{codec}
}

// compressionCodecs returns the codecs set by FileCompressionCodecs for
// the file, or nil when f is nil or has none.
func (f *File) compressionCodecs() []compress.Codec {
	if f == nil || f.config == nil {
		return nil
	}
	return f.config.CompressionCodecs
}

// lookupCompressionCodec returns the codec of the given code from codecs,
// the last one of that code when there are several, or the default codec
// of LookupCompressionCodec when codecs has none.
func lookupCompressionCodec(codecs []compress.Codec, codec format.CompressionCodec) compress.Codec {
	for _, c := range slices.Backward(codecs) {
		if c != nil && c.CompressionCodec() == codec {
			return c
		}
	}
	return LookupCompressionCodec(codec)
}

type unsupported struct {
	codec format.CompressionCodec
}

func (u *unsupported) String() string {
	return "UNSUPPORTED"
}

func (u *unsupported) CompressionCodec() format.CompressionCodec {
	return u.codec
}

func (u *unsupported) Encode(dst, src []byte) ([]byte, error) {
	return dst[:0], u.error()
}

func (u *unsupported) Decode(dst, src []byte) ([]byte, error) {
	return dst[:0], u.error()
}

func (u *unsupported) error() error {
	return fmt.Errorf("unsupported compression codec: %s", u.codec)
}

// lz4ReadCodec decodes the pages of files written with the deprecated LZ4
// compression codec. Two forms exist in the wild: parquet-mr framed the pages
// with the scheme of the Hadoop compression library, other implementations
// wrote raw LZ4 blocks. The codec detects the Hadoop framing and falls back
// to decoding a raw LZ4 block when the bytes do not parse as a Hadoop
// stream. Encoding is not supported, pages must be written with the LZ4_RAW
// codec instead.
type lz4ReadCodec struct{}

func (lz4ReadCodec) String() string {
	return "LZ4"
}

func (lz4ReadCodec) CompressionCodec() format.CompressionCodec {
	return format.Lz4
}

func (lz4ReadCodec) Encode(dst, src []byte) ([]byte, error) {
	return dst[:0], fmt.Errorf("the deprecated LZ4 compression codec only supports reading pages, use LZ4_RAW to write")
}

func (lz4ReadCodec) Decode(dst, src []byte) ([]byte, error) {
	if out, ok := decodeHadoopLz4(dst, src); ok {
		return out, nil
	}
	// Not a Hadoop stream; the pages hold raw LZ4 blocks, which the LZ4_RAW
	// codec decodes.
	return Lz4Raw.Decode(dst, src)
}

// decodeHadoopLz4 decodes src as a stream of the Hadoop compression library:
// a sequence of LZ4 blocks, each prefixed with its uncompressed and compressed
// lengths as big-endian uint32 values. A prefix whose two lengths are both
// zero is an empty frame, including the terminator some writers append.
//
// ok is false when src does not parse as such a stream. The caller then falls
// back to a raw LZ4 block. Raw blocks can carry a prefix that looks like a
// frame with a multi-gigabyte uncompressed length, so a frame is accepted
// only when both lengths are inside the bytes still available:
//
//   - the compressed length fits in the unread input
//   - the uncompressed length fits in the space still free in dst
//
// The page reader passes a dst whose length is the page's uncompressed size,
// which is the same bound Arrow applies to the output buffer. A length that
// does not fit is not a Hadoop frame: this returns false without allocating
// it. When dst has no buffer, the uncompressed length still has to be
// producible by an LZ4 block of the claimed compressed length, and the block
// is decoded with LZ4_RAW, whose growth stops at maxDecodedLen.
func decodeHadoopLz4(dst, src []byte) (out []byte, ok bool) {
	if len(src) == 0 {
		return dst[:0], true
	}

	// len(dst) is the page size when the page reader calls Decode. cap(dst)
	// may be larger because the buffer pool rounds up, and that slack is not
	// part of the page. A caller that passes dst[:0] is offering cap(dst).
	budget := len(dst)
	if budget == 0 {
		budget = cap(dst)
	}
	if budget > 0 {
		out = dst[:budget:budget]
		out = out[:0]
	}

	rest := src
	for len(rest) > 0 {
		if len(rest) < 8 {
			return nil, false
		}
		uncompressed64 := uint64(binary.BigEndian.Uint32(rest[:4]))
		compressed64 := uint64(binary.BigEndian.Uint32(rest[4:8]))
		body := rest[8:]
		if compressed64 > uint64(len(body)) || uncompressed64 > uint64(math.MaxInt) {
			return nil, false
		}
		compressed := int(compressed64)
		uncompressed := int(uncompressed64)
		remaining := -1
		if budget > 0 {
			remaining = budget - len(out)
			if uncompressed > remaining {
				return nil, false
			}
		}
		if !hadoopDecompressedFitsBlock(compressed, uncompressed) {
			return nil, false
		}
		if uncompressed == 0 {
			// Both lengths zero is an empty frame. A positive compressed
			// length still has to decode to zero bytes; otherwise the prefix
			// is not a Hadoop frame.
			if compressed != 0 {
				n, err := pierrec.UncompressBlock(body[:compressed], nil)
				if err != nil || n != 0 {
					return nil, false
				}
			}
			rest = body[compressed:]
			continue
		}
		if budget > 0 {
			// Pin the capacity so a short block cannot write past this frame
			// into the rest of the page buffer.
			tail := out[len(out) : len(out)+uncompressed : len(out)+uncompressed]
			n, err := pierrec.UncompressBlock(body[:compressed], tail)
			if err != nil || n != uncompressed {
				return nil, false
			}
			out = out[:len(out)+uncompressed]
		} else {
			// No page buffer. LZ4_RAW refuses to grow past the most output
			// the block can produce, so a hostile uncompressed length is
			// never allocated.
			block, err := Lz4Raw.Decode(nil, body[:compressed])
			if err != nil || len(block) != uncompressed {
				return nil, false
			}
			out = append(out, block...)
		}
		rest = body[compressed:]
	}
	return out, true
}

// hadoopDecompressedFitsBlock reports whether an LZ4 block of compressed
// bytes can produce uncompressed bytes. One input byte adds at most 255
// output bytes, which is the expansion compress/lz4.maxDecodedLen saturates
// toward. A claimed size above that cannot be a Hadoop frame.
func hadoopDecompressedFitsBlock(compressed, uncompressed int) bool {
	if compressed < 0 || uncompressed < 0 {
		return false
	}
	if compressed > math.MaxInt/255 {
		return true
	}
	return uncompressed <= 255*compressed
}

func isCompressed(c compress.Codec) bool {
	return c != nil && c.CompressionCodec() != format.Uncompressed
}
