package parquet

import (
	"encoding/binary"
	"fmt"
	"slices"

	"github.com/parquet-go/parquet-go/compress"
	"github.com/parquet-go/parquet-go/compress/brotli"
	"github.com/parquet-go/parquet-go/compress/gzip"
	"github.com/parquet-go/parquet-go/compress/lz4"
	"github.com/parquet-go/parquet-go/compress/snappy"
	"github.com/parquet-go/parquet-go/compress/uncompressed"
	"github.com/parquet-go/parquet-go/compress/zstd"
	"github.com/parquet-go/parquet-go/format"
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
// lengths as big-endian 32 bit integers, and optionally ended by an entry
// where both lengths are zero. The ok result is false when src does not parse
// as such a stream, in which case the caller falls back to decoding a raw
// LZ4 block.
func decodeHadoopLz4(dst, src []byte) (out []byte, ok bool) {
	out = dst[:0]
	for pos := 0; pos < len(src); {
		if len(src)-pos < 8 {
			return nil, false
		}
		uncompressed := int(binary.BigEndian.Uint32(src[pos:]))
		compressed := int(binary.BigEndian.Uint32(src[pos+4:]))
		if uncompressed == 0 && compressed == 0 {
			pos += 8
			if pos != len(src) {
				return nil, false
			}
			break // end of stream
		}
		if uncompressed <= 0 || compressed <= 0 || compressed > len(src)-pos-8 {
			return nil, false
		}
		if cap(out)-len(out) < uncompressed {
			grown := make([]byte, len(out), 2*(len(out)+uncompressed))
			copy(grown, out)
			out = grown
		}
		block, err := Lz4Raw.Decode(out[len(out):cap(out)], src[pos+8:pos+8+compressed])
		if err != nil || len(block) != uncompressed {
			return nil, false
		}
		out = out[:len(out)+uncompressed]
		pos += 8 + compressed
	}
	return out, true
}

func isCompressed(c compress.Codec) bool {
	return c != nil && c.CompressionCodec() != format.Uncompressed
}
