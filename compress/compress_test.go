package compress_test

import (
	"bytes"
	_ "embed"
	"fmt"
	"io"
	"testing"

	"github.com/parquet-go/parquet-go/compress"
	"github.com/parquet-go/parquet-go/compress/brotli"
	"github.com/parquet-go/parquet-go/compress/gzip"
	"github.com/parquet-go/parquet-go/compress/lz4"
	"github.com/parquet-go/parquet-go/compress/snappy"
	"github.com/parquet-go/parquet-go/compress/uncompressed"
	"github.com/parquet-go/parquet-go/compress/zstd"
)

var tests = [...]struct {
	scenario string
	codec    compress.Codec
}{
	{
		scenario: "uncompressed",
		codec:    new(uncompressed.Codec),
	},

	{
		scenario: "snappy",
		codec:    new(snappy.Codec),
	},

	{
		scenario: "gzip",
		codec:    new(gzip.Codec),
	},

	{
		scenario: "brotli",
		codec:    new(brotli.Codec),
	},

	{
		scenario: "zstd",
		codec:    new(zstd.Codec),
	},

	{
		scenario: "lz4-fastest",
		codec:    &lz4.Codec{Level: lz4.Fastest},
	},
	{
		scenario: "lz4-fast",
		codec:    &lz4.Codec{Level: lz4.Fast},
	},
	{
		scenario: "lz4-l1",
		codec:    &lz4.Codec{Level: lz4.Level1},
	},
	{
		scenario: "lz4-l5",
		codec:    &lz4.Codec{Level: lz4.Level5},
	},
	{
		scenario: "lz4-l9",
		codec:    &lz4.Codec{Level: lz4.Level9},
	},
}

var (
	testdata = bytes.Repeat([]byte("1234567890qwertyuiopasdfghjklzxcvbnm"), 10e3)
	//go:embed testdata/e.txt
	testdataE []byte
	//go:embed testdata/gettysburg.txt
	testdataGettysburg []byte
	//go:embed testdata/html.txt
	testdataHTML []byte
	//go:embed testdata/Mark.Twain-Tom.Sawyer.txt
	testdataTomSawyer []byte
	//go:embed testdata/pi.txt
	testdataPi []byte
	//go:embed testdata/pngdata.bin
	testdataPNGData []byte
)

func TestCompressionCodec(t *testing.T) {
	buffer := make([]byte, 0, len(testdata))
	output := make([]byte, 0, len(testdata))

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			const N = 10
			// Run the test multiple times to exercise codecs that maintain
			// state across compression/decompression.
			for i := range N {
				var err error

				buffer, err = test.codec.Encode(buffer[:0], testdata)
				if err != nil {
					t.Fatal(err)
				}

				output, err = test.codec.Decode(output[:0], buffer)
				if err != nil {
					t.Fatal(err)
				}

				if !bytes.Equal(testdata, output) {
					t.Errorf("content mismatch after compressing and decompressing (attempt %d/%d)", i+1, N)
				}
			}
		})
	}
}

func BenchmarkEncode(b *testing.B) {
	buffer := make([]byte, 0, len(testdata))

	for testdataName, testdataBytes := range map[string][]byte{
		"e":          testdataE,
		"gettysburg": testdataGettysburg,
		"html":       testdataHTML,
		"tom-sawyer": testdataTomSawyer,
		"pi":         testdataPi,
		"png":        testdataPNGData,
	} {
		for _, test := range tests {
			testName := fmt.Sprintf("%s-%s", test.scenario, testdataName)

			buffer, _ = test.codec.Encode(buffer[:0], testdataBytes)
			b.Logf("%s | Compression ratio: %.2f%%", testName, float64(len(buffer))/float64(len(testdataBytes))*100)

			b.Run(testName, func(b *testing.B) {
				b.SetBytes(int64(len(testdataBytes)))
				benchmarkZeroAllocsPerRun(b, func() {
					buffer, _ = test.codec.Encode(buffer[:0], testdataBytes)
				})
			})
		}
	}
}

func BenchmarkDecode(b *testing.B) {
	buffer := make([]byte, 0, len(testdata))
	output := make([]byte, 0, len(testdata))

	for _, test := range tests {
		b.Run(test.scenario, func(b *testing.B) {
			buffer, _ = test.codec.Encode(buffer[:0], testdata)
			b.SetBytes(int64(len(testdata)))
			benchmarkZeroAllocsPerRun(b, func() {
				output, _ = test.codec.Encode(output[:0], buffer)
			})
		})
	}
}

type simpleReader struct{ io.Reader }

func (s *simpleReader) Close() error            { return nil }
func (s *simpleReader) Reset(r io.Reader) error { s.Reader = r; return nil }

type simpleWriter struct{ io.Writer }

func (s *simpleWriter) Close() error      { return nil }
func (s *simpleWriter) Reset(w io.Writer) { s.Writer = w }

func BenchmarkCompressor(b *testing.B) {
	compressor := compress.Compressor{}
	src := make([]byte, 1000)
	dst := make([]byte, 1000)

	benchmarkZeroAllocsPerRun(b, func() {
		dst, _ = compressor.Encode(dst, src, func(w io.Writer) (compress.Writer, error) {
			return &simpleWriter{Writer: w}, nil
		})
	})
}

func BenchmarkDecompressor(b *testing.B) {
	decompressor := compress.Decompressor{}
	src := make([]byte, 1000)
	dst := make([]byte, 1000)

	benchmarkZeroAllocsPerRun(b, func() {
		dst, _ = decompressor.Decode(dst, src, func(r io.Reader) (compress.Reader, error) {
			return &simpleReader{Reader: r}, nil
		})
	})
}

func benchmarkZeroAllocsPerRun(b *testing.B, f func()) {
	if allocs := testing.AllocsPerRun(b.N, f); allocs != 0 && !testing.Short() {
		b.Errorf("too many memory allocations: %g > 0", allocs)
	}
}
