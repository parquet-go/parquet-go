package parquet_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"
)

func deprecatedLZ4(t *testing.T) compress.Codec {
	t.Helper()
	c := parquet.LookupCompressionCodec(format.Lz4)
	if c.CompressionCodec() != format.Lz4 || c.String() != "LZ4" {
		t.Fatalf("LookupCompressionCodec(LZ4) = %s (%s)", c, c.CompressionCodec())
	}
	return c
}

func hadoopLZ4Frame(t testing.TB, plain []byte) []byte {
	t.Helper()
	compressed, err := parquet.Lz4Raw.Encode(nil, plain)
	if err != nil {
		t.Fatal(err)
	}
	if len(compressed) == 0 || len(plain) == 0 {
		t.Fatalf("frame fixture is empty (plain %d, compressed %d)", len(plain), len(compressed))
	}
	frame := make([]byte, 8+len(compressed))
	binary.BigEndian.PutUint32(frame[0:4], uint32(len(plain)))
	binary.BigEndian.PutUint32(frame[4:8], uint32(len(compressed)))
	copy(frame[8:], compressed)
	return frame
}

func hadoopLZ4Stream(frames ...[]byte) []byte {
	return bytes.Join(frames, nil)
}

func TestDeprecatedLZ4Decode(t *testing.T) {
	codec := deprecatedLZ4(t)
	plainA := bytes.Repeat([]byte("alpha-"), 50)
	plainB := bytes.Repeat([]byte("bravo-"), 80)
	plainC := bytes.Repeat([]byte("charlie-"), 30)
	frameA := hadoopLZ4Frame(t, plainA)
	frameB := hadoopLZ4Frame(t, plainB)
	frameC := hadoopLZ4Frame(t, plainC)
	many := hadoopLZ4Stream(frameA, frameB, frameC)
	manyPlain := hadoopLZ4Stream(plainA, plainB, plainC)
	terminator := []byte{0, 0, 0, 0, 0, 0, 0, 0}

	raw, err := parquet.Lz4Raw.Encode(nil, plainA)
	if err != nil {
		t.Fatal(err)
	}

	// A second frame that would fit in the original page on its own, but not
	// in the bytes left after the first frame.
	overflow := hadoopLZ4Stream(frameA, frameA)

	tests := []struct {
		name    string
		src     []byte
		dst     func() []byte
		want    []byte
		wantErr bool
	}{
		{
			name: "zero frames",
			src:  nil,
			dst:  func() []byte { return make([]byte, 8) },
			want: []byte{},
		},
		{
			name: "zero frames nil dst",
			src:  []byte{},
			dst:  func() []byte { return nil },
			want: []byte{},
		},
		{
			name: "one empty frame",
			src:  terminator,
			dst:  func() []byte { return make([]byte, 8) },
			want: []byte{},
		},
		{
			name: "many empty frames",
			src:  bytes.Repeat(terminator, 3),
			dst:  func() []byte { return make([]byte, 8) },
			want: []byte{},
		},
		{
			name: "one frame",
			src:  frameA,
			dst:  func() []byte { return make([]byte, len(plainA)) },
			want: plainA,
		},
		{
			name: "one frame nil dst",
			src:  frameA,
			dst:  func() []byte { return nil },
			want: plainA,
		},
		{
			name: "one frame dst cap only",
			src:  frameA,
			dst:  func() []byte { return make([]byte, 0, len(plainA)) },
			want: plainA,
		},
		{
			name: "many frames",
			src:  many,
			dst:  func() []byte { return make([]byte, len(manyPlain)) },
			want: manyPlain,
		},
		{
			name: "many frames nil dst",
			src:  many,
			dst:  func() []byte { return nil },
			want: manyPlain,
		},
		{
			name: "many frames and terminator",
			src:  append(append([]byte{}, many...), terminator...),
			dst:  func() []byte { return make([]byte, len(manyPlain)) },
			want: manyPlain,
		},
		{
			name: "raw fallback",
			src:  raw,
			dst:  func() []byte { return make([]byte, len(plainA)) },
			want: plainA,
		},
		{
			name: "raw fallback nil dst",
			src:  raw,
			dst:  func() []byte { return nil },
			want: plainA,
		},
		{
			name:    "truncated header",
			src:     []byte{0, 0, 0, 1, 0, 0, 0},
			dst:     func() []byte { return make([]byte, 16) },
			wantErr: true,
		},
		{
			name: "zero compressed length",
			src: func() []byte {
				b := make([]byte, 8)
				binary.BigEndian.PutUint32(b[0:4], 5)
				return b
			}(),
			dst:     func() []byte { return make([]byte, 5) },
			wantErr: true,
		},
		{
			name: "truncated payload",
			src: func() []byte {
				b := make([]byte, 12)
				binary.BigEndian.PutUint32(b[0:4], 10)
				binary.BigEndian.PutUint32(b[4:8], 100) // claims 100, 4 bytes follow
				return b
			}(),
			dst:     func() []byte { return make([]byte, 10) },
			wantErr: true,
		},
		{
			name: "corrupt block",
			src: func() []byte {
				b := append([]byte{}, frameA...)
				for i := 8; i < len(b); i++ {
					b[i] = 0xff
				}
				return b
			}(),
			dst:     func() []byte { return make([]byte, len(plainA)) },
			wantErr: true,
		},
		{
			name:    "second frame exceeds remaining page",
			src:     overflow,
			dst:     func() []byte { return make([]byte, len(plainA)) },
			wantErr: true,
		},
		{
			name: "decompressed size exceeds page but fits pool slack",
			src:  frameA,
			dst: func() []byte {
				// len is the page size. cap is the rounded-up pool buffer.
				// The frame is one byte larger than the page and must not
				// be accepted just because the slack could hold it.
				if len(plainA) < 2 {
					t.Fatal("plain")
				}
				return make([]byte, len(plainA)-1, len(plainA)+64)
			},
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			out, err := codec.Decode(test.dst(), test.src)
			if test.wantErr {
				if err == nil {
					t.Fatal("expected an error")
				}
				if out != nil {
					t.Fatalf("error returned %d bytes", len(out))
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(out, test.want) {
				t.Fatalf("got %d bytes, want %d", len(out), len(test.want))
			}
			// An empty Hadoop decode must not be a raw block that carries
			// bytes. Otherwise the Hadoop path would hide that payload.
			if len(test.want) == 0 {
				rawOut, rawErr := parquet.Lz4Raw.Decode(nil, test.src)
				if rawErr == nil && !bytes.Equal(rawOut, test.want) {
					t.Fatalf("raw LZ4 decodes this input to %d bytes", len(rawOut))
				}
			}
		})
	}
}

func TestDeprecatedLZ4DecodeInPlace(t *testing.T) {
	codec := deprecatedLZ4(t)
	plain := bytes.Repeat([]byte("in-place-frame-"), 64)
	src := hadoopLZ4Frame(t, plain)
	dst := make([]byte, len(plain), len(plain)+32)

	allocs := testing.AllocsPerRun(20, func() {
		out, err := codec.Decode(dst, src)
		if err != nil {
			t.Fatal(err)
		}
		if len(out) != len(plain) || &out[0] != &dst[0] {
			t.Fatalf("len %d reallocated %v", len(out), &out[0] != &dst[0])
		}
	})
	if allocs != 0 {
		t.Fatalf("in-place decode allocated %g times", allocs)
	}
	if !bytes.Equal(dst[:len(plain)], plain) {
		t.Fatal("dst does not hold the decoded page")
	}
}

func TestDeprecatedLZ4HostileClaimDoesNotAllocate(t *testing.T) {
	codec := deprecatedLZ4(t)

	// The uncompressed length is attacker-controlled. It must not be used as
	// an allocation size, with or without a page buffer.
	tests := []struct {
		name    string
		claim   uint32
		compLen int
		dst     func() []byte
	}{
		{
			name:    "no page buffer",
			claim:   32 << 20,
			compLen: 4,
			dst:     func() []byte { return nil },
		},
		{
			name:    "claim larger than the page",
			claim:   32 << 20,
			compLen: 4,
			dst:     func() []byte { return make([]byte, 128) },
		},
		{
			// The length pair called out in review of the original change:
			// a real LZ4_RAW page reads as uncompressed 4030800896 and
			// compressed 25399. Here the compressed length does not fit in
			// the input, so the frame is truncated as well as oversized.
			name:    "review lengths truncated",
			claim:   4030800896,
			compLen: 25399,
			dst:     func() []byte { return make([]byte, 400000) },
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Keep the payload shorter than the claimed compressed length so the frame is truncated.
			payload := min(test.compLen, 64)
			src := make([]byte, 8+payload)
			binary.BigEndian.PutUint32(src[0:4], test.claim)
			binary.BigEndian.PutUint32(src[4:8], uint32(test.compLen))
			src[8] = 0xff

			var out []byte
			var err error
			grew := allocatedDuring(func() {
				out, err = codec.Decode(test.dst(), src)
			})
			if err == nil || out != nil {
				t.Fatalf("Decode err=%v len=%d, want an error and no buffer", err, len(out))
			}
			if grew >= uint64(test.claim) {
				t.Fatalf("allocated %d bytes for a claimed uncompressed size of %d", grew, test.claim)
			}
		})
	}
}

func TestDeprecatedLZ4RawPageWithHostilePrefix(t *testing.T) {
	codec := deprecatedLZ4(t)
	body, uncompressed := compressedPage(t, "testdata/lz4_raw_compressed_larger.parquet", 0)
	claim := binary.BigEndian.Uint32(body[:4])
	compressed := binary.BigEndian.Uint32(body[4:8])
	if claim != 4030800896 || compressed != 25399 {
		t.Fatalf("prefix uncompressed %d compressed %d, want 4030800896 and 25399", claim, compressed)
	}
	if uint64(claim) <= uint64(uncompressed) {
		t.Fatalf("claim %d fits in the page %d", claim, uncompressed)
	}

	want, err := parquet.Lz4Raw.Decode(make([]byte, uncompressed), body)
	if err != nil {
		t.Fatal(err)
	}
	dst := make([]byte, uncompressed)
	var out []byte
	grew := allocatedDuring(func() {
		out, err = codec.Decode(dst, body)
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out, want) {
		t.Fatalf("fallback decoded %d bytes, raw decoded %d", len(out), len(want))
	}
	if &out[0] != &dst[0] {
		t.Fatal("fallback reallocated a dst of the page size")
	}
	if grew >= uint64(claim) {
		t.Fatalf("allocated %d bytes for a claimed uncompressed size of %d", grew, claim)
	}
}

func TestDeprecatedLZ4ConcurrentDecode(t *testing.T) {
	codec := deprecatedLZ4(t)
	plain := bytes.Repeat([]byte("concurrent-lz4-"), 64)
	src := hadoopLZ4Frame(t, plain)

	var wg sync.WaitGroup
	errc := make(chan error, 8)
	for range 8 {
		wg.Go(func() {
			out, err := codec.Decode(make([]byte, len(plain)), src)
			if err != nil {
				errc <- err
				return
			}
			if !bytes.Equal(out, plain) {
				errc <- fmt.Errorf("decoded %d bytes, want %d", len(out), len(plain))
			}
		})
	}
	wg.Wait()
	close(errc)
	for err := range errc {
		t.Error(err)
	}
}

func TestDeprecatedLZ4EncodeUnsupported(t *testing.T) {
	codec := deprecatedLZ4(t)
	out, err := codec.Encode([]byte("keep"), []byte("plain"))
	if err == nil {
		t.Fatal("Encode succeeded")
	}
	if !strings.Contains(err.Error(), "LZ4_RAW") {
		t.Fatalf("Encode error %q, want it to name LZ4_RAW", err)
	}
	if len(out) != 0 {
		t.Fatalf("Encode wrote %d bytes", len(out))
	}

	type row struct {
		N int64 `parquet:"n"`
	}
	buf := new(bytes.Buffer)
	w := parquet.NewGenericWriter[row](buf, parquet.Compression(codec))
	if _, err := w.Write([]row{{1}}); err != nil {
		if !strings.Contains(err.Error(), "LZ4_RAW") {
			t.Fatal(err)
		}
		return
	}
	if err := w.Close(); err == nil || !strings.Contains(err.Error(), "LZ4_RAW") {
		t.Fatalf("Close err=%v, want the codec to refuse to write", err)
	}
}

// https://github.com/apache/parquet-testing/blob/master/data/hadoop_lz4_compressed_larger.parquet
// The page is several Hadoop frames. The rows match lz4_raw_compressed_larger.parquet.
func TestReadDeprecatedLZ4LargerFile(t *testing.T) {
	type row struct {
		A string `parquet:"a"`
	}
	want, err := parquet.ReadFile[row]("testdata/lz4_raw_compressed_larger.parquet")
	if err != nil {
		t.Fatal(err)
	}
	if len(want) == 0 {
		t.Fatal("no rows in the LZ4_RAW reference file")
	}
	got, err := parquet.ReadFile[row]("testdata/hadoop_lz4_compressed_larger.parquet")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("got %d rows, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("row %d: got %q, want %q", i, got[i].A, want[i].A)
		}
	}

	// The page is several Hadoop frames, not one frame the size of the page.
	body, uncompressed := compressedPage(t, "testdata/hadoop_lz4_compressed_larger.parquet", 0)
	first := binary.BigEndian.Uint32(body[:4])
	if uint64(first) == 0 || uint64(first) >= uint64(uncompressed) {
		t.Fatalf("first frame uncompressed %d, page %d; want several frames", first, uncompressed)
	}
	codec := deprecatedLZ4(t)
	out, err := codec.Decode(make([]byte, uncompressed), body)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != uncompressed {
		t.Fatalf("decoded %d bytes, page is %d", len(out), uncompressed)
	}
}

func BenchmarkDeprecatedLZ4Decode(b *testing.B) {
	codec := parquet.LookupCompressionCodec(format.Lz4)
	plain := bytes.Repeat([]byte("0123456789abcdef"), 8<<10)
	frame := hadoopLZ4Frame(b, plain)
	src := hadoopLZ4Stream(frame, frame, frame)
	dst := make([]byte, 3*len(plain))
	b.SetBytes(int64(len(dst)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := codec.Decode(dst, src)
		if err != nil {
			b.Fatal(err)
		}
		if len(out) != len(dst) {
			b.Fatalf("len %d", len(out))
		}
	}
}

func allocatedDuring(fn func()) uint64 {
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	fn()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

type countingReader struct {
	b []byte
	n int
}

func (r *countingReader) Read(p []byte) (int, error) {
	if r.n >= len(r.b) {
		return 0, io.EOF
	}
	n := copy(p, r.b[r.n:])
	r.n += n
	return n, nil
}

func compressedPage(t *testing.T, name string, pageIndex int) (body []byte, uncompressed int) {
	t.Helper()
	b, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	f, err := parquet.OpenFile(bytes.NewReader(b), int64(len(b)))
	if err != nil {
		t.Fatal(err)
	}
	md := f.Metadata().RowGroups[0].Columns[0].MetaData
	pos := int(md.DictionaryPageOffset)
	if pos == 0 {
		pos = int(md.DataPageOffset)
	}
	end := pos + int(md.TotalCompressedSize)
	for i := 0; pos < end; i++ {
		cr := &countingReader{b: b[pos:]}
		header := new(format.PageHeader)
		if err := thrift.NewDecoder(new(thrift.CompactProtocol).NewReader(cr)).Decode(header); err != nil {
			t.Fatal(err)
		}
		start := pos + cr.n
		stop := start + int(header.CompressedPageSize)
		if stop > len(b) {
			t.Fatalf("page %d runs past the file", i)
		}
		if i == pageIndex {
			return bytes.Clone(b[start:stop]), int(header.UncompressedPageSize)
		}
		pos = stop
	}
	t.Fatalf("file %s has no page %d", name, pageIndex)
	return nil, 0
}
