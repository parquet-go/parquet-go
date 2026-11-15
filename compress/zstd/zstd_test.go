package zstd_test

import (
	"bytes"
	"math/rand"
	"testing"

	kzstd "github.com/klauspost/compress/zstd"
	"github.com/parquet-go/parquet-go/compress/zstd"
)

// windowFixture returns 32 KiB of data that repeats every 4 KiB, so the
// repetitions can only be matched with a window of at least 4 KiB.
func windowFixture() []byte {
	block := make([]byte, 4<<10)
	rand.New(rand.NewSource(0)).Read(block)
	return bytes.Repeat(block, 8)
}

func TestWindowSizeIsApplied(t *testing.T) {
	src := windowFixture()

	def, err := new(zstd.Codec).Encode(nil, src)
	if err != nil {
		t.Fatal(err)
	}
	small, err := (&zstd.Codec{WindowSize: 1 << 10}).Encode(nil, src)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(def, small) {
		t.Fatal("WindowSize had no effect on the encoded output")
	}

	var h kzstd.Header
	if err := h.Decode(small); err != nil {
		t.Fatal(err)
	}
	if h.WindowSize != 1<<10 {
		t.Fatalf("frame window size = %d, want %d", h.WindowSize, 1<<10)
	}

	out, err := new(zstd.Codec).Decode(nil, small)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out, src) {
		t.Fatal("decoded data differs from the input")
	}
}

func TestWindowSizeZeroUnchanged(t *testing.T) {
	src := windowFixture()
	levels := []zstd.Level{
		zstd.SpeedFastest,
		zstd.SpeedDefault,
		zstd.SpeedBetterCompression,
		zstd.SpeedBestCompression,
	}

	for _, level := range levels {
		t.Run(level.String(), func(t *testing.T) {
			ref, err := kzstd.NewWriter(nil,
				kzstd.WithEncoderConcurrency(zstd.DefaultConcurrency),
				kzstd.WithEncoderLevel(level),
				kzstd.WithZeroFrames(true),
				kzstd.WithEncoderCRC(false),
			)
			if err != nil {
				t.Fatal(err)
			}
			defer ref.Close()
			want := ref.EncodeAll(src, nil)

			got, err := (&zstd.Codec{Level: level}).Encode(nil, src)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, want) {
				t.Fatal("output differs from an encoder created without WithWindowSize")
			}
		})
	}
}

func TestWindowSizeInvalidPanics(t *testing.T) {
	for _, size := range []int{-1, 1 << 9, 3 << 10, 1 << 30} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("WindowSize %d: expected panic", size)
				}
			}()
			_, _ = (&zstd.Codec{WindowSize: size}).Encode(nil, []byte("x"))
		}()
	}
}
