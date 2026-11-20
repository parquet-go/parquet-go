package alp_test

import (
	"encoding/binary"
	"errors"
	"math"
	"slices"
	"testing"

	"github.com/parquet-go/parquet-go/encoding"
	"github.com/parquet-go/parquet-go/encoding/alp"
)

func TestDecodeDouble(t *testing.T) {
	const nanBits = 0x7ff00000000000a5
	// One 4-value vector. Packed 2-bit deltas = [0, 1, 2, 3], frame = -2.
	page := []byte{0, 0, 3, 4, 0, 0, 0, 4, 0, 0, 0, 2, 1, 2, 0}
	page = binary.LittleEndian.AppendUint64(page, math.MaxUint64-1)
	page = append(page, 2, 0xe4, 1, 0, 3, 0)
	page = binary.LittleEndian.AppendUint64(page, nanBits)
	page = binary.LittleEndian.AppendUint64(page, 1<<63)
	got, err := new(alp.Encoding).DecodeDouble(make([]float64, 2, 4), page)
	if err != nil {
		t.Fatal(err)
	}
	want := []uint64{math.Float64bits(-0.2), nanBits, 0, 1 << 63}
	if len(got) != len(want) {
		t.Fatalf("got %d values, want %d", len(got), len(want))
	}
	for i := range got {
		if math.Float64bits(got[i]) != want[i] {
			t.Errorf("value %d: got %#x, want %#x", i, math.Float64bits(got[i]), want[i])
		}
	}
}

func TestDecodeFullWidthFloat(t *testing.T) {
	page := []byte{0, 0, 3, 2, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0}
	page = binary.LittleEndian.AppendUint32(page, 1<<31)
	page = append(page, 32)
	page = binary.LittleEndian.AppendUint32(page, 0)
	page = binary.LittleEndian.AppendUint32(page, math.MaxUint32)
	got, err := new(alp.Encoding).DecodeFloat(nil, page[:len(page):len(page)])
	if err != nil || !slices.Equal(got, []float32{math.MinInt32, float32(math.MaxInt32)}) {
		t.Fatalf("got %v, %v", got, err)
	}
}

func TestALPPageLayout(t *testing.T) {
	e := new(alp.Encoding)
	empty := []byte{0, 0, 10, 0, 0, 0, 0}
	if result, err := e.DecodeFloat([]float32{42}, empty); err != nil || len(result) != 0 {
		t.Fatalf("empty page: got %v, %v", result, err)
	}
	for _, tc := range []struct {
		name string
		page []byte
		want error
	}{
		{"short header", empty[:6], encoding.ErrInvalidArgument},
		{"future mode", []byte{1, 0, 10, 0, 0, 0, 0}, encoding.ErrNotSupported},
		{"vector size", []byte{0, 0, 2, 0, 0, 0, 0}, encoding.ErrInvalidArgument},
		{"offset", []byte{0, 0, 3, 1, 0, 0, 0, 5, 0, 0, 0}, encoding.ErrInvalidArgument},
		{"short vector", []byte{0, 0, 3, 1, 0, 0, 0, 4, 0, 0, 0}, encoding.ErrInvalidArgument},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := e.DecodeFloat(nil, tc.page); !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}
	if encoding.CanEncodeFloat(e) || encoding.CanEncodeDouble(e) {
		t.Fatal("ALP must be decode-only")
	}
}

func TestDecodeFullWidthDouble(t *testing.T) {
	page := []byte{0, 0, 3, 2, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0}
	page = binary.LittleEndian.AppendUint64(page, 1<<63)
	page = append(page, 64)
	page = binary.LittleEndian.AppendUint64(page, 0)
	page = binary.LittleEndian.AppendUint64(page, math.MaxUint64)
	got, err := new(alp.Encoding).DecodeDouble(nil, page)
	if err != nil || !slices.Equal(got, []float64{math.MinInt64, float64(math.MaxInt64)}) {
		t.Fatalf("got %v, %v", got, err)
	}
}

func BenchmarkDecodeDouble(b *testing.B) {
	// One vector of 1024 eight-bit deltas and no exceptions.
	const count = 1024
	page := []byte{0, 0, 10, 0, 4, 0, 0, 4, 0, 0, 0, 2, 1, 0, 0}
	page = binary.LittleEndian.AppendUint64(page, 1000)
	page = append(page, 8)
	for i := range count {
		page = append(page, byte(i))
	}
	e := new(alp.Encoding)
	dst := make([]float64, count)
	b.ReportAllocs()
	b.SetBytes(count * 8)
	for b.Loop() {
		if _, err := e.DecodeDouble(dst, page); err != nil {
			b.Fatal(err)
		}
	}
}

func TestPackedWidths(t *testing.T) {
	// Use a non-byte-aligned final group and remove all spare source capacity.
	// The first and last values of each delta also exercise the width boundary.
	const count = 257
	for width := 1; width <= 64; width++ {
		page := []byte{0, 0, 9, 1, 1, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0}
		page = binary.LittleEndian.AppendUint64(page, 0)
		page = append(page, byte(width))
		packed := make([]byte, (count*width+7)/8)
		want := make([]float64, count)
		for i := range count {
			mask := (uint64(1) << width) - 1
			delta := (uint64(i)*0x9e3779b97f4a7c15 ^ mask) & mask
			want[i] = float64(int64(delta))
			for bit := range width {
				if delta&(uint64(1)<<bit) != 0 {
					offset := i*width + bit
					packed[offset/8] |= 1 << (offset % 8)
				}
			}
		}
		page = append(page, packed...)
		got, err := new(alp.Encoding).DecodeDouble(nil, page[:len(page):len(page)])
		if err != nil || !slices.Equal(got, want) {
			t.Fatalf("incorrect %d-bit deltas: %v", width, err)
		}
	}
}
