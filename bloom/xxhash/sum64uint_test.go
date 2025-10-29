package xxhash_test

import (
	"encoding/binary"
	"fmt"
	"testing"
	"testing/quick"
	"time"

	"github.com/parquet-go/parquet-go/bloom/xxhash"
)

func TestSumUint8(t *testing.T) {
	b := [1]byte{0: 42}
	h := xxhash.Sum64Uint8(42)
	x := xxhash.Sum64(b[:])
	if h != x {
		t.Errorf("got %064b; want %064b", h, x)
	}
}

func TestSumUint16(t *testing.T) {
	b := [2]byte{0: 42}
	h := xxhash.Sum64Uint16(42)
	x := xxhash.Sum64(b[:])
	if h != x {
		t.Errorf("got %064b; want %064b", h, x)
	}
}

func TestSumUint32(t *testing.T) {
	b := [4]byte{0: 42}
	h := xxhash.Sum64Uint32(42)
	x := xxhash.Sum64(b[:])
	if h != x {
		t.Errorf("got %064b; want %064b", h, x)
	}
}

func TestSumUint64(t *testing.T) {
	b := [8]byte{0: 42}
	h := xxhash.Sum64Uint64(42)
	x := xxhash.Sum64(b[:])
	if h != x {
		t.Errorf("got %064b; want %064b", h, x)
	}
}

func TestSumUint128(t *testing.T) {
	b := [16]byte{0: 42}
	h := xxhash.Sum64Uint128(b)
	x := xxhash.Sum64(b[:])
	if h != x {
		t.Errorf("got %064b; want %064b", h, x)
	}
}

func TestMultiSum64Uint8(t *testing.T) {
	f := func(v []uint8) bool {
		h := make([]uint64, len(v))
		n := xxhash.MultiSum64Uint8(h, v)
		if n != len(v) {
			t.Errorf("return value mismatch: got %d; want %d", n, len(v))
			return false
		}
		for i := range h {
			x := xxhash.Sum64(v[i : i+1])
			if h[i] != x {
				t.Errorf("sum at index %d mismatch: got %064b; want %064b", i, h[i], x)
				return false
			}
		}
		return true
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

func TestMultiSum64Uint16(t *testing.T) {
	f := func(v []uint16) bool {
		h := make([]uint64, len(v))
		n := xxhash.MultiSum64Uint16(h, v)
		if n != len(v) {
			t.Errorf("return value mismatch: got %d; want %d", n, len(v))
			return false
		}
		for i := range h {
			b := [2]byte{}
			binary.LittleEndian.PutUint16(b[:], v[i])
			x := xxhash.Sum64(b[:])
			if h[i] != x {
				t.Errorf("sum at index %d mismatch: got %064b; want %064b", i, h[i], x)
				return false
			}
		}
		return true
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

func TestMultiSum64Uint32(t *testing.T) {
	f := func(v []uint32) bool {
		h := make([]uint64, len(v))
		n := xxhash.MultiSum64Uint32(h, v)
		if n != len(v) {
			t.Errorf("return value mismatch: got %d; want %d", n, len(v))
			return false
		}
		for i := range h {
			b := [4]byte{}
			binary.LittleEndian.PutUint32(b[:], v[i])
			x := xxhash.Sum64(b[:])
			if h[i] != x {
				t.Errorf("sum at index %d mismatch: got %064b; want %064b", i, h[i], x)
				return false
			}
		}
		return true
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

func TestMultiSum64Uint64(t *testing.T) {
	f := func(v []uint64) bool {
		h := make([]uint64, len(v))
		n := xxhash.MultiSum64Uint64(h, v)
		if n != len(v) {
			t.Errorf("return value mismatch: got %d; want %d", n, len(v))
			return false
		}
		for i := range h {
			b := [8]byte{}
			binary.LittleEndian.PutUint64(b[:], v[i])
			x := xxhash.Sum64(b[:])
			if h[i] != x {
				t.Errorf("sum at index %d mismatch: got %064b; want %064b", i, h[i], x)
				return false
			}
		}
		return true
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

func TestMultiSum64Uint128(t *testing.T) {
	f := func(v [][16]byte) bool {
		h := make([]uint64, len(v))
		n := xxhash.MultiSum64Uint128(h, v)
		if n != len(v) {
			t.Errorf("return value mismatch: got %d; want %d", n, len(v))
			return false
		}
		for i := range h {
			x := xxhash.Sum64(v[i][:])
			if h[i] != x {
				t.Errorf("sum at index %d mismatch: got %064b; want %064b", i, h[i], x)
				return false
			}
		}
		return true
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

func reportThroughput(b *testing.B, loops, count int, start time.Time) {
	throughput := float64(loops*count) / time.Since(start).Seconds()
	// Measure the throughput of writes to the output buffer;
	// it makes the results comparable across benchmarks that
	// have inputs of different sizes.
	b.SetBytes(8 * int64(count))
	b.ReportMetric(0, "ns/op")
	b.ReportMetric(throughput, "hash/s")
}

const benchmarkBufferSize = 4096

func BenchmarkMultiSum64Uint8(b *testing.B) {
	in := make([]uint8, benchmarkBufferSize)
	for i := range in {
		in[i] = uint8(i)
	}
	b.Run(fmt.Sprintf("%dKB", benchmarkBufferSize/1024), func(b *testing.B) {
		out := make([]uint64, len(in))
		start := time.Now()
		for b.Loop() {
			_ = xxhash.MultiSum64Uint8(out, in)
		}
		reportThroughput(b, b.N, len(out), start)
	})
}

func BenchmarkMultiSum64Uint16(b *testing.B) {
	in := make([]uint16, benchmarkBufferSize/2)
	for i := range in {
		in[i] = uint16(i)
	}
	b.Run(fmt.Sprintf("%dKB", benchmarkBufferSize/1024), func(b *testing.B) {
		out := make([]uint64, len(in))
		start := time.Now()
		for b.Loop() {
			_ = xxhash.MultiSum64Uint16(out, in)
		}
		reportThroughput(b, b.N, len(out), start)
	})
}

func BenchmarkMultiSum64Uint32(b *testing.B) {
	in := make([]uint32, benchmarkBufferSize/4)
	for i := range in {
		in[i] = uint32(i)
	}
	b.Run(fmt.Sprintf("%dKB", benchmarkBufferSize/1024), func(b *testing.B) {
		out := make([]uint64, len(in))
		start := time.Now()
		for b.Loop() {
			_ = xxhash.MultiSum64Uint32(out, in)
		}
		reportThroughput(b, b.N, len(out), start)
	})
}

func BenchmarkMultiSum64Uint64(b *testing.B) {
	in := make([]uint64, benchmarkBufferSize/8)
	for i := range in {
		in[i] = uint64(i)
	}
	b.Run(fmt.Sprintf("%dKB", benchmarkBufferSize/1024), func(b *testing.B) {
		out := make([]uint64, len(in))
		start := time.Now()
		for b.Loop() {
			_ = xxhash.MultiSum64Uint64(out, in)
		}
		reportThroughput(b, b.N, len(out), start)
	})
}

func BenchmarkMultiSum64Uint128(b *testing.B) {
	in := make([][16]byte, benchmarkBufferSize/16)
	for i := range in {
		binary.LittleEndian.PutUint64(in[i][:8], uint64(i))
		binary.LittleEndian.PutUint64(in[i][8:], uint64(i))
	}
	b.Run(fmt.Sprintf("%dKB", benchmarkBufferSize/1024), func(b *testing.B) {
		out := make([]uint64, len(in))
		start := time.Now()
		for b.Loop() {
			_ = xxhash.MultiSum64Uint128(out, in)
		}
		reportThroughput(b, b.N, len(out), start)
	})
}
