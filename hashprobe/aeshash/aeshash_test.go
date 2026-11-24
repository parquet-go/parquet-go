package aeshash

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go/sparse"
)

func init() {
	testingInitAesKeySched()
}

func TestHash32(t *testing.T) {
	if !Enabled() {
		t.Skip("AES hash not supported on this platform")
	}

	h := Hash32(42, 1)

	expected := uintptr(0x5e6ec6d2d7f7e0a0)
	if h != expected {
		t.Errorf("want=%016x got=%016x", expected, h)
	}
}

func TestMultiHash32(t *testing.T) {
	if !Enabled() {
		t.Skip("AES hash not supported on this platform")
	}

	const N = 10
	hashes := [N]uintptr{}
	values := [N]uint32{}
	seed := uintptr(32)

	for i := range values {
		values[i] = uint32(i)
	}

	MultiHash32(hashes[:], values[:], seed)

	for i := range values {
		h := Hash32(values[i], seed)

		if h != hashes[i] {
			t.Errorf("hash(%d): want=%016x got=%016x", values[i], h, hashes[i])
		}
	}
}

func TestHash64(t *testing.T) {
	if !Enabled() {
		t.Skip("AES hash not supported on this platform")
	}

	h := Hash64(42, 1)

	expected := uintptr(0x5e6ec6d2d7f7e0a0)
	if h != expected {
		t.Errorf("want=%016x got=%016x", expected, h)
	}
}

func TestMultiHash64(t *testing.T) {
	if !Enabled() {
		t.Skip("AES hash not supported on this platform")
	}

	const N = 10
	hashes := [N]uintptr{}
	values := [N]uint64{}
	seed := uintptr(64)

	for i := range values {
		values[i] = uint64(i)
	}

	MultiHash64(hashes[:], values[:], seed)

	for i := range values {
		h := Hash64(values[i], seed)

		if h != hashes[i] {
			t.Errorf("hash(%d): want=%016x got=%016x", values[i], h, hashes[i])
		}
	}
}

func TestMultiHashArrayStopsAtValues(t *testing.T) {
	if !Enabled() {
		t.Skip("AES hash not supported on this platform")
	}
	seed := uintptr(7)
	check32 := func(n int) {
		t.Helper()
		values := make([]uint32, n)
		for i := range values {
			values[i] = uint32(i + 1)
		}
		hashes := make([]uintptr, n+3)
		for i := range hashes {
			hashes[i] = 1
		}
		MultiHashUint32Array(hashes, sparse.MakeUint32Array(values), seed)
		for i, v := range values {
			if hashes[i] != Hash32(v, seed) {
				t.Errorf("uint32 n=%d [%d]: want %x got %x", n, i, Hash32(v, seed), hashes[i])
			}
		}
		for i := n; i < len(hashes); i++ {
			if hashes[i] != 1 {
				t.Errorf("uint32 n=%d tail[%d] = %x", n, i, hashes[i])
			}
		}
	}
	check64 := func(n int) {
		t.Helper()
		values := make([]uint64, n)
		for i := range values {
			values[i] = uint64(i + 1)
		}
		hashes := make([]uintptr, n+3)
		for i := range hashes {
			hashes[i] = 1
		}
		MultiHashUint64Array(hashes, sparse.MakeUint64Array(values), seed)
		for i, v := range values {
			if hashes[i] != Hash64(v, seed) {
				t.Errorf("uint64 n=%d [%d]: want %x got %x", n, i, Hash64(v, seed), hashes[i])
			}
		}
		for i := n; i < len(hashes); i++ {
			if hashes[i] != 1 {
				t.Errorf("uint64 n=%d tail[%d] = %x", n, i, hashes[i])
			}
		}
	}
	check128 := func(n int) {
		t.Helper()
		values := make([][16]byte, n)
		for i := range values {
			values[i][0] = byte(i + 1)
		}
		hashes := make([]uintptr, n+3)
		for i := range hashes {
			hashes[i] = 1
		}
		MultiHashUint128Array(hashes, sparse.MakeUint128Array(values), seed)
		for i, v := range values {
			if hashes[i] != Hash128(v, seed) {
				t.Errorf("uint128 n=%d [%d]: want %x got %x", n, i, Hash128(v, seed), hashes[i])
			}
		}
		for i := n; i < len(hashes); i++ {
			if hashes[i] != 1 {
				t.Errorf("uint128 n=%d tail[%d] = %x", n, i, hashes[i])
			}
		}
	}
	for _, n := range []int{0, 1, 3, 5} {
		check32(n)
		check64(n)
		check128(n)
	}
}

func BenchmarkMultiHash64(b *testing.B) {
	if !Enabled() {
		b.Skip("AES hash not supported on this platform")
	}

	hashes := [512]uintptr{}
	values := [512]uint64{}
	b.SetBytes(8 * int64(len(hashes)))
	benchmarkHashThroughput(b, func(seed uintptr) int {
		MultiHash64(hashes[:], values[:], seed)
		return len(hashes)
	})
}

func TestHash128(t *testing.T) {
	if !Enabled() {
		t.Skip("AES hash not supported on this platform")
	}

	h := Hash128([16]byte{0: 42}, 1)

	expected := uintptr(0x5db3281d2806690a)
	if h != expected {
		t.Errorf("want=%016x got=%016x", expected, h)
	}
}

func TestMultiHash128(t *testing.T) {
	if !Enabled() {
		t.Skip("AES hash not supported on this platform")
	}

	const N = 10
	hashes := [N]uintptr{}
	values := [N][16]byte{}
	seed := uintptr(128)

	for i := range values {
		binary.LittleEndian.PutUint64(values[i][:8], uint64(i))
	}

	MultiHash128(hashes[:], values[:], seed)

	for i := range values {
		h := Hash128(values[i], seed)

		if h != hashes[i] {
			t.Errorf("hash(%d): want=%016x got=%016x", values[i], h, hashes[i])
		}
	}
}

func benchmarkHashThroughput(b *testing.B, f func(seed uintptr) int) {
	hashes := int64(0)
	start := time.Now()

	for i := 0; b.Loop(); i++ {
		hashes += int64(f(uintptr(i)))
	}

	seconds := time.Since(start).Seconds()
	b.ReportMetric(float64(hashes)/seconds, "hash/s")
}
