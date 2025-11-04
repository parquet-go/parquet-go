package wyhash

import (
	"encoding/binary"
	"math/rand"
	"testing"
	"time"
)

func TestHash32(t *testing.T) {
	if h := Hash32(42, 1); h != 0xda93b6f668a0496e {
		t.Errorf("hash mismatch: %08x", h)
	}
}

func TestMultiHash32(t *testing.T) {
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
			t.Errorf("hash(%d): want=%08x got=%08x", values[i], h, hashes[i])
		}
	}
}

func BenchmarkHash32(b *testing.B) {
	b.SetBytes(8)
	value := rand.Uint32()
	benchmarkHashThroughput(b, func(seed uintptr) int {
		value = uint32(Hash32(value, seed))
		return 1
	})
}

func BenchmarkMultiHash32(b *testing.B) {
	hashes := [512]uintptr{}
	values := [512]uint32{}
	b.SetBytes(4 * int64(len(hashes)))
	benchmarkHashThroughput(b, func(seed uintptr) int {
		MultiHash32(hashes[:], values[:], seed)
		return len(hashes)
	})
}

func TestHash64(t *testing.T) {
	if h := Hash64(42, 1); h != 0x6e69a6ede6b5a25e {
		t.Errorf("hash mismatch: %016x", h)
	}
}

func TestMultiHash64(t *testing.T) {
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

func BenchmarkHash64(b *testing.B) {
	b.SetBytes(8)
	value := rand.Uint64()
	benchmarkHashThroughput(b, func(seed uintptr) int {
		value = uint64(Hash64(value, seed))
		return 1
	})
}

func BenchmarkMultiHash64(b *testing.B) {
	hashes := [512]uintptr{}
	values := [512]uint64{}
	b.SetBytes(8 * int64(len(hashes)))
	benchmarkHashThroughput(b, func(seed uintptr) int {
		MultiHash64(hashes[:], values[:], seed)
		return len(hashes)
	})
}

func TestHash128(t *testing.T) {
	if h := Hash128([16]byte{0: 42}, 1); h != 0xcd09fcdae9a79e7c {
		t.Errorf("hash mismatch: %016x", h)
	}
}

func TestMultiHash128(t *testing.T) {
	const N = 10
	hashes := [N]uintptr{}
	values := [N][16]byte{}
	seed := uintptr(64)

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

func BenchmarkHash128(b *testing.B) {
	b.SetBytes(8)
	hash := uintptr(0)
	value := [16]byte{}
	binary.LittleEndian.PutUint64(value[:8], rand.Uint64())
	binary.LittleEndian.PutUint64(value[8:], rand.Uint64())
	benchmarkHashThroughput(b, func(seed uintptr) int {
		hash = Hash128(value, seed)
		return 1
	})
	_ = hash
}

func BenchmarkMultiHash128(b *testing.B) {
	hashes := [512]uintptr{}
	values := [512][16]byte{}
	b.SetBytes(16 * int64(len(hashes)))
	benchmarkHashThroughput(b, func(seed uintptr) int {
		MultiHash128(hashes[:], values[:], seed)
		return len(hashes)
	})
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
