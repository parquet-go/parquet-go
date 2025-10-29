package hashprobe

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"
	"time"
	"unsafe"
)

func TestTable32GroupSize(t *testing.T) {
	if n := unsafe.Sizeof(table32Group{}); n != 64 {
		t.Errorf("size of 32 bit table group is not 64 bytes: %d", n)
	}
}

func TestUint32TableProbeOneByOne(t *testing.T) {
	const N = 500
	table := NewUint32Table(0, 0.9)

	for range 2 {
		// Do two passes, both should behave the same.
		for i := 1; i <= N; i++ {
			k := [1]uint32{}
			v := [1]int32{}

			k[0] = uint32(i)
			table.Probe(k[:], v[:])

			if v[0] != int32(i-1) {
				t.Errorf("wrong value probed for key=%d: want=%d got=%d", i, i-1, v[0])
			}
		}
	}
}

func TestUint32TableProbeBulk(t *testing.T) {
	const N = 999
	table := NewUint32Table(0, 0.9)

	k := make([]uint32, N)
	v := make([]int32, N)

	for i := range k {
		k[i] = uint32(i)
	}

	for range 2 {
		table.Probe(k, v)

		for i := range v {
			if v[i] != int32(i) {
				t.Errorf("wrong value probed for key=%d: want=%d got=%d", k[i], i, v[i])
			}
		}

		if t.Failed() {
			break
		}

		for i := range v {
			v[i] = 0
		}
	}
}

func TestTable64GroupSize(t *testing.T) {
	if n := unsafe.Sizeof(table64Group{}); n != 64 {
		t.Errorf("size of 64 bit table group is not 64 bytes: %d", n)
	}
}

func TestUint64TableProbeOneByOne(t *testing.T) {
	const N = 500
	table := NewUint64Table(0, 0.9)

	for range 2 {
		// Do two passes, both should behave the same.
		for i := 1; i <= N; i++ {
			k := [1]uint64{}
			v := [1]int32{}

			k[0] = uint64(i)
			table.Probe(k[:], v[:])

			if v[0] != int32(i-1) {
				t.Errorf("wrong value probed for key=%d: want=%d got=%d", i, i-1, v[0])
			}
		}
	}
}

func TestUint64TableProbeBulk(t *testing.T) {
	const N = 999
	table := NewUint64Table(0, 0.9)

	k := make([]uint64, N)
	v := make([]int32, N)

	for i := range k {
		k[i] = uint64(i)
	}

	for range 2 {
		table.Probe(k, v)

		for i := range v {
			if v[i] != int32(i) {
				t.Errorf("wrong value probed for key=%d: want=%d got=%d", k[i], i, v[i])
			}
		}

		if t.Failed() {
			break
		}

		for i := range v {
			v[i] = 0
		}
	}
}

func TestUint128TableProbeOneByOne(t *testing.T) {
	const N = 500
	table := NewUint128Table(0, 0.9)

	for range 2 {
		// Do two passes, both should behave the same.
		for i := 1; i <= N; i++ {
			k := [1][16]byte{}
			v := [1]int32{}

			binary.LittleEndian.PutUint64(k[0][:8], uint64(i))
			table.Probe(k[:], v[:])

			if v[0] != int32(i-1) {
				t.Errorf("wrong value probed for key=%x: want=%d got=%d", i, i-1, v[0])
			}
		}
	}
}

func TestUint128TableProbeBulk(t *testing.T) {
	const N = 999
	table := NewUint128Table(0, 0.9)

	k := make([][16]byte, N)
	v := make([]int32, N)

	for i := range k {
		binary.LittleEndian.PutUint64(k[i][:8], uint64(i))
	}

	for range 2 {
		table.Probe(k, v)

		for i := range v {
			if v[i] != int32(i) {
				t.Errorf("wrong value probed for key=%x: want=%d got=%d", k[i], i, v[i])
			}
		}

		if t.Failed() {
			break
		}

		for i := range v {
			v[i] = 0
		}
	}
}

const (
	benchmarkProbesPerLoop = 500
	benchmarkMaxLoad       = 0.9
)

type uint32Table interface {
	Reset()
	Len() int
	Probe([]uint32, []int32) int
}

type uint32Map map[uint32]int32

func (m uint32Map) Reset() {
	for k := range m {
		delete(m, k)
	}
}

func (m uint32Map) Len() int {
	return len(m)
}

func (m uint32Map) Probe(keys []uint32, values []int32) (n int) {
	_ = values[:len(keys)]

	for i, k := range keys {
		v, ok := m[k]
		if !ok {
			v = int32(len(m))
			m[k] = v
			n++
		}
		values[i] = v
	}

	return n
}

func BenchmarkUint32Table(b *testing.B) {
	benchmarkUint32Table(b, func(size int) uint32Table { return NewUint32Table(size, benchmarkMaxLoad) })
}

func BenchmarkGoUint32Map(b *testing.B) {
	benchmarkUint32Table(b, func(size int) uint32Table { return make(uint32Map, size) })
}

func benchmarkUint32Table(b *testing.B, newTable func(size int) uint32Table) {
	for n := 100; n <= 1e6; n *= 10 {
		table := newTable(0)
		keys, values := generateUint32Table(n)

		b.Run(fmt.Sprintf("N=%d", n), func(b *testing.B) {
			benchmarkUint32Loop(b, table.Probe, keys, values)
		})
	}
}

func benchmarkUint32Loop(b *testing.B, f func([]uint32, []int32) int, keys []uint32, values []int32) {
	i := 0
	j := benchmarkProbesPerLoop
	b.SetBytes(4 * int64(benchmarkProbesPerLoop))

	_ = keys[:len(values)]
	_ = values[:len(keys)]
	start := time.Now()

	for b.Loop() {
		if j > len(keys) {
			j = len(keys)
		}
		f(keys[i:j:j], values[i:j:j])
		if j == len(keys) {
			i, j = 0, benchmarkProbesPerLoop
		} else {
			i, j = j, j+benchmarkProbesPerLoop
		}
	}

	seconds := time.Since(start).Seconds()
	b.ReportMetric(float64(benchmarkProbesPerLoop*b.N)/seconds, "probe/s")
}

func generateUint32Table(n int) ([]uint32, []int32) {
	prng := rand.New(rand.NewSource(int64(n)))
	keys := make([]uint32, n)
	values := make([]int32, n)

	for i := range keys {
		keys[i] = prng.Uint32()
	}

	return keys, values
}

type uint64Table interface {
	Reset()
	Len() int
	Probe([]uint64, []int32) int
}

type uint64Map map[uint64]int32

func (m uint64Map) Reset() {
	for k := range m {
		delete(m, k)
	}
}

func (m uint64Map) Len() int {
	return len(m)
}

func (m uint64Map) Probe(keys []uint64, values []int32) (n int) {
	_ = values[:len(keys)]

	for i, k := range keys {
		v, ok := m[k]
		if !ok {
			v = int32(len(m))
			m[k] = v
			n++
		}
		values[i] = v
	}

	return n
}

func BenchmarkUint64Table(b *testing.B) {
	benchmarkUint64Table(b, func(size int) uint64Table { return NewUint64Table(size, benchmarkMaxLoad) })
}

func BenchmarkGoUint64Map(b *testing.B) {
	benchmarkUint64Table(b, func(size int) uint64Table { return make(uint64Map, size) })
}

func benchmarkUint64Table(b *testing.B, newTable func(size int) uint64Table) {
	for n := 100; n <= 1e6; n *= 10 {
		table := newTable(0)
		keys, values := generateUint64Table(n)

		b.Run(fmt.Sprintf("N=%d", n), func(b *testing.B) {
			benchmarkUint64Loop(b, table.Probe, keys, values)
		})
	}
}

func benchmarkUint64Loop(b *testing.B, f func([]uint64, []int32) int, keys []uint64, values []int32) {
	i := 0
	j := benchmarkProbesPerLoop
	b.SetBytes(8 * int64(benchmarkProbesPerLoop))

	_ = keys[:len(values)]
	_ = values[:len(keys)]
	start := time.Now()

	for b.Loop() {
		if j > len(keys) {
			j = len(keys)
		}
		f(keys[i:j:j], values[i:j:j])
		if j == len(keys) {
			i, j = 0, benchmarkProbesPerLoop
		} else {
			i, j = j, j+benchmarkProbesPerLoop
		}
	}

	seconds := time.Since(start).Seconds()
	b.ReportMetric(float64(benchmarkProbesPerLoop*b.N)/seconds, "probe/s")
}

func generateUint64Table(n int) ([]uint64, []int32) {
	prng := rand.New(rand.NewSource(int64(n)))
	keys := make([]uint64, n)
	values := make([]int32, n)

	for i := range keys {
		keys[i] = prng.Uint64()
	}

	return keys, values
}

type uint128Table interface {
	Reset()
	Len() int
	Probe([][16]byte, []int32) int
}

type uint128Map map[[16]byte]int32

func (m uint128Map) Reset() {
	for k := range m {
		delete(m, k)
	}
}

func (m uint128Map) Len() int {
	return len(m)
}

func (m uint128Map) Probe(keys [][16]byte, values []int32) (n int) {
	_ = values[:len(keys)]

	for i, k := range keys {
		v, ok := m[k]
		if !ok {
			v = int32(len(m))
			m[k] = v
			n++
		}
		values[i] = v
	}

	return n
}

func BenchmarkUint128Table(b *testing.B) {
	benchmarkUint128Table(b, func(size int) uint128Table { return NewUint128Table(size, benchmarkMaxLoad) })
}

func BenchmarkGoUint128Map(b *testing.B) {
	benchmarkUint128Table(b, func(size int) uint128Table { return make(uint128Map, size) })
}

func benchmarkUint128Table(b *testing.B, newTable func(size int) uint128Table) {
	for n := 100; n <= 1e6; n *= 10 {
		table := newTable(0)
		keys, values := generateUint128Table(n)

		b.Run(fmt.Sprintf("N=%d", n), func(b *testing.B) {
			benchmarkUint128Loop(b, table.Probe, keys, values)
		})
	}
}

func benchmarkUint128Loop(b *testing.B, f func([][16]byte, []int32) int, keys [][16]byte, values []int32) {
	i := 0
	j := benchmarkProbesPerLoop
	b.SetBytes(16 * int64(benchmarkProbesPerLoop))

	_ = keys[:len(values)]
	_ = values[:len(keys)]
	start := time.Now()

	for b.Loop() {
		if j > len(keys) {
			j = len(keys)
		}
		f(keys[i:j:j], values[i:j:j])
		if j == len(keys) {
			i, j = 0, benchmarkProbesPerLoop
		} else {
			i, j = j, j+benchmarkProbesPerLoop
		}
	}

	seconds := time.Since(start).Seconds()
	b.ReportMetric(float64(benchmarkProbesPerLoop*b.N)/seconds, "probe/s")
}

func generateUint128Table(n int) ([][16]byte, []int32) {
	prng := rand.New(rand.NewSource(int64(n)))
	keys := make([][16]byte, n)
	values := make([]int32, n)

	for i := range keys {
		prng.Read(keys[i][:])
	}

	return keys, values
}
