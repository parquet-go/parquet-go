package sparse_test

import (
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"testing"
	"time"
	"unsafe"

	"github.com/parquet-go/parquet-go/sparse"
)

const (
	benchmarkGatherPerLoop = 1000
)

func ExampleGatherUint32() {
	type point2D struct{ X, Y uint32 }

	buf := make([]point2D, 10)
	dst := make([]uint32, 10)
	src := sparse.UnsafeUint32Array(unsafe.Pointer(&buf[0].Y), len(buf), unsafe.Sizeof(buf[0]))

	for i := range buf {
		buf[i].X = math.MaxUint32
		buf[i].Y = uint32(2 * i)
	}

	n := sparse.GatherUint32(dst, src)

	for i, v := range dst[:n] {
		fmt.Printf("points[%d].Y = %d\n", i, v)
	}

	// Output:
	// points[0].Y = 0
	// points[1].Y = 2
	// points[2].Y = 4
	// points[3].Y = 6
	// points[4].Y = 8
	// points[5].Y = 10
	// points[6].Y = 12
	// points[7].Y = 14
	// points[8].Y = 16
	// points[9].Y = 18
}

func ExampleGatherUint64() {
	type point2D struct{ X, Y uint64 }

	buf := make([]point2D, 10)
	dst := make([]uint64, 10)
	src := sparse.UnsafeUint64Array(unsafe.Pointer(&buf[0].Y), len(buf), unsafe.Sizeof(buf[0]))

	for i := range buf {
		buf[i].X = math.MaxUint64
		buf[i].Y = uint64(2 * i)
	}

	n := sparse.GatherUint64(dst, src)

	for i, v := range dst[:n] {
		fmt.Printf("points[%d].Y = %v\n", i, v)
	}

	// Output:
	// points[0].Y = 0
	// points[1].Y = 2
	// points[2].Y = 4
	// points[3].Y = 6
	// points[4].Y = 8
	// points[5].Y = 10
	// points[6].Y = 12
	// points[7].Y = 14
	// points[8].Y = 16
	// points[9].Y = 18
}

func ExampleGatherUint128() {
	type point2D struct{ X, Y [16]byte }

	buf := make([]point2D, 10)
	dst := make([][16]byte, 10)
	src := sparse.UnsafeUint128Array(unsafe.Pointer(&buf[0].Y), len(buf), unsafe.Sizeof(buf[0]))

	for i := range buf {
		x := uint64(math.MaxUint64)
		y := uint64(2 * i)
		binary.LittleEndian.PutUint64(buf[i].X[:], x)
		binary.LittleEndian.PutUint64(buf[i].Y[:], y)
	}

	n := sparse.GatherUint128(dst, src)

	for i, v := range dst[:n] {
		fmt.Printf("points[%d].Y = %v\n", i, binary.LittleEndian.Uint64(v[:]))
	}

	// Output:
	// points[0].Y = 0
	// points[1].Y = 2
	// points[2].Y = 4
	// points[3].Y = 6
	// points[4].Y = 8
	// points[5].Y = 10
	// points[6].Y = 12
	// points[7].Y = 14
	// points[8].Y = 16
	// points[9].Y = 18
}

func ExampleGatherString() {
	buf := make([][2]string, 10)
	dst := make([]string, 10)
	src := sparse.UnsafeStringArray(unsafe.Pointer(&buf[0][1]), len(buf), unsafe.Sizeof(buf[0]))

	for i := range buf {
		buf[i][0] = "-"
		buf[i][1] = strconv.Itoa(i)
	}

	n := sparse.GatherString(dst, src)

	for i, v := range dst[:n] {
		fmt.Printf("points[%d].Y = %v\n", i, v)
	}

	// Output:
	// points[0].Y = 0
	// points[1].Y = 1
	// points[2].Y = 2
	// points[3].Y = 3
	// points[4].Y = 4
	// points[5].Y = 5
	// points[6].Y = 6
	// points[7].Y = 7
	// points[8].Y = 8
	// points[9].Y = 9
}

func TestGatherUint32(t *testing.T) {
	type point2D struct{ X, Y uint32 }

	const N = 100
	buf := make([]point2D, N+1)
	dst := make([]uint32, N)
	src := sparse.UnsafeUint32Array(unsafe.Pointer(&buf[0].Y), len(buf), unsafe.Sizeof(buf[0]))

	for i := range buf {
		buf[i].X = math.MaxUint32
		buf[i].Y = uint32(2 * i)
	}

	if n := sparse.GatherUint32(dst, src); n != N {
		t.Errorf("wrong number of values gathered: want=%d got=%d", N, n)
	}

	for i, v := range dst {
		if v != uint32(2*i) {
			t.Errorf("wrong value gathered at index %d: want=%d got=%d", i, 2*i, v)
		}
	}
}

func TestGatherUint64(t *testing.T) {
	type point2D struct{ X, Y uint64 }

	const N = 100
	buf := make([]point2D, N+1)
	dst := make([]uint64, N)
	src := sparse.UnsafeUint64Array(unsafe.Pointer(&buf[0].Y), len(buf), unsafe.Sizeof(buf[0]))

	for i := range buf {
		buf[i].X = math.MaxUint64
		buf[i].Y = uint64(2 * i)
	}

	if n := sparse.GatherUint64(dst, src); n != N {
		t.Errorf("wrong number of values gathered: want=%d got=%d", N, n)
	}

	for i, v := range dst {
		if v != uint64(2*i) {
			t.Errorf("wrong value gathered at index %d: want=%d got=%d", i, 2*i, v)
		}
	}
}

func TestGatherUint128(t *testing.T) {
	type point2D struct{ X, Y [16]byte }

	const N = 100
	buf := make([]point2D, N+1)
	dst := make([][16]byte, N)
	src := sparse.UnsafeUint128Array(unsafe.Pointer(&buf[0].Y), len(buf), unsafe.Sizeof(buf[0]))

	for i := range buf {
		x := uint64(math.MaxUint64)
		y := uint64(2 * i)
		binary.LittleEndian.PutUint64(buf[i].X[:], x)
		binary.LittleEndian.PutUint64(buf[i].Y[:], y)
	}

	if n := sparse.GatherUint128(dst, src); n != N {
		t.Errorf("wrong number of values gathered: want=%d got=%d", N, n)
	}

	for i, v := range dst {
		if y := binary.LittleEndian.Uint64(v[:]); y != uint64(2*i) {
			t.Errorf("wrong value gathered at index %d: want=%d got=%d", i, 2*i, y)
		}
	}
}

func BenchmarkGather32(b *testing.B) {
	type point2D struct{ X, Y uint32 }

	buf := make([]point2D, benchmarkGatherPerLoop)
	dst := make([]uint32, benchmarkGatherPerLoop)
	src := sparse.UnsafeUint32Array(unsafe.Pointer(&buf[0].Y), len(buf), unsafe.Sizeof(buf[0]))

	b.SetBytes(4 * benchmarkGatherPerLoop)
	benchmarkThroughput(b, "gather", func() int {
		return sparse.GatherUint32(dst, src)
	})
}

func BenchmarkGather64(b *testing.B) {
	type point2D struct{ X, Y uint64 }

	buf := make([]point2D, benchmarkGatherPerLoop)
	dst := make([]uint64, benchmarkGatherPerLoop)
	src := sparse.UnsafeUint64Array(unsafe.Pointer(&buf[0].Y), len(buf), unsafe.Sizeof(buf[0]))

	b.SetBytes(8 * benchmarkGatherPerLoop)
	benchmarkThroughput(b, "gather", func() int {
		return sparse.GatherUint64(dst, src)
	})
}

func BenchmarkGather128(b *testing.B) {
	type point2D struct{ X, Y [16]byte }

	buf := make([]point2D, benchmarkGatherPerLoop)
	dst := make([][16]byte, benchmarkGatherPerLoop)
	src := sparse.UnsafeUint128Array(unsafe.Pointer(&buf[0].Y), len(buf), unsafe.Sizeof(buf[0]))

	b.SetBytes(16 * benchmarkGatherPerLoop)
	benchmarkThroughput(b, "gather", func() int {
		return sparse.GatherUint128(dst, src)
	})
}

func benchmarkThroughput(b *testing.B, m string, f func() int) {
	start := time.Now()
	count := 0

	for b.Loop() {
		count += f()
	}

	seconds := time.Since(start).Seconds()
	b.ReportMetric(float64(count)/seconds, m+"/s")
}
