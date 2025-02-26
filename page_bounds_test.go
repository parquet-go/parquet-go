package parquet

import (
	"bytes"
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/parquet-go/parquet-go/internal/quick"
)

var benchmarkBufferSizes = [...]int{
	4 * 1024,
	256 * 1024,
	2048 * 1024,
}

func forEachBenchmarkBufferSize(b *testing.B, f func(*testing.B, int)) {
	for _, bufferSize := range benchmarkBufferSizes {
		b.Run(fmt.Sprintf("%dKiB", bufferSize/1024), func(b *testing.B) {
			b.SetBytes(int64(bufferSize))
			f(b, bufferSize)
		})
	}
}

func TestBoundsInt32(t *testing.T) {
	err := quick.Check(func(values []int32) bool {
		min := int32(0)
		max := int32(0)
		if len(values) > 0 {
			min = values[0]
			max = values[0]
			for _, v := range values[1:] {
				if v < min {
					min = v
				}
				if v > max {
					max = v
				}
			}
		}
		minValue, maxValue := boundsInt32(values)
		return min == minValue && max == maxValue
	})
	if err != nil {
		t.Error(err)
	}
}

func TestBoundsInt64(t *testing.T) {
	err := quick.Check(func(values []int64) bool {
		min := int64(0)
		max := int64(0)
		if len(values) > 0 {
			min = values[0]
			max = values[0]
			for _, v := range values[1:] {
				if v < min {
					min = v
				}
				if v > max {
					max = v
				}
			}
		}
		minValue, maxValue := boundsInt64(values)
		return min == minValue && max == maxValue
	})
	if err != nil {
		t.Error(err)
	}
}

func TestBoundsUint32(t *testing.T) {
	err := quick.Check(func(values []uint32) bool {
		min := uint32(0)
		max := uint32(0)
		if len(values) > 0 {
			min = values[0]
			max = values[0]
			for _, v := range values[1:] {
				if v < min {
					min = v
				}
				if v > max {
					max = v
				}
			}
		}
		minValue, maxValue := boundsUint32(values)
		return min == minValue && max == maxValue
	})
	if err != nil {
		t.Error(err)
	}
}

func TestBoundsUint64(t *testing.T) {
	err := quick.Check(func(values []uint64) bool {
		min := uint64(0)
		max := uint64(0)
		if len(values) > 0 {
			min = values[0]
			max = values[0]
			for _, v := range values[1:] {
				if v < min {
					min = v
				}
				if v > max {
					max = v
				}
			}
		}
		minValue, maxValue := boundsUint64(values)
		return min == minValue && max == maxValue
	})
	if err != nil {
		t.Error(err)
	}
}

func TestBoundsFloat32(t *testing.T) {
	err := quick.Check(func(values []float32) bool {
		min := float32(0)
		max := float32(0)
		if len(values) > 0 {
			min = values[0]
			max = values[0]
			for _, v := range values[1:] {
				if v < min {
					min = v
				}
				if v > max {
					max = v
				}
			}
		}
		minValue, maxValue := boundsFloat32(values)
		return min == minValue && max == maxValue
	})
	if err != nil {
		t.Error(err)
	}
}

func TestBoundsFloat64(t *testing.T) {
	err := quick.Check(func(values []float64) bool {
		min := float64(0)
		max := float64(0)
		if len(values) > 0 {
			min = values[0]
			max = values[0]
			for _, v := range values[1:] {
				if v < min {
					min = v
				}
				if v > max {
					max = v
				}
			}
		}
		minValue, maxValue := boundsFloat64(values)
		return min == minValue && max == maxValue
	})
	if err != nil {
		t.Error(err)
	}
}

func TestBE128MinMaxSimilar(t *testing.T) {
	var min [16]byte

	// Test values:
	//   [1 1 ... 1 1]
	//   [0 1 ... 1 1]
	//   ...
	//   [0 0 ... 0 1]
	//   [0 0 ... 0 0]
	for i := range 17 {
		var max [16]byte
		for j := i; j < 16; j++ {
			max[j] = 1
		}
		testBE182MinMaxPerm(t, min, max)
	}

	// Test values:
	//   [0 0 ... 0 0]
	//   [1 0 ... 0 0]
	//   ...
	//   [1 1 ... 1 0]
	//   [1 1 ... 1 1]
	for i := range 17 {
		var max [16]byte
		for j := range i {
			max[j] = 1
		}
		testBE182MinMaxPerm(t, min, max)
	}
}

func testBE182MinMaxPerm(t *testing.T, min, max [16]byte) {
	testBE128MinMax(t, min[:], max[:], [][16]byte{min, max})
	testBE128MinMax(t, min[:], max[:], [][16]byte{max, min})
}

func testBE128MinMax(t *testing.T, min, max []byte, data [][16]byte) {
	bmin := minBE128(data)
	if !reflect.DeepEqual(bmin, min[:]) {
		t.Errorf("unexpected min value\nexpected %v\n     got %v", min, bmin)
	}

	bmax := maxBE128(data)
	if !reflect.DeepEqual(bmax, max[:]) {
		t.Errorf("unexpected max value\nexpected %v\n     got %v", max, bmax)
	}
}

func TestBoundsBE128(t *testing.T) {
	err := quick.Check(func(values [][16]byte) bool {
		min := [16]byte{}
		max := [16]byte{}
		if len(values) > 0 {
			min = values[0]
			max = values[0]
			for _, v := range values[1:] {
				if bytes.Compare(v[:], min[:]) < 0 {
					min = v
				}
				if bytes.Compare(v[:], max[:]) > 0 {
					max = v
				}
			}
		}
		minValue, maxValue := boundsBE128(values)
		return (len(values) == 0 && minValue == nil && maxValue == nil) ||
			(bytes.Equal(min[:], minValue) && bytes.Equal(max[:], maxValue))
	})
	if err != nil {
		t.Error(err)
	}
}

func TestBoundsFixedLenByteArray(t *testing.T) {
	err := quick.Check(func(values []byte) bool {
		min := [1]byte{}
		max := [1]byte{}
		if len(values) > 0 {
			min[0] = values[0]
			max[0] = values[0]
			for _, v := range values[1:] {
				if v < min[0] {
					min[0] = v
				}
				if v > max[0] {
					max[0] = v
				}
			}
		}
		minValue, maxValue := boundsFixedLenByteArray(values, 1)
		return (len(values) == 0 && minValue == nil && maxValue == nil) ||
			(bytes.Equal(min[:], minValue) && bytes.Equal(max[:], maxValue))
	})
	if err != nil {
		t.Error(err)
	}
}

func BenchmarkBoundsInt32(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B, bufferSize int) {
		values := make([]int32, bufferSize/4)
		prng := rand.New(rand.NewSource(1))
		for i := range values {
			values[i] = prng.Int31()
		}
		for i := 0; i < b.N; i++ {
			boundsInt32(values)
		}
	})
}

func BenchmarkBoundsInt64(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B, bufferSize int) {
		values := make([]int64, bufferSize/8)
		prng := rand.New(rand.NewSource(1))
		for i := range values {
			values[i] = prng.Int63()
		}
		for i := 0; i < b.N; i++ {
			boundsInt64(values)
		}
	})
}

func BenchmarkBoundsUint32(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B, bufferSize int) {
		values := make([]uint32, bufferSize/4)
		prng := rand.New(rand.NewSource(1))
		for i := range values {
			values[i] = prng.Uint32()
		}
		for i := 0; i < b.N; i++ {
			boundsUint32(values)
		}
	})
}

func BenchmarkBoundsUint64(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B, bufferSize int) {
		values := make([]uint64, bufferSize/8)
		prng := rand.New(rand.NewSource(1))
		for i := range values {
			values[i] = prng.Uint64()
		}
		for i := 0; i < b.N; i++ {
			boundsUint64(values)
		}
	})
}

func BenchmarkBoundsFloat32(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B, bufferSize int) {
		values := make([]float32, bufferSize/4)
		prng := rand.New(rand.NewSource(1))
		for i := range values {
			values[i] = prng.Float32()
		}
		for i := 0; i < b.N; i++ {
			boundsFloat32(values)
		}
	})
}

func BenchmarkBoundsFloat64(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B, bufferSize int) {
		values := make([]float64, bufferSize/8)
		prng := rand.New(rand.NewSource(1))
		for i := range values {
			values[i] = prng.Float64()
		}
		for i := 0; i < b.N; i++ {
			boundsFloat64(values)
		}
	})
}

func BenchmarkBoundsBE128(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B, bufferSize int) {
		values := make([][16]byte, bufferSize)
		prng := rand.New(rand.NewSource(1))
		for i := range values {
			prng.Read(values[i][:])
		}
		for i := 0; i < b.N; i++ {
			boundsBE128(values)
		}
	})
}

func BenchmarkBoundsFixedLenByteArray(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B, bufferSize int) {
		values := make([]byte, bufferSize)
		prng := rand.New(rand.NewSource(1))
		prng.Read(values)
		for i := 0; i < b.N; i++ {
			boundsFixedLenByteArray(values, 32)
		}
	})
}
