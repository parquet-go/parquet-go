package parquet

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/parquet-go/parquet-go/internal/quick"
)

func TestMaxInt32(t *testing.T) {
	err := quick.Check(func(values []int32) bool {
		max := int32(0)
		if len(values) > 0 {
			max = values[0]
			for _, v := range values[1:] {
				if v > max {
					max = v
				}
			}
		}
		return max == maxInt32(values)
	})
	if err != nil {
		t.Error(err)
	}
}

func TestMaxInt64(t *testing.T) {
	err := quick.Check(func(values []int64) bool {
		max := int64(0)
		if len(values) > 0 {
			max = values[0]
			for _, v := range values[1:] {
				if v > max {
					max = v
				}
			}
		}
		return max == maxInt64(values)
	})
	if err != nil {
		t.Error(err)
	}
}

func TestMaxUint32(t *testing.T) {
	err := quick.Check(func(values []uint32) bool {
		max := uint32(0)
		if len(values) > 0 {
			max = values[0]
			for _, v := range values[1:] {
				if v > max {
					max = v
				}
			}
		}
		return max == maxUint32(values)
	})
	if err != nil {
		t.Error(err)
	}
}

func TestMaxUint64(t *testing.T) {
	err := quick.Check(func(values []uint64) bool {
		max := uint64(0)
		if len(values) > 0 {
			max = values[0]
			for _, v := range values[1:] {
				if v > max {
					max = v
				}
			}
		}
		return max == maxUint64(values)
	})
	if err != nil {
		t.Error(err)
	}
}

func TestMaxFloat32(t *testing.T) {
	err := quick.Check(func(values []float32) bool {
		max := float32(0)
		if len(values) > 0 {
			max = values[0]
			for _, v := range values[1:] {
				if v > max {
					max = v
				}
			}
		}
		return max == maxFloat32(values)
	})
	if err != nil {
		t.Error(err)
	}
}

func TestMaxFloat64(t *testing.T) {
	err := quick.Check(func(values []float64) bool {
		max := float64(0)
		if len(values) > 0 {
			max = values[0]
			for _, v := range values[1:] {
				if v > max {
					max = v
				}
			}
		}
		return max == maxFloat64(values)
	})
	if err != nil {
		t.Error(err)
	}
}

func TestMaxBE128(t *testing.T) {
	err := quick.Check(func(values [][16]byte) bool {
		max := [16]byte{}
		if len(values) > 0 {
			max = values[0]
			for _, v := range values[1:] {
				if bytes.Compare(v[:], max[:]) > 0 {
					max = v
				}
			}
		}
		ret := maxBE128(values)
		return (len(values) == 0 && ret == nil) || bytes.Equal(max[:], ret)
	})
	if err != nil {
		t.Error(err)
	}
}

func TestMaxFixedLenByteArray(t *testing.T) {
	err := quick.Check(func(values []byte) bool {
		max := [1]byte{}
		if len(values) > 0 {
			max[0] = values[0]
			for _, v := range values[1:] {
				if v > max[0] {
					max[0] = v
				}
			}
		}
		ret := maxFixedLenByteArray(values, 1)
		return (len(values) == 0 && ret == nil) || bytes.Equal(max[:], ret)
	})
	if err != nil {
		t.Error(err)
	}
}

func BenchmarkMaxInt32(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B, bufferSize int) {
		values := make([]int32, bufferSize/4)
		prng := rand.New(rand.NewSource(1))
		for i := range values {
			values[i] = prng.Int31()
		}
		for b.Loop() {
			maxInt32(values)
		}
	})
}

func BenchmarkMaxInt64(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B, bufferSize int) {
		values := make([]int64, bufferSize/8)
		prng := rand.New(rand.NewSource(1))
		for i := range values {
			values[i] = prng.Int63()
		}
		for b.Loop() {
			maxInt64(values)
		}
	})
}

func BenchmarkMaxUint32(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B, bufferSize int) {
		values := make([]uint32, bufferSize/4)
		prng := rand.New(rand.NewSource(1))
		for i := range values {
			values[i] = prng.Uint32()
		}
		for b.Loop() {
			maxUint32(values)
		}
	})
}

func BenchmarkMaxUint64(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B, bufferSize int) {
		values := make([]uint64, bufferSize/8)
		prng := rand.New(rand.NewSource(1))
		for i := range values {
			values[i] = prng.Uint64()
		}
		for b.Loop() {
			maxUint64(values)
		}
	})
}

func BenchmarkMaxFloat32(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B, bufferSize int) {
		values := make([]float32, bufferSize/4)
		prng := rand.New(rand.NewSource(1))
		for i := range values {
			values[i] = prng.Float32()
		}
		for b.Loop() {
			maxFloat32(values)
		}
	})
}

func BenchmarkMaxFloat64(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B, bufferSize int) {
		values := make([]float64, bufferSize/8)
		prng := rand.New(rand.NewSource(1))
		for i := range values {
			values[i] = prng.Float64()
		}
		for b.Loop() {
			maxFloat64(values)
		}
	})
}

func BenchmarkMaxBE128(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B, bufferSize int) {
		values := make([][16]byte, bufferSize)
		prng := rand.New(rand.NewSource(1))
		for i := range values {
			prng.Read(values[i][:])
		}
		for b.Loop() {
			maxBE128(values)
		}
	})
}

func BenchmarkMaxFixedLenByteArray(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B, bufferSize int) {
		values := make([]byte, bufferSize)
		prng := rand.New(rand.NewSource(1))
		prng.Read(values)
		for b.Loop() {
			maxFixedLenByteArray(values, 32)
		}
	})
}
