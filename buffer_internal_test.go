package parquet

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestBufferAlwaysCorrectSize(t *testing.T) {
	var p bufferPool[byte]
	for range 1000 {
		n := rand.Intn(1024 * 1024)
		b := p.get(n)
		if len(b.data) != n {
			t.Fatalf("Expected buffer of size %d, got %d", n, len(b.data))
		}
		b.unref()
	}
}

func TestBufferPoolBucketIndexAndSizeOf(t *testing.T) {
	tests := []struct {
		size        int
		bucketIndex int
		bucketSize  int
	}{
		{size: 0, bucketIndex: 0, bucketSize: 4096},
		{size: 1, bucketIndex: 0, bucketSize: 4096},
		{size: 2049, bucketIndex: 0, bucketSize: 4096},
		{size: 4096, bucketIndex: 0, bucketSize: 4096},
		{size: 4097, bucketIndex: 1, bucketSize: 8192},
		{size: 8192, bucketIndex: 1, bucketSize: 8192},
		{size: 8193, bucketIndex: 2, bucketSize: 16384},
		{size: 16384, bucketIndex: 2, bucketSize: 16384},
		{size: 16385, bucketIndex: 3, bucketSize: 32768},
		{size: 32768, bucketIndex: 3, bucketSize: 32768},
		{size: 32769, bucketIndex: 4, bucketSize: 65536},
		{size: 262143, bucketIndex: 6, bucketSize: 262144},
		{size: 262144, bucketIndex: 6, bucketSize: 262144},
		{size: 262145, bucketIndex: 7, bucketSize: 393216},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("size=%d", test.size), func(t *testing.T) {
			bucketIndex, bucketSize := bufferPoolBucketIndexAndSizeOfGet(test.size)

			if bucketIndex != test.bucketIndex {
				t.Errorf("wrong bucket index, want %d but got %d", test.bucketIndex, bucketIndex)
			}

			if bucketSize != test.bucketSize {
				t.Errorf("wrong bucket size, want %d but got %d", test.bucketSize, bucketSize)
			}
		})
	}
}
