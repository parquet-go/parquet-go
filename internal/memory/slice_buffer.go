package memory

import (
	"math/bits"
	"unsafe"

	"github.com/parquet-go/parquet-go/internal/unsafecast"
)

// slice is a wrapper around a slice to enable pooling.
type slice[T Datum] struct {
	data []T
}

// SliceBuffer is a buffer that stores data in a single contiguous slice.
// The slice grows by moving to larger size buckets from pools as needed.
// This design provides efficient sequential access and minimal overhead for small datasets.
type SliceBuffer[T Datum] struct {
	slice *slice[T]
}

const (
	minBucketBits = 10 // 1024 bytes
	maxBucketBits = 23 // 8 MiB
	numBuckets    = maxBucketBits - minBucketBits + 1
)

var slicePools [numBuckets]Pool[slice[byte]]

// Append adds data to the buffer, growing the slice as needed by promoting to larger pool buckets.
func (b *SliceBuffer[T]) Append(data ...T) {
	if len(data) == 0 {
		return
	}

	elemSize := int(unsafe.Sizeof(*new(T)))
	requiredBytes := (b.Len() + len(data)) * elemSize

	if b.slice == nil {
		bucketIndex := findBucket(requiredBytes)
		b.slice = getSliceFromPool[T](bucketIndex, elemSize)
	}

	if requiredBytes > cap(b.slice.data)*elemSize {
		oldSlice := b.slice
		bucketIndex := findBucket(requiredBytes)
		b.slice = getSliceFromPool[T](bucketIndex, elemSize)
		b.slice.data = append(b.slice.data, oldSlice.data...)
		putSliceToPool(oldSlice, elemSize)
	}

	b.slice.data = append(b.slice.data, data...)
}

// Reset returns the slice to its pool and resets the buffer to empty.
func (b *SliceBuffer[T]) Reset() {
	if b.slice != nil {
		elemSize := int(unsafe.Sizeof(*new(T)))
		putSliceToPool(b.slice, elemSize)
		b.slice = nil
	}
}

// Len returns the number of elements currently in the buffer.
func (b *SliceBuffer[T]) Len() int {
	if b.slice == nil {
		return 0
	}
	return len(b.slice.data)
}

// Slice returns a view of the current data.
// The returned slice is only valid until the next call to Append or Reset.
func (b *SliceBuffer[T]) Slice() []T {
	if b.slice == nil {
		return nil
	}
	return b.slice.data
}

func findBucket(requiredBytes int) int {
	if requiredBytes <= 0 {
		return 0
	}
	bitLen := bits.Len(uint(requiredBytes - 1))
	if bitLen < minBucketBits {
		return 0
	}
	bucketIndex := bitLen - minBucketBits
	if bucketIndex >= numBuckets {
		return numBuckets - 1
	}
	return bucketIndex
}

func bucketSize(bucketIndex int) int {
	return 1 << (minBucketBits + bucketIndex)
}

func getSliceFromPool[T Datum](bucketIndex int, elemSize int) *slice[T] {
	byteSlice := slicePools[bucketIndex].Get(
		func() *slice[byte] {
			size := bucketSize(bucketIndex)
			return &slice[byte]{
				data: make([]byte, 0, size),
			}
		},
		func(s *slice[byte]) {
			s.data = s.data[:0]
		},
	)

	typeSlice := (*slice[T])(unsafe.Pointer(byteSlice))
	typeSlice.data = unsafecast.Slice[T](byteSlice.data)
	return typeSlice
}

func putSliceToPool[T Datum](s *slice[T], elemSize int) {
	if s == nil || s.data == nil {
		return
	}

	byteLen := cap(s.data) * elemSize
	bucketIndex := findBucket(byteLen)

	// Verify the bucket size matches exactly (safety check)
	if bucketSize(bucketIndex) != byteLen {
		return
	}

	byteSlice := (*slice[byte])(unsafe.Pointer(s))
	byteSlice.data = unsafecast.Slice[byte](s.data)
	slicePools[bucketIndex].Put(byteSlice)
}
