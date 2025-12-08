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
	slice *slice[T] // non-nil if data came from pool (used to return to pool on Reset)
	data  []T       // the active slice (always used for access)
}

const (
	minBucketBits = 10 // 1024 bytes
	maxBucketBits = 23 // 8 MiB
	numBuckets    = maxBucketBits - minBucketBits + 1
)

var slicePools [numBuckets]Pool[slice[byte]]

// SliceBufferFrom creates a SliceBuffer that wraps an existing slice without copying.
// The buffer takes ownership of the slice and will not return it to any pool.
func SliceBufferFrom[T Datum](data []T) SliceBuffer[T] {
	return SliceBuffer[T]{data: data}
}

// SliceBufferFor creates a SliceBuffer with pre-allocated capacity for the given number of elements.
// The buffer will be backed by a pooled slice large enough to hold cap elements.
func SliceBufferFor[T Datum](cap int) SliceBuffer[T] {
	var buf SliceBuffer[T]
	buf.Grow(cap)
	return buf
}

// reserveMore ensures the buffer has capacity for at least one more element.
//
// This is moved to a separate function to keep the complexity cost of AppendValue
// within the inlining budget.
//
//go:noinline
func (b *SliceBuffer[T]) reserveMore() { b.reserve(1) }

// reserve ensures the buffer has capacity for at least count more elements.
// It handles transitioning from external data to pooled storage and growing when needed.
// Caller must check that len(b.data)+count > cap(b.data) before calling.
func (b *SliceBuffer[T]) reserve(count int) {
	elemSize := int(unsafe.Sizeof(*new(T)))
	requiredBytes := (len(b.data) + count) * elemSize
	if b.slice == nil {
		// Either empty or using external data
		bucketIndex := findBucket(requiredBytes)
		b.slice = getSliceFromPool[T](bucketIndex, elemSize)
		if b.data != nil {
			// Transition from external data to pooled storage
			b.slice.data = append(b.slice.data, b.data...)
		}
		b.data = b.slice.data
	} else {
		// Already using pooled storage, grow to new bucket
		oldSlice := b.slice
		bucketIndex := findBucket(requiredBytes)
		b.slice = getSliceFromPool[T](bucketIndex, elemSize)
		b.slice.data = append(b.slice.data, b.data...)
		oldSlice.data = b.data // Sync before returning to pool
		putSliceToPool(oldSlice, elemSize)
		b.data = b.slice.data
	}
}

// Append adds data to the buffer, growing the slice as needed by promoting to
// larger pool buckets.
func (b *SliceBuffer[T]) Append(data ...T) {
	if len(b.data)+len(data) > cap(b.data) {
		b.reserve(len(data))
	}
	b.data = append(b.data, data...)
}

// AppendValue appends a single value to the buffer.
func (b *SliceBuffer[T]) AppendValue(value T) {
	if len(b.data) == cap(b.data)-1 {
		b.reserveMore()
	}
	b.data = append(b.data, value)
}

// Reset returns the slice to its pool and resets the buffer to empty.
func (b *SliceBuffer[T]) Reset() {
	if b.slice != nil {
		b.slice.data = b.data // Sync before returning to pool
		elemSize := int(unsafe.Sizeof(*new(T)))
		putSliceToPool(b.slice, elemSize)
		b.slice = nil
	}
	b.data = nil
}

// Cap returns the current capacity.
func (b *SliceBuffer[T]) Cap() int { return cap(b.data) }

// Len returns the number of elements currently in the buffer.
func (b *SliceBuffer[T]) Len() int { return len(b.data) }

// Slice returns a view of the current data.
// The returned slice is only valid until the next call to Append or Reset.
func (b *SliceBuffer[T]) Slice() []T { return b.data }

// Swap swaps the elements at indices i and j.
func (b *SliceBuffer[T]) Swap(i, j int) {
	b.data[i], b.data[j] = b.data[j], b.data[i]
}

// Less reports whether the element at index i is less than the element at index j.
func (b *SliceBuffer[T]) Less(i, j int) bool {
	s := b.data
	return s[i] < s[j]
}

// Grow ensures the buffer has capacity for at least n more elements.
func (b *SliceBuffer[T]) Grow(n int) {
	if n > 0 && len(b.data)+n > cap(b.data) {
		b.reserve(n)
	}
}

// Clone creates a copy of the buffer with its own pooled allocation.
// The cloned buffer is allocated from the pool with exactly the right size.
func (b *SliceBuffer[T]) Clone() SliceBuffer[T] {
	if len(b.data) == 0 {
		return SliceBuffer[T]{}
	}

	elemSize := int(unsafe.Sizeof(*new(T)))
	requiredBytes := len(b.data) * elemSize
	bucketIndex := findBucket(requiredBytes)

	cloned := SliceBuffer[T]{
		slice: getSliceFromPool[T](bucketIndex, elemSize),
	}
	cloned.slice.data = append(cloned.slice.data, b.data...)
	cloned.data = cloned.slice.data
	return cloned
}

// Resize changes the length of the buffer to size, growing capacity if needed.
// If size is larger than the current length, the new elements contain uninitialized data.
// If size is smaller, the buffer is truncated.
func (b *SliceBuffer[T]) Resize(size int) {
	if size <= len(b.data) {
		b.data = b.data[:size]
	} else {
		if size > cap(b.data) {
			b.reserve(size - len(b.data))
		}
		b.data = b.data[:size]
	}
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

var _ SliceBuffer[byte]
