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

// ensureCapacity ensures the buffer has at least the required capacity in bytes.
// It handles transitioning from external data to pooled storage and growing when needed.
func (b *SliceBuffer[T]) ensureCapacity(requiredBytes int) {
	elemSize := int(unsafe.Sizeof(*new(T)))

	if b.slice == nil {
		// Either empty or using external data
		bucketIndex := findBucket(requiredBytes)
		b.slice = getSliceFromPool[T](bucketIndex, elemSize)
		if b.data != nil {
			// Transition from external data to pooled storage
			b.slice.data = append(b.slice.data, b.data...)
		}
		b.data = b.slice.data
		return
	}

	// Already using pooled storage, check if we need to grow
	if requiredBytes > cap(b.data)*elemSize {
		oldSlice := b.slice
		bucketIndex := findBucket(requiredBytes)
		b.slice = getSliceFromPool[T](bucketIndex, elemSize)
		b.slice.data = append(b.slice.data, b.data...)
		putSliceToPool(oldSlice, elemSize)
		b.data = b.slice.data
	}
}

// Append adds data to the buffer, growing the slice as needed by promoting to larger pool buckets.
func (b *SliceBuffer[T]) Append(data ...T) {
	if len(data) == 0 {
		return
	}

	elemSize := int(unsafe.Sizeof(*new(T)))
	requiredBytes := (len(b.data) + len(data)) * elemSize
	b.ensureCapacity(requiredBytes)

	b.data = append(b.data, data...)
	if b.slice != nil {
		b.slice.data = b.data
	}
}

// AppendValue appends a single value to the buffer.
// This is more efficient than Append for single values as it avoids slice conversion overhead.
func (b *SliceBuffer[T]) AppendValue(value T) {
	elemSize := int(unsafe.Sizeof(*new(T)))
	requiredBytes := (len(b.data) + 1) * elemSize
	b.ensureCapacity(requiredBytes)

	b.data = append(b.data, value)
	if b.slice != nil {
		b.slice.data = b.data
	}
}

// Reset returns the slice to its pool and resets the buffer to empty.
func (b *SliceBuffer[T]) Reset() {
	if b.slice != nil {
		elemSize := int(unsafe.Sizeof(*new(T)))
		putSliceToPool(b.slice, elemSize)
		b.slice = nil
	}
	b.data = nil
}

// Len returns the number of elements currently in the buffer.
func (b *SliceBuffer[T]) Len() int {
	return len(b.data)
}

// Slice returns a view of the current data.
// The returned slice is only valid until the next call to Append or Reset.
func (b *SliceBuffer[T]) Slice() []T {
	return b.data
}

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
	if n <= 0 {
		return
	}

	elemSize := int(unsafe.Sizeof(*new(T)))
	requiredBytes := (len(b.data) + n) * elemSize
	b.ensureCapacity(requiredBytes)
}

// Cap returns the current capacity.
func (b *SliceBuffer[T]) Cap() int {
	return cap(b.data)
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
// If size is larger than the current length, the new elements are zero-initialized.
// If size is smaller, the buffer is truncated.
func (b *SliceBuffer[T]) Resize(size int) {
	if size < 0 {
		size = 0
	}

	currentLen := len(b.data)
	if size == currentLen {
		return
	}

	if size < currentLen {
		// Truncate
		b.data = b.data[:size]
		if b.slice != nil {
			b.slice.data = b.data
		}
		return
	}

	// Need to grow
	elemSize := int(unsafe.Sizeof(*new(T)))
	requiredBytes := size * elemSize
	b.ensureCapacity(requiredBytes)

	// Extend the slice to the new size and zero-initialize new elements
	oldLen := len(b.data)
	b.data = b.data[:size]
	// Zero-initialize the new elements
	for i := oldLen; i < size; i++ {
		b.data[i] = *new(T)
	}
	if b.slice != nil {
		b.slice.data = b.data
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
