package memory

import (
	"unsafe"
)

// Datum is a constraint for types that can be stored in chunk and slice buffers.
// It includes the common numeric types used in parquet files.
type Datum interface {
	~byte | ~int32 | ~int64 | ~uint32 | ~uint64 | ~float32 | ~float64
}

// ChunkBuffer is a buffer that stores data in fixed-size chunks.
// Chunks are allocated lazily on demand and reused via slice pools.
// The chunk size is rounded up to the nearest power of two to utilize slice buffers fully.
// This design minimizes memory fragmentation and provides predictable memory usage.
type ChunkBuffer[T Datum] struct {
	chunks    []*slice[byte]
	chunkSize int // in bytes, always a power of 2
	length    int
}

// ChunkBufferFor creates a new ChunkBuffer with the given chunk size (in bytes).
// The chunk size will be rounded up to the nearest power of two.
func ChunkBufferFor[T Datum](chunkSize int) ChunkBuffer[T] {
	// Round up to nearest power of 2 that fits in our bucket system
	bucketIndex := findBucket(chunkSize)
	return ChunkBuffer[T]{
		chunkSize: bucketSize(bucketIndex),
	}
}

// Append adds data to the buffer, allocating new chunks as needed.
func (b *ChunkBuffer[T]) Append(data ...T) {
	elemSize := int(unsafe.Sizeof(*new(T)))
	capacity := b.chunkSize / elemSize
	for len(data) > 0 {
		if len(b.chunks) == 0 || (b.length&(capacity-1)) == 0 {
			bucketIndex := findBucket(b.chunkSize)
			chunk := slicePools[bucketIndex].Get(
				func() *slice[byte] {
					return &slice[byte]{
						data: make([]byte, 0, b.chunkSize),
					}
				},
				func(s *slice[byte]) {
					s.data = s.data[:0]
				},
			)
			b.chunks = append(b.chunks, chunk)
		}

		currentChunk := b.chunks[len(b.chunks)-1]
		chunkData := (*T)(unsafe.Pointer(unsafe.SliceData(currentChunk.data)))
		typeChunk := unsafe.Slice(chunkData, capacity)
		offset := b.length & (capacity - 1)
		available := capacity - offset
		toWrite := min(len(data), available)

		copy(typeChunk[offset:], data[:toWrite])
		b.length += toWrite
		data = data[toWrite:]
	}
}

// Reset returns all chunks to the pool and resets the buffer to empty.
func (b *ChunkBuffer[T]) Reset() {
	if len(b.chunks) > 0 {
		bucketIndex := findBucket(b.chunkSize)
		for i := range b.chunks {
			slicePools[bucketIndex].Put(b.chunks[i])
			b.chunks[i] = nil
		}
		b.chunks = b.chunks[:0]
	}
	b.length = 0
}

// Len returns the number of elements currently in the buffer.
func (b *ChunkBuffer[T]) Len() int { return b.length }

// NumChunks returns the number of chunks currently allocated.
func (b *ChunkBuffer[T]) NumChunks() int { return len(b.chunks) }

// Chunk returns the data for the chunk at the given index.
// The caller must ensure idx < NumChunks().
func (b *ChunkBuffer[T]) Chunk(idx int) []T {
	elemSize := int(unsafe.Sizeof(*new(T)))
	capacity := b.chunkSize / elemSize
	chunk := b.chunks[idx]
	chunkData := (*T)(unsafe.Pointer(unsafe.SliceData(chunk.data)))
	// For the last chunk, only return valid elements
	remaining := b.length - idx*capacity
	chunkSize := min(remaining, capacity)
	return unsafe.Slice(chunkData, chunkSize)
}

// ChunkCap returns the full capacity view of a chunk at the given index.
// This allows writing beyond the current valid length.
// The caller must ensure idx < NumChunks().
func (b *ChunkBuffer[T]) ChunkCap(idx int) []T {
	elemSize := int(unsafe.Sizeof(*new(T)))
	capacity := b.chunkSize / elemSize
	chunk := b.chunks[idx]
	chunkData := (*T)(unsafe.Pointer(unsafe.SliceData(chunk.data)))
	return unsafe.Slice(chunkData, capacity)
}

// SetLen sets the length of the buffer to the given value.
// This is used by ByteBuffer after writing to chunks directly.
func (b *ChunkBuffer[T]) SetLen(length int) {
	b.length = length
}

// Chunks returns an iterator over the chunks in the buffer.
// Each chunk is yielded as a slice containing the valid data.
func (b *ChunkBuffer[T]) Chunks(yield func([]T) bool) {
	elemSize := int(unsafe.Sizeof(*new(T)))
	capacity := b.chunkSize / elemSize
	remaining := b.length
	for _, c := range b.chunks {
		if remaining == 0 {
			break
		}
		chunkData := (*T)(unsafe.Pointer(unsafe.SliceData(c.data)))
		chunkSize := min(remaining, capacity)
		typeChunk := unsafe.Slice(chunkData, chunkSize)
		if !yield(typeChunk) {
			return
		}
		remaining -= chunkSize
	}
}
