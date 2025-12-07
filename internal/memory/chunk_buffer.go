package memory

import (
	"unsafe"
)

// Datum is a constraint for types that can be stored in chunk and slice buffers.
// It includes the common numeric types used in parquet files.
type Datum interface {
	~byte | ~int32 | ~int64 | ~uint32 | ~uint64 | ~float32 | ~float64
}

// chunkSize is the size in bytes of each chunk (64 KiB).
const chunkSize = 64 * 1024

type chunk [chunkSize]byte

// ChunkBuffer is a buffer that stores data in fixed-size chunks.
// Chunks are allocated lazily on demand and reused via a global pool.
// This design minimizes memory fragmentation and provides predictable memory usage.
type ChunkBuffer[T Datum] struct {
	chunks []*chunk
	length int
}

var chunkPool Pool[chunk]

// Append adds data to the buffer, allocating new chunks as needed.
func (b *ChunkBuffer[T]) Append(data ...T) {
	capacity := int(chunkSize / unsafe.Sizeof(*new(T)))
	for len(data) > 0 {
		if len(b.chunks) == 0 || (b.length&(capacity-1)) == 0 {
			b.chunks = append(b.chunks, chunkPool.Get(
				func() *chunk { return new(chunk) },
				func(*chunk) {},
			))
		}

		currentChunk := b.chunks[len(b.chunks)-1]
		chunkData := (*T)(unsafe.Pointer(&currentChunk[0]))
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
	for i := range b.chunks {
		chunkPool.Put(b.chunks[i])
	}
	clear(b.chunks)
	b.chunks = b.chunks[:0]
	b.length = 0
}

// Len returns the number of elements currently in the buffer.
func (b *ChunkBuffer[T]) Len() int { return b.length }

// Chunks returns an iterator over the chunks in the buffer.
// Each chunk is yielded as a slice containing the valid data.
func (b *ChunkBuffer[T]) Chunks(yield func([]T) bool) {
	capacity := int(chunkSize / unsafe.Sizeof(*new(T)))
	remaining := b.length
	for _, c := range b.chunks {
		if remaining == 0 {
			break
		}
		chunkData := (*T)(unsafe.Pointer(&c[0]))
		chunkSize := min(remaining, capacity)
		typeChunk := unsafe.Slice(chunkData, chunkSize)
		if !yield(typeChunk) {
			return
		}
		remaining -= chunkSize
	}
}
