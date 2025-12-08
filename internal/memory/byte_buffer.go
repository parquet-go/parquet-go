package memory

import (
	"fmt"
	"io"
)

// ByteBuffer is a buffer that stores bytes in fixed-size chunks and implements io.ReadWriteSeeker.
// It uses ChunkBuffer[byte] internally for chunk management.
type ByteBuffer struct {
	chunks ChunkBuffer[byte]
	idx    int // current chunk index for read/write operations
	off    int // offset within current chunk
}

// NewByteBuffer creates a new ByteBuffer with the given chunk size.
func NewByteBuffer(chunkSize int) *ByteBuffer {
	return &ByteBuffer{
		chunks: ChunkBufferFor[byte](chunkSize),
	}
}

// Reset returns all chunks to the pool and resets the buffer to empty.
func (b *ByteBuffer) Reset() {
	b.chunks.Reset()
	b.idx = 0
	b.off = 0
}

// Read implements io.Reader.
func (b *ByteBuffer) Read(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	if b.idx >= b.chunks.NumChunks() {
		return 0, io.EOF
	}

	curData := b.chunks.Chunk(b.idx)

	if b.idx == b.chunks.NumChunks()-1 && b.off == len(curData) {
		return 0, io.EOF
	}

	n = copy(p, curData[b.off:])
	b.off += n

	if b.off == len(curData) && b.idx < b.chunks.NumChunks()-1 {
		b.idx++
		b.off = 0
	}

	return n, nil
}

// Write implements io.Writer.
func (b *ByteBuffer) Write(p []byte) (int, error) {
	lenP := len(p)

	if lenP == 0 {
		return 0, nil
	}

	chunkCap := b.chunks.chunkSize
	for len(p) > 0 {
		// Allocate new chunk if needed
		if b.idx == b.chunks.NumChunks() {
			// Append dummy byte to allocate chunk, we'll overwrite it
			b.chunks.Append(0)
		}

		curData := b.chunks.ChunkCap(b.idx)
		n := copy(curData[b.off:], p)

		// Update buffer length if we extended it
		currentPos := b.idx*chunkCap + b.off + n
		if currentPos > b.chunks.Len() {
			b.chunks.SetLen(currentPos)
		}

		b.off += n
		p = p[n:]
		if b.off >= chunkCap {
			b.idx++
			b.off = 0
		}
	}

	return lenP, nil
}

// WriteTo implements io.WriterTo.
func (b *ByteBuffer) WriteTo(w io.Writer) (int64, error) {
	var numWritten int64
	var err error
	for err == nil && b.idx < b.chunks.NumChunks() {
		curData := b.chunks.Chunk(b.idx)
		n, e := w.Write(curData[b.off:])
		numWritten += int64(n)
		b.off += n
		err = e
		if b.idx == b.chunks.NumChunks()-1 {
			break
		}
		b.idx++
		b.off = 0
	}
	return numWritten, err
}

// Seek implements io.Seeker.
func (b *ByteBuffer) Seek(offset int64, whence int) (int64, error) {
	// Because this is the common case, we check it first to avoid computing endOff.
	if offset == 0 && whence == io.SeekStart {
		b.idx = 0
		b.off = 0
		return offset, nil
	}
	endOff := b.endOff()
	switch whence {
	case io.SeekCurrent:
		offset += b.currentOff()
	case io.SeekEnd:
		offset += endOff
	}
	if offset < 0 {
		return 0, fmt.Errorf("seek: negative offset: %d<0", offset)
	}
	if offset > endOff {
		offset = endOff
	}
	// Repeat this case now that we know the absolute offset. This is a bit faster, but
	// mainly protects us from an out-of-bounds if b.chunks is empty. (If the buffer is
	// empty and the absolute offset isn't zero, we'd have errored (if negative) or
	// clamped to zero (if positive) above.
	if offset == 0 {
		b.idx = 0
		b.off = 0
	} else if b.chunks.NumChunks() > 0 {
		stride := b.chunks.chunkSize
		b.idx = int(offset) / stride
		b.off = int(offset) % stride
	}
	return offset, nil
}

func (b *ByteBuffer) currentOff() int64 {
	if b.idx == 0 {
		return int64(b.off)
	}
	return int64(b.idx*b.chunks.chunkSize + b.off)
}

func (b *ByteBuffer) endOff() int64 {
	return int64(b.chunks.Len())
}
