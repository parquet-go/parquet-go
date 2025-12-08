package memory

import (
	"fmt"
	"io"
)

// ByteBuffer is a buffer that stores bytes in fixed-size chunks and implements io.ReadWriteSeeker.
// Chunks are allocated lazily on demand and reused via a pool.
type ByteBuffer struct {
	pool      *Pool[[]byte]
	chunks    []*[]byte
	chunkSize int
	idx       int // current chunk index for read/write operations
	off       int // offset within current chunk
}

// NewByteBuffer creates a new ByteBuffer with the given chunk size and pool.
func NewByteBuffer(chunkSize int, pool *Pool[[]byte]) *ByteBuffer {
	return &ByteBuffer{
		pool:      pool,
		chunkSize: chunkSize,
	}
}

// Reset returns all chunks to the pool and resets the buffer to empty.
func (b *ByteBuffer) Reset() {
	for i := range b.chunks {
		b.pool.Put(b.chunks[i])
		b.chunks[i] = nil
	}
	b.chunks = b.chunks[:0]
	b.idx = 0
	b.off = 0
}

// Read implements io.Reader.
func (b *ByteBuffer) Read(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	if b.idx >= len(b.chunks) {
		return 0, io.EOF
	}

	curData := *b.chunks[b.idx]

	if b.idx == len(b.chunks)-1 && b.off == len(curData) {
		return 0, io.EOF
	}

	n = copy(p, curData[b.off:])
	b.off += n

	if b.off == cap(curData) {
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

	for len(p) > 0 {
		if b.idx == len(b.chunks) {
			chunk := b.pool.Get(
				func() *[]byte {
					c := make([]byte, 0, b.chunkSize)
					return &c
				},
				func(c *[]byte) {
					*c = (*c)[:0]
				},
			)
			b.chunks = append(b.chunks, chunk)
		}
		curData := *b.chunks[b.idx]
		n := copy(curData[b.off:cap(curData)], p)

		// Only extend the slice if writing beyond current length
		newLen := b.off + n
		if newLen > len(curData) {
			*b.chunks[b.idx] = curData[:newLen]
		}

		b.off += n
		p = p[n:]
		if b.off >= cap(curData) {
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
	for err == nil && b.idx < len(b.chunks) {
		curData := *b.chunks[b.idx]
		n, e := w.Write(curData[b.off:])
		numWritten += int64(n)
		b.off += n
		err = e
		if b.idx == len(b.chunks)-1 {
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
	} else if len(b.chunks) > 0 {
		stride := cap(*b.chunks[0])
		b.idx = int(offset) / stride
		b.off = int(offset) % stride
	}
	return offset, nil
}

func (b *ByteBuffer) currentOff() int64 {
	if b.idx == 0 {
		return int64(b.off)
	}
	return int64(b.idx*cap(*b.chunks[0]) + b.off)
}

func (b *ByteBuffer) endOff() int64 {
	if len(b.chunks) == 0 {
		return 0
	}
	l := len(b.chunks)
	last := *b.chunks[l-1]
	return int64(cap(last)*(l-1) + len(last))
}
