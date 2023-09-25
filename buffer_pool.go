package parquet

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// BufferPool is an interface abstracting the underlying implementation of
// page buffer pools.
//
// The parquet-go package provides two implementations of this interface, one
// backed by in-memory buffers (on the Go heap), and the other using temporary
// files on disk.
//
// Applications which need finer grain control over the allocation and retention
// of page buffers may choose to provide their own implementation and install it
// via the parquet.ColumnPageBuffers writer option.
//
// BufferPool implementations must be safe to use concurrently from multiple
// goroutines.
type BufferPool interface {
	// GetBuffer is called when a parquet writer needs to acquire a new
	// page buffer from the pool.
	GetBuffer() io.ReadWriteSeeker

	// PutBuffer is called when a parquet writer releases a page buffer to
	// the pool.
	//
	// The parquet.Writer type guarantees that the buffers it calls this method
	// with were previously acquired by a call to GetBuffer on the same
	// pool, and that it will not use them anymore after the call.
	PutBuffer(io.ReadWriteSeeker)
}

// NewBufferPool creates a new in-memory page buffer pool.
//
// The implementation is backed by sync.Pool and allocates memory buffers on the
// Go heap.
func NewBufferPool() BufferPool { return new(memoryBufferPool) }

type memoryBuffer struct {
	data []byte
	off  int
}

func (p *memoryBuffer) Reset() {
	p.data, p.off = p.data[:0], 0
}

func (p *memoryBuffer) Read(b []byte) (n int, err error) {
	n = copy(b, p.data[p.off:])
	p.off += n
	if p.off == len(p.data) {
		err = io.EOF
	}
	return n, err
}

func (p *memoryBuffer) Write(b []byte) (int, error) {
	n := copy(p.data[p.off:cap(p.data)], b)
	p.data = p.data[:p.off+n]

	if n < len(b) {
		p.data = append(p.data, b[n:]...)
	}

	p.off += len(b)
	return len(b), nil
}

func (p *memoryBuffer) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write(p.data[p.off:])
	p.off += n
	return int64(n), err
}

func (p *memoryBuffer) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekCurrent:
		offset += int64(p.off)
	case io.SeekEnd:
		offset += int64(len(p.data))
	}
	if offset < 0 {
		return 0, fmt.Errorf("seek: negative offset: %d<0", offset)
	}
	if offset > int64(len(p.data)) {
		offset = int64(len(p.data))
	}
	p.off = int(offset)
	return offset, nil
}

type memoryBufferPool struct{ sync.Pool }

func (pool *memoryBufferPool) GetBuffer() io.ReadWriteSeeker {
	b, _ := pool.Get().(*memoryBuffer)
	if b == nil {
		b = new(memoryBuffer)
	} else {
		b.Reset()
	}
	return b
}

func (pool *memoryBufferPool) PutBuffer(buf io.ReadWriteSeeker) {
	if b, _ := buf.(*memoryBuffer); b != nil {
		pool.Put(b)
	}
}

// NewChunkBufferPool creates a new in-memory page buffer pool.
//
// The implementation is backed by sync.Pool and allocates memory buffers on the
// Go heap in fixed-size chunks.
func NewChunkBufferPool(chunkSize int) BufferPool {
	return newChunkMemoryBufferPool(chunkSize)
}

func newChunkMemoryBufferPool(chunkSize int) *chunkMemoryBufferPool {
	pool := &chunkMemoryBufferPool{}
	pool.bytesPool.New = func() any {
		return make([]byte, chunkSize)
	}
	return pool
}

// chunkMemoryBuffer implements an io.ReadWriteSeeker by storing a slice of fixed-size
// buffers into which it copies data. (It uses a sync.Pool to reuse buffers across
// instances.)
// This should be a good fit for the default io.ReadWriteSeeker implementation to use for
// in memory column buffers, but for some reason copying into these buffers is much slower
// than copying into the buffers allocated by allocMemoryBuffer, so that is used as the
// default instead.
type chunkMemoryBuffer struct {
	bytesPool *sync.Pool

	data [][]byte
	idx  int
	off  int
}

func (p *chunkMemoryBuffer) Reset() {
	// Return the bytes to the bytesPool?
	for i := range p.data {
		p.bytesPool.Put(p.data[i])
	}
	for i := range p.data {
		p.data[i] = nil
	}
	p.data, p.idx, p.off = p.data[:0], 0, 0
}

func (p *chunkMemoryBuffer) Read(b []byte) (n int, err error) {

	if len(b) == 0 {
		return 0, nil
	}

	if p.idx >= len(p.data) {
		return 0, io.EOF
	}

	curData := p.data[p.idx]

	if p.idx == len(p.data)-1 && p.off == len(curData) {
		return 0, io.EOF
	}

	n = copy(b, curData[p.off:])
	p.off += n

	if p.off == cap(curData) {
		p.idx++
		p.off = 0
	}

	return n, err
}

func (p *chunkMemoryBuffer) Write(b []byte) (int, error) {

	lenB := len(b)

	if lenB == 0 {
		return 0, nil
	}

	for len(b) > 0 {
		if p.idx == len(p.data) {
			p.data = append(p.data, p.bytesPool.Get().([]byte)[:0])
		}
		curData := p.data[p.idx]
		n := copy(curData[p.off:cap(curData)], b)
		p.data[p.idx] = curData[:p.off+n]
		p.off += n
		b = b[n:]
		if p.off >= cap(curData) {
			p.idx++
			p.off = 0
		}
	}

	return lenB, nil
}

func (p *chunkMemoryBuffer) WriteTo(w io.Writer) (int64, error) {
	var numWritten int64
	var err error
	for err == nil {
		curData := p.data[p.idx]
		n, e := w.Write(curData[p.off:])
		numWritten += int64(n)
		err = e
		if p.idx == len(p.data)-1 {
			p.off = int(numWritten)
			break
		}
		p.idx++
		p.off = 0
	}
	return numWritten, err
}

func (p *chunkMemoryBuffer) Seek(offset int64, whence int) (int64, error) {
	endOff := p.endOff()
	switch whence {
	case io.SeekCurrent:
		offset += p.currentOff()
	case io.SeekEnd:
		offset += endOff
	}
	if offset < 0 {
		return 0, fmt.Errorf("seek: negative offset: %d<0", offset)
	}
	if offset > endOff {
		offset = endOff
	}
	p.off = int(offset)
	return offset, nil
}

func (p *chunkMemoryBuffer) currentOff() int64 {
	if p.idx == 0 {
		return int64(p.off)
	}
	return int64((p.idx-1)*cap(p.data[0]) + p.off)
}

func (p *chunkMemoryBuffer) endOff() int64 {
	if len(p.data) == 0 {
		return 0
	}
	l := len(p.data)
	last := p.data[l-1]
	return int64(cap(last)*(l-1) + len(last))
}

type chunkMemoryBufferPool struct {
	sync.Pool
	bytesPool sync.Pool
}

func (pool *chunkMemoryBufferPool) GetBuffer() io.ReadWriteSeeker {
	b, _ := pool.Get().(*chunkMemoryBuffer)
	if b == nil {
		b = &chunkMemoryBuffer{bytesPool: &pool.bytesPool}
	} else {
		b.Reset()
	}
	return b
}

func (pool *chunkMemoryBufferPool) PutBuffer(buf io.ReadWriteSeeker) {
	if b, _ := buf.(*chunkMemoryBuffer); b != nil {
		for _, bytes := range b.data {
			b.bytesPool.Put(bytes)
		}
		for i := range b.data {
			b.data[i] = nil
		}
		b.data = b.data[:0]
		pool.Put(b)
	}
}

// allocMemoryBuffer implements an io.ReadWriteSeeker (well enough for the parquet.Writer)
// by allocating a new slice of bytes for each write and copying into it. (If a suitable
// slice can be found in the allocPool, that is used instead.)
// By all accounts, this should be worse than the chunkMemoryBuffer, which allocates
// larger fixed-size chunks, which results in fewer re/allocations, reads and writes into
// larger contiguous chunks of memory, and better buffer reuse through the pool. However,
// in practice, the copy call in chunkMemoryBuffer is much slower for unknown reasons, so
// this winds up being faster.
type allocMemoryBuffer struct {
	allocPool *sync.Pool
	data      [][]byte
	idx       int
	off       int
}

func (a *allocMemoryBuffer) Reset() {
	for _, buf := range a.data {
		a.allocPool.Put(buf)
	}
	for i := range a.data {
		a.data[i] = nil
	}
	a.data = a.data[:0]
	a.idx = 0
	a.off = 0
}

func (a *allocMemoryBuffer) Read(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}
	if a.idx >= len(a.data) {
		return 0, io.EOF
	}

	dataBuf := a.data[a.idx]
	n = copy(p, dataBuf[a.off:])
	a.off += n
	if a.off == len(a.data) {
		a.idx++
		a.off = 0
	}
	return n, nil
}

func (a *allocMemoryBuffer) WriteTo(w io.Writer) (n int64, err error) {
	for a.idx < len(a.data) {
		var numWritten int
		numWritten, err = w.Write(a.data[a.idx][a.off:])
		n += int64(numWritten)
		if err != nil {
			a.off = numWritten
			return n, err
		}
		a.idx++
	}
	return n, nil
}

func (a *allocMemoryBuffer) Write(p []byte) (n int, err error) {
	if a.idx != len(a.data) {
		panic("allocMemoryBuffer only supports appending to the end.")
	}

	buf := a.allocPool.Get().([]byte)[:0]
	buf = append(buf, p...)
	a.data = append(a.data, buf)
	a.idx++
	return len(p), nil
}

func (a *allocMemoryBuffer) Seek(offset int64, whence int) (int64, error) {
	// We only ever seek to the beginning, so we only implement for this case.
	if whence != io.SeekStart || offset != 0 {
		panic("allocMemoryBuffer only supports seeking to the start.")
	}
	a.idx = 0
	a.off = 0
	return 0, nil
}

type allocMemBufferPool struct {
	sync.Pool
	bufferPool sync.Pool
}

// newAllocMemBufferPool is not exposed because the allocMemoryBuffer is specialized to
// the parquet.Writer and does not implement all behavior to correctly satisfy the
// io.ReadWriteSeeker interface. (In particular, it does not support seeking except to
// the start, and it only supports appending.)
func newAllocMemBufferPool() *allocMemBufferPool {
	pool := allocMemBufferPool{}
	pool.bufferPool.New = func() any {
		return ([]byte)(nil)
	}
	return &pool
}

func (pool *allocMemBufferPool) GetBuffer() io.ReadWriteSeeker {
	b, _ := pool.Get().(*allocMemoryBuffer)
	if b == nil {
		b = new(allocMemoryBuffer)
		b.allocPool = &pool.bufferPool
	} else {
		b.Reset()
	}
	return b
}

func (pool *allocMemBufferPool) PutBuffer(buf io.ReadWriteSeeker) {
	if b, _ := buf.(*allocMemoryBuffer); b != nil {
		b.Reset()
		pool.Put(b)
	}
}

type fileBufferPool struct {
	err     error
	tempdir string
	pattern string
}

// NewFileBufferPool creates a new on-disk page buffer pool.
func NewFileBufferPool(tempdir, pattern string) BufferPool {
	pool := &fileBufferPool{
		tempdir: tempdir,
		pattern: pattern,
	}
	pool.tempdir, pool.err = filepath.Abs(pool.tempdir)
	return pool
}

func (pool *fileBufferPool) GetBuffer() io.ReadWriteSeeker {
	if pool.err != nil {
		return &errorBuffer{err: pool.err}
	}
	f, err := os.CreateTemp(pool.tempdir, pool.pattern)
	if err != nil {
		return &errorBuffer{err: err}
	}
	return f
}

func (pool *fileBufferPool) PutBuffer(buf io.ReadWriteSeeker) {
	if f, _ := buf.(*os.File); f != nil {
		defer f.Close()
		os.Remove(f.Name())
	}
}

type errorBuffer struct{ err error }

func (buf *errorBuffer) Read([]byte) (int, error)          { return 0, buf.err }
func (buf *errorBuffer) Write([]byte) (int, error)         { return 0, buf.err }
func (buf *errorBuffer) ReadFrom(io.Reader) (int64, error) { return 0, buf.err }
func (buf *errorBuffer) WriteTo(io.Writer) (int64, error)  { return 0, buf.err }
func (buf *errorBuffer) Seek(int64, int) (int64, error)    { return 0, buf.err }

var (
	defaultColumnBufferPool  = *newAllocMemBufferPool()
	defaultSortingBufferPool memoryBufferPool

	_ io.ReaderFrom      = (*errorBuffer)(nil)
	_ io.WriterTo        = (*errorBuffer)(nil)
	_ io.ReadWriteSeeker = (*memoryBuffer)(nil)
	_ io.WriterTo        = (*memoryBuffer)(nil)
	_ io.ReadWriteSeeker = (*chunkMemoryBuffer)(nil)
	_ io.WriterTo        = (*chunkMemoryBuffer)(nil)
	_ io.ReadWriteSeeker = (*allocMemoryBuffer)(nil)
	_ io.WriterTo        = (*allocMemoryBuffer)(nil)
)

type readerAt struct {
	reader io.ReadSeeker
	offset int64
}

func (r *readerAt) ReadAt(b []byte, off int64) (int, error) {
	if r.offset < 0 || off != r.offset {
		off, err := r.reader.Seek(off, io.SeekStart)
		if err != nil {
			return 0, err
		}
		r.offset = off
	}
	n, err := r.reader.Read(b)
	r.offset += int64(n)
	return n, err
}

func newReaderAt(r io.ReadSeeker) io.ReaderAt {
	if rr, ok := r.(io.ReaderAt); ok {
		return rr
	}
	return &readerAt{reader: r, offset: -1}
}
