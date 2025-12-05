package parquet_test

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"testing/iotest"

	"github.com/parquet-go/parquet-go"
)

func TestBufferPool(t *testing.T) {
	testBufferPool(t, parquet.NewBufferPool())
}

func TestChunkBufferPool(t *testing.T) {
	// Test various chunk sizes to exercise different code paths
	chunkSizes := []int{
		1,           // Tiny chunks - extreme edge case
		2,           // Very small chunks
		3,           // Odd size
		10,          // Small chunks
		64,          // Cache line size
		100,         // Medium small
		256,         // Common small buffer
		1024,        // 1KB
		4096,        // Page size
		8192,        // Common buffer size
		65536,       // 64KB
		256 * 1024,  // 256KB (default)
		1024 * 1024, // 1MB
	}

	for _, size := range chunkSizes {
		t.Run(fmt.Sprintf("chunk_size=%d", size), func(t *testing.T) {
			testBufferPool(t, parquet.NewChunkBufferPool(size))
		})
	}
}

func TestFileBufferPool(t *testing.T) {
	testBufferPool(t, parquet.NewFileBufferPool("/tmp", "buffers.*"))
}

func testBufferPool(t *testing.T, pool parquet.BufferPool) {
	tests := []struct {
		scenario string
		function func(*testing.T, parquet.BufferPool)
	}{
		{
			scenario: "write bytes",
			function: testBufferPoolWriteBytes,
		},

		{
			scenario: "write string",
			function: testBufferPoolWriteString,
		},

		{
			scenario: "copy to buffer",
			function: testBufferPoolCopyToBuffer,
		},

		{
			scenario: "copy from buffer",
			function: testBufferPoolCopyFromBuffer,
		},
	}

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) { test.function(t, pool) })
	}
}

func testBufferPoolWriteBytes(t *testing.T, pool parquet.BufferPool) {
	const content = "Hello World!"

	buffer := pool.GetBuffer()
	defer pool.PutBuffer(buffer)

	_, err := buffer.Write([]byte(content))
	if err != nil {
		t.Fatal(err)
	}
	assertBufferContent(t, buffer, content)
}

func testBufferPoolWriteString(t *testing.T, pool parquet.BufferPool) {
	const content = "Hello World!"

	buffer := pool.GetBuffer()
	defer pool.PutBuffer(buffer)

	_, err := io.WriteString(buffer, content)
	if err != nil {
		t.Fatal(err)
	}

	assertBufferContent(t, buffer, content)
}

func testBufferPoolCopyToBuffer(t *testing.T, pool parquet.BufferPool) {
	const content = "ABC"

	buffer := pool.GetBuffer()
	defer pool.PutBuffer(buffer)

	reader := strings.NewReader(content)
	_, err := io.Copy(buffer, struct{ io.Reader }{reader})
	if err != nil {
		t.Fatal(err)
	}

	assertBufferContent(t, buffer, content)
}

func testBufferPoolCopyFromBuffer(t *testing.T, pool parquet.BufferPool) {
	const content = "0123456789"

	buffer := pool.GetBuffer()
	defer pool.PutBuffer(buffer)

	if _, err := io.WriteString(buffer, content); err != nil {
		t.Fatal(err)
	}
	if _, err := buffer.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}

	writer := new(bytes.Buffer)
	_, err := io.Copy(struct{ io.Writer }{writer}, buffer)
	if err != nil {
		t.Fatal(err)
	}

	assertBufferContent(t, bytes.NewReader(writer.Bytes()), content)
}

func assertBufferContent(t *testing.T, b io.ReadSeeker, s string) {
	t.Helper()

	offset, err := b.Seek(0, io.SeekStart)
	if err != nil {
		t.Error("seek:", err)
	}
	if offset != 0 {
		t.Errorf("seek: invalid offset returned: want=0 got=%d", offset)
	}
	if err := iotest.TestReader(b, []byte(s)); err != nil {
		t.Error("iotest:", err)
	}
}

// Stress tests for buffer pool

func TestBufferPoolStress(t *testing.T) {
	testBufferPoolStress(t, parquet.NewBufferPool())
}

func TestChunkBufferPoolStress(t *testing.T) {
	// Test stress scenarios with various chunk sizes
	chunkSizes := []int{
		1,          // Extreme: 1 byte chunks
		10,         // Very small chunks
		100,        // Small chunks
		1024,       // 1KB chunks
		4096,       // 4KB chunks
		65536,      // 64KB chunks
		256 * 1024, // 256KB chunks (default)
	}

	for _, size := range chunkSizes {
		t.Run(fmt.Sprintf("chunk_size=%d", size), func(t *testing.T) {
			testBufferPoolStress(t, parquet.NewChunkBufferPool(size))
		})
	}
}

func testBufferPoolStress(t *testing.T, pool parquet.BufferPool) {
	tests := []struct {
		scenario string
		function func(*testing.T, parquet.BufferPool)
	}{
		{
			scenario: "large sequential writes",
			function: testBufferPoolLargeSequentialWrites,
		},
		{
			scenario: "large random access",
			function: testBufferPoolLargeRandomAccess,
		},
		{
			scenario: "many small writes",
			function: testBufferPoolManySmallWrites,
		},
		{
			scenario: "interleaved read write",
			function: testBufferPoolInterleavedReadWrite,
		},
		{
			scenario: "buffer reuse",
			function: testBufferPoolReuse,
		},
		{
			scenario: "seek boundaries",
			function: testBufferPoolSeekBoundaries,
		},
		{
			scenario: "partial reads",
			function: testBufferPoolPartialReads,
		},
		{
			scenario: "overwrite existing data",
			function: testBufferPoolOverwriteExistingData,
		},
		{
			scenario: "write after seek",
			function: testBufferPoolWriteAfterSeek,
		},
		{
			scenario: "concurrent access",
			function: testBufferPoolConcurrentAccess,
		},
		{
			scenario: "multi-megabyte writes",
			function: testBufferPoolMultiMegabyteWrites,
		},
		{
			scenario: "chunk boundary crossing",
			function: testBufferPoolChunkBoundaryCrossing,
		},
	}

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) { test.function(t, pool) })
	}
}

func testBufferPoolLargeSequentialWrites(t *testing.T, pool parquet.BufferPool) {
	buffer := pool.GetBuffer()
	defer pool.PutBuffer(buffer)

	// Write 10MB of sequential data
	const chunkSize = 100 * 1024 // 100KiB chunks
	const numChunks = 10
	expected := make([]byte, 0, chunkSize*numChunks)

	for i := range numChunks {
		chunk := make([]byte, chunkSize)
		for j := range chunk {
			chunk[j] = byte((i*chunkSize + j) % 256)
		}
		expected = append(expected, chunk...)

		n, err := buffer.Write(chunk)
		if err != nil {
			t.Fatalf("write chunk %d failed: %v", i, err)
		}
		if n != chunkSize {
			t.Fatalf("write chunk %d: expected %d bytes, got %d", i, chunkSize, n)
		}
	}

	// Verify the data
	if _, err := buffer.Seek(0, io.SeekStart); err != nil {
		t.Fatal("seek failed:", err)
	}

	actual := make([]byte, len(expected))
	n, err := io.ReadFull(buffer, actual)
	if err != nil {
		t.Fatal("read failed:", err)
	}
	if n != len(expected) {
		t.Fatalf("expected to read %d bytes, got %d", len(expected), n)
	}

	if !bytes.Equal(expected, actual) {
		t.Fatal("data mismatch after large sequential writes")
	}
}

func testBufferPoolLargeRandomAccess(t *testing.T, pool parquet.BufferPool) {
	buffer := pool.GetBuffer()
	defer pool.PutBuffer(buffer)

	// Write 5MB of data
	const size = 5 * 1024 * 1024
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 256)
	}

	if _, err := buffer.Write(data); err != nil {
		t.Fatal("write failed:", err)
	}

	// Perform random seeks and reads
	testPositions := []int64{0, size / 4, size / 2, 3 * size / 4, size - 100}

	for _, pos := range testPositions {
		offset, err := buffer.Seek(pos, io.SeekStart)
		if err != nil {
			t.Fatalf("seek to %d failed: %v", pos, err)
		}
		if offset != pos {
			t.Fatalf("seek to %d returned offset %d", pos, offset)
		}

		readBuf := make([]byte, 100)
		n, err := buffer.Read(readBuf)
		if err != nil && err != io.EOF {
			t.Fatalf("read at position %d failed: %v", pos, err)
		}
		if n > 0 && !bytes.Equal(readBuf[:n], data[pos:pos+int64(n)]) {
			t.Fatalf("data mismatch at position %d", pos)
		}
	}
}

func testBufferPoolManySmallWrites(t *testing.T, pool parquet.BufferPool) {
	buffer := pool.GetBuffer()
	defer pool.PutBuffer(buffer)

	// Write 10000 small chunks of varying sizes
	const numWrites = 10000
	var expected bytes.Buffer

	for i := range numWrites {
		size := (i % 100) + 1 // 1 to 100 bytes
		chunk := make([]byte, size)
		for j := range chunk {
			chunk[j] = byte((i + j) % 256)
		}
		expected.Write(chunk)

		n, err := buffer.Write(chunk)
		if err != nil {
			t.Fatalf("write %d failed: %v", i, err)
		}
		if n != len(chunk) {
			t.Fatalf("write %d: expected %d bytes, got %d", i, len(chunk), n)
		}
	}

	// Verify all data
	if _, err := buffer.Seek(0, io.SeekStart); err != nil {
		t.Fatal("seek failed:", err)
	}

	actual, err := io.ReadAll(buffer)
	if err != nil {
		t.Fatal("read all failed:", err)
	}

	if !bytes.Equal(expected.Bytes(), actual) {
		t.Fatal("data mismatch after many small writes")
	}
}

func testBufferPoolInterleavedReadWrite(t *testing.T, pool parquet.BufferPool) {
	buffer := pool.GetBuffer()
	defer pool.PutBuffer(buffer)

	// Write initial data
	initial := []byte("AAAAAAAAAA")
	if _, err := buffer.Write(initial); err != nil {
		t.Fatal("initial write failed:", err)
	}

	// Seek back and read part of it
	if _, err := buffer.Seek(0, io.SeekStart); err != nil {
		t.Fatal("seek failed:", err)
	}
	readBuf := make([]byte, 5)
	if _, err := io.ReadFull(buffer, readBuf); err != nil {
		t.Fatal("read failed:", err)
	}
	if !bytes.Equal(readBuf, []byte("AAAAA")) {
		t.Fatalf("expected AAAAA, got %s", readBuf)
	}

	// Write more data at current position
	more := []byte("BBBBBBBBBB")
	if _, err := buffer.Write(more); err != nil {
		t.Fatal("second write failed:", err)
	}

	// Verify final state
	if _, err := buffer.Seek(0, io.SeekStart); err != nil {
		t.Fatal("seek failed:", err)
	}
	final, err := io.ReadAll(buffer)
	if err != nil {
		t.Fatal("final read failed:", err)
	}

	expected := []byte("AAAAABBBBBBBBBB")
	if !bytes.Equal(expected, final) {
		t.Fatalf("expected %s, got %s", expected, final)
	}
}

func testBufferPoolReuse(t *testing.T, pool parquet.BufferPool) {
	// Get and use a buffer
	buffer1 := pool.GetBuffer()
	data1 := []byte("first buffer data")
	if _, err := buffer1.Write(data1); err != nil {
		t.Fatal("write to buffer1 failed:", err)
	}
	pool.PutBuffer(buffer1)

	// Get another buffer (might be the same one reused)
	buffer2 := pool.GetBuffer()
	defer pool.PutBuffer(buffer2)

	// It should be reset/empty
	if _, err := buffer2.Seek(0, io.SeekStart); err != nil {
		t.Fatal("seek failed:", err)
	}

	data2 := []byte("second buffer data")
	if _, err := buffer2.Write(data2); err != nil {
		t.Fatal("write to buffer2 failed:", err)
	}

	// Verify buffer2 contains only new data
	if _, err := buffer2.Seek(0, io.SeekStart); err != nil {
		t.Fatal("seek failed:", err)
	}
	actual, err := io.ReadAll(buffer2)
	if err != nil {
		t.Fatal("read failed:", err)
	}

	if !bytes.Equal(data2, actual) {
		t.Fatalf("buffer not properly reset: expected %s, got %s", data2, actual)
	}
}

func testBufferPoolSeekBoundaries(t *testing.T, pool parquet.BufferPool) {
	buffer := pool.GetBuffer()
	defer pool.PutBuffer(buffer)

	data := []byte("0123456789ABCDEF")
	if _, err := buffer.Write(data); err != nil {
		t.Fatal("write failed:", err)
	}

	tests := []struct {
		offset int64
		whence int
		want   int64
	}{
		{0, io.SeekStart, 0},
		{5, io.SeekStart, 5},
		{int64(len(data)), io.SeekStart, int64(len(data))},
		{-5, io.SeekEnd, int64(len(data)) - 5},
		{0, io.SeekEnd, int64(len(data))},
		{-int64(len(data)), io.SeekEnd, 0},
		{3, io.SeekCurrent, int64(len(data))}, // seek beyond EOF is clamped to EOF
		{-10, io.SeekCurrent, int64(len(data)) - 10},
	}

	for i, tt := range tests {
		// Reset to known position for SeekCurrent tests
		if tt.whence == io.SeekCurrent && i > 5 {
			if _, err := buffer.Seek(int64(len(data)), io.SeekStart); err != nil {
				t.Fatalf("test %d: reset seek failed: %v", i, err)
			}
		}

		got, err := buffer.Seek(tt.offset, tt.whence)
		if err != nil {
			t.Fatalf("test %d: seek(%d, %d) failed: %v", i, tt.offset, tt.whence, err)
		}
		if got != tt.want {
			t.Fatalf("test %d: seek(%d, %d) = %d, want %d", i, tt.offset, tt.whence, got, tt.want)
		}
	}

	// Test negative seek (should error)
	if _, err := buffer.Seek(-1, io.SeekStart); err == nil {
		t.Fatal("negative seek should have failed")
	}
}

func testBufferPoolPartialReads(t *testing.T, pool parquet.BufferPool) {
	buffer := pool.GetBuffer()
	defer pool.PutBuffer(buffer)

	data := make([]byte, 1000)
	for i := range data {
		data[i] = byte(i % 256)
	}
	if _, err := buffer.Write(data); err != nil {
		t.Fatal("write failed:", err)
	}

	if _, err := buffer.Seek(0, io.SeekStart); err != nil {
		t.Fatal("seek failed:", err)
	}

	// Read in various chunk sizes
	// Note: Read() may return less than requested, so we use io.ReadFull
	chunkSizes := []int{1, 7, 13, 50, 100, 200}
	pos := 0

	for _, size := range chunkSizes {
		if pos >= len(data) {
			break
		}
		readSize := size
		if pos+size > len(data) {
			readSize = len(data) - pos
		}

		chunk := make([]byte, readSize)
		n, err := io.ReadFull(buffer, chunk)
		if err != nil {
			t.Fatalf("read failed at position %d: %v", pos, err)
		}
		if n != readSize {
			t.Fatalf("expected to read %d bytes at position %d, got %d", readSize, pos, n)
		}
		if !bytes.Equal(chunk, data[pos:pos+readSize]) {
			t.Fatalf("data mismatch at position %d", pos)
		}
		pos += readSize
	}
}

func testBufferPoolOverwriteExistingData(t *testing.T, pool parquet.BufferPool) {
	buffer := pool.GetBuffer()
	defer pool.PutBuffer(buffer)

	// Write initial data
	initial := []byte("XXXXXXXXXXXXXXXXXXXX")
	if _, err := buffer.Write(initial); err != nil {
		t.Fatal("initial write failed:", err)
	}

	// Seek to middle and overwrite
	if _, err := buffer.Seek(5, io.SeekStart); err != nil {
		t.Fatal("seek failed:", err)
	}

	overwrite := []byte("YYYY")
	if _, err := buffer.Write(overwrite); err != nil {
		t.Fatal("overwrite failed:", err)
	}

	// Write more to extend
	extend := []byte("ZZZZZZ")
	if _, err := buffer.Write(extend); err != nil {
		t.Fatal("extend write failed:", err)
	}

	// Verify final content
	if _, err := buffer.Seek(0, io.SeekStart); err != nil {
		t.Fatal("final seek failed:", err)
	}
	final, err := io.ReadAll(buffer)
	if err != nil {
		t.Fatal("final read failed:", err)
	}

	expected := []byte("XXXXXYYYYZZZZZZXXXXX")
	if !bytes.Equal(expected, final) {
		t.Fatalf("expected %s, got %s", expected, final)
	}
}

func testBufferPoolWriteAfterSeek(t *testing.T, pool parquet.BufferPool) {
	buffer := pool.GetBuffer()
	defer pool.PutBuffer(buffer)

	// Write initial data
	if _, err := buffer.Write([]byte("0123456789")); err != nil {
		t.Fatal("initial write failed:", err)
	}

	// Seek to various positions and write
	seekWrites := []struct {
		pos  int64
		data string
	}{
		{0, "AAA"},
		{3, "BBB"},
		{6, "CCC"},
	}

	for _, sw := range seekWrites {
		if _, err := buffer.Seek(sw.pos, io.SeekStart); err != nil {
			t.Fatalf("seek to %d failed: %v", sw.pos, err)
		}
		if _, err := buffer.Write([]byte(sw.data)); err != nil {
			t.Fatalf("write at %d failed: %v", sw.pos, err)
		}
	}

	// Verify result
	if _, err := buffer.Seek(0, io.SeekStart); err != nil {
		t.Fatal("final seek failed:", err)
	}
	result, err := io.ReadAll(buffer)
	if err != nil {
		t.Fatal("final read failed:", err)
	}

	expected := []byte("AAABBBCCC9")
	if !bytes.Equal(expected, result) {
		t.Fatalf("expected %s, got %s", expected, result)
	}
}

func testBufferPoolConcurrentAccess(t *testing.T, pool parquet.BufferPool) {
	const numGoroutines = 10
	const writesPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for g := range numGoroutines {
		go func(id int) {
			defer wg.Done()

			buffer := pool.GetBuffer()
			defer pool.PutBuffer(buffer)

			// Each goroutine writes its own data
			for i := range writesPerGoroutine {
				data := fmt.Appendf(nil, "goroutine-%d-write-%d\n", id, i)
				if _, err := buffer.Write(data); err != nil {
					t.Errorf("goroutine %d write %d failed: %v", id, i, err)
					return
				}
			}

			// Verify by reading back
			if _, err := buffer.Seek(0, io.SeekStart); err != nil {
				t.Errorf("goroutine %d seek failed: %v", id, err)
				return
			}

			for i := range writesPerGoroutine {
				expected := fmt.Appendf(nil, "goroutine-%d-write-%d\n", id, i)
				actual := make([]byte, len(expected))
				if _, err := io.ReadFull(buffer, actual); err != nil {
					t.Errorf("goroutine %d read %d failed: %v", id, i, err)
					return
				}
				if !bytes.Equal(expected, actual) {
					t.Errorf("goroutine %d data mismatch at write %d", id, i)
					return
				}
			}
		}(g)
	}

	wg.Wait()
}

func testBufferPoolMultiMegabyteWrites(t *testing.T, pool parquet.BufferPool) {
	buffer := pool.GetBuffer()
	defer pool.PutBuffer(buffer)

	// Write 500KB of data
	const totalSize = 500 * 1024
	const chunkSize = 50 * 1024

	written := 0
	for written < totalSize {
		size := chunkSize
		if written+size > totalSize {
			size = totalSize - written
		}

		chunk := make([]byte, size)
		// Fill with deterministic pattern
		for i := range chunk {
			chunk[i] = byte((written + i) % 256)
		}

		n, err := buffer.Write(chunk)
		if err != nil {
			t.Fatalf("write at offset %d failed: %v", written, err)
		}
		if n != size {
			t.Fatalf("write at offset %d: expected %d bytes, got %d", written, size, n)
		}
		written += n
	}

	// Verify size by seeking to end
	pos, err := buffer.Seek(0, io.SeekEnd)
	if err != nil {
		t.Fatal("seek to end failed:", err)
	}
	if pos != totalSize {
		t.Fatalf("expected size %d, got %d", totalSize, pos)
	}

	// Spot check some data
	checkPositions := []int64{0, totalSize / 4, totalSize / 2, 3 * totalSize / 4, totalSize - 1000}
	for _, checkPos := range checkPositions {
		if _, err := buffer.Seek(checkPos, io.SeekStart); err != nil {
			t.Fatalf("seek to %d failed: %v", checkPos, err)
		}

		readBuf := make([]byte, 1000)
		n, err := buffer.Read(readBuf)
		if err != nil && err != io.EOF {
			t.Fatalf("read at %d failed: %v", checkPos, err)
		}

		// Verify pattern
		for i := range n {
			expected := byte((checkPos + int64(i)) % 256)
			if readBuf[i] != expected {
				t.Fatalf("data mismatch at position %d+%d: expected %d, got %d",
					checkPos, i, expected, readBuf[i])
			}
		}
	}
}

func testBufferPoolChunkBoundaryCrossing(t *testing.T, pool parquet.BufferPool) {
	buffer := pool.GetBuffer()
	defer pool.PutBuffer(buffer)

	// Write data that will definitely cross chunk boundaries
	// ChunkBufferPool uses 256KB chunks by default
	const chunkSize = 256 * 1024

	// Write data around chunk boundaries
	testSizes := []int{
		chunkSize - 100,
		chunkSize,
		chunkSize + 100,
		chunkSize * 2,
		chunkSize*2 + 500,
	}

	for _, size := range testSizes {
		// Reset buffer for each test
		buffer = pool.GetBuffer()

		data := make([]byte, size)
		if _, err := rand.Read(data); err != nil {
			t.Fatal("failed to generate random data:", err)
		}

		// Write the data
		n, err := buffer.Write(data)
		if err != nil {
			t.Fatalf("write of size %d failed: %v", size, err)
		}
		if n != size {
			t.Fatalf("write of size %d: expected %d bytes, got %d", size, size, n)
		}

		// Read it back
		if _, err := buffer.Seek(0, io.SeekStart); err != nil {
			t.Fatal("seek failed:", err)
		}

		readData := make([]byte, size)
		n, err = io.ReadFull(buffer, readData)
		if err != nil {
			t.Fatalf("read of size %d failed: %v", size, err)
		}
		if n != size {
			t.Fatalf("read of size %d: expected %d bytes, got %d", size, size, n)
		}

		if !bytes.Equal(data, readData) {
			t.Fatalf("data mismatch for size %d", size)
		}

		pool.PutBuffer(buffer)
	}
}
