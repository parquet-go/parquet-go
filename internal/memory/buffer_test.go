package memory

import (
	"bytes"
	"io"
	"testing"
	"testing/iotest"
)

const testBufferChunkSize = 64 * 1024 // 64 KiB

func TestBufferReadWriter(t *testing.T) {
	buf := NewBuffer(testBufferChunkSize)

	// Test basic write
	data := []byte("Hello, World!")
	n, err := buf.Write(data)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != len(data) {
		t.Errorf("Write returned %d, want %d", n, len(data))
	}

	// Seek to start
	pos, err := buf.Seek(0, io.SeekStart)
	if err != nil {
		t.Fatalf("Seek failed: %v", err)
	}
	if pos != 0 {
		t.Errorf("Seek returned %d, want 0", pos)
	}

	// Read back
	result := make([]byte, len(data))
	n, err = buf.Read(result)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if n != len(data) {
		t.Errorf("Read returned %d, want %d", n, len(data))
	}
	if !bytes.Equal(result, data) {
		t.Errorf("Read returned %q, want %q", result, data)
	}
}

func TestBufferSeek(t *testing.T) {
	buf := NewBuffer(testBufferChunkSize)
	data := []byte("0123456789")
	buf.Write(data)

	tests := []struct {
		offset int64
		whence int
		want   int64
	}{
		{0, io.SeekStart, 0},
		{5, io.SeekStart, 5},
		{-2, io.SeekEnd, 8},
		{2, io.SeekCurrent, 10},
		{-5, io.SeekCurrent, 5},
		{100, io.SeekStart, 10}, // Clamped to end
		{-100, io.SeekStart, 0}, // Negative clamped to 0
	}

	for _, tt := range tests {
		got, err := buf.Seek(tt.offset, tt.whence)
		if tt.offset < 0 && tt.whence == io.SeekStart {
			if err == nil {
				t.Errorf("Seek(%d, %d) expected error, got position %d", tt.offset, tt.whence, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("Seek(%d, %d) failed: %v", tt.offset, tt.whence, err)
			continue
		}
		if got != tt.want {
			t.Errorf("Seek(%d, %d) = %d, want %d", tt.offset, tt.whence, got, tt.want)
		}
	}
}

func TestBufferWriteTo(t *testing.T) {
	buf := NewBuffer(testBufferChunkSize)
	data := []byte("Hello, World!")
	buf.Write(data)
	buf.Seek(0, io.SeekStart)

	var out bytes.Buffer
	n, err := buf.WriteTo(&out)
	if err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}
	if n != int64(len(data)) {
		t.Errorf("WriteTo wrote %d bytes, want %d", n, len(data))
	}
	if !bytes.Equal(out.Bytes(), data) {
		t.Errorf("WriteTo wrote %q, want %q", out.Bytes(), data)
	}
}

func TestBufferAcrossChunks(t *testing.T) {
	chunkSize := 16
	buf := NewBuffer(chunkSize)

	// Write data that spans multiple chunks
	data := make([]byte, chunkSize*3+5)
	for i := range data {
		data[i] = byte(i % 256)
	}

	n, err := buf.Write(data)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != len(data) {
		t.Errorf("Write returned %d, want %d", n, len(data))
	}

	// Read back
	buf.Seek(0, io.SeekStart)
	result := make([]byte, len(data))
	n, err = io.ReadFull(buf, result)
	if err != nil {
		t.Fatalf("ReadFull failed: %v", err)
	}
	if n != len(data) {
		t.Errorf("ReadFull returned %d, want %d", n, len(data))
	}
	if !bytes.Equal(result, data) {
		t.Errorf("Data mismatch after reading across chunks")
	}
}

func TestBufferOverwrite(t *testing.T) {
	buf := NewBuffer(testBufferChunkSize)

	// Write initial data
	buf.Write([]byte("Hello, World!"))

	// Seek to middle and overwrite
	buf.Seek(7, io.SeekStart)
	buf.Write([]byte("Go!!!!"))

	// Read back full content
	buf.Seek(0, io.SeekStart)
	result := make([]byte, 13)
	io.ReadFull(buf, result)

	expected := []byte("Hello, Go!!!!")
	if !bytes.Equal(result, expected) {
		t.Errorf("Overwrite result = %q, want %q", result, expected)
	}
}

func TestBufferReset(t *testing.T) {
	buf := NewBuffer(testBufferChunkSize)

	// Write some data
	buf.Write([]byte("test data"))

	// Reset
	buf.Reset()

	// Verify buffer is empty
	if buf.seek != 0 {
		t.Errorf("After Reset, seek = %d, want 0", buf.seek)
	}
	if buf.data.Len() != 0 {
		t.Errorf("After Reset, length = %d, want 0", buf.data.Len())
	}

	// Should be able to write again
	newData := []byte("new data")
	buf.Write(newData)
	buf.Seek(0, io.SeekStart)
	result := make([]byte, len(newData))
	io.ReadFull(buf, result)
	if !bytes.Equal(result, newData) {
		t.Errorf("After Reset and Write, got %q, want %q", result, newData)
	}
}

func TestBufferEOF(t *testing.T) {
	buf := NewBuffer(testBufferChunkSize)
	data := []byte("test")
	buf.Write(data)
	buf.Seek(0, io.SeekStart)

	// Read all data
	result := make([]byte, len(data))
	n, err := io.ReadFull(buf, result)
	if err != nil {
		t.Fatalf("ReadFull failed: %v", err)
	}
	if n != len(data) {
		t.Errorf("ReadFull returned %d, want %d", n, len(data))
	}

	// Next read should return EOF
	var tmp [1]byte
	n, err = buf.Read(tmp[:])
	if err != io.EOF {
		t.Errorf("Read after end returned err=%v, want io.EOF", err)
	}
	if n != 0 {
		t.Errorf("Read after end returned n=%d, want 0", n)
	}
}

func TestBufferTestReader(t *testing.T) {
	buf := NewBuffer(testBufferChunkSize)
	data := []byte("The quick brown fox jumps over the lazy dog")
	buf.Write(data)
	buf.Seek(0, io.SeekStart)

	// Use iotest.TestReader to verify Reader implementation
	err := iotest.TestReader(buf, data)
	if err != nil {
		t.Errorf("iotest.TestReader failed: %v", err)
	}
}

func TestBufferOneByteReader(t *testing.T) {
	buf := NewBuffer(testBufferChunkSize)
	data := []byte("Hello, World!")
	buf.Write(data)
	buf.Seek(0, io.SeekStart)

	// Read one byte at a time
	reader := iotest.OneByteReader(buf)
	result := make([]byte, len(data))
	n, err := io.ReadFull(reader, result)
	if err != nil {
		t.Fatalf("OneByteReader ReadFull failed: %v", err)
	}
	if n != len(data) {
		t.Errorf("OneByteReader ReadFull returned %d, want %d", n, len(data))
	}
	if !bytes.Equal(result, data) {
		t.Errorf("OneByteReader result = %q, want %q", result, data)
	}
}

func TestBufferHalfReader(t *testing.T) {
	buf := NewBuffer(testBufferChunkSize)
	data := []byte("Hello, World!")
	buf.Write(data)
	buf.Seek(0, io.SeekStart)

	// Read half at a time
	reader := iotest.HalfReader(buf)
	result, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("HalfReader ReadAll failed: %v", err)
	}
	if !bytes.Equal(result, data) {
		t.Errorf("HalfReader result = %q, want %q", result, data)
	}
}

func TestBufferEmptyWrites(t *testing.T) {
	buf := NewBuffer(testBufferChunkSize)

	n, err := buf.Write(nil)
	if err != nil {
		t.Errorf("Write(nil) failed: %v", err)
	}
	if n != 0 {
		t.Errorf("Write(nil) = %d, want 0", n)
	}

	n, err = buf.Write([]byte{})
	if err != nil {
		t.Errorf("Write([]) failed: %v", err)
	}
	if n != 0 {
		t.Errorf("Write([]) = %d, want 0", n)
	}
}

func TestBufferEmptyReads(t *testing.T) {
	buf := NewBuffer(testBufferChunkSize)
	buf.Write([]byte("test"))

	n, err := buf.Read(nil)
	if err != nil {
		t.Errorf("Read(nil) failed: %v", err)
	}
	if n != 0 {
		t.Errorf("Read(nil) = %d, want 0", n)
	}

	n, err = buf.Read([]byte{})
	if err != nil {
		t.Errorf("Read([]) failed: %v", err)
	}
	if n != 0 {
		t.Errorf("Read([]) = %d, want 0", n)
	}
}

func BenchmarkBufferWrite(b *testing.B) {
	buf := NewBuffer(testBufferChunkSize)
	data := make([]byte, 1024)

	for b.Loop() {
		buf.Write(data)
		if b.N%100 == 99 {
			buf.Reset()
		}
	}
}

func BenchmarkBufferRead(b *testing.B) {
	buf := NewBuffer(testBufferChunkSize)
	data := make([]byte, 1024*100)
	buf.Write(data)

	readBuf := make([]byte, 1024)

	for b.Loop() {
		buf.Seek(0, io.SeekStart)
		for {
			_, err := buf.Read(readBuf)
			if err == io.EOF {
				break
			}
		}
	}
}

func BenchmarkBufferSeek(b *testing.B) {
	buf := NewBuffer(testBufferChunkSize)
	data := make([]byte, 1024*100)
	buf.Write(data)

	b.ResetTimer()
	for i := range b.N {
		offset := int64(i % len(data))
		buf.Seek(offset, io.SeekStart)
	}
}
