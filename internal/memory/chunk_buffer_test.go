package memory

import (
	"testing"
)

const testChunkSize = 64 * 1024 // 64 KiB for tests

func TestChunkBufferEmpty(t *testing.T) {
	buf := ChunkBufferFor[byte](testChunkSize)
	if buf.Len() != 0 {
		t.Errorf("expected length 0, got %d", buf.Len())
	}

	count := 0
	for range buf.Chunks {
		count++
	}
	if count != 0 {
		t.Errorf("expected 0 chunks, got %d", count)
	}
}

func TestChunkBufferAppendSingle(t *testing.T) {
	buf := ChunkBufferFor[byte](testChunkSize)
	buf.Append(42)

	if buf.Len() != 1 {
		t.Errorf("expected length 1, got %d", buf.Len())
	}

	count := 0
	for chunk := range buf.Chunks {
		count++
		if len(chunk) != 1 {
			t.Errorf("expected chunk length 1, got %d", len(chunk))
		}
		if chunk[0] != 42 {
			t.Errorf("expected chunk[0] == 42, got %d", chunk[0])
		}
	}
	if count != 1 {
		t.Errorf("expected 1 chunk, got %d", count)
	}
}

func TestChunkBufferAppendMultiple(t *testing.T) {
	buf := ChunkBufferFor[int32](testChunkSize)
	data := []int32{1, 2, 3, 4, 5}
	buf.Append(data...)

	if buf.Len() != len(data) {
		t.Errorf("expected length %d, got %d", len(data), buf.Len())
	}

	result := []int32{}
	for chunk := range buf.Chunks {
		result = append(result, chunk...)
	}

	if len(result) != len(data) {
		t.Fatalf("expected %d elements, got %d", len(data), len(result))
	}
	for i, v := range data {
		if result[i] != v {
			t.Errorf("index %d: expected %d, got %d", i, v, result[i])
		}
	}
}

func TestChunkBufferAcrossChunkBoundary(t *testing.T) {
	buf := ChunkBufferFor[byte](testChunkSize)

	chunkCap := testChunkSize
	firstChunk := make([]byte, chunkCap)
	for i := range firstChunk {
		firstChunk[i] = byte(i % 256)
	}
	buf.Append(firstChunk...)

	buf.Append(255)

	if buf.Len() != chunkCap+1 {
		t.Errorf("expected length %d, got %d", chunkCap+1, buf.Len())
	}

	chunkCount := 0
	totalLen := 0
	for chunk := range buf.Chunks {
		chunkCount++
		totalLen += len(chunk)
	}

	if chunkCount != 2 {
		t.Errorf("expected 2 chunks, got %d", chunkCount)
	}
	if totalLen != chunkCap+1 {
		t.Errorf("expected total length %d, got %d", chunkCap+1, totalLen)
	}
}

func TestChunkBufferMultipleChunks(t *testing.T) {
	buf := ChunkBufferFor[int64](testChunkSize)

	chunkCap := testChunkSize / 8
	totalElements := chunkCap*2 + chunkCap/2
	for i := range totalElements {
		buf.Append(int64(i))
	}

	if buf.Len() != totalElements {
		t.Errorf("expected length %d, got %d", totalElements, buf.Len())
	}

	chunkCount := 0
	elementCount := 0
	for chunk := range buf.Chunks {
		chunkCount++
		for _, v := range chunk {
			if v != int64(elementCount) {
				t.Errorf("element %d: expected %d, got %d", elementCount, elementCount, v)
			}
			elementCount++
		}
	}

	if chunkCount != 3 {
		t.Errorf("expected 3 chunks, got %d", chunkCount)
	}
	if elementCount != totalElements {
		t.Errorf("expected %d total elements, got %d", totalElements, elementCount)
	}
}

func TestChunkBufferReset(t *testing.T) {
	buf := ChunkBufferFor[uint32](testChunkSize)

	buf.Append(1, 2, 3, 4, 5)
	if buf.Len() != 5 {
		t.Fatalf("expected length 5 before reset, got %d", buf.Len())
	}

	buf.Reset()

	if buf.Len() != 0 {
		t.Errorf("expected length 0 after reset, got %d", buf.Len())
	}

	chunkCount := 0
	for range buf.Chunks {
		chunkCount++
	}
	if chunkCount != 0 {
		t.Errorf("expected 0 chunks after reset, got %d", chunkCount)
	}

	buf.Append(10, 20, 30)
	if buf.Len() != 3 {
		t.Errorf("expected length 3 after reset and append, got %d", buf.Len())
	}
}

func TestChunkBufferEmptyAppend(t *testing.T) {
	buf := ChunkBufferFor[float32](testChunkSize)
	buf.Append()

	if buf.Len() != 0 {
		t.Errorf("expected length 0 after empty append, got %d", buf.Len())
	}
}

func TestChunkBufferDifferentTypes(t *testing.T) {
	tests := []struct {
		name string
		test func(*testing.T)
	}{
		{"byte", func(t *testing.T) { testChunkBufferType[byte](t, []byte{1, 2, 3}) }},
		{"int32", func(t *testing.T) { testChunkBufferType[int32](t, []int32{-1, 0, 1}) }},
		{"int64", func(t *testing.T) { testChunkBufferType[int64](t, []int64{-1000, 0, 1000}) }},
		{"uint32", func(t *testing.T) { testChunkBufferType[uint32](t, []uint32{0, 100, 1000}) }},
		{"uint64", func(t *testing.T) { testChunkBufferType[uint64](t, []uint64{0, 100, 1000}) }},
		{"float32", func(t *testing.T) { testChunkBufferType[float32](t, []float32{-1.5, 0.0, 1.5}) }},
		{"float64", func(t *testing.T) { testChunkBufferType[float64](t, []float64{-1.5, 0.0, 1.5}) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

func testChunkBufferType[T Datum](t *testing.T, data []T) {
	t.Helper()
	buf := ChunkBufferFor[T](testChunkSize)
	buf.Append(data...)

	if buf.Len() != len(data) {
		t.Errorf("expected length %d, got %d", len(data), buf.Len())
	}

	result := []T{}
	for chunk := range buf.Chunks {
		result = append(result, chunk...)
	}

	if len(result) != len(data) {
		t.Fatalf("expected %d elements, got %d", len(data), len(result))
	}

	for i := range data {
		if result[i] != data[i] {
			t.Errorf("index %d: expected %v, got %v", i, data[i], result[i])
		}
	}
}

func TestChunkBufferLargeAppend(t *testing.T) {
	buf := ChunkBufferFor[byte](testChunkSize)

	largeData := make([]byte, testChunkSize*5+100)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}
	buf.Append(largeData...)

	if buf.Len() != len(largeData) {
		t.Errorf("expected length %d, got %d", len(largeData), buf.Len())
	}

	result := []byte{}
	for chunk := range buf.Chunks {
		result = append(result, chunk...)
	}

	if len(result) != len(largeData) {
		t.Fatalf("expected %d bytes, got %d", len(largeData), len(result))
	}

	for i := range largeData {
		if result[i] != largeData[i] {
			t.Errorf("index %d: expected %d, got %d", i, largeData[i], result[i])
		}
	}
}

func TestChunkBufferPoolReuse(t *testing.T) {
	buffers := make([]ChunkBuffer[int32], 10)
	for i := range buffers {
		buffers[i] = ChunkBufferFor[int32](testChunkSize)
		data := make([]int32, testChunkSize/4+100)
		buffers[i].Append(data...)
	}

	for i := range buffers {
		buffers[i].Reset()
	}

	newBuf := ChunkBufferFor[int32](testChunkSize)
	data := make([]int32, testChunkSize/4+100)
	for i := range data {
		data[i] = int32(i)
	}
	newBuf.Append(data...)

	result := []int32{}
	for chunk := range newBuf.Chunks {
		result = append(result, chunk...)
	}

	if len(result) != len(data) {
		t.Fatalf("expected %d elements, got %d", len(data), len(result))
	}
	for i := range data {
		if result[i] != data[i] {
			t.Errorf("index %d: expected %d, got %d", i, data[i], result[i])
		}
	}
}

func BenchmarkChunkBufferAppendSmall(b *testing.B) {
	buf := ChunkBufferFor[byte](testChunkSize)
	data := []byte{1, 2, 3, 4, 5}
	for i := range b.N {
		buf.Append(data...)
		if i&1023 == 1023 {
			buf.Reset()
		}
	}
}

func BenchmarkChunkBufferAppendLarge(b *testing.B) {
	buf := ChunkBufferFor[int64](testChunkSize)
	data := make([]int64, 10000)
	for i := range b.N {
		buf.Append(data...)
		if i&15 == 15 {
			buf.Reset()
		}
	}
}

func BenchmarkChunkBufferChunks(b *testing.B) {
	buf := ChunkBufferFor[byte](testChunkSize)
	data := make([]byte, testChunkSize*10)
	buf.Append(data...)

	for b.Loop() {
		for range buf.Chunks {
		}
	}
}

func BenchmarkChunkBufferReset(b *testing.B) {
	buf := ChunkBufferFor[int32](testChunkSize)
	data := make([]int32, testChunkSize)
	buf.Append(data...)

	for b.Loop() {
		buf.Reset()
		buf.Append(data...)
	}
}
