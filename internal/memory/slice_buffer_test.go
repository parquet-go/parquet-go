package memory

import (
	"testing"
)

func TestSliceBufferEmpty(t *testing.T) {
	buf := new(SliceBuffer[byte])
	if buf.Len() != 0 {
		t.Errorf("expected length 0, got %d", buf.Len())
	}
	if buf.Slice() != nil {
		t.Errorf("expected nil slice, got %v", buf.Slice())
	}
}

func TestSliceBufferAppendSingle(t *testing.T) {
	buf := new(SliceBuffer[byte])
	buf.Append(42)

	if buf.Len() != 1 {
		t.Errorf("expected length 1, got %d", buf.Len())
	}

	slice := buf.Slice()
	if len(slice) != 1 {
		t.Errorf("expected slice length 1, got %d", len(slice))
	}
	if slice[0] != 42 {
		t.Errorf("expected slice[0] == 42, got %d", slice[0])
	}
}

func TestSliceBufferAppendValue(t *testing.T) {
	buf := new(SliceBuffer[int32])

	// Append multiple single values
	for i := range 10 {
		buf.AppendValue(int32(i * 10))
	}

	if buf.Len() != 10 {
		t.Errorf("expected length 10, got %d", buf.Len())
	}

	slice := buf.Slice()
	for i := range 10 {
		expected := int32(i * 10)
		if slice[i] != expected {
			t.Errorf("index %d: expected %d, got %d", i, expected, slice[i])
		}
	}
}

func TestSliceBufferAppendValueGrowth(t *testing.T) {
	buf := new(SliceBuffer[float64])

	// Append enough to trigger bucket growth
	for i := range 5000 {
		buf.AppendValue(float64(i))
	}

	if buf.Len() != 5000 {
		t.Errorf("expected length 5000, got %d", buf.Len())
	}

	slice := buf.Slice()
	for i := range 5000 {
		if slice[i] != float64(i) {
			t.Errorf("index %d: expected %f, got %f", i, float64(i), slice[i])
		}
	}
}

func TestSliceBufferAppendMultiple(t *testing.T) {
	buf := new(SliceBuffer[int32])
	data := []int32{1, 2, 3, 4, 5}
	buf.Append(data...)

	if buf.Len() != len(data) {
		t.Errorf("expected length %d, got %d", len(data), buf.Len())
	}

	slice := buf.Slice()
	if len(slice) != len(data) {
		t.Fatalf("expected slice length %d, got %d", len(data), len(slice))
	}

	for i, v := range data {
		if slice[i] != v {
			t.Errorf("index %d: expected %d, got %d", i, v, slice[i])
		}
	}
}

func TestSliceBufferGrowthAcrossBuckets(t *testing.T) {
	buf := new(SliceBuffer[byte])

	for _, size := range []int{100, 1000, 1024, 2048, 3000} {
		data := make([]byte, size)
		for i := range data {
			data[i] = byte(i % 256)
		}
		buf.Append(data...)

		if buf.Len() != size {
			t.Errorf("after appending %d bytes, expected length %d, got %d", size, size, buf.Len())
		}

		slice := buf.Slice()
		if len(slice) != size {
			t.Errorf("after appending %d bytes, expected slice length %d, got %d", size, size, len(slice))
		}

		buf.Reset()
	}
}

func TestSliceBufferGrowthPreservesData(t *testing.T) {
	buf := new(SliceBuffer[int64])

	expected := []int64{}
	for i := range 5000 {
		buf.Append(int64(i))
		expected = append(expected, int64(i))
	}

	if buf.Len() != len(expected) {
		t.Errorf("expected length %d, got %d", len(expected), buf.Len())
	}

	slice := buf.Slice()
	if len(slice) != len(expected) {
		t.Fatalf("expected slice length %d, got %d", len(expected), len(slice))
	}

	for i, v := range expected {
		if slice[i] != v {
			t.Errorf("index %d: expected %d, got %d", i, v, slice[i])
		}
	}
}

func TestSliceBufferReset(t *testing.T) {
	buf := new(SliceBuffer[uint32])

	buf.Append(1, 2, 3, 4, 5)
	if buf.Len() != 5 {
		t.Fatalf("expected length 5 before reset, got %d", buf.Len())
	}

	buf.Reset()

	if buf.Len() != 0 {
		t.Errorf("expected length 0 after reset, got %d", buf.Len())
	}
	if buf.Slice() != nil {
		t.Errorf("expected nil slice after reset, got %v", buf.Slice())
	}

	buf.Append(10, 20, 30)
	if buf.Len() != 3 {
		t.Errorf("expected length 3 after reset and append, got %d", buf.Len())
	}
}

func TestSliceBufferEmptyAppend(t *testing.T) {
	buf := new(SliceBuffer[float32])
	buf.Append()

	if buf.Len() != 0 {
		t.Errorf("expected length 0 after empty append, got %d", buf.Len())
	}
	if buf.Slice() != nil {
		t.Errorf("expected nil slice after empty append, got %v", buf.Slice())
	}
}

func TestSliceBufferDifferentTypes(t *testing.T) {
	tests := []struct {
		name string
		test func(*testing.T)
	}{
		{"byte", func(t *testing.T) { testSliceBufferType[byte](t, []byte{1, 2, 3}) }},
		{"int32", func(t *testing.T) { testSliceBufferType[int32](t, []int32{-1, 0, 1}) }},
		{"int64", func(t *testing.T) { testSliceBufferType[int64](t, []int64{-1000, 0, 1000}) }},
		{"uint32", func(t *testing.T) { testSliceBufferType[uint32](t, []uint32{0, 100, 1000}) }},
		{"uint64", func(t *testing.T) { testSliceBufferType[uint64](t, []uint64{0, 100, 1000}) }},
		{"float32", func(t *testing.T) { testSliceBufferType[float32](t, []float32{-1.5, 0.0, 1.5}) }},
		{"float64", func(t *testing.T) { testSliceBufferType[float64](t, []float64{-1.5, 0.0, 1.5}) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

func testSliceBufferType[T Datum](t *testing.T, data []T) {
	t.Helper()
	buf := new(SliceBuffer[T])
	buf.Append(data...)

	if buf.Len() != len(data) {
		t.Errorf("expected length %d, got %d", len(data), buf.Len())
	}

	slice := buf.Slice()
	if len(slice) != len(data) {
		t.Fatalf("expected slice length %d, got %d", len(data), len(slice))
	}

	for i := range data {
		if slice[i] != data[i] {
			t.Errorf("index %d: expected %v, got %v", i, data[i], slice[i])
		}
	}
}

func TestSliceBufferLargeAppend(t *testing.T) {
	buf := new(SliceBuffer[byte])

	// Append data larger than the largest bucket
	largeData := make([]byte, 10*1024*1024) // 10 MiB
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}
	buf.Append(largeData...)

	if buf.Len() != len(largeData) {
		t.Errorf("expected length %d, got %d", len(largeData), buf.Len())
	}

	slice := buf.Slice()
	if len(slice) != len(largeData) {
		t.Fatalf("expected slice length %d, got %d", len(largeData), len(slice))
	}

	for i := range largeData {
		if slice[i] != largeData[i] {
			t.Errorf("index %d: expected %d, got %d", i, largeData[i], slice[i])
		}
	}
}

func TestSliceBufferBucketTransitions(t *testing.T) {
	// Test specific bucket transitions
	transitions := []struct {
		size        int
		expectedCap int
		description string
	}{
		{500, 1024, "should use 1024 bucket"},
		{1024, 1024, "should use 1024 bucket exactly"},
		{1025, 2048, "should use 2048 bucket"},
		{2048, 2048, "should use 2048 bucket exactly"},
		{2049, 4096, "should use 4096 bucket"},
		{4096, 4096, "should use 4096 bucket exactly"},
		{100000, 131072, "should use 131072 bucket"},
	}

	for _, tt := range transitions {
		t.Run(tt.description, func(t *testing.T) {
			buf := new(SliceBuffer[byte])
			data := make([]byte, tt.size)
			buf.Append(data...)

			slice := buf.Slice()
			if cap(slice) < tt.expectedCap {
				t.Errorf("expected capacity >= %d, got %d", tt.expectedCap, cap(slice))
			}
		})
	}
}

func TestSliceBufferPoolReuse(t *testing.T) {
	buffers := make([]*SliceBuffer[int32], 10)
	for i := range buffers {
		buffers[i] = new(SliceBuffer[int32])
		data := make([]int32, 2000)
		buffers[i].Append(data...)
	}

	for _, buf := range buffers {
		buf.Reset()
	}

	newBuf := new(SliceBuffer[int32])
	data := make([]int32, 2000)
	for i := range data {
		data[i] = int32(i)
	}
	newBuf.Append(data...)

	slice := newBuf.Slice()
	if len(slice) != len(data) {
		t.Fatalf("expected slice length %d, got %d", len(data), len(slice))
	}
	for i := range data {
		if slice[i] != data[i] {
			t.Errorf("index %d: expected %d, got %d", i, data[i], slice[i])
		}
	}
}

func TestSliceBufferSliceValidity(t *testing.T) {
	buf := new(SliceBuffer[int32])
	buf.Append(1, 2, 3)

	slice1 := buf.Slice()
	if len(slice1) != 3 {
		t.Fatalf("expected slice1 length 3, got %d", len(slice1))
	}

	buf.Append(4, 5, 6, 7, 8, 9, 10)

	slice2 := buf.Slice()
	if len(slice2) != 10 {
		t.Fatalf("expected slice2 length 10, got %d", len(slice2))
	}

	for i := range 10 {
		if slice2[i] != int32(i+1) {
			t.Errorf("index %d: expected %d, got %d", i, i+1, slice2[i])
		}
	}
}

func TestSliceBufferIncrementalAppend(t *testing.T) {
	buf := new(SliceBuffer[byte])

	for i := range 5000 {
		buf.Append(byte(i % 256))
	}

	if buf.Len() != 5000 {
		t.Errorf("expected length 5000, got %d", buf.Len())
	}

	slice := buf.Slice()
	for i := range 5000 {
		if slice[i] != byte(i%256) {
			t.Errorf("index %d: expected %d, got %d", i, byte(i%256), slice[i])
		}
	}
}

func BenchmarkSliceBufferAppendSmall(b *testing.B) {
	var buf SliceBuffer[byte]
	data := []byte{1, 2, 3, 4, 5}
	for i := 0; b.Loop(); i++ {
		buf.Append(data...)
		if i&1023 == 1023 {
			buf.Reset()
		}
	}
}

func BenchmarkSliceBufferAppendLarge(b *testing.B) {
	var buf SliceBuffer[int64]
	data := make([]int64, 10000)
	for i := 0; b.Loop(); i++ {
		buf.Append(data...)
		if i&15 == 15 {
			buf.Reset()
		}
	}
}

func BenchmarkSliceBufferGrowth(b *testing.B) {
	for b.Loop() {
		var buf SliceBuffer[int32]
		for j := range 10000 {
			buf.Append(int32(j))
		}
		buf.Reset()
	}
}

func BenchmarkSliceBufferReset(b *testing.B) {
	var buf SliceBuffer[int32]
	data := make([]int32, 5000)
	buf.Append(data...)

	for b.Loop() {
		buf.Reset()
		buf.Append(data...)
	}
}

func BenchmarkSliceBufferSlice(b *testing.B) {
	var buf SliceBuffer[byte]
	data := make([]byte, 100000)
	buf.Append(data...)

	for b.Loop() {
		_ = buf.Slice()
	}
}

func TestSliceBufferSwap(t *testing.T) {
	buf := new(SliceBuffer[int32])
	buf.Append(1, 2, 3, 4, 5)

	buf.Swap(0, 4)
	slice := buf.Slice()
	if slice[0] != 5 || slice[4] != 1 {
		t.Errorf("swap failed: got [%d, _, _, _, %d], want [5, _, _, _, 1]", slice[0], slice[4])
	}

	buf.Swap(1, 3)
	slice = buf.Slice()
	if slice[1] != 4 || slice[3] != 2 {
		t.Errorf("swap failed: got [_, %d, _, %d, _], want [_, 4, _, 2, _]", slice[1], slice[3])
	}
}

func TestSliceBufferLess(t *testing.T) {
	buf := new(SliceBuffer[int32])
	buf.Append(5, 2, 8, 1, 9, 3)

	tests := []struct {
		i, j int
		want bool
	}{
		{0, 1, false}, // 5 < 2 = false
		{1, 0, true},  // 2 < 5 = true
		{1, 3, false}, // 2 < 1 = false
		{3, 2, true},  // 1 < 8 = true
		{4, 0, false}, // 9 < 5 = false
		{5, 4, true},  // 3 < 9 = true
	}

	for _, tt := range tests {
		got := buf.Less(tt.i, tt.j)
		if got != tt.want {
			slice := buf.Slice()
			t.Errorf("Less(%d, %d): got %v, want %v (values: %d < %d)",
				tt.i, tt.j, got, tt.want, slice[tt.i], slice[tt.j])
		}
	}
}

func TestSliceBufferLessFloat(t *testing.T) {
	buf := new(SliceBuffer[float64])
	buf.Append(3.14, -2.5, 0.0, 1.5)

	if !buf.Less(1, 2) { // -2.5 < 0.0
		t.Errorf("Less(1, 2): expected true for -2.5 < 0.0")
	}
	if buf.Less(0, 3) { // 3.14 < 1.5
		t.Errorf("Less(0, 3): expected false for 3.14 < 1.5")
	}
	if !buf.Less(2, 0) { // 0.0 < 3.14
		t.Errorf("Less(2, 0): expected true for 0.0 < 3.14")
	}
}

func TestSliceBufferGrow(t *testing.T) {
	buf := new(SliceBuffer[int32])
	buf.Grow(100)

	if buf.Cap() < 100 {
		t.Errorf("after Grow(100), expected capacity >= 100, got %d", buf.Cap())
	}

	buf.Append(1, 2, 3)
	oldCap := buf.Cap()

	buf.Grow(10)
	if buf.Cap() < oldCap {
		t.Errorf("Grow should not reduce capacity")
	}

	buf.Grow(10000)
	if buf.Cap() < 10003 {
		t.Errorf("after Grow(10000) with 3 elements, expected capacity >= 10003, got %d", buf.Cap())
	}
}

func TestSliceBufferCap(t *testing.T) {
	buf := new(SliceBuffer[int32])
	if buf.Cap() != 0 {
		t.Errorf("empty buffer should have capacity 0, got %d", buf.Cap())
	}

	buf.Append(1, 2, 3)
	if buf.Cap() == 0 {
		t.Errorf("buffer with elements should have non-zero capacity")
	}

	cap1 := buf.Cap()
	buf.Reset()
	if buf.Cap() != 0 {
		t.Errorf("reset buffer should have capacity 0, got %d", buf.Cap())
	}

	buf.Append(1, 2, 3)
	cap2 := buf.Cap()
	if cap2 < cap1 {
		t.Errorf("after reset and append, capacity should be similar, got %d < %d", cap2, cap1)
	}
}

func TestSliceBufferClone(t *testing.T) {
	// Test cloning empty buffer
	buf := new(SliceBuffer[int32])
	cloned := buf.Clone()
	if cloned.Len() != 0 {
		t.Errorf("cloned empty buffer should have length 0, got %d", cloned.Len())
	}
	if cloned.Slice() != nil {
		t.Errorf("cloned empty buffer should have nil slice")
	}

	// Test cloning buffer with data
	buf.Append(1, 2, 3, 4, 5)
	cloned = buf.Clone()

	if cloned.Len() != buf.Len() {
		t.Errorf("cloned buffer should have same length as original: want %d, got %d", buf.Len(), cloned.Len())
	}

	originalSlice := buf.Slice()
	clonedSlice := cloned.Slice()

	// Verify data is identical
	for i := range originalSlice {
		if clonedSlice[i] != originalSlice[i] {
			t.Errorf("index %d: cloned data mismatch: want %d, got %d", i, originalSlice[i], clonedSlice[i])
		}
	}

	// Verify they are independent - modifying one shouldn't affect the other
	cloned.Append(6, 7, 8)
	if buf.Len() == cloned.Len() {
		t.Errorf("modifying clone should not affect original")
	}

	buf.Append(9, 10)
	if cloned.Len() != 8 {
		t.Errorf("modifying original should not affect clone: want 8, got %d", cloned.Len())
	}

	// Verify clone can be reset independently
	cloned.Reset()
	if cloned.Len() != 0 {
		t.Errorf("cloned buffer should be empty after reset")
	}
	if buf.Len() == 0 {
		t.Errorf("original buffer should not be affected by clone reset")
	}
}

func TestSliceBufferCloneLarge(t *testing.T) {
	buf := new(SliceBuffer[int64])
	data := make([]int64, 10000)
	for i := range data {
		data[i] = int64(i)
	}
	buf.Append(data...)

	cloned := buf.Clone()

	if cloned.Len() != len(data) {
		t.Errorf("cloned buffer should have %d elements, got %d", len(data), cloned.Len())
	}

	clonedSlice := cloned.Slice()
	for i := range data {
		if clonedSlice[i] != data[i] {
			t.Errorf("index %d: expected %d, got %d", i, data[i], clonedSlice[i])
		}
	}
}

func TestSliceBufferResize(t *testing.T) {
	// Test growing from empty
	buf := new(SliceBuffer[int32])
	buf.Resize(10)
	if buf.Len() != 10 {
		t.Errorf("after Resize(10), expected length 10, got %d", buf.Len())
	}

	// Test growing with existing data
	buf.Resize(5)
	for i := range 5 {
		buf.Slice()[i] = int32(i + 1)
	}
	buf.Resize(10)
	if buf.Len() != 10 {
		t.Errorf("after Resize(10), expected length 10, got %d", buf.Len())
	}
	slice := buf.Slice()
	for i := range 5 {
		if slice[i] != int32(i+1) {
			t.Errorf("index %d: expected %d, got %d", i, i+1, slice[i])
		}
	}

	// Test shrinking
	buf.Resize(3)
	if buf.Len() != 3 {
		t.Errorf("after Resize(3), expected length 3, got %d", buf.Len())
	}
	slice = buf.Slice()
	for i := range 3 {
		if slice[i] != int32(i+1) {
			t.Errorf("index %d: expected %d, got %d", i, i+1, slice[i])
		}
	}

	// Test resize to 0
	buf.Resize(0)
	if buf.Len() != 0 {
		t.Errorf("after Resize(0), expected length 0, got %d", buf.Len())
	}
}

func TestSliceBufferResizePattern(t *testing.T) {
	// Test the pattern: offset := buf.Len(); buf.Resize(offset + n); slice[offset:]
	buf := new(SliceBuffer[float32])
	buf.Append(1.0, 2.0, 3.0)

	offset := buf.Len()
	buf.Resize(offset + 5)

	slice := buf.Slice()
	for i := offset; i < offset+5; i++ {
		slice[i] = float32(i + 1)
	}

	expected := []float32{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0}
	if buf.Len() != len(expected) {
		t.Fatalf("expected length %d, got %d", len(expected), buf.Len())
	}

	for i, v := range expected {
		if slice[i] != v {
			t.Errorf("index %d: expected %f, got %f", i, v, slice[i])
		}
	}
}

func TestSliceBufferFrom(t *testing.T) {
	// Test with nil slice
	buf := SliceBufferFrom[int32](nil)
	if buf.Len() != 0 {
		t.Errorf("SliceBufferFrom(nil) should have length 0, got %d", buf.Len())
	}

	// Test with existing data
	data := []int32{1, 2, 3, 4, 5}
	buf = SliceBufferFrom(data)

	if buf.Len() != len(data) {
		t.Errorf("expected length %d, got %d", len(data), buf.Len())
	}

	slice := buf.Slice()
	for i, v := range data {
		if slice[i] != v {
			t.Errorf("index %d: expected %d, got %d", i, v, slice[i])
		}
	}

	// Verify it wraps without copying (modifying original affects buffer)
	data[0] = 100
	if slice[0] != 100 {
		t.Errorf("SliceBufferFrom should wrap without copying")
	}

	// Test that appending transitions to pooled storage
	buf.Append(6, 7, 8)
	if buf.Len() != 8 {
		t.Errorf("after append, expected length 8, got %d", buf.Len())
	}
}

func TestSliceBufferFor(t *testing.T) {
	// Test with zero capacity
	buf := SliceBufferFor[int32](0)
	if buf.Cap() != 0 {
		t.Errorf("SliceBufferFor(0) should have capacity 0, got %d", buf.Cap())
	}
	if buf.Len() != 0 {
		t.Errorf("SliceBufferFor(0) should have length 0, got %d", buf.Len())
	}

	// Test with small capacity
	buf = SliceBufferFor[int32](100)
	if buf.Cap() < 100 {
		t.Errorf("SliceBufferFor(100) should have capacity >= 100, got %d", buf.Cap())
	}
	if buf.Len() != 0 {
		t.Errorf("SliceBufferFor should create empty buffer with pre-allocated capacity")
	}

	// Verify we can append without reallocation
	buf.Append(1, 2, 3, 4, 5)
	if buf.Len() != 5 {
		t.Errorf("after append, expected length 5, got %d", buf.Len())
	}

	// Test with large capacity
	largeBuf := SliceBufferFor[byte](10000)
	if largeBuf.Cap() < 10000 {
		t.Errorf("SliceBufferFor(10000) should have capacity >= 10000, got %d", largeBuf.Cap())
	}

	// Test with capacity larger than largest bucket
	hugeBuf := SliceBufferFor[byte](20 * 1024 * 1024) // 20 MiB
	if hugeBuf.Cap() < 20*1024*1024 {
		t.Errorf("SliceBufferFor(20MB) should have capacity >= 20MB, got %d", hugeBuf.Cap())
	}
}

func TestSliceBufferExternalDataTransition(t *testing.T) {
	// Create buffer from external data
	externalData := []int32{1, 2, 3}
	buf := SliceBufferFrom(externalData)

	// Verify it starts with external data
	if buf.Len() != 3 {
		t.Errorf("expected length 3, got %d", buf.Len())
	}

	// Append enough to trigger transition to pooled storage
	for i := range 5000 {
		buf.Append(int32(i))
	}

	// Verify data is intact
	if buf.Len() != 5003 {
		t.Errorf("expected length 5003, got %d", buf.Len())
	}

	slice := buf.Slice()
	if slice[0] != 1 || slice[1] != 2 || slice[2] != 3 {
		t.Errorf("original data should be preserved after transition")
	}
}

func TestSliceBufferOversizedAllocation(t *testing.T) {
	// Test allocation that exceeds largest bucket
	buf := new(SliceBuffer[byte])

	// Append data larger than largest bucket (should allocate directly)
	hugeData := make([]byte, 50*1024*1024) // 50 MiB
	for i := range hugeData {
		hugeData[i] = byte(i % 256)
	}
	buf.Append(hugeData...)

	if buf.Len() != len(hugeData) {
		t.Errorf("expected length %d, got %d", len(hugeData), buf.Len())
	}

	// Verify data is correct
	slice := buf.Slice()
	for i := range min(1000, len(hugeData)) {
		if slice[i] != byte(i%256) {
			t.Errorf("index %d: data mismatch", i)
			break
		}
	}

	// Test clone of oversized buffer
	cloned := buf.Clone()
	if cloned.Len() != buf.Len() {
		t.Errorf("cloned oversized buffer should have same length")
	}

	// Reset should work
	buf.Reset()
	if buf.Len() != 0 {
		t.Errorf("reset should empty the buffer")
	}
}

func TestSliceBufferBucketEdgeCases(t *testing.T) {
	// Test bucketIndexOfGet edge cases
	tests := []struct {
		name     string
		setup    func() SliceBuffer[byte]
		validate func(*testing.T, SliceBuffer[byte])
	}{
		{
			name: "zero sized bucket",
			setup: func() SliceBuffer[byte] {
				return SliceBufferFor[byte](0)
			},
			validate: func(t *testing.T, buf SliceBuffer[byte]) {
				if buf.Len() != 0 {
					t.Errorf("expected length 0")
				}
			},
		},
		{
			name: "smallest bucket boundary",
			setup: func() SliceBuffer[byte] {
				buf := SliceBufferFor[byte](minBucketSize)
				return buf
			},
			validate: func(t *testing.T, buf SliceBuffer[byte]) {
				if buf.Cap() < minBucketSize {
					t.Errorf("expected capacity >= %d", minBucketSize)
				}
			},
		},
		{
			name: "transition point bucket",
			setup: func() SliceBuffer[byte] {
				buf := SliceBufferFor[byte](lastShortBucketSize)
				return buf
			},
			validate: func(t *testing.T, buf SliceBuffer[byte]) {
				if buf.Cap() < lastShortBucketSize {
					t.Errorf("expected capacity >= %d", lastShortBucketSize)
				}
			},
		},
		{
			name: "just after transition",
			setup: func() SliceBuffer[byte] {
				buf := SliceBufferFor[byte](lastShortBucketSize + 1)
				return buf
			},
			validate: func(t *testing.T, buf SliceBuffer[byte]) {
				if buf.Cap() < lastShortBucketSize {
					t.Errorf("expected capacity >= %d", lastShortBucketSize)
				}
			},
		},
		{
			name: "pooled buffer with external data appending",
			setup: func() SliceBuffer[byte] {
				external := make([]byte, 100)
				for i := range external {
					external[i] = byte(i)
				}
				buf := SliceBufferFrom(external)
				// Append more to trigger reallocation
				buf.Append(make([]byte, 10000)...)
				return buf
			},
			validate: func(t *testing.T, buf SliceBuffer[byte]) {
				if buf.Len() != 10100 {
					t.Errorf("expected length 10100, got %d", buf.Len())
				}
				// Verify original data preserved
				slice := buf.Slice()
				for i := range 100 {
					if slice[i] != byte(i) {
						t.Errorf("original data not preserved at index %d", i)
						break
					}
				}
			},
		},
		{
			name: "growing from pooled to larger pooled",
			setup: func() SliceBuffer[byte] {
				buf := new(SliceBuffer[byte])
				buf.Append(make([]byte, 1000)...)
				buf.Append(make([]byte, 10000)...)
				return *buf
			},
			validate: func(t *testing.T, buf SliceBuffer[byte]) {
				if buf.Len() != 11000 {
					t.Errorf("expected length 11000, got %d", buf.Len())
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := tt.setup()
			tt.validate(t, buf)
			buf.Reset() // Ensure reset works for all cases
		})
	}
}

func TestSliceBufferResizeEdgeCases(t *testing.T) {
	// Test Resize triggering reserve with oversized allocation
	buf := new(SliceBuffer[byte])
	buf.Resize(100 * 1024 * 1024) // 100 MiB
	if buf.Len() != 100*1024*1024 {
		t.Errorf("expected length %d, got %d", 100*1024*1024, buf.Len())
	}

	// Test Resize on buffer with external data
	external := []byte{1, 2, 3}
	buf2 := SliceBufferFrom(external)
	buf2.Resize(10000)
	if buf2.Len() != 10000 {
		t.Errorf("expected length 10000, got %d", buf2.Len())
	}
	// Original data should still be there
	slice := buf2.Slice()
	if slice[0] != 1 || slice[1] != 2 || slice[2] != 3 {
		t.Errorf("original data not preserved after resize")
	}
}

func TestBucketIndexOfGetEdgeCases(t *testing.T) {
	tests := []struct {
		name string
		test func(*testing.T)
	}{
		{
			name: "zero returns first bucket",
			test: func(t *testing.T) {
				buf := SliceBufferFor[byte](0)
				// Should succeed without panic
				buf.Append(1, 2, 3)
			},
		},
		{
			name: "negative value handled",
			test: func(t *testing.T) {
				// This tests internal behavior via Grow with negative
				// In practice this shouldn't happen, but the code handles it
				buf := new(SliceBuffer[byte])
				buf.Append(1, 2, 3)
				// Grow(0) or Grow(-1) should be no-op
				buf.Grow(0)
				buf.Grow(-1)
				if buf.Len() != 3 {
					t.Errorf("expected length unchanged")
				}
			},
		},
		{
			name: "exact bucket boundaries",
			test: func(t *testing.T) {
				// Test exact bucket size boundaries
				bucketSizes := []int{
					minBucketSize,               // 4096
					minBucketSize * 2,           // 8192
					minBucketSize * 4,           // 16384
					lastShortBucketSize,         // 262144
					lastShortBucketSize * 3 / 2, // 393216 (first large bucket)
				}
				for _, size := range bucketSizes {
					buf := SliceBufferFor[byte](size)
					if buf.Cap() < size {
						t.Errorf("bucket size %d: expected capacity >= %d, got %d", size, size, buf.Cap())
					}
				}
			},
		},
		{
			name: "all buckets reachable",
			test: func(t *testing.T) {
				// Ensure we can reach all 32 buckets by growing
				buf := new(SliceBuffer[byte])
				size := minBucketSize
				for i := range numBuckets {
					buf.Grow(size - buf.Len())
					if buf.Cap() < size {
						t.Errorf("bucket %d: expected capacity >= %d, got %d", i, size, buf.Cap())
					}
					if size < lastShortBucketSize {
						size = size * 2
					} else {
						size = size + (size / 2)
					}
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

func TestBucketIndexOfPutEdgeCases(t *testing.T) {
	tests := []struct {
		name string
		test func(*testing.T)
	}{
		{
			name: "buffer smaller than min bucket not pooled",
			test: func(t *testing.T) {
				// Create a tiny buffer and ensure it still works
				tiny := make([]byte, 10)
				buf := SliceBufferFrom(tiny)
				buf.Reset() // Should not panic even though too small to pool
				if buf.Len() != 0 {
					t.Errorf("expected empty after reset")
				}
			},
		},
		{
			name: "non-standard capacity from append",
			test: func(t *testing.T) {
				// This tests the putSliceToPool behavior when capacity
				// doesn't match bucket sizes exactly (from append)
				buf := new(SliceBuffer[byte])
				buf.Append(make([]byte, 5000)...)
				// Internal capacity might not match bucket size exactly
				// Reset should still work
				buf.Reset()
				if buf.Len() != 0 {
					t.Errorf("expected empty after reset")
				}
			},
		},
		{
			name: "boundary between buckets",
			test: func(t *testing.T) {
				// Test sizes at boundaries
				boundaries := []int{
					minBucketSize - 1,
					minBucketSize,
					minBucketSize + 1,
					lastShortBucketSize - 1,
					lastShortBucketSize,
					lastShortBucketSize + 1,
				}
				for _, size := range boundaries {
					buf := SliceBufferFor[byte](size)
					buf.Append(make([]byte, size)...)
					buf.Reset() // Should handle all boundary cases
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

func TestBucketSizeEdgeCases(t *testing.T) {
	// bucketSize is internal, but we can test it via SliceBufferFor
	tests := []struct {
		name        string
		bucketIndex int
		test        func(*testing.T)
	}{
		{
			name:        "negative bucket index",
			bucketIndex: -1,
			test: func(t *testing.T) {
				// Can't directly test bucketSize(-1), but we can test
				// that oversized allocations work
				buf := new(SliceBuffer[byte])
				huge := make([]byte, 200*1024*1024) // 200 MiB
				buf.Append(huge...)
				if buf.Len() != len(huge) {
					t.Errorf("oversized buffer should work")
				}
			},
		},
		{
			name:        "first bucket (index 0)",
			bucketIndex: 0,
			test: func(t *testing.T) {
				buf := SliceBufferFor[byte](minBucketSize)
				if buf.Cap() < minBucketSize {
					t.Errorf("first bucket should have min size")
				}
			},
		},
		{
			name:        "last bucket (index 31)",
			bucketIndex: numBuckets - 1,
			test: func(t *testing.T) {
				// Calculate last bucket size
				size := minBucketSize
				for range numBuckets - 1 {
					if size < lastShortBucketSize {
						size = size * 2
					} else {
						size = size + (size / 2)
					}
				}
				buf := SliceBufferFor[byte](size)
				if buf.Cap() < size {
					t.Errorf("last bucket should accommodate its size")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

func TestReserveComplexPaths(t *testing.T) {
	tests := []struct {
		name string
		test func(*testing.T)
	}{
		{
			name: "empty to pooled",
			test: func(t *testing.T) {
				buf := new(SliceBuffer[byte])
				buf.Append(make([]byte, 5000)...)
				if buf.Len() != 5000 {
					t.Errorf("empty to pooled transition failed")
				}
			},
		},
		{
			name: "external to pooled",
			test: func(t *testing.T) {
				external := make([]byte, 100)
				for i := range external {
					external[i] = byte(i)
				}
				buf := SliceBufferFrom(external)
				buf.Append(make([]byte, 10000)...)
				if buf.Len() != 10100 {
					t.Errorf("external to pooled transition failed")
				}
				// Verify original data preserved
				slice := buf.Slice()
				for i := range 100 {
					if slice[i] != byte(i) {
						t.Errorf("data not preserved")
						break
					}
				}
			},
		},
		{
			name: "pooled to larger pooled",
			test: func(t *testing.T) {
				buf := new(SliceBuffer[byte])
				buf.Append(make([]byte, 5000)...)
				buf.Append(make([]byte, 50000)...)
				if buf.Len() != 55000 {
					t.Errorf("pooled to larger pooled failed")
				}
			},
		},
		{
			name: "pooled to oversized",
			test: func(t *testing.T) {
				buf := new(SliceBuffer[byte])
				buf.Append(make([]byte, 5000)...)
				// Now append huge amount to exceed all buckets
				buf.Append(make([]byte, 100*1024*1024)...)
				if buf.Len() != 5000+100*1024*1024 {
					t.Errorf("pooled to oversized failed")
				}
			},
		},
		{
			name: "external to oversized",
			test: func(t *testing.T) {
				external := []byte{1, 2, 3}
				buf := SliceBufferFrom(external)
				buf.Append(make([]byte, 100*1024*1024)...)
				if buf.Len() != 3+100*1024*1024 {
					t.Errorf("external to oversized failed")
				}
				// Verify original data
				slice := buf.Slice()
				if slice[0] != 1 || slice[1] != 2 || slice[2] != 3 {
					t.Errorf("data not preserved")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

func TestCloneEdgeCases(t *testing.T) {
	tests := []struct {
		name string
		test func(*testing.T)
	}{
		{
			name: "clone empty buffer",
			test: func(t *testing.T) {
				buf := new(SliceBuffer[int32])
				cloned := buf.Clone()
				if cloned.Len() != 0 {
					t.Errorf("cloned empty should be empty")
				}
				if cloned.Slice() != nil {
					t.Errorf("cloned empty should have nil slice")
				}
			},
		},
		{
			name: "clone small buffer",
			test: func(t *testing.T) {
				buf := new(SliceBuffer[byte])
				buf.Append(1, 2, 3)
				cloned := buf.Clone()
				if cloned.Len() != 3 {
					t.Errorf("clone should preserve length")
				}
			},
		},
		{
			name: "clone large buffer",
			test: func(t *testing.T) {
				buf := new(SliceBuffer[int64])
				buf.Append(make([]int64, 50000)...)
				cloned := buf.Clone()
				if cloned.Len() != 50000 {
					t.Errorf("clone should preserve length")
				}
			},
		},
		{
			name: "clone oversized buffer",
			test: func(t *testing.T) {
				buf := new(SliceBuffer[byte])
				buf.Append(make([]byte, 100*1024*1024)...)
				cloned := buf.Clone()
				if cloned.Len() != 100*1024*1024 {
					t.Errorf("oversized clone should preserve length")
				}
			},
		},
		{
			name: "clone with external data",
			test: func(t *testing.T) {
				external := []byte{1, 2, 3, 4, 5}
				buf := SliceBufferFrom(external)
				cloned := buf.Clone()
				if cloned.Len() != 5 {
					t.Errorf("clone should preserve length")
				}
				// Modify original
				external[0] = 99
				// Clone should be independent
				if cloned.Slice()[0] != 1 {
					t.Errorf("clone should be independent copy")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

func TestPutSliceToPoolEdgeCases(t *testing.T) {
	// These edge cases are tested indirectly via Reset
	tests := []struct {
		name string
		test func(*testing.T)
	}{
		{
			name: "reset nil buffer",
			test: func(t *testing.T) {
				var buf SliceBuffer[byte]
				buf.Reset() // Should not panic
				if buf.Len() != 0 {
					t.Errorf("nil buffer reset should be no-op")
				}
			},
		},
		{
			name: "reset external data buffer",
			test: func(t *testing.T) {
				external := []byte{1, 2, 3}
				buf := SliceBufferFrom(external)
				buf.Reset()
				if buf.Len() != 0 {
					t.Errorf("reset should clear buffer")
				}
			},
		},
		{
			name: "reset tiny buffer (below min bucket)",
			test: func(t *testing.T) {
				tiny := make([]byte, 10)
				buf := SliceBufferFrom(tiny)
				buf.Reset() // Should not panic even though too small to pool
				if buf.Len() != 0 {
					t.Errorf("tiny buffer reset should work")
				}
			},
		},
		{
			name: "reset oversized buffer",
			test: func(t *testing.T) {
				buf := new(SliceBuffer[byte])
				buf.Append(make([]byte, 100*1024*1024)...)
				buf.Reset() // Should handle non-pooled allocation
				if buf.Len() != 0 {
					t.Errorf("oversized buffer reset should work")
				}
			},
		},
		{
			name: "multiple resets",
			test: func(t *testing.T) {
				buf := new(SliceBuffer[byte])
				buf.Append(make([]byte, 5000)...)
				buf.Reset()
				buf.Append(make([]byte, 5000)...)
				buf.Reset()
				buf.Append(make([]byte, 5000)...)
				buf.Reset()
				if buf.Len() != 0 {
					t.Errorf("multiple resets should work")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}
