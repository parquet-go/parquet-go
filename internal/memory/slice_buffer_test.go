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
	slice := buf.Slice()
	for i := range slice {
		if slice[i] != 0 {
			t.Errorf("index %d: expected zero-initialized value, got %d", i, slice[i])
		}
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
	slice = buf.Slice()
	for i := range 5 {
		if slice[i] != int32(i+1) {
			t.Errorf("index %d: expected %d, got %d", i, i+1, slice[i])
		}
	}
	for i := 5; i < 10; i++ {
		if slice[i] != 0 {
			t.Errorf("index %d: expected zero-initialized value, got %d", i, slice[i])
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

	// Test resize negative (should become 0)
	buf.Resize(-5)
	if buf.Len() != 0 {
		t.Errorf("after Resize(-5), expected length 0, got %d", buf.Len())
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
