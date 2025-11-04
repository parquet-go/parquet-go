package unsafecast_test

import (
	"testing"

	"github.com/parquet-go/bitpack/unsafecast"
	"golang.org/x/sys/cpu"
)

func TestUnsafeCastSlice(t *testing.T) {
	// Note: this test is currently disabled on Big-Endian architectures because
	// it assumes a Little-Endian memory layout.
	if cpu.IsBigEndian {
		t.Skip("skipping test on big-endian architecture")
	}

	a := make([]uint32, 4, 13)
	a[0] = 1
	a[1] = 0
	a[2] = 2
	a[3] = 0

	b := unsafecast.Slice[int64](a)
	if len(b) != 2 { // (4 * sizeof(uint32)) / sizeof(int64)
		t.Fatalf("length mismatch: want=2 got=%d", len(b))
	}
	if cap(b) != 6 { // (13 * sizeof(uint32)) / sizeof(int64)
		t.Fatalf("capacity mismatch: want=6 got=%d", cap(b))
	}
	if b[0] != 1 {
		t.Errorf("wrong value at index 0: want=1 got=%d", b[0])
	}
	if b[1] != 2 {
		t.Errorf("wrong value at index 1: want=2 got=%d", b[1])
	}

	c := unsafecast.Slice[uint32](b)
	if len(c) != 4 {
		t.Fatalf("length mismatch: want=2 got=%d", len(b))
	}
	if cap(c) != 12 {
		t.Fatalf("capacity mismatch: want=7 got=%d", cap(b))
	}
	for i := range c {
		if c[i] != a[i] {
			t.Errorf("wrong value at index %d: want=%d got=%d", i, a[i], c[i])
		}
	}
}
