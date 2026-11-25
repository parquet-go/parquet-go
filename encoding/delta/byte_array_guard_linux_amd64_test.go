package delta

import (
	"bytes"
	"os"
	"syscall"
	"testing"
)

func TestEncodeByteArrayGuardedBuffers(t *testing.T) {
	guarded := func(n int) []byte {
		t.Helper()
		pageSize := os.Getpagesize()
		size := (n + pageSize - 1) / pageSize * pageSize
		mapping, err := syscall.Mmap(-1, 0, size+pageSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_PRIVATE|syscall.MAP_ANON)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := syscall.Munmap(mapping); err != nil {
				t.Error(err)
			}
		})
		if err := syscall.Mprotect(mapping[size:], syscall.PROT_NONE); err != nil {
			t.Fatal(err)
		}
		return mapping[size-n : size : size]
	}
	e := new(ByteArrayEncoding)
	for residue := range 32 {
		src := guarded(4096 + residue)
		offsets := make([]uint32, len(src)+1)
		for i := range src {
			src[i] = byte(i)
			offsets[i] = uint32(i)
		}
		offsets[len(src)] = uint32(len(src))
		want, err := e.EncodeByteArray(nil, src, offsets)
		if err != nil {
			t.Fatal(err)
		}
		dst := guarded(len(want))
		got, err := e.EncodeByteArray(dst[:0], src, offsets)
		if err != nil {
			t.Fatal(err)
		}
		if &got[0] != &dst[0] {
			t.Fatal("exact-size destination was reallocated")
		}
		if !bytes.Equal(got, want) {
			t.Fatal("encoded bytes differ with guarded buffers")
		}
	}
}
