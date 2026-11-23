package thrift

import (
	"bytes"
	"fmt"
	"io"
	"strings"
)

// maxContainerPrealloc is the most elements or payload bytes a decoder
// allocates from a size header before it has read that many elements.
// Larger containers and byte strings grow as they are decoded. A declared
// count by itself cannot request an arbitrarily large allocation.
const maxContainerPrealloc = 4096

// remainingOf reports how many unread bytes r can see. -1 means the total
// is not known, which is the case for a streaming reader over an arbitrary
// io.Reader. The type switch stays on concrete readers so the hot path does
// not convert between interfaces.
func remainingOf(r Reader) int {
	switch x := r.(type) {
	case *compactBytesReader:
		return bytesRemain(x.offset, len(x.data))
	case *binaryBytesReader:
		return bytesRemain(x.offset, len(x.data))
	case *compactReader:
		return ioRemain(x.binary.r)
	case *binaryReader:
		return ioRemain(x.r)
	case *debugReader:
		return remainingOf(x.r)
	default:
		return -1
	}
}

func bytesRemain(offset, length int) int {
	if offset >= length {
		return 0
	}
	return length - offset
}

func ioRemain(r io.Reader) int {
	if n, ok := ioRemaining(r); ok {
		return n
	}
	return -1
}

func ioRemaining(r io.Reader) (int, bool) {
	switch x := r.(type) {
	case *bytes.Reader:
		return x.Len(), true
	case *bytes.Buffer:
		return x.Len(), true
	case *strings.Reader:
		return x.Len(), true
	default:
		return 0, false
	}
}

// checkCount rejects a declared element count that is negative or that
// cannot fit in the unread input. minBytes is the minimum encoded size of
// one element. Every thrift value is at least one byte, and a map entry is
// a key plus a value.
func checkCount(r Reader, kind string, n, minBytes int) error {
	if countFits(r, n, minBytes) {
		return nil
	}
	return countError(r, kind, n)
}

// countFits is split from the error formatting so the success path can inline.
func countFits(r Reader, n, minBytes int) bool {
	if n < 0 {
		return false
	}
	rem := remainingOf(r)
	if rem < 0 {
		return true
	}
	if minBytes < 1 {
		minBytes = 1
	}
	return n <= rem/minBytes
}

func countError(r Reader, kind string, n int) error {
	if n < 0 {
		return fmt.Errorf("negative thrift %s: %d", kind, n)
	}
	return fmt.Errorf("thrift %s %d exceeds remaining input of %d bytes", kind, n, remainingOf(r))
}

// readLengthBytes reads exactly n payload bytes. Lengths up to
// maxContainerPrealloc are allocated up front. Larger lengths are allocated
// only when the unread input is known to contain them; otherwise the buffer
// grows as bytes arrive, and a length that cannot fit is rejected before
// that allocation.
func readLengthBytes(r io.Reader, n int) ([]byte, error) {
	if n < 0 {
		return nil, fmt.Errorf("negative thrift length: %d", n)
	}
	if n == 0 {
		return make([]byte, 0), nil
	}
	// Lengths within the cap are safe to allocate up front. That is the
	// common path (metadata strings and byte fields) and matches the
	// previous make-and-read sequence.
	if n <= maxContainerPrealloc {
		b := make([]byte, n)
		_, err := io.ReadFull(r, b)
		if err != nil {
			return nil, err
		}
		return b, nil
	}
	if rem, known := ioRemaining(r); known {
		if n > rem {
			return nil, io.ErrUnexpectedEOF
		}
		b := make([]byte, n)
		_, err := io.ReadFull(r, b)
		if err != nil {
			return nil, err
		}
		return b, nil
	}
	b := make([]byte, 0, maxContainerPrealloc)
	var buf [maxContainerPrealloc]byte
	for n > 0 {
		want := min(maxContainerPrealloc, n)
		nr, err := io.ReadFull(r, buf[:want])
		b = append(b, buf[:nr]...)
		n -= nr
		if err != nil {
			return nil, err
		}
	}
	return b, nil
}

// takeBytes returns data[offset:offset+n] when those bytes are in data.
// n is not added to offset until the caller applies the returned end, and
// the comparison does not overflow.
func takeBytes(data []byte, offset, n int) ([]byte, int, error) {
	if n < 0 || offset > len(data) || n > len(data)-offset {
		return nil, offset, io.ErrUnexpectedEOF
	}
	end := offset + n
	return data[offset:end], end, nil
}
