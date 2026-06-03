package parquet

import (
	"bytes"
	"io"
	"testing"
)

// readSeekerOnly wraps an io.ReadSeeker to hide any io.ReaderAt implementation,
// forcing newReaderAt to wrap it in a readerAt.
type readSeekerOnly struct{ io.ReadSeeker }

func newTestReaderAt(data []byte) *readerAt {
	return &readerAt{reader: readSeekerOnly{bytes.NewReader(data)}, offset: -1}
}

// TestReaderAtEOFConversion is the core correctness test: io.ReadFull returns
// io.ErrUnexpectedEOF when a short read occurs at EOF, but io.ReaderAt callers
// expect io.EOF. The fix converts ErrUnexpectedEOF → EOF.
func TestReaderAtEOFConversion(t *testing.T) {
	data := []byte("hello")
	r := newTestReaderAt(data)

	buf := make([]byte, len(data)+1) // request more bytes than available
	n, err := r.ReadAt(buf, 0)

	if err != io.EOF {
		t.Fatalf("expected io.EOF, got %v", err)
	}
	if n != len(data) {
		t.Fatalf("expected %d bytes read, got %d", len(data), n)
	}
	if !bytes.Equal(buf[:n], data) {
		t.Fatalf("data mismatch: got %q, want %q", buf[:n], data)
	}
}

// TestReaderAtSeeksWhenOffsetMismatch verifies that ReadAt seeks when the
// requested offset differs from the tracked position (non-sequential access).
func TestReaderAtSeeksWhenOffsetMismatch(t *testing.T) {
	data := []byte("0123456789")
	r := newTestReaderAt(data)

	// First read at offset 5
	buf := make([]byte, 3)
	n, err := r.ReadAt(buf, 5)
	if err != nil && err != io.EOF {
		t.Fatalf("first ReadAt: unexpected error: %v", err)
	}
	if n != 3 || !bytes.Equal(buf, []byte("567")) {
		t.Fatalf("first ReadAt: got %q, want %q", buf[:n], "567")
	}

	// Second read at offset 0 — requires a seek backwards
	n, err = r.ReadAt(buf, 0)
	if err != nil && err != io.EOF {
		t.Fatalf("second ReadAt: unexpected error: %v", err)
	}
	if n != 3 || !bytes.Equal(buf, []byte("012")) {
		t.Fatalf("second ReadAt: got %q, want %q", buf[:n], "012")
	}
}

// TestReaderAtSequentialNoSeek verifies that consecutive ReadAt calls at
// sequential offsets advance correctly without re-seeking.
func TestReaderAtSequentialNoSeek(t *testing.T) {
	data := []byte("abcdefghij")
	r := newTestReaderAt(data)

	buf := make([]byte, 2)
	for i := 0; i < len(data); i += 2 {
		n, err := r.ReadAt(buf, int64(i))
		if err != nil && err != io.EOF {
			t.Fatalf("ReadAt(%d): unexpected error: %v", i, err)
		}
		if n != 2 {
			t.Fatalf("ReadAt(%d): expected 2 bytes, got %d", i, n)
		}
		if !bytes.Equal(buf, data[i:i+2]) {
			t.Fatalf("ReadAt(%d): got %q, want %q", i, buf, data[i:i+2])
		}
	}
}

// TestNewReaderAtPassthrough verifies that newReaderAt returns the underlying
// value directly when it already implements io.ReaderAt (no wrapping overhead).
func TestNewReaderAtPassthrough(t *testing.T) {
	r := bytes.NewReader([]byte("test"))
	ra := newReaderAt(r)
	if ra != r {
		t.Fatal("expected newReaderAt to return the original bytes.Reader directly")
	}
}
