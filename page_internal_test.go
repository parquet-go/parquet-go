package parquet

import (
	"errors"
	"io"
	"math/rand"
	"sync/atomic"
	"testing"

	"github.com/parquet-go/parquet-go/encoding"
)

type testPages struct {
	pageIndex int
	pages     []Page

	retErr atomic.Bool
}

func (p *testPages) ReadPage() (Page, error) {
	// if we have a return error, return it and reset it
	if p.retErr.Load() {
		p.retErr.Store(false)
		return nil, errors.New("test error")
	}

	if p.pageIndex >= len(p.pages) {
		return nil, io.EOF
	}
	page := p.pages[p.pageIndex]
	p.pageIndex++
	return page, nil
}

func (p *testPages) Close() error {
	p.pages = nil
	return nil
}

func (p *testPages) SeekToRow(rowIndex int64) error {
	if rowIndex < 0 || rowIndex >= int64(len(p.pages)) {
		return ErrSeekOutOfRange
	}
	p.pageIndex = int(rowIndex)
	return nil
}

// TestAsyncPagesRead tests an ordered read of async pages.
func TestAsyncPagesRead(t *testing.T) {
	tp := &testPages{
		pages: make([]Page, 10),
	}
	for i := range tp.pages {
		tp.pages[i] = newInt32Page(int32Type{}, 0, 1, encoding.Int32Values([]int32{int32(i)}))
	}

	// read all 10 pages in order and confirm expected values
	asyncPages := AsyncPages(tp)
	defer asyncPages.Close()
	for i := range tp.pages {
		expectInt(t, int32(i), asyncPages)
	}
}

// TestAsyncPagesSeekToRow tests seeking to a random row and confirming expected values.
func TestAsyncPagesSeekToRow(t *testing.T) {
	tp := &testPages{
		pages: make([]Page, 10),
	}
	for i := range tp.pages {
		tp.pages[i] = newInt32Page(int32Type{}, 0, 1, encoding.Int32Values([]int32{int32(i)}))
	}

	// seek to random row 10 timesand confirm expected values
	asyncPages := AsyncPages(tp)
	defer asyncPages.Close()
	for range 10 {
		rowIndex := rand.Intn(len(tp.pages))
		err := asyncPages.SeekToRow(int64(rowIndex))
		if err != nil {
			t.Fatalf("error seeking to row: %v", err)
		}

		expectInt(t, int32(rowIndex), asyncPages)
	}
}

// TestAsyncPagesSeekToRow recovers from ErrSeekOutOfRange
func TestAsyncPagesRecoversFromSeekOutOfRange(t *testing.T) {
	tp := &testPages{
		pages: make([]Page, 10),
	}
	for i := range tp.pages {
		tp.pages[i] = newInt32Page(int32Type{}, 0, 1, encoding.Int32Values([]int32{int32(i)}))
	}

	asyncPages := AsyncPages(tp)
	defer asyncPages.Close()

	// seek to a known row and confirm expected value
	expected := int32(3)
	err := asyncPages.SeekToRow(int64(expected))
	if err != nil {
		t.Fatalf("error seeking to row: %v", err)
	}
	expectInt(t, expected, asyncPages)

	// seek to an invalid row and confirm we get an error. read page should error repeatedly until we do
	// a valid seek
	err = asyncPages.SeekToRow(int64(15))
	if err != nil {
		t.Fatalf("error seeking to row: %v", err)
	}
	_, err = asyncPages.ReadPage()
	if err != ErrSeekOutOfRange {
		t.Fatalf("expected ErrSeekOutOfRange, got %v", err)
	}
	_, err = asyncPages.ReadPage()
	if err != ErrSeekOutOfRange {
		t.Fatalf("expected ErrSeekOutOfRange, got %v", err)
	}

	// do a valid seek and confirm all is well
	expected = int32(7)
	err = asyncPages.SeekToRow(int64(expected))
	if err != nil {
		t.Fatalf("error seeking to row: %v", err)
	}
	expectInt(t, expected, asyncPages)
}

// TestAsyncPagesAlwaysReturnsSameErrorAfterOne tests that AsyncPages always returns an error after the underlying
// pages object has returned an error.
func TestAsyncPagesAlwaysReturnsSameErrorAfterOne(t *testing.T) {
	tp := &testPages{
		pages: make([]Page, 10),
	}
	for i := range tp.pages {
		tp.pages[i] = newInt32Page(int32Type{}, 0, 1, encoding.Int32Values([]int32{int32(i)}))
	}

	asyncPages := AsyncPages(tp)
	defer asyncPages.Close()

	// read a few pages
	for i := range 3 {
		expectInt(t, int32(i), asyncPages)
	}

	// set an error
	tp.retErr.Store(true)

	// read until we get an error. AsyncPages has likely loaded at least one more page so the next read will
	// probably succeed and the one after will fail
	var err error
	for {
		_, err = asyncPages.ReadPage()
		if err != nil {
			break
		}
	}
	if err == io.EOF {
		t.Fatalf("expected fatal error, got EOF")
	}

	// read again and confirm we get the same error
	_, nextErr := asyncPages.ReadPage()
	if nextErr == nil || nextErr != err {
		t.Fatalf("expected error, got %v", nextErr)
	}
}

func expectInt(t *testing.T, expected int32, p Pages) {
	page, err := p.ReadPage()
	if err != nil && err != io.EOF {
		t.Fatalf("error reading page: %v", err)
	}

	values := make([]Value, 10)
	vr := page.Values()
	n, err := vr.ReadValues(values)
	if err != nil && err != io.EOF {
		t.Fatalf("error reading values: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 value, got %d", n)
	}

	if values[0].Int32() != int32(expected) {
		t.Fatalf("expected page to have value %d, got %d", expected, values[0].Int32())
	}
}
