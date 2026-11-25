package parquet

import (
	"bytes"
	"io"
	"reflect"
	"runtime"
	"testing"
	"unsafe"

	"github.com/parquet-go/parquet-go/format"
)

func TestDeltaLengthPagePayloadView(t *testing.T) {
	col := &Column{typ: ByteArrayType}
	for _, tc := range []struct {
		name     string
		n        int
		nonempty bool
	}{
		{name: "zero", n: 0},
		{name: "one-empty", n: 1},
		{name: "one", n: 1, nonempty: true},
		{name: "many-empty", n: 300},
		{name: "many", n: 300, nonempty: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data, offsets := sampleByteArrays(tc.n, tc.nonempty)
			encoded, err := DeltaLengthByteArray.EncodeByteArray(nil, data, offsets)
			if err != nil {
				t.Fatal(err)
			}
			page, full := pooledBytes(encoded)
			origPtr := unsafe.SliceData(full)
			origCap := cap(full)
			headerLen := len(encoded) - len(data)

			result, err := decodeLengthPage(col, &DeltaLengthByteArray, tc.n, page)
			if err != nil {
				page.unref()
				t.Fatal(err)
			}
			assertPayloadView(t, result, page, encoded, data, offsets, headerLen)
			owned := result.(*bufferedPage).values.data.Slice()
			if unsafe.SliceData(owned) != origPtr || cap(owned) != origCap || len(owned) != len(encoded) {
				t.Fatalf("buffer before release len=%d cap=%d, want len=%d cap=%d", len(owned), cap(owned), len(encoded), origCap)
			}
			releaseDecoded(result, page)
			if page.refc.Load() != 0 {
				t.Fatalf("release left refcount %d", page.refc.Load())
			}
		})
	}
}

func TestDeltaLengthPageDetachKeepsValues(t *testing.T) {
	col := &Column{typ: ByteArrayType}
	data, offsets := sampleByteArrays(40, true)
	encoded, err := DeltaLengthByteArray.EncodeByteArray(nil, data, offsets)
	if err != nil {
		t.Fatal(err)
	}
	page, _ := pooledBytes(encoded)
	result, err := decodeLengthPage(col, &DeltaLengthByteArray, 40, page)
	if err != nil {
		page.unref()
		t.Fatal(err)
	}
	page.unref()
	bp := result.(*bufferedPage)
	if bp.values.refc.Load() != 1 {
		t.Fatalf("values refcount = %d, want 1", bp.values.refc.Load())
	}
	before, beforeOffsets := byteArrayData(result)
	if !bytes.Equal(before, data) || !offsetsEqual(beforeOffsets, offsets) {
		t.Fatalf("values = %q %v, want %q %v", before, beforeOffsets, data, offsets)
	}
	releaseAndDetachValues(result)
	if bp.values.refc.Load() != 1 {
		t.Fatalf("detach released the values buffer (refcount %d)", bp.values.refc.Load())
	}
	live := bp.values.data.Slice()

	for range 8 {
		other := buffers.get(len(encoded))
		for i := range other.data.Slice() {
			other.data.Slice()[i] = 0xFF
		}
		other.unref()
	}
	got, _ := byteArrayData(result)
	if !bytes.Equal(got, data) {
		t.Fatal("detached page values were overwritten")
	}
	if len(got) > 0 && !bytesInside(got, live) {
		t.Fatal("detached values left the original buffer")
	}
	bp.values.unref()
}

func TestDeltaLengthPageRejectsCorrupt(t *testing.T) {
	col := &Column{typ: ByteArrayType}
	valid, validOffsets := sampleByteArrays(4, true)
	encoded, err := DeltaLengthByteArray.EncodeByteArray(nil, valid, validOffsets)
	if err != nil {
		t.Fatal(err)
	}
	for _, src := range [][]byte{nil, {}, encoded[:len(encoded)/2]} {
		page, full := pooledBytes(src)
		saved := append([]byte(nil), full...)
		_, err := decodeLengthPage(col, &DeltaLengthByteArray, 4, page)
		if err == nil {
			page.unref()
			t.Fatal("corrupt page decoded")
		}
		if !bytes.Equal(page.data.Slice(), saved) {
			page.unref()
			t.Fatal("failed decode modified the page buffer")
		}
		page.unref()
	}
}

func TestPageDecodeOtherEncodings(t *testing.T) {
	col := &Column{typ: ByteArrayType}
	data, offsets := sampleByteArrays(20, true)

	t.Run("plain", func(t *testing.T) {
		encoded, err := Plain.EncodeByteArray(nil, data, offsets)
		if err != nil {
			t.Fatal(err)
		}
		page, _ := pooledBytes(encoded)
		result, err := decodeLengthPage(col, &Plain, 20, page)
		if err != nil {
			page.unref()
			t.Fatal(err)
		}
		defer releaseDecoded(result, page)
		bp := result.(*bufferedPage)
		if bp.values != page {
			t.Fatal("PLAIN did not decode into the page buffer")
		}
		got, _ := byteArrayData(result)
		if !bytes.Equal(got, data) {
			t.Fatalf("plain values = %q", got)
		}
		if len(got) > 0 && unsafe.SliceData(got) != unsafe.SliceData(bp.values.data.Slice()) {
			t.Fatal("PLAIN values do not start at the page buffer")
		}
	})

	t.Run("delta-byte-array", func(t *testing.T) {
		encoded, err := DeltaByteArray.EncodeByteArray(nil, data, offsets)
		if err != nil {
			t.Fatal(err)
		}
		page, full := pooledBytes(encoded)
		saved := append([]byte(nil), full...)
		result, err := decodeLengthPage(col, &DeltaByteArray, 20, page)
		if err != nil {
			page.unref()
			t.Fatal(err)
		}
		defer releaseDecoded(result, page)
		bp := result.(*bufferedPage)
		if bp.values == page {
			t.Fatal("DELTA_BYTE_ARRAY decoded into the original page buffer")
		}
		if !bytes.Equal(page.data.Slice(), saved) {
			t.Fatal("DELTA_BYTE_ARRAY modified the source page")
		}
		got, _ := byteArrayData(result)
		if !bytes.Equal(got, data) {
			t.Fatalf("delta byte array values = %q", got)
		}
		if len(got) > 0 && unsafe.SliceData(got) != unsafe.SliceData(bp.values.data.Slice()) {
			t.Fatal("copied values do not start at the destination buffer")
		}
	})
}

func TestDeltaLengthPageRoundTrip(t *testing.T) {
	for _, version := range []int{1, 2} {
		t.Run(versionName(version), func(t *testing.T) {
			for _, tc := range pageRoundTripCases() {
				t.Run(tc.name, func(t *testing.T) {
					rows := tc.rows
					var buf bytes.Buffer
					w := NewGenericWriter[pageStringRow](&buf, DataPageVersion(version))
					if len(rows) > 0 {
						if _, err := w.Write(rows); err != nil {
							t.Fatal(err)
						}
					}
					if err := w.Close(); err != nil {
						t.Fatal(err)
					}
					f, err := OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
					if err != nil {
						t.Fatal(err)
					}
					got := readRows[pageStringRow](t, f)
					if !reflect.DeepEqual(got, rows) {
						t.Fatalf("rows = %#v, want %#v", got, rows)
					}
					if tc.checkView && len(rows) > 0 {
						assertFilePayloadView(t, f)
					}
				})
			}
		})
	}
}

func TestDeltaLengthPageOptionalRepeatedPlain(t *testing.T) {
	for _, version := range []int{1, 2} {
		opt := versionName(version)
		t.Run(opt+"/optional", func(t *testing.T) {
			a, empty := "alpha", ""
			rows := []pageOptRow{
				{},
				{V: &empty},
				{V: &a},
				{},
				{V: &a},
			}
			assertRoundTrip(t, rows, DataPageVersion(version))
		})
		t.Run(opt+"/repeated", func(t *testing.T) {
			rows := []pageListRow{
				{V: []string{}},
				{V: []string{"a", ""}},
				{V: []string{"bbbbbbbbbbbbbbbbbbbb", "c", "d"}},
			}
			assertRoundTrip(t, rows, DataPageVersion(version))
		})
		t.Run(opt+"/plain", func(t *testing.T) {
			rows := []pagePlainRow{{V: ""}, {V: "abc"}, {V: "hello"}}
			assertRoundTrip(t, rows, DataPageVersion(version))
		})
		t.Run(opt+"/delta-byte-array", func(t *testing.T) {
			rows := []pageDeltaRow{{V: ""}, {V: "abc"}, {V: "hello-hello"}}
			assertRoundTrip(t, rows, DataPageVersion(version))
		})
		t.Run(opt+"/snappy", func(t *testing.T) {
			text := []pageSnappyRow{{V: ""}, {V: "abc"}, {V: string(bytes.Repeat([]byte("z"), 100))}}
			assertRoundTrip(t, text, DataPageVersion(version))
		})
	}
}

func TestDeltaLengthPageReadAlloc(t *testing.T) {
	const n, width = 2000, 200
	payload := n * width
	f := writeStringFile(t, wideRows(n, width))
	// The values have to be the payload inside the page buffer. A move onto
	// the front of that buffer, or a copy into a new one, fails this.
	scanColumnView(t, f)
	// Warm the buffer pool, then count bytes for later reads. A fresh copy of
	// the payload on every read is at least `payload` bytes. Page headers are
	// not. Do not GC here: that drops the pool and the next read allocates it
	// again, which is unrelated to copying the values.
	scanColumn(t, f)
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	const reads = 8
	for range reads {
		scanColumn(t, f)
	}
	runtime.ReadMemStats(&after)
	cost := (after.TotalAlloc - before.TotalAlloc) / reads
	if cost >= uint64(payload) {
		t.Fatalf("page read allocated %d bytes, payload is %d", cost, payload)
	}
}

type pageStringRow struct {
	V string `parquet:"v"`
}

type pageOptRow struct {
	V *string `parquet:"v,optional"`
}

type pageListRow struct {
	V []string `parquet:"v,list"`
}

type pagePlainRow struct {
	V string `parquet:"v,plain"`
}

type pageDeltaRow struct {
	V string `parquet:"v,delta"`
}

type pageSnappyRow struct {
	V string `parquet:"v,snappy"`
}

type roundTripCase struct {
	name      string
	rows      []pageStringRow
	checkView bool
}

func pageRoundTripCases() []roundTripCase {
	many := make([]pageStringRow, 300)
	manyEmpty := make([]pageStringRow, 300)
	for i := range many {
		if i%4 == 0 {
			many[i].V = ""
			continue
		}
		many[i].V = string(bytes.Repeat([]byte{'a' + byte(i%26)}, 8+i%5))
	}
	return []roundTripCase{
		{name: "zero", rows: nil},
		{name: "one-empty", rows: []pageStringRow{{}}, checkView: true},
		{name: "one", rows: []pageStringRow{{V: "hello-hello-hello"}}, checkView: true},
		{name: "many-empty", rows: manyEmpty, checkView: true},
		{name: "many", rows: many, checkView: true},
	}
}

func decodeLengthPage(col *Column, enc interface{ Encoding() format.Encoding }, n int, page *buffer[byte]) (Page, error) {
	header := DataPageHeaderV2{header: &format.DataPageHeaderV2{
		NumValues: int32(n),
		NumRows:   int32(n),
		Encoding:  enc.Encoding(),
	}}
	return col.decodeDataPage(header, n, nil, nil, page, page.data.Slice(), nil)
}

func pooledBytes(encoded []byte) (*buffer[byte], []byte) {
	page := buffers.get(len(encoded))
	dst := page.data.Slice()
	copy(dst, encoded)
	if len(encoded) < len(dst) {
		// buffers.get may round the length up only when the pool grows; the
		// requested length is len(encoded). Extra cap stays past len.
	}
	return page, dst[:len(encoded)]
}

func assertPayloadView(t *testing.T, result Page, page *buffer[byte], encoded, data []byte, offsets []uint32, headerLen int) {
	t.Helper()
	bp := result.(*bufferedPage)
	if bp.values != page {
		t.Fatal("page does not own the original buffer")
	}
	full := bp.values.data.Slice()
	if cap(full) != cap(page.data.Slice()) {
		t.Fatal("buffer capacity changed")
	}
	if headerLen > len(full) || !bytes.Equal(full[:headerLen], encoded[:headerLen]) {
		t.Fatal("length header was overwritten")
	}
	got, gotOffsets := byteArrayData(result)
	if !bytes.Equal(got, data) || !offsetsEqual(gotOffsets, offsets) {
		t.Fatalf("values = %q %v", got, gotOffsets)
	}
	if len(data) == 0 {
		return
	}
	payload := full[headerLen : headerLen+len(data)]
	if unsafe.SliceData(got) != unsafe.SliceData(payload) {
		t.Fatal("values are not the payload view")
	}
	if cap(got) != len(got) {
		t.Fatalf("payload cap = %d, want %d", cap(got), len(got))
	}
}

func assertFilePayloadView(t *testing.T, f *File) {
	t.Helper()
	pages := f.RowGroups()[0].ColumnChunks()[0].Pages()
	defer pages.Close()
	var saw bool
	for {
		page, err := pages.ReadPage()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		bp := page.(*bufferedPage)
		full := bp.values.data.Slice()
		values, _ := byteArrayData(page)
		if len(values) > 0 {
			saw = true
			if unsafe.SliceData(values) == unsafe.SliceData(full) {
				Release(page)
				t.Fatal("page values were moved to the start of the buffer")
			}
			if !bytesInside(values, full) {
				Release(page)
				t.Fatal("page values are outside the page buffer")
			}
		}
		if cap(full) < len(full) {
			Release(page)
			t.Fatal("page buffer cap is smaller than its length")
		}
		Release(page)
	}
	if !saw {
		// Empty values have no payload bytes to view. The buffer must still
		// be the page allocation, which the cap check above covers.
	}
}

func releaseDecoded(result Page, input *buffer[byte]) {
	Release(result)
	input.unref()
}

func sampleByteArrays(n int, nonempty bool) ([]byte, []uint32) {
	offsets := make([]uint32, n+1)
	if n == 0 || !nonempty {
		return nil, offsets
	}
	var data []byte
	for i := range n {
		offsets[i] = uint32(len(data))
		// Keep a mix of empty and non-empty values once there is more than one.
		// A single value has to be non-empty so the payload view is observable.
		if n > 1 && i%6 == 0 {
			continue
		}
		data = append(data, byte('a'+i%26))
		if i%3 == 0 {
			data = append(data, bytes.Repeat([]byte{'Q'}, 24)...)
		}
	}
	offsets[n] = uint32(len(data))
	return data, offsets
}

func byteArrayData(page Page) ([]byte, []uint32) {
	values := page.Data()
	return values.ByteArray()
}

func offsetsEqual(got, want []uint32) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func bytesInside(inner, outer []byte) bool {
	if len(inner) == 0 {
		return true
	}
	start := uintptr(unsafe.Pointer(unsafe.SliceData(inner)))
	base := uintptr(unsafe.Pointer(unsafe.SliceData(outer)))
	end := base + uintptr(len(outer))
	return start >= base && start+uintptr(len(inner)) <= end
}

func versionName(v int) string {
	if v == 1 {
		return "v1"
	}
	return "v2"
}

func readRows[T any](t *testing.T, f *File) []T {
	t.Helper()
	r := NewGenericReader[T](f)
	defer r.Close()
	var got []T
	buf := make([]T, 64)
	for {
		n, err := r.Read(buf)
		got = append(got, buf[:n]...)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			break
		}
	}
	return got
}

func assertRoundTrip[T any](t *testing.T, rows []T, opts ...WriterOption) {
	t.Helper()
	var buf bytes.Buffer
	w := NewGenericWriter[T](&buf, opts...)
	if len(rows) > 0 {
		if _, err := w.Write(rows); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}
	got := readRows[T](t, f)
	if !reflect.DeepEqual(got, rows) {
		t.Fatalf("rows = %#v, want %#v", got, rows)
	}
}

func wideRows(n, width int) []pageStringRow {
	rows := make([]pageStringRow, n)
	for i := range rows {
		rows[i].V = string(bytes.Repeat([]byte{'m'}, width))
	}
	return rows
}

func writeStringFile(t *testing.T, rows []pageStringRow) *File {
	t.Helper()
	var buf bytes.Buffer
	w := NewGenericWriter[pageStringRow](&buf)
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func scanColumn(t *testing.T, f *File) {
	t.Helper()
	scanColumnFunc(t, f, nil)
}

func scanColumnView(t *testing.T, f *File) {
	t.Helper()
	var saw bool
	scanColumnFunc(t, f, func(page Page) {
		bp := page.(*bufferedPage)
		full := bp.values.data.Slice()
		values, _ := byteArrayData(page)
		if len(values) == 0 {
			return
		}
		saw = true
		if unsafe.SliceData(values) == unsafe.SliceData(full) {
			t.Fatal("page values were moved to the start of the buffer")
		}
		if !bytesInside(values, full) {
			t.Fatal("page values are outside the page buffer")
		}
		if cap(full) <= len(values) {
			t.Fatalf("page buffer cap = %d, payload = %d", cap(full), len(values))
		}
	})
	if !saw {
		t.Fatal("large page had no payload")
	}
}

func scanColumnFunc(t *testing.T, f *File, each func(Page)) {
	t.Helper()
	pages := f.RowGroups()[0].ColumnChunks()[0].Pages()
	defer pages.Close()
	for {
		page, err := pages.ReadPage()
		if err == io.EOF {
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		if each != nil {
			each(page)
		}
		Release(page)
	}
}
