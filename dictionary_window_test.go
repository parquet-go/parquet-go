package parquet

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"slices"
	"testing"

	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"
)

func TestWindowedDictionaryPage(t *testing.T) {
	dict := newTestByteArrayDictionary("a", "b", "c")
	ids := make([]int32, 3*1024)
	for i := 1024; i < len(ids); i++ {
		ids[i] = int32(i % 3)
	}
	encoded, err := RLEDictionary.EncodeInt32(nil, ids)
	if err != nil {
		t.Fatal(err)
	}
	ids = append(ids, 0, 0, 0) // a clean short stream is zero-extended
	page, err := newWindowedIndexedPage(newIndexedType(ByteArrayType, dict), 0, len(ids), encoded)
	if err != nil {
		t.Fatal(err)
	}
	for _, span := range [][2]int64{{0, int64(len(ids))}, {997, 1080}, {1917, 2301}, {3071, 3075}} {
		p := page.Slice(span[0], span[1]).(*windowedIndexedPage)
		want := ids[span[0]:span[1]]
		r := p.Values().(*windowedIndexedPageValues)
		values := make([]Value, 127)
		var read int
		for {
			n, err := r.ReadValues(values)
			for i, value := range values[:n] {
				if string(value.ByteArray()) != string(dict.Index(want[read+i]).ByteArray()) {
					t.Fatalf("slice %v: value %d = %v", span, read+i, value)
				}
			}
			read += n
			if err == io.EOF {
				break
			}
			if err != nil || n == 0 {
				t.Fatalf("read = %d, %v", n, err)
			}
		}
		if read != len(want) || len(*r.window) > 128 {
			t.Fatalf("read %d values using %d indexes", read, len(*r.window))
		}
		lo, hi, ok := p.Bounds()
		if !ok || string(lo.ByteArray()) != string(dict.Index(slices.Min(want)).ByteArray()) || string(hi.ByteArray()) != string(dict.Index(slices.Max(want)).ByteArray()) {
			t.Fatalf("slice %v: bounds = %v, %v, %v", span, lo, hi, ok)
		}
		if p.materialized != nil {
			t.Fatal("Values or Bounds materialized the index array")
		}
		data := p.Data()
		if got := data.Int32(); !slices.Equal(got, want) {
			t.Fatalf("slice %v: Data indexes differ", span)
		}
	}
}

func TestWindowedDictionarySparseSlices(t *testing.T) {
	const runs = 5*dictionaryRunsPerCheckpoint + 1
	data := []byte{1} // one-bit IDs
	var ids []int32
	var wantCheckpoints []dictionaryRunCheckpoint
	for run := range runs {
		if run > 0 && run%dictionaryRunsPerCheckpoint == 0 {
			wantCheckpoints = append(wantCheckpoints, dictionaryRunCheckpoint{row: int64(len(ids)), offset: len(data) - 1})
			data = append(data, 0) // a zero-length run at the checkpoint
		}
		if run%3 == 0 {
			data = append(data, 3, 0x55) // one group of eight alternating IDs
			ids = append(ids, 1, 0, 1, 0, 1, 0, 1, 0)
		} else {
			id, n := run%2, run%9+1
			data = binary.AppendUvarint(data, uint64(n*2))
			data = append(data, byte(id))
			for range n {
				ids = append(ids, int32(id))
			}
		}
	}
	encodedRows := int64(len(ids))
	ids = append(ids, 0, 0, 0) // clean short stream
	page, err := newWindowedIndexedPage(newIndexedType(ByteArrayType, newTestByteArrayDictionary("a", "b")), 0, len(ids), data)
	if err != nil {
		t.Fatal(err)
	}
	p := page.(*windowedIndexedPage)
	if !slices.Equal(p.checkpoints, wantCheckpoints) {
		t.Fatalf("checkpoints = %v, want %v", p.checkpoints, wantCheckpoints)
	}
	check := func(page Page, start int64) {
		t.Helper()
		// Visit slices out of order; nested slices may start mid-run or beyond EOF.
		for i := page.NumRows(); i >= 0; i-- {
			j := min(i+13, page.NumRows())
			data := page.Slice(i, j).Data()
			if got, want := data.Int32(), ids[start+i:start+j]; !slices.Equal(got, want) {
				t.Fatalf("slice [%d,%d) = %v, want %v", start+i, start+j, got, want)
			}
		}
	}
	check(p, 0)
	for _, start := range []int64{1, p.checkpoints[2].row + 1, encodedRows - 1, encodedRows + 1} {
		check(p.Slice(start, p.NumRows()), start)
	}
	if p.materialized != nil {
		t.Fatal("slicing materialized the parent index array")
	}
}

func TestWindowedDictionaryRejectsTruncatedRun(t *testing.T) {
	typ := newIndexedType(ByteArrayType, newTestByteArrayDictionary("a", "b"))
	data := make([]byte, 2, 64) // a packed run whose byte is only in spare capacity
	copy(data, []byte{1, 3})
	_, eager := RLEDictionary.DecodeInt32(nil, data)
	_, windowed := newWindowedIndexedPage(typ, 0, 1, data)
	if !errors.Is(eager, io.ErrUnexpectedEOF) || !errors.Is(windowed, io.ErrUnexpectedEOF) {
		t.Fatalf("eager = %v, windowed = %v; want unexpected EOF", eager, windowed)
	}
}

func TestWindowedDictionaryExportedPageOwnership(t *testing.T) {
	dict := newTestByteArrayDictionary("a", "b")
	for _, version := range []int{1, 2} {
		for _, compressed := range []bool{false, true} {
			t.Run(fmt.Sprintf("v%d/compressed=%t", version, compressed), func(t *testing.T) {
				data := []byte{1, 4, 1} // two copies of "b"
				col := &Column{typ: ByteArrayType}
				var err error
				if compressed {
					col.compression = &Snappy
					data, err = Snappy.Encode(nil, data)
					if err != nil {
						t.Fatal(err)
					}
				}
				var p Page
				if version == 1 {
					p, err = col.DecodeDataPageV1(DataPageHeaderV1{header: &format.DataPageHeader{NumValues: 2, Encoding: format.RLEDictionary}}, data, dict)
				} else {
					p, err = col.DecodeDataPageV2(DataPageHeaderV2{header: &format.DataPageHeaderV2{NumValues: 2, NumRows: 2, Encoding: format.RLEDictionary, IsCompressed: thrift.New(compressed)}}, data, dict)
				}
				if err != nil {
					t.Fatal(err)
				}
				clear(data)
				s := p.Slice(1, 2)
				Release(p)
				defer Release(s)
				values := make([]Value, 1)
				if n, err := s.Values().ReadValues(values); n != 1 || err != io.EOF || string(values[0].ByteArray()) != "b" {
					t.Fatalf("slice read = %d, %v: %v", n, err, values)
				}
			})
		}
	}
}

func TestWindowedDictionaryDetach(t *testing.T) {
	col := &Column{typ: ByteArrayType}
	dict := newTestByteArrayDictionary("a", "b")
	buf := buffers.get(3)
	copy(buf.data.Slice(), []byte{1, 2, 1})
	p, err := col.decodeDataPageV1(DataPageHeaderV1{header: &format.DataPageHeader{NumValues: 1, Encoding: format.RLEDictionary}}, buf, dict, -1)
	buf.unref()
	if err != nil {
		t.Fatal(err)
	}
	values := make([]Value, 1)
	if n, err := p.Values().ReadValues(values); n != 1 || err != io.EOF {
		t.Fatalf("read = %d, %v", n, err)
	}
	releaseAndDetachValues(p)
	if got := buf.refc.Load(); got != 0 || string(values[0].ByteArray()) != "b" {
		t.Fatalf("encoded buffer refs = %d, retained value = %v", got, values[0])
	}
}

func TestWindowedDictionaryFile(t *testing.T) {
	type row struct {
		Label    string  `parquet:"label,dict"`
		Optional *string `parquet:"optional,dict"`
	}
	var buf bytes.Buffer
	label := "optional"
	want := make([]row, 4096)
	for i := range want {
		want[i].Label = fmt.Sprint(i % 11)
		if i%2 == 0 {
			want[i].Optional = &label
		}
	}
	w := NewGenericWriter[row](&buf, PageBufferSize(1024), Compression(&Snappy))
	if n, err := w.Write(want); n != len(want) || err != nil {
		t.Fatalf("write = %d, %v", n, err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}
	for i, chunk := range f.RowGroups()[0].ColumnChunks() {
		pages := chunk.Pages()
		p, err := pages.ReadPage()
		if err != nil {
			t.Fatal(err)
		}
		_, windowed := p.(*bufferedPage).Page.(*windowedIndexedPage)
		Release(p)
		pages.Close()
		if windowed != (i == 0) {
			t.Fatalf("column %d: windowed = %t", i, windowed)
		}
	}
	r := NewGenericReader[row](f)
	defer r.Close()
	got := make([]row, len(want))
	if n, err := r.Read(got); n != len(want) || (err != nil && err != io.EOF) {
		t.Fatalf("read = %d, %v", n, err)
	}
	for i, v := range got {
		if v.Label != want[i].Label || (v.Optional == nil) != (want[i].Optional == nil) || (v.Optional != nil && *v.Optional != label) {
			t.Fatalf("row %d = %+v, want %+v", i, v, want[i])
		}
	}
}
