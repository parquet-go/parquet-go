package parquet

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/parquet-go/parquet-go/encoding"
	"github.com/parquet-go/parquet-go/format"
)

func windowTestDictionary() *indexedType {
	d := ByteArrayType.NewDictionary(0, 256, ByteArrayType.NewValues(nil, nil))
	values := make([]Value, 256)
	for i := range values {
		values[i] = ByteArrayValue([]byte(fmt.Sprintf("label-%03d", i)))
	}
	d.Insert(make([]int32, len(values)), values)
	return newIndexedType(ByteArrayType, d)
}

func appendWindowRun(data []byte, count int, id uint32, width uint) []byte {
	data = binary.AppendUvarint(data, uint64(count)<<1)
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], id)
	return append(data, b[:(width+7)/8]...)
}

func appendWindowPacked(data []byte, ids []int32, width uint) []byte {
	groups := (len(ids) + 7) / 8
	data = binary.AppendUvarint(data, uint64(groups)<<1|1)
	start := len(data)
	data = append(data, make([]byte, groups*int(width))...)
	for i, id := range ids {
		for bit := uint(0); bit < width; bit++ {
			if uint32(id)&(1<<bit) != 0 {
				pos := uint(i)*width + bit
				data[start+int(pos/8)] |= 1 << (pos % 8)
			}
		}
	}
	return data
}

// assertWindowPageParity compares got against the eager page want through
// Values (read in batches of the given size), Bounds and Data.
func assertWindowPageParity(t testing.TB, want, got Page, batch int) {
	t.Helper()
	wr, gr := want.Values(), got.Values()
	wv, gv := make([]Value, batch), make([]Value, batch)
	for {
		wn, we := wr.ReadValues(wv)
		gn, ge := gr.ReadValues(gv)
		if wn != gn || we != ge || !reflect.DeepEqual(wv[:wn], gv[:gn]) {
			t.Fatalf("Values mismatch: %d/%v, %d/%v", wn, we, gn, ge)
		}
		if we == io.EOF {
			break
		}
	}
	wl, wh, wok := want.Bounds()
	gl, gh, gok := got.Bounds()
	if wok != gok || !reflect.DeepEqual(wl, gl) || !reflect.DeepEqual(wh, gh) {
		t.Fatalf("Bounds mismatch: %v..%v, %v..%v", wl, wh, gl, gh)
	}
	wd, gd := want.Data(), got.Data()
	if !reflect.DeepEqual(wd.Int32(), gd.Int32()) {
		t.Fatal("Data mismatch")
	}
}

func TestWindowedDictionaryPageParity(t *testing.T) {
	typ := windowTestDictionary()
	type stream struct {
		name  string
		data  []byte
		count int
	}
	var streams []stream
	// Long runs and packed spans that cross batch and window boundaries, with
	// unpadded tails.
	for _, width := range []uint{0, 1, 2, 3, 7, 8, 9, 16, 31, 32} {
		mask := int32(255)
		if width < 8 {
			mask = int32(1<<width) - 1
		}
		ids := make([]int32, 9003)
		for i := range ids {
			ids[i] = int32(i*17) & mask
		}
		data := appendWindowRun([]byte{byte(width)}, 8193, uint32(mask), width)
		data = appendWindowPacked(data, ids, width)
		data = appendWindowRun(data, 4101, 0, width)
		// Five packed padding IDs are logical values here, not surplus.
		streams = append(streams, stream{fmt.Sprintf("runs/width%d", width), data[:len(data):len(data)], 8193 + 9008 + 4101})
	}
	// Clean short streams are zero-extended to the page count.
	for _, width := range []uint{0, 1, 3, 8, 9, 16, 31, 32} {
		id := uint32(min(width, 1))
		for _, prefix := range []string{"empty", "rle", "packed", "mixed"} {
			data := []byte{byte(width)}
			if prefix == "rle" || prefix == "mixed" {
				data = appendWindowRun(data, 3, id, width)
			}
			if prefix == "packed" || prefix == "mixed" {
				data = appendWindowPacked(data, []int32{int32(id), 0, int32(id)}, width)
			}
			streams = append(streams, stream{fmt.Sprintf("short/width%d/%s", width, prefix), data, 43})
		}
	}
	// Writer-produced short runs with late extremes and a zero-extended tail.
	for _, run := range []int{1, 3, 8, 64} {
		ids := make([]int32, 5*dictionaryIndexWindowSize+11)
		for i := range ids {
			ids[i] = int32(17 + (i/run*31)%200)
		}
		ids[len(ids)-20], ids[len(ids)-40] = 255, 2
		data, err := RLEDictionary.EncodeInt32(nil, ids)
		if err != nil {
			t.Fatal(err)
		}
		streams = append(streams, stream{fmt.Sprintf("encoded/run%d", run), data, len(ids) + 9})
	}

	for _, s := range streams {
		t.Run(s.name, func(t *testing.T) {
			decoded, err := RLEDictionary.DecodeInt32(nil, s.data)
			if err != nil {
				t.Fatal(err)
			}
			decoded = append(decoded, make([]int32, max(0, s.count-len(decoded)))...)[:s.count]
			eager := newIndexedPage(typ, makeColumnIndex(0), int32(s.count), encoding.Int32Values(decoded))
			lazy, err := newWindowedIndexedPage(typ, 0, s.count, s.data)
			if err != nil {
				t.Fatal(err)
			}
			for _, batch := range []int{1, 3, 4097} {
				assertWindowPageParity(t, eager, lazy, batch)
			}
			n := int64(s.count)
			for _, span := range [][2]int64{{0, 0}, {1, n - 1}, {n - 3, n}, {n / 3, n / 2}} {
				assertWindowPageParity(t, eager.Slice(span[0], span[1]), lazy.Slice(span[0], span[1]), 7)
			}
		})
	}
}

func TestWindowedDictionaryNoMaterialization(t *testing.T) {
	typ := windowTestDictionary()
	ids := make([]int32, 10001)
	for i := range ids {
		ids[i] = int32(i % 256)
	}
	data, err := RLEDictionary.EncodeInt32(nil, ids)
	if err != nil {
		t.Fatal(err)
	}
	// The dictionary belongs to column 0, so values are relabeled for column 2.
	page, err := newWindowedIndexedPage(typ, 2, len(ids), data)
	if err != nil {
		t.Fatal(err)
	}
	r := page.Values()
	held := make([]Value, 3)
	r.ReadValues(held)
	clone := Row(held).Clone()
	if held[0].Column() != 2 || string(held[2].ByteArray()) != "label-002" {
		t.Fatalf("value %v", held[2])
	}
	for {
		_, err := r.ReadValues(make([]Value, 5001))
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(Row(held), clone) {
		t.Fatal("reused index window escaped into values")
	}
	slice := page.Slice(7, 9000)
	page.Bounds()
	slice.Bounds()
	if page.(*windowedIndexedPage).materialized != nil || slice.(*windowedIndexedPage).materialized != nil {
		t.Fatal("ReadValues or Bounds materialized the index array")
	}
	// Data stays compatible with page encoders.
	encoded, err := slice.Type().Encode(nil, slice.Data(), &RLEDictionary)
	if err != nil {
		t.Fatal(err)
	}
	if decoded, err := RLEDictionary.DecodeInt32(nil, encoded); err != nil || !reflect.DeepEqual(decoded[:8993], ids[7:9000]) {
		t.Fatalf("Data encoding: %v", err)
	}
}

func TestWindowedDictionaryValidation(t *testing.T) {
	typ := windowTestDictionary()
	tests := []struct {
		name  string
		data  []byte
		count int
		valid bool
	}{
		{"zero legacy", nil, 100, true}, {"zero width", []byte{0}, 100, true},
		{"width", []byte{33}, 1, false}, {"header truncated", []byte{1, 128}, 1, false},
		{"header overflow", append([]byte{1}, bytes.Repeat([]byte{255}, 11)...), 1, false},
		{"empty nonzero", []byte{1}, 1, true}, {"short run", []byte{1, 2, 0}, 2, true},
		{"run truncated", []byte{8, 2}, 1, false}, {"packed truncated", []byte{8, 3, 0}, 1, false},
		{"id out of range", appendWindowRun([]byte{16}, 1, 256, 16), 1, false},
		{"negative id", appendWindowRun([]byte{32}, 1, 0xffffffff, 32), 1, false},
		{"packed invalid id", appendWindowPacked([]byte{16}, []int32{256}, 16), 1, false},
		{"invalid padding ignored", appendWindowPacked([]byte{16}, []int32{1, 256}, 16), 1, true},
		{"zero count runs", []byte{1, 0, 0, 2, 1}, 1, true},
		{"truncated after count", []byte{1, 2, 0, 128}, 1, false},
		{"oversized run", appendWindowRun([]byte{1}, 1<<30, 0, 1), 7, true},
	}
	// Runs after the logical count are still structurally checked.
	for _, width := range []uint{0, 1, 8, 16, 32} {
		tails := [][]byte{{128}, bytes.Repeat([]byte{255}, 11)}
		if width > 0 {
			tails = append(tails, []byte{2}, []byte{3})
		}
		for i, tail := range tails {
			data := append(appendWindowRun([]byte{byte(width)}, 1, 0, width), tail...)
			tests = append(tests, struct {
				name  string
				data  []byte
				count int
				valid bool
			}{fmt.Sprintf("surplus width%d/%d", width, i), data, 1, false})
		}
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := newWindowedIndexedPage(typ, 0, tt.count, tt.data)
			if (err == nil) != tt.valid {
				t.Fatalf("valid=%v, err=%v", tt.valid, err)
			}
		})
	}
	// Synthesized zeros need a dictionary entry.
	empty := newIndexedType(ByteArrayType, ByteArrayType.NewDictionary(0, 0, ByteArrayType.NewValues(nil, nil)))
	for _, width := range []byte{0, 8, 32} {
		if _, err := newWindowedIndexedPage(empty, 0, 1, []byte{width}); err == nil {
			t.Fatal("accepted synthesized zero without a dictionary entry")
		}
	}
	// Invalid packed IDs at every max-reduction lane, the scalar tail and
	// across validation windows are reported exactly.
	for _, width := range []uint{16, 32} {
		bad := int32(256)
		if width == 32 {
			bad = -1
		}
		for _, pos := range []int{0, 1, 2, 3, 5, dictionaryIndexWindowSize - 1, dictionaryIndexWindowSize, 2*dictionaryIndexWindowSize + 7} {
			ids := make([]int32, 2*dictionaryIndexWindowSize+8)
			for i := range ids {
				ids[i] = int32(i % 256)
			}
			ids[pos] = bad
			_, err := newWindowedIndexedPage(typ, 0, len(ids), appendWindowPacked([]byte{byte(width)}, ids, width))
			if want := fmt.Sprintf("invalid dictionary index %d", bad); err == nil || err.Error() != want {
				t.Fatalf("width%d/pos%d: %v", width, pos, err)
			}
		}
	}
}

func TestWindowedDictionaryUnvalidatedStream(t *testing.T) {
	// Pages are only built after validation; bypass it to prove a broken
	// invariant fails loudly instead of zero-filling or looping.
	typ := windowTestDictionary()
	truncated := appendWindowRun([]byte{16}, 2, 1, 16)[:3]
	for name, data := range map[string][]byte{"width": {33}, "truncated": truncated} {
		for _, op := range []string{"Data", "Bounds"} {
			t.Run(name+"/"+op, func(t *testing.T) {
				p := &windowedIndexedPage{typ: typ, data: data, count: 4}
				defer func() {
					if recover() == nil {
						t.Fatal("expected panic")
					}
				}()
				if op == "Data" {
					p.Data()
				} else {
					p.Bounds()
				}
			})
		}
	}
	// ReadValues returns stream errors and leaves the output untouched.
	p := &windowedIndexedPage{typ: typ, data: truncated, count: 4}
	values := []Value{ByteArrayValue([]byte("unchanged"))}
	if n, err := p.Values().ReadValues(values); n != 0 || err != io.ErrUnexpectedEOF || string(values[0].ByteArray()) != "unchanged" {
		t.Fatalf("ReadValues = %d, %v", n, err)
	}
}

func TestWindowedDictionaryWindowSizing(t *testing.T) {
	typ := windowTestDictionary()
	ids := make([]int32, 3*dictionaryIndexWindowSize)
	for i := range ids {
		ids[i] = int32(i % 256)
	}
	packed := appendWindowPacked([]byte{8}, ids, 8)
	rle := appendWindowRun([]byte{8}, len(ids), 7, 8)
	for _, tt := range []struct {
		name        string
		data        []byte
		batch, want int
	}{
		{"packed small batch", packed, 170, 176},
		{"packed batch capped", packed, 5 * dictionaryIndexWindowSize, dictionaryIndexWindowSize},
		{"rle never allocates", rle, 170, 0},
		{"clean eof never allocates", []byte{8}, 170, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			p, err := newWindowedIndexedPage(typ, 0, len(ids), tt.data)
			if err != nil {
				t.Fatal(err)
			}
			r := p.Values().(*windowedIndexedPageValues)
			values := make([]Value, tt.batch)
			for {
				_, err := r.ReadValues(values)
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			if got := len(*r.window); got != tt.want {
				t.Fatalf("window %d, want %d", got, tt.want)
			}
		})
	}
}

func TestWindowedDictionaryConcurrentReaders(t *testing.T) {
	typ := windowTestDictionary()
	ids := make([]int32, 9003)
	for i := range ids {
		ids[i] = int32(i % 256)
	}
	p, err := newWindowedIndexedPage(typ, 0, len(ids), appendWindowPacked([]byte{9}, ids, 9))
	if err != nil {
		t.Fatal(err)
	}
	want := newIndexedPage(typ, makeColumnIndex(0), int32(len(ids)), encoding.Int32Values(ids))
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			assertWindowPageParity(t, want, p, 3)
			assertWindowPageParity(t, want.Slice(3, 8001), p.Slice(3, 8001), 3)
		}()
	}
	wg.Wait()
}

func TestWindowedDictionaryExportedSourceOwnership(t *testing.T) {
	typ := windowTestDictionary()
	c := &Column{typ: ByteArrayType, file: &File{config: &FileConfig{WindowedDictionaryReading: true}}}
	for _, version := range []int{1, 2} {
		for _, mutation := range []string{"id", "header", "payload"} {
			t.Run(fmt.Sprintf("v%d/%s", version, mutation), func(t *testing.T) {
				data := appendWindowRun([]byte{16}, 2, 1, 16)
				var p Page
				var err error
				if version == 1 {
					p, err = c.DecodeDataPageV1(DataPageHeaderV1{header: &format.DataPageHeader{NumValues: 2, Encoding: format.RLEDictionary}}, data, typ.dict)
				} else {
					p, err = c.DecodeDataPageV2(DataPageHeaderV2{header: &format.DataPageHeaderV2{NumValues: 2, NumRows: 2, Encoding: format.RLEDictionary}}, data, typ.dict)
				}
				if err != nil {
					t.Fatal(err)
				}
				defer Release(p)
				slice := p.Slice(1, 2)
				defer Release(slice)
				switch mutation {
				case "id":
					data[2], data[3] = 0, 1 // ID 256 is outside the dictionary.
				case "header":
					data[1] = 2 // Clean short run after validation.
				case "payload":
					clear(data) // Even the width and run framing may be reused.
				}
				want := newIndexedPage(typ, makeColumnIndex(c.Index()), 2, encoding.Int32Values([]int32{1, 1}))
				assertWindowPageParity(t, want, p, 3)
				assertWindowPageParity(t, want.Slice(1, 2), slice, 3)
			})
		}
	}
}

func TestWindowedDictionaryEncodedBufferDetach(t *testing.T) {
	typ := windowTestDictionary()
	data := appendWindowRun([]byte{8}, 10, 1, 8)
	buf := buffers.get(len(data))
	copy(buf.data.Slice(), data)
	c := &Column{typ: ByteArrayType, file: &File{config: &FileConfig{WindowedDictionaryReading: true}}}
	page, err := c.decodeDataPageV2(DataPageHeaderV2{header: &format.DataPageHeaderV2{NumValues: 10, Encoding: format.RLEDictionary}}, buf, typ.dict, -1)
	if err != nil {
		t.Fatal(err)
	}
	p := page.(*bufferedDictionaryPage)
	if p.encoded != buf || &p.Page.(*windowedIndexedPage).data[0] != &buf.data.Slice()[0] {
		t.Fatal("owned file buffer was copied or classified as borrowed contents")
	}
	slice := p.Slice(1, 3)
	Retain(p) // Simulate a cache reference independent of the caller.
	buf.unref()
	values := make([]Value, 10)
	p.Values().ReadValues(values)
	want := Row(values).Clone()
	releaseAndDetachValues(p)
	if buf.refc.Load() != 2 {
		t.Fatal("detach did not release encoded storage normally")
	}
	Release(p)
	assertWindowPageParity(t, newIndexedPage(typ, makeColumnIndex(c.Index()), 2, encoding.Int32Values([]int32{1, 1})), slice, 3)
	releaseAndDetachValues(slice)
	if buf.refc.Load() != 0 {
		t.Fatal("slice abandoned encoded storage")
	}
	if !reflect.DeepEqual(Row(values), want) {
		t.Fatal("detaching encoded bytes invalidated borrowed dictionary values")
	}
}

func windowTestFile(t testing.TB, width, rows, pageSize, version int) []byte {
	t.Helper()
	group := Group{}
	for i := 0; i < width; i++ {
		group[fmt.Sprintf("label_%03d", i)] = Encoded(String(), &RLEDictionary)
	}
	group["plain"] = String()
	group["optional"] = Optional(Encoded(String(), &RLEDictionary))
	group["number"] = Encoded(Leaf(Int64Type), &RLEDictionary)
	group["repeated"] = Repeated(Encoded(String(), &RLEDictionary))
	schema := NewSchema("synthetic", group)
	var buf bytes.Buffer
	w := NewWriter(&buf, schema, PageBufferSize(pageSize), DataPageVersion(version), Compression(&Snappy), MaxRowsPerRowGroup(int64(rows/2+1)))
	batch := make([]Row, 17)
	for start := 0; start < rows; {
		n := min(len(batch), rows-start)
		for j := 0; j < n; j++ {
			row := make(Row, width+4)
			for col, path := range schema.Columns() {
				switch path[0] {
				case "number":
					row[col] = Int64Value(int64((start+j)%13)).Level(0, 0, col)
				case "repeated":
					row[col] = ByteArrayValue([]byte("repeated")).Level(0, 1, col)
				case "optional":
					if (start+j)%3 == 0 {
						row[col] = NullValue().Level(0, 0, col)
					} else {
						row[col] = ByteArrayValue([]byte("optional")).Level(0, 1, col)
					}
				default:
					row[col] = ByteArrayValue([]byte(fmt.Sprintf("value-%02d", (start+j)/41%13))).Level(0, 0, col)
				}
			}
			batch[j] = row
		}
		if _, err := w.WriteRows(batch[:n]); err != nil {
			t.Fatal(err)
		}
		start += n
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestWindowedDictionaryFileParity(t *testing.T) {
	for _, version := range []int{1, 2} {
		for _, pageSize := range []int{1024, 1 << 20} {
			t.Run(fmt.Sprintf("v%d/page%d", version, pageSize), func(t *testing.T) {
				data := windowTestFile(t, 32, 9001, pageSize, version)
				mode := FileReadMode(ReadModeSync)
				if version == 2 {
					mode = FileReadMode(ReadModeAsync)
				}
				base, err := OpenFile(bytes.NewReader(data), int64(len(data)), mode)
				if err != nil {
					t.Fatal(err)
				}
				file, err := OpenFile(bytes.NewReader(data), int64(len(data)), WindowedDictionaryReading(true), mode)
				if err != nil {
					t.Fatal(err)
				}
				// Only supported columns take the lazy path, and only when enabled.
				// A slice outlives its page and the closed page iterator.
				for i, chunk := range file.RowGroups()[0].ColumnChunks() {
					for _, f := range []*File{base, file} {
						pages := f.RowGroups()[0].ColumnChunks()[i].Pages()
						p, err := pages.ReadPage()
						if err != nil {
							t.Fatal(err)
						}
						_, lazy := p.(*bufferedDictionaryPage)
						path := chunk.Column()
						name := file.Schema().Columns()[path][0]
						expected := f == file && name != "plain" && name != "optional" && name != "number" && name != "repeated"
						if lazy != expected {
							t.Fatalf("%s lazy=%v", name, lazy)
						}
						slice := p.Slice(0, min(p.NumValues(), 8))
						Release(p)
						pages.Close()
						if _, err := slice.Values().ReadValues(make([]Value, 8)); err != nil && err != io.EOF {
							t.Fatal(err)
						}
						Release(slice)
					}
				}
				a, b := NewReader(base), NewReader(file)
				defer a.Close()
				ar, br := make([]Row, 127), make([]Row, 127)
				total := 0
				for {
					an, ae := a.ReadRows(ar)
					bn, be := b.ReadRows(br)
					if an != bn || ae != be || !reflect.DeepEqual(ar[:an], br[:bn]) {
						t.Fatalf("full scan parity at %d", total)
					}
					total += bn
					if be == io.EOF {
						break
					}
					if be != nil {
						t.Fatal(be)
					}
				}
				if total != 9001 {
					t.Fatal(total)
				}
				var held, want []Row
				for _, seek := range []int64{0, 4095, 8995, 33, 4500, 0} {
					if err := a.SeekToRow(seek); err != nil {
						t.Fatal(err)
					}
					if err := b.SeekToRow(seek); err != nil {
						t.Fatal(err)
					}
					ar, br := ar[:7], br[:7]
					for k := 0; k < 9; k++ {
						an, ae := a.ReadRows(ar)
						bn, be := b.ReadRows(br)
						if an != bn || ae != be || !reflect.DeepEqual(ar[:an], br[:bn]) {
							t.Fatalf("parity at %d: %d/%d %v/%v", seek, an, bn, ae, be)
						}
						if bn > 0 {
							// Keep the Value structs, not their borrowed bytes.
							held = append(held, append(Row(nil), br[0]...))
							want = append(want, br[0].Clone())
						}
						if be == io.EOF {
							break
						}
					}
				}
				// Borrowed byte values remain valid after Close and after other
				// readers recycle buffers (the usual Rows ownership guarantee).
				b.Close()
				for k := 0; k < 3; k++ {
					r := NewReader(file)
					r.ReadRows(make([]Row, 50))
					r.Close()
				}
				if !reflect.DeepEqual(held, want) {
					t.Fatal("retained values changed")
				}
			})
		}
	}
}

// Files from other writers (Impala, parquet-mr, legacy PLAIN_DICTIONARY) cover
// run layouts and padding this library's writer may never produce.
func TestWindowedDictionaryTestdataParity(t *testing.T) {
	paths, err := filepath.Glob("testdata/*.parquet")
	if err != nil {
		t.Fatal(err)
	}
	lazyPages := 0
	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			base, baseErr := OpenFile(bytes.NewReader(data), int64(len(data)))
			file, fileErr := OpenFile(bytes.NewReader(data), int64(len(data)), WindowedDictionaryReading(true))
			if (baseErr == nil) != (fileErr == nil) {
				t.Fatalf("open: eager=%v windowed=%v", baseErr, fileErr)
			}
			if baseErr != nil {
				t.Skip(baseErr)
			}
			for g, rg := range file.RowGroups() {
				for _, chunk := range rg.ColumnChunks() {
					pages := chunk.Pages()
					for {
						p, err := pages.ReadPage()
						if err != nil {
							break
						}
						if _, ok := p.(*bufferedDictionaryPage); ok {
							lazyPages++
						}
						Release(p)
					}
					pages.Close()
				}
				a, b := base.RowGroups()[g].Rows(), rg.Rows()
				ar, br := make([]Row, 64), make([]Row, 64)
				for total := 0; ; {
					an, ae := a.ReadRows(ar)
					bn, be := b.ReadRows(br)
					if an != bn || (ae == nil) != (be == nil) || (ae == io.EOF) != (be == io.EOF) {
						t.Fatalf("row group %d at row %d: eager=%d,%v windowed=%d,%v", g, total, an, ae, bn, be)
					}
					for i := range an {
						if !ar[i].Equal(br[i]) {
							t.Fatalf("row group %d row %d differs", g, total+i)
						}
					}
					total += an
					if ae != nil {
						break
					}
				}
				a.Close()
				b.Close()
			}
		})
	}
	if lazyPages == 0 {
		t.Fatal("no testdata page used windowed dictionary reading")
	}
}

func TestWindowedDictionaryColumnWindowReuse(t *testing.T) {
	data := windowTestFile(t, 1, 10001, 1024, 2)
	f, err := OpenFile(bytes.NewReader(data), int64(len(data)), WindowedDictionaryReading(true))
	if err != nil {
		t.Fatal(err)
	}
	r := NewColumnChunkValueReader(f.RowGroups()[0].ColumnChunks()[0]).(*columnChunkValueReader)
	values := make([]Value, 127)
	var first *int32
	var firstPage Page
	transitions := 0
	for {
		n, err := r.ReadValues(values)
		if n > 0 {
			if first == nil {
				first = &r.indexWindow[0]
				firstPage = r.page
			}
			if first != &r.indexWindow[0] {
				t.Fatal("window reallocated across pages")
			}
			if r.page != firstPage {
				transitions++
				firstPage = r.page
			}
			if r.values.(*windowedIndexedPageValues).page.materialized != nil {
				t.Fatal("eager column read")
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if transitions == 0 {
		t.Fatal("no page transitions")
	}
	if err := r.SeekToRow(7); err != nil {
		t.Fatal(err)
	}
	if _, err := r.ReadValues(values); err != nil {
		t.Fatal(err)
	}
	if first != &r.indexWindow[0] {
		t.Fatal("window reallocated on seek")
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if r.indexWindow != nil {
		t.Fatal("window retained after close")
	}
}

func BenchmarkWindowedDictionaryRead(b *testing.B) {
	typ := windowTestDictionary()
	for _, count := range []int{4096, 1 << 20} {
		for _, packed := range []bool{false, true} {
			ids := make([]int32, count)
			for i := range ids {
				if packed {
					ids[i] = int32(i % 256)
				} else {
					ids[i] = 1
				}
			}
			data, err := RLEDictionary.EncodeInt32(nil, ids)
			if err != nil {
				b.Fatal(err)
			}
			for _, lazy := range []bool{false, true} {
				b.Run(fmt.Sprintf("n%d/packed%v/lazy%v", count, packed, lazy), func(b *testing.B) {
					b.ReportAllocs()
					b.SetBytes(int64(count * 4))
					values := make([]Value, 127)
					for i := 0; i < b.N; i++ {
						var p Page
						if lazy {
							p, err = newWindowedIndexedPage(typ, 0, count, data)
						} else {
							var decoded []int32
							decoded, err = RLEDictionary.DecodeInt32(nil, data)
							p = newIndexedPage(typ, makeColumnIndex(0), int32(count), encoding.Int32Values(decoded))
						}
						if err != nil {
							b.Fatal(err)
						}
						r := p.Values()
						for {
							_, err := r.ReadValues(values)
							if err == io.EOF {
								break
							}
							if err != nil {
								b.Fatal(err)
							}
						}
					}
				})
			}
		}
	}
}

func BenchmarkWindowedDictionaryWideFile(b *testing.B) {
	for _, pageSize := range []int{4096, 1 << 22} {
		data := windowTestFile(b, 64, 32769, pageSize, 2)
		for _, lazy := range []bool{false, true} {
			b.Run(fmt.Sprintf("page%d/lazy%v", pageSize, lazy), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					f, err := OpenFile(bytes.NewReader(data), int64(len(data)), WindowedDictionaryReading(lazy))
					if err != nil {
						b.Fatal(err)
					}
					r := NewReader(f)
					rows := make([]Row, 127)
					for {
						_, err := r.ReadRows(rows)
						if err == io.EOF {
							break
						}
						if err != nil {
							b.Fatal(err)
						}
					}
					r.Close()
				}
			})
		}
	}
}
