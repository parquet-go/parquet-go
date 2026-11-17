package parquet

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"sync"
	"unsafe"

	"github.com/parquet-go/bitpack"
	"github.com/parquet-go/parquet-go/encoding"
)

const dictionaryIndexWindowSize = 1024

// dictionaryIndexCursor pauses inside both RLE runs and bit-packed groups.
// It never expands a run or allocates storage proportional to its length.
type dictionaryIndexCursor struct {
	data      []byte
	width     uint
	remaining int64
	word      int32
	packed    []byte
	bit       uint64
}

func newDictionaryIndexCursor(data []byte) (dictionaryIndexCursor, error) {
	c := dictionaryIndexCursor{}
	if len(data) != 0 {
		c.width = uint(data[0])
		c.data = data[1:]
	}
	if c.width > 32 {
		return c, fmt.Errorf("invalid dictionary index bit width %d", c.width)
	}
	return c, nil
}

func (c *dictionaryIndexCursor) nextRun() error {
	for len(c.data) > 0 {
		u, n := uint64(c.data[0]), 1
		if u >= 0x80 {
			u, n = binary.Uvarint(c.data)
			if n == 0 {
				return io.ErrUnexpectedEOF
			}
			if n < 0 {
				return fmt.Errorf("dictionary index run header overflow")
			}
		}
		c.data = c.data[n:]
		count := u >> 1
		if count > math.MaxInt32 {
			return fmt.Errorf("dictionary index run too long: %d", count)
		}
		if count == 0 {
			continue
		}
		// Width-0 packed runs are repeats of c.word, so every run resets it.
		c.packed, c.bit, c.word = nil, 0, 0
		if u&1 != 0 {
			size := count * uint64(c.width)
			if size > uint64(len(c.data)) {
				return io.ErrUnexpectedEOF
			}
			c.packed = c.data[:int(size)]
			c.data = c.data[int(size):]
			c.remaining = int64(count * 8)
		} else {
			size := int((c.width + 7) / 8)
			if size > len(c.data) {
				return io.ErrUnexpectedEOF
			}
			switch size {
			case 0:
			case 1:
				c.word = int32(c.data[0])
			default:
				var b [4]byte
				copy(b[:], c.data[:size])
				c.word = int32(binary.LittleEndian.Uint32(b[:]))
			}
			c.data = c.data[size:]
			c.remaining = int64(count)
		}
		return nil
	}
	return io.EOF
}

func (c *dictionaryIndexCursor) consume(n int64) {
	if c.packed != nil {
		c.bit += uint64(n) * uint64(c.width)
	}
	c.remaining -= n
}

func (c *dictionaryIndexCursor) skip(n int64) error {
	for n > 0 {
		if c.remaining == 0 {
			if err := c.nextRun(); err != nil {
				// Eager indexed pages zero-extend successfully decoded short streams.
				if err == io.EOF {
					return nil
				}
				return err
			}
		}
		m := min(n, c.remaining)
		c.consume(m)
		n -= m
	}
	return nil
}

func (c *dictionaryIndexCursor) read(dst []int32) (int, error) {
	n := 0
	for n < len(dst) {
		if c.remaining == 0 {
			if err := c.nextRun(); err != nil {
				if err == io.EOF {
					clear(dst[n:])
					return len(dst), nil
				}
				return n, err
			}
		}
		m := int(min(int64(len(dst)-n), c.remaining))
		if c.packed == nil || c.width == 0 {
			for i := range dst[n : n+m] {
				dst[n+i] = c.word
			}
		} else {
			c.readPacked(dst[n : n+m])
		}
		c.consume(int64(m))
		n += m
	}
	return n, nil
}

// load parses the next run when the current one is exhausted. eof reports a
// clean end of stream, which callers zero-extend like eager indexed pages.
func (c *dictionaryIndexCursor) load() (eof bool, err error) {
	if c.remaining != 0 {
		return false, nil
	}
	if err := c.nextRun(); err != nil {
		return err == io.EOF, err
	}
	return false, nil
}

func (c *dictionaryIndexCursor) repeated() bool { return c.packed == nil || c.width == 0 }

// take consumes at most limit IDs from the loaded run without expanding
// repeats: an RLE run returns ids == nil and the repeated id. Bit-packed IDs
// are decoded into window, which also bounds n.
func (c *dictionaryIndexCursor) take(window []int32, limit int) (ids []int32, id int32, n int) {
	if c.repeated() {
		n = int(min(int64(limit), c.remaining))
		c.consume(int64(n))
		return nil, c.word, n
	}
	n = int(min(int64(min(limit, len(window))), c.remaining))
	c.readPacked(window[:n])
	c.consume(int64(n))
	return window[:n], 0, n
}

// readPacked unpacks whole aligned groups directly. Only partial groups and
// the unpadded tail need fixed scratch, independent of the encoded run length.
func (c *dictionaryIndexCursor) readPacked(dst []int32) {
	index := c.bit / uint64(c.width)
	for len(dst) > 0 {
		group, offset := index/8, int(index%8)
		in := c.packed[int(group)*int(c.width):]
		if offset == 0 && cap(in) >= bitpack.PaddingInt32 {
			n := min(len(dst)/8, (cap(in)-bitpack.PaddingInt32)/int(c.width)) * 8
			if n > 0 {
				bitpack.Unpack(dst[:n], in[:cap(in)], c.width)
				dst = dst[n:]
				index += uint64(n)
				continue
			}
		}
		var packed [32 + bitpack.PaddingInt32]byte
		var ids [8]int32
		copy(packed[:], in[:int(c.width)])
		bitpack.Unpack(ids[:], packed[:], c.width)
		n := copy(dst, ids[offset:])
		dst = dst[n:]
		index += uint64(n)
	}
}

// Validate before publishing the page: Data and Bounds cannot return errors,
// and dictionary lookup must never be handed an invalid index. RLE validation
// is constant-time per run; bit-packed IDs are checked in fixed-size windows.
func validateDictionaryIndexes(data []byte, count int64, dict Dictionary) error {
	c, err := newDictionaryIndexCursor(data)
	if err != nil {
		return err
	}
	var window [dictionaryIndexWindowSize]int32
	dictLen := uint64(dict.Len())
	left := count
	for {
		if err := c.nextRun(); err != nil {
			if err != io.EOF {
				return err
			}
			if left > 0 && dictLen == 0 {
				return fmt.Errorf("invalid synthesized dictionary index 0")
			}
			return nil
		}
		n := min(left, c.remaining)
		if c.packed == nil || c.width == 0 {
			if n > 0 && uint64(uint32(c.word)) >= dictLen {
				return fmt.Errorf("invalid dictionary index %d", c.word)
			}
			left -= n
			c.consume(c.remaining)
		} else {
			// Width < 32 excludes negative int32 IDs. Still parse every run,
			// including surplus values, to preserve structural validation.
			if c.width < 32 && dictLen >= uint64(1)<<c.width {
				left -= n
				c.consume(c.remaining)
				continue
			}
			for n > 0 {
				m := min(n, int64(len(window)))
				ids := window[:int(m)]
				if _, err := c.read(ids); err != nil {
					return err
				}
				if uint64(maxDictionaryIndex(ids)) >= dictLen {
					for _, id := range ids {
						if uint64(uint32(id)) >= dictLen {
							return fmt.Errorf("invalid dictionary index %d", id)
						}
					}
				}
				n -= m
				left -= m
			}
			c.consume(c.remaining) // Last bit-packed group may have padding IDs.
		}
	}
}

// Encoded indexes are retained for lazy decoding, but returned byte values
// borrow the dictionary instead. Detach therefore releases encoded storage.
type bufferedDictionaryPage struct {
	Page
	encoded *buffer[byte]
}

func newBufferedDictionaryPage(page Page, encoded *buffer[byte]) *bufferedDictionaryPage {
	bufferRef(encoded)
	return &bufferedDictionaryPage{Page: page, encoded: encoded}
}

func (p *bufferedDictionaryPage) Slice(i, j int64) Page {
	return newBufferedDictionaryPage(p.Page.Slice(i, j), p.encoded)
}
func (p *bufferedDictionaryPage) Retain()                 { bufferRef(p.encoded) }
func (p *bufferedDictionaryPage) Release()                { bufferUnref(p.encoded) }
func (p *bufferedDictionaryPage) ReleaseAndDetachValues() { p.Release() }

type windowedIndexedPage struct {
	typ          *indexedType
	data         []byte
	start, count int64
	columnIndex  uint16
	once         sync.Once
	materialized []int32
}

func newWindowedIndexedPage(typ *indexedType, column int, count int, data []byte) (Page, error) {
	if count < 0 {
		return nil, fmt.Errorf("negative dictionary value count")
	}
	if typ.dict == nil {
		return nil, fmt.Errorf("dictionary page is missing")
	}
	if err := validateDictionaryIndexes(data, int64(count), typ.dict); err != nil {
		return nil, err
	}
	return &windowedIndexedPage{typ: typ, data: data, count: int64(count), columnIndex: ^makeColumnIndex(column)}, nil
}

func (p *windowedIndexedPage) Type() Type               { return indexedPageType{p.typ} }
func (p *windowedIndexedPage) Column() int              { return int(^p.columnIndex) }
func (p *windowedIndexedPage) Dictionary() Dictionary   { return p.typ.dict }
func (p *windowedIndexedPage) NumRows() int64           { return p.count }
func (p *windowedIndexedPage) NumValues() int64         { return p.count }
func (p *windowedIndexedPage) NumNulls() int64          { return 0 }
func (p *windowedIndexedPage) Size() int64              { return 4 * p.count }
func (p *windowedIndexedPage) RepetitionLevels() []byte { return nil }
func (p *windowedIndexedPage) DefinitionLevels() []byte { return nil }

// cursor and Data rely on newWindowedIndexedPage having validated the complete
// stream; an error here means that invariant was broken, so fail loudly rather
// than return zero-filled indexes.
func (p *windowedIndexedPage) cursor() dictionaryIndexCursor {
	c, err := newDictionaryIndexCursor(p.data)
	if err == nil {
		err = c.skip(p.start)
	}
	if err != nil {
		panic("parquet: windowed dictionary page violates constructor validation: " + err.Error())
	}
	return c
}
func (p *windowedIndexedPage) Data() encoding.Values {
	p.once.Do(func() {
		p.materialized = make([]int32, int(p.count))
		c := p.cursor()
		if _, err := c.read(p.materialized); err != nil {
			panic("parquet: windowed dictionary page violates constructor validation: " + err.Error())
		}
	})
	return encoding.Int32Values(p.materialized)
}
func (p *windowedIndexedPage) Slice(i, j int64) Page {
	if i < 0 || j < i || j > p.count {
		panic("dictionary page slice out of bounds")
	}
	return &windowedIndexedPage{typ: p.typ, data: p.data, start: p.start + i, count: j - i, columnIndex: p.columnIndex}
}

// Bounds accumulates IDs into bounded scratch. One entry per RLE run (and for
// a zero-extended tail) suffices for min/max, so runs never expand.
func (p *windowedIndexedPage) Bounds() (minValue, maxValue Value, ok bool) {
	c := p.cursor()
	var window [dictionaryIndexWindowSize]int32
	fill := 0
	flush := func() {
		lo, hi := p.typ.dict.Bounds(window[:fill])
		if !ok || p.typ.Compare(lo, minValue) < 0 {
			minValue = lo
		}
		if !ok || p.typ.Compare(hi, maxValue) > 0 {
			maxValue = hi
		}
		ok, fill = true, 0
	}
	for left := p.count; left > 0; {
		if fill == len(window) {
			flush()
		}
		limit := int(min(left, math.MaxInt32))
		if eof, err := c.load(); eof {
			window[fill] = 0
			fill++
			break
		} else if err != nil {
			panic("parquet: windowed dictionary page violates constructor validation: " + err.Error())
		}
		ids, id, n := c.take(window[fill:], limit)
		if ids == nil {
			window[fill] = id
			fill++
		} else {
			fill += len(ids)
		}
		left -= int64(n)
	}
	if fill > 0 {
		flush()
	}
	if ok {
		minValue.columnIndex = p.columnIndex
		maxValue.columnIndex = p.columnIndex
	}
	return
}
func (p *windowedIndexedPage) Values() ValueReader {
	r := &windowedIndexedPageValues{page: p, cursor: p.cursor(), left: p.count}
	r.window = &r.own
	if d, ok := p.typ.dict.(*byteArrayDictionary); ok && d.columnIndex == p.columnIndex {
		r.bytes = d
	}
	return r
}

type windowedIndexedPageValues struct {
	page   *windowedIndexedPage
	cursor dictionaryIndexCursor
	left   int64
	// window points at storage owned by the column reader, or at own for
	// standalone Page.Values readers. It grows to the largest packed request,
	// capped at dictionaryIndexWindowSize, so small-batch readers stay small.
	window *[]int32
	own    []int32
	one    [1]int32
	bytes  *byteArrayDictionary
}

// indexWindow sizes storage by the caller's batch, not the remainder of a
// partially filled batch, so steady-state readers allocate once.
func (r *windowedIndexedPageValues) indexWindow(batch int) []int32 {
	need := min(batch, dictionaryIndexWindowSize)
	if w := *r.window; len(w) >= need {
		return w
	}
	w := make([]int32, min((need+7)&^7, dictionaryIndexWindowSize))
	*r.window = w
	return w
}

func (r *windowedIndexedPageValues) ReadValues(values []Value) (n int, err error) {
	if r.left == 0 {
		return 0, io.EOF
	}
	for n < len(values) && r.left > 0 {
		limit := int(min(int64(len(values)-n), r.left))
		eof, loadErr := r.cursor.load()
		if loadErr != nil && !eof {
			return n, loadErr
		}
		var ids []int32
		var id int32
		m := limit
		switch {
		case eof:
		case r.cursor.repeated():
			ids, id, m = r.cursor.take(nil, limit)
		default:
			ids, id, m = r.cursor.take(r.indexWindow(len(values)), limit)
		}
		out := values[n : n+m]
		if ids == nil {
			r.one[0] = id
			r.lookup(r.one[:], out[:1])
			fillValues(out[1:], out[0])
		} else {
			r.lookup(ids, out)
		}
		n += m
		r.left -= int64(m)
	}
	if r.left == 0 {
		err = io.EOF
	}
	return
}

// lookup writes each Value once for the file-reader dictionary type, rather
// than memset-then-overwrite plus per-ID buffer slicing in the generic path.
// IDs were validated when the page was constructed.
func (r *windowedIndexedPageValues) lookup(ids []int32, out []Value) {
	d := r.bytes
	if d == nil {
		r.page.typ.dict.Lookup(ids, out)
		for i := range out {
			out[i].columnIndex = r.page.columnIndex
		}
		return
	}
	offsets := d.offsets.Slice()
	data := d.values.Slice()
	out = out[:len(ids)]
	for i, id := range ids {
		j, k := offsets[id], offsets[id+1]
		b := data[j:k:k]
		out[i] = Value{kind: ^int8(ByteArray), ptr: unsafe.SliceData(b), u64: uint64(len(b)), columnIndex: d.columnIndex}
	}
}

// fillValues uses plain stores for short runs, where memsetValues' doubling
// copies cost more than they save.
func fillValues(out []Value, v Value) {
	if len(out) > 32 {
		memsetValues(out, v)
		return
	}
	for i := range out {
		out[i] = v
	}
}

// maxDictionaryIndex is an unsigned max-reduction: negative IDs wrap above any
// dictionary length. Four accumulators break the serial dependency chain.
func maxDictionaryIndex(ids []int32) uint32 {
	var h0, h1, h2, h3 uint32
	for len(ids) >= 4 {
		h0 = max(h0, uint32(ids[0]))
		h1 = max(h1, uint32(ids[1]))
		h2 = max(h2, uint32(ids[2]))
		h3 = max(h3, uint32(ids[3]))
		ids = ids[4:]
	}
	for _, id := range ids {
		h0 = max(h0, uint32(id))
	}
	return max(h0, h1, h2, h3)
}
