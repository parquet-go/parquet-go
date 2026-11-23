package parquet

import (
	"fmt"
	"io"
	"sync"

	"github.com/parquet-go/parquet-go/encoding"
	"github.com/parquet-go/parquet-go/internal/rle"
)

const dictionaryIndexWindowSize = 1024

// Pages and their slices share the encoded bytes. Only Page.Data materializes
// indexes; Values and Bounds consume runs directly.
type windowedIndexedPage struct {
	indexedPage
	first        rle.Int32Runs
	count        int64
	once         sync.Once
	materialized []int32
}

func newWindowedIndexedPage(typ *indexedType, column, count int, data []byte) (Page, error) {
	if count < 0 {
		return nil, fmt.Errorf("negative dictionary value count: %w", ErrCorrupted)
	}
	var width uint
	var payload []byte
	if len(data) > 0 {
		width, payload = uint(data[0]), data[1:]
	}
	first, err := rle.NewInt32Runs(payload, width)
	if err != nil {
		return nil, encoding.Error(&RLEDictionary, err)
	}
	dictLen := -1
	if typ.dict != nil {
		dictLen = typ.dict.Len()
	}
	if err := validateDictionaryRuns(first, width, count, dictLen); err != nil {
		return nil, err
	}
	if typ.dict == nil {
		return nil, fmt.Errorf("no dictionary found: %w", ErrCorrupted)
	}
	return &windowedIndexedPage{
		indexedPage: indexedPage{typ: typ, columnIndex: ^makeColumnIndex(column)},
		first:       first, count: int64(count),
	}, nil
}

// Validate the whole encoded stream before publishing a page. Surplus indexes
// are ignored but their run framing is still checked. Clean short streams are
// zero-extended, just like newIndexedPage. A negative dictLen means no dictionary.
func validateDictionaryRuns(c rle.Int32Runs, width uint, count, dictLen int) error {
	var window [dictionaryIndexWindowSize]int32
	var boundsErr error
	coversPacked := width < 32 && dictLen > 0 && uint64(dictLen) >= uint64(1)<<width
	left := count
	for {
		if err := c.Next(); err != nil {
			if err != io.EOF {
				return encoding.Error(&RLEDictionary, err)
			}
			if dictLen == 0 && left > 0 {
				// Eager validation checks the zero-filled tail first for an empty dictionary.
				return checkDictionaryIndexBounds(nil, left, dictLen)
			}
			return boundsErr
		}
		n := int(min(int64(left), c.Remaining()))
		left -= n
		if n == 0 || dictLen < 0 || boundsErr != nil {
			continue
		}
		if id, repeated := c.Repeated(); repeated {
			boundsErr = checkDictionaryIndexBounds([]int32{id}, 1, dictLen)
		} else if !coversPacked {
			for n > 0 && boundsErr == nil {
				ids := window[:min(n, len(window))]
				c.Unpack(ids)
				boundsErr = checkDictionaryIndexBounds(ids, len(ids), dictLen)
				n -= len(ids)
			}
		}
	}
}

func (p *windowedIndexedPage) NumRows() int64   { return p.count }
func (p *windowedIndexedPage) NumValues() int64 { return p.count }
func (p *windowedIndexedPage) Size() int64      { return 4 * p.count }

func (p *windowedIndexedPage) Data() encoding.Values {
	p.once.Do(func() {
		ids := make([]int32, int(p.count))
		c := p.first
		for rest := ids; len(rest) > 0 && loadDictionaryRun(&c); {
			n := int(min(int64(len(rest)), c.Remaining()))
			if id, repeated := c.Repeated(); repeated {
				for i := range rest[:n] {
					rest[i] = id
				}
				c.Skip(int64(n))
			} else {
				c.Unpack(rest[:n])
			}
			rest = rest[n:]
		}
		p.materialized = ids
	})
	return encoding.Int32Values(p.materialized)
}

// Slice remembers its position for later reads. Separate slices of the same
// page each scan from that page's starting position.
func (p *windowedIndexedPage) Slice(i, j int64) Page {
	if i < 0 || j < i || j > p.count {
		panic("dictionary page slice out of bounds")
	}
	c := p.first
	for skip := i; skip > 0 && loadDictionaryRun(&c); {
		n := min(skip, c.Remaining())
		c.Skip(n)
		skip -= n
	}
	return &windowedIndexedPage{
		indexedPage: p.indexedPage, first: c, count: j - i,
	}
}

// Bounds needs only one ID per repeated run, including a synthesized zero tail.
func (p *windowedIndexedPage) Bounds() (minValue, maxValue Value, ok bool) {
	c := p.first
	var window [dictionaryIndexWindowSize]int32
	for left := p.count; left > 0; {
		var fill int
		for left > 0 && fill < len(window) {
			if !loadDictionaryRun(&c) {
				window[fill] = 0
				fill++
				left = 0
				break
			}
			n := min(left, c.Remaining())
			if id, repeated := c.Repeated(); repeated {
				window[fill] = id
				fill++
				c.Skip(n)
			} else {
				n = min(n, int64(len(window)-fill))
				c.Unpack(window[fill : fill+int(n)])
				fill += int(n)
			}
			left -= n
		}
		lo, hi := p.typ.dict.Bounds(window[:fill])
		if !ok || p.typ.Compare(lo, minValue) < 0 {
			minValue = lo
		}
		if !ok || p.typ.Compare(hi, maxValue) > 0 {
			maxValue = hi
		}
		ok = true
	}
	if ok {
		minValue.columnIndex = p.columnIndex
		maxValue.columnIndex = p.columnIndex
	}
	return
}

func loadDictionaryRun(c *rle.Int32Runs) bool {
	if c.Remaining() == 0 {
		if err := c.Next(); err != nil {
			if err == io.EOF {
				return false
			}
			panic("parquet: dictionary stream changed after validation: " + err.Error())
		}
	}
	return true
}

func (p *windowedIndexedPage) Values() ValueReader {
	r := &windowedIndexedPageValues{page: p, cursor: p.first, left: p.count}
	if d, ok := p.typ.dict.(*byteArrayDictionary); ok && d.columnIndex == p.columnIndex {
		r.bytes = d
	}
	r.window = &r.own
	return r
}

type windowedIndexedPageValues struct {
	page   *windowedIndexedPage
	cursor rle.Int32Runs
	left   int64
	window *[]int32 // owned by the column reader, or by this reader for Page.Values
	own    []int32
	bytes  *byteArrayDictionary
}

func (r *windowedIndexedPageValues) indexWindow(batch int) []int32 {
	need := min(batch, dictionaryIndexWindowSize)
	if w := *r.window; len(w) >= need {
		return w
	}
	w := make([]int32, (need+7)&^7)
	*r.window = w
	return w
}

func (r *windowedIndexedPageValues) ReadValues(values []Value) (n int, err error) {
	for n < len(values) && r.left > 0 {
		limit := int(min(int64(len(values)-n), r.left))
		if loadDictionaryRun(&r.cursor) {
			limit = int(min(int64(limit), r.cursor.Remaining()))
			if id, repeated := r.cursor.Repeated(); repeated {
				r.repeat(values[n:n+limit], id)
				r.cursor.Skip(int64(limit))
			} else {
				window := r.indexWindow(len(values))
				limit = min(limit, len(window))
				r.cursor.Unpack(window[:limit])
				r.lookup(window[:limit], values[n:n+limit])
			}
		} else {
			r.repeat(values[n:n+limit], 0)
		}
		n += limit
		r.left -= int64(limit)
	}
	if r.left == 0 {
		err = io.EOF
	}
	return
}

func (r *windowedIndexedPageValues) repeat(out []Value, id int32) {
	out[0] = r.page.typ.dict.Index(id)
	out[0].columnIndex = r.page.columnIndex
	if len(out) > 32 {
		memsetValues(out[1:], out[0])
	} else {
		for i := range out[1:] {
			out[i+1] = out[0]
		}
	}
}

func (r *windowedIndexedPageValues) lookup(ids []int32, out []Value) {
	if d := r.bytes; d != nil {
		offsets, data := d.offsets.Slice(), d.values.Slice()
		for i, id := range ids {
			j, k := offsets[id], offsets[id+1]
			out[i] = d.makeValueBytes(data[j:k:k])
		}
		return
	}
	r.page.typ.dict.Lookup(ids, out)
	for i := range out {
		out[i].columnIndex = r.page.columnIndex
	}
}
