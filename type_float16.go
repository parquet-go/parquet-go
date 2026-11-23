package parquet

import (
	"encoding/binary"
	"math"
	"reflect"
	"slices"

	"github.com/parquet-go/parquet-go/deprecated"
	"github.com/parquet-go/parquet-go/encoding"
	"github.com/parquet-go/parquet-go/format"
)

// float16Type is the FLOAT16 logical type: an IEEE 754 binary16 value stored
// as a little-endian FIXED_LEN_BYTE_ARRAY of length 2. Files that carry the
// annotation keep it, and bounds use the signed numeric order shared with
// FLOAT and DOUBLE. The type is not constructed from Go values; it is
// recognized while reading a schema element.
type float16Type struct{}

var float16LogicalType = format.LogicalType{Value: new(format.Float16Type)}

func (float16Type) String() string { return "FLOAT16" }

func (float16Type) Kind() Kind { return FixedLenByteArray }

func (float16Type) Length() int { return 2 }

func (float16Type) EstimateSize(n int) int {
	return fixedLenByteArrayType{length: 2}.EstimateSize(n)
}

func (float16Type) EstimateNumValues(n int) int {
	return fixedLenByteArrayType{length: 2}.EstimateNumValues(n)
}

func (float16Type) Compare(a, b Value) int {
	return compareFloat16Bytes(a.ByteArray(), b.ByteArray())
}

func (float16Type) ColumnOrder() *format.ColumnOrder { return &typeDefinedColumnOrder }

func (float16Type) PhysicalType() *format.Type { return &physicalTypes[FixedLenByteArray] }

func (float16Type) LogicalType() *format.LogicalType { return &float16LogicalType }

func (float16Type) ConvertedType() *deprecated.ConvertedType { return nil }

func (float16Type) NewColumnIndexer(int) ColumnIndexer { return &float16ColumnIndexer{} }

func (t float16Type) NewColumnBuffer(columnIndex, numValues int) ColumnBuffer {
	base := newFixedLenByteArrayColumnBuffer(t, makeColumnIndex(columnIndex), makeNumValues(numValues))
	return &float16ColumnBuffer{fixedLenByteArrayColumnBuffer: base}
}

func (t float16Type) NewDictionary(columnIndex, numValues int, data encoding.Values) Dictionary {
	base := newFixedLenByteArrayDictionary(t, makeColumnIndex(columnIndex), makeNumValues(numValues), data)
	return &float16Dictionary{fixedLenByteArrayDictionary: base}
}

func (t float16Type) NewPage(columnIndex, numValues int, data encoding.Values) Page {
	return float16Page{
		fixedLenByteArrayPage: newFixedLenByteArrayPage(t, makeColumnIndex(columnIndex), makeNumValues(numValues), data),
	}
}

func (float16Type) NewValues(values []byte, _ []uint32) encoding.Values {
	return encoding.FixedLenByteArrayValues(values, 2)
}

func (float16Type) Encode(dst []byte, src encoding.Values, enc encoding.Encoding) ([]byte, error) {
	return fixedLenByteArrayType{length: 2}.Encode(dst, src, enc)
}

func (float16Type) Decode(dst encoding.Values, src []byte, enc encoding.Encoding) (encoding.Values, error) {
	return fixedLenByteArrayType{length: 2}.Decode(dst, src, enc)
}

func (float16Type) EstimateDecodeSize(numValues int, src []byte, enc encoding.Encoding) int {
	return fixedLenByteArrayType{length: 2}.EstimateDecodeSize(numValues, src, enc)
}

func (float16Type) AssignValue(dst reflect.Value, src Value) error {
	return fixedLenByteArrayType{length: 2}.AssignValue(dst, src)
}

func (float16Type) ConvertValue(val Value, typ Type) (Value, error) {
	return fixedLenByteArrayType{length: 2}.ConvertValue(val, typ)
}

// float16ToFloat32 decodes a little-endian binary16 value. Every binary16
// value is exactly representable as float32. A slice that is not 2 bytes is
// treated as NaN so a corrupt bound cannot be ordered as a finite number.
func float16ToFloat32(b []byte) float32 {
	if len(b) != 2 {
		return float32(math.NaN())
	}
	return math.Float32frombits(float16bitsToFloat32bits(binary.LittleEndian.Uint16(b)))
}

func float16bitsToFloat32bits(u16 uint16) uint32 {
	sign := uint32(u16&0x8000) << 16
	exp := uint32(u16>>10) & 0x1f
	coef := uint32(u16&0x03ff) << 13
	switch exp {
	case 0x1f:
		return sign | 0x7f800000 | coef
	case 0:
		if coef == 0 {
			return sign
		}
		// Normalize a subnormal. coef is already in the float32 mantissa
		// position; shift until the implicit bit lands on bit 23.
		exp = 127 - 14
		for coef&0x00800000 == 0 {
			coef <<= 1
			exp--
		}
		coef &= 0x007fffff
		return sign | (exp << 23) | coef
	default:
		return sign | ((exp + (127 - 15)) << 23) | coef
	}
}

func float16IsNaN(b []byte) bool {
	return math.IsNaN(float64(float16ToFloat32(b)))
}

func compareFloat16Bytes(a, b []byte) int {
	return compareFloat32(float16ToFloat32(a), float16ToFloat32(b))
}

func float16ColumnIndexHasNaN(typ Type, index *format.ColumnIndex) bool {
	if index == nil {
		return false
	}
	if _, ok := typ.(float16Type); !ok {
		return false
	}
	n := len(index.MinValues)
	if m := len(index.MaxValues); m > n {
		n = m
	}
	for i := range n {
		if i < len(index.NullPages) && index.NullPages[i] {
			continue
		}
		if i < len(index.MinValues) && float16IsNaN(index.MinValues[i]) {
			return true
		}
		if i < len(index.MaxValues) && float16IsNaN(index.MaxValues[i]) {
			return true
		}
	}
	return false
}

// float16Bounds reports the numeric minimum and maximum of a packed binary16
// page. NaN values are skipped when any finite value exists, matching FLOAT
// and DOUBLE. An all-NaN page reports NaN so readers can tell the page held
// data. ok is false only when the page has no values.
func float16Bounds(data []byte) (min, max []byte, ok bool) {
	if len(data) < 2 {
		return nil, nil, false
	}
	var nan []byte
	for i := 0; i+2 <= len(data); i += 2 {
		v := data[i : i+2]
		if float16IsNaN(v) {
			if nan == nil {
				nan = v
			}
			continue
		}
		if !ok {
			min, max, ok = v, v, true
			continue
		}
		if compareFloat16Bytes(v, min) < 0 {
			min = v
		} else if compareFloat16Bytes(v, max) > 0 {
			max = v
		}
	}
	if !ok && nan != nil {
		return nan, nan, true
	}
	return min, max, ok
}

type float16Page struct {
	*fixedLenByteArrayPage
}

func (p float16Page) Bounds() (min, max Value, ok bool) {
	lo, hi, ok := float16Bounds(p.data.Slice())
	if !ok {
		return Value{}, Value{}, false
	}
	return p.makeValueBytes(lo), p.makeValueBytes(hi), true
}

func (p float16Page) Slice(i, j int64) Page {
	next := p.fixedLenByteArrayPage.Slice(i, j).(*fixedLenByteArrayPage)
	return float16Page{fixedLenByteArrayPage: next}
}

type float16ColumnBuffer struct {
	*fixedLenByteArrayColumnBuffer
}

func (c *float16ColumnBuffer) Clone() ColumnBuffer {
	cloned := c.fixedLenByteArrayColumnBuffer.Clone().(*fixedLenByteArrayColumnBuffer)
	return &float16ColumnBuffer{fixedLenByteArrayColumnBuffer: cloned}
}

func (c *float16ColumnBuffer) Page() Page {
	return float16Page{fixedLenByteArrayPage: &c.fixedLenByteArrayPage}
}

func (c *float16ColumnBuffer) Pages() Pages { return onePage(c.Page()) }

func (c *float16ColumnBuffer) Bounds() (min, max Value, ok bool) {
	return c.Page().Bounds()
}

func (c *float16ColumnBuffer) ColumnIndex() (ColumnIndex, error) {
	return float16BufferColumnIndex{page: c.Page().(float16Page)}, nil
}

func (c *float16ColumnBuffer) Less(i, j int) bool {
	return compareFloat16Bytes(c.index(i), c.index(j)) < 0
}

type float16BufferColumnIndex struct{ page float16Page }

func (i float16BufferColumnIndex) NumPages() int       { return 1 }
func (i float16BufferColumnIndex) NullCount(int) int64 { return 0 }
func (i float16BufferColumnIndex) NullPage(int) bool   { return false }
func (i float16BufferColumnIndex) MinValue(int) Value {
	min, _, ok := i.page.Bounds()
	if !ok {
		return Value{}
	}
	return min
}
func (i float16BufferColumnIndex) MaxValue(int) Value {
	_, max, ok := i.page.Bounds()
	if !ok {
		return Value{}
	}
	return max
}
func (i float16BufferColumnIndex) IsAscending() bool  { return false }
func (i float16BufferColumnIndex) IsDescending() bool { return false }

type float16Dictionary struct {
	*fixedLenByteArrayDictionary
}

func (d *float16Dictionary) Type() Type { return newIndexedType(float16Type{}, d) }

func (d *float16Dictionary) Page() Page {
	return float16Page{fixedLenByteArrayPage: &d.fixedLenByteArrayPage}
}

func (d *float16Dictionary) Bounds(indexes []int32) (min, max Value) {
	if len(indexes) == 0 {
		return min, max
	}
	var lo, hi, nan []byte
	for _, index := range indexes {
		v := d.index(index)
		if float16IsNaN(v) {
			if nan == nil {
				nan = v
			}
			continue
		}
		if lo == nil {
			lo, hi = v, v
			continue
		}
		if compareFloat16Bytes(v, lo) < 0 {
			lo = v
		} else if compareFloat16Bytes(v, hi) > 0 {
			hi = v
		}
	}
	if lo == nil {
		return d.makeValueBytes(nan), d.makeValueBytes(nan)
	}
	return d.makeValueBytes(lo), d.makeValueBytes(hi)
}

type float16ColumnIndexer struct {
	baseColumnIndexer
	minValues [][]byte
	maxValues [][]byte
}

func (i *float16ColumnIndexer) Reset() {
	i.reset()
	i.minValues = i.minValues[:0]
	i.maxValues = i.maxValues[:0]
}

func (i *float16ColumnIndexer) IndexPage(numValues, numNulls int64, min, max Value) {
	if i.missingBounds {
		return
	}
	if !i.observeFixedSize(numValues, numNulls, min, max) {
		i.minValues = append(i.minValues, []byte{})
		i.maxValues = append(i.maxValues, []byte{})
		return
	}
	i.minValues = append(i.minValues, copyBytes(min.ByteArray()))
	i.maxValues = append(i.maxValues, copyBytes(max.ByteArray()))
}

func (i *float16ColumnIndexer) ColumnIndex() format.ColumnIndex {
	if i.missingBounds {
		return format.ColumnIndex{}
	}
	mins := slices.Clone(i.minValues)
	maxs := slices.Clone(i.maxValues)
	minBounded := make([][]byte, 0, len(mins))
	maxBounded := make([][]byte, 0, len(maxs))
	for j, nullPage := range i.nullPages {
		if nullPage {
			mins[j], maxs[j] = []byte{}, []byte{}
			continue
		}
		minBounded = append(minBounded, mins[j])
		maxBounded = append(maxBounded, maxs[j])
	}
	return i.columnIndex(mins, maxs, orderOfFloat16(minBounded), orderOfFloat16(maxBounded))
}

// orderOfFloat16 mirrors orderOfDecimalBytes using the signed binary16 order.
func orderOfFloat16(data [][]byte) int {
	switch len(data) {
	case 0, 1:
		return 0
	}
	i := 1
	for i < len(data) && compareFloat16Bytes(data[i-1], data[i]) == 0 {
		i++
	}
	data = data[i-1:]
	if len(data) < 2 {
		return 1
	}
	switch ordering := compareFloat16Bytes(data[0], data[1]); {
	case ordering < 0:
		for j := 2; j < len(data); j++ {
			if compareFloat16Bytes(data[j-1], data[j]) > 0 {
				return 0
			}
		}
		return +1
	case ordering > 0:
		for j := 2; j < len(data); j++ {
			if compareFloat16Bytes(data[j-1], data[j]) < 0 {
				return 0
			}
		}
		return -1
	}
	return 0
}
