package parquet

import (
	"fmt"
	"io"
	"slices"

	"github.com/parquet-go/parquet-go/deprecated"
	"github.com/parquet-go/parquet-go/sparse"
)

type be128ColumnBuffer struct{ be128Page }

func newBE128ColumnBuffer(typ Type, columnIndex int16, numValues int32) *be128ColumnBuffer {
	return &be128ColumnBuffer{
		be128Page: be128Page{
			typ:         typ,
			values:      make([][16]byte, 0, numValues),
			columnIndex: ^columnIndex,
		},
	}
}

func (col *be128ColumnBuffer) Clone() ColumnBuffer {
	return &be128ColumnBuffer{
		be128Page: be128Page{
			typ:         col.typ,
			values:      slices.Clone(col.values),
			columnIndex: col.columnIndex,
		},
	}
}

func (col *be128ColumnBuffer) ColumnIndex() (ColumnIndex, error) {
	return be128ColumnIndex{&col.be128Page}, nil
}

func (col *be128ColumnBuffer) OffsetIndex() (OffsetIndex, error) {
	return be128OffsetIndex{&col.be128Page}, nil
}

func (col *be128ColumnBuffer) BloomFilter() BloomFilter { return nil }

func (col *be128ColumnBuffer) Dictionary() Dictionary { return nil }

func (col *be128ColumnBuffer) Pages() Pages { return onePage(col.Page()) }

func (col *be128ColumnBuffer) Page() Page { return &col.be128Page }

func (col *be128ColumnBuffer) Reset() { col.values = col.values[:0] }

func (col *be128ColumnBuffer) Cap() int { return cap(col.values) }

func (col *be128ColumnBuffer) Len() int { return len(col.values) }

func (col *be128ColumnBuffer) Less(i, j int) bool {
	return lessBE128(&col.values[i], &col.values[j])
}

func (col *be128ColumnBuffer) Swap(i, j int) {
	col.values[i], col.values[j] = col.values[j], col.values[i]
}

func (col *be128ColumnBuffer) WriteValues(values []Value) (int, error) {
	if n := len(col.values) + len(values); n > cap(col.values) {
		col.values = append(make([][16]byte, 0, max(n, 2*cap(col.values))), col.values...)
	}
	n := len(col.values)
	col.values = col.values[:n+len(values)]
	newValues := col.values[n:]
	for i, v := range values {
		copy(newValues[i][:], v.byteArray())
	}
	return len(values), nil
}

func (col *be128ColumnBuffer) writeValues(_ columnLevels, rows sparse.Array) {
	if n := len(col.values) + rows.Len(); n > cap(col.values) {
		col.values = append(make([][16]byte, 0, max(n, 2*cap(col.values))), col.values...)
	}
	n := len(col.values)
	col.values = col.values[:n+rows.Len()]
	sparse.GatherUint128(col.values[n:], rows.Uint128Array())
}

func (col *be128ColumnBuffer) writeBoolean(_ columnLevels, _ bool) {
	panic("cannot write boolean to be128 column")
}

func (col *be128ColumnBuffer) writeInt32(_ columnLevels, _ int32) {
	panic("cannot write int32 to be128 column")
}

func (col *be128ColumnBuffer) writeInt64(_ columnLevels, _ int64) {
	panic("cannot write int64 to be128 column")
}

func (col *be128ColumnBuffer) writeInt96(_ columnLevels, _ deprecated.Int96) {
	panic("cannot write int96 to be128 column")
}

func (col *be128ColumnBuffer) writeFloat(_ columnLevels, _ float32) {
	panic("cannot write float to be128 column")
}

func (col *be128ColumnBuffer) writeDouble(_ columnLevels, _ float64) {
	panic("cannot write double to be128 column")
}

func (col *be128ColumnBuffer) writeByteArray(_ columnLevels, value []byte) {
	if len(value) != 16 {
		panic(fmt.Sprintf("cannot write %d bytes to [16]byte column", len(value)))
	}
	col.values = append(col.values, [16]byte(value))
}

func (col *be128ColumnBuffer) writeNull(_ columnLevels) {
	panic("cannot write null to be128 column")
}

func (col *be128ColumnBuffer) ReadValuesAt(values []Value, offset int64) (n int, err error) {
	i := int(offset)
	switch {
	case i < 0:
		return 0, errRowIndexOutOfBounds(offset, int64(len(col.values)))
	case i >= len(col.values):
		return 0, io.EOF
	default:
		for n < len(values) && i < len(col.values) {
			values[n] = col.makeValue(&col.values[i])
			n++
			i++
		}
		if n < len(values) {
			err = io.EOF
		}
		return n, err
	}
}
