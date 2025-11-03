package parquet

import (
	"fmt"
	"io"
	"slices"

	"github.com/parquet-go/bitpack/unsafecast"
	"github.com/parquet-go/parquet-go/deprecated"
	"github.com/parquet-go/parquet-go/sparse"
)

type uint64ColumnBuffer struct{ uint64Page }

func newUint64ColumnBuffer(typ Type, columnIndex int16, numValues int32) *uint64ColumnBuffer {
	return &uint64ColumnBuffer{
		uint64Page: uint64Page{
			typ:         typ,
			values:      make([]uint64, 0, numValues),
			columnIndex: ^columnIndex,
		},
	}
}

func (col *uint64ColumnBuffer) Clone() ColumnBuffer {
	return &uint64ColumnBuffer{
		uint64Page: uint64Page{
			typ:         col.typ,
			values:      slices.Clone(col.values),
			columnIndex: col.columnIndex,
		},
	}
}

func (col *uint64ColumnBuffer) ColumnIndex() (ColumnIndex, error) {
	return uint64ColumnIndex{&col.uint64Page}, nil
}

func (col *uint64ColumnBuffer) OffsetIndex() (OffsetIndex, error) {
	return uint64OffsetIndex{&col.uint64Page}, nil
}

func (col *uint64ColumnBuffer) BloomFilter() BloomFilter { return nil }

func (col *uint64ColumnBuffer) Dictionary() Dictionary { return nil }

func (col *uint64ColumnBuffer) Pages() Pages { return onePage(col.Page()) }

func (col *uint64ColumnBuffer) Page() Page { return &col.uint64Page }

func (col *uint64ColumnBuffer) Reset() { col.values = col.values[:0] }

func (col *uint64ColumnBuffer) Cap() int { return cap(col.values) }

func (col *uint64ColumnBuffer) Len() int { return len(col.values) }

func (col *uint64ColumnBuffer) Less(i, j int) bool { return col.values[i] < col.values[j] }

func (col *uint64ColumnBuffer) Swap(i, j int) {
	col.values[i], col.values[j] = col.values[j], col.values[i]
}

func (col *uint64ColumnBuffer) Write(b []byte) (int, error) {
	if (len(b) % 8) != 0 {
		return 0, fmt.Errorf("cannot write INT64 values from input of size %d", len(b))
	}
	col.values = append(col.values, unsafecast.Slice[uint64](b)...)
	return len(b), nil
}

func (col *uint64ColumnBuffer) WriteUint64s(values []uint64) (int, error) {
	col.values = append(col.values, values...)
	return len(values), nil
}

func (col *uint64ColumnBuffer) WriteValues(values []Value) (int, error) {
	col.writeValues(columnLevels{}, makeArrayValue(values, offsetOfU64))
	return len(values), nil
}

func (col *uint64ColumnBuffer) writeValues(_ columnLevels, rows sparse.Array) {
	if n := len(col.values) + rows.Len(); n > cap(col.values) {
		col.values = append(make([]uint64, 0, max(n, 2*cap(col.values))), col.values...)
	}
	n := len(col.values)
	col.values = col.values[:n+rows.Len()]
	sparse.GatherUint64(col.values[n:], rows.Uint64Array())
}

func (col *uint64ColumnBuffer) writeBoolean(_ columnLevels, _ bool) {
	panic("cannot write boolean to uint64 column")
}

func (col *uint64ColumnBuffer) writeInt32(_ columnLevels, value int32) {
	if value < 0 {
		panic(fmt.Sprintf("int32 value %d out of range for uint64", value))
	}
	col.values = append(col.values, uint64(value))
}

func (col *uint64ColumnBuffer) writeInt64(_ columnLevels, value int64) {
	if value < 0 {
		panic(fmt.Sprintf("int64 value %d out of range for uint64", value))
	}
	col.values = append(col.values, uint64(value))
}

func (col *uint64ColumnBuffer) writeInt96(_ columnLevels, _ deprecated.Int96) {
	panic("cannot write int96 to uint64 column")
}

func (col *uint64ColumnBuffer) writeFloat(_ columnLevels, _ float32) {
	panic("cannot write float to uint64 column")
}

func (col *uint64ColumnBuffer) writeDouble(_ columnLevels, _ float64) {
	panic("cannot write double to uint64 column")
}

func (col *uint64ColumnBuffer) writeByteArray(_ columnLevels, _ []byte) {
	panic("cannot write byte array to uint64 column")
}

func (col *uint64ColumnBuffer) writeNull(_ columnLevels) {
	panic("cannot write null to uint64 column")
}

func (col *uint64ColumnBuffer) ReadValuesAt(values []Value, offset int64) (n int, err error) {
	i := int(offset)
	switch {
	case i < 0:
		return 0, errRowIndexOutOfBounds(offset, int64(len(col.values)))
	case i >= len(col.values):
		return 0, io.EOF
	default:
		for n < len(values) && i < len(col.values) {
			values[n] = col.makeValue(col.values[i])
			n++
			i++
		}
		if n < len(values) {
			err = io.EOF
		}
		return n, err
	}
}
