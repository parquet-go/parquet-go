package parquet

import (
	"encoding/binary"
	"sync"

	"github.com/parquet-go/parquet-go/deprecated"
)

// CompareDescending constructs a comparison function which inverses the order
// of values.
//
//go:noinline
func CompareDescending(cmp func(Value, Value) int) func(Value, Value) int {
	return func(a, b Value) int { return -cmp(a, b) }
}

// CompareNullsFirst constructs a comparison function which assumes that null
// values are smaller than all other values.
//
//go:noinline
func CompareNullsFirst(cmp func(Value, Value) int) func(Value, Value) int {
	return func(a, b Value) int {
		switch {
		case a.IsNull():
			if b.IsNull() {
				return 0
			}
			return -1
		case b.IsNull():
			return +1
		default:
			return cmp(a, b)
		}
	}
}

// CompareNullsLast constructs a comparison function which assumes that null
// values are greater than all other values.
//
//go:noinline
func CompareNullsLast(cmp func(Value, Value) int) func(Value, Value) int {
	return func(a, b Value) int {
		switch {
		case a.IsNull():
			if b.IsNull() {
				return 0
			}
			return +1
		case b.IsNull():
			return -1
		default:
			return cmp(a, b)
		}
	}
}

func compareBool(v1, v2 bool) int {
	switch {
	case !v1 && v2:
		return -1
	case v1 && !v2:
		return +1
	default:
		return 0
	}
}

func compareInt32(v1, v2 int32) int {
	switch {
	case v1 < v2:
		return -1
	case v1 > v2:
		return +1
	default:
		return 0
	}
}

func compareInt64(v1, v2 int64) int {
	switch {
	case v1 < v2:
		return -1
	case v1 > v2:
		return +1
	default:
		return 0
	}
}

func compareInt96(v1, v2 deprecated.Int96) int {
	switch {
	case v1.Less(v2):
		return -1
	case v2.Less(v1):
		return +1
	default:
		return 0
	}
}

func compareFloat32(v1, v2 float32) int {
	switch {
	case v1 < v2:
		return -1
	case v1 > v2:
		return +1
	default:
		return 0
	}
}

func compareFloat64(v1, v2 float64) int {
	switch {
	case v1 < v2:
		return -1
	case v1 > v2:
		return +1
	default:
		return 0
	}
}

func compareUint32(v1, v2 uint32) int {
	switch {
	case v1 < v2:
		return -1
	case v1 > v2:
		return +1
	default:
		return 0
	}
}

func compareUint64(v1, v2 uint64) int {
	switch {
	case v1 < v2:
		return -1
	case v1 > v2:
		return +1
	default:
		return 0
	}
}

func compareBE128(v1, v2 *[16]byte) int {
	x := binary.BigEndian.Uint64(v1[:8])
	y := binary.BigEndian.Uint64(v2[:8])
	switch {
	case x < y:
		return -1
	case x > y:
		return +1
	}
	x = binary.BigEndian.Uint64(v1[8:])
	y = binary.BigEndian.Uint64(v2[8:])
	switch {
	case x < y:
		return -1
	case x > y:
		return +1
	default:
		return 0
	}
}

func lessBE128(v1, v2 *[16]byte) bool {
	x := binary.BigEndian.Uint64(v1[:8])
	y := binary.BigEndian.Uint64(v2[:8])
	switch {
	case x < y:
		return true
	case x > y:
		return false
	}
	x = binary.BigEndian.Uint64(v1[8:])
	y = binary.BigEndian.Uint64(v2[8:])
	return x < y
}

// ColumnPool is an interface for pooling [][2]int32 buffers used during row comparison.
// Implementations should provide Get and Put methods to acquire and release buffers.
// The Get method accepts a maxCapacity hint to allocate buffers with appropriate capacity.
type ColumnPool interface {
	Get(maxCapacity int) [][2]int32
	Put(buf [][2]int32)
}

// ComparatorConfig carries configuration options for row comparators.
type ComparatorConfig struct {
	ColumnPool ColumnPool
}

// WithColumnPool creates a comparator config which sets the ColumnPool used for
// buffer allocation during row comparison.
//
// If pool is nil, a default sync.Pool-based implementation is used.
func WithColumnPool(pool ColumnPool) *ComparatorConfig {
	return &ComparatorConfig{ColumnPool: pool}
}

// defaultColumnPool is a default implementation using sync.Pool for backward compatibility.
type defaultColumnPool struct {
	pool sync.Pool
}

func newDefaultColumnPool() *defaultColumnPool {
	return &defaultColumnPool{
		pool: sync.Pool{
			New: func() any { return make([][2]int32, 0, 128) },
		},
	}
}

func (p *defaultColumnPool) Get(maxCapacity int) [][2]int32 {
	buf := p.pool.Get().([][2]int32)
	if cap(buf) < maxCapacity {
		buf = make([][2]int32, 0, maxCapacity)
	} else {
		buf = buf[:0]
	}
	return buf
}

func (p *defaultColumnPool) Put(buf [][2]int32) {
	buf = buf[:0]
	p.pool.Put(buf)
}

var defaultPool ColumnPool = newDefaultColumnPool()

const stackAllocThreshold = 32

func compareRowsFuncOf(schema *Schema, sortingColumns []SortingColumn, pool ColumnPool) func(Row, Row) int {
	leafColumns := make([]leafColumn, len(sortingColumns))
	canCompareRows := true

	forEachLeafColumnOf(schema, func(leaf leafColumn) {
		if leaf.maxRepetitionLevel > 0 {
			canCompareRows = false
		}

		if sortingIndex := searchSortingColumn(sortingColumns, leaf.path); sortingIndex < len(sortingColumns) {
			leafColumns[sortingIndex] = leaf

			if leaf.maxDefinitionLevel > 0 {
				canCompareRows = false
			}
		}
	})

	// This is an optimization for the common case where rows
	// are sorted by non-optional, non-repeated columns.
	//
	// The sort function can make the assumption that it will
	// find the column value at the current column index, and
	// does not need to scan the rows looking for values with
	// a matching column index.
	if canCompareRows {
		return compareRowsFuncOfColumnIndexes(leafColumns, sortingColumns)
	}

	return compareRowsFuncOfColumnValues(leafColumns, sortingColumns, pool)
}

func compareRowsUnordered(Row, Row) int { return 0 }

//go:noinline
func compareRowsFuncOfIndexColumns(compareFuncs []func(Row, Row) int) func(Row, Row) int {
	return func(row1, row2 Row) int {
		for _, compare := range compareFuncs {
			if cmp := compare(row1, row2); cmp != 0 {
				return cmp
			}
		}
		return 0
	}
}

//go:noinline
func compareRowsFuncOfIndexAscending(columnIndex int16, typ Type) func(Row, Row) int {
	return func(row1, row2 Row) int { return typ.Compare(row1[columnIndex], row2[columnIndex]) }
}

//go:noinline
func compareRowsFuncOfIndexDescending(columnIndex int16, typ Type) func(Row, Row) int {
	return func(row1, row2 Row) int { return -typ.Compare(row1[columnIndex], row2[columnIndex]) }
}

//go:noinline
func compareRowsFuncOfColumnIndexes(leafColumns []leafColumn, sortingColumns []SortingColumn) func(Row, Row) int {
	compareFuncs := make([]func(Row, Row) int, len(sortingColumns))

	for sortingIndex, sortingColumn := range sortingColumns {
		leaf := leafColumns[sortingIndex]
		typ := leaf.node.Type()

		if sortingColumn.Descending() {
			compareFuncs[sortingIndex] = compareRowsFuncOfIndexDescending(leaf.columnIndex, typ)
		} else {
			compareFuncs[sortingIndex] = compareRowsFuncOfIndexAscending(leaf.columnIndex, typ)
		}
	}

	switch len(compareFuncs) {
	case 0:
		return compareRowsUnordered
	case 1:
		return compareFuncs[0]
	default:
		return compareRowsFuncOfIndexColumns(compareFuncs)
	}
}

//go:noinline
func compareRowsFuncOfColumnValues(leafColumns []leafColumn, sortingColumns []SortingColumn, pool ColumnPool) func(Row, Row) int {
	if pool == nil {
		pool = defaultPool
	}

	highestColumnIndex := int16(0)
	columnIndexes := make([]int16, len(sortingColumns))
	compareFuncs := make([]func(Value, Value) int, len(sortingColumns))

	for sortingIndex, sortingColumn := range sortingColumns {
		leaf := leafColumns[sortingIndex]
		compare := leaf.node.Type().Compare

		if sortingColumn.Descending() {
			compare = CompareDescending(compare)
		}

		if leaf.maxDefinitionLevel > 0 {
			if sortingColumn.NullsFirst() {
				compare = CompareNullsFirst(compare)
			} else {
				compare = CompareNullsLast(compare)
			}
		}

		columnIndexes[sortingIndex] = leaf.columnIndex
		compareFuncs[sortingIndex] = compare

		if leaf.columnIndex > highestColumnIndex {
			highestColumnIndex = leaf.columnIndex
		}
	}

	requiredCapacity := int(highestColumnIndex) + 1
	useStackAlloc := requiredCapacity <= stackAllocThreshold

	return func(row1, row2 Row) int {
		var columns1, columns2 [][2]int32

		if useStackAlloc {
			// Use stack allocation for small sizes
			var stack1, stack2 [stackAllocThreshold][2]int32
			columns1 = stack1[:0:requiredCapacity]
			columns2 = stack2[:0:requiredCapacity]
		} else {
			// Use pool with capacity hint
			columns1 = pool.Get(requiredCapacity)
			columns2 = pool.Get(requiredCapacity)
			defer func() {
				pool.Put(columns1)
				pool.Put(columns2)
			}()
			// Pre-allocate with correct capacity to avoid append reallocations
			columns1 = columns1[:0:requiredCapacity]
			columns2 = columns2[:0:requiredCapacity]
		}

		i1 := 0
		i2 := 0

		for columnIndex := int16(0); columnIndex <= highestColumnIndex; columnIndex++ {
			j1 := i1 + 1
			j2 := i2 + 1

			for j1 < len(row1) && row1[j1].columnIndex == ^columnIndex {
				j1++
			}

			for j2 < len(row2) && row2[j2].columnIndex == ^columnIndex {
				j2++
			}

			columns1 = append(columns1, [2]int32{int32(i1), int32(j1)})
			columns2 = append(columns2, [2]int32{int32(i2), int32(j2)})
			i1 = j1
			i2 = j2
		}

		for i, compare := range compareFuncs {
			columnIndex := columnIndexes[i]
			offsets1 := columns1[columnIndex]
			offsets2 := columns2[columnIndex]
			// Compute offsets inline to reduce slice view allocations
			start1, end1 := int(offsets1[0]), int(offsets1[1])
			start2, end2 := int(offsets2[0]), int(offsets2[1])
			values1 := row1[start1:end1]
			values2 := row2[start2:end2]
			i1 := 0
			i2 := 0

			for i1 < len(values1) && i2 < len(values2) {
				if cmp := compare(values1[i1], values2[i2]); cmp != 0 {
					return cmp
				}
				i1++
				i2++
			}

			if i1 < len(values1) {
				return +1
			}
			if i2 < len(values2) {
				return -1
			}
		}
		return 0
	}
}
