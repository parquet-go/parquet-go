package parquet

import (
	"github.com/parquet-go/bitpack/unsafecast"
	"github.com/parquet-go/parquet-go/deprecated"
	"github.com/parquet-go/parquet-go/encoding/plain"
	"github.com/parquet-go/parquet-go/format"
)

type ColumnIndex interface {
	// NumPages returns the number of paged in the column index.
	NumPages() int

	// Returns the number of null values in the page at the given index.
	NullCount(int) int64

	// Tells whether the page at the given index contains null values only.
	NullPage(int) bool

	// PageIndex return min/max bounds for the page at the given index in the
	// column.
	MinValue(int) Value
	MaxValue(int) Value

	// IsAscending returns true if the column index min/max values are sorted
	// in ascending order (based on the ordering rules of the column's logical
	// type).
	IsAscending() bool

	// IsDescending returns true if the column index min/max values are sorted
	// in descending order (based on the ordering rules of the column's logical
	// type).
	IsDescending() bool
}

// NewColumnIndex constructs a ColumnIndex instance from the given parquet
// format column index. The kind argument configures the type of values
func NewColumnIndex(kind Kind, index *format.ColumnIndex) ColumnIndex {
	return &formatColumnIndex{
		kind:  kind,
		index: index,
	}
}

type formatColumnIndex struct {
	kind  Kind
	index *format.ColumnIndex
}

func (f *formatColumnIndex) NumPages() int {
	return len(f.index.MinValues)
}

func (f *formatColumnIndex) NullCount(i int) int64 {
	if len(f.index.NullCounts) > 0 {
		return f.index.NullCounts[i]
	}
	return 0
}

func (f *formatColumnIndex) NullPage(i int) bool {
	return len(f.index.NullPages) > 0 && f.index.NullPages[i]
}

func (f *formatColumnIndex) MinValue(i int) Value {
	if f.NullPage(i) {
		return Value{}
	}
	return f.kind.Value(f.index.MinValues[i])
}

func (f *formatColumnIndex) MaxValue(i int) Value {
	if f.NullPage(i) {
		return Value{}
	}
	return f.kind.Value(f.index.MaxValues[i])
}

func (f *formatColumnIndex) IsAscending() bool {
	return f.index.BoundaryOrder == format.Ascending
}

func (f *formatColumnIndex) IsDescending() bool {
	return f.index.BoundaryOrder == format.Descending
}

type FileColumnIndex struct {
	index *format.ColumnIndex
	kind  Kind
}

func (i *FileColumnIndex) NumPages() int {
	return len(i.index.NullPages)
}

func (i *FileColumnIndex) NullCount(j int) int64 {
	if len(i.index.NullCounts) > 0 {
		return i.index.NullCounts[j]
	}
	return 0
}

func (i *FileColumnIndex) NullPage(j int) bool {
	return isNullPage(j, i.index)
}

func (i *FileColumnIndex) MinValue(j int) Value {
	if i.NullPage(j) {
		return Value{}
	}
	return i.makeValue(i.index.MinValues[j])
}

func (i *FileColumnIndex) MaxValue(j int) Value {
	if i.NullPage(j) {
		return Value{}
	}
	return i.makeValue(i.index.MaxValues[j])
}

func (i *FileColumnIndex) IsAscending() bool {
	return i.index.BoundaryOrder == format.Ascending
}

func (i *FileColumnIndex) IsDescending() bool {
	return i.index.BoundaryOrder == format.Descending
}

func (i *FileColumnIndex) makeValue(b []byte) Value {
	return i.kind.Value(b)
}

func isNullPage(j int, index *format.ColumnIndex) bool {
	return len(index.NullPages) > 0 && index.NullPages[j]
}

type emptyColumnIndex struct{}

func (emptyColumnIndex) NumPages() int       { return 0 }
func (emptyColumnIndex) NullCount(int) int64 { return 0 }
func (emptyColumnIndex) NullPage(int) bool   { return false }
func (emptyColumnIndex) MinValue(int) Value  { return Value{} }
func (emptyColumnIndex) MaxValue(int) Value  { return Value{} }
func (emptyColumnIndex) IsAscending() bool   { return false }
func (emptyColumnIndex) IsDescending() bool  { return false }

type booleanColumnIndex struct{ page *booleanPage }

func (i booleanColumnIndex) NumPages() int       { return 1 }
func (i booleanColumnIndex) NullCount(int) int64 { return 0 }
func (i booleanColumnIndex) NullPage(int) bool   { return false }
func (i booleanColumnIndex) MinValue(int) Value  { return makeValueBoolean(i.page.min()) }
func (i booleanColumnIndex) MaxValue(int) Value  { return makeValueBoolean(i.page.max()) }
func (i booleanColumnIndex) IsAscending() bool   { return false }
func (i booleanColumnIndex) IsDescending() bool  { return false }

type int32ColumnIndex struct{ page *int32Page }

func (i int32ColumnIndex) NumPages() int       { return 1 }
func (i int32ColumnIndex) NullCount(int) int64 { return 0 }
func (i int32ColumnIndex) NullPage(int) bool   { return false }
func (i int32ColumnIndex) MinValue(int) Value  { return makeValueInt32(i.page.min()) }
func (i int32ColumnIndex) MaxValue(int) Value  { return makeValueInt32(i.page.max()) }
func (i int32ColumnIndex) IsAscending() bool   { return false }
func (i int32ColumnIndex) IsDescending() bool  { return false }

type int64ColumnIndex struct{ page *int64Page }

func (i int64ColumnIndex) NumPages() int       { return 1 }
func (i int64ColumnIndex) NullCount(int) int64 { return 0 }
func (i int64ColumnIndex) NullPage(int) bool   { return false }
func (i int64ColumnIndex) MinValue(int) Value  { return makeValueInt64(i.page.min()) }
func (i int64ColumnIndex) MaxValue(int) Value  { return makeValueInt64(i.page.max()) }
func (i int64ColumnIndex) IsAscending() bool   { return false }
func (i int64ColumnIndex) IsDescending() bool  { return false }

type int96ColumnIndex struct{ page *int96Page }

func (i int96ColumnIndex) NumPages() int       { return 1 }
func (i int96ColumnIndex) NullCount(int) int64 { return 0 }
func (i int96ColumnIndex) NullPage(int) bool   { return false }
func (i int96ColumnIndex) MinValue(int) Value  { return makeValueInt96(i.page.min()) }
func (i int96ColumnIndex) MaxValue(int) Value  { return makeValueInt96(i.page.max()) }
func (i int96ColumnIndex) IsAscending() bool   { return false }
func (i int96ColumnIndex) IsDescending() bool  { return false }

type floatColumnIndex struct{ page *floatPage }

func (i floatColumnIndex) NumPages() int       { return 1 }
func (i floatColumnIndex) NullCount(int) int64 { return 0 }
func (i floatColumnIndex) NullPage(int) bool   { return false }
func (i floatColumnIndex) MinValue(int) Value  { return makeValueFloat(i.page.min()) }
func (i floatColumnIndex) MaxValue(int) Value  { return makeValueFloat(i.page.max()) }
func (i floatColumnIndex) IsAscending() bool   { return false }
func (i floatColumnIndex) IsDescending() bool  { return false }

type doubleColumnIndex struct{ page *doublePage }

func (i doubleColumnIndex) NumPages() int       { return 1 }
func (i doubleColumnIndex) NullCount(int) int64 { return 0 }
func (i doubleColumnIndex) NullPage(int) bool   { return false }
func (i doubleColumnIndex) MinValue(int) Value  { return makeValueDouble(i.page.min()) }
func (i doubleColumnIndex) MaxValue(int) Value  { return makeValueDouble(i.page.max()) }
func (i doubleColumnIndex) IsAscending() bool   { return false }
func (i doubleColumnIndex) IsDescending() bool  { return false }

type byteArrayColumnIndex struct{ page *byteArrayPage }

func (i byteArrayColumnIndex) NumPages() int       { return 1 }
func (i byteArrayColumnIndex) NullCount(int) int64 { return 0 }
func (i byteArrayColumnIndex) NullPage(int) bool   { return false }
func (i byteArrayColumnIndex) MinValue(int) Value  { return makeValueBytes(ByteArray, i.page.min()) }
func (i byteArrayColumnIndex) MaxValue(int) Value  { return makeValueBytes(ByteArray, i.page.max()) }
func (i byteArrayColumnIndex) IsAscending() bool   { return false }
func (i byteArrayColumnIndex) IsDescending() bool  { return false }

type fixedLenByteArrayColumnIndex struct{ page *fixedLenByteArrayPage }

func (i fixedLenByteArrayColumnIndex) NumPages() int       { return 1 }
func (i fixedLenByteArrayColumnIndex) NullCount(int) int64 { return 0 }
func (i fixedLenByteArrayColumnIndex) NullPage(int) bool   { return false }
func (i fixedLenByteArrayColumnIndex) MinValue(int) Value {
	return makeValueBytes(FixedLenByteArray, i.page.min())
}
func (i fixedLenByteArrayColumnIndex) MaxValue(int) Value {
	return makeValueBytes(FixedLenByteArray, i.page.max())
}
func (i fixedLenByteArrayColumnIndex) IsAscending() bool  { return false }
func (i fixedLenByteArrayColumnIndex) IsDescending() bool { return false }

type uint32ColumnIndex struct{ page *uint32Page }

func (i uint32ColumnIndex) NumPages() int       { return 1 }
func (i uint32ColumnIndex) NullCount(int) int64 { return 0 }
func (i uint32ColumnIndex) NullPage(int) bool   { return false }
func (i uint32ColumnIndex) MinValue(int) Value  { return makeValueUint32(i.page.min()) }
func (i uint32ColumnIndex) MaxValue(int) Value  { return makeValueUint32(i.page.max()) }
func (i uint32ColumnIndex) IsAscending() bool   { return false }
func (i uint32ColumnIndex) IsDescending() bool  { return false }

type uint64ColumnIndex struct{ page *uint64Page }

func (i uint64ColumnIndex) NumPages() int       { return 1 }
func (i uint64ColumnIndex) NullCount(int) int64 { return 0 }
func (i uint64ColumnIndex) NullPage(int) bool   { return false }
func (i uint64ColumnIndex) MinValue(int) Value  { return makeValueUint64(i.page.min()) }
func (i uint64ColumnIndex) MaxValue(int) Value  { return makeValueUint64(i.page.max()) }
func (i uint64ColumnIndex) IsAscending() bool   { return false }
func (i uint64ColumnIndex) IsDescending() bool  { return false }

type be128ColumnIndex struct{ page *be128Page }

func (i be128ColumnIndex) NumPages() int       { return 1 }
func (i be128ColumnIndex) NullCount(int) int64 { return 0 }
func (i be128ColumnIndex) NullPage(int) bool   { return false }
func (i be128ColumnIndex) MinValue(int) Value  { return makeValueBytes(FixedLenByteArray, i.page.min()) }
func (i be128ColumnIndex) MaxValue(int) Value  { return makeValueBytes(FixedLenByteArray, i.page.max()) }
func (i be128ColumnIndex) IsAscending() bool   { return false }
func (i be128ColumnIndex) IsDescending() bool  { return false }

// The ColumnIndexer interface is implemented by types that support generating
// parquet column indexes.
//
// The package does not export any types that implement this interface, programs
// must call NewColumnIndexer on a Type instance to construct column indexers.
type ColumnIndexer interface {
	// Resets the column indexer state.
	Reset()

	// Add a page to the column indexer.
	IndexPage(numValues, numNulls int64, min, max Value)

	// ColumnIndex Generates a format.ColumnIndex value from the current state of the
	// column indexer. If r is not nil, the buffers in r are reused.
	//
	// The returned value may reference internal buffers, in which case the
	// values remain valid until the next call to IndexPage or Reset on the
	// column indexer.
	ColumnIndex(r *format.ColumnIndex) format.ColumnIndex
}

type baseColumnIndexer struct {
	nullPages  []bool
	nullCounts []int64
}

func (i *baseColumnIndexer) reset() {
	i.nullPages = i.nullPages[:0]
	i.nullCounts = i.nullCounts[:0]
}

func (i *baseColumnIndexer) observe(numValues, numNulls int64) {
	i.nullPages = append(i.nullPages, numValues == numNulls)
	i.nullCounts = append(i.nullCounts, numNulls)
}

func (i *baseColumnIndexer) columnIndex(r *format.ColumnIndex, minValues, maxValues [][]byte, minOrder, maxOrder int) format.ColumnIndex {
	if r == nil {
		nullPages := make([]bool, len(i.nullPages))
		copy(nullPages, i.nullPages)
		nullCounts := make([]int64, len(i.nullCounts))
		copy(nullCounts, i.nullCounts)
		return format.ColumnIndex{
			NullPages:     nullPages,
			NullCounts:    nullCounts,
			MinValues:     minValues,
			MaxValues:     maxValues,
			BoundaryOrder: boundaryOrderOf(minOrder, maxOrder),
		}
	} else {
		r.NullPages = append(r.NullPages[:0], i.nullPages...)
		r.NullCounts = append(r.NullCounts[:0], i.nullCounts...)
		r.MinValues = append(r.MinValues[:0], minValues...)
		r.MaxValues = append(r.MaxValues[:0], maxValues...)
		r.BoundaryOrder = boundaryOrderOf(minOrder, maxOrder)
	}
	return *r
}

type booleanColumnIndexer struct {
	baseColumnIndexer
	minValues []bool
	maxValues []bool
	scratch1  [][]byte
	scratch2  [][]byte
}

func newBooleanColumnIndexer() *booleanColumnIndexer {
	return &booleanColumnIndexer{
		scratch1: make([][]byte, 0),
		scratch2: make([][]byte, 0),
	}
}

func (i *booleanColumnIndexer) Reset() {
	i.reset()
	i.minValues = i.minValues[:0]
	i.maxValues = i.maxValues[:0]
	i.scratch1 = i.scratch1[:0]
	i.scratch2 = i.scratch2[:0]
}

func (i *booleanColumnIndexer) IndexPage(numValues, numNulls int64, min, max Value) {
	i.observe(numValues, numNulls)
	i.minValues = append(i.minValues, min.boolean())
	i.maxValues = append(i.maxValues, max.boolean())
}

func (i *booleanColumnIndexer) ColumnIndex(r *format.ColumnIndex) format.ColumnIndex {
	return i.columnIndex(r,
		splitFixedLenByteArrays(&i.scratch1, unsafecast.Slice[byte](i.minValues), 1),
		splitFixedLenByteArrays(&i.scratch2, unsafecast.Slice[byte](i.maxValues), 1),
		orderOfBool(i.minValues),
		orderOfBool(i.maxValues),
	)
}

type int32ColumnIndexer struct {
	baseColumnIndexer
	minValues []int32
	maxValues []int32
	scratch1  [][]byte
	scratch2  [][]byte
}

func newInt32ColumnIndexer() *int32ColumnIndexer {
	return &int32ColumnIndexer{
		scratch1: make([][]byte, 0),
		scratch2: make([][]byte, 0),
	}
}

func (i *int32ColumnIndexer) Reset() {
	i.reset()
	i.minValues = i.minValues[:0]
	i.maxValues = i.maxValues[:0]
	i.scratch1 = i.scratch1[:0]
	i.scratch2 = i.scratch2[:0]
}

func (i *int32ColumnIndexer) IndexPage(numValues, numNulls int64, min, max Value) {
	i.observe(numValues, numNulls)
	i.minValues = append(i.minValues, min.int32())
	i.maxValues = append(i.maxValues, max.int32())
}

func (i *int32ColumnIndexer) ColumnIndex(r *format.ColumnIndex) format.ColumnIndex {
	return i.columnIndex(r,
		splitFixedLenByteArrays(&i.scratch1, columnIndexInt32Values(i.minValues), 4),
		splitFixedLenByteArrays(&i.scratch2, columnIndexInt32Values(i.maxValues), 4),
		orderOfInt32(i.minValues),
		orderOfInt32(i.maxValues),
	)
}

type int64ColumnIndexer struct {
	baseColumnIndexer
	minValues []int64
	maxValues []int64
	scratch1  [][]byte
	scratch2  [][]byte
}

func newInt64ColumnIndexer() *int64ColumnIndexer {
	return &int64ColumnIndexer{
		scratch1: make([][]byte, 0),
		scratch2: make([][]byte, 0),
	}
}

func (i *int64ColumnIndexer) Reset() {
	i.reset()
	i.minValues = i.minValues[:0]
	i.maxValues = i.maxValues[:0]
	i.scratch1 = i.scratch1[:0]
	i.scratch2 = i.scratch2[:0]
}

func (i *int64ColumnIndexer) IndexPage(numValues, numNulls int64, min, max Value) {
	i.observe(numValues, numNulls)
	i.minValues = append(i.minValues, min.int64())
	i.maxValues = append(i.maxValues, max.int64())
}

func (i *int64ColumnIndexer) ColumnIndex(r *format.ColumnIndex) format.ColumnIndex {
	return i.columnIndex(r,
		splitFixedLenByteArrays(&i.scratch1, columnIndexInt64Values(i.minValues), 8),
		splitFixedLenByteArrays(&i.scratch2, columnIndexInt64Values(i.maxValues), 8),
		orderOfInt64(i.minValues),
		orderOfInt64(i.maxValues),
	)
}

type int96ColumnIndexer struct {
	baseColumnIndexer
	minValues []deprecated.Int96
	maxValues []deprecated.Int96
	scratch1  [][]byte
	scratch2  [][]byte
}

func newInt96ColumnIndexer() *int96ColumnIndexer {
	return &int96ColumnIndexer{
		scratch1: make([][]byte, 0),
		scratch2: make([][]byte, 0),
	}
}

func (i *int96ColumnIndexer) Reset() {
	i.reset()
	i.minValues = i.minValues[:0]
	i.maxValues = i.maxValues[:0]
	i.scratch1 = i.scratch1[:0]
	i.scratch2 = i.scratch2[:0]
}

func (i *int96ColumnIndexer) IndexPage(numValues, numNulls int64, min, max Value) {
	i.observe(numValues, numNulls)
	i.minValues = append(i.minValues, min.Int96())
	i.maxValues = append(i.maxValues, max.Int96())
}

func (i *int96ColumnIndexer) ColumnIndex(r *format.ColumnIndex) format.ColumnIndex {
	return i.columnIndex(r,
		splitFixedLenByteArrays(&i.scratch1, columnIndexInt96Values(i.minValues), 12),
		splitFixedLenByteArrays(&i.scratch2, columnIndexInt96Values(i.maxValues), 12),
		deprecated.OrderOfInt96(i.minValues),
		deprecated.OrderOfInt96(i.maxValues),
	)
}

type floatColumnIndexer struct {
	baseColumnIndexer
	minValues []float32
	maxValues []float32
	scratch1  [][]byte
	scratch2  [][]byte
}

func newFloatColumnIndexer() *floatColumnIndexer {
	return &floatColumnIndexer{
		scratch1: make([][]byte, 0),
		scratch2: make([][]byte, 0),
	}
}

func (i *floatColumnIndexer) Reset() {
	i.reset()
	i.minValues = i.minValues[:0]
	i.maxValues = i.maxValues[:0]
	i.scratch1 = i.scratch1[:0]
	i.scratch2 = i.scratch2[:0]
}

func (i *floatColumnIndexer) IndexPage(numValues, numNulls int64, min, max Value) {
	i.observe(numValues, numNulls)
	i.minValues = append(i.minValues, min.float())
	i.maxValues = append(i.maxValues, max.float())
}

func (i *floatColumnIndexer) ColumnIndex(r *format.ColumnIndex) format.ColumnIndex {
	return i.columnIndex(r,
		splitFixedLenByteArrays(&i.scratch1, columnIndexFloatValues(i.minValues), 4),
		splitFixedLenByteArrays(&i.scratch2, columnIndexFloatValues(i.maxValues), 4),
		orderOfFloat32(i.minValues),
		orderOfFloat32(i.maxValues),
	)
}

type doubleColumnIndexer struct {
	baseColumnIndexer
	minValues []float64
	maxValues []float64
	scratch1  [][]byte
	scratch2  [][]byte
}

func newDoubleColumnIndexer() *doubleColumnIndexer {
	return &doubleColumnIndexer{
		scratch1: make([][]byte, 0),
		scratch2: make([][]byte, 0),
	}
}

func (i *doubleColumnIndexer) Reset() {
	i.reset()
	i.minValues = i.minValues[:0]
	i.maxValues = i.maxValues[:0]
	i.scratch1 = i.scratch1[:0]
	i.scratch2 = i.scratch2[:0]
}

func (i *doubleColumnIndexer) IndexPage(numValues, numNulls int64, min, max Value) {
	i.observe(numValues, numNulls)
	i.minValues = append(i.minValues, min.double())
	i.maxValues = append(i.maxValues, max.double())
}

func (i *doubleColumnIndexer) ColumnIndex(r *format.ColumnIndex) format.ColumnIndex {
	return i.columnIndex(r,
		splitFixedLenByteArrays(&i.scratch1, columnIndexDoubleValues(i.minValues), 8),
		splitFixedLenByteArrays(&i.scratch2, columnIndexDoubleValues(i.maxValues), 8),
		orderOfFloat64(i.minValues),
		orderOfFloat64(i.maxValues),
	)
}

type byteArrayColumnIndexer struct {
	baseColumnIndexer
	sizeLimit int
	minValues []byte
	maxValues []byte
	scratch   []byte
}

func newByteArrayColumnIndexer(sizeLimit int) *byteArrayColumnIndexer {
	return &byteArrayColumnIndexer{
		sizeLimit: sizeLimit,
		scratch:   make([]byte, 0),
	}
}

func (i *byteArrayColumnIndexer) Reset() {
	i.reset()
	i.minValues = i.minValues[:0]
	i.maxValues = i.maxValues[:0]
	i.scratch = i.scratch[:0]
}

func (i *byteArrayColumnIndexer) IndexPage(numValues, numNulls int64, min, max Value) {
	i.observe(numValues, numNulls)
	i.minValues = plain.AppendByteArray(i.minValues, min.byteArray())
	i.maxValues = plain.AppendByteArray(i.maxValues, max.byteArray())
}

func (i *byteArrayColumnIndexer) ColumnIndex(r *format.ColumnIndex) format.ColumnIndex {
	var reuseMin, reuseMax *[][]byte
	if r != nil {
		reuseMin = &r.MinValues
		reuseMax = &r.MaxValues
	}
	minValues := splitByteArrays(reuseMin, &i.scratch, i.minValues)
	maxValues := splitByteArrays(reuseMax, &i.scratch, i.maxValues)
	if sizeLimit := i.sizeLimit; sizeLimit > 0 {
		for i, v := range minValues {
			minValues[i] = truncateLargeMinByteArrayValue(v, sizeLimit)
		}
		for i, v := range maxValues {
			maxValues[i] = truncateLargeMaxByteArrayValue(v, sizeLimit)
		}
	}
	return i.columnIndex(r,
		minValues,
		maxValues,
		orderOfBytes(minValues),
		orderOfBytes(maxValues),
	)
}

type fixedLenByteArrayColumnIndexer struct {
	baseColumnIndexer
	size      int
	sizeLimit int
	minValues []byte
	maxValues []byte
	scratch1  [][]byte
	scratch2  [][]byte
}

func newFixedLenByteArrayColumnIndexer(size, sizeLimit int) *fixedLenByteArrayColumnIndexer {
	return &fixedLenByteArrayColumnIndexer{
		size:      size,
		sizeLimit: sizeLimit,
		scratch1:  make([][]byte, 0),
		scratch2:  make([][]byte, 0),
	}
}

func (i *fixedLenByteArrayColumnIndexer) Reset() {
	i.reset()
	i.minValues = i.minValues[:0]
	i.maxValues = i.maxValues[:0]
	i.scratch1 = i.scratch1[:0]
	i.scratch2 = i.scratch2[:0]
}

func (i *fixedLenByteArrayColumnIndexer) IndexPage(numValues, numNulls int64, min, max Value) {
	i.observe(numValues, numNulls)
	i.minValues = append(i.minValues, min.byteArray()...)
	i.maxValues = append(i.maxValues, max.byteArray()...)
}

func (i *fixedLenByteArrayColumnIndexer) ColumnIndex(r *format.ColumnIndex) format.ColumnIndex {
	minValues := splitFixedLenByteArrays(&i.scratch1, i.minValues, i.size)
	maxValues := splitFixedLenByteArrays(&i.scratch2, i.maxValues, i.size)
	if sizeLimit := i.sizeLimit; sizeLimit > 0 {
		for i, v := range minValues {
			minValues[i] = truncateLargeMinByteArrayValue(v, sizeLimit)
		}
		for i, v := range maxValues {
			maxValues[i] = truncateLargeMaxByteArrayValue(v, sizeLimit)
		}
	}
	return i.columnIndex(r,
		minValues,
		maxValues,
		orderOfBytes(minValues),
		orderOfBytes(maxValues),
	)
}

type uint32ColumnIndexer struct {
	baseColumnIndexer
	minValues []uint32
	maxValues []uint32
	scratch1  [][]byte
	scratch2  [][]byte
}

func newUint32ColumnIndexer() *uint32ColumnIndexer {
	return &uint32ColumnIndexer{
		scratch1: make([][]byte, 0),
		scratch2: make([][]byte, 0),
	}
}

func (i *uint32ColumnIndexer) Reset() {
	i.reset()
	i.minValues = i.minValues[:0]
	i.maxValues = i.maxValues[:0]
	i.scratch1 = i.scratch1[:0]
	i.scratch2 = i.scratch2[:0]
}

func (i *uint32ColumnIndexer) IndexPage(numValues, numNulls int64, min, max Value) {
	i.observe(numValues, numNulls)
	i.minValues = append(i.minValues, min.uint32())
	i.maxValues = append(i.maxValues, max.uint32())
}

func (i *uint32ColumnIndexer) ColumnIndex(r *format.ColumnIndex) format.ColumnIndex {
	return i.columnIndex(r,
		splitFixedLenByteArrays(&i.scratch1, columnIndexUint32Values(i.minValues), 4),
		splitFixedLenByteArrays(&i.scratch2, columnIndexUint32Values(i.maxValues), 4),
		orderOfUint32(i.minValues),
		orderOfUint32(i.maxValues),
	)
}

type uint64ColumnIndexer struct {
	baseColumnIndexer
	minValues []uint64
	maxValues []uint64
	scratch1  [][]byte
	scratch2  [][]byte
}

func newUint64ColumnIndexer() *uint64ColumnIndexer {
	return &uint64ColumnIndexer{
		scratch1: make([][]byte, 0),
		scratch2: make([][]byte, 0),
	}
}

func (i *uint64ColumnIndexer) Reset() {
	i.reset()
	i.minValues = i.minValues[:0]
	i.maxValues = i.maxValues[:0]
	i.scratch1 = i.scratch1[:0]
	i.scratch2 = i.scratch2[:0]
}

func (i *uint64ColumnIndexer) IndexPage(numValues, numNulls int64, min, max Value) {
	i.observe(numValues, numNulls)
	i.minValues = append(i.minValues, min.uint64())
	i.maxValues = append(i.maxValues, max.uint64())
}

func (i *uint64ColumnIndexer) ColumnIndex(r *format.ColumnIndex) format.ColumnIndex {
	return i.columnIndex(r,
		splitFixedLenByteArrays(&i.scratch1, columnIndexUint64Values(i.minValues), 8),
		splitFixedLenByteArrays(&i.scratch2, columnIndexUint64Values(i.maxValues), 8),
		orderOfUint64(i.minValues),
		orderOfUint64(i.maxValues),
	)
}

type be128ColumnIndexer struct {
	baseColumnIndexer
	minValues [][16]byte
	maxValues [][16]byte
	scratch1  [][]byte
	scratch2  [][]byte
}

func newBE128ColumnIndexer() *be128ColumnIndexer {
	return &be128ColumnIndexer{
		scratch1: make([][]byte, 0),
		scratch2: make([][]byte, 0),
	}
}

func (i *be128ColumnIndexer) Reset() {
	i.reset()
	i.minValues = i.minValues[:0]
	i.maxValues = i.maxValues[:0]
	i.scratch1 = i.scratch1[:0]
	i.scratch2 = i.scratch2[:0]
}

func (i *be128ColumnIndexer) IndexPage(numValues, numNulls int64, min, max Value) {
	i.observe(numValues, numNulls)
	if !min.IsNull() {
		i.minValues = append(i.minValues, *(*[16]byte)(min.byteArray()))
	}
	if !max.IsNull() {
		i.maxValues = append(i.maxValues, *(*[16]byte)(max.byteArray()))
	}
}

func (i *be128ColumnIndexer) ColumnIndex(r *format.ColumnIndex) format.ColumnIndex {
	minValues := splitFixedLenByteArrays(&i.scratch1, unsafecast.Slice[byte](i.minValues), 16)
	maxValues := splitFixedLenByteArrays(&i.scratch2, unsafecast.Slice[byte](i.maxValues), 16)
	return i.columnIndex(r,
		minValues,
		maxValues,
		orderOfBytes(minValues),
		orderOfBytes(maxValues),
	)
}

func truncateLargeMinByteArrayValue(value []byte, sizeLimit int) []byte {
	if len(value) > sizeLimit {
		value = value[:sizeLimit]
	}
	return value
}

// truncateLargeMaxByteArrayValue truncates the given byte array to the given size limit.
// If the given byte array is truncated, it is incremented by 1 in place.
func truncateLargeMaxByteArrayValue(value []byte, sizeLimit int) []byte {
	if len(value) > sizeLimit {
		value = value[:sizeLimit]
		incrementByteArrayInplace(value)
	}
	return value
}

// incrementByteArray increments the given byte array by 1.
// Reference: https://github.com/apache/parquet-java/blob/master/parquet-column/src/main/java/org/apache/parquet/internal/column/columnindex/BinaryTruncator.java#L124
func incrementByteArrayInplace(value []byte) {
	for i := len(value) - 1; i >= 0; i-- {
		value[i]++
		if value[i] != 0 { // Did not overflow: 0xFF -> 0x00
			return
		}
	}
	// Fully overflowed, so restore all to 0xFF
	for i := range value {
		value[i] = 0xFF
	}
}

// splitByteArrays splits the given byte array into multiple byte arrays.
// The returned slice is a slice of slices. Each slice contains the values of a single column.
// The length of the returned slice is the same as the number of values in the given byte array.
// The reuse and scratch parameters are used to avoid allocations.
func splitByteArrays(reuse *[][]byte, scratch *[]byte, data []byte) [][]byte {
	length := 0
	plain.RangeByteArray(data, func([]byte) error {
		length++
		return nil
	})
	var buffer []byte
	var values [][]byte
	if reuse == nil {
		panic("reuse must not be nil")
	} else if *reuse == nil {
		values = make([][]byte, 0, length)
	} else {
		values = (*reuse)[:0]
	}
	if scratch == nil {
		panic("scratch must not be nil")
	} else if *scratch == nil {
		buffer = make([]byte, 0, length)
	} else {
		buffer = (*scratch)[:0]
	}
	plain.RangeByteArray(data, func(value []byte) error {
		offset := len(buffer)
		buffer = append(buffer, value...)
		// Use append if there's not enough capacity in the slice
		if cap(values) <= len(values) {
			// need to make a new slice to hold the actual value and append it,
			// which will grow values
			values = append(values, copyBytes(buffer[offset:]))
		} else {
			values = values[:len(values)+1]
			values[len(values)-1] = append(values[len(values)-1], buffer[offset:]...)
		}

		return nil
	})
	// Need to ensure the passed-in reuse and scratch slices are updated
	*scratch = buffer
	*reuse = values

	return values
}

func splitFixedLenByteArrays(reuse *[][]byte, data []byte, size int) [][]byte {
	var values [][]byte
	if reuse == nil {
		panic("reuse must not be nil")
	} else if *reuse == nil {
		values = make([][]byte, 0, len(data)/size)
	} else {
		values = (*reuse)[:0]
	}
	sizeReq := len(data) / size
	for i := range sizeReq {
		j := (i + 0) * size
		k := (i + 1) * size
		if cap(values) <= sizeReq {
			scratch := make([]byte, 0)
			scratch = append(scratch, data[j:k:k]...)
			values = append(values, scratch)
		} else {
			values = values[:len(values)+1]
			values[i] = append(values[i][:0], data[j:k:k]...)
		}
	}
	// Ensure the passed-in reuse slice is updated
	*reuse = values

	return values
}

func boundaryOrderOf(minOrder, maxOrder int) format.BoundaryOrder {
	if minOrder == maxOrder {
		switch {
		case minOrder > 0:
			return format.Ascending
		case minOrder < 0:
			return format.Descending
		}
	}
	return format.Unordered
}
