package parquet_test

import (
	"encoding/binary"
	"io"
	"math"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
)

// TestSearchNaNColumnIndexBounds covers column indexes whose null_pages flags
// say the endpoints are valid while a FLOAT or DOUBLE endpoint is NaN. An
// ascending index used to binary-search those endpoints and skip the page.
func TestSearchNaNColumnIndexBounds(t *testing.T) {
	nan := math.NaN()
	payloadNaN := math.Float64frombits(0x7ff0000000000001)

	tests := []struct {
		name   string
		kind   parquet.Kind
		pages  [][2]float64
		order  format.BoundaryOrder
		value  float64
		want   int
		cmpNaN bool
	}{
		{
			name:  "ascending nan page is not pruned",
			kind:  parquet.Double,
			pages: [][2]float64{{10, 11}, {12, 13}, {nan, nan}},
			order: format.Ascending,
			value: 0,
			want:  2,
		},
		{
			name:  "ascending payload nan page is not pruned",
			kind:  parquet.Double,
			pages: [][2]float64{{10, 11}, {12, 13}, {payloadNaN, payloadNaN}},
			order: format.Ascending,
			value: 5,
			want:  2,
		},
		{
			name:  "ascending finite hit stays on its page",
			kind:  parquet.Double,
			pages: [][2]float64{{10, 11}, {12, 13}, {nan, nan}},
			order: format.Ascending,
			value: 12.5,
			want:  1,
		},
		{
			name:  "nan max does not hide an earlier page",
			kind:  parquet.Double,
			pages: [][2]float64{{1, nan}, {10, 11}, {20, 21}},
			order: format.Ascending,
			value: 15,
			want:  0,
		},
		{
			name:  "finite min still excludes values below it",
			kind:  parquet.Double,
			pages: [][2]float64{{1, nan}, {10, 11}, {20, 21}},
			order: format.Ascending,
			value: 0,
			want:  3,
		},
		{
			name:  "unordered nan page matches",
			kind:  parquet.Double,
			pages: [][2]float64{{10, 11}, {nan, nan}},
			order: format.Unordered,
			value: 0,
			want:  1,
		},
		{
			name:  "float32 ascending nan page is not pruned",
			kind:  parquet.Float,
			pages: [][2]float64{{10, 11}, {12, 13}, {nan, nan}},
			order: format.Ascending,
			value: 0,
			want:  2,
		},
		{
			// A comparator that sorts NaN above every number must still not
			// use a NaN endpoint to exclude the page.
			name:   "total order comparator ignores nan endpoints",
			kind:   parquet.Double,
			pages:  [][2]float64{{10, 11}, {nan, nan}},
			order:  format.Unordered,
			value:  0,
			want:   1,
			cmpNaN: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			index := makeFloatColumnIndex(test.kind, test.pages, test.order)
			value := floatValue(test.kind, test.value)
			typ := floatType(test.kind)
			var got int
			if test.cmpNaN {
				got = parquet.Find(index, value, compareNaNGreater(test.kind))
			} else {
				got = parquet.Search(index, value, typ)
			}
			if got != test.want {
				t.Fatalf("Find/Search(%v) = %d, want %d", test.value, got, test.want)
			}
		})
	}
}

// TestMergeNaNColumnIndexBounds merges sorted row groups whose column index
// contains a NaN endpoint. Using that endpoint as a row-group bound used to
// concatenate a later finite group in front of a larger value.
func TestMergeNaNColumnIndexBounds(t *testing.T) {
	schema := parquet.NewSchema("s", parquet.Group{"v": parquet.Leaf(parquet.DoubleType)})
	nan := math.NaN()

	sorted := mergeFloatGroups(t, schema, []floatGroup{
		{rows: []float64{1, 2, 3, 4, 100}, pages: [][2]float64{{1, 2}, {nan, nan}, {3, 4}}},
		{rows: []float64{60, 70}, pages: [][2]float64{{60, 70}}},
	})
	want := []float64{1, 2, 3, 4, 60, 70, 100}
	if !floatRowsEqual(sorted, want) {
		t.Fatalf("merged rows = %v, want %v", sorted, want)
	}

	// Finite bounds keep the non-overlapping concatenation order.
	control := mergeFloatGroups(t, schema, []floatGroup{
		{rows: []float64{10, 20}, pages: [][2]float64{{10, 20}}},
		{rows: []float64{30, 40}, pages: [][2]float64{{30, 40}}},
		{rows: []float64{50}, pages: [][2]float64{{50, 50}}},
	})
	want = []float64{10, 20, 30, 40, 50}
	if !floatRowsEqual(control, want) {
		t.Fatalf("finite merged rows = %v, want %v", control, want)
	}
}

type floatGroup struct {
	rows  []float64
	pages [][2]float64
}

func mergeFloatGroups(t *testing.T, schema *parquet.Schema, groups []floatGroup) []float64 {
	t.Helper()
	rowGroups := make([]parquet.RowGroup, len(groups))
	for i, group := range groups {
		rowGroups[i] = &nanBoundsRowGroup{
			schema: schema,
			rows:   group.rows,
			index:  makeFloatColumnIndex(parquet.Double, group.pages, format.Ascending),
		}
	}
	merged, err := parquet.MergeRowGroups(rowGroups, parquet.SortingRowGroupConfig(
		parquet.SortingColumns(parquet.Ascending("v")),
	))
	if err != nil {
		t.Fatal(err)
	}
	rows := merged.Rows()
	defer rows.Close()
	buf := make([]parquet.Row, 8)
	var got []float64
	for {
		n, err := rows.ReadRows(buf)
		for _, row := range buf[:n] {
			got = append(got, row[0].Double())
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	return got
}

func floatRowsEqual(got, want []float64) bool {
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

func makeFloatColumnIndex(kind parquet.Kind, pages [][2]float64, order format.BoundaryOrder) parquet.ColumnIndex {
	minValues := make([][]byte, len(pages))
	maxValues := make([][]byte, len(pages))
	nulls := make([]bool, len(pages))
	counts := make([]int64, len(pages))
	for i, page := range pages {
		minValues[i] = appendFloat(kind, page[0])
		maxValues[i] = appendFloat(kind, page[1])
	}
	index := &format.ColumnIndex{
		NullPages:     nulls,
		NullCounts:    counts,
		MinValues:     minValues,
		MaxValues:     maxValues,
		BoundaryOrder: order,
	}
	return parquet.NewColumnIndex(kind, index)
}

func appendFloat(kind parquet.Kind, value float64) []byte {
	if kind == parquet.Float {
		return binary.LittleEndian.AppendUint32(nil, math.Float32bits(float32(value)))
	}
	return binary.LittleEndian.AppendUint64(nil, math.Float64bits(value))
}

func floatValue(kind parquet.Kind, value float64) parquet.Value {
	if kind == parquet.Float {
		return parquet.ValueOf(float32(value))
	}
	return parquet.ValueOf(value)
}

func floatType(kind parquet.Kind) parquet.Type {
	if kind == parquet.Float {
		return parquet.FloatType
	}
	return parquet.DoubleType
}

// compareNaNGreater orders NaN above every finite value. Column-index endpoints
// that are NaN are still not valid bounds, so Find must not exclude on them.
func compareNaNGreater(kind parquet.Kind) func(parquet.Value, parquet.Value) int {
	typ := floatType(kind)
	return func(a, b parquet.Value) int {
		aNaN := (kind == parquet.Float && math.IsNaN(float64(a.Float()))) || (kind == parquet.Double && math.IsNaN(a.Double()))
		bNaN := (kind == parquet.Float && math.IsNaN(float64(b.Float()))) || (kind == parquet.Double && math.IsNaN(b.Double()))
		switch {
		case aNaN && bNaN:
			return 0
		case aNaN:
			return 1
		case bNaN:
			return -1
		default:
			return typ.Compare(a, b)
		}
	}
}

type nanBoundsRowGroup struct {
	schema *parquet.Schema
	rows   []float64
	index  parquet.ColumnIndex
}

func (g *nanBoundsRowGroup) NumRows() int64 { return int64(len(g.rows)) }
func (g *nanBoundsRowGroup) ColumnChunks() []parquet.ColumnChunk {
	return []parquet.ColumnChunk{nanBoundsChunk{rows: g.rows, index: g.index}}
}
func (g *nanBoundsRowGroup) Schema() *parquet.Schema { return g.schema }
func (g *nanBoundsRowGroup) SortingColumns() []parquet.SortingColumn {
	return []parquet.SortingColumn{parquet.Ascending("v")}
}
func (g *nanBoundsRowGroup) Rows() parquet.Rows {
	return &nanBoundsRows{schema: g.schema, rows: g.rows}
}

type nanBoundsChunk struct {
	rows  []float64
	index parquet.ColumnIndex
}

func (c nanBoundsChunk) Type() parquet.Type   { return parquet.DoubleType }
func (c nanBoundsChunk) Column() int          { return 0 }
func (c nanBoundsChunk) Pages() parquet.Pages { return nil }
func (c nanBoundsChunk) ColumnIndex() (parquet.ColumnIndex, error) {
	return c.index, nil
}
func (c nanBoundsChunk) OffsetIndex() (parquet.OffsetIndex, error) {
	return nil, parquet.ErrMissingOffsetIndex
}
func (c nanBoundsChunk) BloomFilter() parquet.BloomFilter { return nil }
func (c nanBoundsChunk) NumValues() int64                 { return int64(len(c.rows)) }

type nanBoundsRows struct {
	schema *parquet.Schema
	rows   []float64
	i      int
}

func (r *nanBoundsRows) Schema() *parquet.Schema { return r.schema }
func (r *nanBoundsRows) Close() error            { return nil }
func (r *nanBoundsRows) SeekToRow(i int64) error {
	if i < 0 || i > int64(len(r.rows)) {
		return parquet.ErrSeekOutOfRange
	}
	r.i = int(i)
	return nil
}
func (r *nanBoundsRows) ReadRows(buf []parquet.Row) (int, error) {
	n := 0
	for n < len(buf) && r.i < len(r.rows) {
		value := parquet.ValueOf(r.rows[r.i]).Level(0, 0, 0)
		if cap(buf[n]) > 0 {
			buf[n] = buf[n][:1]
			buf[n][0] = value
		} else {
			buf[n] = parquet.Row{value}
		}
		r.i++
		n++
	}
	if r.i >= len(r.rows) {
		return n, io.EOF
	}
	return n, nil
}
