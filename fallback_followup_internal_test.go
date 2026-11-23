package parquet

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"testing"

	"github.com/google/uuid"
	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"
)

func TestFixedLenByteArrayMissingTypeLengthOnFallback(t *testing.T) {
	type row struct {
		ID    int32
		Stamp [12]byte
	}
	var data bytes.Buffer
	in := []row{{ID: 10, Stamp: [12]byte{255}}, {ID: 20, Stamp: [12]byte{0, 1}}}
	if err := Write(&data, in); err != nil {
		t.Fatal(err)
	}
	patched := rewriteFileMetadata(t, data.Bytes(), func(meta *format.FileMetaData) {
		meta.Schema[2].LogicalType = *Timestamp(Millisecond).Type().LogicalType()
		meta.Schema[2].TypeLength.Reset()
	})
	_, err := OpenFile(bytes.NewReader(patched), int64(len(patched)))
	if !errors.Is(err, ErrCorrupted) {
		t.Fatalf("OpenFile = %v, want an error wrapping ErrCorrupted", err)
	}
}

func TestUUIDMissingTypeLengthKeepsFixedWidth(t *testing.T) {
	id := uuid.MustParse("00112233-4455-6677-8899-aabbccddeeff")
	type row struct {
		ID uuid.UUID
	}
	var data bytes.Buffer
	if err := Write(&data, []row{{ID: id}}); err != nil {
		t.Fatal(err)
	}
	patched := rewriteFileMetadata(t, data.Bytes(), func(meta *format.FileMetaData) {
		meta.Schema[1].TypeLength.Reset()
	})
	f, err := OpenFile(bytes.NewReader(patched), int64(len(patched)))
	if err != nil {
		t.Fatal(err)
	}
	typ := f.RowGroups()[0].ColumnChunks()[0].Type()
	if typ.Kind() != FixedLenByteArray || typ.Length() != 16 {
		t.Fatalf("type = %v len %d, want FIXED_LEN_BYTE_ARRAY(16)", typ, typ.Length())
	}
	if _, ok := typ.LogicalType().Value.(*format.UUIDType); !ok {
		t.Fatalf("logical type = %v, want UUID", typ.LogicalType())
	}
	rows := readFileRows(t, f)
	if len(rows) != 1 || !bytes.Equal(rows[0][0].ByteArray(), id[:]) {
		t.Fatalf("rows = %v, want uuid bytes %x", rows, id[:])
	}
	ci, err := f.RowGroups()[0].ColumnChunks()[0].ColumnIndex()
	if err != nil {
		t.Fatalf("ColumnIndex: %v", err)
	}
	if got := ci.MinValue(0).ByteArray(); !bytes.Equal(got, id[:]) {
		t.Fatalf("column index min = %x, want %x", got, id[:])
	}
}

func TestFloat16IsNotPhysicalFallback(t *testing.T) {
	neg2 := [2]byte{}
	one := [2]byte{}
	binary.LittleEndian.PutUint16(neg2[:], 0xC000)
	binary.LittleEndian.PutUint16(one[:], 0x3C00)
	type row struct {
		V [2]byte
	}
	var data bytes.Buffer
	if err := Write(&data, []row{{neg2}, {one}}); err != nil {
		t.Fatal(err)
	}
	patched := rewriteFileMetadata(t, data.Bytes(), func(meta *format.FileMetaData) {
		meta.Schema[1].LogicalType = format.LogicalType{Value: new(format.Float16Type)}
	})
	f, err := OpenFile(bytes.NewReader(patched), int64(len(patched)))
	if err != nil {
		t.Fatal(err)
	}
	typ := f.RowGroups()[0].ColumnChunks()[0].Type()
	lt := typ.LogicalType()
	if lt == nil {
		t.Fatal("logical type is nil, FLOAT16 was routed through the physical fallback")
	}
	if _, ok := lt.Value.(*format.Float16Type); !ok {
		t.Fatalf("logical type = %v, want FLOAT16", lt)
	}
	if typ.Kind() != FixedLenByteArray || typ.Length() != 2 {
		t.Fatalf("type = %v len %d, want FIXED_LEN_BYTE_ARRAY(2)", typ, typ.Length())
	}
	rows := readFileRows(t, f)
	if len(rows) != 2 || !bytes.Equal(rows[0][0].ByteArray(), neg2[:]) || !bytes.Equal(rows[1][0].ByteArray(), one[:]) {
		t.Fatalf("rows = %#v", rows)
	}
	min, max, ok := f.RowGroups()[0].ColumnChunks()[0].(*FileColumnChunk).Bounds()
	if !ok {
		t.Fatal("Bounds ok = false, want float16 bounds")
	}
	// The file was written as plain FIXED_LEN_BYTE_ARRAY, so its stored
	// statistics follow unsigned byte order. Recognition must still surface
	// those endpoints instead of dropping them.
	if !bytes.Equal(min.ByteArray(), one[:]) || !bytes.Equal(max.ByteArray(), neg2[:]) {
		t.Fatalf("Bounds = %x %x, want stored endpoints %x %x", min.ByteArray(), max.ByteArray(), one[:], neg2[:])
	}
	ci, err := f.RowGroups()[0].ColumnChunks()[0].ColumnIndex()
	if err != nil {
		t.Fatalf("ColumnIndex: %v", err)
	}
	if !bytes.Equal(ci.MinValue(0).ByteArray(), one[:]) || !bytes.Equal(ci.MaxValue(0).ByteArray(), neg2[:]) {
		t.Fatalf("column index = %x %x", ci.MinValue(0).ByteArray(), ci.MaxValue(0).ByteArray())
	}
	oi, err := f.RowGroups()[0].ColumnChunks()[0].OffsetIndex()
	if err != nil || oi.NumPages() != 1 {
		t.Fatalf("OffsetIndex = (%v, %v), want 1 page", oi, err)
	}
}

func TestFloat16WriteReadBounds(t *testing.T) {
	neg2 := float16Bytes(0xC000)
	one := float16Bytes(0x3C00)
	nan := float16Bytes(0x7E00)
	schema := NewSchema("row", Group{"v": Leaf(float16Type{})})

	t.Run("numeric order", func(t *testing.T) {
		f := writeFloat16File(t, schema, [][]byte{one, neg2})
		assertFloat16(t, f, [][]byte{one, neg2}, neg2, one, true)
		rewritten := rewriteFloat16(t, f)
		assertFloat16(t, rewritten, [][]byte{one, neg2}, neg2, one, true)
		if got := rewritten.Schema().Fields()[0].Type().LogicalType(); got == nil || got.String() != "FLOAT16" {
			t.Fatalf("rewritten logical type = %v, want FLOAT16", got)
		}
	})

	t.Run("nan excluded", func(t *testing.T) {
		f := writeFloat16File(t, schema, [][]byte{nan, neg2})
		assertFloat16(t, f, [][]byte{nan, neg2}, neg2, neg2, true)
	})

	t.Run("all nan", func(t *testing.T) {
		f := writeFloat16File(t, schema, [][]byte{nan, nan})
		_, _, ok := f.RowGroups()[0].ColumnChunks()[0].(*FileColumnChunk).Bounds()
		if ok {
			t.Fatal("Bounds ok = true, want false for an all-NaN column")
		}
	})

	t.Run("missing type_length", func(t *testing.T) {
		raw := writeFloat16Bytes(t, schema, [][]byte{neg2, one})
		patched := rewriteFileMetadata(t, raw, func(meta *format.FileMetaData) {
			meta.Schema[1].TypeLength.Reset()
		})
		f, err := OpenFile(bytes.NewReader(patched), int64(len(patched)))
		if err != nil {
			t.Fatal(err)
		}
		assertFloat16(t, f, [][]byte{neg2, one}, neg2, one, true)
	})
}

func TestFloat16Bits(t *testing.T) {
	cases := []struct {
		bits uint16
		want float32
	}{
		{0x0000, 0},
		{0x8000, float32(math.Copysign(0, -1))},
		{0x3C00, 1},
		{0xC000, -2},
		{0x7BFF, 65504},
		{0x0400, float32(math.Ldexp(1, -14))},
		{0x0001, float32(math.Ldexp(1, -24))},
	}
	for _, tc := range cases {
		var b [2]byte
		binary.LittleEndian.PutUint16(b[:], tc.bits)
		got := float16ToFloat32(b[:])
		if got != tc.want || math.Signbit(float64(got)) != math.Signbit(float64(tc.want)) {
			t.Errorf("0x%04X = %g (sign %v), want %g (sign %v)", tc.bits, got, math.Signbit(float64(got)), tc.want, math.Signbit(float64(tc.want)))
		}
	}
	if !math.IsInf(float64(float16ToFloat32(float16Bytes(0x7C00))), +1) {
		t.Error("+Inf")
	}
	if !math.IsInf(float64(float16ToFloat32(float16Bytes(0xFC00))), -1) {
		t.Error("-Inf")
	}
	if !float16IsNaN(float16Bytes(0x7E00)) {
		t.Error("NaN")
	}
}

func TestFloat16NaNBoundsAreVisible(t *testing.T) {
	nan := float16Bytes(0x7E00)
	ten := float16Bytes(0x4900)    // 10
	eleven := float16Bytes(0x4980) // 11
	twelve := float16Bytes(0x4A00) // 12
	hi := float16Bytes(0x4B40)     // 14.5

	page := float16Type{}.NewPage(0, 1, float16Type{}.NewValues(nan, nil)).(float16Page)
	bufferIndex := float16BufferColumnIndex{page: page}
	if !columnIndexHasNaNBounds(bufferIndex) {
		t.Fatal("in-memory FLOAT16 column index did not report NaN bounds")
	}

	formatIndex := &format.ColumnIndex{
		NullPages:     []bool{false, false, false},
		NullCounts:    []int64{0, 0, 0},
		MinValues:     [][]byte{ten, twelve, nan},
		MaxValues:     [][]byte{eleven, hi, nan},
		BoundaryOrder: format.Ascending,
	}
	wrapped := NewColumnIndex(FixedLenByteArray, formatIndex)
	if columnIndexHasNaNBounds(wrapped) {
		t.Fatal("NewColumnIndex reported NaN bounds from the physical kind alone")
	}
	// 0 is below every finite page. Binary search prunes the all-NaN page.
	// Search is given the FLOAT16 type, so the page must still be found.
	if got := Search(wrapped, FixedLenByteArrayValue(float16Bytes(0)), float16Type{}); got != 2 {
		t.Fatalf("Search(0) = %d, want 2", got)
	}
	// NullCounts is absent, so a null may sit on the page. The FLOAT16 search
	// wrapper has to preserve that fact.
	optional := NewColumnIndex(FixedLenByteArray, &format.ColumnIndex{
		NullPages:     []bool{false},
		MinValues:     [][]byte{ten},
		MaxValues:     [][]byte{eleven},
		BoundaryOrder: format.Ascending,
	})
	if got := Search(optional, Value{}, float16Type{}); got != 0 {
		t.Fatalf("null Search = %d, want 0", got)
	}

	schema := NewSchema("s", Group{"v": Leaf(float16Type{})})
	sorted := mergeFloat16Groups(t, schema, []float16Group{
		{rows: [][]byte{float16Bytes(0x3C00), float16Bytes(0x4000), float16Bytes(0x4200), float16Bytes(0x4400), float16Bytes(0x5600)}, pages: [][2][]byte{{float16Bytes(0x3C00), float16Bytes(0x4000)}, {nan, nan}, {float16Bytes(0x4200), float16Bytes(0x4400)}}},
		{rows: [][]byte{float16Bytes(0x5100), float16Bytes(0x5460)}, pages: [][2][]byte{{float16Bytes(0x5100), float16Bytes(0x5460)}}},
	})
	wantBits := []uint16{0x3C00, 0x4000, 0x4200, 0x4400, 0x5100, 0x5460, 0x5600}
	if len(sorted) != len(wantBits) {
		t.Fatalf("merged %d rows, want %d: %x", len(sorted), len(wantBits), sorted)
	}
	for i, bits := range wantBits {
		if got := binary.LittleEndian.Uint16(sorted[i]); got != bits {
			t.Fatalf("merged row %d = 0x%04X, want 0x%04X", i, got, bits)
		}
	}
}

type float16Group struct {
	rows  [][]byte
	pages [][2][]byte
}

func mergeFloat16Groups(t *testing.T, schema *Schema, groups []float16Group) [][]byte {
	t.Helper()
	rowGroups := make([]RowGroup, len(groups))
	for i, group := range groups {
		mins := make([][]byte, len(group.pages))
		maxs := make([][]byte, len(group.pages))
		nulls := make([]bool, len(group.pages))
		counts := make([]int64, len(group.pages))
		for j, page := range group.pages {
			mins[j] = page[0]
			maxs[j] = page[1]
		}
		rowGroups[i] = &float16NaNRowGroup{
			schema: schema,
			rows:   group.rows,
			typ:    float16Type{},
			index: NewColumnIndex(FixedLenByteArray, &format.ColumnIndex{
				NullPages:     nulls,
				NullCounts:    counts,
				MinValues:     mins,
				MaxValues:     maxs,
				BoundaryOrder: format.Ascending,
			}),
		}
	}
	merged, err := MergeRowGroups(rowGroups, SortingRowGroupConfig(SortingColumns(Ascending("v"))))
	if err != nil {
		t.Fatal(err)
	}
	rows := merged.Rows()
	defer rows.Close()
	buf := make([]Row, 8)
	var got [][]byte
	for {
		n, err := rows.ReadRows(buf)
		for _, row := range buf[:n] {
			got = append(got, append([]byte(nil), row[0].ByteArray()...))
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

type float16NaNRowGroup struct {
	schema *Schema
	rows   [][]byte
	typ    Type
	index  ColumnIndex
}

func (g *float16NaNRowGroup) NumRows() int64 { return int64(len(g.rows)) }
func (g *float16NaNRowGroup) ColumnChunks() []ColumnChunk {
	return []ColumnChunk{float16NaNChunk{typ: g.typ, rows: g.rows, index: g.index}}
}
func (g *float16NaNRowGroup) Schema() *Schema { return g.schema }
func (g *float16NaNRowGroup) SortingColumns() []SortingColumn {
	return []SortingColumn{Ascending("v")}
}
func (g *float16NaNRowGroup) Rows() Rows {
	return &float16NaNRows{schema: g.schema, rows: g.rows}
}

type float16NaNChunk struct {
	typ   Type
	rows  [][]byte
	index ColumnIndex
}

func (c float16NaNChunk) Type() Type   { return c.typ }
func (c float16NaNChunk) Column() int  { return 0 }
func (c float16NaNChunk) Pages() Pages { return nil }
func (c float16NaNChunk) ColumnIndex() (ColumnIndex, error) {
	return c.index, nil
}
func (c float16NaNChunk) OffsetIndex() (OffsetIndex, error) { return nil, ErrMissingOffsetIndex }
func (c float16NaNChunk) BloomFilter() BloomFilter          { return nil }
func (c float16NaNChunk) NumValues() int64                  { return int64(len(c.rows)) }

type float16NaNRows struct {
	schema *Schema
	rows   [][]byte
	i      int
}

func (r *float16NaNRows) Schema() *Schema { return r.schema }
func (r *float16NaNRows) Close() error    { return nil }
func (r *float16NaNRows) SeekToRow(i int64) error {
	if i < 0 || i > int64(len(r.rows)) {
		return ErrSeekOutOfRange
	}
	r.i = int(i)
	return nil
}
func (r *float16NaNRows) ReadRows(buf []Row) (int, error) {
	n := 0
	for n < len(buf) && r.i < len(r.rows) {
		value := FixedLenByteArrayValue(r.rows[r.i]).Level(0, 0, 0)
		if cap(buf[n]) > 0 {
			buf[n] = buf[n][:1]
			buf[n][0] = value
		} else {
			buf[n] = Row{value}
		}
		r.i++
		n++
	}
	if r.i >= len(r.rows) {
		return n, io.EOF
	}
	return n, nil
}

func TestFixedLenByteArrayNaNBitPatternKeepsBounds(t *testing.T) {
	// 0x7e00 is a binary16 NaN and also a legal 2-byte fixed value. A value
	// between the low page and those bytes sits in neither page.
	nanBits := []byte{0x00, 0x7e}
	lo := []byte{0x00, 0x01}
	hi := []byte{0x00, 0x02}
	needle := []byte{0x00, 0x10}
	for _, tc := range []struct {
		name string
		node Node
	}{
		{"plain", Leaf(FixedLenByteArrayType(2))},
		{"decimal", Decimal(0, 4, FixedLenByteArrayType(2))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			index := NewColumnIndex(FixedLenByteArray, &format.ColumnIndex{
				NullPages:     []bool{false, false},
				NullCounts:    []int64{0, 0},
				MinValues:     [][]byte{lo, nanBits},
				MaxValues:     [][]byte{hi, nanBits},
				BoundaryOrder: format.Ascending,
			})
			if columnIndexHasNaNBounds(index) {
				t.Fatal("column index reported NaN bounds")
			}
			if !bytes.Equal(index.MinValue(1).ByteArray(), nanBits) || !bytes.Equal(index.MaxValue(1).ByteArray(), nanBits) {
				t.Fatalf("column index endpoint = %x/%x", index.MinValue(1).ByteArray(), index.MaxValue(1).ByteArray())
			}
			if got := Search(index, FixedLenByteArrayValue(needle), tc.node.Type()); got != 2 {
				t.Fatalf("Search = %d, want 2", got)
			}

			schema := NewSchema("s", Group{"v": tc.node})
			fileIndex := writtenFileColumnIndex(t, schema, nanBits)
			if columnIndexHasNaNBounds(fileIndex) {
				t.Fatal("file column index reported NaN bounds")
			}
			if !bytes.Equal(fileIndex.MinValue(0).ByteArray(), nanBits) || !bytes.Equal(fileIndex.MaxValue(0).ByteArray(), nanBits) {
				t.Fatalf("file endpoint = %x/%x", fileIndex.MinValue(0).ByteArray(), fileIndex.MaxValue(0).ByteArray())
			}

			// The high value sorts after the low one and the ranges do not
			// overlap, so the merge keeps two segments. Treating the high
			// bytes as NaN drops the bounds and collapses the merge.
			merged := mergeFixedGroups(t, schema, tc.node.Type(), []float16Group{
				{rows: [][]byte{nanBits}, pages: [][2][]byte{{nanBits, nanBits}}},
				{rows: [][]byte{lo}, pages: [][2][]byte{{lo, lo}}},
			})
			segs, ok := merged.(orderedRowGroupSegments)
			if !ok {
				t.Fatal("merged row group has no segments")
			}
			if n := len(segs.rowGroupSegments()); n != 2 {
				t.Fatalf("segments = %d, want 2", n)
			}
		})
	}
}

func writtenFileColumnIndex(t *testing.T, schema *Schema, value []byte) ColumnIndex {
	t.Helper()
	raw := writeFloat16Bytes(t, schema, [][]byte{value})
	f, err := OpenFile(bytes.NewReader(raw), int64(len(raw)))
	if err != nil {
		t.Fatal(err)
	}
	index, err := f.RowGroups()[0].ColumnChunks()[0].ColumnIndex()
	if err != nil {
		t.Fatal(err)
	}
	return index
}

func mergeFixedGroups(t *testing.T, schema *Schema, typ Type, groups []float16Group) RowGroup {
	t.Helper()
	rowGroups := make([]RowGroup, len(groups))
	for i, group := range groups {
		mins := make([][]byte, len(group.pages))
		maxs := make([][]byte, len(group.pages))
		nulls := make([]bool, len(group.pages))
		counts := make([]int64, len(group.pages))
		for j, page := range group.pages {
			mins[j] = page[0]
			maxs[j] = page[1]
		}
		rowGroups[i] = &float16NaNRowGroup{
			schema: schema,
			rows:   group.rows,
			typ:    typ,
			index: NewColumnIndex(FixedLenByteArray, &format.ColumnIndex{
				NullPages:     nulls,
				NullCounts:    counts,
				MinValues:     mins,
				MaxValues:     maxs,
				BoundaryOrder: format.Ascending,
			}),
		}
	}
	merged, err := MergeRowGroups(rowGroups, SortingRowGroupConfig(SortingColumns(Ascending("v"))))
	if err != nil {
		t.Fatal(err)
	}
	return merged
}

func TestDecimalFixedLenByteArrayMissingTypeLength(t *testing.T) {
	elem := format.SchemaElement{
		Name:        "d",
		Type:        thrift.New(format.FixedLenByteArray),
		LogicalType: format.LogicalType{Value: &format.DecimalType{Scale: 2, Precision: 5}},
	}
	_, _, err := schemaElementTypeOf(&elem)
	if !errors.Is(err, ErrCorrupted) {
		t.Fatalf("schemaElementTypeOf = %v, want an error wrapping ErrCorrupted", err)
	}
}

func writeFloat16Bytes(t *testing.T, schema *Schema, values [][]byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := NewWriter(&buf, schema)
	rows := make([]Row, len(values))
	for i, v := range values {
		rows[i] = Row{FixedLenByteArrayValue(v).Level(0, 0, 0)}
	}
	if _, err := w.WriteRows(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func writeFloat16File(t *testing.T, schema *Schema, values [][]byte) *File {
	t.Helper()
	raw := writeFloat16Bytes(t, schema, values)
	f, err := OpenFile(bytes.NewReader(raw), int64(len(raw)))
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func rewriteFloat16(t *testing.T, f *File) *File {
	t.Helper()
	var buf bytes.Buffer
	w := NewWriter(&buf, f.Schema())
	if _, err := w.WriteRowGroup(f.RowGroups()[0]); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	out, err := OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func assertFloat16(t *testing.T, f *File, wantRows [][]byte, wantMin, wantMax []byte, wantOK bool) {
	t.Helper()
	rows := readFileRows(t, f)
	if len(rows) != len(wantRows) {
		t.Fatalf("got %d rows, want %d", len(rows), len(wantRows))
	}
	for i, want := range wantRows {
		if !bytes.Equal(rows[i][0].ByteArray(), want) {
			t.Fatalf("row %d = %x, want %x", i, rows[i][0].ByteArray(), want)
		}
	}
	chunk := f.RowGroups()[0].ColumnChunks()[0]
	min, max, ok := chunk.(*FileColumnChunk).Bounds()
	if ok != wantOK || !bytes.Equal(min.ByteArray(), wantMin) || !bytes.Equal(max.ByteArray(), wantMax) {
		t.Fatalf("Bounds = (%x, %x, %v), want (%x, %x, %v)", min.ByteArray(), max.ByteArray(), ok, wantMin, wantMax, wantOK)
	}
	ci, err := chunk.ColumnIndex()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(ci.MinValue(0).ByteArray(), wantMin) || !bytes.Equal(ci.MaxValue(0).ByteArray(), wantMax) {
		t.Fatalf("column index = %x %x, want %x %x", ci.MinValue(0).ByteArray(), ci.MaxValue(0).ByteArray(), wantMin, wantMax)
	}
	lt := chunk.Type().LogicalType()
	if lt == nil || lt.String() != "FLOAT16" {
		t.Fatalf("logical type = %v, want FLOAT16", lt)
	}
}

func float16Bytes(bits uint16) []byte {
	b := make([]byte, 2)
	binary.LittleEndian.PutUint16(b, bits)
	return b
}

func rewriteFileMetadata(t *testing.T, original []byte, mutate func(*format.FileMetaData)) []byte {
	t.Helper()
	start := len(original) - 8 - int(binary.LittleEndian.Uint32(original[len(original)-8:]))
	var meta format.FileMetaData
	var proto thrift.CompactProtocol
	if err := thrift.Unmarshal(&proto, original[start:len(original)-8], &meta); err != nil {
		t.Fatal(err)
	}
	mutate(&meta)
	footer, err := thrift.Marshal(&proto, &meta)
	if err != nil {
		t.Fatal(err)
	}
	b := append(bytes.Clone(original[:start]), footer...)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(footer)))
	return append(b, "PAR1"...)
}

func readFileRows(t *testing.T, f *File) []Row {
	t.Helper()
	rows := f.RowGroups()[0].Rows()
	defer rows.Close()
	buf := make([]Row, 8)
	var all []Row
	for {
		n, err := rows.ReadRows(buf)
		for _, row := range buf[:n] {
			all = append(all, row.Clone())
		}
		if errors.Is(err, io.EOF) {
			return all
		}
		if err != nil {
			t.Fatal(err)
		}
	}
}
