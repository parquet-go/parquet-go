package parquet_test

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"testing"

	"github.com/parquet-go/parquet-go"
)

var errSiblingRead = errors.New("sibling column was read")

// accessChunk counts Pages calls and optionally fails them. Pages is the point
// at which a column chunk is opened, including over the network.
type accessChunk struct {
	parquet.ColumnChunk
	reads int
	fail  bool
}

func (c *accessChunk) Pages() parquet.Pages {
	c.reads++
	if c.fail {
		return failingPages{}
	}
	return c.ColumnChunk.Pages()
}

type failingPages struct{}

func (failingPages) ReadPage() (parquet.Page, error) { return nil, errSiblingRead }
func (failingPages) SeekToRow(int64) error           { return errSiblingRead }
func (failingPages) Close() error                    { return nil }

type accessRowGroup struct {
	parquet.RowGroup
	columns []parquet.ColumnChunk
}

func (g accessRowGroup) ColumnChunks() []parquet.ColumnChunk { return g.columns }

func wrapAccess(rg parquet.RowGroup, fail []bool) ([]*accessChunk, parquet.RowGroup) {
	orig := rg.ColumnChunks()
	chunks := make([]*accessChunk, len(orig))
	cols := make([]parquet.ColumnChunk, len(orig))
	for i, col := range orig {
		chunks[i] = &accessChunk{ColumnChunk: col, fail: len(fail) > i && fail[i]}
		cols[i] = chunks[i]
	}
	return chunks, accessRowGroup{RowGroup: rg, columns: cols}
}

func cloneRow(row parquet.Row) parquet.Row {
	out := make(parquet.Row, len(row))
	copy(out, row)
	return out
}

func readConvertedRows(t *testing.T, rows parquet.Rows) []parquet.Row {
	t.Helper()
	defer rows.Close()
	var out []parquet.Row
	buf := make([]parquet.Row, 8)
	for {
		n, err := rows.ReadRows(buf)
		for i := range n {
			out = append(out, cloneRow(buf[i]))
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			t.Fatal("ReadRows returned no rows and no error")
		}
	}
	return out
}

func readColumnValues(t *testing.T, chunk parquet.ColumnChunk) []parquet.Value {
	t.Helper()
	pages := chunk.Pages()
	t.Cleanup(func() { pages.Close() })
	var out []parquet.Value
	buf := make([]parquet.Value, 16)
	for {
		page, err := pages.ReadPage()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		reader := page.Values()
		for {
			n, err := reader.ReadValues(buf)
			for i := range n {
				out = append(out, buf[i])
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			if n == 0 {
				t.Fatal("ReadValues returned no values and no error")
			}
		}
	}
	return out
}

func columnValues(row parquet.Row, column int) []parquet.Value {
	var out []parquet.Value
	for _, v := range row {
		if v.Column() == column {
			out = append(out, v)
		}
	}
	return out
}

func levelsEqual(t *testing.T, got, want []parquet.Value) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d values, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].RepetitionLevel() != want[i].RepetitionLevel() ||
			got[i].DefinitionLevel() != want[i].DefinitionLevel() ||
			got[i].IsNull() != want[i].IsNull() ||
			got[i].Kind() != want[i].Kind() ||
			got[i].Column() != want[i].Column() {
			t.Errorf("value %d: got rep=%d def=%d null=%v kind=%v col=%d, want rep=%d def=%d null=%v kind=%v col=%d",
				i,
				got[i].RepetitionLevel(), got[i].DefinitionLevel(), got[i].IsNull(), got[i].Kind(), got[i].Column(),
				want[i].RepetitionLevel(), want[i].DefinitionLevel(), want[i].IsNull(), want[i].Kind(), want[i].Column())
		}
	}
}

func TestMissingFlatColumnDoesNotReadSibling(t *testing.T) {
	type Source struct {
		Alpha int64 `parquet:"alpha"`
		Kept  int64 `parquet:"kept"`
	}
	src := parquet.NewGenericBuffer[Source]()
	if _, err := src.Write([]Source{{Alpha: 10, Kept: 1}, {Alpha: 20, Kept: 2}}); err != nil {
		t.Fatal(err)
	}

	t.Run("required", func(t *testing.T) {
		type Target struct {
			Kept  int64  `parquet:"kept"`
			Extra string `parquet:"extra"`
		}
		chunks, rg := wrapAccess(src, []bool{true, false})
		conv, err := parquet.Convert(parquet.SchemaOf(new(Target)), src.Schema())
		if err != nil {
			t.Fatal(err)
		}
		converted := parquet.ConvertRowGroup(rg, conv)
		rows := readConvertedRows(t, converted.Rows())
		if chunks[0].reads != 0 {
			t.Fatalf("sibling column opened %d times while reading rows", chunks[0].reads)
		}
		if len(rows) != 2 {
			t.Fatalf("got %d rows, want 2", len(rows))
		}
		for i, row := range rows {
			if got := columnValues(row, 0); len(got) != 1 || got[0].Int64() != int64(i+1) {
				t.Errorf("row %d kept: got %v", i, got)
			}
			extra := columnValues(row, 1)
			if len(extra) != 1 || extra[0].RepetitionLevel() != 0 || extra[0].DefinitionLevel() != 0 || extra[0].IsNull() || extra[0].String() != "" {
				t.Errorf("row %d extra: rep=%d def=%d null=%v %q", i, extra[0].RepetitionLevel(), extra[0].DefinitionLevel(), extra[0].IsNull(), extra[0].String())
			}
		}
		before := chunks[0].reads
		got := readColumnValues(t, converted.ColumnChunks()[1])
		if chunks[0].reads != before {
			t.Fatalf("reading the missing column opened the sibling %d times", chunks[0].reads-before)
		}
		if len(got) != 2 {
			t.Fatalf("chunk values: got %d, want 2", len(got))
		}
		for i, v := range got {
			if v.RepetitionLevel() != 0 || v.DefinitionLevel() != 0 || v.IsNull() || v.String() != "" || v.Column() != 1 {
				t.Errorf("chunk %d: rep=%d def=%d null=%v col=%d %q", i, v.RepetitionLevel(), v.DefinitionLevel(), v.IsNull(), v.Column(), v.String())
			}
		}
	})

	t.Run("optional", func(t *testing.T) {
		type Target struct {
			Kept  int64   `parquet:"kept"`
			Extra *string `parquet:"extra,optional"`
		}
		chunks, rg := wrapAccess(src, []bool{true, false})
		conv, err := parquet.Convert(parquet.SchemaOf(new(Target)), src.Schema())
		if err != nil {
			t.Fatal(err)
		}
		converted := parquet.ConvertRowGroup(rg, conv)
		rows := readConvertedRows(t, converted.Rows())
		if chunks[0].reads != 0 {
			t.Fatalf("sibling column opened %d times while reading rows", chunks[0].reads)
		}
		for i, row := range rows {
			extra := columnValues(row, 1)
			if len(extra) != 1 || extra[0].RepetitionLevel() != 0 || extra[0].DefinitionLevel() != 0 || !extra[0].IsNull() {
				t.Errorf("row %d extra: %+v", i, extra)
			}
			var out Target
			if err := parquet.SchemaOf(new(Target)).Reconstruct(&out, row); err != nil {
				t.Fatal(err)
			}
			if out.Kept != int64(i+1) || out.Extra != nil {
				t.Errorf("row %d: got %+v", i, out)
			}
		}
		before := chunks[0].reads
		got := readColumnValues(t, converted.ColumnChunks()[1])
		if chunks[0].reads != before {
			t.Fatalf("reading the missing column opened the sibling %d times", chunks[0].reads-before)
		}
		for i, v := range got {
			if v.RepetitionLevel() != 0 || v.DefinitionLevel() != 0 || !v.IsNull() {
				t.Errorf("chunk %d: rep=%d def=%d null=%v", i, v.RepetitionLevel(), v.DefinitionLevel(), v.IsNull())
			}
		}
	})
}

func TestMissingColumnUnderRequiredGroupDoesNotReadSibling(t *testing.T) {
	type Inner struct {
		Alpha int64 `parquet:"alpha"`
		Kept  int64 `parquet:"kept"`
	}
	type Source struct {
		G Inner `parquet:"g"`
	}
	src := parquet.NewGenericBuffer[Source]()
	if _, err := src.Write([]Source{{G: Inner{Alpha: 10, Kept: 1}}, {G: Inner{Alpha: 20, Kept: 2}}}); err != nil {
		t.Fatal(err)
	}
	type InnerTarget struct {
		Kept  int64 `parquet:"kept"`
		Extra int64 `parquet:"extra"`
	}
	type Target struct {
		G InnerTarget `parquet:"g"`
	}
	chunks, rg := wrapAccess(src, []bool{true, false})
	conv, err := parquet.Convert(parquet.SchemaOf(new(Target)), src.Schema())
	if err != nil {
		t.Fatal(err)
	}
	converted := parquet.ConvertRowGroup(rg, conv)
	rows := readConvertedRows(t, converted.Rows())
	if chunks[0].reads != 0 {
		t.Fatalf("sibling column opened %d times while reading rows", chunks[0].reads)
	}
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(rows))
	}
	for i, row := range rows {
		extra := columnValues(row, 1)
		if len(extra) != 1 || extra[0].RepetitionLevel() != 0 || extra[0].DefinitionLevel() != 0 || extra[0].IsNull() || extra[0].Int64() != 0 {
			t.Errorf("row %d extra: rep=%d def=%d null=%v %d", i, extra[0].RepetitionLevel(), extra[0].DefinitionLevel(), extra[0].IsNull(), extra[0].Int64())
		}
		var out Target
		if err := parquet.SchemaOf(new(Target)).Reconstruct(&out, row); err != nil {
			t.Fatal(err)
		}
		want := Target{G: InnerTarget{Kept: int64(i + 1), Extra: 0}}
		if !reflect.DeepEqual(out, want) {
			t.Errorf("row %d: got %+v, want %+v", i, out, want)
		}
	}
	before := chunks[0].reads
	got := readColumnValues(t, converted.ColumnChunks()[1])
	if chunks[0].reads != before {
		t.Fatalf("reading the missing column opened the sibling %d times", chunks[0].reads-before)
	}
	for i, v := range got {
		if v.RepetitionLevel() != 0 || v.DefinitionLevel() != 0 || v.IsNull() || v.Int64() != 0 || v.Column() != 1 {
			t.Errorf("chunk %d: rep=%d def=%d null=%v col=%d %d", i, v.RepetitionLevel(), v.DefinitionLevel(), v.IsNull(), v.Column(), v.Int64())
		}
	}
}

func TestMissingColumnUnderAbsentOptionalGroupDoesNotReadSibling(t *testing.T) {
	type Source struct {
		Alpha int64 `parquet:"alpha"`
		Kept  int64 `parquet:"kept"`
	}
	src := parquet.NewGenericBuffer[Source]()
	if _, err := src.Write([]Source{{Alpha: 10, Kept: 1}, {Alpha: 20, Kept: 2}}); err != nil {
		t.Fatal(err)
	}
	type Inner struct {
		B int64 `parquet:"b"`
	}
	type Target struct {
		Kept int64  `parquet:"kept"`
		G    *Inner `parquet:"g,optional"`
	}
	chunks, rg := wrapAccess(src, []bool{true, false})
	conv, err := parquet.Convert(parquet.SchemaOf(new(Target)), src.Schema())
	if err != nil {
		t.Fatal(err)
	}
	converted := parquet.ConvertRowGroup(rg, conv)
	rows := readConvertedRows(t, converted.Rows())
	if chunks[0].reads != 0 {
		t.Fatalf("sibling column opened %d times while reading rows", chunks[0].reads)
	}
	schema := parquet.SchemaOf(new(Target))
	for i, row := range rows {
		// The group is missing, so the leaf is null at definition level 0.
		b := columnValues(row, 1)
		if len(b) != 1 || b[0].RepetitionLevel() != 0 || b[0].DefinitionLevel() != 0 {
			t.Fatalf("row %d: got %+v", i, b)
		}
		var out Target
		if err := schema.Reconstruct(&out, row); err != nil {
			t.Fatal(err)
		}
		if out.Kept != int64(i+1) || out.G != nil {
			t.Errorf("row %d: got %+v", i, out)
		}
	}
	before := chunks[0].reads
	got := readColumnValues(t, converted.ColumnChunks()[1])
	if chunks[0].reads != before {
		t.Fatalf("reading the missing column opened the sibling %d times", chunks[0].reads-before)
	}
	for i, v := range got {
		if v.RepetitionLevel() != 0 || v.DefinitionLevel() != 0 || v.Column() != 1 {
			t.Errorf("chunk %d: rep=%d def=%d col=%d", i, v.RepetitionLevel(), v.DefinitionLevel(), v.Column())
		}
	}
}

func TestMissingColumnUnderOptionalGroupReadsSibling(t *testing.T) {
	type Inner struct {
		A int64 `parquet:"a"`
	}
	type Source struct {
		G *Inner `parquet:"g,optional"`
	}
	first := Inner{A: 3}
	third := Inner{A: 4}
	src := parquet.NewGenericBuffer[Source]()
	if _, err := src.Write([]Source{{G: &first}, {G: nil}, {G: &third}}); err != nil {
		t.Fatal(err)
	}

	t.Run("required leaf", func(t *testing.T) {
		type InnerTarget struct {
			A int64 `parquet:"a"`
			B int64 `parquet:"b"`
		}
		type Target struct {
			G *InnerTarget `parquet:"g,optional"`
		}
		assertOptionalGroupSibling(t, src, parquet.SchemaOf(new(Target)), []int{1, 0, 1}, false)
	})

	t.Run("optional leaf", func(t *testing.T) {
		type InnerTarget struct {
			A int64  `parquet:"a"`
			B *int64 `parquet:"b,optional"`
		}
		type Target struct {
			G *InnerTarget `parquet:"g,optional"`
		}
		assertOptionalGroupSibling(t, src, parquet.SchemaOf(new(Target)), []int{1, 0, 1}, true)
	})
}

func assertOptionalGroupSibling(t *testing.T, src parquet.RowGroup, target *parquet.Schema, wantDef []int, wantNull bool) {
	t.Helper()
	chunks, rg := wrapAccess(src, nil)
	conv, err := parquet.Convert(target, src.Schema())
	if err != nil {
		t.Fatal(err)
	}
	converted := parquet.ConvertRowGroup(rg, conv)
	before := chunks[0].reads
	chunkValues := readColumnValues(t, converted.ColumnChunks()[1])
	if chunks[0].reads == before {
		t.Fatal("missing column under an optional group did not open the sibling column")
	}
	rows := readConvertedRows(t, converted.Rows())
	if len(rows) != len(wantDef) {
		t.Fatalf("got %d rows, want %d", len(rows), len(wantDef))
	}
	var fromRows []parquet.Value
	for i, row := range rows {
		vals := columnValues(row, 1)
		if len(vals) != 1 {
			t.Fatalf("row %d: got %d values", i, len(vals))
		}
		if vals[0].RepetitionLevel() != 0 || vals[0].DefinitionLevel() != wantDef[i] || vals[0].IsNull() != (wantNull || wantDef[i] == 0) {
			t.Errorf("row %d: rep=%d def=%d null=%v, want rep=0 def=%d null=%v",
				i, vals[0].RepetitionLevel(), vals[0].DefinitionLevel(), vals[0].IsNull(), wantDef[i], wantNull)
		}
		fromRows = append(fromRows, vals...)
	}
	levelsEqual(t, chunkValues, fromRows)

	// A slice starts at the sibling's row, not at the beginning of the column.
	pages := converted.ColumnChunks()[1].Pages()
	defer pages.Close()
	page, err := pages.ReadPage()
	if err != nil {
		t.Fatal(err)
	}
	sliced := page.Slice(1, 3)
	buf := make([]parquet.Value, 4)
	n, err := sliced.Values().ReadValues(buf)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("sliced values: got %d, want 2", n)
	}
	for i, def := range wantDef[1:3] {
		if buf[i].RepetitionLevel() != 0 || buf[i].DefinitionLevel() != def || buf[i].IsNull() != (wantNull || def == 0) {
			t.Errorf("slice %d: rep=%d def=%d null=%v, want rep=0 def=%d null=%v",
				i, buf[i].RepetitionLevel(), buf[i].DefinitionLevel(), buf[i].IsNull(), def, wantNull)
		}
	}
}

func TestMissingColumnUnderOptionalGroupLargeRead(t *testing.T) {
	type Inner struct {
		A int64 `parquet:"a"`
	}
	type Source struct {
		G *Inner `parquet:"g,optional"`
	}
	const n = 1100
	rowsIn := make([]Source, n)
	for i := range rowsIn {
		if i%2 == 0 {
			rowsIn[i].G = &Inner{A: int64(i)}
		}
	}
	src := parquet.NewGenericBuffer[Source]()
	if _, err := src.Write(rowsIn); err != nil {
		t.Fatal(err)
	}
	type InnerTarget struct {
		A int64 `parquet:"a"`
		B int64 `parquet:"b"`
	}
	type Target struct {
		G *InnerTarget `parquet:"g,optional"`
	}
	conv, err := parquet.Convert(parquet.SchemaOf(new(Target)), src.Schema())
	if err != nil {
		t.Fatal(err)
	}
	converted := parquet.ConvertRowGroup(src, conv)
	pages := converted.ColumnChunks()[1].Pages()
	defer pages.Close()
	page, err := pages.ReadPage()
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]parquet.Value, n+50)
	got, err := page.Values().ReadValues(buf)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if got != n {
		t.Fatalf("read %d values, want %d", got, n)
	}
	for i := range n {
		wantDef := 0
		if i%2 == 0 {
			wantDef = 1
		}
		wantNull := wantDef == 0
		if buf[i].RepetitionLevel() != 0 || buf[i].DefinitionLevel() != wantDef || buf[i].IsNull() != wantNull {
			t.Errorf("value %d: rep=%d def=%d null=%v, want rep=0 def=%d null=%v",
				i, buf[i].RepetitionLevel(), buf[i].DefinitionLevel(), buf[i].IsNull(), wantDef, wantNull)
		}
	}
}

type seekFailPages struct {
	closed int
}

func (p *seekFailPages) ReadPage() (parquet.Page, error) { return nil, errSiblingRead }
func (p *seekFailPages) SeekToRow(int64) error           { return errSiblingRead }
func (p *seekFailPages) Close() error {
	p.closed++
	return nil
}

type seekFailChunk struct {
	parquet.ColumnChunk
	opened []*seekFailPages
}

func (c *seekFailChunk) Pages() parquet.Pages {
	p := &seekFailPages{}
	c.opened = append(c.opened, p)
	return p
}

func TestMissingColumnSliceSeekClosesOpenedSibling(t *testing.T) {
	type Inner struct {
		A int64 `parquet:"a"`
	}
	type Source struct {
		G *Inner `parquet:"g,optional"`
	}
	present := Inner{A: 1}
	src := parquet.NewGenericBuffer[Source]()
	if _, err := src.Write([]Source{{G: &present}, {G: nil}, {G: &present}}); err != nil {
		t.Fatal(err)
	}
	type InnerTarget struct {
		A int64 `parquet:"a"`
		B int64 `parquet:"b"`
	}
	type Target struct {
		G *InnerTarget `parquet:"g,optional"`
	}
	orig := src.ColumnChunks()
	sibling := &seekFailChunk{ColumnChunk: orig[0]}
	cols := []parquet.ColumnChunk{sibling}
	conv, err := parquet.Convert(parquet.SchemaOf(new(Target)), src.Schema())
	if err != nil {
		t.Fatal(err)
	}
	converted := parquet.ConvertRowGroup(accessRowGroup{RowGroup: src, columns: cols}, conv)
	// The missing column opens the sibling once. Slice opens a second reader
	// so it does not move the first. A failed seek closes that second reader.
	pages := converted.ColumnChunks()[1].Pages()
	defer pages.Close()
	page, err := pages.ReadPage()
	if err != nil {
		t.Fatal(err)
	}
	sliced := page.Slice(1, 3)
	_, err = sliced.Values().ReadValues(make([]parquet.Value, 2))
	if !errors.Is(err, errSiblingRead) {
		t.Fatalf("slice read err = %v, want %v", err, errSiblingRead)
	}
	if len(sibling.opened) != 2 {
		t.Fatalf("sibling Pages opened %d times, want 2", len(sibling.opened))
	}
	if sibling.opened[0].closed != 0 {
		t.Fatalf("slice closed the original sibling reader %d times", sibling.opened[0].closed)
	}
	if sibling.opened[1].closed != 1 {
		t.Fatalf("slice seek closed the dedicated reader %d times, want 1", sibling.opened[1].closed)
	}
}

func TestMissingColumnSliceDoesNotMoveOriginal(t *testing.T) {
	type Inner struct {
		A int64 `parquet:"a"`
	}
	type Source struct {
		G *Inner `parquet:"g,optional"`
	}
	first := Inner{A: 1}
	third := Inner{A: 3}
	src := parquet.NewGenericBuffer[Source]()
	if _, err := src.Write([]Source{{G: &first}, {G: nil}, {G: &third}}); err != nil {
		t.Fatal(err)
	}
	type InnerTarget struct {
		A int64 `parquet:"a"`
		B int64 `parquet:"b"`
	}
	type Target struct {
		G *InnerTarget `parquet:"g,optional"`
	}
	conv, err := parquet.Convert(parquet.SchemaOf(new(Target)), src.Schema())
	if err != nil {
		t.Fatal(err)
	}
	converted := parquet.ConvertRowGroup(src, conv)
	pages := converted.ColumnChunks()[1].Pages()
	defer pages.Close()
	page, err := pages.ReadPage()
	if err != nil {
		t.Fatal(err)
	}

	readDef := func(p parquet.Page, n int) []int {
		t.Helper()
		buf := make([]parquet.Value, n)
		got, err := p.Values().ReadValues(buf)
		if err != nil && err != io.EOF {
			t.Fatal(err)
		}
		if got != n {
			t.Fatalf("read %d values, want %d", got, n)
		}
		defs := make([]int, n)
		for i := range n {
			defs[i] = buf[i].DefinitionLevel()
		}
		return defs
	}

	if defs := readDef(page.Slice(1, 3), 2); !reflect.DeepEqual(defs, []int{0, 1}) {
		t.Fatalf("first slice defs = %v, want [0 1]", defs)
	}
	// A second slice is relative to the first, so its tail is the last row.
	if defs := readDef(page.Slice(1, 3).Slice(1, 2), 1); !reflect.DeepEqual(defs, []int{1}) {
		t.Fatalf("nested slice defs = %v, want [1]", defs)
	}
	if defs := readDef(page.Slice(0, 1), 1); !reflect.DeepEqual(defs, []int{1}) {
		t.Fatalf("second slice defs = %v, want [1]", defs)
	}
	if defs := readDef(page, 3); !reflect.DeepEqual(defs, []int{1, 0, 1}) {
		t.Fatalf("original page defs = %v, want [1 0 1]", defs)
	}
}

func TestMissingColumnUnderRepeatedGroupReadsSibling(t *testing.T) {
	type Item struct {
		X int32 `parquet:"x"`
	}
	type Source struct {
		Items []Item `parquet:"items"`
	}
	src := parquet.NewGenericBuffer[Source]()
	if _, err := src.Write([]Source{
		{Items: []Item{{X: 1}, {X: 2}}},
		{Items: nil},
	}); err != nil {
		t.Fatal(err)
	}
	type ItemTarget struct {
		X int32 `parquet:"x"`
		Y int32 `parquet:"y"`
	}
	type Target struct {
		Items []ItemTarget `parquet:"items"`
	}
	chunks, rg := wrapAccess(src, nil)
	conv, err := parquet.Convert(parquet.SchemaOf(new(Target)), src.Schema())
	if err != nil {
		t.Fatal(err)
	}
	converted := parquet.ConvertRowGroup(rg, conv)
	before := chunks[0].reads
	_ = readColumnValues(t, converted.ColumnChunks()[1])
	if chunks[0].reads == before {
		t.Fatal("missing column under a repeated group did not open the sibling column")
	}
	rows := readConvertedRows(t, converted.Rows())
	schema := parquet.SchemaOf(new(Target))
	var out Target
	if err := schema.Reconstruct(&out, rows[0]); err != nil {
		t.Fatal(err)
	}
	want0 := Target{Items: []ItemTarget{{X: 1, Y: 0}, {X: 2, Y: 0}}}
	if !reflect.DeepEqual(out, want0) {
		t.Errorf("row 0: got %+v, want %+v", out, want0)
	}
	if err := schema.Reconstruct(&out, rows[1]); err != nil {
		t.Fatal(err)
	}
	if len(out.Items) != 0 {
		t.Errorf("row 1: got %+v, want no items", out.Items)
	}
}

// TestConvertAddedNestedFieldLevels is the regression test for issue 778.
// A field added inside a nested group must follow the enclosing group's
// instances, not a sibling leaf's repetition or definition levels.
func TestConvertAddedNestedFieldLevels(t *testing.T) {
	i32 := parquet.Leaf(parquet.Int32Type)
	opt, rep := parquet.Optional, parquet.Repeated

	// The sibling is repeated: s.b is one value for the group, not one per element of s.a.
	row := convertAddedField(t,
		parquet.Group{"s": opt(parquet.Group{"a": rep(i32), "b": opt(i32)})},
		parquet.Group{"s": opt(parquet.Group{"a": rep(i32)})},
		parquet.Row{
			parquet.ValueOf(int32(1)).Level(0, 2, 0),
			parquet.ValueOf(int32(2)).Level(1, 2, 0),
			parquet.ValueOf(int32(3)).Level(1, 2, 0),
		})
	if got := addedFieldLevels(row, 1); !reflect.DeepEqual(got, [][2]int{{0, 1}}) {
		t.Errorf("repeated sibling: s.b (rep, def) = %v, want [[0 1]]", got)
	}

	// A required field added next to an optional sibling stays within its own max definition level.
	row = convertAddedField(t,
		parquet.Group{"s": opt(parquet.Group{"a": opt(i32), "b": i32})},
		parquet.Group{"s": opt(parquet.Group{"a": opt(i32)})},
		parquet.Row{parquet.ValueOf(int32(5)).Level(0, 2, 0)})
	if got := addedFieldLevels(row, 1); !reflect.DeepEqual(got, [][2]int{{0, 1}}) {
		t.Errorf("required added field: s.b (rep, def) = %v, want [[0 1]]", got)
	}
	if v := columnValues(row, 1); len(v) != 1 || v[0].IsNull() || v[0].Int32() != 0 {
		t.Errorf("required added field: got %+v, want a zero int32", v)
	}

	// The enclosing group has no direct leaf, so the nested leaf supplies the group's presence.
	row = convertAddedField(t,
		parquet.Group{"s": opt(parquet.Group{"inner": parquet.Group{"x": i32}, "y": opt(i32)})},
		parquet.Group{"s": opt(parquet.Group{"inner": parquet.Group{"x": i32}})},
		parquet.Row{parquet.ValueOf(int32(5)).Level(0, 1, 0)})
	if got := addedFieldLevels(row, 1); !reflect.DeepEqual(got, [][2]int{{0, 1}}) {
		t.Errorf("no leaf sibling: s.y (rep, def) = %v, want [[0 1]]", got)
	}

	// The enclosing group changed from required to optional.
	row = convertAddedField(t,
		parquet.Group{"s": opt(parquet.Group{"a": i32, "b": opt(i32)})},
		parquet.Group{"s": parquet.Group{"a": i32}},
		parquet.Row{parquet.ValueOf(int32(5)).Level(0, 0, 0)})
	if got := addedFieldLevels(row, 1); !reflect.DeepEqual(got, [][2]int{{0, 1}}) {
		t.Errorf("required to optional ancestor: s.b (rep, def) = %v, want [[0 1]]", got)
	}
}

func addedFieldLevels(row parquet.Row, column int) [][2]int {
	var out [][2]int
	for _, v := range row {
		if v.Column() == column {
			out = append(out, [2]int{v.RepetitionLevel(), v.DefinitionLevel()})
		}
	}
	return out
}

func convertAddedField(t *testing.T, to, from parquet.Node, row parquet.Row) parquet.Row {
	t.Helper()
	conv, err := parquet.Convert(parquet.NewSchema("s", to), parquet.NewSchema("s", from))
	if err != nil {
		t.Fatal(err)
	}
	rows := []parquet.Row{row}
	if _, err := conv.Convert(rows); err != nil {
		t.Fatal(err)
	}
	return rows[0]
}

// A repeated sibling must not be copied onto a non-repeated field, including
// when the column is read from the converted row group rather than from rows.
func TestConvertAddedFieldDropsRepeatedSiblingValues(t *testing.T) {
	type Inner struct {
		A []int32 `parquet:"a"`
	}
	type Source struct {
		S *Inner `parquet:"s,optional"`
	}
	type InnerTarget struct {
		A []int32 `parquet:"a"`
		B *int32  `parquet:"b,optional"`
	}
	type Target struct {
		S *InnerTarget `parquet:"s,optional"`
	}

	first := []int32{1, 2, 3}
	src := parquet.NewGenericBuffer[Source]()
	if _, err := src.Write([]Source{
		{S: &Inner{A: first}},
		{S: nil},
		{S: &Inner{}},
	}); err != nil {
		t.Fatal(err)
	}
	conv, err := parquet.Convert(parquet.SchemaOf(new(Target)), src.Schema())
	if err != nil {
		t.Fatal(err)
	}
	converted := parquet.ConvertRowGroup(src, conv)
	rows := readConvertedRows(t, converted.Rows())
	schema := parquet.SchemaOf(new(Target))
	want := []Target{
		{S: &InnerTarget{A: first}},
		{},
		{S: &InnerTarget{A: []int32{}}},
	}
	if len(rows) != len(want) {
		t.Fatalf("got %d rows, want %d", len(rows), len(want))
	}
	for i, row := range rows {
		var out Target
		if err := schema.Reconstruct(&out, row); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(out, want[i]) {
			t.Errorf("row %d: got %+v, want %+v", i, out, want[i])
		}
		b := columnValues(row, 1)
		wantDef := 1
		if i == 1 {
			wantDef = 0
		}
		if len(b) != 1 || b[0].RepetitionLevel() != 0 || b[0].DefinitionLevel() != wantDef || !b[0].IsNull() {
			t.Errorf("row %d b: %+v, want one null at rep=0 def=%d", i, b, wantDef)
		}
	}
	var fromRows []parquet.Value
	for _, row := range rows {
		fromRows = append(fromRows, columnValues(row, 1)...)
	}
	levelsEqual(t, readColumnValues(t, converted.ColumnChunks()[1]), fromRows)
}

// An optional field added to a repeated group whose existing children are all groups.
func TestConvertAddedFieldToRepeatedGroupWithoutLeafSiblings(t *testing.T) {
	type Inner struct {
		X int32 `parquet:"x"`
	}
	type ItemV1 struct {
		Inner Inner `parquet:"inner"`
	}
	type V1 struct {
		S []ItemV1 `parquet:"s"`
	}
	type ItemV2 struct {
		Inner Inner  `parquet:"inner"`
		Y     *int32 `parquet:"y,optional"`
	}
	type V2 struct {
		S []ItemV2 `parquet:"s"`
	}

	input := []V1{
		{S: []ItemV1{{Inner: Inner{X: 5}}, {Inner: Inner{X: 6}}}},
		{S: []ItemV1{{Inner: Inner{X: 7}}}},
		{S: nil},
	}
	buf := new(bytes.Buffer)
	if err := parquet.Write(buf, input); err != nil {
		t.Fatal(err)
	}
	r := parquet.NewGenericReader[V2](bytes.NewReader(buf.Bytes()))
	t.Cleanup(func() { r.Close() })
	out := make([]V2, len(input))
	n, err := r.Read(out)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if n != len(input) {
		t.Fatalf("read %d rows, err: %v", n, err)
	}
	want := []V2{
		{S: []ItemV2{{Inner: Inner{X: 5}}, {Inner: Inner{X: 6}}}},
		{S: []ItemV2{{Inner: Inner{X: 7}}}},
		{S: []ItemV2{}},
	}
	if !reflect.DeepEqual(out, want) {
		t.Fatalf("got %+v, want %+v", out, want)
	}

	// Column chunks of the converted row group follow the same levels as the rows.
	src := parquet.NewGenericBuffer[V1]()
	if _, err := src.Write(input); err != nil {
		t.Fatal(err)
	}
	conv, err := parquet.Convert(parquet.SchemaOf(new(V2)), src.Schema())
	if err != nil {
		t.Fatal(err)
	}
	converted := parquet.ConvertRowGroup(src, conv)
	rows := readConvertedRows(t, converted.Rows())
	var fromRows []parquet.Value
	for _, row := range rows {
		fromRows = append(fromRows, columnValues(row, 1)...)
	}
	levelsEqual(t, readColumnValues(t, converted.ColumnChunks()[1]), fromRows)
	if len(fromRows) != 4 {
		t.Fatalf("got %d y values, want 4", len(fromRows))
	}
	wantLevels := [][2]int{{0, 1}, {1, 1}, {0, 1}, {0, 0}}
	for i, v := range fromRows {
		if !v.IsNull() || v.RepetitionLevel() != wantLevels[i][0] || v.DefinitionLevel() != wantLevels[i][1] {
			t.Errorf("y[%d]: rep=%d def=%d null=%v, want rep=%d def=%d null=true",
				i, v.RepetitionLevel(), v.DefinitionLevel(), v.IsNull(), wantLevels[i][0], wantLevels[i][1])
		}
	}
}

// A field added beside a nested repeated leaf must count group instances, and
// seeking must start at that row rather than replaying the column.
func TestProjectedColumnCountsGroupInstances(t *testing.T) {
	type Inner struct {
		X []int32 `parquet:"x"`
	}
	type Item struct {
		Inner Inner `parquet:"inner"`
	}
	type Source struct {
		S []Item `parquet:"s"`
	}
	type ItemTarget struct {
		Inner Inner  `parquet:"inner"`
		Y     *int32 `parquet:"y,optional"`
	}
	type Target struct {
		S []ItemTarget `parquet:"s"`
	}

	src := parquet.NewGenericBuffer[Source]()
	if _, err := src.Write([]Source{
		{S: []Item{{Inner: Inner{X: []int32{1, 2}}}, {Inner: Inner{X: []int32{3}}}}},
		{S: []Item{{Inner: Inner{X: []int32{4, 5}}}}},
		{},
	}); err != nil {
		t.Fatal(err)
	}
	conv, err := parquet.Convert(parquet.SchemaOf(new(Target)), src.Schema())
	if err != nil {
		t.Fatal(err)
	}
	converted := parquet.ConvertRowGroup(src, conv)
	y := converted.ColumnChunks()[1]
	template := src.ColumnChunks()[0]
	if template.NumValues() <= y.NumValues() {
		t.Fatalf("template values %d, projected values %d", template.NumValues(), y.NumValues())
	}
	if y.NumValues() != 4 {
		t.Fatalf("NumValues = %d, want 4", y.NumValues())
	}
	got := readColumnValues(t, y)
	if int64(len(got)) != y.NumValues() {
		t.Fatalf("read %d values, NumValues = %d", len(got), y.NumValues())
	}
	want := [][2]int{{0, 1}, {1, 1}, {0, 1}, {0, 0}}
	for i, v := range got {
		if !v.IsNull() || v.RepetitionLevel() != want[i][0] || v.DefinitionLevel() != want[i][1] {
			t.Errorf("value %d: rep=%d def=%d null=%v, want rep=%d def=%d",
				i, v.RepetitionLevel(), v.DefinitionLevel(), v.IsNull(), want[i][0], want[i][1])
		}
	}

	pages := y.Pages()
	defer pages.Close()
	if err := pages.SeekToRow(1); err != nil {
		t.Fatal(err)
	}
	page, err := pages.ReadPage()
	if err != nil {
		t.Fatal(err)
	}
	if page.NumRows() != 2 || page.NumValues() != 2 {
		t.Fatalf("seek page rows=%d values=%d, want 2 and 2", page.NumRows(), page.NumValues())
	}
	buf := make([]parquet.Value, 8)
	n, err := page.Values().ReadValues(buf)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if n != 2 || buf[0].RepetitionLevel() != 0 || buf[0].DefinitionLevel() != 1 || buf[1].DefinitionLevel() != 0 {
		t.Fatalf("seek values n=%d rep0=%d def0=%d def1=%d", n, buf[0].RepetitionLevel(), buf[0].DefinitionLevel(), buf[1].DefinitionLevel())
	}

	// Slice indexes are relative to the receiver. The tail of a page that
	// already starts at row 1 is the empty row, not the first row of the column.
	base, err := y.Pages().ReadPage()
	if err != nil {
		t.Fatal(err)
	}
	tail := base.Slice(1, 3).Slice(1, 2)
	if tail.NumRows() != 1 || tail.NumValues() != 1 {
		t.Fatalf("nested slice rows=%d values=%d, want 1 and 1", tail.NumRows(), tail.NumValues())
	}
	n, err = tail.Values().ReadValues(buf)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if n != 1 || buf[0].DefinitionLevel() != 0 {
		t.Fatalf("nested slice: n=%d def=%d, want one value at def 0", n, buf[0].DefinitionLevel())
	}

	// A slice must stop at its rows even when NumValues has not been filled in.
	fresh, err := y.Pages().ReadPage()
	if err != nil {
		t.Fatal(err)
	}
	firstRow := fresh.Slice(0, 1)
	wide := make([]parquet.Value, 32)
	n, err = firstRow.Values().ReadValues(wide)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("unread slice returned %d values, want 2", n)
	}
}

func TestAddedFieldWhenAncestorBecomesRepeated(t *testing.T) {
	type Item struct {
		A int32 `parquet:"a"`
	}
	type Source struct {
		S Item `parquet:"s"`
	}
	type ItemTarget struct {
		A int32  `parquet:"a"`
		B *int32 `parquet:"b,optional"`
	}
	type Target struct {
		S []ItemTarget `parquet:"s"`
	}

	buf := new(bytes.Buffer)
	if err := parquet.Write(buf, []Source{{S: Item{A: 5}}}); err != nil {
		t.Fatal(err)
	}
	r := parquet.NewGenericReader[Target](bytes.NewReader(buf.Bytes()))
	t.Cleanup(func() { r.Close() })
	out := make([]Target, 1)
	n, err := r.Read(out)
	if n != 1 || (err != nil && err != io.EOF) {
		t.Fatalf("read %d rows, err %v", n, err)
	}
	if len(out[0].S) != 1 || out[0].S[0].A != 5 || out[0].S[0].B != nil {
		t.Fatalf("got %+v", out[0].S)
	}
}

type failAfterChunk struct {
	parquet.ColumnChunk
	calls  int
	failAt int
}

func (c *failAfterChunk) Pages() parquet.Pages {
	c.calls++
	if c.calls >= c.failAt {
		return failingPages{}
	}
	return c.ColumnChunk.Pages()
}

func TestProjectedCountErrorSurfacesOnRead(t *testing.T) {
	type Inner struct {
		X []int32 `parquet:"x"`
	}
	type Item struct {
		Inner Inner `parquet:"inner"`
	}
	type Source struct {
		S []Item `parquet:"s"`
	}
	type ItemTarget struct {
		Inner Inner  `parquet:"inner"`
		Y     *int32 `parquet:"y,optional"`
	}
	type Target struct {
		S []ItemTarget `parquet:"s"`
	}
	src := parquet.NewGenericBuffer[Source]()
	if _, err := src.Write([]Source{{S: []Item{{Inner: Inner{X: []int32{1, 2}}}}}}); err != nil {
		t.Fatal(err)
	}
	orig := src.ColumnChunks()
	wrapped := &failAfterChunk{ColumnChunk: orig[0], failAt: 2}
	rg := accessRowGroup{RowGroup: src, columns: []parquet.ColumnChunk{wrapped}}
	conv, err := parquet.Convert(parquet.SchemaOf(new(Target)), src.Schema())
	if err != nil {
		t.Fatal(err)
	}
	y := parquet.ConvertRowGroup(rg, conv).ColumnChunks()[1]
	pages := y.Pages()
	defer pages.Close()
	page, err := pages.ReadPage()
	if err != nil {
		t.Fatal(err)
	}
	if y.NumValues() != 0 {
		t.Fatalf("NumValues = %d after a failed count, want 0", y.NumValues())
	}
	buf := make([]parquet.Value, 4)
	n, err := page.Values().ReadValues(buf)
	if !errors.Is(err, errSiblingRead) {
		t.Fatalf("ReadValues n=%d err=%v, want %v", n, err, errSiblingRead)
	}
}

func TestAddedColumnNullWhenNotPresent(t *testing.T) {
	i32 := parquet.Leaf(parquet.Int32Type)
	opt, rep := parquet.Optional, parquet.Repeated

	// An added repeated field is an empty list: a null, not a non-null zero.
	row := convertAddedField(t,
		parquet.Group{"a": i32, "b": rep(i32)},
		parquet.Group{"a": i32},
		parquet.Row{parquet.ValueOf(int32(1)).Level(0, 0, 0)},
	)
	got := columnValues(row, 1)
	if len(got) != 1 || !got[0].IsNull() || got[0].DefinitionLevel() != 0 {
		t.Fatalf("repeated field: %+v, want one null at def 0", got)
	}

	// A required field inside a null optional parent stays null.
	row = convertAddedField(t,
		parquet.Group{"s": opt(parquet.Group{"a": opt(i32), "b": i32})},
		parquet.Group{"s": opt(parquet.Group{"a": opt(i32)})},
		parquet.Row{parquet.NullValue().Level(0, 0, 0)},
	)
	got = columnValues(row, 1)
	if len(got) != 1 || !got[0].IsNull() || got[0].DefinitionLevel() != 0 {
		t.Fatalf("null parent: %+v, want one null at def 0", got)
	}

	// The same field is a zero when the parent is present.
	row = convertAddedField(t,
		parquet.Group{"s": opt(parquet.Group{"a": opt(i32), "b": i32})},
		parquet.Group{"s": opt(parquet.Group{"a": opt(i32)})},
		parquet.Row{parquet.ValueOf(int32(5)).Level(0, 1, 0)},
	)
	got = columnValues(row, 1)
	if len(got) != 1 || got[0].IsNull() || got[0].DefinitionLevel() != 1 || got[0].Int32() != 0 {
		t.Fatalf("present parent: %+v, want a zero at def 1", got)
	}

	// A required leaf inside a group the source does not have is null.
	row = convertAddedField(t,
		parquet.Group{"a": i32, "g": opt(parquet.Group{"x": i32})},
		parquet.Group{"a": i32},
		parquet.Row{parquet.ValueOf(int32(1)).Level(0, 0, 0)},
	)
	got = columnValues(row, 1)
	if len(got) != 1 || !got[0].IsNull() || got[0].DefinitionLevel() != 0 {
		t.Fatalf("new group: %+v, want one null at def 0", got)
	}

	// A present fixed-length byte array zero has the type's length.
	flba := parquet.FixedLenByteArrayType(4)
	row = convertAddedField(t,
		parquet.Group{"b": parquet.Leaf(flba)},
		parquet.Group{},
		parquet.Row{},
	)
	got = columnValues(row, 0)
	if len(got) != 1 || got[0].IsNull() || len(got[0].Bytes()) != 4 {
		t.Fatalf("fixed-length zero: %+v len=%d", got, len(got[0].Bytes()))
	}
}

func TestReplacingNestedGroupKeepsParentPresence(t *testing.T) {
	type Inner struct {
		X int32 `parquet:"x"`
	}
	type SourceGroup struct {
		Inner Inner `parquet:"inner"`
	}
	type Source struct {
		S *SourceGroup `parquet:"s,optional"`
	}
	type Target struct {
		S *struct {
			Y int32 `parquet:"y"`
		} `parquet:"s,optional"`
	}
	present := Source{S: &SourceGroup{Inner: Inner{X: 3}}}
	src := parquet.NewGenericBuffer[Source]()
	if _, err := src.Write([]Source{present, {}}); err != nil {
		t.Fatal(err)
	}
	conv, err := parquet.Convert(parquet.SchemaOf(new(Target)), src.Schema())
	if err != nil {
		t.Fatal(err)
	}
	rows := readConvertedRows(t, parquet.ConvertRowGroup(src, conv).Rows())
	if len(rows) != 2 {
		t.Fatalf("got %d rows", len(rows))
	}
	y0 := columnValues(rows[0], 0)
	if len(y0) != 1 || y0[0].IsNull() || y0[0].DefinitionLevel() != 1 || y0[0].Int32() != 0 {
		t.Fatalf("present parent: %+v", y0)
	}
	y1 := columnValues(rows[1], 0)
	if len(y1) != 1 || !y1[0].IsNull() || y1[0].DefinitionLevel() != 0 {
		t.Fatalf("absent parent: %+v", y1)
	}
}

func TestProjectedCountIsLazyAndSliceDoesNotOpen(t *testing.T) {
	type Inner struct {
		X []int32 `parquet:"x"`
	}
	type Item struct {
		Inner Inner `parquet:"inner"`
	}
	type Source struct {
		S []Item `parquet:"s"`
	}
	type ItemTarget struct {
		Inner Inner  `parquet:"inner"`
		Y     *int32 `parquet:"y,optional"`
	}
	type Target struct {
		S []ItemTarget `parquet:"s"`
	}
	srcRows := []Source{
		{S: []Item{{Inner: Inner{X: []int32{1, 2}}}, {Inner: Inner{X: []int32{3}}}}},
		{S: []Item{{Inner: Inner{X: []int32{4, 5}}}}},
	}
	buf := parquet.NewGenericBuffer[Source]()
	if _, err := buf.Write(srcRows); err != nil {
		t.Fatal(err)
	}
	chunks, rg := wrapAccess(buf, nil)
	conv, err := parquet.Convert(parquet.SchemaOf(new(Target)), buf.Schema())
	if err != nil {
		t.Fatal(err)
	}
	converted := parquet.ConvertRowGroup(rg, conv)
	if chunks[0].reads != 0 {
		t.Fatalf("ConvertRowGroup opened the template %d times", chunks[0].reads)
	}
	_ = readConvertedRows(t, converted.Rows())
	if chunks[0].reads != 1 {
		t.Fatalf("reading rows opened the template %d times, want 1", chunks[0].reads)
	}

	y := converted.ColumnChunks()[1]
	if y.NumValues() != 3 {
		t.Fatalf("NumValues = %d, want 3", y.NumValues())
	}
	if chunks[0].reads == 0 {
		t.Fatal("NumValues did not read the template")
	}
	index, err := y.ColumnIndex()
	if err != nil {
		t.Fatal(err)
	}
	if index.NullCount(0) != y.NumValues() {
		t.Fatalf("NullCount = %d, want %d", index.NullCount(0), y.NumValues())
	}

	pages := y.Pages()
	defer pages.Close()
	page, err := pages.ReadPage()
	if err != nil {
		t.Fatal(err)
	}
	before := chunks[0].reads
	_ = page.Slice(0, 1).Slice(0, 1)
	if chunks[0].reads != before {
		t.Fatalf("chained slice opened the template %d times", chunks[0].reads-before)
	}
}

func TestMissingRequiredRootColumnHasNoNulls(t *testing.T) {
	type Source struct {
		ID int64 `parquet:"id"`
	}
	type Target struct {
		ID   int64  `parquet:"id"`
		Name string `parquet:"name"`
	}
	src := parquet.NewGenericBuffer[Source]()
	if _, err := src.Write([]Source{{ID: 1}, {ID: 2}}); err != nil {
		t.Fatal(err)
	}
	conv, err := parquet.Convert(parquet.SchemaOf(new(Target)), src.Schema())
	if err != nil {
		t.Fatal(err)
	}
	chunk := parquet.ConvertRowGroup(src, conv).ColumnChunks()[1]
	if chunk.NumValues() != 2 {
		t.Fatalf("NumValues = %d, want 2", chunk.NumValues())
	}
	index, err := chunk.ColumnIndex()
	if err != nil {
		t.Fatal(err)
	}
	if index.NullCount(0) != 0 {
		t.Fatalf("NullCount = %d, want 0", index.NullCount(0))
	}
	pages := chunk.Pages()
	defer pages.Close()
	page, err := pages.ReadPage()
	if err != nil {
		t.Fatal(err)
	}
	if page.NumNulls() != 0 {
		t.Fatalf("page NumNulls = %d, want 0", page.NumNulls())
	}
}
