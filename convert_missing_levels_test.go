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
		if vals[0].RepetitionLevel() != 0 || vals[0].DefinitionLevel() != wantDef[i] || vals[0].IsNull() != wantNull {
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
		if buf[i].RepetitionLevel() != 0 || buf[i].DefinitionLevel() != def || buf[i].IsNull() != wantNull {
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
		if buf[i].RepetitionLevel() != 0 || buf[i].DefinitionLevel() != wantDef || buf[i].IsNull() {
			t.Errorf("value %d: rep=%d def=%d null=%v, want rep=0 def=%d null=false",
				i, buf[i].RepetitionLevel(), buf[i].DefinitionLevel(), buf[i].IsNull(), wantDef)
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

// An optional field added to a repeated group whose existing children are all
// groups has no sibling leaf; its values follow the instances of the group.
func TestReadAddedFieldInRepeatedGroupWithoutLeafSiblings(t *testing.T) {
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

	buf := new(bytes.Buffer)
	if err := parquet.Write(buf, []V1{
		{S: []ItemV1{{Inner{5}}, {Inner{6}}}},
		{},
		{S: []ItemV1{{Inner{7}}}},
	}); err != nil {
		t.Fatal(err)
	}
	got, err := parquet.Read[V2](bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}
	want := []V2{
		{S: []ItemV2{{Inner: Inner{5}}, {Inner: Inner{6}}}},
		{S: []ItemV2{}},
		{S: []ItemV2{{Inner: Inner{7}}}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

func TestConvertAddedNestedFieldLevels(t *testing.T) {
	i32 := parquet.Leaf(parquet.Int32Type)
	opt, rep := parquet.Optional, parquet.Repeated
	type level struct{ rep, def int }
	type row struct {
		in   []level // levels of the only source column
		want []level // levels of the added column
	}
	tests := []struct {
		scenario string
		from, to parquet.Group
		added    []string
		rows     []row
	}{
		{
			scenario: "repeated sibling",
			from:     parquet.Group{"s": opt(parquet.Group{"a": rep(i32)})},
			to:       parquet.Group{"s": opt(parquet.Group{"a": rep(i32), "b": opt(i32)})},
			added:    []string{"s", "b"},
			rows: []row{
				{in: []level{{0, 2}, {1, 2}, {1, 2}}, want: []level{{0, 1}}},
				{in: []level{{0, 1}}, want: []level{{0, 1}}}, // empty list
				{in: []level{{0, 0}}, want: []level{{0, 0}}}, // null s
			},
		},
		{
			scenario: "required field next to an optional sibling",
			from:     parquet.Group{"s": opt(parquet.Group{"a": opt(i32)})},
			to:       parquet.Group{"s": opt(parquet.Group{"a": opt(i32), "b": i32})},
			added:    []string{"s", "b"},
			rows: []row{
				{in: []level{{0, 2}}, want: []level{{0, 1}}},
				{in: []level{{0, 1}}, want: []level{{0, 1}}}, // null a
				{in: []level{{0, 0}}, want: []level{{0, 0}}}, // null s
			},
		},
		{
			scenario: "no leaf sibling in a repeated group",
			from:     parquet.Group{"s": rep(parquet.Group{"inner": parquet.Group{"x": i32}})},
			to:       parquet.Group{"s": rep(parquet.Group{"inner": parquet.Group{"x": i32}, "y": opt(i32)})},
			added:    []string{"s", "y"},
			rows: []row{
				{in: []level{{0, 1}, {1, 1}}, want: []level{{0, 1}, {1, 1}}},
				{in: []level{{0, 0}}, want: []level{{0, 0}}},
			},
		},
		{
			scenario: "only sibling is in a nested repeated group",
			from:     parquet.Group{"s": opt(parquet.Group{"items": rep(parquet.Group{"x": i32})})},
			to:       parquet.Group{"s": opt(parquet.Group{"items": rep(parquet.Group{"x": i32}), "y": opt(i32)})},
			added:    []string{"s", "y"},
			rows: []row{
				{in: []level{{0, 2}, {1, 2}}, want: []level{{0, 1}}},
				{in: []level{{0, 1}}, want: []level{{0, 1}}}, // no items
				{in: []level{{0, 0}}, want: []level{{0, 0}}}, // null s
			},
		},
		{
			scenario: "required ancestor became optional",
			from:     parquet.Group{"s": parquet.Group{"a": i32}},
			to:       parquet.Group{"s": opt(parquet.Group{"a": i32, "b": opt(i32)})},
			added:    []string{"s", "b"},
			rows: []row{
				{in: []level{{0, 0}}, want: []level{{0, 1}}},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			from := parquet.NewSchema("s", test.from)
			to := parquet.NewSchema("s", test.to)
			conv, err := parquet.Convert(to, from)
			if err != nil {
				t.Fatal(err)
			}
			source, _ := from.Lookup(from.Columns()[0]...)
			added, ok := to.Lookup(test.added...)
			if !ok {
				t.Fatalf("no column %v", test.added)
			}
			rows := make([]parquet.Row, len(test.rows))
			for i, r := range test.rows {
				for _, l := range r.in {
					v := parquet.Value{}
					if l.def == source.MaxDefinitionLevel {
						v = parquet.ValueOf(int32(1))
					}
					rows[i] = append(rows[i], v.Level(l.rep, l.def, 0))
				}
			}
			if _, err := conv.Convert(rows); err != nil {
				t.Fatal(err)
			}
			for i, r := range test.rows {
				var got []level
				for _, v := range columnValues(rows[i], added.ColumnIndex) {
					got = append(got, level{v.RepetitionLevel(), v.DefinitionLevel()})
				}
				if !reflect.DeepEqual(got, r.want) {
					t.Errorf("row %d: added column levels %v, want %v", i, got, r.want)
				}
			}
		})
	}
}
