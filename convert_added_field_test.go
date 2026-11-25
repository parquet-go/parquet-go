package parquet_test

import (
	"bytes"
	"io"
	"reflect"
	"testing"

	"github.com/parquet-go/parquet-go"
)

// An optional field added to a repeated group whose existing children are all
// groups. Reported in issue 778.
func TestAddFieldToRepeatedGroupWithoutLeafSiblings(t *testing.T) {
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

	cases := []struct {
		name string
		in   []V1
		want []V2
	}{
		{name: "zero rows", in: nil, want: nil},
		{
			name: "one row",
			in:   []V1{{S: []ItemV1{{Inner: Inner{X: 7}}, {Inner: Inner{X: 8}}}}},
			want: []V2{{S: []ItemV2{{Inner: Inner{X: 7}}, {Inner: Inner{X: 8}}}}},
		},
		{
			name: "many rows",
			in: []V1{
				{S: []ItemV1{{Inner: Inner{X: 5}}, {Inner: Inner{X: 6}}}},
				{S: nil},
				{S: []ItemV1{{Inner: Inner{X: 7}}}},
			},
			want: []V2{
				{S: []ItemV2{{Inner: Inner{X: 5}}, {Inner: Inner{X: 6}}}},
				{S: []ItemV2{}},
				{S: []ItemV2{{Inner: Inner{X: 7}}}},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf := new(bytes.Buffer)
			if err := parquet.Write(buf, tc.in); err != nil {
				t.Fatal(err)
			}
			r := parquet.NewGenericReader[V2](bytes.NewReader(buf.Bytes()))
			t.Cleanup(func() { r.Close() })
			out := make([]V2, len(tc.want)+1)
			n, err := r.Read(out)
			if err != nil && err != io.EOF {
				t.Fatal(err)
			}
			if n != len(tc.want) {
				t.Fatalf("read %d rows, err %v", n, err)
			}
			if tc.want == nil {
				tc.want = []V2{}
			}
			if !reflect.DeepEqual(out[:n], tc.want) {
				t.Fatalf("got %+v, want %+v", out[:n], tc.want)
			}
		})
	}
}

func levelsOf(row parquet.Row, column int) (out [][2]int) {
	for _, v := range row {
		if v.Column() == column {
			out = append(out, [2]int{v.RepetitionLevel(), v.DefinitionLevel()})
		}
	}
	return out
}

func convertRows(t *testing.T, to, from parquet.Node, rows []parquet.Row) []parquet.Row {
	t.Helper()
	conv, err := parquet.Convert(parquet.NewSchema("s", to), parquet.NewSchema("s", from))
	if err != nil {
		t.Fatal(err)
	}
	out := make([]parquet.Row, len(rows))
	for i, row := range rows {
		out[i] = append(parquet.Row(nil), row...)
	}
	if _, err := conv.Convert(out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestAddedFieldLevels(t *testing.T) {
	i32 := parquet.Leaf(parquet.Int32Type)
	opt, rep := parquet.Optional, parquet.Repeated

	// 1. The sibling is repeated: s.b (max rep 0) gets one value per element of s.a.
	row := convertRows(t,
		parquet.Group{"s": opt(parquet.Group{"a": rep(i32), "b": opt(i32)})},
		parquet.Group{"s": opt(parquet.Group{"a": rep(i32)})},
		[]parquet.Row{{
			parquet.ValueOf(int32(1)).Level(0, 2, 0),
			parquet.ValueOf(int32(2)).Level(1, 2, 0),
			parquet.ValueOf(int32(3)).Level(1, 2, 0),
		}},
	)[0]
	if got := levelsOf(row, 1); len(got) != 1 || got[0] != [2]int{0, 1} {
		t.Errorf("repeated sibling: s.b (rep, def) = %v, want [[0 1]]", got)
	}
	if v := columnValues(row, 1); len(v) != 1 || !v[0].IsNull() {
		t.Errorf("repeated sibling: s.b = %+v, want one null", v)
	}

	// 1b. A required field added next to an optional sibling keeps the sibling's definition level.
	row = convertRows(t,
		parquet.Group{"s": opt(parquet.Group{"a": opt(i32), "b": i32})},
		parquet.Group{"s": opt(parquet.Group{"a": opt(i32)})},
		[]parquet.Row{{parquet.ValueOf(int32(5)).Level(0, 2, 0)}},
	)[0]
	if got := levelsOf(row, 1); len(got) != 1 || got[0] != [2]int{0, 1} {
		t.Errorf("required added field: s.b (rep, def) = %v, want [[0 1]] (max def is 1)", got)
	}
	if v := columnValues(row, 1); len(v) != 1 || v[0].IsNull() || v[0].Int32() != 0 {
		t.Errorf("required added field: s.b = %+v, want one zero", v)
	}

	// 2. The enclosing group has no direct leaf: s.y says s is null although s is present.
	row = convertRows(t,
		parquet.Group{"s": opt(parquet.Group{"inner": parquet.Group{"x": i32}, "y": opt(i32)})},
		parquet.Group{"s": opt(parquet.Group{"inner": parquet.Group{"x": i32}})},
		[]parquet.Row{{parquet.ValueOf(int32(5)).Level(0, 1, 0)}},
	)[0]
	if got := levelsOf(row, 1); len(got) != 1 || got[0] != [2]int{0, 1} {
		t.Errorf("no leaf sibling: s.y (rep, def) = %v, want [[0 1]]", got)
	}

	// 3. The enclosing group changed from REQUIRED to OPTIONAL.
	row = convertRows(t,
		parquet.Group{"s": opt(parquet.Group{"a": i32, "b": opt(i32)})},
		parquet.Group{"s": parquet.Group{"a": i32}},
		[]parquet.Row{{parquet.ValueOf(int32(5)).Level(0, 0, 0)}},
	)[0]
	if got := levelsOf(row, 1); len(got) != 1 || got[0] != [2]int{0, 1} {
		t.Errorf("required -> optional ancestor: s.b (rep, def) = %v, want [[0 1]]", got)
	}
}

// synthesizedCase is one shape of a missing target column. Rows are the source
// column only; the added column is what Convert must synthesize.
type synthesizedCase struct {
	name     string
	to, from parquet.Node
	// column is the target index of the added field.
	column int
	leaf   []string
	rows   []parquet.Row
	// want is per source row: repetition and definition levels of the added field.
	want [][][2]int
	// null is true when the added leaf is optional or repeated (a null placeholder).
	// Required leaves are typed zeros.
	null bool
}

func TestSynthesizedAddedFieldMatrix(t *testing.T) {
	i32 := parquet.Leaf(parquet.Int32Type)
	opt, rep := parquet.Optional, parquet.Repeated

	repeatedSibling := parquet.Group{"s": opt(parquet.Group{"a": rep(i32), "b": opt(i32)})}
	repeatedSiblingFrom := parquet.Group{"s": opt(parquet.Group{"a": rep(i32)})}

	// "z" sorts after "inner", so the source leaf stays column 0.
	nestedRep := parquet.Group{"outer": rep(parquet.Group{
		"inner": rep(parquet.Group{"a": i32}),
		"z":     opt(i32),
	})}
	nestedRepFrom := parquet.Group{"outer": rep(parquet.Group{
		"inner": rep(parquet.Group{"a": i32}),
	})}

	noLeaf := parquet.Group{"s": opt(parquet.Group{
		"inner": parquet.Group{"x": i32},
		"y":     opt(i32),
	})}
	noLeafFrom := parquet.Group{"s": opt(parquet.Group{
		"inner": parquet.Group{"x": i32},
	})}

	reqToOpt := parquet.Group{"s": opt(parquet.Group{"a": i32, "b": opt(i32)})}
	reqToOptFrom := parquet.Group{"s": parquet.Group{"a": i32}}

	optSiblingReqField := parquet.Group{"s": opt(parquet.Group{"a": opt(i32), "b": i32})}
	optSiblingReqFieldFrom := parquet.Group{"s": opt(parquet.Group{"a": opt(i32)})}

	deeperNesting := parquet.Group{"s": opt(parquet.Group{
		"a": opt(parquet.Group{"x": opt(i32)}),
		"b": opt(i32),
	})}
	deeperNestingFrom := parquet.Group{"s": opt(parquet.Group{
		"a": opt(parquet.Group{"x": opt(i32)}),
	})}

	ancestorShift := parquet.Group{"outer": opt(parquet.Group{
		"s": opt(parquet.Group{"a": i32, "b": opt(i32)}),
	})}
	ancestorShiftFrom := parquet.Group{"outer": parquet.Group{
		"s": opt(parquet.Group{"a": i32}),
	}}

	cases := []synthesizedCase{
		{
			name:   "repeated sibling, group present",
			to:     repeatedSibling,
			from:   repeatedSiblingFrom,
			column: 1,
			leaf:   []string{"s", "b"},
			rows: []parquet.Row{{
				parquet.ValueOf(int32(1)).Level(0, 2, 0),
				parquet.ValueOf(int32(2)).Level(1, 2, 0),
				parquet.ValueOf(int32(3)).Level(1, 2, 0),
			}},
			want: [][][2]int{{{0, 1}}},
			null: true,
		},
		{
			name:   "repeated sibling, group null",
			to:     repeatedSibling,
			from:   repeatedSiblingFrom,
			column: 1,
			leaf:   []string{"s", "b"},
			rows:   []parquet.Row{{parquet.ValueOf(nil).Level(0, 0, 0)}},
			want:   [][][2]int{{{0, 0}}},
			null:   true,
		},
		{
			name:   "repeated sibling, group present and list empty",
			to:     repeatedSibling,
			from:   repeatedSiblingFrom,
			column: 1,
			leaf:   []string{"s", "b"},
			rows:   []parquet.Row{{parquet.ValueOf(nil).Level(0, 1, 0)}},
			want:   [][][2]int{{{0, 1}}},
			null:   true,
		},
		{
			name:   "no direct leaf, group present",
			to:     noLeaf,
			from:   noLeafFrom,
			column: 1,
			leaf:   []string{"s", "y"},
			rows:   []parquet.Row{{parquet.ValueOf(int32(5)).Level(0, 1, 0)}},
			want:   [][][2]int{{{0, 1}}},
			null:   true,
		},
		{
			name:   "no direct leaf, group null",
			to:     noLeaf,
			from:   noLeafFrom,
			column: 1,
			leaf:   []string{"s", "y"},
			rows:   []parquet.Row{{parquet.ValueOf(nil).Level(0, 0, 0)}},
			want:   [][][2]int{{{0, 0}}},
			null:   true,
		},
		{
			name:   "required field beside optional sibling, sibling present",
			to:     optSiblingReqField,
			from:   optSiblingReqFieldFrom,
			column: 1,
			leaf:   []string{"s", "b"},
			rows:   []parquet.Row{{parquet.ValueOf(int32(5)).Level(0, 2, 0)}},
			want:   [][][2]int{{{0, 1}}},
			null:   false,
		},
		{
			name:   "required field beside optional sibling, sibling null",
			to:     optSiblingReqField,
			from:   optSiblingReqFieldFrom,
			column: 1,
			leaf:   []string{"s", "b"},
			rows:   []parquet.Row{{parquet.ValueOf(nil).Level(0, 1, 0)}},
			want:   [][][2]int{{{0, 1}}},
			null:   false,
		},
		{
			name:   "required field beside optional sibling, group null",
			to:     optSiblingReqField,
			from:   optSiblingReqFieldFrom,
			column: 1,
			leaf:   []string{"s", "b"},
			rows:   []parquet.Row{{parquet.ValueOf(nil).Level(0, 0, 0)}},
			want:   [][][2]int{{{0, 0}}},
			null:   false,
		},
		{
			name:   "required ancestor becomes optional",
			to:     reqToOpt,
			from:   reqToOptFrom,
			column: 1,
			leaf:   []string{"s", "b"},
			rows:   []parquet.Row{{parquet.ValueOf(int32(5)).Level(0, 0, 0)}},
			want:   [][][2]int{{{0, 1}}},
			null:   true,
		},
		{
			name:   "deeper optional nesting, all present",
			to:     deeperNesting,
			from:   deeperNestingFrom,
			column: 1,
			leaf:   []string{"s", "b"},
			// s optional (1) + a optional (2) + x optional (3).
			rows: []parquet.Row{{parquet.ValueOf(int32(5)).Level(0, 3, 0)}},
			want: [][][2]int{{{0, 1}}},
			null: true,
		},
		{
			name:   "deeper optional nesting, inner null and group present",
			to:     deeperNesting,
			from:   deeperNestingFrom,
			column: 1,
			leaf:   []string{"s", "b"},
			rows:   []parquet.Row{{parquet.ValueOf(nil).Level(0, 1, 0)}},
			want:   [][][2]int{{{0, 1}}},
			null:   true,
		},
		{
			name:   "deeper optional nesting, group null",
			to:     deeperNesting,
			from:   deeperNestingFrom,
			column: 1,
			leaf:   []string{"s", "b"},
			rows:   []parquet.Row{{parquet.ValueOf(nil).Level(0, 0, 0)}},
			want:   [][][2]int{{{0, 0}}},
			null:   true,
		},
		{
			name:   "nested repetition yields one value per outer element",
			to:     nestedRep,
			from:   nestedRepFrom,
			column: 1,
			leaf:   []string{"outer", "z"},
			rows: []parquet.Row{{
				parquet.ValueOf(int32(10)).Level(0, 2, 0),
				parquet.ValueOf(int32(11)).Level(2, 2, 0),
				parquet.ValueOf(int32(12)).Level(1, 2, 0),
			}},
			want: [][][2]int{{{0, 1}, {1, 1}}},
			null: true,
		},
		{
			name:   "nested repetition, empty outer group",
			to:     nestedRep,
			from:   nestedRepFrom,
			column: 1,
			leaf:   []string{"outer", "z"},
			rows:   []parquet.Row{{parquet.ValueOf(nil).Level(0, 0, 0)}},
			want:   [][][2]int{{{0, 0}}},
			null:   true,
		},
		{
			name:   "nested repetition, outer present and inner empty",
			to:     nestedRep,
			from:   nestedRepFrom,
			column: 1,
			leaf:   []string{"outer", "z"},
			rows:   []parquet.Row{{parquet.ValueOf(nil).Level(0, 1, 0)}},
			want:   [][][2]int{{{0, 1}}},
			null:   true,
		},
		{
			name:   "ancestor required to optional shifts null and present",
			to:     ancestorShift,
			from:   ancestorShiftFrom,
			column: 1,
			leaf:   []string{"outer", "s", "b"},
			rows: []parquet.Row{
				{parquet.ValueOf(nil).Level(0, 0, 0)},      // s null
				{parquet.ValueOf(int32(4)).Level(0, 1, 0)}, // s present
			},
			want: [][][2]int{{{0, 1}}, {{0, 2}}},
			null: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if len(tc.rows) == 0 {
				t.Fatal("case has no rows")
			}
			schema := parquet.NewSchema("s", tc.to)
			leaf, ok := schema.Lookup(tc.leaf...)
			if !ok {
				t.Fatalf("lookup %v", tc.leaf)
			}
			got := convertRows(t, tc.to, tc.from, tc.rows)
			if len(got) != len(tc.want) {
				t.Fatalf("got %d rows, want %d", len(got), len(tc.want))
			}
			for i := range tc.want {
				values := columnValues(got[i], tc.column)
				if len(values) != len(tc.want[i]) {
					t.Fatalf("row %d: got %d values %v, want %d %v", i, len(values), levelsOf(got[i], tc.column), len(tc.want[i]), tc.want[i])
				}
				for j, want := range tc.want[i] {
					v := values[j]
					if v.RepetitionLevel() != want[0] || v.DefinitionLevel() != want[1] {
						t.Errorf("row %d value %d: rep=%d def=%d, want rep=%d def=%d", i, j, v.RepetitionLevel(), v.DefinitionLevel(), want[0], want[1])
					}
					if v.RepetitionLevel() > leaf.MaxRepetitionLevel || v.DefinitionLevel() > leaf.MaxDefinitionLevel {
						t.Errorf("row %d value %d: levels (%d,%d) exceed max (%d,%d)", i, j, v.RepetitionLevel(), v.DefinitionLevel(), leaf.MaxRepetitionLevel, leaf.MaxDefinitionLevel)
					}
					if v.Column() != tc.column {
						t.Errorf("row %d value %d: column %d, want %d", i, j, v.Column(), tc.column)
					}
					if tc.null {
						if !v.IsNull() {
							t.Errorf("row %d value %d: got non-null %v", i, j, v)
						}
					} else if v.IsNull() || v.Int32() != 0 {
						t.Errorf("row %d value %d: got %v, want zero", i, j, v)
					}
				}
				// The source column is still column 0 and keeps its values.
				// Column 0 is the source leaf. Its value is preserved. Levels stay
				// put when the shared ancestors do not change repetition.
				src := columnValues(got[i], 0)
				in := columnValues(tc.rows[i], 0)
				if len(src) != len(in) {
					t.Fatalf("row %d: source column has %d values, input has %d", i, len(src), len(in))
				}
				for j := range in {
					if src[j].Int32() != in[j].Int32() || src[j].IsNull() != in[j].IsNull() {
						t.Errorf("row %d source value %d changed: got null=%v %v, input null=%v %v", i, j, src[j].IsNull(), src[j], in[j].IsNull(), in[j])
					}
				}
			}
		})
	}
}

func TestSynthesizedFieldRowsAreIndependent(t *testing.T) {
	i32 := parquet.Leaf(parquet.Int32Type)
	opt, rep := parquet.Optional, parquet.Repeated
	to := parquet.Group{"outer": rep(parquet.Group{
		"inner": rep(parquet.Group{"a": i32}),
		"z":     opt(i32),
	})}
	from := parquet.Group{"outer": rep(parquet.Group{
		"inner": rep(parquet.Group{"a": i32}),
	})}
	rows := []parquet.Row{
		nil,
		{
			parquet.ValueOf(int32(1)).Level(0, 2, 0),
			parquet.ValueOf(int32(2)).Level(2, 2, 0),
		},
		{parquet.ValueOf(nil).Level(0, 0, 0)},
		{
			parquet.ValueOf(int32(3)).Level(0, 2, 0),
			parquet.ValueOf(int32(4)).Level(1, 2, 0),
			parquet.ValueOf(int32(5)).Level(2, 2, 0),
		},
	}
	want := [][][2]int{
		{{0, 0}},
		{{0, 1}},
		{{0, 0}},
		{{0, 1}, {1, 1}},
	}
	together := convertRows(t, to, from, rows)
	if len(together) != len(want) {
		t.Fatalf("got %d rows, want %d", len(together), len(want))
	}
	for i, row := range rows {
		alone := convertRows(t, to, from, []parquet.Row{row})[0]
		gotBatch := levelsOf(together[i], 1)
		gotAlone := levelsOf(alone, 1)
		if !reflect.DeepEqual(gotBatch, want[i]) || !reflect.DeepEqual(gotAlone, want[i]) {
			t.Errorf("row %d: batch %v, alone %v, want %v", i, gotBatch, gotAlone, want[i])
		}
	}

	conv, err := parquet.Convert(parquet.NewSchema("s", to), parquet.NewSchema("s", from))
	if err != nil {
		t.Fatal(err)
	}
	populated := []parquet.Row{{
		parquet.ValueOf(int32(1)).Level(0, 2, 0),
		parquet.ValueOf(int32(2)).Level(2, 2, 0),
		parquet.ValueOf(int32(3)).Level(1, 2, 0),
	}}
	if _, err := conv.Convert(populated); err != nil {
		t.Fatal(err)
	}
	n, err := conv.Convert(nil)
	if n != 0 || err != nil {
		t.Fatalf("Convert(nil) = %d, %v", n, err)
	}
	empty := []parquet.Row{}
	n, err = conv.Convert(empty)
	if n != 0 || err != nil || len(empty) != 0 {
		t.Fatalf("Convert(empty) = %d, %v, len %d", n, err, len(empty))
	}
	next := []parquet.Row{{parquet.ValueOf(nil).Level(0, 0, 0)}}
	if _, err := conv.Convert(next); err != nil {
		t.Fatal(err)
	}
	if got := levelsOf(next[0], 1); len(got) != 1 || got[0] != [2]int{0, 0} {
		t.Fatalf("row after a populated conversion: levels %v, want [[0 0]]", got)
	}
}

func TestMissingNestedFieldUnderFixedGroupStaysAtLevelZero(t *testing.T) {
	i32 := parquet.Leaf(parquet.Int32Type)
	opt := parquet.Optional
	// The source group is required and has no direct leaf, so its presence is
	// not a level that can be remapped. The added field stays at level 0 even
	// when the target group is optional. A nil message read into an optional
	// group depends on this.
	row := convertRows(t,
		parquet.Group{"s": opt(parquet.Group{"inner": parquet.Group{"x": i32}, "y": opt(i32)})},
		parquet.Group{"s": parquet.Group{"inner": parquet.Group{"x": i32}}},
		[]parquet.Row{{parquet.ValueOf(int32(5)).Level(0, 0, 0)}},
	)[0]
	if got := levelsOf(row, 1); len(got) != 1 || got[0] != [2]int{0, 0} {
		t.Fatalf("s.y levels = %v, want [[0 0]]", got)
	}
	if v := columnValues(row, 0); len(v) != 1 || v[0].Int32() != 5 || v[0].DefinitionLevel() != 1 {
		t.Fatalf("s.inner.x = rep=%d def=%d %v, want value 5 at def 1", v[0].RepetitionLevel(), v[0].DefinitionLevel(), v[0])
	}
}

func TestFlatMissingColumnIndex(t *testing.T) {
	type Source struct {
		Kept int64 `parquet:"kept"`
		Beta int64 `parquet:"beta"`
	}
	type Target struct {
		Kept  int64  `parquet:"kept"`
		Extra string `parquet:"extra"`
	}
	conv, err := parquet.Convert(parquet.SchemaOf(new(Target)), parquet.SchemaOf(new(Source)))
	if err != nil {
		t.Fatal(err)
	}
	// "beta" sorts before "kept", so the flat missing column still reports that
	// sibling. The value itself does not depend on the sibling.
	if got := conv.Column(1); got != 1 {
		t.Fatalf("Column(extra) = %d, want 1 (source beta)", got)
	}
	rows := []parquet.Row{{
		parquet.Int64Value(9).Level(0, 0, 0),
		parquet.Int64Value(4).Level(0, 0, 1),
	}}
	if _, err := conv.Convert(rows); err != nil {
		t.Fatal(err)
	}
	extra := columnValues(rows[0], 1)
	if len(extra) != 1 || extra[0].RepetitionLevel() != 0 || extra[0].DefinitionLevel() != 0 || extra[0].IsNull() || extra[0].String() != "" {
		t.Fatalf("extra = rep=%d def=%d null=%v %q", extra[0].RepetitionLevel(), extra[0].DefinitionLevel(), extra[0].IsNull(), extra[0].String())
	}
	kept := columnValues(rows[0], 0)
	if len(kept) != 1 || kept[0].Int64() != 9 {
		t.Fatalf("kept = %+v", kept)
	}
}

func TestAddedListAndMapFieldsMatchWriter(t *testing.T) {
	t.Run("list", func(t *testing.T) {
		type Inner struct {
			X int32 `parquet:"x"`
		}
		type ItemV1 struct {
			Inner Inner `parquet:"inner"`
		}
		type ItemV2 struct {
			Inner Inner  `parquet:"inner"`
			Y     *int32 `parquet:"y,optional"`
		}
		type V1 struct {
			S []ItemV1 `parquet:"s,list"`
		}
		type V2 struct {
			S []ItemV2 `parquet:"s,list"`
		}
		in := []V1{
			{},
			{S: []ItemV1{{Inner: Inner{X: 1}}}},
			{S: []ItemV1{{Inner: Inner{X: 2}}, {Inner: Inner{X: 3}}, {Inner: Inner{X: 4}}}},
		}
		want := []V2{
			{S: []ItemV2{}},
			{S: []ItemV2{{Inner: Inner{X: 1}}}},
			{S: []ItemV2{{Inner: Inner{X: 2}}, {Inner: Inner{X: 3}}, {Inner: Inner{X: 4}}}},
		}
		assertAddedFieldRead(t, in, want)
	})

	t.Run("map", func(t *testing.T) {
		type Inner struct {
			X int32 `parquet:"x"`
		}
		type ValV1 struct {
			Inner Inner `parquet:"inner"`
		}
		type ValV2 struct {
			Inner Inner  `parquet:"inner"`
			Y     *int32 `parquet:"y,optional"`
		}
		type V1 struct {
			M map[string]ValV1 `parquet:"m"`
		}
		type V2 struct {
			M map[string]ValV2 `parquet:"m"`
		}
		in := []V1{
			{},
			{M: map[string]ValV1{"a": {Inner: Inner{X: 1}}}},
			{M: map[string]ValV1{"a": {Inner: Inner{X: 2}}, "b": {Inner: Inner{X: 3}}}},
		}
		want := []V2{
			{M: map[string]ValV2{}},
			{M: map[string]ValV2{"a": {Inner: Inner{X: 1}}}},
			{M: map[string]ValV2{"a": {Inner: Inner{X: 2}}, "b": {Inner: Inner{X: 3}}}},
		}
		assertAddedFieldRead(t, in, want)
	})

	t.Run("optional group", func(t *testing.T) {
		type Inner struct {
			X int32 `parquet:"x"`
		}
		type GV1 struct {
			Inner Inner `parquet:"inner"`
		}
		type GV2 struct {
			Inner Inner  `parquet:"inner"`
			Y     *int32 `parquet:"y,optional"`
		}
		type V1 struct {
			G *GV1 `parquet:"g,optional"`
		}
		type V2 struct {
			G *GV2 `parquet:"g,optional"`
		}
		in := []V1{{}, {G: &GV1{Inner: Inner{X: 8}}}, {}}
		want := []V2{{}, {G: &GV2{Inner: Inner{X: 8}}}, {}}
		assertAddedFieldRead(t, in, want)
	})
}

func TestMissingColumnChunkMatchesRows(t *testing.T) {
	t.Run("zero rows", func(t *testing.T) {
		type Item struct {
			X int32 `parquet:"x"`
		}
		type Item2 struct {
			X int32  `parquet:"x"`
			Y *int32 `parquet:"y,optional"`
		}
		type Src struct {
			S []Item `parquet:"s"`
		}
		type Dst struct {
			S []Item2 `parquet:"s"`
		}
		src := parquet.NewGenericBuffer[Src]()
		assertChunkMatchesRows[Dst](t, src, 1)
	})

	t.Run("repeated sibling", func(t *testing.T) {
		type Inner struct {
			A []int32 `parquet:"a"`
		}
		type Inner2 struct {
			A []int32 `parquet:"a"`
			B *int32  `parquet:"b,optional"`
		}
		type Src struct {
			S *Inner `parquet:"s,optional"`
		}
		type Dst struct {
			S *Inner2 `parquet:"s,optional"`
		}
		one := int32(1)
		src := parquet.NewGenericBuffer[Src]()
		if _, err := src.Write([]Src{
			{},
			{S: &Inner{}},
			{S: &Inner{A: []int32{one, 2, 3}}},
		}); err != nil {
			t.Fatal(err)
		}
		assertChunkMatchesRows[Dst](t, src, 1)
	})

	t.Run("no direct leaf", func(t *testing.T) {
		type Box struct {
			X int32 `parquet:"x"`
		}
		type Item struct {
			Inner Box `parquet:"inner"`
		}
		type Item2 struct {
			Inner Box    `parquet:"inner"`
			Y     *int32 `parquet:"y,optional"`
		}
		type Src struct {
			S []Item `parquet:"s"`
		}
		type Dst struct {
			S []Item2 `parquet:"s"`
		}
		src := parquet.NewGenericBuffer[Src]()
		if _, err := src.Write([]Src{
			{},
			{S: []Item{{Inner: Box{X: 1}}}},
			{S: []Item{{Inner: Box{X: 2}}, {Inner: Box{X: 3}}, {Inner: Box{X: 4}}}},
		}); err != nil {
			t.Fatal(err)
		}
		assertChunkMatchesRows[Dst](t, src, 1)
	})

	t.Run("required beside optional sibling", func(t *testing.T) {
		type Inner struct {
			A *int32 `parquet:"a,optional"`
		}
		type Inner2 struct {
			A *int32 `parquet:"a,optional"`
			B int32  `parquet:"b"`
		}
		type Src struct {
			S *Inner `parquet:"s,optional"`
		}
		type Dst struct {
			S *Inner2 `parquet:"s,optional"`
		}
		v := int32(5)
		src := parquet.NewGenericBuffer[Src]()
		if _, err := src.Write([]Src{
			{},
			{S: &Inner{}},
			{S: &Inner{A: &v}},
		}); err != nil {
			t.Fatal(err)
		}
		assertChunkMatchesRows[Dst](t, src, 1)
	})
}

func assertChunkMatchesRows[Dst any](t *testing.T, src parquet.RowGroup, column int) {
	t.Helper()
	conv, err := parquet.Convert(parquet.SchemaOf(new(Dst)), src.Schema())
	if err != nil {
		t.Fatal(err)
	}
	converted := parquet.ConvertRowGroup(src, conv)
	rows := readConvertedRows(t, converted.Rows())
	var fromRows []parquet.Value
	for _, row := range rows {
		fromRows = append(fromRows, columnValues(row, column)...)
	}
	got := readColumnValues(t, converted.ColumnChunks()[column])
	levelsEqual(t, got, fromRows)

	// One value at a time, so a template batch is not consumed in one read.
	pages := converted.ColumnChunks()[column].Pages()
	t.Cleanup(func() { pages.Close() })
	var single []parquet.Value
	buf := make([]parquet.Value, 1)
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
			single = append(single, buf[:n]...)
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
	levelsEqual(t, single, fromRows)
}

func assertAddedFieldRead[Src, Dst any](t *testing.T, in []Src, want []Dst) {
	t.Helper()
	buf := new(bytes.Buffer)
	if err := parquet.Write(buf, in); err != nil {
		t.Fatal(err)
	}
	file := bytes.NewReader(buf.Bytes())
	r := parquet.NewGenericReader[Dst](file)
	t.Cleanup(func() { r.Close() })
	got := make([]Dst, len(want))
	n, err := r.Read(got)
	if err != nil && err != io.EOF {
		t.Fatalf("read: %v", err)
	}
	if n != len(want) || !reflect.DeepEqual(got[:n], want) {
		t.Fatalf("reader got %+v, want %+v (n=%d err=%v)", got[:n], want, n, err)
	}

	// The same bytes converted as rows, including a second call on the same
	// Conversion after a zero-row call, match the writer of the target schema.
	srcReader := parquet.NewGenericReader[Src](bytes.NewReader(buf.Bytes()))
	t.Cleanup(func() { srcReader.Close() })
	srcRows := make([]parquet.Row, len(in))
	if n, err := srcReader.ReadRows(srcRows); n != len(in) {
		t.Fatalf("source rows: n=%d err=%v", n, err)
	}
	conv, err := parquet.Convert(parquet.SchemaOf(new(Dst)), parquet.SchemaOf(new(Src)))
	if err != nil {
		t.Fatal(err)
	}
	if n, err := conv.Convert(nil); n != 0 || err != nil {
		t.Fatalf("Convert(nil) = %d, %v", n, err)
	}
	if _, err := conv.Convert(srcRows); err != nil {
		t.Fatal(err)
	}
	dstSchema := parquet.SchemaOf(new(Dst))
	decoded := make([]Dst, len(want))
	for i := range srcRows {
		if err := dstSchema.Reconstruct(&decoded[i], srcRows[i]); err != nil {
			t.Fatalf("row %d: %v", i, err)
		}
	}
	if !reflect.DeepEqual(decoded, want) {
		t.Fatalf("Convert got %+v, want %+v", decoded, want)
	}
}
