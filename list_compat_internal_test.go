package parquet

import (
	"bytes"
	"io"
	"reflect"
	"slices"
	"testing"
)

func TestListLogicalElementRules(t *testing.T) {
	pyarrow := listNode{Group{"list": Repeated(Group{"item": Optional(String())})}}
	elem, elemName, ok := listLogicalElement(pyarrow, "answers")
	if !ok || !elem.Leaf() || !elem.Optional() || elem.(Field).Name() != "item" || elemName != "item" {
		t.Fatalf("rule 5 item: elem=%v name=%q ok=%v", elem, elemName, ok)
	}

	standard := listNode{Group{"list": Repeated(Group{"element": String()})}}
	elem, _, ok = listLogicalElement(standard, "answers")
	if !ok || !elem.Leaf() || elem.(Field).Name() != "element" || elem.Optional() {
		t.Fatalf("standard element: %#v ok=%v", elem, ok)
	}

	primitive := listNode{Group{"values": Repeated(Int(32))}}
	elem, _, ok = listLogicalElement(primitive, "nums")
	if !ok || !elem.Leaf() || !elem.Required() || elem.Repeated() {
		t.Fatalf("rule 1: leaf=%v required=%v repeated=%v", elem.Leaf(), elem.Required(), elem.Repeated())
	}

	tuple := listNode{Group{"element": Repeated(Group{
		"str": String(),
		"num": Int(32),
	})}}
	elem, _, ok = listLogicalElement(tuple, "pairs")
	if !ok || elem.Leaf() || elem.Repeated() || len(elem.Fields()) != 2 {
		t.Fatalf("rule 2: %+v", elem)
	}

	arrayTuple := listNode{Group{"array": Repeated(Group{"str": String()})}}
	elem, _, ok = listLogicalElement(arrayTuple, "my_list")
	if !ok || elem.Leaf() || len(elem.Fields()) != 1 || elem.Fields()[0].Name() != "str" {
		t.Fatalf("rule 4 array: unwrapped or renamed: %v", elem)
	}

	namedTuple := listNode{Group{"my_list_tuple": Repeated(Group{"str": String()})}}
	elem, _, ok = listLogicalElement(namedTuple, "my_list")
	if !ok || elem.Leaf() || elem.Fields()[0].Name() != "str" {
		t.Fatalf("rule 4 _tuple: %v", elem)
	}

	// A single field whose repeated group is not "array" or "<name>_tuple"
	// stays unwrapped, even when the inner field is a group.
	notTuple := listNode{Group{"element": Repeated(Group{"str": String()})}}
	elem, _, ok = listLogicalElement(notTuple, "my_list")
	if !ok || !elem.Leaf() || elem.(Field).Name() != "str" {
		t.Fatalf("rule 5 renamed inner field: %v", elem)
	}

	nested := listNode{Group{"array": Repeated(listNode{Group{"array": Repeated(Int(32))}})}}
	elem, elemName, ok = listLogicalElement(nested, "matrix")
	if !ok || elem.Leaf() || !isList(elem) || elem.Repeated() || elemName != "array" {
		t.Fatalf("rule 3: leaf=%v list=%v repeated=%v name=%q", elem.Leaf(), isList(elem), elem.Repeated(), elemName)
	}
}

func TestNormalizeListsPreservesLeafLevels(t *testing.T) {
	roots := []Node{
		Group{"answers": Optional(listNode{Group{"list": Repeated(Group{"item": Optional(String())})}})},
		Group{"answers": Optional(listNode{Group{"list": Repeated(Group{"item": String()})}})},
		Group{"nums": Optional(listNode{Group{"values": Repeated(Int(32))}})},
		Group{"pairs": Optional(listNode{Group{"element": Repeated(Group{"str": String(), "num": Int(32)})}})},
		Group{"my_list": Optional(listNode{Group{"array": Repeated(Group{"str": String()})}})},
		Group{"my_list": Optional(listNode{Group{"my_list_tuple": Repeated(Group{"str": String()})}})},
		Group{"my_list": Optional(listNode{Group{"element": Repeated(Group{"str": String()})}})},
		Group{"fav": Optional(listNode{Group{"bag": Repeated(Group{"array_element": Optional(String())})}})},
		Group{"matrix": Optional(listNode{Group{"array": Repeated(listNode{Group{"array": Repeated(Int(32))}})}})},
		Group{"matrix": Optional(listNode{Group{"list": Repeated(Group{"item": Optional(listNode{Group{"list": Repeated(Group{"item": Optional(Int(32))})}})})}})},
		Group{
			"answers": Optional(listNode{Group{"list": Repeated(Group{"item": Optional(String())})}}),
			"passages": Optional(Group{
				"is_selected":  Optional(listNode{Group{"list": Repeated(Group{"item": Optional(Int(32))})}}),
				"passage_text": Optional(listNode{Group{"list": Repeated(Group{"item": Optional(String())})}}),
			}),
			"query": Optional(String()),
		},
	}
	for _, root := range roots {
		norm := normalizeLists(root, "schema")
		sameLeafLevels(t, root, norm)
	}
}

func sameLeafLevels(t *testing.T, original, normalized Node) {
	t.Helper()
	var want, got []leafColumn
	forEachLeafColumnOf(original, func(leaf leafColumn) { want = append(want, leaf) })
	forEachLeafColumnOf(normalized, func(leaf leafColumn) { got = append(got, leaf) })
	if len(want) != len(got) {
		t.Fatalf("leaf count %d, want %d\noriginal:\n%s", len(got), len(want), original)
	}
	for i := range want {
		if got[i].columnIndex != want[i].columnIndex ||
			got[i].maxDefinitionLevel != want[i].maxDefinitionLevel ||
			got[i].maxRepetitionLevel != want[i].maxRepetitionLevel {
			t.Fatalf("leaf %d got index=%d def=%d rep=%d path=%v, want index=%d def=%d rep=%d path=%v\noriginal:\n%s",
				i,
				got[i].columnIndex, got[i].maxDefinitionLevel, got[i].maxRepetitionLevel, got[i].path,
				want[i].columnIndex, want[i].maxDefinitionLevel, want[i].maxRepetitionLevel, want[i].path,
				original)
		}
	}
}

func TestListWriterUsesElementName(t *testing.T) {
	type row struct {
		Answers []string `parquet:"answers,list"`
	}
	var buf bytes.Buffer
	w := NewGenericWriter[row](&buf)
	if _, err := w.Write([]row{{Answers: []string{"a", "b"}}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	f := openTestFile(t, buf.Bytes())
	if _, ok := f.Schema().Lookup("answers", "list", "element"); !ok {
		t.Fatalf("writer schema:\n%s", f.Schema())
	}
	if _, ok := f.Schema().Lookup("answers", "list", "item"); ok {
		t.Fatalf("writer renamed the element to item:\n%s", f.Schema())
	}
	got := readGeneric[row](t, f)
	want := []row{{Answers: []string{"a", "b"}}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v", got)
	}
}

func TestReadLegacyLists(t *testing.T) {
	t.Run("pyarrow item", func(t *testing.T) {
		// optional group v (LIST) { repeated group list { optional binary item (STRING); } }
		root := Group{"v": Optional(listNode{Group{"list": Repeated(Group{"item": Optional(String())})}})}
		schema := NewSchema("schema", root)
		leaf := mustLeaf(t, schema, "v", "list", "item")
		if _, ok := schema.Lookup("v", "list", "element"); ok {
			t.Fatal("fixture used the standard element name")
		}
		if leaf.MaxDefinitionLevel != 3 || leaf.MaxRepetitionLevel != 1 {
			t.Fatalf("def=%d rep=%d\n%s", leaf.MaxDefinitionLevel, leaf.MaxRepetitionLevel, schema)
		}
		col := leaf.ColumnIndex
		rows := []Row{
			{NullValue().Level(0, 0, col)},
			{NullValue().Level(0, 1, col)},
			{
				ByteArrayValue([]byte("a")).Level(0, 3, col),
				ByteArrayValue([]byte("b")).Level(1, 3, col),
			},
			{
				NullValue().Level(0, 2, col),
				ByteArrayValue([]byte("c")).Level(1, 3, col),
			},
			{ByteArrayValue([]byte("")).Level(0, 3, col)},
		}
		f := writeTestFile(t, root, rows)
		sameLeafLevels(t, f.Schema(), normalizeLists(f.Schema(), f.Schema().Name()))

		type optionalStrings struct {
			V []*string `parquet:"v,optional,list"`
		}
		sp := func(s string) *string { return &s }
		want := []optionalStrings{
			{},
			{V: []*string{}},
			{V: []*string{sp("a"), sp("b")}},
			{V: []*string{nil, sp("c")}},
			{V: []*string{sp("")}},
		}
		got := readGeneric[optionalStrings](t, f)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("generic reader got %#v", got)
		}
		if got[1].V == nil {
			t.Fatal("empty list became nil")
		}
		classic := readClassic[optionalStrings](t, f)
		if !reflect.DeepEqual(classic, want) {
			t.Fatalf("reader got %#v", classic)
		}

		// Required Go strings keep present values. A null element has no
		// representation and becomes the zero string.
		type requiredStrings struct {
			V []string `parquet:"v,optional,list"`
		}
		gotReq := readGeneric[requiredStrings](t, f)
		wantReq := []requiredStrings{
			{},
			{V: []string{}},
			{V: []string{"a", "b"}},
			{V: []string{"", "c"}},
			{V: []string{""}},
		}
		if !reflect.DeepEqual(gotReq, wantReq) {
			t.Fatalf("required elements got %#v", gotReq)
		}
	})

	t.Run("pyarrow required item", func(t *testing.T) {
		root := Group{"v": Optional(listNode{Group{"list": Repeated(Group{"item": String()})}})}
		schema := NewSchema("schema", root)
		leaf := mustLeaf(t, schema, "v", "list", "item")
		if leaf.MaxDefinitionLevel != 2 || leaf.MaxRepetitionLevel != 1 {
			t.Fatalf("def=%d rep=%d\n%s", leaf.MaxDefinitionLevel, leaf.MaxRepetitionLevel, schema)
		}
		col := leaf.ColumnIndex
		f := writeTestFile(t, root, []Row{
			{NullValue().Level(0, 0, col)},
			{NullValue().Level(0, 1, col)},
			{
				ByteArrayValue([]byte("x")).Level(0, 2, col),
				ByteArrayValue([]byte("y")).Level(1, 2, col),
			},
		})
		type row struct {
			V []string `parquet:"v,list"`
		}
		got := readGeneric[row](t, f)
		// A required Go list cannot represent a null list, so both the null
		// and the empty file lists reconstruct as empty slices.
		want := []row{{V: []string{}}, {V: []string{}}, {V: []string{"x", "y"}}}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %#v", got)
		}
	})

	t.Run("rule 1 repeated primitive", func(t *testing.T) {
		root := Group{"nums": Optional(listNode{Group{"values": Repeated(Int(32))}})}
		schema := NewSchema("schema", root)
		if _, ok := schema.Lookup("nums", "list", "element"); ok {
			t.Fatal("fixture was rewritten to the standard list shape")
		}
		leaf := mustLeaf(t, schema, "nums", "values")
		if leaf.MaxDefinitionLevel != 2 || leaf.MaxRepetitionLevel != 1 {
			t.Fatalf("def=%d rep=%d\n%s", leaf.MaxDefinitionLevel, leaf.MaxRepetitionLevel, schema)
		}
		col := leaf.ColumnIndex
		f := writeTestFile(t, root, []Row{
			{NullValue().Level(0, 0, col)},
			{NullValue().Level(0, 1, col)},
			{Int32Value(10).Level(0, 2, col), Int32Value(20).Level(1, 2, col)},
		})
		type row struct {
			Nums []int32 `parquet:"nums,optional,list"`
		}
		got := readGeneric[row](t, f)
		want := []row{{}, {Nums: []int32{}}, {Nums: []int32{10, 20}}}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %#v", got)
		}
	})

	t.Run("rule 2 repeated group", func(t *testing.T) {
		root := Group{"pairs": Optional(listNode{Group{"element": Repeated(Group{
			"str": String(),
			"num": Int(32),
		})}})}
		schema := NewSchema("schema", root)
		num := mustLeaf(t, schema, "pairs", "element", "num")
		str := mustLeaf(t, schema, "pairs", "element", "str")
		if num.ColumnIndex != 0 || str.ColumnIndex != 1 || num.MaxDefinitionLevel != 2 {
			t.Fatalf("num=%+v str=%+v\n%s", num, str, schema)
		}
		f := writeTestFile(t, root, []Row{
			{
				NullValue().Level(0, 0, num.ColumnIndex),
				NullValue().Level(0, 0, str.ColumnIndex),
			},
			{
				Int32Value(1).Level(0, 2, num.ColumnIndex),
				Int32Value(2).Level(1, 2, num.ColumnIndex),
				ByteArrayValue([]byte("a")).Level(0, 2, str.ColumnIndex),
				ByteArrayValue([]byte("b")).Level(1, 2, str.ColumnIndex),
			},
		})
		type pair struct {
			Str string `parquet:"str"`
			Num int32  `parquet:"num"`
		}
		type row struct {
			Pairs []pair `parquet:"pairs,optional,list"`
		}
		got := readGeneric[row](t, f)
		want := []row{
			{},
			{Pairs: []pair{{Str: "a", Num: 1}, {Str: "b", Num: 2}}},
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %#v", got)
		}
	})

	t.Run("rule 4 array and name_tuple", func(t *testing.T) {
		for _, repeatedName := range []string{"array", "my_list_tuple"} {
			t.Run(repeatedName, func(t *testing.T) {
				root := Group{"my_list": Optional(listNode{Group{repeatedName: Repeated(Group{"str": String()})}})}
				schema := NewSchema("schema", root)
				leaf := mustLeaf(t, schema, "my_list", repeatedName, "str")
				if leaf.MaxDefinitionLevel != 2 {
					t.Fatalf("def=%d\n%s", leaf.MaxDefinitionLevel, schema)
				}
				f := writeTestFile(t, root, []Row{
					{ByteArrayValue([]byte("one")).Level(0, 2, leaf.ColumnIndex)},
					{
						ByteArrayValue([]byte("two")).Level(0, 2, leaf.ColumnIndex),
						ByteArrayValue([]byte("three")).Level(1, 2, leaf.ColumnIndex),
					},
				})
				type tuple struct {
					Str string `parquet:"str"`
				}
				type row struct {
					MyList []tuple `parquet:"my_list,optional,list"`
				}
				got := readGeneric[row](t, f)
				want := []row{
					{MyList: []tuple{{Str: "one"}}},
					{MyList: []tuple{{Str: "two"}, {Str: "three"}}},
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("got %#v", got)
				}
			})
		}
	})

	t.Run("rule 5 renamed element", func(t *testing.T) {
		// Spec example: repeated group "element" { required binary str }.
		// The inner field is the element, not a one-field tuple.
		root := Group{"my_list": Optional(listNode{Group{"element": Repeated(Group{"str": String()})}})}
		schema := NewSchema("schema", root)
		leaf := mustLeaf(t, schema, "my_list", "element", "str")
		f := writeTestFile(t, root, []Row{
			{ByteArrayValue([]byte("s")).Level(0, leaf.MaxDefinitionLevel, leaf.ColumnIndex)},
		})
		type row struct {
			MyList []string `parquet:"my_list,optional,list"`
		}
		got := readGeneric[row](t, f)
		if !reflect.DeepEqual(got, []row{{MyList: []string{"s"}}}) {
			t.Fatalf("got %#v\n%s", got, schema)
		}
	})

	t.Run("athena bag array_element", func(t *testing.T) {
		root := Group{"fav": Optional(listNode{Group{"bag": Repeated(Group{"array_element": Optional(String())})}})}
		schema := NewSchema("schema", root)
		leaf := mustLeaf(t, schema, "fav", "bag", "array_element")
		col := leaf.ColumnIndex
		f := writeTestFile(t, root, []Row{{
			ByteArrayValue([]byte("a")).Level(0, leaf.MaxDefinitionLevel, col),
			ByteArrayValue([]byte("b")).Level(1, leaf.MaxDefinitionLevel, col),
		}})
		type row struct {
			Fav []string `parquet:"fav,optional,list"`
		}
		got := readGeneric[row](t, f)
		if !reflect.DeepEqual(got, []row{{Fav: []string{"a", "b"}}}) {
			t.Fatalf("got %#v", got)
		}
	})

	t.Run("pyarrow nested item", func(t *testing.T) {
		root := Group{"matrix": Optional(listNode{Group{"list": Repeated(Group{
			"item": Optional(listNode{Group{"list": Repeated(Group{"item": Optional(Int(32))})}}),
		})}})}
		schema := NewSchema("schema", root)
		leaf := mustLeaf(t, schema, "matrix", "list", "item", "list", "item")
		if _, ok := schema.Lookup("matrix", "list", "element", "list", "element"); ok {
			t.Fatal("fixture used the standard element name")
		}
		if leaf.MaxDefinitionLevel != 5 || leaf.MaxRepetitionLevel != 2 {
			t.Fatalf("def=%d rep=%d\n%s", leaf.MaxDefinitionLevel, leaf.MaxRepetitionLevel, schema)
		}
		col := leaf.ColumnIndex
		// [[1, 2], [3]]
		f := writeTestFile(t, root, []Row{{
			Int32Value(1).Level(0, 5, col),
			Int32Value(2).Level(2, 5, col),
			Int32Value(3).Level(1, 5, col),
		}})
		type row struct {
			Matrix [][]int32 `parquet:"matrix,list"`
		}
		got := readGeneric[row](t, f)
		want := []row{{Matrix: [][]int32{{1, 2}, {3}}}}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %#v", got)
		}
	})

	t.Run("rule 3 nested 2-level", func(t *testing.T) {
		root := Group{"matrix": Optional(listNode{Group{
			"array": Repeated(listNode{Group{"array": Repeated(Int(32))}}),
		}})}
		schema := NewSchema("schema", root)
		leaf := mustLeaf(t, schema, "matrix", "array", "array")
		if leaf.MaxDefinitionLevel != 3 || leaf.MaxRepetitionLevel != 2 {
			t.Fatalf("def=%d rep=%d\n%s", leaf.MaxDefinitionLevel, leaf.MaxRepetitionLevel, schema)
		}
		col := leaf.ColumnIndex
		f := writeTestFile(t, root, []Row{{
			Int32Value(1).Level(0, 3, col),
			Int32Value(2).Level(2, 3, col),
			Int32Value(3).Level(1, 3, col),
		}})
		type row struct {
			Matrix [][]int32 `parquet:"matrix,optional,list"`
		}
		got := readGeneric[row](t, f)
		if !reflect.DeepEqual(got, []row{{Matrix: [][]int32{{1, 2}, {3}}}}) {
			t.Fatalf("got %#v\n%s", got, schema)
		}
	})

	t.Run("pyarrow group of lists", func(t *testing.T) {
		// Shape from microsoft/ms_marco: the middle group stays "list" and the
		// element is "item".
		root := Group{
			"answers": Optional(listNode{Group{"list": Repeated(Group{"item": Optional(String())})}}),
			"passages": Optional(Group{
				"is_selected":  Optional(listNode{Group{"list": Repeated(Group{"item": Optional(Int(32))})}}),
				"passage_text": Optional(listNode{Group{"list": Repeated(Group{"item": Optional(String())})}}),
			}),
			"query": Optional(String()),
		}
		schema := NewSchema("schema", root)
		cols := schema.Columns()
		wantCols := [][]string{
			{"answers", "list", "item"},
			{"passages", "is_selected", "list", "item"},
			{"passages", "passage_text", "list", "item"},
			{"query"},
		}
		if len(cols) != len(wantCols) {
			t.Fatalf("columns %v\n%s", cols, schema)
		}
		for i := range wantCols {
			if !slices.Equal(cols[i], wantCols[i]) {
				t.Fatalf("column %d = %v, want %v\n%s", i, cols[i], wantCols[i], schema)
			}
		}
		answers := mustLeaf(t, schema, "answers", "list", "item")
		selected := mustLeaf(t, schema, "passages", "is_selected", "list", "item")
		text := mustLeaf(t, schema, "passages", "passage_text", "list", "item")
		query := mustLeaf(t, schema, "query")
		if answers.MaxDefinitionLevel != 3 || selected.MaxDefinitionLevel != 4 || query.MaxDefinitionLevel != 1 {
			t.Fatalf("answers def=%d selected def=%d text def=%d query def=%d\n%s",
				answers.MaxDefinitionLevel, selected.MaxDefinitionLevel, text.MaxDefinitionLevel, query.MaxDefinitionLevel, schema)
		}
		f := writeTestFile(t, root, []Row{
			{
				ByteArrayValue([]byte("alpha")).Level(0, answers.MaxDefinitionLevel, answers.ColumnIndex),
				ByteArrayValue([]byte("beta")).Level(1, answers.MaxDefinitionLevel, answers.ColumnIndex),
				Int32Value(1).Level(0, selected.MaxDefinitionLevel, selected.ColumnIndex),
				Int32Value(0).Level(1, selected.MaxDefinitionLevel, selected.ColumnIndex),
				ByteArrayValue([]byte("p0")).Level(0, text.MaxDefinitionLevel, text.ColumnIndex),
				ByteArrayValue([]byte("p1")).Level(1, text.MaxDefinitionLevel, text.ColumnIndex),
				ByteArrayValue([]byte("what")).Level(0, query.MaxDefinitionLevel, query.ColumnIndex),
			},
			{
				NullValue().Level(0, 0, answers.ColumnIndex),
				NullValue().Level(0, 0, selected.ColumnIndex),
				NullValue().Level(0, 0, text.ColumnIndex),
				NullValue().Level(0, 0, query.ColumnIndex),
			},
		})
		sameLeafLevels(t, f.Schema(), normalizeLists(f.Schema(), f.Schema().Name()))

		type passages struct {
			IsSelected  []int32  `parquet:"is_selected,list"`
			PassageText []string `parquet:"passage_text,list"`
		}
		type row struct {
			Answers  []string  `parquet:"answers,list"`
			Passages *passages `parquet:"passages"`
			Query    *string   `parquet:"query"`
		}
		q := "what"
		want := []row{
			{
				Answers: []string{"alpha", "beta"},
				Passages: &passages{
					IsSelected:  []int32{1, 0},
					PassageText: []string{"p0", "p1"},
				},
				Query: &q,
			},
			{Answers: []string{}},
		}
		got := readGeneric[row](t, f)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("generic reader got %#v", got)
		}
		classic := readClassic[row](t, f)
		if !reflect.DeepEqual(classic, want) {
			t.Fatalf("reader got %#v", classic)
		}
	})
}

func mustLeaf(t *testing.T, schema *Schema, path ...string) LeafColumn {
	t.Helper()
	leaf, ok := schema.Lookup(path...)
	if !ok {
		t.Fatalf("missing column %v\n%s", path, schema)
	}
	return leaf
}

func writeTestFile(t *testing.T, root Node, rows []Row) *File {
	t.Helper()
	schema := NewSchema("schema", root)
	var buf bytes.Buffer
	w := NewWriter(&buf, schema)
	if _, err := w.WriteRows(rows); err != nil {
		t.Fatalf("write rows: %v\nschema:\n%s", err, schema)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return openTestFile(t, buf.Bytes())
}

func openTestFile(t *testing.T, data []byte) *File {
	t.Helper()
	f, err := OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func readGeneric[T any](t *testing.T, f *File) []T {
	t.Helper()
	r := NewGenericReader[T](bytes.NewReader(fileBytes(t, f)))
	t.Cleanup(func() { r.Close() })
	rows := make([]T, 32)
	n, err := r.Read(rows)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if n == len(rows) {
		t.Fatal("row buffer filled")
	}
	return slices.Clone(rows[:n])
}

func readClassic[T any](t *testing.T, f *File) []T {
	t.Helper()
	r := NewReader(bytes.NewReader(fileBytes(t, f)))
	t.Cleanup(func() { r.Close() })
	var out []T
	for {
		var row T
		err := r.Read(&row)
		if err == io.EOF {
			return out
		}
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, row)
	}
}

func fileBytes(t *testing.T, f *File) []byte {
	t.Helper()
	buf := make([]byte, f.Size())
	n, err := f.ReadAt(buf, 0)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if int64(n) != f.Size() {
		t.Fatalf("read %d of %d", n, f.Size())
	}
	return buf
}
