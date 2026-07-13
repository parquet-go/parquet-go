package parquet

import (
	"bytes"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// planTarget builds a refineTarget over a synthetic sorted key space
// [minKey, maxKey] with numRows rows spread uniformly, and cut functions
// rounded to pageRows-page boundaries (pageRows == 1 gives exact cuts).
func planTarget(name string, minKey, maxKey, numRows, pageRows int64) refineTarget {
	rg := &namedTestRowGroup{name: name, numRows: numRows}
	keyOf := func(row int64) int64 {
		if numRows == 1 {
			return minKey
		}
		return minKey + row*(maxKey-minKey)/(numRows-1)
	}
	// exactCutAbove: smallest row such that all rows >= it have key > k.
	exactCutAbove := func(k int64) int64 {
		row := int64(0)
		for row < numRows && keyOf(row) <= k {
			row++
		}
		return row
	}
	// exactCutBelow: largest row such that all rows < it have key < k.
	exactCutBelow := func(k int64) int64 {
		row := int64(0)
		for row < numRows && keyOf(row) < k {
			row++
		}
		return row
	}
	return refineTarget{
		rowGroup: rg,
		minRow:   Row{ValueOf(minKey).Level(0, 0, 0)},
		maxRow:   Row{ValueOf(maxKey).Level(0, 0, 0)},
		numRows:  numRows,
		cutAbove: func(key Row) int64 {
			// Page rounding pushes the boundary page's rows below the cut...
			// no: cutAbove keeps the page containing the key BELOW the cut,
			// i.e. rounds UP to the page end.
			row := exactCutAbove(key[0].Int64())
			return min(((row+pageRows-1)/pageRows)*pageRows, numRows)
		},
		cutBelow: func(key Row) int64 {
			// cutBelow keeps the page containing the key AT OR ABOVE the cut,
			// i.e. rounds DOWN to the page start.
			row := exactCutBelow(key[0].Int64())
			return (row / pageRows) * pageRows
		},
	}
}

// namedTestRowGroup is a placeholder RowGroup carrying an identity; the
// planner only reads NumRows and stores the reference.
type namedTestRowGroup struct {
	name    string
	numRows int64
}

func (g *namedTestRowGroup) NumRows() int64                  { return g.numRows }
func (g *namedTestRowGroup) ColumnChunks() []ColumnChunk     { return nil }
func (g *namedTestRowGroup) Schema() *Schema                 { return nil }
func (g *namedTestRowGroup) SortingColumns() []SortingColumn { return nil }
func (g *namedTestRowGroup) Rows() Rows                      { return nil }

// describePlan renders a plan compactly for assertions, e.g.
// "A[0:5000) M(A[5000:10000) B) C".
func describePlan(plan []RowGroup) string {
	var describe func(rg RowGroup) string
	describe = func(rg RowGroup) string {
		switch g := rg.(type) {
		case *namedTestRowGroup:
			return g.name
		case *rowRangeRowGroup:
			base := describe(g.base)
			return fmt.Sprintf("%s[%d:%d)", base, g.off, g.off+g.length)
		case *testMergedGroup:
			s := "M("
			for i, p := range g.parts {
				if i > 0 {
					s += " "
				}
				s += describe(p)
			}
			return s + ")"
		default:
			return fmt.Sprintf("%T", rg)
		}
	}
	out := ""
	for i, rg := range plan {
		if i > 0 {
			out += " "
		}
		out += describe(rg)
	}
	return out
}

type testMergedGroup struct {
	namedTestRowGroup
	parts []RowGroup
}

func testMakeMerged(parts []RowGroup) RowGroup {
	return &testMergedGroup{parts: parts}
}

func testCompareRows(a, b Row) int {
	switch {
	case a[0].Int64() < b[0].Int64():
		return -1
	case a[0].Int64() > b[0].Int64():
		return +1
	default:
		return 0
	}
}

func TestRefineSegmentPlan(t *testing.T) {
	// Row groups: 10_000 rows each, uniform keys, exact cuts (pageRows=1)
	// unless stated. Keys chosen so overlaps are a small fraction.
	tests := []struct {
		name    string
		targets []refineTarget
		want    string
	}{
		{
			// A: keys [0, 9999], B: keys [9000, 18999] — boundary overlap.
			// A alone below 9000 (rows [0:9000)), overlap [9000,9999],
			// B alone above 9999.
			name: "two_boundary_overlap",
			targets: []refineTarget{
				planTarget("A", 0, 9999, 10000, 1),
				planTarget("B", 9000, 18999, 10000, 1),
			},
			want: "A[0:9000) M(A[9000:10000) B[0:1000)) B[1000:10000)",
		},
		{
			// Chain: A [0,9999], B [9000,18999], C [18000,27999].
			name: "chain_of_three",
			targets: []refineTarget{
				planTarget("A", 0, 9999, 10000, 1),
				planTarget("B", 9000, 18999, 10000, 1),
				planTarget("C", 18000, 27999, 10000, 1),
			},
			want: "A[0:9000) M(A[9000:10000) B[0:1000)) B[1000:9000) M(B[9000:10000) C[0:1000)) C[1000:10000)",
		},
		{
			// Full containment: B inside A. A is lone on both sides.
			name: "containment",
			targets: []refineTarget{
				planTarget("A", 0, 99990, 10000, 1),
				planTarget("B", 40000, 60000, 5000, 1),
			},
			want: "A[0:4000) M(A[4000:6001) B) A[6001:10000)",
		},
		{
			// Identical ranges: fully overlapping, nothing to refine.
			name: "identical",
			targets: []refineTarget{
				planTarget("A", 0, 9999, 10000, 1),
				planTarget("B", 0, 9999, 10000, 1),
			},
			want: "", // nil plan
		},
		{
			// Overlap leaves lone stretches below the floor: no refinement.
			name: "tiny_lone_stretches",
			targets: []refineTarget{
				planTarget("A", 0, 9999, 1500, 1),    // ~6.7 keys/row
				planTarget("B", 500, 10499, 1500, 1), // A alone in [0,500): ~75 rows < floor
			},
			want: "",
		},
		{
			// Cut functions unavailable on B: only A's stretches slice.
			name: "unavailable_cuts",
			targets: []refineTarget{
				planTarget("A", 0, 9999, 10000, 1),
				func() refineTarget {
					t := planTarget("B", 9000, 18999, 10000, 1)
					t.cutAbove = nil
					t.cutBelow = nil
					return t
				}(),
			},
			want: "A[0:9000) M(A[9000:10000) B)",
		},
		{
			// Page-rounded cuts (1000-row pages): boundaries snap so that the
			// boundary pages stay in the merged region.
			name: "page_rounded",
			targets: []refineTarget{
				planTarget("A", 0, 9999, 10000, 1000),
				planTarget("B", 9000, 18999, 10000, 1000),
			},
			want: "A[0:9000) M(A[9000:10000) B[0:1000)) B[1000:10000)",
		},
		{
			// Touching ranges (A.max == B.min): treated as overlapping; both
			// boundary regions merge around the shared key.
			name: "touching",
			targets: []refineTarget{
				planTarget("A", 0, 10000, 10001, 1),
				planTarget("B", 10000, 20000, 10001, 1),
			},
			want: "A[0:10000) M(A[10000:10001) B[0:1)) B[1:10001)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := refineSegment(tt.targets, testCompareRows, testMakeMerged)
			got := describePlan(plan)
			if got != tt.want {
				t.Fatalf("plan mismatch:\ngot  %q\nwant %q", got, tt.want)
			}
		})
	}
}

// refineOracleRow carries a sort key and provenance (source file, position).
type refineOracleRow struct {
	Key int64  `parquet:"key"`
	Src string `parquet:"src"`
}

// TestMergeRefinementOracle verifies that refined merge plans produce the same
// rows as the unrefined merge: identical multiset (checked via provenance),
// globally sorted by key, and stable within each source (each file's rows
// appear in their original relative order). The interleaving of equal keys
// across files is implementation-defined and not compared.
func TestMergeRefinementOracle(t *testing.T) {
	newFile := func(t *testing.T, src string, keys []int64) *File {
		rows := make([]refineOracleRow, len(keys))
		for i, k := range keys {
			rows[i] = refineOracleRow{Key: k, Src: fmt.Sprintf("%s-%d", src, i)}
		}
		var buf bytes.Buffer
		w := NewGenericWriter[refineOracleRow](&buf,
			SortingWriterConfig(SortingColumns(Ascending("key"))),
			PageBufferSize(512), // small pages: fine-grained cuts
		)
		if _, err := w.Write(rows); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		f, err := OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		if err != nil {
			t.Fatal(err)
		}
		return f
	}

	seqKeys := func(start, n, dupEvery int64) []int64 {
		keys := make([]int64, n)
		for i := range keys {
			keys[i] = start + int64(i)
			if dupEvery > 0 && int64(i)%dupEvery == 0 && i > 0 {
				keys[i] = keys[i-1] // duplicate across adjacent rows
			}
		}
		return keys
	}

	scenarios := []struct {
		name       string
		files      map[string][]int64
		wantRefine bool
	}{
		{
			name: "chain_boundary_overlap",
			files: map[string][]int64{
				"a": seqKeys(0, 5000, 0),
				"b": seqKeys(4800, 5000, 0),
				"c": seqKeys(9600, 5000, 0),
			},
			wantRefine: true,
		},
		{
			name: "containment",
			files: map[string][]int64{
				"outer": seqKeys(0, 8000, 0),
				"inner": seqKeys(3000, 2000, 0),
			},
			wantRefine: true,
		},
		{
			name: "duplicates_at_boundaries",
			files: map[string][]int64{
				"a": append(seqKeys(0, 4000, 7), 4000, 4000, 4000),
				"b": append([]int64{4000, 4000}, seqKeys(4001, 4000, 5)...),
			},
			wantRefine: true,
		},
		{
			name: "full_overlap_no_refinement",
			files: map[string][]int64{
				"a": seqKeys(0, 3000, 0),
				"b": seqKeys(1, 3000, 0),
			},
			wantRefine: false,
		},
	}

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			var rowGroups []RowGroup
			for _, name := range sortedKeys(sc.files) {
				rowGroups = append(rowGroups, newFile(t, name, sc.files[name]).RowGroups()[0])
			}

			merge := func(refine bool) (RowGroup, []refineOracleRow) {
				defer func(prev bool) { disableMergeRefinement = prev }(disableMergeRefinement)
				disableMergeRefinement = !refine
				m, err := MergeRowGroups(rowGroups, SortingRowGroupConfig(SortingColumns(Ascending("key"))))
				if err != nil {
					t.Fatal(err)
				}
				rows := m.Rows()
				defer rows.Close()
				var out []refineOracleRow
				buf := make([]Row, 97)
				for {
					n, err := rows.ReadRows(buf)
					for _, row := range buf[:n] {
						out = append(out, refineOracleRow{Key: row[0].Int64(), Src: row[1].String()})
					}
					if err == io.EOF {
						break
					}
					if err != nil {
						t.Fatal(err)
					}
					if n == 0 {
						t.Fatal("no progress")
					}
				}
				return m, out
			}

			refinedGroup, refined := merge(true)
			_, reference := merge(false)

			// Structure: refinement fired (or not) as expected.
			hasRange := false
			if seg, ok := refinedGroup.(*sortedSegmentRowGroup); ok {
				for _, s := range seg.segments {
					if _, ok := s.(*rowRangeRowGroup); ok {
						hasRange = true
					}
				}
			}
			if hasRange != sc.wantRefine {
				t.Fatalf("refinement fired = %v, want %v (merged type %T)", hasRange, sc.wantRefine, refinedGroup)
			}

			// Same number of rows, globally sorted.
			if len(refined) != len(reference) {
				t.Fatalf("refined %d rows, reference %d", len(refined), len(reference))
			}
			for i := 1; i < len(refined); i++ {
				if refined[i].Key < refined[i-1].Key {
					t.Fatalf("refined output not sorted at %d: %d < %d", i, refined[i].Key, refined[i-1].Key)
				}
			}

			// Multiset equality via provenance.
			seen := make(map[refineOracleRow]int, len(reference))
			for _, r := range reference {
				seen[r]++
			}
			for _, r := range refined {
				seen[r]--
				if seen[r] < 0 {
					t.Fatalf("row %+v appears more times in refined than reference", r)
				}
			}

			// Per-source stability: each file's rows appear in original order.
			lastPos := make(map[string]int)
			for _, r := range refined {
				sep := strings.LastIndexByte(r.Src, '-')
				src := r.Src[:sep]
				pos, err := strconv.Atoi(r.Src[sep+1:])
				if err != nil {
					t.Fatalf("bad provenance %q: %v", r.Src, err)
				}
				if last, ok := lastPos[src]; ok && pos < last {
					t.Fatalf("source %q rows out of order: position %d after %d", src, pos, last)
				}
				lastPos[src] = pos
			}
		})
	}
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// TestMergeRefinementSkippedWithDedup verifies that DropDuplicatedRows disables
// refinement (which duplicate of an equal-key run survives depends on tie
// interleaving, which refinement does not preserve) while deduplication remains
// correct.
func TestMergeRefinementSkippedWithDedup(t *testing.T) {
	newFile := func(keys []int64) *File {
		rows := make([]refineOracleRow, len(keys))
		for i, k := range keys {
			rows[i] = refineOracleRow{Key: k, Src: fmt.Sprintf("s-%d", i)}
		}
		var buf bytes.Buffer
		w := NewGenericWriter[refineOracleRow](&buf,
			SortingWriterConfig(SortingColumns(Ascending("key"))), PageBufferSize(512))
		if _, err := w.Write(rows); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		f, err := OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		if err != nil {
			t.Fatal(err)
		}
		return f
	}

	keysA := make([]int64, 5000)
	keysB := make([]int64, 5000)
	for i := range keysA {
		keysA[i] = int64(i)
		keysB[i] = int64(i + 4500) // overlap [4500, 4999] with duplicates
	}
	fa, fb := newFile(keysA), newFile(keysB)

	m, err := MergeRowGroups(
		[]RowGroup{fa.RowGroups()[0], fb.RowGroups()[0]},
		SortingRowGroupConfig(SortingColumns(Ascending("key")), DropDuplicatedRows(true)),
	)
	if err != nil {
		t.Fatal(err)
	}
	if seg, ok := m.(*sortedSegmentRowGroup); ok {
		for _, s := range seg.segments {
			if _, isRange := s.(*rowRangeRowGroup); isRange {
				t.Fatal("refinement must not fire when dropping duplicated rows")
			}
		}
	}

	rows := m.Rows()
	defer rows.Close()
	var keys []int64
	buf := make([]Row, 64)
	for {
		n, err := rows.ReadRows(buf)
		for _, row := range buf[:n] {
			keys = append(keys, row[0].Int64())
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	// Deduplicated union of [0,4999] and [4500,9499] is [0,9499].
	if len(keys) != 9500 {
		t.Fatalf("deduplicated to %d rows, want 9500", len(keys))
	}
	for i, k := range keys {
		if k != int64(i) {
			t.Fatalf("key %d at position %d", k, i)
		}
	}
}

// refineWriteRow includes a repeated column so bloom sizing must handle
// inexact range-view value counts.
type refineWriteRow struct {
	Key  int64   `parquet:"key"`
	Src  string  `parquet:"src"`
	List []int32 `parquet:"list,list"`
}

// TestMergeRefinementWrite writes refined merges through WriteRowGroup and
// verifies that the streamed regions take the column-oriented fast paths, that
// the output matches the row-path reference, and that a bloom filter on a
// repeated column is built exactly despite range views only knowing an upper
// bound of their value count.
func TestMergeRefinementWrite(t *testing.T) {
	newFile := func(src string, start, n int64) *File {
		rows := make([]refineWriteRow, n)
		for i := range rows {
			rows[i] = refineWriteRow{Key: start + int64(i), Src: fmt.Sprintf("%s-%d", src, i)}
			for j := range int(start+int64(i)) % 3 {
				rows[i].List = append(rows[i].List, int32(j))
			}
		}
		var buf bytes.Buffer
		w := NewGenericWriter[refineWriteRow](&buf,
			SortingWriterConfig(SortingColumns(Ascending("key"))), PageBufferSize(512))
		if _, err := w.Write(rows); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		f, err := OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		if err != nil {
			t.Fatal(err)
		}
		return f
	}

	// Chain with ~4% boundary overlaps.
	files := []*File{
		newFile("a", 0, 5000),
		newFile("b", 4800, 5000),
		newFile("c", 9600, 5000),
	}

	write := func(refine bool, opts ...WriterOption) ([]byte, int64) {
		defer func(prev bool) { disableMergeRefinement = prev }(disableMergeRefinement)
		disableMergeRefinement = !refine
		rgs := make([]RowGroup, len(files))
		for i, f := range files {
			rgs[i] = f.RowGroups()[0]
		}
		m, err := MergeRowGroups(rgs,
			&RowGroupConfig{Schema: rgs[0].Schema()},
			SortingRowGroupConfig(SortingColumns(Ascending("key"))),
		)
		if err != nil {
			t.Fatal(err)
		}
		var dst bytes.Buffer
		opts = append(opts, SortingWriterConfig(SortingColumns(Ascending("key"))))
		w := NewGenericWriter[refineWriteRow](&dst, opts...)
		before := reencodePathCounter.Load()
		if _, err := w.WriteRowGroup(m); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		return dst.Bytes(), reencodePathCounter.Load() - before
	}

	refined, l3 := write(true)
	reference, _ := write(false)

	if l3 == 0 {
		t.Fatal("expected refined merge to take the column-oriented fast path for streamed regions")
	}

	readAll := func(b []byte) []refineWriteRow {
		r := NewGenericReader[refineWriteRow](bytes.NewReader(b))
		defer r.Close()
		out := make([]refineWriteRow, 16000)
		n, _ := r.Read(out)
		return out[:n]
	}
	gotR := readAll(refined)
	gotRef := readAll(reference)
	if len(gotR) != len(gotRef) {
		t.Fatalf("refined write has %d rows, reference %d", len(gotR), len(gotRef))
	}
	for i := 1; i < len(gotR); i++ {
		if gotR[i].Key < gotR[i-1].Key {
			t.Fatalf("refined write output not sorted at %d", i)
		}
	}

	t.Run("bloom_on_repeated_column", func(t *testing.T) {
		out, _ := write(true, BloomFilters(SplitBlockFilter(10, "list", "list", "element")))
		f, err := OpenFile(bytes.NewReader(out), int64(len(out)))
		if err != nil {
			t.Fatal(err)
		}
		checked := 0
		for _, rg := range f.RowGroups() {
			var listChunk ColumnChunk
			for _, c := range rg.ColumnChunks() {
				if c.Type().Kind() == Int32 {
					listChunk = c
				}
			}
			bf := listChunk.BloomFilter()
			if bf == nil {
				continue // row groups whose column had no filter written
			}
			ok, err := bf.Check(ValueOf(int32(0)))
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				t.Fatal("bloom filter misses value 0, present in every non-empty list")
			}
			checked++
		}
		if checked == 0 {
			t.Fatal("no bloom filters found in refined output")
		}
	})
}
