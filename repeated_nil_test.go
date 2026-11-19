package parquet_test

import (
	"bytes"
	"cmp"
	"fmt"
	"io"
	"reflect"
	"slices"
	"sort"
	"testing"

	"github.com/parquet-go/parquet-go"
)

// A nil pointer in a required repeated field has no definition level of its
// own. A non-repeated required leaf already stores that nil as a physical
// zero; the repeated column has to do the same or later values slide into
// the wrong row. Optional nulls stay null.
func TestNilRequiredRepeatedPointerPreservesRows(t *testing.T) {
	five, seven := int64(5), int64(7)
	zero := int64(0)

	t.Run("rows", func(t *testing.T) {
		type inputRow struct{ Key []*int64 }
		type outputRow struct{ Key []int64 }

		input := []inputRow{
			{Key: []*int64{nil, &five}},
			{Key: []*int64{&seven}},
			{Key: []*int64{nil, &zero, nil}},
			{Key: []*int64{nil}},
			{Key: []*int64{}},
			{Key: []*int64{&seven, nil, &five}},
		}
		var buf bytes.Buffer
		w := parquet.NewGenericWriter[inputRow](&buf)
		if _, err := w.Write(input); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		rows, err := parquet.Read[outputRow](bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		if err != nil {
			t.Fatal(err)
		}
		got := make([][]int64, len(rows))
		for i := range rows {
			got[i] = rows[i].Key
		}
		want := [][]int64{{0, 5}, {7}, {0, 0, 0}, {0}, {}, {7, 0, 5}}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %#v, want %#v", got, want)
		}
	})

	t.Run("dictionary", func(t *testing.T) {
		type inputRow struct {
			Key []*int64 `parquet:",dict"`
		}
		type outputRow struct {
			Key []int64 `parquet:",dict"`
		}
		var buf bytes.Buffer
		w := parquet.NewGenericWriter[inputRow](&buf)
		if _, err := w.Write([]inputRow{{Key: []*int64{nil, &five}}, {Key: []*int64{&seven}}}); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		rows, err := parquet.Read[outputRow](bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		if err != nil {
			t.Fatal(err)
		}
		got := make([][]int64, len(rows))
		for i := range rows {
			got[i] = rows[i].Key
		}
		want := [][]int64{{0, 5}, {7}}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %#v, want %#v", got, want)
		}
	})

	t.Run("struct element", func(t *testing.T) {
		type inner struct{ V int64 }
		type inputRow struct{ Key []*inner }
		type outputRow struct{ Key []inner }
		a, b := inner{V: 5}, inner{V: 7}
		var buf bytes.Buffer
		w := parquet.NewGenericWriter[inputRow](&buf)
		if _, err := w.Write([]inputRow{{Key: []*inner{nil, &a}}, {Key: []*inner{&b}}}); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		got, err := parquet.Read[outputRow](bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		if err != nil {
			t.Fatal(err)
		}
		want := []outputRow{{Key: []inner{{V: 0}, {V: 5}}}, {Key: []inner{{V: 7}}}}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %#v, want %#v", got, want)
		}
	})

	t.Run("strings", func(t *testing.T) {
		type inputRow struct{ Key []*string }
		type outputRow struct{ Key []string }
		hi, yo := "hi", "yo"
		var buf bytes.Buffer
		w := parquet.NewGenericWriter[inputRow](&buf)
		if _, err := w.Write([]inputRow{{Key: []*string{nil, &hi}}, {Key: []*string{&yo}}}); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		got, err := parquet.Read[outputRow](bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		if err != nil {
			t.Fatal(err)
		}
		want := []outputRow{{Key: []string{"", "hi"}}, {Key: []string{"yo"}}}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %#v, want %#v", got, want)
		}
	})

	t.Run("optional list nulls stay null", func(t *testing.T) {
		type row struct {
			Key []*int64 `parquet:"key,list"`
		}
		input := []row{
			{Key: []*int64{nil, &five, &zero}},
			{Key: []*int64{&seven}},
			{Key: []*int64{nil}},
			{Key: []*int64{}},
		}
		var buf bytes.Buffer
		w := parquet.NewGenericWriter[row](&buf)
		if _, err := w.Write(input); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		got, err := parquet.Read[row](bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != len(input) {
			t.Fatalf("read %d rows, want %d", len(got), len(input))
		}
		for i := range input {
			if !int64PtrsEqual(got[i].Key, input[i].Key) {
				t.Errorf("row %d: got %s, want %s", i, formatInt64Ptrs(got[i].Key), formatInt64Ptrs(input[i].Key))
			}
		}
	})

	// Precedent: a nil pointer written to a required non-repeated leaf is a
	// physical zero, not a null and not a missing value.
	t.Run("required non-repeated", func(t *testing.T) {
		type row struct{ Key *int64 }
		schema := parquet.NewSchema("row", parquet.Group{
			"Key": parquet.Leaf(parquet.Int64Type),
		})
		var buf bytes.Buffer
		w := parquet.NewGenericWriter[row](&buf, schema)
		if _, err := w.Write([]row{{Key: nil}, {Key: &five}, {Key: nil}}); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		type out struct{ Key int64 }
		got, err := parquet.Read[out](bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		if err != nil {
			t.Fatal(err)
		}
		want := []out{{Key: 0}, {Key: 5}, {Key: 0}}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %#v, want %#v", got, want)
		}
	})
}

func TestRepeatedColumnLessComparesLaterValues(t *testing.T) {
	type row struct{ Key []int64 }

	for _, descending := range []bool{false, true} {
		t.Run(fmt.Sprintf("descending=%t", descending), func(t *testing.T) {
			column := parquet.Ascending("Key")
			if descending {
				column = parquet.Descending("Key")
			}
			buffer := parquet.NewGenericBuffer[row](parquet.SortingRowGroupConfig(
				parquet.SortingColumns(column),
			))
			if _, err := buffer.Write([]row{{Key: []int64{0, 1}}, {Key: []int64{0, 0}}}); err != nil {
				t.Fatal(err)
			}
			// [0, 0] comes first ascending and second descending.
			wantLess := !descending
			if got := buffer.Less(1, 0); got != wantLess {
				t.Fatalf("Less(1, 0) = %t, want %t", got, wantLess)
			}
			sort.Stable(buffer)
			got := readBufferRows(t, buffer)
			want := [][]int64{{0, 0}, {0, 1}}
			if descending {
				want = [][]int64{{0, 1}, {0, 0}}
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("got %#v, want %#v", got, want)
			}
		})
	}
}

// Compare successive nullable values. Preserve Buffer's existing prefix rule:
// shorter keys sort first ascending, last descending. An empty repeated group
// has a null placeholder, so it compares like one null element.
func compareRepeatedElements(a, b []repeatedElement, descending, nullsFirst bool) int {
	if len(a) == 0 {
		a = []repeatedElement{{}}
	}
	if len(b) == 0 {
		b = []repeatedElement{{}}
	}
	for i := range min(len(a), len(b)) {
		x, y := a[i].Value, b[i].Value
		if x == nil || y == nil {
			if x == y {
				continue
			}
			if (x == nil) == nullsFirst {
				return -1
			}
			return 1
		}
		c := cmp.Compare(*x, *y)
		if descending {
			c = -c
		}
		if c != 0 {
			return c
		}
	}
	c := cmp.Compare(len(a), len(b))
	if descending {
		c = -c
	}
	return c
}

type repeatedElement struct{ Value *int64 }

func TestRepeatedColumnLessPairs(t *testing.T) {
	type row struct{ Key []repeatedElement }

	minusOne, zero, one := int64(-1), int64(0), int64(1)
	alphabet := []repeatedElement{{}, {&minusOne}, {&zero}, {&one}}
	keys := [][]repeatedElement{nil}
	start, end := 0, 1
	for range 3 {
		for _, key := range keys[start:end] {
			for _, e := range alphabet {
				next := slices.Clone(key)
				keys = append(keys, append(next, e))
			}
		}
		start, end = end, len(keys)
	}

	for _, descending := range []bool{false, true} {
		for _, nullsFirst := range []bool{false, true} {
			column := parquet.Ascending("Key", "Value")
			if descending {
				column = parquet.Descending("Key", "Value")
			}
			if nullsFirst {
				column = parquet.NullsFirst(column)
			}
			buffer := parquet.NewGenericBuffer[row](parquet.SortingRowGroupConfig(
				parquet.SortingColumns(column),
			))
			pairs, wrong := 0, 0
			var first string
			for i, a := range keys {
				for _, b := range keys[i+1:] {
					buffer.Reset()
					if _, err := buffer.Write([]row{{Key: a}, {Key: b}}); err != nil {
						t.Fatal(err)
					}
					c := compareRepeatedElements(a, b, descending, nullsFirst)
					if buffer.Less(0, 1) != (c < 0) || buffer.Less(1, 0) != (c > 0) {
						if wrong == 0 {
							first = fmt.Sprintf("a=%s b=%s compare=%d Less(0,1)=%t Less(1,0)=%t",
								formatRepeatedElements(a), formatRepeatedElements(b), c, buffer.Less(0, 1), buffer.Less(1, 0))
						}
						wrong++
					}
					pairs++
				}
			}
			t.Logf("descending=%t nullsFirst=%t keys=%d pairs=%d wrong_pairs=%d", descending, nullsFirst, len(keys), pairs, wrong)
			if wrong != 0 {
				t.Errorf("descending=%t nullsFirst=%t wrong_pairs=%d first %s", descending, nullsFirst, wrong, first)
			}
		}
	}
}

func readBufferRows(t *testing.T, buffer parquet.RowGroup) [][]int64 {
	t.Helper()
	type row struct{ Key []int64 }
	reader := parquet.NewGenericRowGroupReader[row](buffer)
	defer reader.Close()
	rows := make([]row, int(buffer.NumRows()))
	n, err := reader.Read(rows)
	if n != len(rows) || (err != nil && err != io.EOF) {
		t.Fatalf("read: n=%d err=%v", n, err)
	}
	got := make([][]int64, len(rows))
	for i := range rows {
		got[i] = rows[i].Key
	}
	return got
}

func int64PtrsEqual(got, want []*int64) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range want {
		switch {
		case want[i] == nil && got[i] == nil:
		case want[i] == nil || got[i] == nil:
			return false
		case *want[i] != *got[i]:
			return false
		}
	}
	return true
}

func formatInt64Ptrs(s []*int64) string {
	parts := make([]string, len(s))
	for i, p := range s {
		if p == nil {
			parts[i] = "nil"
		} else {
			parts[i] = fmt.Sprint(*p)
		}
	}
	return fmt.Sprint(parts)
}

func formatRepeatedElements(s []repeatedElement) string {
	parts := make([]string, len(s))
	for i, e := range s {
		if e.Value == nil {
			parts[i] = "nil"
		} else {
			parts[i] = fmt.Sprint(*e.Value)
		}
	}
	return fmt.Sprint(parts)
}
