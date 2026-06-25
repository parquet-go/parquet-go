package parquet

import "testing"

func assertCompare(t *testing.T, a, b Value, cmp func(Value, Value) int, want int) {
	if got := cmp(a, b); got != want {
		t.Errorf("compare(%v, %v): got=%d want=%d", a, b, got, want)
	}
}

func TestCompareNullsFirst(t *testing.T) {
	cmp := CompareNullsFirst(Int32Type.Compare)
	assertCompare(t, Value{}, Value{}, cmp, 0)
	assertCompare(t, Value{}, ValueOf(int32(0)), cmp, -1)
	assertCompare(t, ValueOf(int32(0)), Value{}, cmp, +1)
	assertCompare(t, ValueOf(int32(0)), ValueOf(int32(1)), cmp, -1)
}

func TestCompareNullsLast(t *testing.T) {
	cmp := CompareNullsLast(Int32Type.Compare)
	assertCompare(t, Value{}, Value{}, cmp, 0)
	assertCompare(t, Value{}, ValueOf(int32(0)), cmp, +1)
	assertCompare(t, ValueOf(int32(0)), Value{}, cmp, -1)
	assertCompare(t, ValueOf(int32(0)), ValueOf(int32(1)), cmp, -1)
}

func TestCompareRowsTimeNanosecond(t *testing.T) {
	typ := &timeNanoAdjustedToUTC

	cmpAsc := compareRowsFuncOfIndexAscending(0, typ)
	cmpDesc := compareRowsFuncOfIndexDescending(0, typ)

	// 10,000,000 ns (10 ms)
	row1 := Row{ValueOf(int64(10000000))}
	// 3,000,000,000,000 ns (3000 seconds = 50 minutes).
	// If compared as a signed 32-bit int, 3,000,000,000,000 wraps around or truncates.
	// Specifically, int32(3000000000000) = -1305411584
	row2 := Row{ValueOf(int64(3000000000000))}

	// Ascending: row1 should be less than row2 (returns -1)
	if got := cmpAsc(row1, row2); got >= 0 {
		t.Errorf("Ascending: 10ms should be less than 50min, got %d", got)
	}

	// Descending: row1 should be greater than row2 (returns +1)
	if got := cmpDesc(row1, row2); got <= 0 {
		t.Errorf("Descending: 10ms should be greater than 50min, got %d", got)
	}
}

func BenchmarkCompareBE128(b *testing.B) {
	v1 := [16]byte{}
	v2 := [16]byte{}

	for b.Loop() {
		compareBE128(&v1, &v2)
	}
}

func BenchmarkLessBE128(b *testing.B) {
	v1 := [16]byte{}
	v2 := [16]byte{}

	for b.Loop() {
		lessBE128(&v1, &v2)
	}
}
