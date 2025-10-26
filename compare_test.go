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
